// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package restore

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	lit "github.com/pingcap/tidb/ddl/lightning"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
)

func InitSchema(ctx context.Context, g glue.Glue, database string, tablesSchema map[string]string) error {
	logger := log.With(zap.String("db", database))
	sqlExecutor := g.GetSQLExecutor()

	var createDatabase strings.Builder
	createDatabase.WriteString("CREATE DATABASE IF NOT EXISTS ")
	common.WriteMySQLIdentifier(&createDatabase, database)
	err := sqlExecutor.ExecuteWithLog(ctx, createDatabase.String(), "create database", logger)
	if err != nil {
		return errors.Trace(err)
	}

	task := logger.Begin(zap.InfoLevel, "create tables")
	var sqlCreateStmts []string
loopCreate:
	for tbl, sqlCreateTable := range tablesSchema {
		task.Debug("create table", zap.String("schema", sqlCreateTable))

		sqlCreateStmts, err = createIfNotExistsStmt(g.GetParser(), sqlCreateTable, database, tbl)
		if err != nil {
			break
		}

		// TODO: maybe we should put these createStems into a transaction
		for _, s := range sqlCreateStmts {
			err = sqlExecutor.ExecuteWithLog(
				ctx,
				s,
				"create table",
				logger.With(zap.String("table", common.UniqueTable(database, tbl))),
			)
			if err != nil {
				break loopCreate
			}
		}
	}
	task.End(zap.ErrorLevel, err)

	return errors.Trace(err)
}

func createIfNotExistsStmt(p *parser.Parser, createTable, dbName, tblName string) ([]string, error) {
	stmts, _, err := p.ParseSQL(createTable)
	if err != nil {
		return []string{}, common.ErrInvalidSchemaStmt.Wrap(err).GenWithStackByArgs(createTable)
	}

	var res strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreTiDBSpecialComment, &res)

	retStmts := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		switch node := stmt.(type) {
		case *ast.CreateDatabaseStmt:
			node.Name = dbName
			node.IfNotExists = true
		case *ast.CreateTableStmt:
			node.Table.Schema = model.NewCIStr(dbName)
			node.Table.Name = model.NewCIStr(tblName)
			node.IfNotExists = true
		case *ast.CreateViewStmt:
			node.ViewName.Schema = model.NewCIStr(dbName)
			node.ViewName.Name = model.NewCIStr(tblName)
		case *ast.DropTableStmt:
			node.Tables[0].Schema = model.NewCIStr(dbName)
			node.Tables[0].Name = model.NewCIStr(tblName)
			node.IfExists = true
		}
		if err := stmt.Restore(ctx); err != nil {
			return []string{}, common.ErrInvalidSchemaStmt.Wrap(err).GenWithStackByArgs(createTable)
		}
		ctx.WritePlain(";")
		retStmts = append(retStmts, res.String())
		res.Reset()
	}

	return retStmts, nil
}

func (timgr *TiDBManager) DropTable(ctx context.Context, tableName string) error {
	sql := common.SQLWithRetry{
		DB:     timgr.db,
		Logger: log.With(zap.String("table", tableName)),
	}
	return sql.Exec(ctx, "drop table", "DROP TABLE "+tableName)
}

func LoadSchemaInfo(
	ctx context.Context,
	schemas []*mydump.MDDatabaseMeta,
	getTables func(context.Context, string) ([]*model.TableInfo, error),
) (map[string]*checkpoints.TidbDBInfo, error) {
	result := make(map[string]*checkpoints.TidbDBInfo, len(schemas))
	for _, schema := range schemas {
		tables, err := getTables(ctx, schema.Name)
		if err != nil {
			return nil, err
		}

		tableMap := make(map[string]*model.TableInfo, len(tables))
		for _, tbl := range tables {
			tableMap[tbl.Name.L] = tbl
		}

		dbInfo := &checkpoints.TidbDBInfo{
			Name:   schema.Name,
			Tables: make(map[string]*checkpoints.TidbTableInfo),
		}

		for _, tbl := range schema.Tables {
			tblInfo, ok := tableMap[strings.ToLower(tbl.Name)]
			if !ok {
				return nil, common.ErrSchemaNotExists.GenWithStackByArgs(tbl.DB, tbl.Name)
			}
			tableName := tblInfo.Name.String()
			if tblInfo.State != model.StatePublic {
				err := errors.Errorf("table [%s.%s] state is not public", schema.Name, tableName)
				metric.RecordTableCount(metric.TableStatePending, err)
				return nil, err
			}
			metric.RecordTableCount(metric.TableStatePending, err)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// Table names are case-sensitive in mydump.MDTableMeta.
			// We should always use the original tbl.Name in checkpoints.
			tableInfo := &checkpoints.TidbTableInfo{
				ID:   tblInfo.ID,
				DB:   schema.Name,
				Name: tbl.Name,
				Core: tblInfo,
			}
			dbInfo.Tables[tbl.Name] = tableInfo
		}

		result[schema.Name] = dbInfo
	}
	return result, nil
}

func ObtainGCLifeTime(ctx context.Context, db *sql.DB) (string, error) {
	var gcLifeTime string
	err := common.SQLWithRetry{DB: db, Logger: log.L()}.QueryRow(
		ctx,
		"obtain GC lifetime",
		"SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'",
		&gcLifeTime,
	)
	return gcLifeTime, err
}

func UpdateGCLifeTime(ctx context.Context, db *sql.DB, gcLifeTime string) error {
	sql := common.SQLWithRetry{
		DB:     db,
		Logger: log.With(zap.String("gcLifeTime", gcLifeTime)),
	}
	return sql.Exec(ctx, "update GC lifetime",
		"UPDATE mysql.tidb SET VARIABLE_VALUE = ? WHERE VARIABLE_NAME = 'tikv_gc_life_time'",
		gcLifeTime,
	)
}

func ObtainNewCollationEnabled(ctx context.Context, g glue.SQLExecutor) (bool, error) {
	newCollationEnabled := false
	newCollationVal, err := g.ObtainStringWithLog(
		ctx,
		"SELECT variable_value FROM mysql.tidb WHERE variable_name = 'new_collation_enabled'",
		"obtain new collation enabled",
		log.L(),
	)
	if err == nil && newCollationVal == "True" {
		newCollationEnabled = true
	} else if errors.ErrorEqual(err, sql.ErrNoRows) {
		// ignore if target variable is not found, this may happen if tidb < v4.0
		newCollationEnabled = false
		err = nil
	}

	return newCollationEnabled, errors.Trace(err)
}

// AlterAutoIncrement rebase the table auto increment id
//
// NOTE: since tidb can make sure the auto id is always be rebase even if the `incr` value is smaller
// the the auto incremanet base in tidb side, we needn't fetch currently auto increment value here.
// See: https://github.com/pingcap/tidb/blob/64698ef9a3358bfd0fdc323996bb7928a56cadca/ddl/ddl_api.go#L2528-L2533
func AlterAutoIncrement(ctx context.Context, g glue.SQLExecutor, tableName string, incr uint64) error {
	var query string
	logger := log.With(zap.String("table", tableName), zap.Uint64("auto_increment", incr))
	if incr > math.MaxInt64 {
		// automatically set max value
		logger.Warn("auto_increment out of the maximum value TiDB supports, automatically set to the max", zap.Uint64("auto_increment", incr))
		incr = math.MaxInt64
		query = fmt.Sprintf("ALTER TABLE %s FORCE AUTO_INCREMENT=%d", tableName, incr)
	} else {
		query = fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT=%d", tableName, incr)
	}
	task := logger.Begin(zap.InfoLevel, "alter table auto_increment")
	err := g.ExecuteWithLog(ctx, query, "alter table auto_increment", logger)
	task.End(zap.ErrorLevel, err)
	if err != nil {
		task.Error(
			"alter table auto_increment failed, please perform the query manually (this is needed no matter the table has an auto-increment column or not)",
			zap.String("query", query),
		)
	}
	return errors.Annotatef(err, "%s", query)
}

func AlterAutoRandom(ctx context.Context, g glue.SQLExecutor, tableName string, randomBase uint64, maxAutoRandom uint64) error {
	logger := log.With(zap.String("table", tableName), zap.Uint64("auto_random", randomBase))
	if randomBase == maxAutoRandom+1 {
		// insert a tuple with key maxAutoRandom
		randomBase = maxAutoRandom
	} else if randomBase > maxAutoRandom {
		// TiDB does nothing when inserting an overflow value
		logger.Warn("auto_random out of the maximum value TiDB supports")
		return nil
	}
	query := fmt.Sprintf("ALTER TABLE %s AUTO_RANDOM_BASE=%d", tableName, randomBase)
	task := logger.Begin(zap.InfoLevel, "alter table auto_random")
	err := g.ExecuteWithLog(ctx, query, "alter table auto_random_base", logger)
	task.End(zap.ErrorLevel, err)
	if err != nil {
		task.Error(
			"alter table auto_random_base failed, please perform the query manually (this is needed no matter the table has an auto-random column or not)",
			zap.String("query", query),
		)
	}
	return errors.Annotatef(err, "%s", query)
}
