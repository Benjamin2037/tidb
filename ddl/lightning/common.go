package lightning

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)
type TiDBManager struct {
	db     *sql.DB
	parser *parser.Parser
}

func NewTiDBManager(ctx context.Context, dsn config.DBStore, tls *common.TLS) (*TiDBManager, error) {
	db, err := DBFromConfig(ctx, dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewTiDBManagerWithDB(db, dsn.SQLMode), nil
}

// NewTiDBManagerWithDB creates a new TiDB manager with an existing database
// connection.
func NewTiDBManagerWithDB(db *sql.DB, sqlMode mysql.SQLMode) *TiDBManager {
	parser := parser.New()
	parser.SetSQLMode(sqlMode)

	return &TiDBManager{
		db:     db,
		parser: parser,
	}
}
func (timgr *TiDBManager) Close() {
	timgr.db.Close()
}
// DefaultImportantVariables is used in ObtainImportantVariables to retrieve the system
// variables from downstream which may affect KV encode result. The values record the default
// values if missing.
var DefaultImportantVariables = map[string]string{
	"max_allowed_packet":      "67108864",
	"div_precision_increment": "4",
	"time_zone":               "SYSTEM",
	"lc_time_names":           "en_US",
	"default_week_format":     "0",
	"block_encryption_mode":   "aes-128-ecb",
	"group_concat_max_len":    "1024",
}

// DefaultImportVariablesTiDB is used in ObtainImportantVariables to retrieve the system
// variables from downstream in local/importer backend. The values record the default
// values if missing.
var DefaultImportVariablesTiDB = map[string]string{
	"tidb_row_format_version": "1",
}

func ObtainImportantVariables(ctx context.Context, g glue.SQLExecutor, needTiDBVars bool) map[string]string {
	var query strings.Builder
	query.WriteString("SHOW VARIABLES WHERE Variable_name IN ('")
	first := true
	for k := range DefaultImportantVariables {
		if first {
			first = false
		} else {
			query.WriteString("','")
		}
		query.WriteString(k)
	}
	if needTiDBVars {
		for k := range DefaultImportVariablesTiDB {
			query.WriteString("','")
			query.WriteString(k)
		}
	}
	query.WriteString("')")
	kvs, err := g.QueryStringsWithLog(ctx, query.String(), "obtain system variables", log.L())
	if err != nil {
		// error is not fatal
		log.L().Warn("obtain system variables failed, use default variables instead", log.ShortError(err))
	}

	// convert result into a map. fill in any missing variables with default values.
	result := make(map[string]string, len(DefaultImportantVariables)+len(DefaultImportVariablesTiDB))
	for _, kv := range kvs {
		result[kv[0]] = kv[1]
	}

	setDefaultValue := func(res map[string]string, vars map[string]string) {
		for k, defV := range vars {
			if _, ok := res[k]; !ok {
				res[k] = defV
			}
		}
	}
	setDefaultValue(result, DefaultImportantVariables)
	if needTiDBVars {
		setDefaultValue(result, DefaultImportVariablesTiDB)
	}

	return result
}

func DBFromConfig(ctx context.Context, dsn config.DBStore) (*sql.DB, error) {
	param := common.MySQLConnectParam{
		Host:             dsn.Host,
		Port:             dsn.Port,
		User:             dsn.User,
		Password:         dsn.Psw,
		SQLMode:          dsn.StrSQLMode,
		MaxAllowedPacket: dsn.MaxAllowedPacket,
		TLS:              dsn.TLS,
	}

	db, err := param.Connect()
	if err != nil {
		return nil, errors.Trace(err)
	}

	vars := map[string]string{
		"tidb_build_stats_concurrency":       strconv.Itoa(dsn.BuildStatsConcurrency),
		"tidb_distsql_scan_concurrency":      strconv.Itoa(dsn.DistSQLScanConcurrency),
		"tidb_index_serial_scan_concurrency": strconv.Itoa(dsn.IndexSerialScanConcurrency),
		"tidb_checksum_table_concurrency":    strconv.Itoa(dsn.ChecksumTableConcurrency),

		// after https://github.com/pingcap/tidb/pull/17102 merge,
		// we need set session to true for insert auto_random value in TiDB Backend
		"allow_auto_random_explicit_insert": "1",
		// allow use _tidb_rowid in sql statement
		"tidb_opt_write_row_id": "1",
		// always set auto-commit to ON
		"autocommit": "1",
		// alway set transaction mode to optimistic
		"tidb_txn_mode": "optimistic",
	}

	if dsn.Vars != nil {
		maps.Copy(vars, dsn.Vars)
	}

	for k, v := range vars {
		q := fmt.Sprintf("SET SESSION %s = '%s';", k, v)
		if _, err1 := db.ExecContext(ctx, q); err1 != nil {
			log.L().Warn("set session variable failed, will skip this query", zap.String("query", q),
				zap.Error(err1))
			delete(vars, k)
		}
	}
	_ = db.Close()

	param.Vars = vars
	db, err = param.Connect()
	return db, errors.Trace(err)
}	