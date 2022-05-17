package lightning

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)
type tidbSuite struct {
	mockDB sqlmock.Sqlmock
	timgr  *TiDBManager
	tiGlue glue.Glue
}

func NewTiDBSuite(t *testing.T) (*tidbSuite, func()) {
	var s tidbSuite
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	s.mockDB = mock
	defaultSQLMode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	require.NoError(t, err)

	s.timgr = NewTiDBManagerWithDB(db, defaultSQLMode)
	s.tiGlue = glue.NewExternalTiDBGlue(db, defaultSQLMode)
	return &s, func() {
		s.timgr.Close()
		require.NoError(t, s.mockDB.ExpectationsWereMet())
	}
}

func TestObtainRowFormatVersionSucceed(t *testing.T) {
	s, clean := NewTiDBSuite(t)
	defer clean()
	ctx := context.Background()

	s.mockDB.
		ExpectBegin()
	s.mockDB.
		ExpectQuery(`SHOW VARIABLES WHERE Variable_name IN \(.*'tidb_row_format_version'.*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("tidb_row_format_version", "2").
			AddRow("max_allowed_packet", "1073741824").
			AddRow("div_precision_increment", "10").
			AddRow("time_zone", "-08:00").
			AddRow("lc_time_names", "ja_JP").
			AddRow("default_week_format", "1").
			AddRow("block_encryption_mode", "aes-256-cbc").
			AddRow("group_concat_max_len", "1073741824"))
	s.mockDB.
		ExpectCommit()
	s.mockDB.
		ExpectClose()

	sysVars := ObtainImportantVariables(ctx, s.tiGlue.GetSQLExecutor(), true)
	require.Equal(t, map[string]string{
		"tidb_row_format_version": "2",
		"max_allowed_packet":      "1073741824",
		"div_precision_increment": "10",
		"time_zone":               "-08:00",
		"lc_time_names":           "ja_JP",
		"default_week_format":     "1",
		"block_encryption_mode":   "aes-256-cbc",
		"group_concat_max_len":    "1073741824",
	}, sysVars)
}

func TestObtainRowFormatVersionFailure(t *testing.T) {
	s, clean := NewTiDBSuite(t)
	defer clean()
	ctx := context.Background()

	s.mockDB.
		ExpectBegin()
	s.mockDB.
		ExpectQuery(`SHOW VARIABLES WHERE Variable_name IN \(.*'tidb_row_format_version'.*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("time_zone", "+00:00"))
	s.mockDB.
		ExpectCommit()
	s.mockDB.
		ExpectClose()

	sysVars := ObtainImportantVariables(ctx, s.tiGlue.GetSQLExecutor(), true)
	require.Equal(t, map[string]string{
		"tidb_row_format_version": "1",
		"max_allowed_packet":      "67108864",
		"div_precision_increment": "4",
		"time_zone":               "+00:00",
		"lc_time_names":           "en_US",
		"default_week_format":     "0",
		"block_encryption_mode":   "aes-128-ecb",
		"group_concat_max_len":    "1024",
	}, sysVars)
}