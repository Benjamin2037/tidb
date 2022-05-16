package lightning
func TestObtainRowFormatVersionSucceed(t *testing.T) {
	s, clean := newTiDBSuite(t)
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
	s, clean := newTiDBSuite(t)
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