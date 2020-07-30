--disable_query_log
--disable_result_log
--source ../t/test_init.inc
eval SELECT * INTO OUTFILE '$MYSQLTEST_VARDIR/tmp/spider_filedsn.dsn' FROM (
  SELECT '[ODBC]' UNION ALL
  SELECT 'Driver=MariaDB ODBC 3.0 Driver' UNION ALL
  SELECT 'Servername=127.0.0.1' UNION ALL
  SELECT 'Database=auto_test_remote' UNION ALL
  SELECT 'UserName=root' UNION ALL
  SELECT 'Password=' UNION ALL
  SELECT 'Port=$CHILD2_1_MYPORT'
) a;
--enable_result_log
--enable_query_log
--let $MASTER_1_COMMENT_2_1_BACKUP= $MASTER_1_COMMENT_2_1
let $MASTER_1_COMMENT_2_1=
  COMMENT='filedsn "$MYSQLTEST_VARDIR/tmp/spider_filedsn.dsn", table "ta_r", wrapper "odbc_mariadb"';
let $SPIDER_DIRECT_SQL_1=
  SELECT spider_direct_sql('SELECT 1', '', 'filedsn "$MYSQLTEST_VARDIR/tmp/spider_filedsn.dsn", wrapper "odbc_mariadb"');
