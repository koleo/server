--disable_query_log
--disable_result_log
--source ../t/test_init.inc
--enable_result_log
--enable_query_log

SELECT spider_direct_sql('GRANT ALL ON *.* TO tu@\'%\' IDENTIFIED BY \');{(\'', '', 'srv "s_2_1", driver "MariaDB ODBC 3.0 Driver", database ""');

let $MASTER_1_COMMENT_2_1_BACKUP= $MASTER_1_COMMENT_2_1;
let $MASTER_1_COMMENT_2_1=
  COMMENT='srv "s_2_1", driver "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", table "ta_r", wrapper "odbc_mariadb", user "tu", password ");{("';
