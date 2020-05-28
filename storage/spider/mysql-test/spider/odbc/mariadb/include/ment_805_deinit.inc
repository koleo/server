let $MASTER_1_COMMENT_2_1= $MASTER_1_COMMENT_2_1_BACKUP;

SELECT spider_direct_sql('DROP USER tu@\'%\'', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver", database ""');

--disable_query_log
--disable_result_log
--source ../t/test_deinit.inc
--enable_result_log
--enable_query_log
