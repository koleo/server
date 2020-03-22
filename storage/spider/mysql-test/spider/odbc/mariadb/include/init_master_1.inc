--source ../include/init_spider.inc
SET spider_direct_order_limit= 10000;
SET spider_init_sql_alloc_size= 1;
SET spider_conn_recycle_mode= 0;
let $MASTER_1_COMMENT_2_1=
  COMMENT='srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", table "ta_r", wrapper "odbc_mariadb"';
let $MASTER_1_COMMENT2_2_1=
  COMMENT='srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", table "ta_r_no_idx", wrapper "odbc_mariadb"'
  CONNECTION='prt "2000000"';
let $MASTER_1_COMMENT_P_2_1=
  COMMENT='table "ta_r3"'
  PARTITION BY KEY(a) (
    PARTITION pt1 COMMENT='srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", table "ta_r2",
    priority "1000"',
    PARTITION pt2 COMMENT='srv "s_2_2", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", priority "1000001"'
  );
let $MASTER_1_COMMENT2_P_2_1=
  COMMENT='table "ta_r3"'
  PARTITION BY RANGE(a) (
    PARTITION pt1 VALUES LESS THAN (4) COMMENT='srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver",
    table "ta_r2", priority "1000"',
    PARTITION pt2 VALUES LESS THAN MAXVALUE
    COMMENT='srv "s_2_2", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", priority "1000001"'
  );
let $MASTER_1_COMMENT3_2_1=
  COMMENT='srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", table "ta_r_auto_inc",
  wrapper "odbc_mariadb"';
let $MASTER_1_COMMENT3_P_2_1=
  COMMENT='table "s_2_1_ta_r_int"'
  PARTITION BY LIST(MOD(a, 2)) (
    PARTITION pt1 VALUES IN (0)
    COMMENT='srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", priority "1000"',
    PARTITION pt2 VALUES IN (1)
    COMMENT='srv "s_2_2", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", priority "1000001",
    table "s_2_2_ta_r_int"'
  );
let $MASTER_1_COMMENT4_2_1=
  COMMENT='srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", table "s_2_1_ta_r_int",
  wrapper "odbc_mariadb"';
let $MASTER_1_COMMENT5_2_1=
  COMMENT='srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", table "ta_r_3", wrapper "odbc_mariadb"';
let $MASTER_1_COMMENT_FT_2_1=
  COMMENT='srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", table "ft_r", wrapper "odbc_mariadb"';
let $MASTER_1_COMMENT2_FT_P_2_1=
  COMMENT='table "ft_r3"'
  PARTITION BY RANGE(a) (
    PARTITION pt1 VALUES LESS THAN (4) COMMENT='srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver",
    table "ft_r2", priority "1000"',
    PARTITION pt2 VALUES LESS THAN MAXVALUE
    COMMENT='srv "s_2_2", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", priority "1000001"'
  );
let $MASTER_1_COMMENT_GM_2_1=
  COMMENT='srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", table "gm_r"';
let $MASTER_1_COMMENT2_GM_P_2_1=
  COMMENT='table "gm_r3"'
  PARTITION BY RANGE(a) (
    PARTITION pt1 VALUES LESS THAN (4) COMMENT='srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver",
    table "gm_r2", priority "1000"',
    PARTITION pt2 VALUES LESS THAN MAXVALUE
    COMMENT='srv "s_2_2", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver", priority "1000001"'
  );
let $MASTER_1_COMMENT_LOCK1=
  COMMENT 'tbl "t1_1 t1_2", srv "s_2_1 s_2_2", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver"';
let $MASTER_1_COMMENT_LOCK2=
  COMMENT 'tbl "t2_1 t2_2", srv "s_2_2 s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver"';
let $MASTER_1_COMMENT_INCREMENT1_1=
  COMMENT 'aim "0", tbl "t1_1", srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver"';
let $MASTER_1_COMMENT_INCREMENT1_P_1=
  COMMENT 'aim "0"'
  PARTITION BY LIST(MOD(id, 2)) (
    PARTITION pt1 VALUES IN (0)
    COMMENT='tbl "t1_1", srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver"',
    PARTITION pt2 VALUES IN (1)
    COMMENT='tbl "t1_2", srv "s_2_2", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver"'
  );
let $MASTER_1_COMMENT_READONLY1_1=
  COMMENT 'read_only_mode "1", tbl "t1_1", srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver"';
let $MASTER_1_COMMENT_ERROR_MODE1_1=
  COMMENT 'erm "1", ewm "1", tbl "ter1_1", srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver"';
let $MASTER_1_COMMENT_TEXT_PK1_1=
  COMMENT 'tbl "t1", srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver"';
let $MASTER_1_COMMENT_TEXT_KEY1_1=
  COMMENT 'tbl "t1", srv "s_2_1", default_group "MariaDB\\\\ ODBC\\\\ 3.0\\\\ Driver"';
let $MASTER_1_CHECK_DIRECT_UPDATE_STATUS=
  SHOW GLOBAL STATUS LIKE 'Spider_direct_update%';
let $MASTER_1_CHECK_DIRECT_DELETE_STATUS=
  SHOW GLOBAL STATUS LIKE 'Spider_direct_delete%';
let $MASTER_1_CHECK_DIRECT_ORDER_LIMIT_STATUS=
  SHOW GLOBAL STATUS LIKE 'Spider_direct_order_limit%';
let $MASTER_1_CHECK_DIRECT_AGGREGATE_STATUS=
  SHOW GLOBAL STATUS LIKE 'Spider_direct_aggregate%';
let $MASTER_1_AUTO_INCREMENT_INCREMENT1=
  SET SESSION AUTO_INCREMENT_INCREMENT = 1 $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_INCREMENT = 1', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_OFFSET = 1', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_INCREMENT = 1', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_OFFSET = 1', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $MASTER_1_AUTO_INCREMENT_INCREMENT2=
  SET SESSION AUTO_INCREMENT_INCREMENT = 777 $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_INCREMENT = 4', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_OFFSET = 2', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_INCREMENT = 4', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_OFFSET = 3', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $MASTER_1_AUTO_INCREMENT_OFFSET1=
  SET SESSION AUTO_INCREMENT_OFFSET = 1 $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_INCREMENT = 1', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_OFFSET = 1', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_INCREMENT = 1', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_OFFSET = 1', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $MASTER_1_AUTO_INCREMENT_OFFSET2=
  SET SESSION AUTO_INCREMENT_OFFSET = 777 $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_INCREMENT = 4', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_OFFSET = 2', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_INCREMENT = 4', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('SET SESSION AUTO_INCREMENT_OFFSET = 3', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $MASTER_1_AUTO_INCREMENT_OFFSET3=
  SET SESSION AUTO_INCREMENT_OFFSET = 1;
let $MASTER_1_AUTO_INCREMENT_OFFSET4=
  SET SESSION AUTO_INCREMENT_OFFSET = 777;
