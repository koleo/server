--source ../include/init_spider.inc
--source env_2_1.inc
--source env_2_2.inc
--source env_2_3.inc
SET spider_direct_order_limit= 10000;
SET spider_init_sql_alloc_size= 1;
SET spider_conn_recycle_mode= 0;
let $MASTER_1_COMMENT_2_1=
  COMMENT='dsn "$ODBC_DSN_HA1", table "$S_2_1_TA_R", wrapper "odbc",
  pk_name "$S_2_1_TA_R_PK"';
let $MASTER_1_COMMENT2_2_1=
  COMMENT='dsn "$ODBC_DSN_HA1", table "$S_2_1_TA_R_NO_IDX", wrapper "odbc"'
  CONNECTION='prt "2000000"';
let $MASTER_1_COMMENT_P_2_1=
  COMMENT='table "$S_2_2_TA_R3", wrapper "odbc"'
  PARTITION BY KEY(a) (
    PARTITION pt1 COMMENT='dsn "$ODBC_DSN_HA1", table "$S_2_1_TA_R2",
    priority "1000", pk_name "$S_2_1_TA_R2_PK"',
    PARTITION pt2 COMMENT='dsn "$ODBC_DSN_HA2", priority "1000001",
    pk_name "$S_2_2_TA_R3_PK"'
  );
let $MASTER_1_COMMENT2_P_2_1=
  COMMENT='table "$S_2_2_TA_R3", wrapper "odbc"'
  PARTITION BY RANGE(a) (
    PARTITION pt1 VALUES LESS THAN (4) COMMENT='dsn "$ODBC_DSN_HA1",
    table "$S_2_1_TA_R2", priority "1000", pk_name "$S_2_1_TA_R2_PK"',
    PARTITION pt2 VALUES LESS THAN MAXVALUE
    COMMENT='dsn "$ODBC_DSN_HA2", priority "1000001", pk_name "$S_2_2_TA_R3_PK"'
  );
let $MASTER_1_COMMENT3_2_1=
  COMMENT='dsn "$ODBC_DSN_HA1", table "$S_2_1_TA_R_AUTO_INC",
  wrapper "odbc"'
  CONNECTION='pk_name "$S_2_1_TA_R_AUTO_INC_PK"';
let $MASTER_1_COMMENT3_P_2_1=
  COMMENT='table "$S_2_1_TA_R_INT", wrapper "odbc"'
  PARTITION BY LIST(MOD(a, 2)) (
    PARTITION pt1 VALUES IN (0)
    COMMENT='dsn "$ODBC_DSN_HA1", priority "1000", pk_name "$S_2_1_TA_R_INT_PK"',
    PARTITION pt2 VALUES IN (1)
    COMMENT='dsn "$ODBC_DSN_HA2", priority "1000001", pk_name "$S_2_2_TA_R_INT_PK",
    table "$S_2_2_TA_R_INT"'
  );
let $MASTER_1_COMMENT4_2_1=
  COMMENT='dsn "$ODBC_DSN_HA1", table "$S_2_1_TA_R_INT",
  wrapper "odbc"'
  CONNECTION='pk_name "$S_2_1_TA_R_INT_PK"';
let $MASTER_1_COMMENT5_2_1=
  COMMENT='dsn "$ODBC_DSN_HA1", table "$S_2_1_TA_R3"'
  CONNECTION='wrapper "odbc"';
let $MASTER_1_COMMENT_FT_2_1=
  COMMENT='dsn "$ODBC_DSN_HA1", table "$S_2_1_FT_R", wrapper "odbc"'
  CONNECTION='pk_name "$S_2_1_FT_R_PK"';
let $MASTER_1_COMMENT2_FT_P_2_1=
  COMMENT='table "$S_2_2_FT_R3", wrapper "odbc"'
  PARTITION BY RANGE(a) (
    PARTITION pt1 VALUES LESS THAN (4) COMMENT='dsn "$ODBC_DSN_HA1",
    table "$S_2_1_FT_R2", priority "1000", pk_name "$S_2_1_FT_R2_PK"',
    PARTITION pt2 VALUES LESS THAN MAXVALUE
    COMMENT='dsn "$ODBC_DSN_HA2", priority "1000001", pk_name "$S_2_2_FT_R3_PK"'
  );
let $MASTER_1_COMMENT_GM_2_1=
  COMMENT='dsn "$ODBC_DSN_HA1", wrapper "odbc", table "$S_2_1_GM_R", pk_name "$S_2_1_GM_R_PK"';
let $MASTER_1_COMMENT2_GM_P_2_1=
  COMMENT='table "$S_2_2_GM_R3", wrapper "odbc"'
  PARTITION BY RANGE(a) (
    PARTITION pt1 VALUES LESS THAN (4) COMMENT='dsn "$ODBC_DSN_HA1",
    table "$S_2_1_GM_R2", priority "1000", pk_name "$S_2_1_GM_R2_PK"',
    PARTITION pt2 VALUES LESS THAN MAXVALUE
    COMMENT='dsn "$ODBC_DSN_HA2", priority "1000001", pk_name "$S_2_2_GM_R3_PK"'
  );
let $MASTER_1_COMMENT_LOCK1=
  COMMENT 'tbl "$S_2_1_T1_1 $S_2_2_T1_2", dsn "$ODBC_DSN_HA1 $ODBC_DSN_HA2", wrapper "odbc",
  pk_name "$S_2_1_T1_1_PK $S_2_2_T1_2_PK"';
let $MASTER_1_COMMENT_LOCK2=
  COMMENT 'tbl "$S_2_2_T2_1 $S_2_1_T2_2", dsn "$ODBC_DSN_HA2 $ODBC_DSN_HA1", wrapper "odbc",
  pk_name "$S_2_2_T2_1_PK $S_2_1_T2_2_PK"';
let $MASTER_1_COMMENT_INCREMENT1_1=
  COMMENT 'aim "0", tbl "$S_2_1_T1_1", dsn "$ODBC_DSN_HA1", wrapper "odbc", pk_name "$S_2_1_T1_1_PK"';
let $MASTER_1_COMMENT_INCREMENT1_P_1=
  COMMENT 'aim "0", wrapper "odbc"'
  PARTITION BY LIST(MOD(id, 2)) (
    PARTITION pt1 VALUES IN (0)
    COMMENT='tbl "$S_2_1_T1_1", dsn "$ODBC_DSN_HA1", pk_name "$S_2_1_T1_1_PK"',
    PARTITION pt2 VALUES IN (1)
    COMMENT='tbl "$S_2_2_T1_2", dsn "$ODBC_DSN_HA2", pk_name "$S_2_2_T1_2_PK"'
  );
let $MASTER_1_COMMENT_READONLY1_1=
  COMMENT 'read_only_mode "1", tbl "$S_2_1_T1_1", dsn "$ODBC_DSN_HA1", wrapper "odbc",
  pk_name "$S_2_1_T1_1_PK"';
let $MASTER_1_COMMENT_ERROR_MODE1_1=
  COMMENT 'erm "1", ewm "1", tbl "ter1_1", dsn "$ODBC_DSN_HA1", wrapper "odbc"';
let $MASTER_1_COMMENT_TEXT_PK1_1=
  COMMENT 'tbl "$S_2_1_T1", dsn "$ODBC_DSN_HA1", wrapper "odbc", pk_name "$S_2_1_T1_PK"';
let $MASTER_1_COMMENT_TEXT_KEY1_1=
  COMMENT 'tbl "$S_2_1_T1", dsn "$ODBC_DSN_HA1", wrapper "odbc", pk_name "$S_2_1_T1_PK"';
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
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_1_T1_1_SEQ"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_1_T1_1_SEQ" START WITH 1 INCREMENT BY 1', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_2_T1_2_SEQ"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_2_T1_2_SEQ" START WITH 1 INCREMENT BY 1', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $MASTER_1_AUTO_INCREMENT_INCREMENT2=
  SET SESSION AUTO_INCREMENT_INCREMENT = 777 $STR_SEMICOLON
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_1_T1_1_SEQ"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_1_T1_1_SEQ" START WITH 2 INCREMENT BY 4', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_2_T1_2_SEQ"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_2_T1_2_SEQ" START WITH 3 INCREMENT BY 4', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $MASTER_1_AUTO_INCREMENT_OFFSET1=
  SET SESSION AUTO_INCREMENT_OFFSET = 1 $STR_SEMICOLON
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_1_T1_1_SEQ"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_1_T1_1_SEQ" START WITH 1 INCREMENT BY 1', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_2_T1_2_SEQ"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_2_T1_2_SEQ" START WITH 1 INCREMENT BY 1', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $MASTER_1_AUTO_INCREMENT_OFFSET2=
  SET SESSION AUTO_INCREMENT_OFFSET = 777 $STR_SEMICOLON
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_1_T1_1_SEQ"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_1_T1_1_SEQ" START WITH 2 INCREMENT BY 4', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_2_T1_2_SEQ"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_2_T1_2_SEQ" START WITH 3 INCREMENT BY 4', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $MASTER_1_AUTO_INCREMENT_DROP=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_1_T1_1_SEQ"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_2_T1_2_SEQ"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $MASTER_1_AUTO_INCREMENT_OFFSET3=
  SET SESSION AUTO_INCREMENT_OFFSET = 1;
let $MASTER_1_AUTO_INCREMENT_OFFSET4=
  SET SESSION AUTO_INCREMENT_OFFSET = 777;
