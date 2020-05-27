let $CHILD2_2_DATEFORMAT=
  SELECT spider_direct_sql('set datestyle=\'ISO, YMD\'', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_DROP_DATABASE=
  SELECT 1;
let $CHILD2_2_DROP_DATABASE_IFEXISTS=
  SELECT 1;
let $CHILD2_2_CREATE_DATABASE=
  SELECT 1;
let $CHILD2_2_SELECT1=
  SELECT spider_direct_sql('SELECT 1', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_DROP_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_2_TA_R3"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_CREATE_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_2_TA_R3" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT TO_DATE(\'1999-10-10 10:10:10\', \'YYYY-MM-DD HH24:MI:SS\'),
    CONSTRAINT "$S_2_2_TA_R3_PK" PRIMARY KEY("a")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_DROP_TABLES5=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_2_TA_R_INT"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_CREATE_TABLES5=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_2_TA_R_INT" (
    "a" INT DEFAULT 3,
    "b" INT DEFAULT 10,
    "c" INT DEFAULT 11,
    CONSTRAINT "$S_2_2_TA_R_INT_PK" PRIMARY KEY("a")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX "$S_2_2_TA_R_INT_IDX1" ON "$S_2_2_TA_R_INT"("b")', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX "$S_2_2_TA_R_INT_IDX2" ON "$S_2_2_TA_R_INT"("c")', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_SELECT_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "$S_2_2_TA_R3" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_DROP_FT_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_2_FT_R3"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_CREATE_FT_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_2_FT_R3" (
    "a" INT DEFAULT 0,
    "b" TEXT,
    "c" TEXT,
    "d" TEXT,
    CONSTRAINT "$S_2_2_FT_R3_PK" PRIMARY KEY("a"),
    FULLTEXT INDEX "$S_2_2_FT_R3_FT_IDX1"("b"),
    FULLTEXT INDEX "$S_2_2_FT_R3_FT_IDX2"("c")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_SELECT_FT_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c", "d" FROM "$S_2_2_FT_R3" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_DROP_GM_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_2_GM_R3"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_CREATE_GM_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_2_GM_R3" (
    "a" INT DEFAULT 0,
    "b" GEOMETRY NOT NULL,
    "c" GEOMETRY NOT NULL,
    CONSTRAINT "$S_2_2_GM_R3_PK" PRIMARY KEY("a"),
    SPATIAL INDEX "$S_2_2_GM_R3_GM_IDX1"("b"),
    SPATIAL INDEX "$S_2_2_GM_R3_GM_IDX2"("c")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_SELECT_GM_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "$S_2_2_GM_R3" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_DROP_LOCK_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_2_T1_2"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_CREATE_LOCK_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_2_T1_2" (
    "id" INT NOT NULL,
    CONSTRAINT "$S_2_2_T1_2_PK" PRIMARY KEY ("id")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_DROP_LOCK_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_2_T2_1"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_CREATE_LOCK_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_2_T2_1" (
    "id" INT NOT NULL,
    CONSTRAINT "$S_2_2_T2_1_PK" PRIMARY KEY ("id")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_DROP_INCREMENT_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_2_T1_2"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_CREATE_INCREMENT_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_2_T1_2" (
    "id" INT NOT NULL,
    CONSTRAINT "$S_2_2_T1_2_PK" PRIMARY KEY ("id")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_SELECT_INCREMENT_TABLES1=
  SELECT spider_direct_sql('SELECT "id" FROM "$S_2_2_T1_2" ORDER BY "id"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_AUTO_INCREMENT_INCREMENT1=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_2_T1_2_SEQ"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_2_T1_2_SEQ" START WITH 1 INCREMENT BY 1', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_AUTO_INCREMENT_INCREMENT2=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_2_T1_2_SEQ"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_2_T1_2_SEQ" START WITH 3 INCREMENT BY 4', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_AUTO_INCREMENT_OFFSET1=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_2_T1_2_SEQ"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_2_T1_2_SEQ" START WITH 1 INCREMENT BY 1', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_AUTO_INCREMENT_OFFSET2=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_2_T1_2_SEQ"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_2_T1_2_SEQ" START WITH 3 INCREMENT BY 4', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
