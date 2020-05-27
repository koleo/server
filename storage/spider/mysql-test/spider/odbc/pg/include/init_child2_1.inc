let $CHILD2_1_DATEFORMAT=
  SELECT spider_direct_sql('set datestyle=\'ISO, YMD\'', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_DATABASE=
  SELECT 1;
let $CHILD2_1_DROP_DATABASE_IFEXISTS=
  SELECT 1;
let $CHILD2_1_CREATE_DATABASE=
  SELECT 1;
let $CHILD2_1_SELECT1=
  SELECT spider_direct_sql('SELECT 1', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_TA_R"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_TA_R" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT TO_DATE(\'1999-10-10 10:10:10\', \'YYYY-MM-DD HH24:MI:SS\'),
    CONSTRAINT "$S_2_1_TA_R_PK" PRIMARY KEY("a")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX "$S_2_1_TA_R_IDX1" ON "$S_2_1_TA_R"("b")', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "$S_2_1_TA_R" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_TA_R2"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_TA_R2" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT TO_DATE(\'1999-10-10 10:10:10\', \'YYYY-MM-DD HH24:MI:SS\'),
    CONSTRAINT "$S_2_1_TA_R2_PK" PRIMARY KEY("a")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TABLES2=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "$S_2_1_TA_R2" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TABLES3=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_TA_R_NO_IDX"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TABLES3=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_TA_R_NO_IDX" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT TO_DATE(\'1999-10-10 10:10:10\', \'YYYY-MM-DD HH24:MI:SS\')
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TABLES3=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "$S_2_1_TA_R_NO_IDX" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TABLES4=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_TA_R_AUTO_INC"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TABLES4=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_TA_R_AUTO_INC" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT TO_DATE(\'1999-10-10 10:10:10\', \'YYYY-MM-DD HH24:MI:SS\'),
    CONSTRAINT "$S_2_1_TA_R_AUTO_INC_PK" PRIMARY KEY("a")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TABLES4=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "$S_2_1_TA_R_AUTO_INC"
  ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TABLES5=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_TA_R_INT"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TABLES5=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_TA_R_INT" (
    "a" INT DEFAULT 3,
    "b" INT DEFAULT 10,
    "c" INT DEFAULT 11,
    CONSTRAINT "$S_2_1_TA_R_INT_PK" PRIMARY KEY("a")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX "$S_2_1_TA_R_INT_IDX1" ON "$S_2_1_TA_R_INT"("b")', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX "$S_2_1_TA_R_INT_IDX2" ON "$S_2_1_TA_R_INT"("c")', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TABLES5=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "$S_2_1_TA_R_INT" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TABLES6=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_TA_R3"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TABLES6=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_TA_R3" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT TO_DATE(\'1999-10-10 10:10:10\', \'YYYY-MM-DD HH24:MI:SS\')
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TABLES6=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "$S_2_1_TA_R3" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_FT_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_FT_R"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_FT_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_FT_R" (
    "a" INT DEFAULT 0,
    "b" TEXT,
    "c" TEXT,
    "d" TEXT,
    CONSTRAINT "$S_2_1_FT_R_PK" PRIMARY KEY("a"),
    FULLTEXT INDEX "$S_2_1_FT_R_FT_IDX1"("b"),
    FULLTEXT INDEX "$S_2_1_FT_R_FT_IDX2"("c")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_FT_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c", "d" FROM "$S_2_1_FT_R" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_FT_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_FT_R2"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_FT_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_FT_R2" (
    "a" INT DEFAULT 0,
    "b" TEXT,
    "c" TEXT,
    "d" TEXT,
    CONSTRAINT "$S_2_1_FT_R2_PK" PRIMARY KEY("a"),
    FULLTEXT INDEX "$S_2_1_FT_R2_FT_IDX1"("b"),
    FULLTEXT INDEX "$S_2_1_FT_R2_FT_IDX2"("c")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_FT_TABLES2=
  SELECT spider_direct_sql('SELECT "a", "b", "c", "d" FROM "$S_2_1_FT_R2" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_GM_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_GM_R"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_GM_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_GM_R" (
    "a" INT DEFAULT 0,
    "b" GEOMETRY NOT NULL,
    "c" GEOMETRY NOT NULL,
    CONSTRAINT "$S_2_1_GM_R_PK" PRIMARY KEY("a"),
    SPATIAL INDEX "$S_2_1_GM_R_GM_IDX1"("b"),
    SPATIAL INDEX "$S_2_1_GM_R_GM_IDX1"("c")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_GM_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "$S_2_1_GM_R" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_GM_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_GM_R2"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_GM_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_GM_R2" (
    "a" INT DEFAULT 0,
    "b" GEOMETRY NOT NULL,
    "c" GEOMETRY NOT NULL,
    CONSTRAINT "$S_2_1_GM_R2_PK" PRIMARY KEY("a"),
    SPATIAL INDEX "$S_2_1_GM_R2_GM_IDX1"("b"),
    SPATIAL INDEX "$S_2_1_GM_R2_GM_IDX2"("c")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_GM_TABLES2=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "$S_2_1_GM_R2" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_LOCK_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_T1_1"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_LOCK_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_T1_1" (
    "id" INT NOT NULL,
    CONSTRAINT "$S_2_1_T1_1_PK" PRIMARY KEY ("id")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_LOCK_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_T2_2"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_LOCK_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_T2_2" (
    "id" INT NOT NULL,
    CONSTRAINT "$S_2_1_T2_2_PK" PRIMARY KEY ("id")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_INCREMENT_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_T1_1"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_INCREMENT_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_T1_1" (
    "id" INT NOT NULL,
    CONSTRAINT "$S_2_1_T1_1_PK" PRIMARY KEY ("id")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_INCREMENT_TABLES1=
  SELECT spider_direct_sql('SELECT "id" FROM "$S_2_1_T1_1" ORDER BY "id"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TEXT_PK_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_T1"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TEXT_PK_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_T1" (
    "a" VARCHAR(255),
    CONSTRAINT "$S_2_1_T1_PK" PRIMARY KEY ("a")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TEXT_PK_TABLES1=
  SELECT spider_direct_sql('SELECT "a" FROM "$S_2_1_T1" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TEXT_KEY_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_1_T1"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TEXT_KEY_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_1_T1" (
    "a" VARCHAR(255),
    "b" VARCHAR(255),
    "c" VARCHAR(255),
    CONSTRAINT "$S_2_1_T1_PK" PRIMARY KEY ("c")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX "$S_2_1_T1_IDX1" ON "$S_2_1_T1"("a","b")', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX "$S_2_1_T1_IDX2" ON "$S_2_1_T1"("b")', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TEXT_KEY_TABLES1=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "$S_2_1_T1" ORDER BY "c"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_AUTO_INCREMENT_INCREMENT1=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_1_T1_1_SEQ"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_1_T1_1_SEQ" START WITH 1 INCREMENT BY 1', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_AUTO_INCREMENT_INCREMENT2=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_1_T1_1_SEQ"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_1_T1_1_SEQ" START WITH 2 INCREMENT BY 4', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_AUTO_INCREMENT_OFFSET1=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_1_T1_1_SEQ"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_1_T1_1_SEQ" START WITH 1 INCREMENT BY 1', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_AUTO_INCREMENT_OFFSET2=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "$S_2_1_T1_1_SEQ"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "$S_2_1_T1_1_SEQ" START WITH 2 INCREMENT BY 4', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
