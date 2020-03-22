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
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "ta_r3"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_CREATE_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "ta_r3" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT TO_DATE(\'1999-10-10 10:10:10\', \'YYYY-MM-DD HH24:MI:SS\'),
    CONSTRAINT "pk_s_2_2_ta_r3" PRIMARY KEY("a")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_DROP_TABLES5=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "s_2_2_ta_r_int"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_CREATE_TABLES5=
  SELECT spider_direct_sql('CREATE TABLE "s_2_2_ta_r_int" (
    "a" INT DEFAULT 3,
    "b" INT DEFAULT 10,
    "c" INT DEFAULT 11,
    CONSTRAINT "pk_s_2_2_ta_r_int" PRIMARY KEY("a")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX "idx1_s_2_2_ta_r_int" ON "s_2_2_ta_r_int"("b")', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX "idx2_s_2_2_ta_r_int" ON "s_2_2_ta_r_int"("c")', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_SELECT_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "ta_r3" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_DROP_FT_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "ft_r3"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_CREATE_FT_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "ft_r3" (
    "a" INT DEFAULT 0,
    "b" TEXT,
    "c" TEXT,
    "d" TEXT,
    CONSTRAINT "pk_s_2_2_ft_r3" PRIMARY KEY("a"),
    FULLTEXT INDEX "ft_idx1"("b"),
    FULLTEXT INDEX "ft_idx2"("c")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_SELECT_FT_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c", "d" FROM "ft_r3" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_DROP_GM_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "gm_r3"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_CREATE_GM_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "gm_r3" (
    "a" INT DEFAULT 0,
    "b" GEOMETRY NOT NULL,
    "c" GEOMETRY NOT NULL,
    CONSTRAINT "pk_s_2_2_gm_r3" PRIMARY KEY("a"),
    SPATIAL INDEX "sp_idx1"("b"),
    SPATIAL INDEX "sp_idx2"("c")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_SELECT_GM_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "gm_r3" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_DROP_LOCK_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "t1_2"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_CREATE_LOCK_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE "t1_2" (
    "id" INT NOT NULL,
    CONSTRAINT "pk_s_2_2_t1_2" PRIMARY KEY ("id")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_DROP_LOCK_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "t2_1"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_CREATE_LOCK_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE "t2_1" (
    "id" INT NOT NULL,
    CONSTRAINT "pk_s_2_2_t2_1" PRIMARY KEY ("id")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_DROP_INCREMENT_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "t1_2"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_CREATE_INCREMENT_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE "t1_2" (
    "id" INT NOT NULL,
    CONSTRAINT "pk_s_2_2_t1_2" PRIMARY KEY ("id")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_SELECT_INCREMENT_TABLES1=
  SELECT spider_direct_sql('SELECT "id" FROM "t1_2" ORDER BY "id"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_AUTO_INCREMENT_INCREMENT1=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "seq_t1_2"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "seq_t1_2" START WITH 1 INCREMENT BY 1', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_AUTO_INCREMENT_INCREMENT2=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "seq_t1_2"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "seq_t1_2" START WITH 3 INCREMENT BY 4', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_AUTO_INCREMENT_OFFSET1=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "seq_t1_2"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "seq_t1_2" START WITH 1 INCREMENT BY 1', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_AUTO_INCREMENT_OFFSET2=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "seq_t1_2"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "seq_t1_2" START WITH 3 INCREMENT BY 4', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
