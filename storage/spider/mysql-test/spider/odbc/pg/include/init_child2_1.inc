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
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "ta_r"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "ta_r" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT TO_DATE(\'1999-10-10 10:10:10\', \'YYYY-MM-DD HH24:MI:SS\'),
    CONSTRAINT "pk_s_2_1_ta_r" PRIMARY KEY("a")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX "idx1" ON "ta_r"("b")', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "ta_r" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "ta_r2"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE "ta_r2" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT TO_DATE(\'1999-10-10 10:10:10\', \'YYYY-MM-DD HH24:MI:SS\'),
    CONSTRAINT "pk_s_2_1_ta_r2" PRIMARY KEY("a")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TABLES2=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "ta_r2" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TABLES3=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "ta_r_no_idx"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TABLES3=
  SELECT spider_direct_sql('CREATE TABLE "ta_r_no_idx" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT TO_DATE(\'1999-10-10 10:10:10\', \'YYYY-MM-DD HH24:MI:SS\')
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TABLES3=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "ta_r_no_idx" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TABLES4=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "ta_r_auto_inc"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TABLES4=
  SELECT spider_direct_sql('CREATE TABLE "ta_r_auto_inc" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT TO_DATE(\'1999-10-10 10:10:10\', \'YYYY-MM-DD HH24:MI:SS\'),
    CONSTRAINT "pk_s_2_1_ta_r_auto_inc" PRIMARY KEY("a")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TABLES4=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "ta_r_auto_inc"
  ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TABLES5=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "s_2_1_ta_r_int"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TABLES5=
  SELECT spider_direct_sql('CREATE TABLE "s_2_1_ta_r_int" (
    "a" INT DEFAULT 3,
    "b" INT DEFAULT 10,
    "c" INT DEFAULT 11,
    CONSTRAINT "pk_s_2_1_ta_r_int" PRIMARY KEY("a")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX "idx1_s_2_1_ta_r_int" ON "s_2_1_ta_r_int"("b")', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX "idx2_s_2_1_ta_r_int" ON "s_2_1_ta_r_int"("c")', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TABLES5=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "s_2_1_ta_r_int" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TABLES6=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "ta_r_3"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TABLES6=
  SELECT spider_direct_sql('CREATE TABLE "ta_r_3" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT TO_DATE(\'1999-10-10 10:10:10\', \'YYYY-MM-DD HH24:MI:SS\')
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TABLES6=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "ta_r_3" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_FT_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "ft_r"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_FT_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "ft_r" (
    "a" INT DEFAULT 0,
    "b" TEXT,
    "c" TEXT,
    "d" TEXT,
    CONSTRAINT "pk_s_2_1_ft_r" PRIMARY KEY("a"),
    FULLTEXT INDEX "ft_idx1"("b"),
    FULLTEXT INDEX "ft_idx2"("c")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_FT_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c", "d" FROM "ft_r" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_FT_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "ft_r2"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_FT_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE "ft_r2" (
    "a" INT DEFAULT 0,
    "b" TEXT,
    "c" TEXT,
    "d" TEXT,
    CONSTRAINT "pk_s_2_1_ft_r2" PRIMARY KEY("a"),
    FULLTEXT INDEX "ft_idx1"("b"),
    FULLTEXT INDEX "ft_idx2"("c")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_FT_TABLES2=
  SELECT spider_direct_sql('SELECT "a", "b", "c", "d" FROM "ft_r2" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_GM_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "gm_r"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_GM_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "gm_r" (
    "a" INT DEFAULT 0,
    "b" GEOMETRY NOT NULL,
    "c" GEOMETRY NOT NULL,
    CONSTRAINT "pk_s_2_1_gm_r" PRIMARY KEY("a"),
    SPATIAL INDEX "sp_idx1"("b"),
    SPATIAL INDEX "sp_idx2"("c")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_GM_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "gm_r" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_GM_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "gm_r2"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_GM_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE "gm_r2" (
    "a" INT DEFAULT 0,
    "b" GEOMETRY NOT NULL,
    "c" GEOMETRY NOT NULL,
    CONSTRAINT "pk_s_2_1_gm_r2" PRIMARY KEY("a"),
    SPATIAL INDEX "sp_idx1"("b"),
    SPATIAL INDEX "sp_idx2"("c")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_GM_TABLES2=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "gm_r2" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_LOCK_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "t1_1"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_LOCK_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE "t1_1" (
    "id" INT NOT NULL,
    CONSTRAINT "pk_s_2_1_t1_1" PRIMARY KEY ("id")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_LOCK_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "t2_2"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_LOCK_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE "t2_2" (
    "id" INT NOT NULL,
    CONSTRAINT "pk_s_2_1_t2_2" PRIMARY KEY ("id")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_INCREMENT_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "t1_1"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_INCREMENT_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE "t1_1" (
    "id" INT NOT NULL,
    CONSTRAINT "pk_s_2_1_t1_1" PRIMARY KEY ("id")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_INCREMENT_TABLES1=
  SELECT spider_direct_sql('SELECT "id" FROM "t1_1" ORDER BY "id"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TEXT_PK_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "t1"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TEXT_PK_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE "t1" (
    "a" VARCHAR(255),
    CONSTRAINT "pk_s_2_1_t1" PRIMARY KEY ("a")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TEXT_PK_TABLES1=
  SELECT spider_direct_sql('SELECT "a" FROM "t1" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_DROP_TEXT_KEY_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "t1"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_CREATE_TEXT_KEY_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE "t1" (
    "a" VARCHAR(255),
    "b" VARCHAR(255),
    "c" VARCHAR(255),
    CONSTRAINT "pk_s_2_1_t1" PRIMARY KEY ("c")
  )', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX "idx1_t1" ON "t1"("a","b")', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX "idx2_t1" ON "t1"("b")', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_SELECT_TEXT_KEY_TABLES1=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "t1" ORDER BY "c"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_AUTO_INCREMENT_INCREMENT1=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "seq_t1_1"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "seq_t1_1" START WITH 1 INCREMENT BY 1', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_AUTO_INCREMENT_INCREMENT2=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "seq_t1_1"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "seq_t1_1" START WITH 2 INCREMENT BY 4', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_AUTO_INCREMENT_OFFSET1=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "seq_t1_1"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "seq_t1_1" START WITH 1 INCREMENT BY 1', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
let $CHILD2_1_AUTO_INCREMENT_OFFSET2=
  SELECT spider_direct_sql('DROP SEQUENCE IF EXISTS "seq_t1_1"', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE SEQUENCE "seq_t1_1" START WITH 2 INCREMENT BY 4', '', 'dsn "$ODBC_DSN_HA1", wrapper "odbc"');
