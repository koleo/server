let $CHILD2_3_DATEFORMAT=
  SELECT spider_direct_sql('set datestyle=\'ISO, YMD\'', '', 'dsn "$ODBC_DSN_HA3", wrapper "odbc"');
let $CHILD2_3_DROP_DATABASE=
  SELECT 1;
let $CHILD2_3_DROP_DATABASE_IFEXISTS=
  SELECT 1;
let $CHILD2_3_CREATE_DATABASE=
  SELECT 1;
let $CHILD2_3_SELECT1=
  SELECT spider_direct_sql('SELECT 1', '', 'dsn "$ODBC_DSN_HA3", wrapper "odbc"');
let $CHILD2_3_DROP_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "ta_r4"', '', 'dsn "$ODBC_DSN_HA3", wrapper "odbc"');
let $CHILD2_3_CREATE_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "ta_r4" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT TO_DATE(\'1999-10-10 10:10:10\', \'YYYY-MM-DD HH24:MI:SS\'),
    CONSTRAINT "pk_s_2_3_ta_r4" PRIMARY KEY("a")
  )', '', 'dsn "$ODBC_DSN_HA3", wrapper "odbc"');
let $CHILD2_3_SELECT_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c" FROM "ta_r4" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA3", wrapper "odbc"');
