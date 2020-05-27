let $CHILD2_2_HS_DROP_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "$S_2_2_HS_R3"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_HS_CREATE_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "$S_2_2_HS_R3" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT TO_DATE(\'1999-10-10 10:10:10\', \'YYYY-MM-DD HH24:MI:SS\'),
    "d" INT DEFAULT 11,
    CONSTRAINT "$S_2_2_HS_R3_PK" PRIMARY KEY("a")
  )', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
let $CHILD2_2_HS_SELECT_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c", "d" FROM "$S_2_2_HS_R3" ORDER BY "a"', '', 'dsn "$ODBC_DSN_HA2", wrapper "odbc"');
