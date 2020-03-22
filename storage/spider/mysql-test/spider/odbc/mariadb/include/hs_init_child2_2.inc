let $CHILD2_2_HS_DROP_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS "hs_r3"', '', 'srv "s_2_2"');
let $CHILD2_2_HS_CREATE_TABLES=
  SELECT spider_direct_sql('CREATE TABLE "hs_r3" (
    "a" INT DEFAULT 10,
    "b" CHAR(1) DEFAULT \'c\',
    "c" DATE DEFAULT \'1999-10-10\',
    "d" INT DEFAULT 11,
    PRIMARY KEY("a")
  )', '', 'srv "s_2_2"');
let $CHILD2_2_HS_SELECT_TABLES=
  SELECT spider_direct_sql('SELECT "a", "b", "c", "d" FROM "hs_r3" ORDER BY "a"', '', 'srv "s_2_2"');
