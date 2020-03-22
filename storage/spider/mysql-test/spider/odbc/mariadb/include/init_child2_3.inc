let $CHILD2_3_DATEFORMAT=
  SELECT 1;
let $CHILD2_3_DROP_DATABASE=
  SELECT spider_direct_sql('DROP DATABASE auto_test_remote3', '', 'srv "s_2_3", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_3_DROP_DATABASE_IFEXISTS=
  SELECT spider_direct_sql('DROP DATABASE IF EXISTS auto_test_remote3', '', 'srv "s_2_3_test", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_3_CREATE_DATABASE=
  SELECT spider_direct_sql('CREATE DATABASE auto_test_remote3', '', 'srv "s_2_3_test", default_group "MariaDB ODBC 3.0 Driver"') $STR_SEMICOLON
  SELECT spider_direct_sql('USE auto_test_remote3', '', 'srv "s_2_3_test", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_3_SELECT1=
  SELECT spider_direct_sql('SELECT 1', '', 'srv "s_2_3", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_3_DROP_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `ta_r4`', '', 'srv "s_2_3", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_3_CREATE_TABLES=
  SELECT spider_direct_sql('CREATE TABLE `ta_r4` (
    `a` INT DEFAULT 10,
    `b` CHAR(1) DEFAULT \'c\',
    `c` DATE DEFAULT \'1999-10-10\',
    PRIMARY KEY(`a`)
  )', '', 'srv "s_2_3", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_3_SELECT_TABLES=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c` FROM `ta_r4` ORDER BY `a`', '', 'srv "s_2_3", default_group "MariaDB ODBC 3.0 Driver"');
