let $CHILD2_2_DATEFORMAT=
  SELECT 1;
let $CHILD2_2_DROP_DATABASE=
  SELECT spider_direct_sql('DROP DATABASE auto_test_remote2', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_DROP_DATABASE_IFEXISTS=
  SELECT spider_direct_sql('DROP DATABASE IF EXISTS auto_test_remote2', '', 'srv "s_2_2_test", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_CREATE_DATABASE=
  SELECT spider_direct_sql('CREATE DATABASE auto_test_remote2', '', 'srv "s_2_2_test", default_group "MariaDB ODBC 3.0 Driver"') $STR_SEMICOLON
  SELECT spider_direct_sql('USE auto_test_remote2', '', 'srv "s_2_2_test", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_SELECT1=
  SELECT spider_direct_sql('SELECT 1', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_DROP_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `ta_r3`', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_CREATE_TABLES=
  SELECT spider_direct_sql('CREATE TABLE `ta_r3` (
    `a` INT DEFAULT 10,
    `b` CHAR(1) DEFAULT \'c\',
    `c` DATE DEFAULT \'1999-10-10\',
    PRIMARY KEY(`a`)
  )', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_DROP_TABLES5=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `s_2_2_ta_r_int`', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_CREATE_TABLES5=
  SELECT spider_direct_sql('CREATE TABLE `s_2_2_ta_r_int` (
    `a` INT AUTO_INCREMENT,
    `b` INT DEFAULT 10,
    `c` INT DEFAULT 11,
    PRIMARY KEY(`a`)
  )', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX `idx1_s_2_2_ta_r_int` ON `s_2_2_ta_r_int`(`b`)', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX `idx2_s_2_2_ta_r_int` ON `s_2_2_ta_r_int`(`c`)', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_SELECT_TABLES=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c` FROM `ta_r3` ORDER BY `a`', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_DROP_FT_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `ft_r3`', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_CREATE_FT_TABLES=
  SELECT spider_direct_sql('CREATE TABLE `ft_r3` (
    `a` INT DEFAULT 0,
    `b` TEXT,
    `c` TEXT,
    `d` TEXT,
    PRIMARY KEY(`a`),
    FULLTEXT INDEX "ft_idx1"(`b`),
    FULLTEXT INDEX "ft_idx2"(`c`)
  )', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_SELECT_FT_TABLES=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c`, `d` FROM `ft_r3` ORDER BY `a`', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_DROP_GM_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `gm_r3`', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_CREATE_GM_TABLES=
  SELECT spider_direct_sql('CREATE TABLE `gm_r3` (
    `a` INT DEFAULT 0,
    `b` GEOMETRY NOT NULL,
    `c` GEOMETRY NOT NULL,
    PRIMARY KEY(`a`),
    SPATIAL INDEX "sp_idx1"(`b`),
    SPATIAL INDEX "sp_idx2"(`c`)
  )', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_SELECT_GM_TABLES=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c` FROM `gm_r3` ORDER BY `a`', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_DROP_LOCK_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `t1_2`', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_CREATE_LOCK_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE `t1_2` (
    `id` INT NOT NULL,
    PRIMARY KEY (`id`)
  )', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_DROP_LOCK_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `t2_1`', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_CREATE_LOCK_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE `t2_1` (
    `id` INT NOT NULL,
    PRIMARY KEY (`id`)
  )', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_DROP_INCREMENT_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `t1_2`', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_CREATE_INCREMENT_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE `t1_2` (
    `id` INT NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (`id`)
  )', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_SELECT_INCREMENT_TABLES1=
  SELECT spider_direct_sql('SELECT `id` FROM `t1_2` ORDER BY `id`', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_AUTO_INCREMENT_INCREMENT1=
  SELECT spider_direct_sql('SET GLOBAL AUTO_INCREMENT_INCREMENT = 1', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_AUTO_INCREMENT_INCREMENT2=
  SELECT spider_direct_sql('SET GLOBAL AUTO_INCREMENT_INCREMENT = 4', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_AUTO_INCREMENT_OFFSET1=
  SELECT spider_direct_sql('SET GLOBAL AUTO_INCREMENT_OFFSET = 1', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_2_AUTO_INCREMENT_OFFSET2=
  SELECT spider_direct_sql('SET GLOBAL AUTO_INCREMENT_OFFSET = 3', '', 'srv "s_2_2", default_group "MariaDB ODBC 3.0 Driver"');
