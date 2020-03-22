let $CHILD2_1_DATEFORMAT=
  SELECT 1;
let $CHILD2_1_DROP_DATABASE=
  SELECT spider_direct_sql('DROP DATABASE auto_test_remote', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_DATABASE_IFEXISTS=
  SELECT spider_direct_sql('DROP DATABASE IF EXISTS auto_test_remote', '', 'srv "s_2_1_test", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_DATABASE=
  SELECT spider_direct_sql('CREATE DATABASE auto_test_remote', '', 'srv "s_2_1_test", default_group "MariaDB ODBC 3.0 Driver"') $STR_SEMICOLON
  SELECT spider_direct_sql('USE auto_test_remote', '', 'srv "s_2_1_test", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_SELECT1=
  SELECT spider_direct_sql('SELECT 1', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `ta_r`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_TABLES=
  SELECT spider_direct_sql('CREATE TABLE `ta_r` (
    `a` INT DEFAULT 10,
    `b` CHAR(1) DEFAULT \'c\',
    `c` DATE DEFAULT \'1999-10-10\',
    PRIMARY KEY(`a`)
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX `idx1` ON `ta_r`(`b`)', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_SELECT_TABLES=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c` FROM `ta_r` ORDER BY `a`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `ta_r2`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE `ta_r2` (
    `a` INT DEFAULT 10,
    `b` CHAR(1) DEFAULT \'c\',
    `c` DATE DEFAULT \'1999-10-10\',
    PRIMARY KEY(`a`)
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_SELECT_TABLES2=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c` FROM `ta_r2` ORDER BY `a`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_TABLES3=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `ta_r_no_idx`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_TABLES3=
  SELECT spider_direct_sql('CREATE TABLE `ta_r_no_idx` (
    `a` INT DEFAULT 10,
    `b` CHAR(1) DEFAULT \'c\',
    `c` DATE DEFAULT \'1999-10-10\'
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_SELECT_TABLES3=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c` FROM `ta_r_no_idx` ORDER BY `a`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_TABLES4=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `ta_r_auto_inc`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_TABLES4=
  SELECT spider_direct_sql('CREATE TABLE `ta_r_auto_inc` (
    `a` INT AUTO_INCREMENT,
    `b` CHAR(1) DEFAULT \'c\',
    `c` DATE DEFAULT \'1999-10-10\',
    PRIMARY KEY(`a`)
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_SELECT_TABLES4=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c` FROM `ta_r_auto_inc`
  ORDER BY `a`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_TABLES5=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `s_2_1_ta_r_int`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_TABLES5=
  SELECT spider_direct_sql('CREATE TABLE `s_2_1_ta_r_int` (
    `a` INT AUTO_INCREMENT,
    `b` INT DEFAULT 10,
    `c` INT DEFAULT 11,
    PRIMARY KEY(`a`)
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX `idx1_s_2_1_ta_r_int` ON `s_2_1_ta_r_int`(`b`)', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX `idx2_s_2_1_ta_r_int` ON `s_2_1_ta_r_int`(`c`)', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_SELECT_TABLES5=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c` FROM `s_2_1_ta_r_int` ORDER BY `a`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_TABLES6=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `ta_r_3`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_TABLES6=
  SELECT spider_direct_sql('CREATE TABLE `ta_r_3` (
    `a` INT DEFAULT 10,
    `b` CHAR(1) DEFAULT \'c\',
    `c` DATE DEFAULT \'1999-10-10\'
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_SELECT_TABLES6=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c` FROM `ta_r_3` ORDER BY `a`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_FT_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `ft_r`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_FT_TABLES=
  SELECT spider_direct_sql('CREATE TABLE `ft_r` (
    `a` INT DEFAULT 0,
    `b` TEXT,
    `c` TEXT,
    "d" TEXT,
    PRIMARY KEY(`a`),
    FULLTEXT INDEX "ft_idx1"(`b`),
    FULLTEXT INDEX "ft_idx2"(`c`)
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_SELECT_FT_TABLES=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c`, `d` FROM `ft_r` ORDER BY `a`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_FT_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `ft_r2`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_FT_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE `ft_r2` (
    `a` INT DEFAULT 0,
    `b` TEXT,
    `c` TEXT,
    `d` TEXT,
    PRIMARY KEY(`a`),
    FULLTEXT INDEX "ft_idx1"(`b`),
    FULLTEXT INDEX "ft_idx2"(`c`)
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_SELECT_FT_TABLES2=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c`, `d` FROM `ft_r2` ORDER BY `a`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_GM_TABLES=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `gm_r`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_GM_TABLES=
  SELECT spider_direct_sql('CREATE TABLE `gm_r` (
    `a` INT DEFAULT 0,
    `b` GEOMETRY NOT NULL,
    `c` GEOMETRY NOT NULL,
    PRIMARY KEY(`a`),
    SPATIAL INDEX "sp_idx1"(`b`),
    SPATIAL INDEX "sp_idx2"(`c`)
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_SELECT_GM_TABLES=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c` FROM `gm_r` ORDER BY `a`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_GM_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `gm_r2`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_GM_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE `gm_r2` (
    `a` INT DEFAULT 0,
    `b` GEOMETRY NOT NULL,
    `c` GEOMETRY NOT NULL,
    PRIMARY KEY(`a`),
    SPATIAL INDEX "sp_idx1"(`b`),
    SPATIAL INDEX "sp_idx2"(`c`)
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_SELECT_GM_TABLES2=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c` FROM `gm_r2` ORDER BY `a`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_LOCK_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `t1_1`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_LOCK_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE `t1_1` (
    `id` INT NOT NULL,
    PRIMARY KEY (`id`)
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_LOCK_TABLES2=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `t2_2`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_LOCK_TABLES2=
  SELECT spider_direct_sql('CREATE TABLE `t2_2` (
    `id` INT NOT NULL,
    PRIMARY KEY (`id`)
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_INCREMENT_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `t1_1`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_INCREMENT_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE `t1_1` (
    `id` INT NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (`id`)
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_SELECT_INCREMENT_TABLES1=
  SELECT spider_direct_sql('SELECT `id` FROM `t1_1` ORDER BY `id`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_TEXT_PK_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `t1`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_TEXT_PK_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE `t1` (
    `a` VARCHAR(255),
    PRIMARY KEY (`a`)
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_SELECT_TEXT_PK_TABLES1=
  SELECT spider_direct_sql('SELECT `a` FROM `t1` ORDER BY `a`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_DROP_TEXT_KEY_TABLES1=
  SELECT spider_direct_sql('DROP TABLE IF EXISTS `t1`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_CREATE_TEXT_KEY_TABLES1=
  SELECT spider_direct_sql('CREATE TABLE `t1` (
    `a` VARCHAR(255),
    `b` VARCHAR(255),
    `c` VARCHAR(255),
    PRIMARY KEY (`c`)
  )', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX `idx1_t1` ON `t1`(`a`,`b`)', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"')
  $STR_SEMICOLON
  SELECT spider_direct_sql('CREATE INDEX `idx2_t1` ON `t1`(`b`)', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_SELECT_TEXT_KEY_TABLES1=
  SELECT spider_direct_sql('SELECT `a`, `b`, `c` FROM `t1` ORDER BY `c`', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_AUTO_INCREMENT_INCREMENT1=
  SELECT spider_direct_sql('SET GLOBAL AUTO_INCREMENT_INCREMENT = 1', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_AUTO_INCREMENT_INCREMENT2=
  SELECT spider_direct_sql('SET GLOBAL AUTO_INCREMENT_INCREMENT = 4', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_AUTO_INCREMENT_OFFSET1=
  SELECT spider_direct_sql('SET GLOBAL AUTO_INCREMENT_OFFSET = 1', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
let $CHILD2_1_AUTO_INCREMENT_OFFSET2=
  SELECT spider_direct_sql('SET GLOBAL AUTO_INCREMENT_OFFSET = 2', '', 'srv "s_2_1", default_group "MariaDB ODBC 3.0 Driver"');
