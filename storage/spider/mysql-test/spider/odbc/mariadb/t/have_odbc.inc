--disable_abort_on_error
CREATE TABLE ta_l (
  a INT,
  b CHAR(1),
  c DATETIME,
  PRIMARY KEY(a)
) ENGINE=Spider DEFAULT CHARSET=utf8 COMMENT='database "test", port "$CHILD2_1_MYPORT", host "localhost", user "root", password "", default_group "MariaDB ODBC 3.0 Driver", table "ta_r", wrapper "odbc_mariadb"';
if ($mysql_errno)
{
  --skip Test requires to build with ODBC driver
}
--enable_abort_on_error
DROP TABLE ta_l;
