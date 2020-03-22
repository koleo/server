--disable_abort_on_error
CREATE TABLE ta_l (
  a INT,
  b CHAR(1),
  c DATETIME,
  PRIMARY KEY(a)
) ENGINE=Spider DEFAULT CHARSET=utf8 COMMENT='dsn "$ODBC_DSN_HA1", table "ta_r", wrapper "odbc", pk_name "pk_s_2_1_ta_r"';
if ($mysql_errno)
{
  --skip Test requires to build with ODBC driver
}
--enable_abort_on_error
DROP TABLE ta_l;
