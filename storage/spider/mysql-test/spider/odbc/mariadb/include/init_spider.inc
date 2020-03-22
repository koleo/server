--source ../../../include/init_spider.inc
let $VERSION_COMPILE_OS_WIN=
  `SELECT IF(@@version_compile_os like 'Win%', 1, 0)`;
if ($VERSION_COMPILE_OS_WIN)
{
  eval DROP SERVER IF EXISTS s_2_1_test;
  eval CREATE SERVER s_2_1_test FOREIGN DATA WRAPPER odbc_mariadb OPTIONS (
    HOST '127.0.0.1',
    DATABASE 'test',
    USER 'root',
    PASSWORD '',
    PORT $CHILD2_1_MYPORT
  );
  eval DROP SERVER IF EXISTS s_2_2_test;
  eval CREATE SERVER s_2_2_test FOREIGN DATA WRAPPER odbc_mariadb OPTIONS (
    HOST '127.0.0.1',
    DATABASE 'test',
    USER 'root',
    PASSWORD '',
    PORT $CHILD2_2_MYPORT
  );
  eval DROP SERVER IF EXISTS s_2_3_test;
  eval CREATE SERVER s_2_3_test FOREIGN DATA WRAPPER odbc_mariadb OPTIONS (
    HOST '127.0.0.1',
    DATABASE 'test',
    USER 'root',
    PASSWORD '',
    PORT $CHILD2_3_MYPORT
  );
  eval DROP SERVER IF EXISTS s_2_1;
  eval CREATE SERVER s_2_1 FOREIGN DATA WRAPPER odbc_mariadb OPTIONS (
    HOST '127.0.0.1',
    DATABASE 'auto_test_remote',
    USER 'root',
    PASSWORD '',
    PORT $CHILD2_1_MYPORT
  );
  eval DROP SERVER IF EXISTS s_2_2;
  eval CREATE SERVER s_2_2 FOREIGN DATA WRAPPER odbc_mariadb OPTIONS (
    HOST '127.0.0.1',
    DATABASE 'auto_test_remote2',
    USER 'root',
    PASSWORD '',
    PORT $CHILD2_2_MYPORT
  );
  eval DROP SERVER IF EXISTS s_2_3;
  eval CREATE SERVER s_2_3 FOREIGN DATA WRAPPER odbc_mariadb OPTIONS (
    HOST '127.0.0.1',
    DATABASE 'auto_test_remote3',
    USER 'root',
    PASSWORD '',
    PORT $CHILD2_3_MYPORT
  );
  eval DROP SERVER IF EXISTS s_3_1;
  eval CREATE SERVER s_3_1 FOREIGN DATA WRAPPER mysql OPTIONS (
    HOST 'localhost',
    DATABASE 'auto_test_local',
    USER 'root',
    PASSWORD '',
    PORT $CHILD3_1_MYPORT
  );
  eval DROP SERVER IF EXISTS s_3_2;
  eval CREATE SERVER s_3_2 FOREIGN DATA WRAPPER mysql OPTIONS (
    HOST 'localhost',
    DATABASE 'auto_test_local',
    USER 'root',
    PASSWORD '',
    PORT $CHILD3_2_MYPORT
  );
  eval DROP SERVER IF EXISTS s_3_3;
  eval CREATE SERVER s_3_3 FOREIGN DATA WRAPPER mysql OPTIONS (
    HOST 'localhost',
    DATABASE 'auto_test_local',
    USER 'root',
    PASSWORD '',
    PORT $CHILD3_3_MYPORT
  );
}
if (!$VERSION_COMPILE_OS_WIN)
{
  eval DROP SERVER IF EXISTS s_2_1_test;
  eval CREATE SERVER s_2_1_test FOREIGN DATA WRAPPER odbc_mariadb OPTIONS (
    HOST '127.0.0.1',
    DATABASE 'test',
    USER 'root',
    PASSWORD '',
    PORT $CHILD2_1_MYPORT
  );
  eval DROP SERVER IF EXISTS s_2_2_test;
  eval CREATE SERVER s_2_2_test FOREIGN DATA WRAPPER odbc_mariadb OPTIONS (
    HOST '127.0.0.1',
    DATABASE 'test',
    USER 'root',
    PASSWORD '',
    PORT $CHILD2_2_MYPORT
  );
  eval DROP SERVER IF EXISTS s_2_3_test;
  eval CREATE SERVER s_2_3_test FOREIGN DATA WRAPPER odbc_mariadb OPTIONS (
    HOST '127.0.0.1',
    DATABASE 'test',
    USER 'root',
    PASSWORD '',
    PORT $CHILD2_3_MYPORT
  );
  eval DROP SERVER IF EXISTS s_2_1;
  eval CREATE SERVER s_2_1 FOREIGN DATA WRAPPER odbc_mariadb OPTIONS (
    HOST '127.0.0.1',
    DATABASE 'auto_test_remote',
    USER 'root',
    PASSWORD '',
    PORT $CHILD2_1_MYPORT
  );
  eval DROP SERVER IF EXISTS s_2_2;
  eval CREATE SERVER s_2_2 FOREIGN DATA WRAPPER odbc_mariadb OPTIONS (
    HOST '127.0.0.1',
    DATABASE 'auto_test_remote2',
    USER 'root',
    PASSWORD '',
    PORT $CHILD2_2_MYPORT
  );
  eval DROP SERVER IF EXISTS s_2_3;
  eval CREATE SERVER s_2_3 FOREIGN DATA WRAPPER odbc_mariadb OPTIONS (
    HOST '127.0.0.1',
    DATABASE 'auto_test_remote3',
    USER 'root',
    PASSWORD '',
    PORT $CHILD2_3_MYPORT
  );
  eval DROP SERVER IF EXISTS s_3_1;
  eval CREATE SERVER s_3_1 FOREIGN DATA WRAPPER mysql OPTIONS (
    HOST 'localhost',
    DATABASE 'auto_test_local',
    USER 'root',
    PASSWORD '',
    SOCKET '$CHILD3_1_MYSOCK'
  );
  eval DROP SERVER IF EXISTS s_3_2;
  eval CREATE SERVER s_3_2 FOREIGN DATA WRAPPER mysql OPTIONS (
    HOST 'localhost',
    DATABASE 'auto_test_local',
    USER 'root',
    PASSWORD '',
    SOCKET '$CHILD3_2_MYSOCK'
  );
  eval DROP SERVER IF EXISTS s_3_3;
  eval CREATE SERVER s_3_3 FOREIGN DATA WRAPPER mysql OPTIONS (
    HOST 'localhost',
    DATABASE 'auto_test_local',
    USER 'root',
    PASSWORD '',
    SOCKET '$CHILD3_3_MYSOCK'
  );
}
