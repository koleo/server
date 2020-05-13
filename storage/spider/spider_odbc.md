# Spider ODBC feature

### Abstract
---
-   Spider ODBC feature makes Spider possible to access data nodes through ODBC connections.

### Requirement
---
-   This feature is available from MariaDB Enterprise Server 10.5
-   Currently, this feature is only used with UnixODBC.
-   Spider have to be built with -DSPIDER_WITH_UNIXODBC=ON

### How to use
---
For using MariaDB/MySQL as a data node through ODBC, set the following parameter to Spider table system variable or CREATE/ALTER SERVER command.
-   wrapper "odbc_mariadb"

For using other databases as a data node through ODBC, set the following parameter to Spider table system variable or CREATE/ALTER SERVER command.
-   wrapper "odbc"

### An example to use PostgreSQL as a data node
---
On PostgreSQL (data node)
```
CREATE DATABASE test;
\c test
CREATE TABLE "t1" (
  "c1" INT,
  "c2" VARCHAR(255),
  "c3" TIMESTAMP,
  CONSTRAINT "pk_t1" PRIMARY KEY("c1")
);
SET DATESTYLE='ISO, YMD';
INSERT INTO "t1" VALUES (10, 'abCDEf012', '2020-04-22 09:20:11');
```

On MariaDB (Spider node)
```
CREATE DATABASE test;
USE test;
CREATE TABLE `t1` (
  `c1` INT,
  `c2` VARCHAR(255),
  `c3` DATETIME,
  PRIMARY KEY(`c1`)
)ENGINE=SPIDER COMMENT='dsn "YOURDSN", table "t1", wrapper "odbc"';
INSERT INTO `t1` VALUES (15, 'abCDEf012', '2020-04-20 10:10:10');
```

On PostgreSQL (data node)
```
test=# select * from t1;
 c1 |    c2     |         c3
----+-----------+---------------------
 10 | abCDEf012 | 2020-04-22 09:20:11
 15 | abCDEf012 | 2020-04-20 10:10:10
(2 rows)

test=# select avg(c1), max(c3) from t1;
         avg         |         max
---------------------+---------------------
 12.5000000000000000 | 2020-04-22 09:20:11
(1 row)
```

On MariaDB (Spider node)
```
MariaDB [test]> select * from t1;
+----+-----------+---------------------+
| c1 | c2        | c3                  |
+----+-----------+---------------------+
| 10 | abCDEf012 | 2020-04-22 09:20:11 |
| 15 | abCDEf012 | 2020-04-20 10:10:10 |
+----+-----------+---------------------+
2 rows in set (0.70 sec)

MariaDB [test]> select avg(c1), max(c3) from t1;
+---------+---------------------+
| avg(c1) | max(c3)             |
+---------+---------------------+
| 12.5000 | 2020-04-22 09:20:11 |
+---------+---------------------+
1 row in set (0.64 sec)
```

### An example to use ODBC data nodes as shards
---
On MariaDB (Spider node)
```
CREATE TABLE `t1` (
  `c1` INT,
  `c2` VARCHAR(255),
  `c3` DATETIME,
  PRIMARY KEY(`c1`)
)ENGINE=SPIDER COMMENT='table "t1", wrapper "odbc"'
PARTITION BY KEY(c1) (
  PARTITION p1 COMMENT='dsn "YOURDSN1"',
  PARTITION p2 COMMENT='dsn "YOURDSN2"',
  PARTITION p3 COMMENT='dsn "YOURDSN3"',
  ...
);
```

### Parameters for ODBC connections
---
The following parameters are available for ODBC connections.
-   Common
    -   default_group: ODBC driver name
    -   default_file: ODBC directory name (default_group is required for using this parameter)
    -   dsn: ODBC DSN
    -   database: ODBC database
    -   user: ODBC UID
    -   password: ODBC PWD (user is required for using this parameter)
-   odbc_mariadb only
    -   host: ODBC server
    -   port: ODBC port
    -   socket: socket file name
-   odbc only
    -   host: ODBC server (database is required for using this parameter)
    -   port: ODBC port (database is required for using this parameter)

Additionally, ssl_ca, ssl_capath, ssl_cert, ssl_cipher, ssl_key can be used for adding specific parameters of ODBC drivers.
-   odbc_mariadb:
    -   ssl_ca is added on top of ODBC connection parameters.
    -   ssl_capath is added between ODBC DRIVER and DSN.
    -   ssl_cert is added between ODBC PORT and DATABASE.
    -   ssl_cipher is added between ODBC DATABASE and UID.
    -   ssl_key is added on bottom of ODBC connection parameters.
-   odbc:
    -   ssl_ca is added on top of ODBC connection parameters.
    -   ssl_capath is added between ODBC DRIVER and DSN.
    -   ssl_cert is added between ODBC DSN and DATABASE.
    -   ssl_cipher is added between ODBC DATABASE and UID.
    -   ssl_key is added on bottom of ODBC connection parameters.

### Difference between odbc_mariadb and odbc
---
There are some internal differences.
-   odbc_mariadb:
    -   "DATABASE=database;SERVER=host;PORT=port;" in connection string.
    -   The name quote is back quote.
    -   The modes of lock table are "read local", "read", "low_priority write", and "write".
    -   Lock table command causes a commit of transaction.
    -   Lock table command has to do at once.
    -   "UNLOCK TABLES" command can be used for releasing table locks.
    -   The shared lock word with SELECT is "lock in shared mode".
    -   The name of cursors have to change for each thread.
-   odbc:
    -   "DATABASE=database:host:port;" in connection string.
    -   The name quote is double quote.
    -   The modes of lock table are "in share mode", and "in exclusive mode".
    -   Lock table command has to inside of transaction.
    -   Lock table command has to do one by one.
    -   The transaction has to be finished for releasing table locks.
    -   The shared lock word with SELECT is "for share".
    -   The name of cursors can use same name for different threads.

### Limitations
---
-   XA transaction is not supported through ODBC connections.
-   Fulltext search is not supported through ODBC connections yet.
-   Pushdown index hint is not supported through ODBC connections yet.
-   Pushdown join is not supported through ODBC connections yet.
-   Index name is not seen when a duplicate entry is detected through an ODBC connection.
-   Table discovery is not supported through ODBC connections yet.

