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

### Limitations
---
-   XA transaction is not supported through ODBC connections.
-   Fulltext search is not supported through ODBC connections yet.
-   Pushdown index hint is not supported through ODBC connections yet.
-   Pushdown join is not supported through ODBC connections yet.
-   Index name is not seen when a duplicate entry is detected through an ODBC connection.
-   Table discovery is not supported through ODBC connections yet.

