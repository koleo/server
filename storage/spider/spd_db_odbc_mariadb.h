/* Copyright (C) 2020 Kentoku Shiba
   Copyright (C) 2020 MariaDB corp

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#include "spd_db_odbc.h"
#ifdef HAVE_SPIDER_ODBC

class spider_db_odbc_mariadb_util: public spider_db_odbc_util
{
public:
  spider_db_odbc_mariadb_util(
    const char *nm_quote,
    const char *val_quote,
    uint nm_quote_length,
    uint val_quote_length
  );
  virtual ~spider_db_odbc_mariadb_util();
  virtual int append_lock_table_body(
    spider_string *str,
    const char *db_name,
    uint db_name_length,
    CHARSET_INFO *db_name_charset,
    const char *table_name,
    uint table_name_length,
    CHARSET_INFO *table_name_charset,
    int lock_type
  );
  virtual int append_lock_table_tail(
    spider_string *str
  );
  virtual int append_unlock_table(
    spider_string *str
  );
};

class spider_db_odbc_mariadb_result: public spider_db_odbc_result
{
public:
  char cur_nm[25];
  uint cur_nm_len;
  spider_db_odbc_mariadb_result(
    SPIDER_DB_CONN *in_db_conn
  );
  virtual ~spider_db_odbc_mariadb_result();
  virtual void free_result();
};

class spider_db_odbc_mariadb: public spider_db_odbc
{
public:
  char cur_nm[25];
  uint cur_nm_len;
  spider_db_odbc_mariadb(
    SPIDER_CONN *conn
  );
  virtual ~spider_db_odbc_mariadb();
  virtual int connect(
    char *tgt_host,
    char *tgt_username,
    char *tgt_password,
    long tgt_port,
    char *tgt_socket,
    char *server_name,
    int connect_retry_count,
    longlong connect_retry_interval
  );
  virtual void disconnect();
  virtual int exec_query(
    const char *query,
    uint length,
    int quick_mode
  );
  virtual spider_db_result *store_result(
    spider_db_result_buffer **spider_res_buf,
    st_spider_db_request_key *request_key,
    int *err
  );
  virtual spider_db_result *use_result(
    st_spider_db_request_key *request_key,
    int *err
  );
  virtual int append_lock_tables(
    spider_string *str
  );
};

class spider_odbc_mariadb_handler: public spider_odbc_handler
{
public:
  spider_odbc_mariadb_handler(
    ha_spider *spider,
    spider_odbc_share *share,
    spider_db_odbc_util *db_util
  );
  virtual ~spider_odbc_mariadb_handler();
  virtual int append_update_where(
    spider_string *str,
    const TABLE *table,
    my_ptrdiff_t ptr_diff
  );
  virtual int append_select_lock(
    spider_string *str
  );
  virtual int lock_tables(
    int link_idx
  );
};

class spider_odbc_mariadb_copy_table: public spider_odbc_copy_table
{
public:
  spider_odbc_mariadb_copy_table(
    spider_odbc_share *db_share,
    spider_db_odbc_util *db_util
  );
  virtual ~spider_odbc_mariadb_copy_table();
  virtual int append_select_lock_str(
    int lock_mode
  );
};
#endif
