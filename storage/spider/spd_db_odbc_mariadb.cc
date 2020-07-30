/* Copyright (C) 2019-2020 Kentoku Shiba
   Copyright (C) 2019-2020 MariaDB corp

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

#define MYSQL_SERVER 1
#include <my_global.h>
#include "mysql_version.h"
#include "spd_environ.h"
#if MYSQL_VERSION_ID < 50500
#include "mysql_priv.h"
#include <mysql/plugin.h>
#else
#include "sql_priv.h"
#include "probes_mysql.h"
#include "sql_class.h"
#include "sql_partition.h"
#include "sql_analyse.h"
#include "sql_base.h"
#include "tztime.h"
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
#include "sql_select.h"
#endif
#endif
#include "sql_common.h"
#include <mysql.h>
#include <errmsg.h>
#include "spd_err.h"
#include "spd_param.h"
#include "spd_db_include.h"
#include "spd_include.h"
#include "spd_db_odbc_mariadb.h"
#include "ha_spider.h"
#include "spd_conn.h"
#include "spd_db_conn.h"
#include "spd_malloc.h"
#include "spd_sys_table.h"
#include "spd_table.h"

#define SPIDER_DB_WRAPPER_ODBC_MARIADB "odbc_mariadb"

#ifdef HAVE_SPIDER_ODBC

#define SPIDER_SQL_CUR_CUR_STR "CURRENT OF "
#define SPIDER_SQL_CUR_CUR_LEN (sizeof(SPIDER_SQL_CUR_CUR_STR) - 1)

#define SPIDER_SQL_UNLOCK_TABLE_STR "unlock tables"
#define SPIDER_SQL_UNLOCK_TABLE_LEN (sizeof(SPIDER_SQL_UNLOCK_TABLE_STR) - 1)

static const char *spider_db_table_lock_str[] =
{
  " read local,",
  " read,",
  " low_priority write,",
  " write,"
};
static const int spider_db_table_lock_len[] =
{
  sizeof(" read local,") - 1,
  sizeof(" read,") - 1,
  sizeof(" low_priority write,") - 1,
  sizeof(" write,") - 1
};

spider_db_odbc_mariadb_util spider_db_odbc_mariadb_utility("`", "'", 1, 1);

spider_db_share *spider_odbc_mariadb_create_share(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_odbc_mariadb_create_share");
  DBUG_RETURN(new spider_odbc_share(share, &spider_db_odbc_mariadb_utility));
}

spider_db_handler *spider_odbc_mariadb_create_handler(
  ha_spider *spider,
  spider_db_share *db_share
) {
  DBUG_ENTER("spider_odbc_mariadb_create_handler");
  DBUG_RETURN(new spider_odbc_mariadb_handler(spider,
    (spider_odbc_share *) db_share, &spider_db_odbc_mariadb_utility));
}

spider_db_copy_table *spider_odbc_mariadb_create_copy_table(
  spider_db_share *db_share
) {
  DBUG_ENTER("spider_odbc_mariadb_create_copy_table");
  DBUG_RETURN(new spider_odbc_mariadb_copy_table(
    (spider_odbc_share *) db_share, &spider_db_odbc_mariadb_utility));
}

SPIDER_DB_CONN *spider_odbc_mariadb_create_conn(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_odbc_mariadb_create_conn");
  DBUG_RETURN(new spider_db_odbc_mariadb(conn));
}

SPIDER_DBTON spider_dbton_odbc_mariadb = {
  0,
  SPIDER_DB_WRAPPER_ODBC_MARIADB,
  SPIDER_DB_ACCESS_TYPE_SQL,
  spider_odbc_init,
  spider_odbc_deinit,
  spider_odbc_mariadb_create_share,
  spider_odbc_mariadb_create_handler,
  spider_odbc_mariadb_create_copy_table,
  spider_odbc_mariadb_create_conn,
#ifdef SPIDER_SQL_TYPE_DDL_SQL
  NULL,
#endif
  spider_odbc_support_direct_join,
  &spider_db_odbc_mariadb_utility,
  "For communicating MariaDB using ODBC protocol, mostly test purpose",
  "1.0.0",
  SPIDER_MATURITY_BETA
};

spider_db_odbc_mariadb_result::spider_db_odbc_mariadb_result(
  SPIDER_DB_CONN *in_db_conn
) : spider_db_odbc_result(in_db_conn)
{
  DBUG_ENTER("spider_db_odbc_mariadb_result::spider_db_odbc_mariadb_result");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

spider_db_odbc_mariadb_result::~spider_db_odbc_mariadb_result()
{
  DBUG_ENTER("spider_db_odbc_mariadb_result::~spider_db_odbc_mariadb_result");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

void spider_db_odbc_mariadb_result::free_result()
{
  SQLRETURN ret;
  DBUG_ENTER("spider_db_odbc_mariadb_result::free_result");
  DBUG_PRINT("info",("spider this=%p", this));
  if (hstm)
  {
    ret = SQLFreeStmt(hstm, SQL_CLOSE);
    if (ret != SQL_SUCCESS)
    {
      stored_error_num = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT,
        hstm, db_conn->conn, stored_error_msg);
    }
    SQLFreeHandle(SQL_HANDLE_STMT, hstm);
    hstm = NULL;
  }
  DBUG_VOID_RETURN;
}

spider_db_odbc_mariadb::spider_db_odbc_mariadb(
  SPIDER_CONN *conn
) : spider_db_odbc(conn, &spider_db_odbc_mariadb_utility)
{
  DBUG_ENTER("spider_db_odbc_mariadb::spider_db_odbc_mariadb");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

spider_db_odbc_mariadb::~spider_db_odbc_mariadb()
{
  DBUG_ENTER("spider_db_odbc_mariadb::~spider_db_odbc_mariadb");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

void spider_db_odbc_mariadb::disconnect()
{
  SQLRETURN ret;
  DBUG_ENTER("spider_db_odbc_mariadb::disconnect");
  DBUG_PRINT("info",("spider this=%p", this));
  if (hstm != SQL_NULL_HSTMT)
  {
    ret = SQLFreeStmt(hstm, SQL_CLOSE);
    if (ret != SQL_SUCCESS)
    {
      stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT,
        hstm, conn, stored_error_msg);
    }
    SQLFreeHandle(SQL_HANDLE_STMT, hstm);
    hstm = SQL_NULL_HSTMT;
  }
  if (hdbc != SQL_NULL_HDBC)
  {
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
    henv = SQL_NULL_HENV;
  }
  DBUG_VOID_RETURN;
}

int spider_db_odbc_mariadb::exec_query(
  const char *query,
  uint length,
  int quick_mode
) {
  SQLRETURN ret;
  DBUG_ENTER("spider_db_odbc_mariadb::exec_query");
  DBUG_PRINT("info",("spider this=%p", this));
  stored_error = 0;
  if (spider_param_general_log())
  {
    const char *tgt_str = conn->tgt_host;
    uint32 tgt_len = conn->tgt_host_length;
    spider_string tmp_query_str;
    tmp_query_str.init_calc_mem(308);
    if (tmp_query_str.reserve(
      length + conn->tgt_wrapper_length +
      tgt_len + (SPIDER_SQL_SPACE_LEN * 2)))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    tmp_query_str.q_append(conn->tgt_wrapper, conn->tgt_wrapper_length);
    tmp_query_str.q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
    tmp_query_str.q_append(tgt_str, tgt_len);
    tmp_query_str.q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
    tmp_query_str.q_append(query, length);
    general_log_write(current_thd, COM_QUERY, tmp_query_str.ptr(),
      tmp_query_str.length());
  }
  if (!spider_param_dry_access())
  {
    if (hstm == SQL_NULL_HSTMT)
    {
      ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstm);
      if (ret != SQL_SUCCESS)
      {
        stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, hdbc,
          conn, stored_error_msg);
        DBUG_RETURN(stored_error);
      }
      my_sprintf(cur_nm, (cur_nm, "cur%p", hstm));
      cur_nm_len = strlen(cur_nm);
      DBUG_PRINT("info",("spider cur_nm:%s", cur_nm));
      ret = SQLSetCursorName(hstm, (SQLCHAR *) cur_nm, SQL_NTS);
      if (ret != SQL_SUCCESS)
      {
        stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT, hstm,
          conn, stored_error_msg);
        SQLFreeHandle(SQL_HANDLE_STMT, hstm);
        hstm = NULL;
        DBUG_RETURN(stored_error);
      }
    }
    ret = SQLExecDirect(hstm, (SQLCHAR *) query, (SQLINTEGER) length);
    if (ret != SQL_SUCCESS && ret != SQL_NO_DATA)
    {
      stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT, hstm,
        conn, stored_error_msg);
      SQLFreeHandle(SQL_HANDLE_STMT, hstm);
      hstm = NULL;
      DBUG_RETURN(stored_error);
    }
  }
  DBUG_RETURN(stored_error);
}

spider_db_result *spider_db_odbc_mariadb::store_result(
  spider_db_result_buffer **spider_res_buf,
  st_spider_db_request_key *request_key,
  int *err
) {
  spider_db_odbc_mariadb_result *result;
  DBUG_ENTER("spider_db_odbc_mariadb::store_result");
  DBUG_PRINT("info",("spider this=%p", this));
  if ((result = new spider_db_odbc_mariadb_result(this)))
  {
    hstm = SQL_NULL_HSTMT;
    memcpy(result->cur_nm, cur_nm, cur_nm_len + 1);
    result->cur_nm_len = cur_nm_len;
    *err = 0;
    if (spider_param_dry_access() ||
      (*err = result->init()))
    {
      delete result;
      result = NULL;
      DBUG_RETURN(NULL);
    }
    result->set_limit(limit);
    result->spider = (ha_spider *) request_key->handler;
  } else {
    *err = HA_ERR_OUT_OF_MEM;
  }
  DBUG_RETURN(result);
}

spider_db_result *spider_db_odbc_mariadb::use_result(
  ha_spider *spider,
  st_spider_db_request_key *request_key,
  int *err
) {
  spider_db_odbc_mariadb_result *result;
  DBUG_ENTER("spider_db_odbc_mariadb::use_result");
  DBUG_PRINT("info",("spider this=%p", this));
  if ((result = new spider_db_odbc_mariadb_result(this)))
  {
    hstm = SQL_NULL_HSTMT;
    memcpy(result->cur_nm, cur_nm, cur_nm_len + 1);
    result->cur_nm_len = cur_nm_len;
    *err = 0;
    if (spider_param_dry_access() ||
      (*err = result->init()))
    {
      delete result;
      result = NULL;
      DBUG_RETURN(NULL);
    }
    result->set_limit(limit);
    result->spider = spider;
  } else {
    *err = HA_ERR_OUT_OF_MEM;
  }
  DBUG_RETURN(result);
}

int spider_db_odbc_mariadb::append_lock_tables(
  spider_string *str
) {
  int error_num;
  ha_spider *tmp_spider;
  int lock_type;
  uint conn_link_idx;
  int tmp_link_idx;
  SPIDER_LINK_FOR_HASH *tmp_link_for_hash;
  const char *db_name;
  uint db_name_length;
  CHARSET_INFO *db_name_charset;
  const char *table_name;
  uint table_name_length;
  CHARSET_INFO *table_name_charset;
  DBUG_ENTER("spider_db_odbc_mariadb::lock_tables");
  DBUG_PRINT("info",("spider this=%p", this));
  if ((error_num = utility->append_lock_table_head(str)))
  {
    DBUG_RETURN(error_num);
  }
  while ((tmp_link_for_hash =
    (SPIDER_LINK_FOR_HASH *) my_hash_element(&lock_table_hash, 0)))
  {
    tmp_spider = tmp_link_for_hash->spider;
    tmp_link_idx = tmp_link_for_hash->link_idx;
    switch (tmp_spider->wide_handler->lock_type)
    {
      case TL_READ:
        lock_type = SPIDER_DB_TABLE_LOCK_READ_LOCAL;
        break;
      case TL_READ_NO_INSERT:
        lock_type = SPIDER_DB_TABLE_LOCK_READ;
        break;
      case TL_WRITE_LOW_PRIORITY:
        lock_type = SPIDER_DB_TABLE_LOCK_LOW_PRIORITY_WRITE;
        break;
      case TL_WRITE:
        lock_type = SPIDER_DB_TABLE_LOCK_WRITE;
        break;
      default:
        // no lock
        DBUG_PRINT("info",("spider lock_type=%d",
          tmp_spider->wide_handler->lock_type));
        DBUG_RETURN(0);
    }
    conn_link_idx = tmp_spider->conn_link_idx[tmp_link_idx];
    spider_odbc_share *db_share = (spider_odbc_share *)
      tmp_spider->share->dbton_share[conn->dbton_id];
    if (&db_share->db_names_str[conn_link_idx])
    {
      db_name = db_share->db_names_str[conn_link_idx].ptr();
      db_name_length = db_share->db_names_str[conn_link_idx].length();
      db_name_charset = tmp_spider->share->access_charset;
    } else {
      db_name = tmp_spider->share->tgt_dbs[conn_link_idx];
      db_name_length = tmp_spider->share->tgt_dbs_lengths[conn_link_idx];
      db_name_charset = system_charset_info;
    }
    if (&db_share->table_names_str[conn_link_idx])
    {
      table_name = db_share->table_names_str[conn_link_idx].ptr();
      table_name_length = db_share->table_names_str[conn_link_idx].length();
      table_name_charset = tmp_spider->share->access_charset;
    } else {
      table_name = tmp_spider->share->tgt_table_names[conn_link_idx];
      table_name_length =
        tmp_spider->share->tgt_table_names_lengths[conn_link_idx];
      table_name_charset = system_charset_info;
    }
    if ((error_num = utility->
      append_lock_table_body(
        str,
        db_name,
        db_name_length,
        db_name_charset,
        table_name,
        table_name_length,
        table_name_charset,
        lock_type
      )
    )) {
      my_hash_reset(&lock_table_hash);
      DBUG_RETURN(error_num);
    }
#ifdef HASH_UPDATE_WITH_HASH_VALUE
    my_hash_delete_with_hash_value(&lock_table_hash,
      tmp_link_for_hash->db_table_str_hash_value, (uchar*) tmp_link_for_hash);
#else
    my_hash_delete(&lock_table_hash, (uchar*) tmp_link_for_hash);
#endif
  }
  if ((error_num = utility->append_lock_table_tail(str)))
  {
    DBUG_RETURN(error_num);
  }
  DBUG_RETURN(0);
}

spider_db_odbc_mariadb_util::spider_db_odbc_mariadb_util(
  const char *nm_quote,
  const char *val_quote,
  uint nm_quote_length,
  uint val_quote_length
) : spider_db_odbc_util(
  nm_quote,
  val_quote,
  nm_quote_length,
  val_quote_length
)
{
  DBUG_ENTER("spider_db_odbc_mariadb_util::spider_db_odbc_mariadb_util");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

spider_db_odbc_mariadb_util::~spider_db_odbc_mariadb_util()
{
  DBUG_ENTER("spider_db_odbc_mariadb_util::~spider_db_odbc_mariadb_util");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

int spider_db_odbc_mariadb_util::append_lock_table_body(
  spider_string *str,
  const char *db_name,
  uint db_name_length,
  CHARSET_INFO *db_name_charset,
  const char *table_name,
  uint table_name_length,
  CHARSET_INFO *table_name_charset,
  int lock_type
) {
  DBUG_ENTER("spider_db_odbc_mariadb_util::append_lock_table_body");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(name_quote_length))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(name_quote, name_quote_length);
  if (db_name)
  {
    if (
      str->append(db_name, db_name_length, db_name_charset) ||
      str->reserve((name_quote_length) * 2 + SPIDER_SQL_DOT_LEN)
    ) {
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    str->q_append(name_quote, name_quote_length);
    str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
    str->q_append(name_quote, name_quote_length);
  }
  if (
    str->append(table_name, table_name_length, table_name_charset) ||
    str->reserve(name_quote_length +
      spider_db_table_lock_len[lock_type])
  ) {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(name_quote, name_quote_length);
  str->q_append(spider_db_table_lock_str[lock_type],
    spider_db_table_lock_len[lock_type]);
  DBUG_RETURN(0);
}

int spider_db_odbc_mariadb_util::append_lock_table_tail(
  spider_string *str
) {
  DBUG_ENTER("spider_db_odbc_mariadb_util::append_lock_table_tail");
  DBUG_PRINT("info",("spider this=%p", this));
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_db_odbc_mariadb_util::append_unlock_table(
  spider_string *str
) {
  DBUG_ENTER("spider_db_odbc_mariadb_util::append_unlock_table");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_UNLOCK_TABLE_LEN))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(SPIDER_SQL_UNLOCK_TABLE_STR, SPIDER_SQL_UNLOCK_TABLE_LEN);
  DBUG_RETURN(0);
}

spider_odbc_mariadb_handler::spider_odbc_mariadb_handler(
  ha_spider *spider,
  spider_odbc_share *db_share,
  spider_db_odbc_util *db_util
) : spider_odbc_handler(
  spider,
  db_share,
  db_util
)
{
  DBUG_ENTER("spider_odbc_mariadb_handler::spider_odbc_mariadb_handler");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

spider_odbc_mariadb_handler::~spider_odbc_mariadb_handler()
{
  DBUG_ENTER("spider_odbc_mariadb_handler::~spider_odbc_mariadb_handler");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

int spider_odbc_mariadb_handler::append_update_where(
  spider_string *str,
  const TABLE *table,
  my_ptrdiff_t ptr_diff
) {
  uint field_name_length;
  Field **field;
  THD *thd = spider->wide_handler->trx->thd;
  SPIDER_SHARE *share = spider->share;
  bool no_pk = (table->s->primary_key == MAX_KEY);
  DBUG_ENTER("spider_odbc_mariadb_handler::append_update_where");
  DBUG_PRINT("info", ("spider table->s->primary_key=%s",
    table->s->primary_key != MAX_KEY ? "TRUE" : "FALSE"));
  uint str_len_bakup = str->length();
  if (str->reserve(SPIDER_SQL_WHERE_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_WHERE_STR, SPIDER_SQL_WHERE_LEN);

  bool uk = no_pk;
  if (!uk)
  {
    for (uint i= 0; i < table->s->keys; i++)
    {
      if (table->s->key_info[i].flags & HA_NOSAME)
      {
        DBUG_PRINT("info",("spider found i:%u", i));
        uk = TRUE;
        break;
      }
    }
  }
  if (!uk)
  {
    spider_db_odbc_mariadb_result *result =
      (spider_db_odbc_mariadb_result *) spider->result_list.current;
    if (str->reserve(SPIDER_SQL_CUR_CUR_LEN + result->cur_nm_len))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_CUR_CUR_STR, SPIDER_SQL_CUR_CUR_LEN);
    str->q_append(result->cur_nm, result->cur_nm_len);
  } else {
    if (
      no_pk ||
      spider_param_use_cond_other_than_pk_for_update(thd)
    ) {
      for (field = table->field; *field; field++)
      {
        if (
          no_pk ||
          bitmap_is_set(table->read_set, (*field)->field_index)
        ) {
          field_name_length =
            odbc_share->column_name_str[(*field)->field_index].length();
          if ((*field)->is_null(ptr_diff))
          {
            if (str->reserve(field_name_length +
              utility->name_quote_length * 2 +
              SPIDER_SQL_IS_NULL_LEN + SPIDER_SQL_AND_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            odbc_share->append_column_name(str, (*field)->field_index);
            str->q_append(SPIDER_SQL_IS_NULL_STR, SPIDER_SQL_IS_NULL_LEN);
          } else {
            if (str->reserve(field_name_length +
              utility->name_quote_length * 2 +
              SPIDER_SQL_EQUAL_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            odbc_share->append_column_name(str, (*field)->field_index);
            str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
            (*field)->move_field_offset(ptr_diff);
            if (
              utility->append_column_value(spider, str, *field, NULL,
                share->access_charset) ||
              str->reserve(SPIDER_SQL_AND_LEN)
            )
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            (*field)->move_field_offset(-ptr_diff);
          }
          str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
        }
      }
    } else {
      KEY *key_info = &table->key_info[table->s->primary_key];
      KEY_PART_INFO *key_part;
      uint part_num;
      for (
        key_part = key_info->key_part, part_num = 0;
        part_num < spider_user_defined_key_parts(key_info);
        key_part++, part_num++
      ) {
        field = &key_part->field;
        field_name_length =
          odbc_share->column_name_str[(*field)->field_index].length();
        if ((*field)->is_null(ptr_diff))
        {
          if (str->reserve(field_name_length +
            utility->name_quote_length * 2 +
            SPIDER_SQL_IS_NULL_LEN + SPIDER_SQL_AND_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          odbc_share->append_column_name(str, (*field)->field_index);
          str->q_append(SPIDER_SQL_IS_NULL_STR, SPIDER_SQL_IS_NULL_LEN);
        } else {
          if (str->reserve(field_name_length +
            utility->name_quote_length * 2 +
            SPIDER_SQL_EQUAL_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          odbc_share->append_column_name(str, (*field)->field_index);
          str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
          (*field)->move_field_offset(ptr_diff);
          if (
            utility->append_column_value(spider, str, *field, NULL,
              share->access_charset) ||
            str->reserve(SPIDER_SQL_AND_LEN)
          )
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          (*field)->move_field_offset(-ptr_diff);
        }
        str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
      }
    }
    if (str->length() == str_len_bakup + SPIDER_SQL_WHERE_LEN)
    {
      /* no condition */
      str->length(str_len_bakup);
    } else {
      str->length(str->length() - SPIDER_SQL_AND_LEN);
    }
  }
  DBUG_RETURN(0);
}

int spider_odbc_mariadb_handler::append_select_lock(
  spider_string *str
) {
  int lock_mode = spider_conn_lock_mode(spider);
  DBUG_ENTER("spider_odbc_mariadb_handler::append_select_lock");
  DBUG_PRINT("info",("spider this=%p", this));
  if (lock_mode == SPIDER_LOCK_MODE_EXCLUSIVE)
  {
    if (str->reserve(SPIDER_SQL_FOR_UPDATE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_FOR_UPDATE_STR, SPIDER_SQL_FOR_UPDATE_LEN);
  } else if (lock_mode == SPIDER_LOCK_MODE_SHARED)
  {
    if (str->reserve(SPIDER_SQL_SHARED_LOCK_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SHARED_LOCK_STR, SPIDER_SQL_SHARED_LOCK_LEN);
  }
  DBUG_RETURN(0);
}

int spider_odbc_mariadb_handler::lock_tables(
  int link_idx
) {
  int error_num;
  SPIDER_CONN *conn = spider->conns[link_idx];
  spider_string *str = &sql;
  DBUG_ENTER("spider_odbc_mariadb_handler::lock_tables");
  str->length(0);
  if ((error_num = conn->db_conn->append_lock_tables(str)))
  {
    DBUG_RETURN(error_num);
  }
  if (str->length())
  {
    pthread_mutex_lock(&conn->mta_conn_mutex);
    SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
    conn->need_mon = &spider->need_mons[link_idx];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    if ((error_num = spider_db_set_names(spider, conn, link_idx)))
    {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      DBUG_RETURN(error_num);
    }
    spider_conn_set_timeout_from_share(conn, link_idx,
      spider->wide_handler->trx->thd,
      spider->share);
    if (spider_db_query(
      conn,
      str->ptr(),
      str->length(),
      -1,
      &spider->need_mons[link_idx])
    ) {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      DBUG_RETURN(spider_db_errorno(conn));
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  if (!conn->table_locked)
  {
    conn->table_locked = TRUE;
    spider->wide_handler->trx->locked_connections++;
  }
  DBUG_RETURN(0);
}

spider_odbc_mariadb_copy_table::spider_odbc_mariadb_copy_table(
  spider_odbc_share *db_share,
  spider_db_odbc_util *db_util
) : spider_odbc_copy_table(
  db_share,
  db_util
)
{
  DBUG_ENTER("spider_odbc_mariadb_copy_table::spider_odbc_mariadb_copy_table");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

spider_odbc_mariadb_copy_table::~spider_odbc_mariadb_copy_table()
{
  DBUG_ENTER("spider_odbc_mariadb_copy_table::~spider_odbc_mariadb_copy_table");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

int spider_odbc_mariadb_copy_table::append_select_lock_str(
  int lock_mode
) {
  DBUG_ENTER("spider_odbc_mariadb_copy_table::append_select_lock_str");
  DBUG_PRINT("info",("spider this=%p", this));
  if (lock_mode == SPIDER_LOCK_MODE_EXCLUSIVE)
  {
    if (sql.reserve(SPIDER_SQL_FOR_UPDATE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    sql.q_append(SPIDER_SQL_FOR_UPDATE_STR, SPIDER_SQL_FOR_UPDATE_LEN);
  } else if (lock_mode == SPIDER_LOCK_MODE_SHARED)
  {
    if (sql.reserve(SPIDER_SQL_SHARED_LOCK_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    sql.q_append(SPIDER_SQL_SHARED_LOCK_STR, SPIDER_SQL_SHARED_LOCK_LEN);
  }
  DBUG_RETURN(0);
}
#endif
