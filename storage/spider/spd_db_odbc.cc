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
#include "spd_db_odbc.h"
#include "ha_spider.h"
#include "spd_conn.h"
#include "spd_db_conn.h"
#include "spd_malloc.h"
#include "spd_sys_table.h"
#include "spd_table.h"

#define SPIDER_DB_WRAPPER_ODBC "odbc"

#ifdef HAVE_SPIDER_UNIXODBC

extern struct charset_info_st *spd_charset_utf8mb3_bin;

extern handlerton *spider_hton_ptr;
extern HASH spider_open_connections;
extern SPIDER_DBTON spider_dbton[SPIDER_DBTON_SIZE];
extern const char spider_dig_upper[];

spider_db_odbc_util spider_db_odbc_utility;

#define SPIDER_DB_DIRECTORY_STR "directory"
#define SPIDER_DB_DIRECTORY_LEN (sizeof(SPIDER_DB_DIRECTORY_STR) - 1)

#define SPIDER_SQL_UID_STR "uid"
#define SPIDER_SQL_UID_LEN (sizeof(SPIDER_SQL_UID_STR) - 1)
#define SPIDER_SQL_PWD_STR "pwd"
#define SPIDER_SQL_PWD_LEN (sizeof(SPIDER_SQL_PWD_STR) - 1)

#define SPIDER_SQL_ODBC_EQUAL_STR "="
#define SPIDER_SQL_ODBC_EQUAL_LEN (sizeof(SPIDER_SQL_ODBC_EQUAL_STR) - 1)

#define SPIDER_SQL_ODBC_TYPE_BINARY_STR "sql_binary"
#define SPIDER_SQL_ODBC_TYPE_BINARY_LEN (sizeof(SPIDER_SQL_ODBC_TYPE_BINARY_STR) - 1)
#define SPIDER_SQL_ODBC_TYPE_WLONGVARCHAR_STR "sql_wlongvarchar"
#define SPIDER_SQL_ODBC_TYPE_WLONGVARCHAR_LEN (sizeof(SPIDER_SQL_ODBC_TYPE_WLONGVARCHAR_STR) - 1)
#define SPIDER_SQL_ODBC_TYPE_DATE_STR "sql_date"
#define SPIDER_SQL_ODBC_TYPE_DATE_LEN (sizeof(SPIDER_SQL_ODBC_TYPE_DATE_STR) - 1)
#define SPIDER_SQL_ODBC_TYPE_TIME_STR "sql_time"
#define SPIDER_SQL_ODBC_TYPE_TIME_LEN (sizeof(SPIDER_SQL_ODBC_TYPE_TIME_STR) - 1)
#define SPIDER_SQL_ODBC_TYPE_TIMESTAMP_STR "sql_timestamp"
#define SPIDER_SQL_ODBC_TYPE_TIMESTAMP_LEN (sizeof(SPIDER_SQL_ODBC_TYPE_TIMESTAMP_STR) - 1)

#define SPIDER_SQL_ODBC_CLOSE_FUNC_STR ")}"
#define SPIDER_SQL_ODBC_CLOSE_FUNC_LEN (sizeof(SPIDER_SQL_ODBC_CLOSE_FUNC_STR) - 1)
#define SPIDER_SQL_ODBC_IN_STR " in "
#define SPIDER_SQL_ODBC_IN_LEN (sizeof(SPIDER_SQL_ODBC_IN_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_ASCII_STR "{fn ascii("
#define SPIDER_SQL_ODBC_FUNC_ASCII_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_ASCII_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_CHAR_STR "{fn char("
#define SPIDER_SQL_ODBC_FUNC_CHAR_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_CHAR_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_CONCAT_STR "{fn concat("
#define SPIDER_SQL_ODBC_FUNC_CONCAT_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_CONCAT_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_INSERT_STR "{fn insert("
#define SPIDER_SQL_ODBC_FUNC_INSERT_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_INSERT_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_LCASE_STR "{fn lcase("
#define SPIDER_SQL_ODBC_FUNC_LCASE_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_LCASE_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_LEFT_STR "{fn left("
#define SPIDER_SQL_ODBC_FUNC_LEFT_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_LEFT_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_LENGTH_STR "{fn length("
#define SPIDER_SQL_ODBC_FUNC_LENGTH_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_LENGTH_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_LOCATE_STR "{fn locate("
#define SPIDER_SQL_ODBC_FUNC_LOCATE_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_LOCATE_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_LTRIM_STR "{fn ltrim("
#define SPIDER_SQL_ODBC_FUNC_LTRIM_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_LTRIM_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_REPEAT_STR "{fn repeat("
#define SPIDER_SQL_ODBC_FUNC_REPEAT_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_REPEAT_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_REPLACE_STR "{fn replace("
#define SPIDER_SQL_ODBC_FUNC_REPLACE_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_REPLACE_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_RIGHT_STR "{fn right("
#define SPIDER_SQL_ODBC_FUNC_RIGHT_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_RIGHT_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_RTRIM_STR "{fn rtrim("
#define SPIDER_SQL_ODBC_FUNC_RTRIM_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_RTRIM_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_SUBSTRING_STR "{fn substring("
#define SPIDER_SQL_ODBC_FUNC_SUBSTRING_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_SUBSTRING_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_UCASE_STR "{fn ucase("
#define SPIDER_SQL_ODBC_FUNC_UCASE_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_UCASE_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_DIFFERENCE_STR "{fn difference("
#define SPIDER_SQL_ODBC_FUNC_DIFFERENCE_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_DIFFERENCE_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_SOUNDEX_STR "{fn soundex("
#define SPIDER_SQL_ODBC_FUNC_SOUNDEX_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_SOUNDEX_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_SPACE_STR "{fn space("
#define SPIDER_SQL_ODBC_FUNC_SPACE_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_SPACE_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_BIT_LENGTH_STR "{fn bit_length("
#define SPIDER_SQL_ODBC_FUNC_BIT_LENGTH_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_BIT_LENGTH_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_CHAR_LENGTH_STR "{fn char_length("
#define SPIDER_SQL_ODBC_FUNC_CHAR_LENGTH_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_CHAR_LENGTH_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_CHARACTER_LENGTH_STR "{fn character_length("
#define SPIDER_SQL_ODBC_FUNC_CHARACTER_LENGTH_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_CHARACTER_LENGTH_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_OCTET_LENGTH_STR "{fn octet_length("
#define SPIDER_SQL_ODBC_FUNC_OCTET_LENGTH_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_OCTET_LENGTH_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_POSITION_STR "{fn position("
#define SPIDER_SQL_ODBC_FUNC_POSITION_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_POSITION_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_ABS_STR "{fn abs("
#define SPIDER_SQL_ODBC_FUNC_ABS_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_ABS_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_ACOS_STR "{fn acos("
#define SPIDER_SQL_ODBC_FUNC_ACOS_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_ACOS_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_ASIN_STR "{fn asin("
#define SPIDER_SQL_ODBC_FUNC_ASIN_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_ASIN_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_ATAN_STR "{fn atan("
#define SPIDER_SQL_ODBC_FUNC_ATAN_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_ATAN_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_CEILING_STR "{fn ceiling("
#define SPIDER_SQL_ODBC_FUNC_CEILING_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_CEILING_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_COS_STR "{fn cos("
#define SPIDER_SQL_ODBC_FUNC_COS_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_COS_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_COT_STR "{fn cot("
#define SPIDER_SQL_ODBC_FUNC_COT_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_COT_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_EXP_STR "{fn exp("
#define SPIDER_SQL_ODBC_FUNC_EXP_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_EXP_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_FLOOR_STR "{fn floor("
#define SPIDER_SQL_ODBC_FUNC_FLOOR_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_FLOOR_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_LOG_STR "{fn log("
#define SPIDER_SQL_ODBC_FUNC_LOG_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_LOG_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_MOD_STR "{fn mod("
#define SPIDER_SQL_ODBC_FUNC_MOD_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_MOD_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_PI_STR "{fn pi("
#define SPIDER_SQL_ODBC_FUNC_PI_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_PI_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_RAND_STR "{fn rand("
#define SPIDER_SQL_ODBC_FUNC_RAND_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_RAND_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_SIGN_STR "{fn sign("
#define SPIDER_SQL_ODBC_FUNC_SIGN_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_SIGN_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_SIN_STR "{fn sin("
#define SPIDER_SQL_ODBC_FUNC_SIN_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_SIN_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_SQRT_STR "{fn sqrt("
#define SPIDER_SQL_ODBC_FUNC_SQRT_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_SQRT_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_TAN_STR "{fn tan("
#define SPIDER_SQL_ODBC_FUNC_TAN_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_TAN_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_ATAN2_STR "{fn atan2("
#define SPIDER_SQL_ODBC_FUNC_ATAN2_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_ATAN2_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_DEGREES_STR "{fn degrees("
#define SPIDER_SQL_ODBC_FUNC_DEGREES_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_DEGREES_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_LOG10_STR "{fn log10("
#define SPIDER_SQL_ODBC_FUNC_LOG10_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_LOG10_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_POWER_STR "{fn power("
#define SPIDER_SQL_ODBC_FUNC_POWER_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_POWER_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_RADIANS_STR "{fn radians("
#define SPIDER_SQL_ODBC_FUNC_RADIANS_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_RADIANS_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_ROUND_STR "{fn round("
#define SPIDER_SQL_ODBC_FUNC_ROUND_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_ROUND_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_TRUNCATE_STR "{fn truncate("
#define SPIDER_SQL_ODBC_FUNC_TRUNCATE_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_TRUNCATE_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_CURDATE_STR "{fn curdate("
#define SPIDER_SQL_ODBC_FUNC_CURDATE_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_CURDATE_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_CURTIME_STR "{fn curtime("
#define SPIDER_SQL_ODBC_FUNC_CURTIME_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_CURTIME_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_DAYOFMONTH_STR "{fn dayofmonth("
#define SPIDER_SQL_ODBC_FUNC_DAYOFMONTH_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_DAYOFMONTH_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_DAYOFWEEK_STR "{fn dayofweek("
#define SPIDER_SQL_ODBC_FUNC_DAYOFWEEK_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_DAYOFWEEK_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_DAYOFYEAR_STR "{fn dayofyear("
#define SPIDER_SQL_ODBC_FUNC_DAYOFYEAR_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_DAYOFYEAR_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_HOUR_STR "{fn hour("
#define SPIDER_SQL_ODBC_FUNC_HOUR_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_HOUR_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_MINUTE_STR "{fn minute("
#define SPIDER_SQL_ODBC_FUNC_MINUTE_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_MINUTE_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_MONTH_STR "{fn month("
#define SPIDER_SQL_ODBC_FUNC_MONTH_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_MONTH_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_NOW_STR "{fn now("
#define SPIDER_SQL_ODBC_FUNC_NOW_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_NOW_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_QUARTER_STR "{fn quarter("
#define SPIDER_SQL_ODBC_FUNC_QUARTER_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_QUARTER_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_SECOND_STR "{fn second("
#define SPIDER_SQL_ODBC_FUNC_SECOND_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_SECOND_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_WEEK_STR "{fn week("
#define SPIDER_SQL_ODBC_FUNC_WEEK_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_WEEK_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_YEAR_STR "{fn year("
#define SPIDER_SQL_ODBC_FUNC_YEAR_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_YEAR_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_DAYNAME_STR "{fn dayname("
#define SPIDER_SQL_ODBC_FUNC_DAYNAME_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_DAYNAME_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_MONTHNAME_STR "{fn monthname("
#define SPIDER_SQL_ODBC_FUNC_MONTHNAME_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_MONTHNAME_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_TIMESTAMPADD_STR "{fn timestampadd("
#define SPIDER_SQL_ODBC_FUNC_TIMESTAMPADD_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_TIMESTAMPADD_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_TIMESTAMPDIFF_STR "{fn timestampdiff("
#define SPIDER_SQL_ODBC_FUNC_TIMESTAMPDIFF_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_TIMESTAMPDIFF_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_CURRENT_DATE_STR "{fn current_date("
#define SPIDER_SQL_ODBC_FUNC_CURRENT_DATE_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_CURRENT_DATE_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_CURRENT_TIME_STR "{fn current_time("
#define SPIDER_SQL_ODBC_FUNC_CURRENT_TIME_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_CURRENT_TIME_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_CURRENT_TIMESTAMP_STR "{fn current_timestamp("
#define SPIDER_SQL_ODBC_FUNC_CURRENT_TIMESTAMP_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_CURRENT_TIMESTAMP_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_EXTRACT_STR "{fn extract("
#define SPIDER_SQL_ODBC_FUNC_EXTRACT_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_EXTRACT_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_DATABASE_STR "{fn database("
#define SPIDER_SQL_ODBC_FUNC_DATABASE_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_DATABASE_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_IFNULL_STR "{fn ifnull("
#define SPIDER_SQL_ODBC_FUNC_IFNULL_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_IFNULL_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_USER_STR "{fn ifnull("
#define SPIDER_SQL_ODBC_FUNC_USER_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_USER_STR) - 1)

#define SPIDER_SQL_ODBC_FUNC_CONVERT_STR "{fn convert("
#define SPIDER_SQL_ODBC_FUNC_CONVERT_LEN (sizeof(SPIDER_SQL_ODBC_FUNC_CONVERT_STR) - 1)


#define SPIDER_SQL_ODBC_COUNT_STR "{fn count("
#define SPIDER_SQL_ODBC_COUNT_LEN (sizeof(SPIDER_SQL_ODBC_COUNT_STR) - 1)
#define SPIDER_SQL_ODBC_COUNT_DISTINCT_STR "{fn count(distinct "
#define SPIDER_SQL_ODBC_COUNT_DISTINCT_LEN (sizeof(SPIDER_SQL_ODBC_COUNT_DISTINCT_STR) - 1)

#define SPIDER_SQL_ODBC_SUM_STR "{fn sum("
#define SPIDER_SQL_ODBC_SUM_LEN (sizeof(SPIDER_SQL_ODBC_SUM_STR) - 1)
#define SPIDER_SQL_ODBC_SUM_DISTINCT_STR "{fn sum(distinct "
#define SPIDER_SQL_ODBC_SUM_DISTINCT_LEN (sizeof(SPIDER_SQL_ODBC_SUM_DISTINCT_STR) - 1)

#define SPIDER_SQL_ODBC_AVG_STR "{fn avg("
#define SPIDER_SQL_ODBC_AVG_LEN (sizeof(SPIDER_SQL_ODBC_AVG_STR) - 1)
#define SPIDER_SQL_ODBC_AVG_DISTINCT_STR "{fn avg(distinct "
#define SPIDER_SQL_ODBC_AVG_DISTINCT_LEN (sizeof(SPIDER_SQL_ODBC_AVG_DISTINCT_STR) - 1)

#define SPIDER_SQL_ODBC_MIN_STR "{fn min("
#define SPIDER_SQL_ODBC_MIN_LEN (sizeof(SPIDER_SQL_ODBC_MIN_STR) - 1)
#define SPIDER_SQL_ODBC_MAX_STR "{fn max("
#define SPIDER_SQL_ODBC_MAX_LEN (sizeof(SPIDER_SQL_ODBC_MAX_STR) - 1)


#define SPIDER_SQL_LOCK_TABLE_START_STR "start transaction"
#define SPIDER_SQL_LOCK_TABLE_START_LEN (sizeof(SPIDER_SQL_LOCK_TABLE_START_STR) - 1)
#define SPIDER_SQL_LOCK_TABLE_STR "lock table "
#define SPIDER_SQL_LOCK_TABLE_LEN (sizeof(SPIDER_SQL_LOCK_TABLE_STR) - 1)
#define SPIDER_SQL_UNLOCK_TABLE_STR "commit"
#define SPIDER_SQL_UNLOCK_TABLE_LEN (sizeof(SPIDER_SQL_UNLOCK_TABLE_STR) - 1)

#define SPIDER_SQL_CUR_CUR_STR "CURRENT OF cur"
#define SPIDER_SQL_CUR_CUR_LEN (sizeof(SPIDER_SQL_CUR_CUR_STR) - 1)

#define SPIDER_SQL_FOR_SHARE_STR " for share"
#define SPIDER_SQL_FOR_SHARE_LEN (sizeof(SPIDER_SQL_FOR_SHARE_STR) - 1)

static uchar SPIDER_SQL_LINESTRING_HEAD_STR[] =
  {0x00,0x00,0x00,0x00,0x01,0x02,0x00,0x00,0x00,0x02,0x00,0x00,0x00};
#define SPIDER_SQL_LINESTRING_HEAD_LEN sizeof(SPIDER_SQL_LINESTRING_HEAD_STR)

static const char *spider_db_table_lock_str[] =
{
  " in share mode",
  " in share mode",
  " in exclusive mode",
  " in exclusive mode"
};
static const int spider_db_table_lock_len[] =
{
  sizeof(" in share mode") - 1,
  sizeof(" in share mode") - 1,
  sizeof(" in exclusive mode") - 1,
  sizeof(" in exclusive mode") - 1
};

/* UTC time zone for timestamp columns */
extern Time_zone *UTC;

int spider_odbc_init()
{
  DBUG_ENTER("spider_odbc_init");
  DBUG_RETURN(0);
}

int spider_odbc_deinit()
{
  DBUG_ENTER("spider_odbc_deinit");
  DBUG_RETURN(0);
}

spider_db_share *spider_odbc_create_share(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_odbc_create_share");
  DBUG_RETURN(new spider_odbc_share(share));
}

spider_db_handler *spider_odbc_create_handler(
  ha_spider *spider,
  spider_db_share *db_share
) {
  DBUG_ENTER("spider_odbc_create_handler");
  DBUG_RETURN(new spider_odbc_handler(spider,
    (spider_odbc_share *) db_share));
}

spider_db_copy_table *spider_odbc_create_copy_table(
  spider_db_share *db_share
) {
  DBUG_ENTER("spider_odbc_create_copy_table");
  DBUG_RETURN(new spider_odbc_copy_table(
    (spider_odbc_share *) db_share));
}

SPIDER_DB_CONN *spider_odbc_create_conn(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_odbc_create_conn");
  DBUG_RETURN(new spider_db_odbc(conn));
}

bool spider_odbc_support_direct_join(
) {
  DBUG_ENTER("spider_odbc_support_direct_join");
  DBUG_RETURN(FALSE);
}

SPIDER_DBTON spider_dbton_odbc = {
  0,
  SPIDER_DB_WRAPPER_ODBC,
  SPIDER_DB_ACCESS_TYPE_SQL,
  spider_odbc_init,
  spider_odbc_deinit,
  spider_odbc_create_share,
  spider_odbc_create_handler,
  spider_odbc_create_copy_table,
  spider_odbc_create_conn,
#ifdef SPIDER_SQL_TYPE_DDL_SQL
  NULL,
#endif
  spider_odbc_support_direct_join,
  &spider_db_odbc_utility
};

static int spider_db_odbc_get_error(
  SQLRETURN ret,
  SQLSMALLINT hnd_type,
  SQLHANDLE hnd,
  SPIDER_CONN *conn,
  char *stored_error_msg
) {
  SQLCHAR sql_state[6];
  SQLCHAR msg[80];
  SQLINTEGER native_err_num = 0;
  SQLSMALLINT msg_lgt;
  SQLSMALLINT rec_num = 1;
  SQLRETURN ret_diag;
  bool dup_entry = FALSE;
  time_t cur_time;
  struct tm lt;
  struct tm *l_time;
  uint log_result_errors = spider_param_log_result_errors();
  DBUG_ENTER("spider_db_odbc_get_error");
  DBUG_PRINT("info",("spider ret:%0d", ret));
#ifdef DBUG_OFF
  if (log_result_errors >= 3 ||
    (log_result_errors >= 1 && ret != SQL_SUCCESS_WITH_INFO))
#endif
  {
    cur_time = (time_t) time((time_t*) 0);
    l_time = localtime_r(&cur_time, &lt);
    DBUG_PRINT("info",("spider %04d%02d%02d %02d:%02d:%02d",
      l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday,
      l_time->tm_hour, l_time->tm_min, l_time->tm_sec));
  }
  ret_diag = SQLGetDiagRec(hnd_type, hnd, rec_num, sql_state,
    &native_err_num, msg, (SQLSMALLINT) 80, &msg_lgt);
  while (ret_diag == SQL_SUCCESS || ret_diag == SQL_SUCCESS_WITH_INFO)
  {
    if (stored_error_msg && rec_num == 1)
    {
      strmov(stored_error_msg, (const char *) msg);
    }
    if (!memcmp(sql_state, "23000", 5) ||
        !memcmp(sql_state, "23505", 5))
    {
      DBUG_PRINT("info",("spider [DUP ENTRY] from %s to %ld: %u[%s]%s",
        conn ? conn->tgt_host : "", (ulong) current_thd->thread_id,
        (uint) native_err_num, sql_state, msg));
      dup_entry = TRUE;
      break;
    } else {
      if (ret == SQL_SUCCESS_WITH_INFO)
      {
        DBUG_PRINT("info",("spider [WARN RESULT] from %s to %ld: %u[%s]%s",
          conn ? conn->tgt_host : "", (ulong) current_thd->thread_id,
          (uint) native_err_num, sql_state, msg));
        push_warning_printf(current_thd, SPIDER_WARN_LEVEL_WARN,
          ER_SPIDER_DATASOURCE_NUM, ER_SPIDER_DATASOURCE_STR, "ODBC",
          native_err_num, sql_state, msg);
        if (log_result_errors >= 3)
        {
          fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [WARN SPIDER RESULT] "
            "from %s to %ld: %u[%s]%s\n",
            l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday,
            l_time->tm_hour, l_time->tm_min, l_time->tm_sec,
            conn ? conn->tgt_host : "", (ulong) current_thd->thread_id,
            (uint) native_err_num, sql_state, msg);
        }
      } else {
        DBUG_PRINT("info",("spider [ERROR RESULT] from %s to %ld: %u[%s]%s",
          conn ? conn->tgt_host : "", (ulong) current_thd->thread_id,
          (uint) native_err_num, sql_state, msg));
        my_printf_error(ER_SPIDER_DATASOURCE_NUM, ER_SPIDER_DATASOURCE_STR,
          MYF(0), "ODBC", native_err_num, sql_state, msg);
        if (log_result_errors >= 1)
        {
          fprintf(stderr, "%04d%02d%02d %02d:%02d:%02d [ERROR SPIDER RESULT] "
            "from %s to %ld: %u[%s]%s\n",
            l_time->tm_year + 1900, l_time->tm_mon + 1, l_time->tm_mday,
            l_time->tm_hour, l_time->tm_min, l_time->tm_sec,
            conn ? conn->tgt_host : "", (ulong) current_thd->thread_id,
            (uint) native_err_num, sql_state, msg);
        }
      }
    }
    ++rec_num;
    ret_diag = SQLGetDiagRec(hnd_type, hnd, rec_num, sql_state,
      &native_err_num, msg, (SQLSMALLINT) 80, &msg_lgt);
  }
  DBUG_PRINT("info",("spider ret_diag=%d", ret_diag));
  if (dup_entry)
  {
    DBUG_RETURN(ER_DUP_ENTRY);
  }
  if (ret_diag == SQL_NO_DATA || ret_diag == SQL_SUCCESS_WITH_INFO)
  {
    if (ret == SQL_SUCCESS_WITH_INFO)
    {
      DBUG_RETURN(0);
    } else {
      DBUG_RETURN(ER_SPIDER_DATASOURCE_NUM);
    }
  }
  DBUG_RETURN(ER_SPIDER_UNKNOWN_NUM);
}

spider_db_odbc_row::spider_db_odbc_row(
  uint dbton_id
) : spider_db_row(dbton_id),
  row(NULL), len(NULL), null(NULL), cloned(FALSE)
{
  DBUG_ENTER("spider_db_odbc_row::spider_db_odbc_row");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

spider_db_odbc_row::~spider_db_odbc_row()
{
  DBUG_ENTER("spider_db_odbc_row::~spider_db_odbc_row");
  DBUG_PRINT("info",("spider this=%p", this));
  if (cloned)
  {
    spider_free(spider_current_trx, len, MYF(0));
    delete [] row;
  }
  DBUG_VOID_RETURN;
}

int spider_db_odbc_row::store_to_field(
  Field *field,
  CHARSET_INFO *access_charset
) {
  DBUG_ENTER("spider_db_odbc_row::store_to_field");
  DBUG_PRINT("info",("spider this=%p", this));
  if (spider_bit_is_set(null, fpos))
  {
    DBUG_PRINT("info", ("spider field is null"));
    field->set_null();
    field->reset();
  } else {
    DBUG_PRINT("info", ("spider field->type()=%u", field->type()));
    field->set_notnull();
    if (field->type() == MYSQL_TYPE_YEAR)
    {
      field->store(row[fpos].ptr(), 4,
        field->table->s->table_charset);
    } else if (field->type() == MYSQL_TYPE_DATE)
    {
      field->store(row[fpos].ptr(), 10,
        field->table->s->table_charset);
    } else if (field->type() == MYSQL_TYPE_TIME)
    {
      field->store(row[fpos].ptr() + 11, 8,
        field->table->s->table_charset);
    } else {
      DBUG_PRINT("info", ("spider val_str->length()=%u", row[fpos].length()));
      if (field->flags & BLOB_FLAG)
      {
        DBUG_PRINT("info", ("spider blob field"));
        ((Field_blob *)field)->set_ptr(
          row[fpos].length(), (uchar *) row[fpos].ptr());
      } else {
        field->store(row[fpos].ptr(), row[fpos].length(),
          field->table->s->table_charset);
      }
    }
  }
  DBUG_RETURN(0);
}

int spider_db_odbc_row::append_to_str(
  spider_string *str
) {
  DBUG_ENTER("spider_db_odbc_row::append_to_str");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(row[fpos].length()))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(row[fpos].ptr(), row[fpos].length());
  DBUG_RETURN(0);
}

int spider_db_odbc_row::append_escaped_to_str(
  spider_string *str,
  uint dbton_id
) {
  DBUG_ENTER("spider_db_odbc_row::append_escaped_to_str");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(row[fpos].length() * 2 + 2))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  spider_dbton[dbton_id].db_util->append_escaped_util(str,
    row[fpos].get_str());
  DBUG_RETURN(0);
}

void spider_db_odbc_row::first()
{
  DBUG_ENTER("spider_db_odbc_row::first");
  DBUG_PRINT("info",("spider this=%p", this));
  fpos = 0;
  DBUG_VOID_RETURN;
}

void spider_db_odbc_row::next()
{
  DBUG_ENTER("spider_db_odbc_row::next");
  DBUG_PRINT("info",("spider this=%p", this));
  ++fpos;
  DBUG_VOID_RETURN;
}

bool spider_db_odbc_row::is_null()
{
  DBUG_ENTER("spider_db_odbc_row::is_null");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(spider_bit_is_set(null, fpos));
}

int spider_db_odbc_row::val_int()
{
  DBUG_ENTER("spider_db_odbc_row::val_int");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(is_null() ? 0 : atoi(row[fpos].ptr()));
}

double spider_db_odbc_row::val_real()
{
  DBUG_ENTER("spider_db_odbc_row::val_real");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(is_null() ? 0.0 : my_atof(row[fpos].ptr()));
}

my_decimal *spider_db_odbc_row::val_decimal(
  my_decimal *decimal_value,
  CHARSET_INFO *access_charset
) {
  DBUG_ENTER("spider_db_odbc_row::val_decimal");
  DBUG_PRINT("info",("spider this=%p", this));
  if (is_null())
    DBUG_RETURN(NULL);

#ifdef SPIDER_HAS_DECIMAL_OPERATION_RESULTS_VALUE_TYPE
  decimal_operation_results(str2my_decimal(0, row[fpos].ptr(),
    row[fpos].length(), access_charset, decimal_value), "", "");
#else
  decimal_operation_results(str2my_decimal(0, row[fpos].ptr(),
    row[fpos].length(), access_charset, decimal_value));
#endif

  DBUG_RETURN(decimal_value);
}

SPIDER_DB_ROW *spider_db_odbc_row::clone()
{
  spider_db_odbc_row *clone_row;
  DBUG_ENTER("spider_db_odbc_row::clone");
  DBUG_PRINT("info",("spider this=%p", this));
  if (!(clone_row = new spider_db_odbc_row(dbton_id)))
  {
    DBUG_RETURN(NULL);
  }
  if (!spider_bulk_malloc(spider_current_trx, 273, MYF(MY_WME),
    &clone_row->len, (uint) (sizeof(uint32) * field_count),
    &clone_row->null, (uint) (sizeof(uchar) * ((field_count + 7) / 8)),
    NullS)
  ) {
    delete clone_row;
    DBUG_RETURN(NULL);
  }
  memcpy(clone_row->len, len, sizeof(uint32) * field_count);
  memcpy(clone_row->null, null, (field_count + 7) / 8);
  if (!(clone_row->row = new spider_string[field_count]))
  {
    spider_free(spider_current_trx, clone_row->len, MYF(0));
    delete clone_row;
    DBUG_RETURN(NULL);
  }
  for (SQLUSMALLINT i = 0; i < field_count; ++i)
  {
    spider_string *c, *o;
    c = &clone_row->row[i];
    o = &row[i];
    c->init_calc_mem(274);
    c->set_charset(o->charset());
    if(c->reserve(o->length()))
    {
      delete [] clone_row->row;
      spider_free(spider_current_trx, clone_row->len, MYF(0));
      delete clone_row;
      DBUG_RETURN(NULL);
    }
    c->q_append(o->ptr(), o->length());
  }
  clone_row->field_count = field_count;
  clone_row->record_size = record_size;
  clone_row->fpos = 0;
  clone_row->cloned = TRUE;
  DBUG_RETURN((SPIDER_DB_ROW *) clone_row);
}

int spider_db_odbc_row::store_to_tmp_table(
  TABLE *tmp_table,
  spider_string *str
) {
  DBUG_ENTER("spider_db_odbc_row::store_to_tmp_table");
  DBUG_PRINT("info",("spider this=%p", this));
  str->length(0);
  if (str->reserve(record_size))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  for (SQLUSMALLINT i = 0; i < field_count; ++i)
  {
    str->q_append(row[i].ptr(), row[i].length());
  }
  tmp_table->field[0]->set_notnull();
  tmp_table->field[0]->store(
    (const char *) len,
    sizeof(uint32) * field_count, &my_charset_bin);
  tmp_table->field[1]->set_notnull();
  tmp_table->field[1]->store(
    (const char *) null, (uint) (sizeof(uchar) * (field_count + 7) / 8),
    &my_charset_bin);
  tmp_table->field[2]->set_notnull();
  tmp_table->field[2]->store(
    str->ptr(), str->length(), &my_charset_bin);
  DBUG_RETURN(tmp_table->file->ha_write_row(tmp_table->record[0]));
}

uint spider_db_odbc_row::get_byte_size()
{
  DBUG_ENTER("spider_db_odbc_row::get_byte_size");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(record_size);
}

spider_db_odbc_result::spider_db_odbc_result(
  SPIDER_DB_CONN *in_db_conn
) : spider_db_result(in_db_conn),
  row(in_db_conn->dbton_id), field_count(0), spider(NULL), buf(NULL),
  len(NULL), null(NULL), stored_error_num(0)
{
  DBUG_ENTER("spider_db_odbc_result::spider_db_odbc_result");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider hstm:%p", ((spider_db_odbc *) db_conn)->hstm));
  DBUG_VOID_RETURN;
}

spider_db_odbc_result::~spider_db_odbc_result()
{
  DBUG_ENTER("spider_db_odbc_result::~spider_db_odbc_result");
  DBUG_PRINT("info",("spider this=%p", this));
  if (((spider_db_odbc *) db_conn)->hstm)
  {
    free_result();
  }
  if (buf)
  {
    delete [] buf;
  }
  if (len)
  {
    spider_free(spider_current_trx, len, MYF(0));
  }
  DBUG_VOID_RETURN;
}

int spider_db_odbc_result::init()
{
  SQLRETURN ret;
  DBUG_ENTER("spider_db_odbc_result::init");
  DBUG_PRINT("info",("spider this=%p", this));
  ret = SQLNumResultCols(((spider_db_odbc *) db_conn)->hstm, &field_count);
  if (ret != SQL_SUCCESS)
  {
    DBUG_RETURN(spider_db_odbc_get_error(ret, SQL_HANDLE_STMT,
      ((spider_db_odbc *) db_conn)->hstm, db_conn->conn, stored_error_msg));
  }
  DBUG_PRINT("info",("spider field_count=%u", field_count));
  if (!spider_bulk_malloc(spider_current_trx, 275, MYF(MY_WME),
    &len, (uint) (sizeof(uint32) * field_count),
    &null, (uint) (sizeof(uchar) * ((field_count + 7) / 8)),
    NullS)
  ) {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  memset(null, 0, (field_count + 7) / 8);
  if (!(buf = new spider_string[field_count]))
  {
    spider_free(spider_current_trx, len, MYF(0));
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  for (SQLUSMALLINT i = 0; i < field_count; ++i)
  {
    buf[i].init_calc_mem(276);
    buf[i].set_charset(db_conn->conn->access_charset);
  }
  row.row = buf;
  row.len = len;
  row.null = null;
  row.field_count = field_count;
  DBUG_RETURN(0);
}

void spider_db_odbc_result::set_limit(longlong value)
{
  DBUG_ENTER("spider_db_odbc_result::set_limit");
  DBUG_PRINT("info",("spider this=%p", this));
  limit = value;
  DBUG_VOID_RETURN;
}

bool spider_db_odbc_result::has_result()
{
  DBUG_ENTER("spider_db_odbc_result::has_result");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(((spider_db_odbc *) db_conn)->hstm);
}

void spider_db_odbc_result::free_result()
{
  SQLRETURN ret;
  DBUG_ENTER("spider_db_odbc_result::free_result");
  DBUG_PRINT("info",("spider this=%p", this));
  SQLHSTMT hstm = ((spider_db_odbc *) db_conn)->hstm;
  if (hstm)
  {
    ret = SQLCloseCursor(hstm);
    if (ret != SQL_SUCCESS)
    {
      stored_error_num = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT,
        hstm, db_conn->conn, stored_error_msg);
    }
    SQLFreeHandle(SQL_HANDLE_STMT, hstm);
    ((spider_db_odbc *) db_conn)->hstm = NULL;
  }
  DBUG_VOID_RETURN;
}

SPIDER_DB_ROW *spider_db_odbc_result::current_row()
{
  DBUG_ENTER("spider_db_odbc_result::current_row");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN((SPIDER_DB_ROW *) row.clone());
}

SPIDER_DB_ROW *spider_db_odbc_result::fetch_row()
{
  SQLRETURN ret;
  DBUG_ENTER("spider_db_odbc_result::fetch_row");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider hstm:%p", ((spider_db_odbc *) db_conn)->hstm));
  SQLLEN buf_sz = spider && spider->share ?
    spider_param_buffer_size(spider->wide_handler->trx->thd,
      spider->share->buffer_size) :
    spider_param_buffer_size(current_thd, 16000), sz;
  row.record_size = 0;
  ret = SQLFetch(((spider_db_odbc *) db_conn)->hstm);
  if (ret != SQL_SUCCESS)
  {
    if (ret == SQL_NO_DATA)
    {
      stored_error_num = HA_ERR_END_OF_FILE;
    } else {
      stored_error_num = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT,
        ((spider_db_odbc *) db_conn)->hstm, db_conn->conn, stored_error_msg);
    }
    DBUG_RETURN(NULL);
  }
  for (SQLUSMALLINT i = 0; i < field_count; ++i)
  {
    spider_string *b = &buf[i];
    uint32 *l = &len[i];
    b->length(0);
    *l = 0;
    if(b->reserve(buf_sz))
    {
      stored_error_num = HA_ERR_OUT_OF_MEM;
      DBUG_RETURN(NULL);
    }
    DBUG_PRINT("info",("spider i:%u", i));
    ret = SQLGetData(((spider_db_odbc *) db_conn)->hstm, i + 1, SQL_C_CHAR,
      (char *) b->ptr(), buf_sz, &sz);
    if (ret != SQL_SUCCESS)
    {
      stored_error_num = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT,
        ((spider_db_odbc *) db_conn)->hstm, db_conn->conn, stored_error_msg);
      DBUG_RETURN(NULL);
    }
    if (sz == SQL_NULL_DATA)
    {
      DBUG_PRINT("info",("spider is null"));
      spider_set_bit(null, i);
      continue;
    }
    spider_clear_bit(null, i);
    b->length(b->length() + sz);
    *l += sz;
    while (buf_sz == sz)
    {
      if(b->reserve(buf_sz))
      {
        stored_error_num = HA_ERR_OUT_OF_MEM;
        DBUG_RETURN(NULL);
      }
      DBUG_PRINT("info",("spider length:%u", b->length()));
      ret = SQLGetData(((spider_db_odbc *) db_conn)->hstm, i + 1, SQL_C_CHAR,
        (char *) b->ptr() + b->length(), buf_sz, &sz);
      if (ret != SQL_SUCCESS)
      {
        stored_error_num = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT,
          ((spider_db_odbc *) db_conn)->hstm, db_conn->conn, stored_error_msg);
        DBUG_RETURN(NULL);
      }
      b->length(b->length() + sz);
      *l += sz;
    }
    row.record_size += *l;
  }
  row.fpos = 0;
  DBUG_RETURN((SPIDER_DB_ROW *) &row);
}

SPIDER_DB_ROW *spider_db_odbc_result::fetch_row_from_result_buffer(
  spider_db_result_buffer *spider_res_buf
) {
  DBUG_ENTER("spider_db_odbc_result::fetch_row_from_result_buffer");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(fetch_row());
}

SPIDER_DB_ROW *spider_db_odbc_result::fetch_row_from_tmp_table(
  TABLE *tmp_table
) {
  uint32 l;
  spider_string tmp_str1, tmp_str2, tmp_str3, *b;
  const char *row_ptr;
  DBUG_ENTER("spider_db_odbc_result::fetch_row_from_tmp_table");
  DBUG_PRINT("info",("spider this=%p", this));
  tmp_str1.init_calc_mem(305);
  tmp_str2.init_calc_mem(306);
  tmp_str3.init_calc_mem(307);
  tmp_table->field[0]->val_str(tmp_str1.get_str());
  tmp_table->field[1]->val_str(tmp_str2.get_str());
  tmp_table->field[2]->val_str(tmp_str3.get_str());
  tmp_str1.mem_calc();
  tmp_str2.mem_calc();
  tmp_str3.mem_calc();
  memcpy(len, tmp_str1.ptr(), tmp_str1.length());
  memcpy(null, tmp_str2.ptr(), tmp_str2.length());
  row_ptr = tmp_str3.ptr();
  row.record_size = tmp_str3.length();
  for (SQLUSMALLINT i = 0; i < field_count; ++i)
  {
    b = &buf[i];
    b->length(0);
    l = len[i];
    if(b->reserve(l))
    {
      stored_error_num = HA_ERR_OUT_OF_MEM;
      DBUG_RETURN(NULL);
    }
    b->q_append(row_ptr, l);
    row_ptr += l;
  }
  row.fpos = 0;
  DBUG_RETURN((SPIDER_DB_ROW *) &row);
}

int spider_db_odbc_result::fetch_table_status(
  int mode,
  ha_statistics &stat
) {
  DBUG_ENTER("spider_db_odbc_result::fetch_table_status");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc_result::fetch_table_records(
  int mode,
  ha_rows &records
) {
  DBUG_ENTER("spider_db_odbc_result::fetch_table_records");
  DBUG_PRINT("info",("spider this=%p", this));
  if (!fetch_row())
  {
    records = 0;
  } else {
    records = row.val_int();
  }
  DBUG_RETURN(0);
}

int spider_db_odbc_result::fetch_table_cardinality(
  int mode,
  TABLE *table,
  longlong *cardinality,
  uchar *cardinality_upd,
  int bitmap_size
) {
  DBUG_ENTER("spider_db_odbc_result::fetch_table_cardinality");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc_result::fetch_table_mon_status(
  int &status
) {
  DBUG_ENTER("spider_db_odbc_result::fetch_table_mon_status");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  status = SPIDER_LINK_MON_OK;
  DBUG_RETURN(0);
}

longlong spider_db_odbc_result::num_rows()
{
/*
  SQLRETURN ret;
  SQLLEN cnt;
*/
  DBUG_ENTER("spider_db_odbc_result::num_rows");
  DBUG_PRINT("info",("spider this=%p", this));
/*
  ret = SQLRowCount(((spider_db_odbc *) db_conn)->hstm, &cnt);
  if (ret != SQL_SUCCESS)
  {
    stored_error_num = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT,
      ((spider_db_odbc *) db_conn)->hstm, db_conn->conn, stored_error_msg);
    DBUG_RETURN(0);
  }
  DBUG_RETURN((longlong) cnt);
*/
  DBUG_RETURN(limit);
}

uint spider_db_odbc_result::num_fields()
{
  DBUG_ENTER("spider_db_odbc_result::num_fields");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(field_count);
}

void spider_db_odbc_result::move_to_pos(
  longlong pos
) {
  SQLRETURN ret;
  DBUG_ENTER("spider_db_odbc_result::move_to_pos");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider pos=%lld", pos));
  ret = SQLFetchScroll(((spider_db_odbc *) db_conn)->hstm, SQL_FETCH_FIRST,
    (SQLLEN) pos);
  if (ret != SQL_SUCCESS)
  {
    stored_error_num = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT,
      ((spider_db_odbc *) db_conn)->hstm, db_conn->conn, stored_error_msg);
  }
  DBUG_VOID_RETURN;
}

int spider_db_odbc_result::get_errno()
{
  DBUG_ENTER("spider_db_odbc_result::get_errno");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider store_err=%d", stored_error_num));
  DBUG_RETURN(stored_error_num);
}

#ifdef SPIDER_HAS_DISCOVER_TABLE_STRUCTURE
int spider_db_odbc_result::fetch_columns_for_discover_table_structure(
  spider_string *str,
  CHARSET_INFO *access_charset
) {
  DBUG_ENTER("spider_db_odbc_result::fetch_columns_for_discover_table_structure");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int spider_db_odbc_result::fetch_index_for_discover_table_structure(
  spider_string *str,
  CHARSET_INFO *access_charset
) {
  DBUG_ENTER("spider_db_odbc_result::fetch_index_for_discover_table_structure");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int spider_db_odbc_result::fetch_table_for_discover_table_structure(
  spider_string *str,
  SPIDER_SHARE *spider_share,
  CHARSET_INFO *access_charset
) {
  DBUG_ENTER("spider_db_odbc_result::fetch_table_for_discover_table_structure");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}
#endif

spider_db_odbc::spider_db_odbc(
  SPIDER_CONN *conn
) : spider_db_conn(conn), utility(&spider_db_odbc_utility),
  henv(SQL_NULL_HENV), hdbc(SQL_NULL_HDBC), hstm(SQL_NULL_HSTMT),
  lock_table_hash_inited(FALSE), handler_open_array_inited(FALSE)
{
  DBUG_ENTER("spider_db_odbc::spider_db_odbc");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

spider_db_odbc::~spider_db_odbc()
{
  DBUG_ENTER("spider_db_odbc::~spider_db_odbc");
  DBUG_PRINT("info",("spider this=%p", this));
  if (henv != SQL_NULL_HENV)
  {
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
  }
  if (handler_open_array_inited)
  {
    reset_opened_handler();
    spider_free_mem_calc(spider_current_trx,
      handler_open_array_id,
      handler_open_array.max_element *
      handler_open_array.size_of_element);
    delete_dynamic(&handler_open_array);
  }
  if (lock_table_hash_inited)
  {
    spider_free_mem_calc(spider_current_trx,
      lock_table_hash_id,
      lock_table_hash.array.max_element *
      lock_table_hash.array.size_of_element);
    my_hash_free(&lock_table_hash);
  }
  DBUG_VOID_RETURN;
}

int spider_db_odbc::init()
{
  SQLRETURN ret;
  DBUG_ENTER("spider_db_odbc::init");
  DBUG_PRINT("info",("spider this=%p", this));
  if (
    my_hash_init(&lock_table_hash, spd_charset_utf8mb3_bin, 32, 0, 0,
      (my_hash_get_key) spider_link_get_key, 0, 0)
  ) {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  spider_alloc_calc_mem_init(lock_table_hash, 303);
  spider_alloc_calc_mem(spider_current_trx,
    lock_table_hash,
    lock_table_hash.array.max_element *
    lock_table_hash.array.size_of_element);
  lock_table_hash_inited = TRUE;

  if (
    SPD_INIT_DYNAMIC_ARRAY2(&handler_open_array,
      sizeof(SPIDER_LINK_FOR_HASH *), NULL, 16, 16, MYF(MY_WME))
  ) {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  spider_alloc_calc_mem_init(handler_open_array, 304);
  spider_alloc_calc_mem(spider_current_trx,
    handler_open_array,
    handler_open_array.max_element *
    handler_open_array.size_of_element);
  handler_open_array_inited = TRUE;

  DBUG_PRINT("info",("spider call SQLAllocHandle()"));
  ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  if (ret != SQL_SUCCESS)
  {
    if (henv == SQL_NULL_HENV)
    {
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    if ((stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_ENV, henv,
      conn, stored_error_msg)))
    {
      DBUG_RETURN(stored_error);
    }
  }

  DBUG_PRINT("info",("spider call SQLSetEnvAttr()"));
  ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION,
    (SQLPOINTER) SQL_OV_ODBC3, 0);
  if (ret != SQL_SUCCESS)
  {
    if ((stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_ENV, henv,
      conn, stored_error_msg)))
    {
      SQLFreeHandle(SQL_HANDLE_ENV, henv);
      henv = SQL_NULL_HENV;
      DBUG_RETURN(stored_error);
    }
  }
  DBUG_RETURN(0);
}

void spider_db_odbc::set_limit(longlong value)
{
  DBUG_ENTER("spider_db_odbc::set_limit");
  DBUG_PRINT("info",("spider this=%p", this));
  limit = value;
  DBUG_VOID_RETURN;
}

bool spider_db_odbc::is_connected()
{
  DBUG_ENTER("spider_db_odbc::is_connected");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(hdbc != SQL_NULL_HDBC);
}

void spider_db_odbc::bg_connect()
{
  DBUG_ENTER("spider_db_odbc::bg_connect");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

int spider_db_odbc::connect(
  char *tgt_host,
  char *tgt_username,
  char *tgt_password,
  long tgt_port,
  char *tgt_socket,
  char *server_name,
  int connect_retry_count,
  longlong connect_retry_interval
) {
  SQLRETURN ret;
  SQLUINTEGER login_tmo;
  SQLUINTEGER net_tmo;
  bool use_driver;
  bool use_dir;
  bool use_dsn;
  bool use_db;
  bool use_uid;
  uint conn_str_len;
  SQLSMALLINT dummy_len;
  char *conn_str;
  char *tmp_str;
  DBUG_ENTER("spider_db_odbc::connect");
  DBUG_PRINT("info",("spider this=%p", this));
#ifndef DBUG_OFF
  {
    SQLCHAR server_name[80];
    SQLCHAR description[80];
    SQLSMALLINT srv_lgt;
    SQLSMALLINT dsc_lgt;
    ret = SQLDataSources(henv, SQL_FETCH_FIRST,
      server_name, (SQLSMALLINT) 80, &srv_lgt,
      description, (SQLSMALLINT) 80, &dsc_lgt);
    while (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
    {
      DBUG_PRINT("info",("spider server_name:%s description:%s",
        server_name, description));
      ret = SQLDataSources(henv, SQL_FETCH_NEXT,
        server_name, (SQLSMALLINT) 80, &srv_lgt,
        description, (SQLSMALLINT) 80, &dsc_lgt);
    }
  }
#endif

  ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
  if (ret != SQL_SUCCESS)
  {
    if (hdbc == SQL_NULL_HDBC)
    {
      DBUG_PRINT("info",("spider ret=%d", ret));
      if ((stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_ENV, henv,
        conn, stored_error_msg)))
      {
        goto error_alloc_handle_dbc1;
      }
    } else {
      DBUG_PRINT("info",("spider ret=%d", ret));
      if ((stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, hdbc,
        conn, stored_error_msg)))
      {
        goto error_alloc_handle_dbc2;
      }
    }
  }

  login_tmo = conn->connect_timeout;
  ret = SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT,
    (SQLPOINTER) (SQLULEN) login_tmo, SQL_IS_UINTEGER);
  if (ret != SQL_SUCCESS)
  {
    DBUG_PRINT("info",("spider ret=%d", ret));
    if ((stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, hdbc,
      conn, stored_error_msg)))
    {
      goto error_set_timeout;
    }
  }

  net_tmo = conn->net_read_timeout > conn->net_write_timeout ?
    conn->net_read_timeout : conn->net_write_timeout;
  ret = SQLSetConnectAttr(hdbc, SQL_ATTR_CONNECTION_TIMEOUT,
    (SQLPOINTER) (SQLULEN) net_tmo, SQL_IS_UINTEGER);
  if (ret != SQL_SUCCESS)
  {
    DBUG_PRINT("info",("spider ret=%d", ret));
    if ((stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, hdbc,
      conn, stored_error_msg)))
    {
      goto error_set_timeout;
    }
  }

  /* create connect string */
  use_driver = conn->tgt_default_group_length ? TRUE : FALSE;
  use_dir = conn->tgt_default_file_length ? TRUE : FALSE;
  use_dsn = conn->tgt_socket_length ? TRUE : FALSE;
  use_db = conn->tgt_db_length ? TRUE : FALSE;
  use_uid = conn->tgt_username_length ? TRUE : FALSE;
  dummy_len = 0;
  conn_str_len =
    (use_driver ?
      (SPIDER_DB_DRIVER_LEN + SPIDER_SQL_ODBC_EQUAL_LEN +
        SPIDER_SQL_OPEN_BRACE_LEN + conn->tgt_default_group_length +
        SPIDER_SQL_CLOSE_BRACE_LEN + SPIDER_SQL_SEMICOLON_LEN +
        (use_dir ?
          (SPIDER_DB_DIRECTORY_LEN + SPIDER_SQL_ODBC_EQUAL_LEN +
            conn->tgt_default_file_length + SPIDER_SQL_SEMICOLON_LEN
            ) :
          0)
        ) :
      0) +
    (use_dsn ?
      (SPIDER_DB_DSN_LEN + SPIDER_SQL_ODBC_EQUAL_LEN +
        conn->tgt_socket_length + SPIDER_SQL_SEMICOLON_LEN) : 0) +
    (use_db ?
      (SPIDER_SQL_DATABASE_LEN + SPIDER_SQL_ODBC_EQUAL_LEN +
        conn->tgt_db_length + SPIDER_SQL_COLON_LEN + conn->tgt_host_length +
        SPIDER_SQL_COLON_LEN + /* conn->tgt_port_length */ 5 +
        SPIDER_SQL_SEMICOLON_LEN) : 0) +
    (use_uid ?
      (SPIDER_SQL_UID_LEN + SPIDER_SQL_ODBC_EQUAL_LEN +
        conn->tgt_username_length + SPIDER_SQL_SEMICOLON_LEN +
        SPIDER_SQL_PWD_LEN + SPIDER_SQL_ODBC_EQUAL_LEN +
        conn->tgt_password_length + SPIDER_SQL_SEMICOLON_LEN) :
      0) +
    conn->tgt_ssl_ca_length +
    conn->tgt_ssl_capath_length +
    conn->tgt_ssl_cert_length +
    conn->tgt_ssl_cipher_length +
    conn->tgt_ssl_key_length;
  conn_str = (char *) my_alloca(conn_str_len + 1);
  if (!conn_str)
  {
    stored_error = HA_ERR_OUT_OF_MEM;
    goto error_alloc_conn_str;
  }
  tmp_str = conn_str;

  /* additional connect parameter for ODBC */
  if (conn->tgt_ssl_ca_length)
  {
    memcpy(tmp_str, conn->tgt_ssl_ca, conn->tgt_ssl_ca_length);
    tmp_str += conn->tgt_ssl_ca_length;
  }

  if (use_driver)
  {
    memcpy(tmp_str, SPIDER_DB_DRIVER_STR, SPIDER_DB_DRIVER_LEN);
    tmp_str += SPIDER_DB_DRIVER_LEN;
    memcpy(tmp_str, SPIDER_SQL_ODBC_EQUAL_STR, SPIDER_SQL_ODBC_EQUAL_LEN);
    tmp_str += SPIDER_SQL_ODBC_EQUAL_LEN;
    memcpy(tmp_str, SPIDER_SQL_OPEN_BRACE_STR, SPIDER_SQL_OPEN_BRACE_LEN);
    tmp_str += SPIDER_SQL_OPEN_BRACE_LEN;
    memcpy(tmp_str, conn->tgt_default_group, conn->tgt_default_group_length);
    tmp_str += conn->tgt_default_group_length;
    memcpy(tmp_str, SPIDER_SQL_CLOSE_BRACE_STR, SPIDER_SQL_CLOSE_BRACE_LEN);
    tmp_str += SPIDER_SQL_CLOSE_BRACE_LEN;
    memcpy(tmp_str, SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
    tmp_str += SPIDER_SQL_SEMICOLON_LEN;
    if (use_dir)
    {
      memcpy(tmp_str, SPIDER_DB_DIRECTORY_STR, SPIDER_DB_DIRECTORY_LEN);
      tmp_str += SPIDER_DB_DIRECTORY_LEN;
      memcpy(tmp_str, SPIDER_SQL_ODBC_EQUAL_STR, SPIDER_SQL_ODBC_EQUAL_LEN);
      tmp_str += SPIDER_SQL_ODBC_EQUAL_LEN;
      memcpy(tmp_str, conn->tgt_default_file, conn->tgt_default_file_length);
      tmp_str += conn->tgt_default_file_length;
      memcpy(tmp_str, SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
      tmp_str += SPIDER_SQL_SEMICOLON_LEN;
    }
  }

  /* additional connect parameter for ODBC */
  if (conn->tgt_ssl_capath_length)
  {
    memcpy(tmp_str, conn->tgt_ssl_capath, conn->tgt_ssl_capath_length);
    tmp_str += conn->tgt_ssl_capath_length;
  }

  if (use_dsn)
  {
    memcpy(tmp_str, SPIDER_DB_DSN_STR, SPIDER_DB_DSN_LEN);
    tmp_str += SPIDER_DB_DSN_LEN;
    memcpy(tmp_str, SPIDER_SQL_ODBC_EQUAL_STR, SPIDER_SQL_ODBC_EQUAL_LEN);
    tmp_str += SPIDER_SQL_ODBC_EQUAL_LEN;
    memcpy(tmp_str, conn->tgt_socket, conn->tgt_socket_length);
    tmp_str += conn->tgt_socket_length;
    memcpy(tmp_str, SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
    tmp_str += SPIDER_SQL_SEMICOLON_LEN;
  }

  /* additional connect parameter for ODBC */
  if (conn->tgt_ssl_cert_length)
  {
    memcpy(tmp_str, conn->tgt_ssl_cert, conn->tgt_ssl_cert_length);
    tmp_str += conn->tgt_ssl_cert_length;
  }

  if (use_db)
  {
    memcpy(tmp_str, SPIDER_SQL_DATABASE_STR, SPIDER_SQL_DATABASE_LEN);
    tmp_str += SPIDER_SQL_DATABASE_LEN;
    memcpy(tmp_str, SPIDER_SQL_ODBC_EQUAL_STR, SPIDER_SQL_ODBC_EQUAL_LEN);
    tmp_str += SPIDER_SQL_ODBC_EQUAL_LEN;
    memcpy(tmp_str, conn->tgt_db, conn->tgt_db_length);
    tmp_str += conn->tgt_db_length;
    memcpy(tmp_str, SPIDER_SQL_COLON_STR, SPIDER_SQL_COLON_LEN);
    tmp_str += SPIDER_SQL_COLON_LEN;
    memcpy(tmp_str, conn->tgt_host, conn->tgt_host_length);
    tmp_str += conn->tgt_host_length;
    memcpy(tmp_str, SPIDER_SQL_COLON_STR, SPIDER_SQL_COLON_LEN);
    tmp_str += SPIDER_SQL_COLON_LEN;
    my_sprintf(tmp_str, (tmp_str, "%05ld", conn->tgt_port));
    tmp_str += /* conn->tgt_port_length */ 5;
    memcpy(tmp_str, SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
    tmp_str += SPIDER_SQL_SEMICOLON_LEN;
  }

  /* additional connect parameter for ODBC */
  if (conn->tgt_ssl_cipher_length)
  {
    memcpy(tmp_str, conn->tgt_ssl_cipher, conn->tgt_ssl_cipher_length);
    tmp_str += conn->tgt_ssl_cipher_length;
  }

  if (use_uid)
  {
    memcpy(tmp_str, SPIDER_SQL_UID_STR, SPIDER_SQL_UID_LEN);
    tmp_str += SPIDER_SQL_UID_LEN;
    memcpy(tmp_str, SPIDER_SQL_ODBC_EQUAL_STR, SPIDER_SQL_ODBC_EQUAL_LEN);
    tmp_str += SPIDER_SQL_ODBC_EQUAL_LEN;
    memcpy(tmp_str, conn->tgt_username, conn->tgt_username_length);
    tmp_str += conn->tgt_username_length;
    memcpy(tmp_str, SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
    tmp_str += SPIDER_SQL_SEMICOLON_LEN;
    memcpy(tmp_str, SPIDER_SQL_PWD_STR, SPIDER_SQL_PWD_LEN);
    tmp_str += SPIDER_SQL_PWD_LEN;
    memcpy(tmp_str, SPIDER_SQL_ODBC_EQUAL_STR, SPIDER_SQL_ODBC_EQUAL_LEN);
    tmp_str += SPIDER_SQL_ODBC_EQUAL_LEN;
    memcpy(tmp_str, conn->tgt_password, conn->tgt_password_length);
    tmp_str += conn->tgt_password_length;
    memcpy(tmp_str, SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
    tmp_str += SPIDER_SQL_SEMICOLON_LEN;
  }

  /* additional connect parameter for ODBC */
  if (conn->tgt_ssl_key_length)
  {
    memcpy(tmp_str, conn->tgt_ssl_key, conn->tgt_ssl_key_length);
    tmp_str += conn->tgt_ssl_key_length;
  }
  *tmp_str = '\0';

  DBUG_PRINT("info",("spider conn_str=%s", conn_str));
  DBUG_PRINT("info",("spider conn_str_len=%u", conn_str_len));
  ret = SQLDriverConnect(hdbc, SQL_NULL_HANDLE, (SQLCHAR *) conn_str,
    (SQLSMALLINT) conn_str_len, (SQLCHAR *) NULL, dummy_len, &dummy_len,
    SQL_DRIVER_NOPROMPT);
  if (ret != SQL_SUCCESS)
  {
    DBUG_PRINT("info",("spider ret=%d", ret));
    if ((stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, hdbc,
      conn, stored_error_msg)))
    {
      goto error_connect;
    }
  }
  my_afree(conn_str);
  DBUG_RETURN(0);

error_connect:
  my_afree(conn_str);
error_alloc_conn_str:
error_set_timeout:
error_alloc_handle_dbc2:
error_alloc_handle_dbc1:
  DBUG_RETURN(stored_error);
}

int spider_db_odbc::ping()
{
  SQLRETURN ret;
  SQLUINTEGER stat;
  SQLINTEGER dummy_len;
  DBUG_ENTER("spider_db_odbc::ping");
  DBUG_PRINT("info",("spider this=%p", this));
  if (spider_param_dry_access())
  {
    DBUG_RETURN(0);
  }
  ret = SQLGetConnectAttr(hdbc, SQL_ATTR_CONNECTION_DEAD, (SQLPOINTER) &stat,
    (SQLSMALLINT) sizeof(SQLUINTEGER), &dummy_len);
  if (ret != SQL_SUCCESS)
  {
    if ((stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, hdbc,
      conn, stored_error_msg)))
    {
      DBUG_RETURN(stored_error);
    }
  }
  if (stat == SQL_CD_TRUE)
  {
    DBUG_RETURN(CR_SERVER_LOST);
  }
  DBUG_RETURN(0);
}

void spider_db_odbc::bg_disconnect()
{
  DBUG_ENTER("spider_db_odbc::bg_disconnect");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

void spider_db_odbc::disconnect()
{
  DBUG_ENTER("spider_db_odbc::disconnect");
  DBUG_PRINT("info",("spider this=%p", this));
  if (hdbc != SQL_NULL_HDBC)
  {
    SQLDisconnect(hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
    henv = SQL_NULL_HENV;
  }
  DBUG_VOID_RETURN;
}

int spider_db_odbc::set_net_timeout()
{
  SQLRETURN ret;
  DBUG_ENTER("spider_db_odbc::set_net_timeout");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider conn=%p", conn));
  SQLUINTEGER net_tmo = conn->net_read_timeout > conn->net_write_timeout ?
    conn->net_read_timeout : conn->net_write_timeout;
  ret = SQLSetConnectAttr(hdbc, SQL_ATTR_CONNECTION_TIMEOUT,
    (SQLPOINTER) (SQLULEN) net_tmo, SQL_IS_UINTEGER);
  if (ret != SQL_SUCCESS)
  {
    if ((stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, hdbc,
      conn, stored_error_msg)))
    {
      DBUG_RETURN(stored_error);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_odbc::exec_query(
  const char *query,
  uint length,
  int quick_mode
) {
  SQLRETURN ret;
  DBUG_ENTER("spider_db_odbc::exec_query");
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
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstm);
    if (ret != SQL_SUCCESS)
    {
      stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, hdbc,
        conn, stored_error_msg);
      DBUG_RETURN(stored_error);
    }
    ret = SQLSetCursorName(hstm, (SQLCHAR *) "cur", SQL_NTS);
    if (ret != SQL_SUCCESS)
    {
      stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT, hstm,
        conn, stored_error_msg);
      SQLFreeHandle(SQL_HANDLE_STMT, hstm);
      hstm = NULL;
      DBUG_RETURN(stored_error);
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

int spider_db_odbc::get_errno()
{
  DBUG_ENTER("spider_db_odbc::get_errno");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider stored_error=%d", stored_error));
  DBUG_RETURN(stored_error);
}

const char *spider_db_odbc::get_error()
{
  DBUG_ENTER("spider_db_odbc::get_error");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider error=%s", stored_error_msg));
  DBUG_RETURN(stored_error_msg);
}

bool spider_db_odbc::is_server_gone_error(
  int err
) {
  SQLRETURN ret;
  SQLUINTEGER stat;
  SQLINTEGER dummy_len;
  DBUG_ENTER("spider_db_odbc::is_server_gone_error");
  DBUG_PRINT("info",("spider this=%p", this));
  if (spider_param_dry_access())
  {
    DBUG_RETURN(FALSE);
  }
  ret = SQLGetConnectAttr(hdbc, SQL_ATTR_CONNECTION_DEAD, (SQLPOINTER) &stat,
    (SQLSMALLINT) sizeof(SQLUINTEGER), &dummy_len);
  DBUG_PRINT("info",("spider server_gone=%s",
    (ret != SQL_SUCCESS || stat == SQL_CD_TRUE) ? "TRUE" : "FALSE"));
  DBUG_RETURN(ret != SQL_SUCCESS || stat == SQL_CD_TRUE);
}

bool spider_db_odbc::is_dup_entry_error(
  int err
) {
  bool dup_entry;
  DBUG_ENTER("spider_db_odbc::is_dup_entry_error");
  DBUG_PRINT("info",("spider this=%p", this));
  dup_entry = (err == ER_DUP_ENTRY);
  DBUG_PRINT("info",("spider dup_entry=%s", dup_entry ? "TRUE" : "FALSE"));
  DBUG_RETURN(dup_entry);
}

bool spider_db_odbc::is_xa_nota_error(
  int err
) {
  bool xa_nota;
  DBUG_ENTER("spider_db_odbc::is_xa_nota_error");
  DBUG_PRINT("info",("spider this=%p", this));
  xa_nota =
    (
      err == ER_XAER_NOTA ||
      err == ER_XA_RBTIMEOUT ||
      err == ER_XA_RBDEADLOCK
    );
  DBUG_PRINT("info",("spider xa_nota=%s", xa_nota ? "TRUE" : "FALSE"));
  DBUG_RETURN(xa_nota);
}

int spider_db_odbc::print_warnings(
  struct tm *l_time
) {
  DBUG_ENTER("spider_db_odbc::print_warnings");
  DBUG_PRINT("info",("spider this=%p", this));
  /* nothing to do */
  DBUG_RETURN(0);
}

spider_db_result *spider_db_odbc::store_result(
  spider_db_result_buffer **spider_res_buf,
  st_spider_db_request_key *request_key,
  int *err
) {
  spider_db_odbc_result *result;
  DBUG_ENTER("spider_db_odbc::store_result");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(!spider_res_buf);
  if ((result = new spider_db_odbc_result(this)))
  {
    *err = 0;
    if (spider_param_dry_access() ||
      (*err = result->init()))
    {
      delete result;
      result = NULL;
    }
    result->set_limit(limit);
    result->spider = (ha_spider *) request_key->handler;
  } else {
    *err = HA_ERR_OUT_OF_MEM;
  }
  DBUG_RETURN(result);
}

spider_db_result *spider_db_odbc::use_result(
  st_spider_db_request_key *request_key,
  int *err
) {
  spider_db_odbc_result *result;
  DBUG_ENTER("spider_db_odbc::use_result");
  DBUG_PRINT("info",("spider this=%p", this));
  if ((result = new spider_db_odbc_result(this)))
  {
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

int spider_db_odbc::next_result()
{
  SQLRETURN ret;
  DBUG_ENTER("spider_db_odbc::next_result");
  DBUG_PRINT("info",("spider this=%p", this));
  ret = SQLMoreResults(hstm);
  if (ret != SQL_SUCCESS)
  {
    if (ret != SQL_NO_DATA)
    {
      DBUG_RETURN(-1);
    }
    DBUG_RETURN(spider_db_odbc_get_error(ret, SQL_HANDLE_STMT, hstm,
      conn, stored_error_msg));
  }
  DBUG_RETURN(0);
}

uint spider_db_odbc::affected_rows()
{
  SQLRETURN ret;
  SQLLEN cnt;
  DBUG_ENTER("spider_db_odbc::affected_rows");
  DBUG_PRINT("info",("spider this=%p", this));
  ret = SQLRowCount(hstm, &cnt);
  if (ret != SQL_SUCCESS)
  {
    stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT,
      hstm, conn, stored_error_msg);
    DBUG_RETURN(0);
  }
  DBUG_RETURN((uint) cnt);
}

uint spider_db_odbc::matched_rows()
{
  DBUG_ENTER("spider_db_odbc::matched_rows");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(0);
}

bool spider_db_odbc::inserted_info(
  spider_db_handler *handler,
  ha_copy_info *copy_info
) {
  DBUG_ENTER("spider_db_odbc::inserted_info");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

ulonglong spider_db_odbc::last_insert_id()
{
  DBUG_ENTER("spider_db_odbc::last_insert_id");
  DBUG_PRINT("info",("spider this=%p", this));
  /* not supported */
  DBUG_RETURN(0);
}

int spider_db_odbc::set_character_set(
  const char *csname
) {
  DBUG_ENTER("spider_db_odbc::set_character_set");
  DBUG_PRINT("info",("spider this=%p", this));
  /* nothing to do here */
  /* please add character set settion to dns file */
  DBUG_RETURN(0);
}

int spider_db_odbc::select_db(
  const char *dbname
) {
  DBUG_ENTER("spider_db_odbc::select_db");
  DBUG_PRINT("info",("spider this=%p", this));
  /* nothing to do here */
  /* please add detabase settion to dns file */
  DBUG_RETURN(0);
}

int spider_db_odbc::consistent_snapshot(
  int *need_mon
) {
  DBUG_ENTER("spider_db_odbc::consistent_snapshot");
  DBUG_PRINT("info",("spider this=%p", this));
  /* nothing to do for odbc */
  DBUG_RETURN(0);
}

bool spider_db_odbc::trx_start_in_bulk_sql()
{
  DBUG_ENTER("spider_db_odbc::trx_start_in_bulk_sql");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

int spider_db_odbc::start_transaction(
  int *need_mon
) {
  DBUG_ENTER("spider_db_odbc::start_transaction");
  DBUG_PRINT("info",("spider this=%p", this));
  /* nothing to do */
  DBUG_RETURN(0);
}

int spider_db_odbc::commit(
  int *need_mon
) {
  SQLRETURN ret;
  DBUG_ENTER("spider_db_odbc::commit");
  DBUG_PRINT("info",("spider this=%p", this));
  ret = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_COMMIT);
  if (ret != SQL_SUCCESS)
  {
    DBUG_RETURN(spider_db_odbc_get_error(ret, SQL_HANDLE_DBC,
      hdbc, conn, stored_error_msg));
  }
  DBUG_RETURN(0);
}

int spider_db_odbc::rollback(
  int *need_mon
) {
  SQLRETURN ret;
  DBUG_ENTER("spider_db_odbc::rollback");
  DBUG_PRINT("info",("spider this=%p", this));
  ret = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_ROLLBACK);
  if (ret != SQL_SUCCESS)
  {
    DBUG_RETURN(spider_db_odbc_get_error(ret, SQL_HANDLE_DBC,
      hdbc, conn, stored_error_msg));
  }
  DBUG_RETURN(0);
}

int spider_db_odbc::xa_start(
  XID *xid,
  int *need_mon
) {
#ifdef TMNOFLAGS
  RETCODE ret;
  XACALLPARAM param;
#endif
  DBUG_ENTER("spider_db_odbc::xa_start");
  DBUG_PRINT("info",("spider this=%p", this));
#ifdef TMNOFLAGS
  memset(&param, 0, sizeof(XACALLPARAM));
  param.flags = TMNOFLAGS;
  param.operation = OP_START;
  param.sizeParam = sizeof(XACALLPARAM);
  param.xid = *xid;
  ret = SQLSetConnectAttr(hdbc, SQL_COPT_SS_ENLIST_IN_XA, &param,
    SQL_IS_POINTER);
  if (ret != SQL_SUCCESS)
  {
    if ((stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, hdbc,
      conn, stored_error_msg)))
    {
      DBUG_RETURN(stored_error);
    }
  }
#else
  /* nothing to do */
#endif
  DBUG_RETURN(0);
}

bool spider_db_odbc::xa_start_in_bulk_sql()
{
  DBUG_ENTER("spider_db_odbc::xa_start_in_bulk_sql");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

int spider_db_odbc::xa_end(
  XID *xid,
  int *need_mon
) {
#ifdef TMNOFLAGS
  RETCODE ret;
  XACALLPARAM param;
#endif
  DBUG_ENTER("spider_db_odbc::xa_end");
  DBUG_PRINT("info",("spider this=%p", this));
#ifdef TMNOFLAGS
  memset(&param, 0, sizeof(XACALLPARAM));
  param.flags = TMSUCCESS;
  param.operation = OP_END;
  param.sizeParam = sizeof(XACALLPARAM);
  param.xid = *xid;
  ret = SQLSetConnectAttr(hdbc, SQL_COPT_SS_ENLIST_IN_XA, &param,
    SQL_IS_POINTER);
  if (ret != SQL_SUCCESS)
  {
    if ((stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, hdbc,
      conn, stored_error_msg)))
    {
      DBUG_RETURN(stored_error);
    }
  }
#else
  /* nothing to do */
#endif
  DBUG_RETURN(0);
}

int spider_db_odbc::xa_prepare(
  XID *xid,
  int *need_mon
) {
#ifdef TMNOFLAGS
  RETCODE ret;
  XACALLPARAM param;
#endif
  DBUG_ENTER("spider_db_odbc::xa_prepare");
  DBUG_PRINT("info",("spider this=%p", this));
#ifdef TMNOFLAGS
  memset(&param, 0, sizeof(XACALLPARAM));
  param.flags = TMNOFLAGS;
  param.operation = OP_PREPARE;
  param.sizeParam = sizeof(XACALLPARAM);
  param.xid = *xid;
  ret = SQLSetConnectAttr(hdbc, SQL_COPT_SS_ENLIST_IN_XA, &param,
    SQL_IS_POINTER);
  if (ret != SQL_SUCCESS)
  {
    if ((stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, hdbc,
      conn, stored_error_msg)))
    {
      DBUG_RETURN(stored_error);
    }
  }
#else
  /* nothing to do */
#endif
  DBUG_RETURN(0);
}

int spider_db_odbc::xa_commit(
  XID *xid,
  int *need_mon
) {
  RETCODE ret;
#ifdef TMNOFLAGS
  XACALLPARAM param;
#endif
  DBUG_ENTER("spider_db_odbc::xa_commit");
  DBUG_PRINT("info",("spider this=%p", this));
#ifdef TMNOFLAGS
  memset(&param, 0, sizeof(XACALLPARAM));
  param.flags = TMNOFLAGS;
  param.operation = OP_COMMIT;
  param.sizeParam = sizeof(XACALLPARAM);
  param.xid = *xid;
  ret = SQLSetConnectAttr(hdbc, SQL_COPT_SS_ENLIST_IN_XA, &param,
    SQL_IS_POINTER);
#else
  ret = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_COMMIT);
#endif
  if (ret != SQL_SUCCESS)
  {
    if ((stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, hdbc,
      conn, stored_error_msg)))
    {
      DBUG_RETURN(stored_error);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_odbc::xa_rollback(
  XID *xid,
  int *need_mon
) {
  RETCODE ret;
#ifdef TMNOFLAGS
  XACALLPARAM param;
#endif
  DBUG_ENTER("spider_db_odbc::xa_rollback");
  DBUG_PRINT("info",("spider this=%p", this));
#ifdef TMNOFLAGS
  memset(&param, 0, sizeof(XACALLPARAM));
  param.flags = TMNOFLAGS;
  param.operation = OP_ROLLBACK;
  param.sizeParam = sizeof(XACALLPARAM);
  param.xid = *xid;
  ret = SQLSetConnectAttr(hdbc, SQL_COPT_SS_ENLIST_IN_XA, &param,
    SQL_IS_POINTER);
#else
  ret = SQLEndTran(SQL_HANDLE_DBC, hdbc, SQL_ROLLBACK);
#endif
  if (ret != SQL_SUCCESS)
  {
    if ((stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, hdbc,
      conn, stored_error_msg)))
    {
      DBUG_RETURN(stored_error);
    }
  }
  DBUG_RETURN(0);
}

bool spider_db_odbc::set_trx_isolation_in_bulk_sql()
{
  DBUG_ENTER("spider_db_odbc::set_trx_isolation_in_bulk_sql");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

int spider_db_odbc::set_trx_isolation(
  int trx_isolation,
  int *need_mon
) {
  RETCODE ret;
  DBUG_ENTER("spider_db_odbc::set_trx_isolation");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (trx_isolation)
  {
    case ISO_READ_UNCOMMITTED:
      ret = SQLSetConnectAttr(hdbc, SQL_ATTR_TXN_ISOLATION,
        (SQLPOINTER) SQL_TXN_READ_UNCOMMITTED, SQL_IS_UINTEGER);
      break;
    case ISO_READ_COMMITTED:
      ret = SQLSetConnectAttr(hdbc, SQL_ATTR_TXN_ISOLATION,
        (SQLPOINTER) SQL_TXN_READ_COMMITTED, SQL_IS_UINTEGER);
      break;
    case ISO_REPEATABLE_READ:
      ret = SQLSetConnectAttr(hdbc, SQL_ATTR_TXN_ISOLATION,
        (SQLPOINTER) SQL_TXN_REPEATABLE_READ, SQL_IS_UINTEGER);
      break;
    case ISO_SERIALIZABLE:
      ret = SQLSetConnectAttr(hdbc, SQL_ATTR_TXN_ISOLATION,
        (SQLPOINTER) SQL_TXN_SERIALIZABLE, SQL_IS_UINTEGER);
      break;
    default:
      DBUG_RETURN(HA_ERR_UNSUPPORTED);
  }
  if (ret != SQL_SUCCESS)
  {
    DBUG_RETURN(spider_db_odbc_get_error(ret, SQL_HANDLE_DBC,
      hdbc, conn, stored_error_msg));
  }
  DBUG_RETURN(0);
}

bool spider_db_odbc::set_autocommit_in_bulk_sql()
{
  DBUG_ENTER("spider_db_odbc::set_autocommit_in_bulk_sql");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

int spider_db_odbc::set_autocommit(
  bool autocommit,
  int *need_mon
) {
  SQLRETURN ret;
  DBUG_ENTER("spider_db_odbc::set_autocommit");
  DBUG_PRINT("info",("spider this=%p", this));
  if (autocommit)
  {
    ret = SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT,
      (SQLPOINTER) SQL_AUTOCOMMIT_ON, SQL_IS_UINTEGER);
  } else {
    ret = SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT,
      (SQLPOINTER) SQL_AUTOCOMMIT_OFF, SQL_IS_UINTEGER);
  }
  if (ret != SQL_SUCCESS)
  {
    DBUG_RETURN(spider_db_odbc_get_error(ret, SQL_HANDLE_DBC,
      hdbc, conn, stored_error_msg));
  }
  DBUG_RETURN(0);
}

bool spider_db_odbc::set_sql_log_off_in_bulk_sql()
{
  DBUG_ENTER("spider_db_odbc::set_sql_log_off_in_bulk_sql");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

int spider_db_odbc::set_sql_log_off(
  bool sql_log_off,
  int *need_mon
) {
  DBUG_ENTER("spider_db_odbc::set_sql_log_off");
  DBUG_PRINT("info",("spider this=%p", this));
  /* nothing to do */
  DBUG_RETURN(0);
}

bool spider_db_odbc::set_wait_timeout_in_bulk_sql()
{
  DBUG_ENTER("spider_db_odbc::set_wait_timeout_in_bulk_sql");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

int spider_db_odbc::set_wait_timeout(
  int wait_timeout,
  int *need_mon
) {
/*
  SQLRETURN ret;
  SQLUINTEGER tot = wait_timeout;
*/
  DBUG_ENTER("spider_db_odbc::set_wait_timeout");
  DBUG_PRINT("info",("spider this=%p", this));
/*
  ret = SQLSetConnectAttr(hdbc, SQL_ATTR_CONNECTION_TIMEOUT,
    (SQLPOINTER) (SQLULEN) tot, SQL_IS_UINTEGER);
  if (ret != SQL_SUCCESS)
  {
    DBUG_RETURN(spider_db_odbc_get_error(ret, SQL_HANDLE_DBC,
      hdbc, conn, stored_error_msg));
  }
*/
  DBUG_RETURN(0);
}

bool spider_db_odbc::set_sql_mode_in_bulk_sql()
{
  DBUG_ENTER("spider_db_odbc::set_sql_mode_in_bulk_sql");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

int spider_db_odbc::set_sql_mode(
  sql_mode_t sql_mode,
  int *need_mon
) {
  DBUG_ENTER("spider_db_odbc::set_sql_mode");
  DBUG_PRINT("info",("spider this=%p", this));
  /* nothing to do */
  /* please add sqlmode settion to dns file */
  DBUG_RETURN(0);
}

bool spider_db_odbc::set_time_zone_in_bulk_sql()
{
  DBUG_ENTER("spider_db_odbc::set_time_zone_in_bulk_sql");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

int spider_db_odbc::set_time_zone(
  Time_zone *time_zone,
  int *need_mon
) {
  DBUG_ENTER("spider_db_odbc::set_time_zone");
  DBUG_PRINT("info",("spider this=%p", this));
  /* nothing to do here */
  /* please add timezone settion to dns file */
  DBUG_RETURN(0);
}

int spider_db_odbc::show_master_status(
  SPIDER_TRX *trx,
  SPIDER_SHARE *share,
  int all_link_idx,
  int *need_mon,
  TABLE *table,
  spider_string *str,
  int mode,
  SPIDER_DB_RESULT **res1,
  SPIDER_DB_RESULT **res2
) {
  DBUG_ENTER("spider_db_odbc::show_master_status");
  DBUG_PRINT("info",("spider this=%p", this));
  /* nothing to do */
  DBUG_RETURN(0);
}

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
int spider_db_odbc::append_sql(
  char *sql,
  ulong sql_length,
  st_spider_db_request_key *request_key
) {
  DBUG_ENTER("spider_db_odbc::append_sql");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc::append_open_handler(
  uint handler_id,
  const char *db_name,
  const char *table_name,
  const char *index_name,
  const char *sql,
  st_spider_db_request_key *request_key
) {
  DBUG_ENTER("spider_db_odbc::append_open_handler");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc::append_select(
  uint handler_id,
  spider_string *sql,
  SPIDER_DB_HS_STRING_REF_BUFFER *keys,
  int limit,
  int skip,
  st_spider_db_request_key *request_key
) {
  DBUG_ENTER("spider_db_odbc::append_select");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc::append_insert(
  uint handler_id,
  SPIDER_DB_HS_STRING_REF_BUFFER *upds,
  st_spider_db_request_key *request_key
) {
  DBUG_ENTER("spider_db_odbc::append_insert");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc::append_update(
  uint handler_id,
  spider_string *sql,
  SPIDER_DB_HS_STRING_REF_BUFFER *keys,
  SPIDER_DB_HS_STRING_REF_BUFFER *upds,
  int limit,
  int skip,
  bool increment,
  bool decrement,
  st_spider_db_request_key *request_key
) {
  DBUG_ENTER("spider_db_odbc::append_update");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc::append_delete(
  uint handler_id,
  spider_string *sql,
  SPIDER_DB_HS_STRING_REF_BUFFER *keys,
  int limit,
  int skip,
  st_spider_db_request_key *request_key
) {
  DBUG_ENTER("spider_db_odbc::append_delete");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

void spider_db_odbc::reset_request_queue()
{
  DBUG_ENTER("spider_db_odbc::reset_request_queue");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_VOID_RETURN;
}
#endif

size_t spider_db_odbc::escape_string(
  char *to,
  const char *from,
  size_t from_length
) {
  DBUG_ENTER("spider_db_odbc::escape_string");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(escape_quotes_for_mysql(conn->access_charset, to, 0,
    from, from_length));
}

bool spider_db_odbc::have_lock_table_list()
{
  DBUG_ENTER("spider_db_odbc::have_lock_table_list");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(lock_table_hash.records);
}

int spider_db_odbc::append_lock_tables(
  spider_string *str
) {
  int err;
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
  DBUG_ENTER("spider_db_odbc::lock_tables");
  DBUG_PRINT("info",("spider this=%p", this));
  if ((tmp_link_for_hash =
    (SPIDER_LINK_FOR_HASH *) my_hash_element(&lock_table_hash, 0)))
  {
    if ((err = utility->append_lock_table_head(str)))
    {
      DBUG_RETURN(err);
    }
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
    if ((err = utility->
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
      DBUG_RETURN(err);
    }
#ifdef HASH_UPDATE_WITH_HASH_VALUE
    my_hash_delete_with_hash_value(&lock_table_hash,
      tmp_link_for_hash->db_table_str_hash_value, (uchar*) tmp_link_for_hash);
#else
    my_hash_delete(&lock_table_hash, (uchar*) tmp_link_for_hash);
#endif
    if ((err = utility->append_lock_table_tail(str)))
    {
      DBUG_RETURN(err);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_odbc::append_unlock_tables(
  spider_string *str
) {
  int err;
  DBUG_ENTER("spider_db_odbc::append_unlock_tables");
  DBUG_PRINT("info",("spider this=%p", this));
  if ((err = utility->append_unlock_table(str)))
  {
    DBUG_RETURN(err);
  }
  DBUG_RETURN(0);
}

uint spider_db_odbc::get_lock_table_hash_count()
{
  DBUG_ENTER("spider_db_odbc::get_lock_table_hash_count");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(lock_table_hash.records);
}

void spider_db_odbc::reset_lock_table_hash()
{
  DBUG_ENTER("spider_db_odbc::reset_lock_table_hash");
  DBUG_PRINT("info",("spider this=%p", this));
  my_hash_reset(&lock_table_hash);
  DBUG_VOID_RETURN;
}

uint spider_db_odbc::get_opened_handler_count()
{
  DBUG_ENTER("spider_db_odbc::get_opened_handler_count");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(handler_open_array.elements);
}

void spider_db_odbc::reset_opened_handler()
{
  ha_spider *tmp_spider;
  int tmp_link_idx;
  SPIDER_LINK_FOR_HASH **tmp_link_for_hash;
  DBUG_ENTER("spider_db_odbc::reset_opened_handler");
  DBUG_PRINT("info",("spider this=%p", this));
  while ((tmp_link_for_hash =
    (SPIDER_LINK_FOR_HASH **) pop_dynamic(&handler_open_array)))
  {
    tmp_spider = (*tmp_link_for_hash)->spider;
    tmp_link_idx = (*tmp_link_for_hash)->link_idx;
    tmp_spider->clear_handler_opened(tmp_link_idx, conn->conn_kind);
  }
  DBUG_VOID_RETURN;
}

void spider_db_odbc::set_dup_key_idx(
  ha_spider *spider,
  int link_idx
) {
  bool found = FALSE;
  TABLE *table = spider->get_table();
  DBUG_ENTER("spider_db_odbc::set_dup_key_idx");
  DBUG_PRINT("info",("spider this=%p", this));
  spider->dup_key_idx = (uint) -1;
  for (uint i= 0; i < table->s->keys; i++)
  {
    if (table->s->key_info[i].flags & HA_NOSAME)
    {
      DBUG_PRINT("info",("spider found i:%u", i));
      if (found)
      {
        /* 2 or more unique key */
        spider->dup_key_idx = (uint) -1;
        DBUG_VOID_RETURN;
      } else {
        spider->dup_key_idx = i;
        found = TRUE;
      }
    }
  }
  DBUG_VOID_RETURN;
}

bool spider_db_odbc::cmp_request_key_to_snd(
  st_spider_db_request_key *request_key
) {
  DBUG_ENTER("spider_db_odbc::cmp_request_key_to_snd");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(TRUE);
}

spider_db_odbc_util::spider_db_odbc_util() : spider_db_util(),
  name_quote("\""), value_quote("'"), name_quote_length(1),
  value_quote_length(1)
{
  DBUG_ENTER("spider_db_odbc_util::spider_db_odbc_util");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

spider_db_odbc_util::~spider_db_odbc_util()
{
  DBUG_ENTER("spider_db_odbc_util::~spider_db_odbc_util");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

int spider_db_odbc_util::append_name(
  spider_string *str,
  const char *name,
  uint name_length
) {
  DBUG_ENTER("spider_db_odbc_util::append_name");
  str->q_append(name_quote, name_quote_length);
  str->q_append(name, name_length);
  str->q_append(name_quote, name_quote_length);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_name_with_charset(
  spider_string *str,
  const char *name,
  uint name_length,
  CHARSET_INFO *name_charset
) {
  DBUG_ENTER("spider_db_odbc_util::append_name_with_charset");
  if (str->reserve(name_quote_length * 2 + name_length * 2))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(name_quote, name_quote_length);
  str->append(name, name_length, name_charset);
  if (str->reserve(name_quote_length))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(name_quote, name_quote_length);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_escaped_name(
  spider_string *str,
  const char *name,
  uint name_length
) {
  int err;
  DBUG_ENTER("spider_db_odbc_util::append_name");
  if (str->reserve(name_quote_length * 2 + name_length * 2))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(name_quote, name_quote_length);
  if ((err = spider_db_append_name_with_quote_str_internal(
    str, name, name_length, dbton_id)))
  {
    DBUG_RETURN(err);
  }
  if (str->reserve(name_quote_length))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(name_quote, name_quote_length);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_escaped_name_with_charset(
  spider_string *str,
  const char *name,
  uint name_length,
  CHARSET_INFO *name_charset
) {
  int err;
  DBUG_ENTER("spider_db_odbc_util::append_name_with_charset");
  if (str->reserve(name_quote_length * 2 + name_length * 2))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(name_quote, name_quote_length);
  if ((err = spider_db_append_name_with_quote_str_internal(
    str, name, name_length, name_charset, dbton_id)))
  {
    DBUG_RETURN(err);
  }
  if (str->reserve(name_quote_length))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(name_quote, name_quote_length);
  DBUG_RETURN(0);
}

bool spider_db_odbc_util::is_name_quote(
  const char head_code
) {
  DBUG_ENTER("spider_db_odbc_util::is_name_quote");
  DBUG_RETURN(head_code == *name_quote);
}

int spider_db_odbc_util::append_escaped_name_quote(
  spider_string *str
) {
  DBUG_ENTER("spider_db_odbc_util::append_escaped_name_quote");
  if (str->reserve(name_quote_length * 2))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(name_quote, name_quote_length);
  str->q_append(name_quote, name_quote_length);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_column_value(
  ha_spider *spider,
  spider_string *str,
  Field *field,
  const uchar *new_ptr,
  CHARSET_INFO *access_charset
) {
  bool float_value = FALSE;
  char buf[MAX_FIELD_WIDTH];
  spider_string tmp_str(buf, MAX_FIELD_WIDTH, field->charset());
  String *ptr;
  uint length;
  THD *thd = field->table->in_use;
  Time_zone *saved_time_zone = thd->variables.time_zone;
  DBUG_ENTER("spider_db_odbc_util::append_column_value");
  tmp_str.init_calc_mem(277);

  thd->variables.time_zone = UTC;

  if (new_ptr)
  {
    if (
      field->type() == MYSQL_TYPE_BLOB ||
      field->real_type() == MYSQL_TYPE_VARCHAR
    ) {
      length = uint2korr(new_ptr);
      tmp_str.set_quick((char *) new_ptr + HA_KEY_BLOB_LENGTH, length,
        field->charset());
      ptr = tmp_str.get_str();
    } else if (field->type() == MYSQL_TYPE_GEOMETRY)
    {
/*
      uint mlength = SIZEOF_STORED_DOUBLE, lcnt;
      uchar *dest = (uchar *) buf;
      const uchar *source;
      for (lcnt = 0; lcnt < 4; lcnt++)
      {
        mlength = SIZEOF_STORED_DOUBLE;
        source = new_ptr + mlength + SIZEOF_STORED_DOUBLE * lcnt;
        while (mlength--)
          *dest++ = *--source;
      }
      tmp_str.length(SIZEOF_STORED_DOUBLE * lcnt);
*/
#ifndef DBUG_OFF
      double xmin, xmax, ymin, ymax;
/*
      float8store(buf,xmin);
      float8store(buf+8,xmax);
      float8store(buf+16,ymin);
      float8store(buf+24,ymax);
      memcpy(&xmin,new_ptr,sizeof(xmin));
      memcpy(&xmax,new_ptr + 8,sizeof(xmax));
      memcpy(&ymin,new_ptr + 16,sizeof(ymin));
      memcpy(&ymax,new_ptr + 24,sizeof(ymax));
      float8get(xmin, buf);
      float8get(xmax, buf + 8);
      float8get(ymin, buf + 16);
      float8get(ymax, buf + 24);
      DBUG_PRINT("info", ("spider geo is %f %f %f %f",
        xmin, xmax, ymin, ymax));
      DBUG_PRINT("info", ("spider geo is %.14g %.14g %.14g %.14g",
        xmin, xmax, ymin, ymax));
*/
      float8get(xmin, new_ptr);
      float8get(xmax, new_ptr + 8);
      float8get(ymin, new_ptr + 16);
      float8get(ymax, new_ptr + 24);
      DBUG_PRINT("info", ("spider geo is %f %f %f %f",
        xmin, xmax, ymin, ymax));
/*
      float8get(xmin, new_ptr + SIZEOF_STORED_DOUBLE * 4);
      float8get(xmax, new_ptr + SIZEOF_STORED_DOUBLE * 5);
      float8get(ymin, new_ptr + SIZEOF_STORED_DOUBLE * 6);
      float8get(ymax, new_ptr + SIZEOF_STORED_DOUBLE * 7);
      DBUG_PRINT("info", ("spider geo is %f %f %f %f",
        xmin, xmax, ymin, ymax));
      float8get(xmin, new_ptr + SIZEOF_STORED_DOUBLE * 8);
      float8get(xmax, new_ptr + SIZEOF_STORED_DOUBLE * 9);
      float8get(ymin, new_ptr + SIZEOF_STORED_DOUBLE * 10);
      float8get(ymax, new_ptr + SIZEOF_STORED_DOUBLE * 11);
      DBUG_PRINT("info", ("spider geo is %f %f %f %f",
        xmin, xmax, ymin, ymax));
      float8get(xmin, new_ptr + SIZEOF_STORED_DOUBLE * 12);
      float8get(xmax, new_ptr + SIZEOF_STORED_DOUBLE * 13);
      float8get(ymin, new_ptr + SIZEOF_STORED_DOUBLE * 14);
      float8get(ymax, new_ptr + SIZEOF_STORED_DOUBLE * 15);
      DBUG_PRINT("info", ("spider geo is %f %f %f %f",
        xmin, xmax, ymin, ymax));
*/
#endif
/*
      tmp_str.set_quick((char *) new_ptr, SIZEOF_STORED_DOUBLE * 4,
        &my_charset_bin);
*/
      tmp_str.length(0);
      tmp_str.q_append((char *) SPIDER_SQL_LINESTRING_HEAD_STR,
        SPIDER_SQL_LINESTRING_HEAD_LEN);
      tmp_str.q_append((char *) new_ptr, SIZEOF_STORED_DOUBLE);
      tmp_str.q_append((char *) new_ptr + SIZEOF_STORED_DOUBLE * 2,
        SIZEOF_STORED_DOUBLE);
      tmp_str.q_append((char *) new_ptr + SIZEOF_STORED_DOUBLE,
        SIZEOF_STORED_DOUBLE);
      tmp_str.q_append((char *) new_ptr + SIZEOF_STORED_DOUBLE * 3,
        SIZEOF_STORED_DOUBLE);
      ptr = tmp_str.get_str();
    } else {
      ptr = field->val_str(tmp_str.get_str(), new_ptr);
      tmp_str.mem_calc();
    }
  } else {
    if (field->type() == MYSQL_TYPE_FLOAT)
    {
      float_value = TRUE;
    } else {
      ptr = field->val_str(tmp_str.get_str());
      tmp_str.mem_calc();
    }
  }

  thd->variables.time_zone = saved_time_zone;

  DBUG_PRINT("info", ("spider field->type() is %d", field->type()));
  DBUG_PRINT("info", ("spider ptr->length() is %d", ptr->length()));
/*
  if (
    field->type() == MYSQL_TYPE_BIT ||
    (field->type() >= MYSQL_TYPE_TINY_BLOB &&
      field->type() <= MYSQL_TYPE_BLOB)
  ) {
    uchar *hex_ptr = (uchar *) ptr->ptr(), *end_ptr;
    char *str_ptr;
    DBUG_PRINT("info", ("spider HEX"));
    if (str->reserve(SPIDER_SQL_HEX_LEN + ptr->length() * 2))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_HEX_STR, SPIDER_SQL_HEX_LEN);
    str_ptr = (char *) str->ptr() + str->length();
    for (end_ptr = hex_ptr + ptr->length(); hex_ptr < end_ptr; hex_ptr++)
    {
      *str_ptr++ = spider_dig_upper[(*hex_ptr) >> 4];
      *str_ptr++ = spider_dig_upper[(*hex_ptr) & 0x0F];
    }
    str->length(str->length() + ptr->length() * 2);
  } else 
*/
  if (field->result_type() == STRING_RESULT)
  {
    DBUG_PRINT("info", ("spider STRING_RESULT"));
    /*
      ODBC does not support this
    if (str->charset() != field->charset())
    {
      if ((err = spider_db_append_charset_name_before_string(str,
        field->charset())))
      {
        DBUG_RETURN(err);
      }
    }
    */
    if (str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    if (
      field->type() == MYSQL_TYPE_VARCHAR ||
      (field->type() >= MYSQL_TYPE_ENUM &&
        field->type() <= MYSQL_TYPE_GEOMETRY)
    ) {
      DBUG_PRINT("info", ("spider append_escaped"));
      char buf2[MAX_FIELD_WIDTH];
      spider_string tmp_str2(buf2, MAX_FIELD_WIDTH, field->charset());
      tmp_str2.init_calc_mem(309);
      tmp_str2.length(0);
      if (
        tmp_str2.append(ptr->ptr(), ptr->length(), field->charset()) ||
        str->reserve(tmp_str2.length() * 2) ||
        append_escaped_util(str, tmp_str2.get_str())
      )
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    } else if (str->append(*ptr))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    if (str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  } else if (field->str_needs_quotes())
  {
    /*
      ODBC does not support this
    if (str->charset() != field->charset())
    {
      if ((err = spider_db_append_charset_name_before_string(str,
        field->charset())))
      {
        DBUG_RETURN(err);
      }
    }
    */
    if (str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN * 2 + ptr->length() * 2 + 2))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    append_escaped_util(str, ptr);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  } else if (float_value)
  {
    union { float f; uint32 i; } u;
    u.f = (float) field->val_real();
    bool minus = (u.i & 0x80000000) ? TRUE : FALSE;
    int32 e = (u.i & 0x7F800000) >> 23;
/*
    TODO:
    if(e == 255)
    {
*/
      /* NaN or Infinity */
/*
    }
    else
*/
    {
      if(e == 0)
      {
        u.i = (u.i & 0x007FFFFF);
        e = -126 - 23;
      }
      else
      {
        u.i = ((u.i & 0x007FFFFF) | 0x00800000);
        e = e - 127 - 23;
      }
      bool divide = (e < 0);
      if (divide)
      {
        e *= -1;
      }
      ulonglong a1, a2, a3;
      if (e > 126)
      {
        a1 = (1ULL << 63);
        a2 = (1ULL << 63);
        a3 = (1ULL << (e - 126));
      }
      else if (e > 63)
      {
        a1 = (1ULL << 63);
        a2 = (1ULL << (e - 63));
        a3 = 1;
      }
      else
      {
        a1 = (1ULL << e);
        a2 = 1;
        a3 = 1;
      }
      length = my_sprintf(buf, (buf, "%s%u%s%llu%s%llu%s%llu",
        minus ? "-" : "", u.i,
        divide ? " / " : " * ", a1,
        divide ? " / " : " * ", a2,
        divide ? " / " : " * ", a3));
      if (str->reserve(length))
      {
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
      str->q_append(buf, length);
    }
  } else if (str->append(*ptr))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_from_with_alias(
  spider_string *str,
  const char **table_names,
  uint *table_name_lengths,
  const char **table_aliases,
  uint *table_alias_lengths,
  uint table_count,
  int *table_name_pos,
  bool over_write
) {
  uint roop_count, length = 0;
  DBUG_ENTER("spider_db_odbc_util::append_from_with_alias");
  DBUG_PRINT("info",("spider this=%p", this));
  if (!over_write)
  {
    for (roop_count = 0; roop_count < table_count; roop_count++)
      length += table_name_lengths[roop_count] + SPIDER_SQL_SPACE_LEN +
        table_alias_lengths[roop_count] + SPIDER_SQL_COMMA_LEN;
    if (str->reserve(SPIDER_SQL_FROM_LEN + length))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
    *table_name_pos = str->length();
  }
  for (roop_count = 0; roop_count < table_count; roop_count++)
  {
    str->q_append(table_names[roop_count], table_name_lengths[roop_count]);
    str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
    str->q_append(table_aliases[roop_count], table_alias_lengths[roop_count]);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_trx_isolation(
  spider_string *str,
  int trx_isolation
) {
  DBUG_ENTER("spider_db_odbc_util::append_trx_isolation");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_autocommit(
  spider_string *str,
  bool autocommit
) {
  DBUG_ENTER("spider_db_odbc_util::append_autocommit");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_sql_log_off(
  spider_string *str,
  bool sql_log_off
) {
  DBUG_ENTER("spider_db_odbc_util::append_sql_log_off");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_wait_timeout(
  spider_string *str,
  int wait_timeout
) {
  DBUG_ENTER("spider_db_odbc_util::append_wait_timeout");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_sql_mode(
  spider_string *str,
  sql_mode_t sql_mode
) {
  DBUG_ENTER("spider_db_odbc_util::append_sql_mode");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_time_zone(
  spider_string *str,
  Time_zone *time_zone
) {
  DBUG_ENTER("spider_db_odbc_util::append_time_zone");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_start_transaction(
  spider_string *str
) {
  DBUG_ENTER("spider_db_odbc_util::append_start_transaction");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_xa_start(
  spider_string *str,
  XID *xid
) {
  DBUG_ENTER("spider_db_odbc_util::append_xa_start");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_lock_table_head(
  spider_string *str
) {
  DBUG_ENTER("spider_db_odbc_util::append_lock_table_head");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_LOCK_TABLE_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_LOCK_TABLE_STR, SPIDER_SQL_LOCK_TABLE_LEN);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_lock_table_body(
  spider_string *str,
  const char *db_name,
  uint db_name_length,
  CHARSET_INFO *db_name_charset,
  const char *table_name,
  uint table_name_length,
  CHARSET_INFO *table_name_charset,
  int lock_type
) {
  DBUG_ENTER("spider_db_odbc_util::append_lock_table_body");
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

int spider_db_odbc_util::append_lock_table_tail(
  spider_string *str
) {
  DBUG_ENTER("spider_db_odbc_util::append_lock_table_tail");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_unlock_table(
  spider_string *str
) {
  DBUG_ENTER("spider_db_odbc_util::append_unlock_table");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_UNLOCK_TABLE_LEN))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(SPIDER_SQL_UNLOCK_TABLE_STR, SPIDER_SQL_UNLOCK_TABLE_LEN);
  DBUG_RETURN(0);
}

int spider_db_odbc_util::open_item_func(
  Item_func *item_func,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length,
  bool use_fields,
  spider_fields *fields
) {
  int err;
  Item *item, **item_list = item_func->arguments();
  Field *field;
  uint roop_count, item_count = item_func->argument_count(), start_item = 0;
  const char *func_name = SPIDER_SQL_NULL_CHAR_STR,
    *separator_str = SPIDER_SQL_NULL_CHAR_STR,
    *last_str = SPIDER_SQL_NULL_CHAR_STR;
  int func_name_length = SPIDER_SQL_NULL_CHAR_LEN,
    separator_str_length = SPIDER_SQL_NULL_CHAR_LEN,
    last_str_length = SPIDER_SQL_NULL_CHAR_LEN;
  bool merge_func = FALSE;
  DBUG_ENTER("spider_db_odbc_util::open_item_func");
  if (str)
  {
    if (str->reserve(SPIDER_SQL_OPEN_PAREN_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  }
  DBUG_PRINT("info",("spider functype = %d", item_func->functype()));
  switch (item_func->functype())
  {
    case Item_func::ISNULL_FUNC:
      last_str = SPIDER_SQL_IS_NULL_STR;
      last_str_length = SPIDER_SQL_IS_NULL_LEN;
      break;
    case Item_func::ISNOTNULL_FUNC:
      last_str = SPIDER_SQL_IS_NOT_NULL_STR;
      last_str_length = SPIDER_SQL_IS_NOT_NULL_LEN;
      break;
    case Item_func::UNKNOWN_FUNC:
      func_name = (char*) item_func->func_name();
      func_name_length = strlen(func_name);
      DBUG_PRINT("info",("spider func_name = %s", func_name));
      DBUG_PRINT("info",("spider func_name_length = %d", func_name_length));
      if (func_name_length == 1 &&
        (
          !strncasecmp("+", func_name, func_name_length) ||
          !strncasecmp("-", func_name, func_name_length) ||
          !strncasecmp("*", func_name, func_name_length) ||
          !strncasecmp("/", func_name, func_name_length) ||
          !strncasecmp("%", func_name, func_name_length) ||
          !strncasecmp("&", func_name, func_name_length) ||
          !strncasecmp("|", func_name, func_name_length) ||
          !strncasecmp("^", func_name, func_name_length)
        )
      ) {
        /* no action */
        break;
      } else if (func_name_length == 2 &&
        (
          !strncasecmp("<<", func_name, func_name_length) ||
          !strncasecmp(">>", func_name, func_name_length)
        )
      ) {
        /* no action */
        break;
      } else if (func_name_length == 3)
      {
#if SPIDER_ODBC_VERSION >= 0100
        if (!strncasecmp("abs", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_ABS_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_ABS_STR,
              SPIDER_SQL_ODBC_FUNC_ABS_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("cos", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_COS_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_COS_STR,
              SPIDER_SQL_ODBC_FUNC_COS_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("cot", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_COS_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_COS_STR,
              SPIDER_SQL_ODBC_FUNC_COS_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("log", func_name, func_name_length))
        {
          if (item_count > 1)
          {
            DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
          }
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_LOG_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_LOG_STR,
              SPIDER_SQL_ODBC_FUNC_LOG_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("MOD", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_MOD_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_MOD_STR,
              SPIDER_SQL_ODBC_FUNC_MOD_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("sin", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_SIN_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_SIN_STR,
              SPIDER_SQL_ODBC_FUNC_SIN_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("tan", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_TAN_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_TAN_STR,
              SPIDER_SQL_ODBC_FUNC_TAN_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0200
        else if (!strncasecmp("pow", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_POWER_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_POWER_STR,
              SPIDER_SQL_ODBC_FUNC_POWER_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      } else if (func_name_length == 4)
      {
#if SPIDER_ODBC_VERSION >= 0100
        if (!strncasecmp("char", func_name, func_name_length))
        {
          if (item_count > 1)
          {
            DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
          }
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_CHAR_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_CHAR_STR,
              SPIDER_SQL_ODBC_FUNC_CHAR_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("left", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_LEFT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_LEFT_STR,
              SPIDER_SQL_ODBC_FUNC_LEFT_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("acos", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_ACOS_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_ACOS_STR,
              SPIDER_SQL_ODBC_FUNC_ACOS_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("asin", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_ASIN_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_ASIN_STR,
              SPIDER_SQL_ODBC_FUNC_ASIN_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("atan", func_name, func_name_length))
        {
          if (item_count > 1)
          {
#if SPIDER_ODBC_VERSION >= 0200
            if (str)
            {
              if (str->reserve(SPIDER_SQL_ODBC_FUNC_ATAN2_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_ODBC_FUNC_ATAN2_STR,
                SPIDER_SQL_ODBC_FUNC_ATAN2_LEN);
            }
#else
            DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
#endif
          } else {
            if (str)
            {
              if (str->reserve(SPIDER_SQL_ODBC_FUNC_ATAN_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_ODBC_FUNC_ATAN_STR,
                SPIDER_SQL_ODBC_FUNC_ATAN_LEN);
            }
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("pi()", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_PI_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_PI_STR,
              SPIDER_SQL_ODBC_FUNC_PI_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("rand", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_RAND_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_RAND_STR,
              SPIDER_SQL_ODBC_FUNC_RAND_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("sign", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_SIGN_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_SIGN_STR,
              SPIDER_SQL_ODBC_FUNC_SIGN_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("sqrt", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_SQRT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_SQRT_STR,
              SPIDER_SQL_ODBC_FUNC_SQRT_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("hour", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_HOUR_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_HOUR_STR,
              SPIDER_SQL_ODBC_FUNC_HOUR_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("week", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_WEEK_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_WEEK_STR,
              SPIDER_SQL_ODBC_FUNC_WEEK_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("year", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_YEAR_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_YEAR_STR,
              SPIDER_SQL_ODBC_FUNC_YEAR_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("user", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_USER_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_USER_STR,
              SPIDER_SQL_ODBC_FUNC_USER_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      } else if (func_name_length == 5)
      {
#if SPIDER_ODBC_VERSION >= 0100
        if (!strncasecmp("ascii", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_ASCII_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_ASCII_STR,
              SPIDER_SQL_ODBC_FUNC_ASCII_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("lcase", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_LCASE_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_LCASE_STR,
              SPIDER_SQL_ODBC_FUNC_LCASE_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("ltrim", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_LTRIM_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_LTRIM_STR,
              SPIDER_SQL_ODBC_FUNC_LTRIM_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("right", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_RIGHT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_RIGHT_STR,
              SPIDER_SQL_ODBC_FUNC_RIGHT_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("rtrim", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_RTRIM_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_RTRIM_STR,
              SPIDER_SQL_ODBC_FUNC_RTRIM_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("ucase", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_UCASE_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_UCASE_STR,
              SPIDER_SQL_ODBC_FUNC_UCASE_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0200
        else if (!strncasecmp("space", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_SPACE_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_SPACE_STR,
              SPIDER_SQL_ODBC_FUNC_SPACE_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("floor", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_FLOOR_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_FLOOR_STR,
              SPIDER_SQL_ODBC_FUNC_FLOOR_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0200
        else if (!strncasecmp("log10", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_LOG10_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_LOG10_STR,
              SPIDER_SQL_ODBC_FUNC_LOG10_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0200
        else if (!strncasecmp("round", func_name, func_name_length))
        {
          if (item_count < 2)
          {
            DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
          }
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_ROUND_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_ROUND_STR,
              SPIDER_SQL_ODBC_FUNC_ROUND_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("month", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_MONTH_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_MONTH_STR,
              SPIDER_SQL_ODBC_FUNC_MONTH_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      } else if (func_name_length == 6)
      {
#if SPIDER_ODBC_VERSION >= 0100
        if (!strncasecmp("concat", func_name, func_name_length))
        {
          if (item_count > 2)
          {
            DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
          }
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_CONCAT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_CONCAT_STR,
              SPIDER_SQL_ODBC_FUNC_CONCAT_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("insert", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_INSERT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_INSERT_STR,
              SPIDER_SQL_ODBC_FUNC_INSERT_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("locate", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_LOCATE_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_LOCATE_STR,
              SPIDER_SQL_ODBC_FUNC_LOCATE_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("repeat", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_REPEAT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_REPEAT_STR,
              SPIDER_SQL_ODBC_FUNC_REPEAT_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("substr", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_SUBSTRING_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_SUBSTRING_STR,
              SPIDER_SQL_ODBC_FUNC_SUBSTRING_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0300
        else if (!strncasecmp("locate", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_POSITION_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_POSITION_STR,
              SPIDER_SQL_ODBC_FUNC_POSITION_LEN);
          }
          func_name = SPIDER_SQL_ODBC_IN_STR;
          func_name_length = SPIDER_SQL_ODBC_IN_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("minute", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_MINUTE_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_MINUTE_STR,
              SPIDER_SQL_ODBC_FUNC_MINUTE_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("second", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_SECOND_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_SECOND_STR,
              SPIDER_SQL_ODBC_FUNC_SECOND_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("ifnull", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_IFNULL_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_IFNULL_STR,
              SPIDER_SQL_ODBC_FUNC_IFNULL_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      } else if (func_name_length == 7)
      {
#if SPIDER_ODBC_VERSION >= 0100
        if (!strncasecmp("replace", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_REPLACE_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_REPLACE_STR,
              SPIDER_SQL_ODBC_FUNC_REPLACE_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0200
        else if (!strncasecmp("soundex", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_SOUNDEX_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_SOUNDEX_STR,
              SPIDER_SQL_ODBC_FUNC_SOUNDEX_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("ceiling", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_CEILING_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_CEILING_STR,
              SPIDER_SQL_ODBC_FUNC_CEILING_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0200
        else if (!strncasecmp("degrees", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_DEGREES_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_DEGREES_STR,
              SPIDER_SQL_ODBC_FUNC_DEGREES_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0200
        else if (!strncasecmp("radians", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_RADIANS_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_RADIANS_STR,
              SPIDER_SQL_ODBC_FUNC_RADIANS_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0200
        else if (!strncasecmp("curdate", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_CURDATE_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_CURDATE_STR,
              SPIDER_SQL_ODBC_FUNC_CURDATE_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0200
        else if (!strncasecmp("curtime", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_CURTIME_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_CURTIME_STR,
              SPIDER_SQL_ODBC_FUNC_CURTIME_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("quarter", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_QUARTER_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_QUARTER_STR,
              SPIDER_SQL_ODBC_FUNC_QUARTER_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0200
        else if (!strncasecmp("dayname", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_DAYNAME_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_DAYNAME_STR,
              SPIDER_SQL_ODBC_FUNC_DAYNAME_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0300
        else if (!strncasecmp("extract", func_name, func_name_length))
        {
          Item_extract *item_extract =
            (Item_extract *) item_func;
          func_name = spider_db_timefunc_period_str[
            item_extract->int_type];
          if (!func_name)
          {
            DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
          }
          func_name_length = strlen(func_name);
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_EXTRACT_LEN +
              func_name_length + SPIDER_SQL_FROM_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_EXTRACT_STR,
              SPIDER_SQL_ODBC_FUNC_EXTRACT_LEN);
            str->q_append(func_name, func_name_length);
            str->q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
          }
          if ((err = spider_db_print_item_type(item_list[0], NULL,
            spider, str, alias, alias_length, dbton_id, use_fields, fields)))
            DBUG_RETURN(err);
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_CLOSE_FUNC_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
              SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
          }
          DBUG_RETURN(0);
        }
#endif
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      } else if (func_name_length == 8)
      {
#if SPIDER_ODBC_VERSION >= 0100
        if (!strncasecmp("database", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_DATABASE_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_DATABASE_STR,
              SPIDER_SQL_ODBC_FUNC_DATABASE_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0200
        else if (!strncasecmp("truncate", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_TRUNCATE_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_TRUNCATE_STR,
              SPIDER_SQL_ODBC_FUNC_TRUNCATE_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      } else if (func_name_length == 9)
      {
#if SPIDER_ODBC_VERSION >= 0100
        if (!strncasecmp("dayofweek", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_DAYOFWEEK_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_DAYOFWEEK_STR,
              SPIDER_SQL_ODBC_FUNC_DAYOFWEEK_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("dayofyear", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_DAYOFYEAR_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_DAYOFYEAR_STR,
              SPIDER_SQL_ODBC_FUNC_DAYOFYEAR_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0200
        else if (!strncasecmp("monthname", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_MONTHNAME_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_MONTHNAME_STR,
              SPIDER_SQL_ODBC_FUNC_MONTHNAME_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      } else if (func_name_length == 10)
      {
#if SPIDER_ODBC_VERSION >= 0300
        if (!strncasecmp("bit_length", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_BIT_LENGTH_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_BIT_LENGTH_STR,
              SPIDER_SQL_ODBC_FUNC_BIT_LENGTH_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      } else if (func_name_length == 11)
      {
#if SPIDER_ODBC_VERSION >= 0300
        if (!strncasecmp("char_length", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_CHAR_LENGTH_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_CHAR_LENGTH_STR,
              SPIDER_SQL_ODBC_FUNC_CHAR_LENGTH_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      } else if (func_name_length == 12)
      {
#if SPIDER_ODBC_VERSION >= 0100
        if (!strncasecmp("cast_as_char", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_CONVERT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_CONVERT_STR,
              SPIDER_SQL_ODBC_FUNC_CONVERT_LEN);
          }
          if ((err = spider_db_print_item_type(item_list[0], NULL,
            spider, str, alias, alias_length, dbton_id, use_fields, fields)))
            DBUG_RETURN(err);
          if (str)
          {
            if (str->reserve(SPIDER_SQL_COMMA_LEN +
              SPIDER_SQL_ODBC_TYPE_WLONGVARCHAR_LEN +
              SPIDER_SQL_ODBC_CLOSE_FUNC_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
            str->q_append(SPIDER_SQL_ODBC_TYPE_WLONGVARCHAR_STR,
              SPIDER_SQL_ODBC_TYPE_WLONGVARCHAR_LEN);
            str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
              SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
          }
          DBUG_RETURN(0);
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("cast_as_date", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_CONVERT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_CONVERT_STR,
              SPIDER_SQL_ODBC_FUNC_CONVERT_LEN);
          }
          if ((err = spider_db_print_item_type(item_list[0], NULL,
            spider, str, alias, alias_length, dbton_id, use_fields, fields)))
            DBUG_RETURN(err);
          if (str)
          {
            if (str->reserve(SPIDER_SQL_COMMA_LEN +
              SPIDER_SQL_ODBC_TYPE_DATE_LEN +
              SPIDER_SQL_ODBC_CLOSE_FUNC_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
            str->q_append(SPIDER_SQL_ODBC_TYPE_DATE_STR,
              SPIDER_SQL_ODBC_TYPE_DATE_LEN);
            str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
              SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
          }
          DBUG_RETURN(0);
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("cast_as_time", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_CONVERT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_CONVERT_STR,
              SPIDER_SQL_ODBC_FUNC_CONVERT_LEN);
          }
          if ((err = spider_db_print_item_type(item_list[0], NULL,
            spider, str, alias, alias_length, dbton_id, use_fields, fields)))
            DBUG_RETURN(err);
          if (str)
          {
            if (str->reserve(SPIDER_SQL_COMMA_LEN +
              SPIDER_SQL_ODBC_TYPE_TIME_LEN +
              SPIDER_SQL_ODBC_CLOSE_FUNC_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
            str->q_append(SPIDER_SQL_ODBC_TYPE_TIME_STR,
              SPIDER_SQL_ODBC_TYPE_TIME_LEN);
            str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
              SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
          }
          DBUG_RETURN(0);
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("octet_length", func_name, func_name_length))
        {
          /* octet_length() is mapped to length() */
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_LENGTH_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_LENGTH_STR,
              SPIDER_SQL_ODBC_FUNC_LENGTH_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("ltrim_oracle", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_LTRIM_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_LTRIM_STR,
              SPIDER_SQL_ODBC_FUNC_LTRIM_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("rtrim_oracle", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_RTRIM_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_RTRIM_STR,
              SPIDER_SQL_ODBC_FUNC_RTRIM_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      } else if (func_name_length == 13)
      {
#if SPIDER_ODBC_VERSION >= 0100
        if (!strncasecmp("substr_oracle", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_SUBSTRING_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_SUBSTRING_STR,
              SPIDER_SQL_ODBC_FUNC_SUBSTRING_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0200
        else if (!strncasecmp("timestampdiff", func_name, func_name_length))
        {
#ifdef ITEM_FUNC_TIMESTAMPDIFF_ARE_PUBLIC
          Item_func_timestamp_diff *item_func_timestamp_diff =
            (Item_func_timestamp_diff *) item_func;
          func_name = spider_db_timefunc_interval_str[
            item_func_timestamp_diff->int_type];
          if (!func_name)
          {
            DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
          }
          func_name_length = strlen(func_name);
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_TIMESTAMPDIFF_LEN +
              func_name_length + SPIDER_SQL_COMMA_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_TIMESTAMPDIFF_STR,
              SPIDER_SQL_ODBC_FUNC_TIMESTAMPDIFF_LEN);
            str->q_append(func_name, func_name_length);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
          }
          if ((err = spider_db_print_item_type(item_list[0], NULL,
            spider, str, alias, alias_length, dbton_id, use_fields, fields)))
            DBUG_RETURN(err);
          if (str)
          {
            if (str->reserve(SPIDER_SQL_COMMA_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
          }
          if ((err = spider_db_print_item_type(item_list[1], NULL,
            spider, str, alias, alias_length, dbton_id, use_fields, fields)))
            DBUG_RETURN(err);
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_CLOSE_FUNC_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
              SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
          }
          DBUG_RETURN(0);
#else
          DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
#endif
        }
#endif
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      } else if (func_name_length == 14)
      {
#if SPIDER_ODBC_VERSION >= 0100
        if (!strncasecmp("cast_as_binary", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_CONVERT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_CONVERT_STR,
              SPIDER_SQL_ODBC_FUNC_CONVERT_LEN);
          }
          if ((err = spider_db_print_item_type(item_list[0], NULL,
            spider, str, alias, alias_length, dbton_id, use_fields, fields)))
            DBUG_RETURN(err);
          if (str)
          {
            if (str->reserve(SPIDER_SQL_COMMA_LEN +
              SPIDER_SQL_ODBC_TYPE_BINARY_LEN +
              SPIDER_SQL_ODBC_CLOSE_FUNC_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
            str->q_append(SPIDER_SQL_ODBC_TYPE_BINARY_STR,
              SPIDER_SQL_ODBC_TYPE_BINARY_LEN);
            str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
              SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
          }
          DBUG_RETURN(0);
        }
#endif
#if SPIDER_ODBC_VERSION >= 0100
        else if (!strncasecmp("replace_oracle", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_REPLACE_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_REPLACE_STR,
              SPIDER_SQL_ODBC_FUNC_REPLACE_LEN);
          }
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      } else if (func_name_length == 16)
      {
#if SPIDER_ODBC_VERSION >= 0100
        if (!strncasecmp("cast_as_datetime", func_name, func_name_length))
        {
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_CONVERT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_CONVERT_STR,
              SPIDER_SQL_ODBC_FUNC_CONVERT_LEN);
          }
          if ((err = spider_db_print_item_type(item_list[0], NULL,
            spider, str, alias, alias_length, dbton_id, use_fields, fields)))
            DBUG_RETURN(err);
          if (str)
          {
            if (str->reserve(SPIDER_SQL_COMMA_LEN +
              SPIDER_SQL_ODBC_TYPE_TIMESTAMP_LEN +
              SPIDER_SQL_ODBC_CLOSE_FUNC_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
            str->q_append(SPIDER_SQL_ODBC_TYPE_TIMESTAMP_STR,
              SPIDER_SQL_ODBC_TYPE_TIMESTAMP_LEN);
            str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
              SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
          }
          DBUG_RETURN(0);
        }
#endif
      } else if (func_name_length == 17)
      {
#if SPIDER_ODBC_VERSION >= 0100
        if (!strncasecmp("current_timestamp",
          func_name, func_name_length))
        {
#if SPIDER_ODBC_VERSION >= 0300
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_CURRENT_TIMESTAMP_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_CURRENT_TIMESTAMP_STR,
              SPIDER_SQL_ODBC_FUNC_CURRENT_TIMESTAMP_LEN);
          }
#else
          if (item_count)
          {
            DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
          }
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_NOW_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_NOW_STR,
              SPIDER_SQL_ODBC_FUNC_NOW_LEN);
          }
#endif
          func_name = SPIDER_SQL_COMMA_STR;
          func_name_length = SPIDER_SQL_COMMA_LEN;
          separator_str = SPIDER_SQL_COMMA_STR;
          separator_str_length = SPIDER_SQL_COMMA_LEN;
          last_str = SPIDER_SQL_ODBC_CLOSE_FUNC_STR;
          last_str_length = SPIDER_SQL_ODBC_CLOSE_FUNC_LEN;
          break;
        }
#endif
#if SPIDER_ODBC_VERSION >= 0200
        else if (!strncasecmp("date_add_interval",
          func_name, func_name_length))
        {
          Item_date_add_interval *item_date_add_interval =
            (Item_date_add_interval *) item_func;
          func_name = spider_db_timefunc_interval_str[
            item_date_add_interval->int_type];
          if (!func_name)
          {
            DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
          }
          func_name_length = strlen(func_name);
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_FUNC_TIMESTAMPADD_LEN +
              func_name_length + SPIDER_SQL_COMMA_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_FUNC_TIMESTAMPADD_STR,
              SPIDER_SQL_ODBC_FUNC_TIMESTAMPADD_LEN);
            str->q_append(func_name, func_name_length);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
          }
          if ((err = spider_db_print_item_type(item_list[0], NULL,
            spider, str, alias, alias_length, dbton_id, use_fields, fields)))
            DBUG_RETURN(err);
          if (str)
          {
            if (str->reserve(SPIDER_SQL_COMMA_LEN + SPIDER_SQL_MINUS_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
            if (item_date_add_interval->date_sub_interval)
            {
              str->q_append(SPIDER_SQL_MINUS_STR, SPIDER_SQL_MINUS_LEN);
            }
          }
          if ((err = spider_db_print_item_type(item_list[1], NULL,
            spider, str, alias, alias_length, dbton_id, use_fields, fields)))
            DBUG_RETURN(err);
          if (str)
          {
            if (str->reserve(SPIDER_SQL_ODBC_CLOSE_FUNC_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
              SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
          }
          DBUG_RETURN(0);
        }
#endif
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      }
      if (str)
      {
        if (str->reserve(func_name_length + SPIDER_SQL_OPEN_PAREN_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(func_name, func_name_length);
        str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
      }
      func_name = SPIDER_SQL_COMMA_STR;
      func_name_length = SPIDER_SQL_COMMA_LEN;
      separator_str = SPIDER_SQL_COMMA_STR;
      separator_str_length = SPIDER_SQL_COMMA_LEN;
      last_str = SPIDER_SQL_CLOSE_PAREN_STR;
      last_str_length = SPIDER_SQL_CLOSE_PAREN_LEN;
      break;
    case Item_func::NOW_FUNC:
      DBUG_PRINT("info",("spider NOW_FUNC"));
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    case Item_func::CHAR_TYPECAST_FUNC:
      DBUG_PRINT("info",("spider CHAR_TYPECAST_FUNC"));
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    case Item_func::NOT_FUNC:
      DBUG_PRINT("info",("spider NOT_FUNC"));
      if (item_list[0]->type() == Item::COND_ITEM)
      {
        DBUG_PRINT("info",("spider item_list[0] is COND_ITEM"));
        Item_cond *item_cond = (Item_cond *) item_list[0];
        if (item_cond->functype() == Item_func::COND_AND_FUNC)
        {
          DBUG_PRINT("info",("spider item_cond is COND_AND_FUNC"));
          List_iterator_fast<Item> lif(*(item_cond->argument_list()));
          bool has_expr_cache_item = FALSE;
          bool has_isnotnull_func = FALSE;
          bool has_other_item = FALSE;
          while((item = lif++))
          {
#ifdef SPIDER_HAS_EXPR_CACHE_ITEM
            if (
              item->type() == Item::EXPR_CACHE_ITEM
            ) {
              DBUG_PRINT("info",("spider EXPR_CACHE_ITEM"));
              has_expr_cache_item = TRUE;
            } else
#endif
            if (
              item->type() == Item::FUNC_ITEM &&
              ((Item_func *) item)->functype() == Item_func::ISNOTNULL_FUNC
            ) {
              DBUG_PRINT("info",("spider ISNOTNULL_FUNC"));
              has_isnotnull_func = TRUE;
            } else {
              DBUG_PRINT("info",("spider has other item"));
              DBUG_PRINT("info",("spider COND type=%d", item->type()));
              has_other_item = TRUE;
            }
          }
          if (has_expr_cache_item && has_isnotnull_func && !has_other_item)
          {
            DBUG_PRINT("info",("spider NOT EXISTS skip"));
            DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
          }
        }
      }
      if (str)
      {
        func_name = (char*) item_func->func_name();
        func_name_length = strlen(func_name);
        if (str->reserve(func_name_length + SPIDER_SQL_SPACE_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(func_name, func_name_length);
        str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
      }
      break;
    case Item_func::NEG_FUNC:
      if (str)
      {
        func_name = (char*) item_func->func_name();
        func_name_length = strlen(func_name);
        if (str->reserve(func_name_length + SPIDER_SQL_SPACE_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(func_name, func_name_length);
        str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
      }
      break;
    case Item_func::IN_FUNC:
      if (((Item_func_opt_neg *) item_func)->negated)
      {
        func_name = SPIDER_SQL_NOT_IN_STR;
        func_name_length = SPIDER_SQL_NOT_IN_LEN;
        separator_str = SPIDER_SQL_COMMA_STR;
        separator_str_length = SPIDER_SQL_COMMA_LEN;
        last_str = SPIDER_SQL_CLOSE_PAREN_STR;
        last_str_length = SPIDER_SQL_CLOSE_PAREN_LEN;
      } else {
        func_name = SPIDER_SQL_IN_STR;
        func_name_length = SPIDER_SQL_IN_LEN;
        separator_str = SPIDER_SQL_COMMA_STR;
        separator_str_length = SPIDER_SQL_COMMA_LEN;
        last_str = SPIDER_SQL_CLOSE_PAREN_STR;
        last_str_length = SPIDER_SQL_CLOSE_PAREN_LEN;
      }
      break;
    case Item_func::BETWEEN:
      if (((Item_func_opt_neg *) item_func)->negated)
      {
        func_name = SPIDER_SQL_NOT_BETWEEN_STR;
        func_name_length = SPIDER_SQL_NOT_BETWEEN_LEN;
        separator_str = SPIDER_SQL_AND_STR;
        separator_str_length = SPIDER_SQL_AND_LEN;
      } else {
        func_name = (char*) item_func->func_name();
        func_name_length = strlen(func_name);
        separator_str = SPIDER_SQL_AND_STR;
        separator_str_length = SPIDER_SQL_AND_LEN;
      }
      break;
    case Item_func::UDF_FUNC:
      DBUG_PRINT("info",("spider UDF_FUNC"));
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
#ifdef MARIADB_BASE_VERSION
    case Item_func::XOR_FUNC:
#else
    case Item_func::COND_XOR_FUNC:
#endif
      if (str)
        str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
      DBUG_RETURN(
        spider_db_open_item_cond((Item_cond *) item_func, spider, str,
          alias, alias_length, dbton_id, use_fields, fields));
    case Item_func::TRIG_COND_FUNC:
      DBUG_PRINT("info",("spider TRIG_COND_FUNC"));
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    case Item_func::GUSERVAR_FUNC:
      if (str)
        str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
      if (item_func->result_type() == STRING_RESULT)
        DBUG_RETURN(spider_db_open_item_string(item_func, NULL, spider, str,
          alias, alias_length, dbton_id, use_fields, fields));
      else
        DBUG_RETURN(spider_db_open_item_int(item_func, NULL, spider, str,
          alias, alias_length, dbton_id, use_fields, fields));
    case Item_func::FT_FUNC:
      DBUG_PRINT("info",("spider FT_FUNC"));
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    case Item_func::SP_EQUALS_FUNC:
      DBUG_PRINT("info",("spider SP_EQUALS_FUNC"));
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    case Item_func::SP_DISJOINT_FUNC:
      DBUG_PRINT("info",("spider SP_DISJOINT_FUNC"));
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    case Item_func::SP_INTERSECTS_FUNC:
      DBUG_PRINT("info",("spider SP_INTERSECTS_FUNC"));
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    case Item_func::SP_TOUCHES_FUNC:
      DBUG_PRINT("info",("spider SP_TOUCHES_FUNC"));
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    case Item_func::SP_CROSSES_FUNC:
      DBUG_PRINT("info",("spider SP_CROSSES_FUNC"));
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    case Item_func::SP_WITHIN_FUNC:
      DBUG_PRINT("info",("spider SP_WITHIN_FUNC"));
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    case Item_func::SP_CONTAINS_FUNC:
      DBUG_PRINT("info",("spider SP_CONTAINS_FUNC"));
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    case Item_func::SP_OVERLAPS_FUNC:
      DBUG_PRINT("info",("spider SP_OVERLAPS_FUNC"));
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    case Item_func::EQ_FUNC:
    case Item_func::EQUAL_FUNC:
    case Item_func::NE_FUNC:
    case Item_func::LT_FUNC:
    case Item_func::LE_FUNC:
    case Item_func::GE_FUNC:
    case Item_func::GT_FUNC:
      if (str)
      {
        func_name = (char*) item_func->func_name();
        func_name_length = strlen(func_name);
      }
      break;
    case Item_func::LIKE_FUNC:
#ifdef SPIDER_LIKE_FUNC_HAS_GET_NEGATED
      if (str)
      {
         if (((Item_func_like *)item_func)->get_negated())
         {
            func_name = SPIDER_SQL_NOT_LIKE_STR;
            func_name_length = SPIDER_SQL_NOT_LIKE_LEN;
         }
         else
         {
            func_name = (char*)item_func->func_name();
            func_name_length = strlen(func_name);
         }
      }
      break;
#else
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
#endif
    default:
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
  }
  DBUG_PRINT("info",("spider func_name = %s", func_name));
  DBUG_PRINT("info",("spider func_name_length = %d", func_name_length));
  DBUG_PRINT("info",("spider separator_str = %s", separator_str));
  DBUG_PRINT("info",("spider separator_str_length = %d", separator_str_length));
  DBUG_PRINT("info",("spider last_str = %s", last_str));
  DBUG_PRINT("info",("spider last_str_length = %d", last_str_length));
  if (item_count)
  {
    /* Find the field in the list of items of the expression tree */
    field = spider_db_find_field_in_item_list(item_list,
                                              item_count, start_item,
                                              str,
                                              func_name, func_name_length);
    item_count--;
    /*
      Loop through the items of the current function expression to
      print its portion of the statement
    */
    for (roop_count = start_item; roop_count < item_count; roop_count++)
    {
      item = item_list[roop_count];
      if ((err = spider_db_print_item_type(item, field, spider, str,
        alias, alias_length, dbton_id, use_fields, fields)))
        DBUG_RETURN(err);
      if (roop_count == 1)
      {
        /* Remaining operands need to be preceded by the separator */
        func_name = separator_str;
        func_name_length = separator_str_length;
      }
      if (str)
      {
        if (str->reserve(func_name_length + SPIDER_SQL_SPACE_LEN * 2))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
        str->q_append(func_name, func_name_length);
        str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
      }
    }

    /* Print the last operand value */
    item = item_list[roop_count];
    if ((err = spider_db_print_item_type(item, field, spider, str,
      alias, alias_length, dbton_id, use_fields, fields)))
      DBUG_RETURN(err);
  }

  if (item_func->functype() == Item_func::FT_FUNC)
  {
    Item_func_match *item_func_match = (Item_func_match *)item_func;
    if (str)
    {
      if (str->reserve(SPIDER_SQL_AGAINST_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_AGAINST_STR, SPIDER_SQL_AGAINST_LEN);
    }
    item = item_list[0];
    if ((err = spider_db_print_item_type(item, NULL, spider, str,
      alias, alias_length, dbton_id, use_fields, fields)))
      DBUG_RETURN(err);
    if (str)
    {
      if (str->reserve(
        ((item_func_match->flags & FT_BOOL) ?
          SPIDER_SQL_IN_BOOLEAN_MODE_LEN : 0) +
        ((item_func_match->flags & FT_EXPAND) ?
          SPIDER_SQL_WITH_QUERY_EXPANSION_LEN : 0)
      ))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      if (item_func_match->flags & FT_BOOL)
        str->q_append(SPIDER_SQL_IN_BOOLEAN_MODE_STR,
          SPIDER_SQL_IN_BOOLEAN_MODE_LEN);
      if (item_func_match->flags & FT_EXPAND)
        str->q_append(SPIDER_SQL_WITH_QUERY_EXPANSION_STR,
          SPIDER_SQL_WITH_QUERY_EXPANSION_LEN);
    }
  } else if (item_func->functype() == Item_func::UNKNOWN_FUNC)
  {
    if (
      func_name_length == 7 &&
      !strncasecmp("convert", func_name, func_name_length)
    ) {
      if (str)
      {
        Item_func_conv_charset *item_func_conv_charset =
          (Item_func_conv_charset *)item_func;
        CHARSET_INFO *conv_charset =
          item_func_conv_charset->SPIDER_Item_func_conv_charset_conv_charset;
        uint cset_length = strlen(conv_charset->csname);
        if (str->reserve(SPIDER_SQL_USING_LEN + cset_length))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_USING_STR, SPIDER_SQL_USING_LEN);
        str->q_append(conv_charset->csname, cset_length);
      }
    }
  }
  if (str)
  {
    if (merge_func)
      str->length(str->length() - SPIDER_SQL_CLOSE_PAREN_LEN);
    if (str->reserve(last_str_length + SPIDER_SQL_CLOSE_PAREN_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(last_str, last_str_length);
    str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  }
  DBUG_RETURN(0);
}

#ifdef HANDLER_HAS_DIRECT_AGGREGATE
int spider_db_odbc_util::open_item_sum_func_internal(
  Item_sum *item_sum,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length,
  bool use_fields,
  spider_fields *fields,
  const char *func_name,
  uint func_name_length
) {
  uint roop_count, item_count = item_sum->get_arg_count();
  int err;
  Item *item, **args = item_sum->get_args();
  DBUG_ENTER("spider_db_odbc_util::open_item_sum_func_internal");
  if (str)
  {
    if (str->reserve(func_name_length))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(func_name, func_name_length);
  }
  if (item_count)
  {
    item_count--;
    for (roop_count = 0; roop_count < item_count; roop_count++)
    {
      item = args[roop_count];
      if ((err = spider_db_print_item_type(item, NULL, spider, str,
        alias, alias_length, dbton_id, use_fields, fields)))
        DBUG_RETURN(err);
      if (str)
      {
        if (str->reserve(SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      }
    }
    item = args[roop_count];
    if ((err = spider_db_print_item_type(item, NULL, spider, str,
      alias, alias_length, dbton_id, use_fields, fields)))
      DBUG_RETURN(err);
  }
  if (str)
  {
    if (str->reserve(SPIDER_SQL_ODBC_CLOSE_FUNC_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
      SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_odbc_util::open_item_sum_func(
  Item_sum *item_sum,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length,
  bool use_fields,
  spider_fields *fields
) {
  DBUG_ENTER("spider_db_odbc_util::open_item_sum_func");
  DBUG_PRINT("info",("spider Sumfunctype = %d", item_sum->sum_func()));
  switch (item_sum->sum_func())
  {
    case Item_sum::COUNT_FUNC:
      DBUG_RETURN(open_item_sum_func_internal(item_sum, spider, str,
        alias, alias_length, use_fields, fields,
        SPIDER_SQL_ODBC_COUNT_STR, SPIDER_SQL_ODBC_COUNT_LEN));
    case Item_sum::SUM_FUNC:
      DBUG_RETURN(open_item_sum_func_internal(item_sum, spider, str,
        alias, alias_length, use_fields, fields,
        SPIDER_SQL_ODBC_SUM_STR, SPIDER_SQL_ODBC_SUM_LEN));
    case Item_sum::MIN_FUNC:
      DBUG_RETURN(open_item_sum_func_internal(item_sum, spider, str,
        alias, alias_length, use_fields, fields,
        SPIDER_SQL_ODBC_MIN_STR, SPIDER_SQL_ODBC_MIN_LEN));
    case Item_sum::MAX_FUNC:
      DBUG_RETURN(open_item_sum_func_internal(item_sum, spider, str,
        alias, alias_length, use_fields, fields,
        SPIDER_SQL_ODBC_MAX_STR, SPIDER_SQL_ODBC_MAX_LEN));
    case Item_sum::COUNT_DISTINCT_FUNC:
      if (!use_fields)
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      DBUG_RETURN(open_item_sum_func_internal(item_sum, spider, str,
        alias, alias_length, use_fields, fields,
        SPIDER_SQL_ODBC_COUNT_DISTINCT_STR,
        SPIDER_SQL_ODBC_COUNT_DISTINCT_LEN));
    case Item_sum::SUM_DISTINCT_FUNC:
      if (!use_fields)
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      DBUG_RETURN(open_item_sum_func_internal(item_sum, spider, str,
        alias, alias_length, use_fields, fields,
        SPIDER_SQL_ODBC_SUM_DISTINCT_STR, SPIDER_SQL_ODBC_SUM_DISTINCT_LEN));
    case Item_sum::AVG_FUNC:
      if (!use_fields)
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      DBUG_RETURN(open_item_sum_func_internal(item_sum, spider, str,
        alias, alias_length, use_fields, fields,
        SPIDER_SQL_ODBC_AVG_STR, SPIDER_SQL_ODBC_AVG_LEN));
    case Item_sum::AVG_DISTINCT_FUNC:
      if (!use_fields)
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      DBUG_RETURN(open_item_sum_func_internal(item_sum, spider, str,
        alias, alias_length, use_fields, fields,
        SPIDER_SQL_ODBC_AVG_DISTINCT_STR, SPIDER_SQL_ODBC_AVG_DISTINCT_LEN));
    case Item_sum::STD_FUNC:
    case Item_sum::VARIANCE_FUNC:
    case Item_sum::SUM_BIT_FUNC:
    case Item_sum::UDF_SUM_FUNC:
    case Item_sum::GROUP_CONCAT_FUNC:
    default:
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
  }
  DBUG_RETURN(0);
}
#endif

int spider_db_odbc_util::append_escaped_util(
  spider_string *to,
  String *from
) {
  DBUG_ENTER("spider_db_odbc_util::append_escaped_util");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider from=%s", from->charset()->csname));
  DBUG_PRINT("info",("spider to=%s", to->charset()->csname));
  to->append_escape_string(from->ptr(), from->length());
  DBUG_RETURN(0);
}

#ifdef SPIDER_HAS_GROUP_BY_HANDLER
int spider_db_odbc_util::append_table(
  ha_spider *spider,
  spider_fields *fields,
  spider_string *str,
  TABLE_LIST *table_list,
  TABLE_LIST **used_table_list,
  uint *current_pos,
  TABLE_LIST **cond_table_list_ptr,
  bool top_down,
  bool first
) {
  DBUG_ENTER("spider_db_odbc_util::append_table");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_tables_top_down(
  ha_spider *spider,
  spider_fields *fields,
  spider_string *str,
  TABLE_LIST *table_list,
  TABLE_LIST **used_table_list,
  uint *current_pos,
  TABLE_LIST **cond_table_list_ptr
) {
  DBUG_ENTER("spider_db_odbc_util::append_tables_top_down");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_tables_top_down_check(
  TABLE_LIST *table_list,
  TABLE_LIST **used_table_list,
  uint *current_pos
) {
  DBUG_ENTER("spider_db_odbc_util::append_tables_top_down_check");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_embedding_tables(
  ha_spider *spider,
  spider_fields *fields,
  spider_string *str,
  TABLE_LIST *table_list,
  TABLE_LIST **used_table_list,
  uint *current_pos,
  TABLE_LIST **cond_table_list_ptr
) {
  DBUG_ENTER("spider_db_odbc_util::append_embedding_tables");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_from_and_tables(
  ha_spider *spider,
  spider_fields *fields,
  spider_string *str,
  TABLE_LIST *table_list,
  uint table_count
) {
  DBUG_ENTER("spider_db_odbc_util::append_from_and_tables");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(0);
}

int spider_db_odbc_util::reappend_tables(
  spider_fields *fields,
  SPIDER_LINK_IDX_CHAIN *link_idx_chain,
  spider_string *str
) {
  DBUG_ENTER("spider_db_odbc_util::reappend_tables");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_where(
  spider_string *str
) {
  DBUG_ENTER("spider_db_odbc_util::append_where");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(0);
}

int spider_db_odbc_util::append_having(
  spider_string *str
) {
  DBUG_ENTER("spider_db_odbc_util::append_having");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(0);
}
#endif

bool spider_db_odbc_util::tables_on_different_db_are_joinable()
{
  DBUG_ENTER("spider_db_odbc_util::tables_on_different_db_are_joinable");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

bool spider_db_odbc_util::socket_has_default_value()
{
  DBUG_ENTER("spider_db_odbc_util::socket_has_default_value");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

bool spider_db_odbc_util::database_has_default_value()
{
  DBUG_ENTER("spider_db_odbc_util::database_has_default_value");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

uint spider_db_odbc_util::limit_mode()
{
  DBUG_ENTER("spider_db_odbc_util::limit_mode");
  DBUG_PRINT("info",("spider this=%p", this));
  /* don't use offset limit */
  /* use cursor */
  DBUG_RETURN(1);
}

spider_odbc_share::spider_odbc_share(
  st_spider_share *share
) : spider_db_share(
  share,
  spider_db_odbc_utility.dbton_id
),
  utility(&spider_db_odbc_utility),
  table_select(NULL),
  table_select_pos(0),
  key_select(NULL),
  key_select_pos(NULL),
  key_hint(NULL),
  table_names_str(NULL),
  db_names_str(NULL),
  db_table_str(NULL),
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
  db_table_str_hash_value(NULL),
#endif
  table_nm_max_length(0),
  db_nm_max_length(0),
  column_name_str(NULL),
  same_db_table_name(TRUE),
  first_all_link_idx(-1)
{
  DBUG_ENTER("spider_odbc_share::spider_odbc_share");
  DBUG_PRINT("info",("spider this=%p", this));
  spider_alloc_calc_mem_init(mem_calc, 278);
  spider_alloc_calc_mem(spider_current_trx, mem_calc, sizeof(*this));
  DBUG_VOID_RETURN;
}

spider_odbc_share::~spider_odbc_share()
{
  DBUG_ENTER("spider_odbc_share::~spider_odbc_share");
  DBUG_PRINT("info",("spider this=%p", this));
  if (table_select)
    delete [] table_select;
  if (key_select)
    delete [] key_select;
  if (key_hint)
    delete [] key_hint;
  free_show_table_status();
  free_show_records();
  free_show_index();
  free_column_name_str();
  free_table_names_str();
  if (key_select_pos)
  {
    spider_free(spider_current_trx, key_select_pos, MYF(0));
  }
  spider_free_mem_calc(spider_current_trx, mem_calc_id, sizeof(*this));
  DBUG_VOID_RETURN;
}

int spider_odbc_share::init()
{
  int err;
  uint roop_count;
  TABLE_SHARE *table_share = spider_share->table_share;
  uint keys = table_share ? table_share->keys : 0;
  DBUG_ENTER("spider_odbc_share::init");
  DBUG_PRINT("info",("spider this=%p", this));
  if (!(key_select_pos = (int *)
    spider_bulk_alloc_mem(spider_current_trx, 279,
      __func__, __FILE__, __LINE__, MYF(MY_WME | MY_ZEROFILL),
      &key_select_pos,
        sizeof(int) * keys,
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
      &db_table_str_hash_value,
        sizeof(my_hash_value_type) * spider_share->all_link_count,
#endif
      NullS))
  ) {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }

  if (keys > 0 &&
    !(key_hint = new spider_string[keys])
  ) {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  for (roop_count = 0; roop_count < keys; roop_count++)
  {
    key_hint[roop_count].init_calc_mem(280);
    key_hint[roop_count].set_charset(spider_share->access_charset);
  }
  DBUG_PRINT("info",("spider key_hint=%p", key_hint));

  if (
    !(table_select = new spider_string[1]) ||
    (keys > 0 &&
      !(key_select = new spider_string[keys])
    ) ||
    (err = create_table_names_str()) ||
    (table_share &&
      (
        (err = create_column_name_str()) ||
        (err = convert_key_hint_str()) ||
        (err = append_show_table_status()) ||
        (err = append_show_records()) ||
        (err = append_show_index())
      )
    )
  ) {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }

  table_select->init_calc_mem(281);
  if (table_share && (err = append_table_select()))
    DBUG_RETURN(err);

  for (roop_count = 0; roop_count < keys; roop_count++)
  {
    key_select[roop_count].init_calc_mem(282);
    if ((err = append_key_select(roop_count)))
      DBUG_RETURN(err);
  }

  DBUG_RETURN(err);
}

uint spider_odbc_share::get_column_name_length(
  uint field_index
) {
  DBUG_ENTER("spider_odbc_share::get_column_name_length");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(column_name_str[field_index].length());
}

int spider_odbc_share::append_column_name(
  spider_string *str,
  uint field_index
) {
  int err;
  DBUG_ENTER("spider_odbc_share::append_column_name");
  DBUG_PRINT("info",("spider this=%p", this));
  err = utility->append_name(str,
    column_name_str[field_index].ptr(), column_name_str[field_index].length());
  DBUG_RETURN(err);
}

int spider_odbc_share::append_column_name_with_alias(
  spider_string *str,
  uint field_index,
  const char *alias,
  uint alias_length
) {
  DBUG_ENTER("spider_odbc_share::append_column_name_with_alias");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(
    alias_length +
    column_name_str[field_index].length() +
    utility->name_quote_length * 2))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(alias, alias_length);
  append_column_name(str, field_index);
  DBUG_RETURN(0);
}

int spider_odbc_share::append_table_name(
  spider_string *str,
  int all_link_idx
) {
  const char *db_nm = db_names_str[all_link_idx].ptr();
  uint db_nm_len = db_names_str[all_link_idx].length();
  const char *table_nm = table_names_str[all_link_idx].ptr();
  uint table_nm_len = table_names_str[all_link_idx].length();
  DBUG_ENTER("spider_odbc_share::append_table_name");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(db_nm_len + SPIDER_SQL_DOT_LEN + table_nm_len +
    utility->name_quote_length * 4))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  if (db_nm_len)
  {
    utility->append_name(str, db_nm, db_nm_len);
    str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  }
  utility->append_name(str, table_nm, table_nm_len);
  DBUG_RETURN(0);
}

int spider_odbc_share::append_table_name_with_adjusting(
  spider_string *str,
  int all_link_idx
) {
  const char *db_nm = db_names_str[all_link_idx].ptr();
  uint db_nm_len = db_names_str[all_link_idx].length();
  uint db_nm_max_len = db_nm_max_length;
  const char *table_nm = table_names_str[all_link_idx].ptr();
  uint table_nm_len = table_names_str[all_link_idx].length();
  uint table_nm_max_len = table_nm_max_length;
  DBUG_ENTER("spider_odbc_share::append_table_name_with_adjusting");
  DBUG_PRINT("info",("spider this=%p", this));
  if (db_nm_len)
  {
    utility->append_name(str, db_nm, db_nm_len);
    str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  }
  utility->append_name(str, table_nm, table_nm_len);
  uint length =
    db_nm_max_len - db_nm_len +
    table_nm_max_len - table_nm_len;
  memset((char *) str->ptr() + str->length(), ' ', length);
  str->length(str->length() + length);
  DBUG_RETURN(0);
}

int spider_odbc_share::append_from_with_adjusted_table_name(
  spider_string *str,
  int *table_name_pos
) {
  const char *db_nm = db_names_str[0].ptr();
  uint db_nm_len = db_names_str[0].length();
  uint db_nm_max_len = db_nm_max_length;
  const char *table_nm = table_names_str[0].ptr();
  uint table_nm_len = table_names_str[0].length();
  uint table_nm_max_len = table_nm_max_length;
  DBUG_ENTER("spider_odbc_share::append_from_with_adjusted_table_name");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_FROM_LEN + db_nm_max_length +
    SPIDER_SQL_DOT_LEN + table_nm_max_length +
    utility->name_quote_length * 4))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
  *table_name_pos = str->length();
  if (db_nm_len)
  {
    utility->append_name(str, db_nm, db_nm_len);
    str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  }
  utility->append_name(str, table_nm, table_nm_len);
  uint length =
    db_nm_max_len - db_nm_len +
    table_nm_max_len - table_nm_len;
  memset((char *) str->ptr() + str->length(), ' ', length);
  str->length(str->length() + length);
  DBUG_RETURN(0);
}

int spider_odbc_share::create_table_names_str()
{
  int err, roop_count;
  uint table_nm_len, db_nm_len;
  spider_string *str, *first_tbl_nm_str, *first_db_nm_str, *first_db_tbl_str;
  char *first_tbl_nm, *first_db_nm;
  DBUG_ENTER("spider_odbc_share::create_table_names_str");
  table_names_str = NULL;
  db_names_str = NULL;
  db_table_str = NULL;
  if (
    !(table_names_str = new spider_string[spider_share->all_link_count]) ||
    !(db_names_str = new spider_string[spider_share->all_link_count]) ||
    !(db_table_str = new spider_string[spider_share->all_link_count])
  ) {
    err = HA_ERR_OUT_OF_MEM;
    goto error;
  }

  same_db_table_name = TRUE;
  first_tbl_nm = spider_share->tgt_table_names[0];
  first_db_nm = spider_share->tgt_dbs[0];
  table_nm_len = spider_share->tgt_table_names_lengths[0];
  db_nm_len = spider_share->tgt_dbs_lengths[0];
  first_tbl_nm_str = &table_names_str[0];
  first_db_nm_str = &db_names_str[0];
  first_db_tbl_str = &db_table_str[0];
  for (roop_count = 0; roop_count < (int) spider_share->all_link_count;
    roop_count++)
  {
    table_names_str[roop_count].init_calc_mem(283);
    db_names_str[roop_count].init_calc_mem(284);
    db_table_str[roop_count].init_calc_mem(285);
    if (spider_share->sql_dbton_ids[roop_count] != dbton_id)
      continue;
    if (first_all_link_idx == -1)
      first_all_link_idx = roop_count;

    str = &table_names_str[roop_count];
    if (
      roop_count != 0 &&
      same_db_table_name &&
      spider_share->tgt_table_names_lengths[roop_count] == table_nm_len &&
      !memcmp(first_tbl_nm, spider_share->tgt_table_names[roop_count],
        table_nm_len)
    ) {
      if (str->copy(*first_tbl_nm_str))
      {
        err = HA_ERR_OUT_OF_MEM;
        goto error;
      }
    } else {
      str->set_charset(spider_share->access_charset);
      if ((err = spider_db_append_name_with_quote_str(str,
        spider_share->tgt_table_names[roop_count], dbton_id)))
        goto error;
      if (roop_count)
      {
        same_db_table_name = FALSE;
        DBUG_PRINT("info", ("spider found different table name %s",
          spider_share->tgt_table_names[roop_count]));
        if (str->length() > table_nm_max_length)
          table_nm_max_length = str->length();
      } else
        table_nm_max_length = str->length();
    }

    if (spider_share->tgt_dbs[roop_count])
    {
      str = &db_names_str[roop_count];
      if (
        roop_count != 0 &&
        same_db_table_name &&
        spider_share->tgt_dbs_lengths[roop_count] == db_nm_len &&
        !memcmp(first_db_nm, spider_share->tgt_dbs[roop_count],
          db_nm_len)
      ) {
        if (str->copy(*first_db_nm_str))
        {
          err = HA_ERR_OUT_OF_MEM;
          goto error;
        }
      } else {
        str->set_charset(spider_share->access_charset);
        if ((err = spider_db_append_name_with_quote_str(str,
          spider_share->tgt_dbs[roop_count], dbton_id)))
          goto error;
        if (roop_count)
        {
          same_db_table_name = FALSE;
          DBUG_PRINT("info", ("spider found different db name %s",
            spider_share->tgt_dbs[roop_count]));
          if (str->length() > db_nm_max_length)
            db_nm_max_length = str->length();
        } else
          db_nm_max_length = str->length();
      }

      str = &db_table_str[roop_count];
      if (
        roop_count != 0 &&
        same_db_table_name
      ) {
        if (str->copy(*first_db_tbl_str))
        {
          err = HA_ERR_OUT_OF_MEM;
          goto error;
        }
      } else {
        str->set_charset(spider_share->access_charset);
        if ((err = append_table_name(str, roop_count)))
          goto error;
      }
    } else {
      if (db_table_str[roop_count].copy(*str))
      {
        err = HA_ERR_OUT_OF_MEM;
        goto error;
      }
    }
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
    db_table_str_hash_value[roop_count] = my_calc_hash(
      &spider_open_connections, (uchar*) str->ptr(), str->length());
#endif
  }
  DBUG_RETURN(0);

error:
  if (db_table_str)
  {
    delete [] db_table_str;
    db_table_str = NULL;
  }
  if (db_names_str)
  {
    delete [] db_names_str;
    db_names_str = NULL;
  }
  if (table_names_str)
  {
    delete [] table_names_str;
    table_names_str = NULL;
  }
  DBUG_RETURN(err);
}

void spider_odbc_share::free_table_names_str()
{
  DBUG_ENTER("spider_odbc_share::free_table_names_str");
  if (db_table_str)
  {
    delete [] db_table_str;
    db_table_str = NULL;
  }
  if (db_names_str)
  {
    delete [] db_names_str;
    db_names_str = NULL;
  }
  if (table_names_str)
  {
    delete [] table_names_str;
    table_names_str = NULL;
  }
  DBUG_VOID_RETURN;
}

int spider_odbc_share::create_column_name_str()
{
  spider_string *str;
  int err;
  Field **field;
  TABLE_SHARE *table_share = spider_share->table_share;
  DBUG_ENTER("spider_odbc_share::create_column_name_str");
  if (
    table_share->fields &&
    !(column_name_str = new spider_string[table_share->fields])
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  for (field = table_share->field, str = column_name_str;
   *field; field++, str++)
  {
    str->init_calc_mem(286);
    str->set_charset(spider_share->access_charset);
    if ((err = spider_db_append_name_with_quote_str(str,
      (*field)->field_name, dbton_id)))
      goto error;
  }
  DBUG_RETURN(0);

error:
  if (column_name_str)
  {
    delete [] column_name_str;
    column_name_str = NULL;
  }
  DBUG_RETURN(err);
}

void spider_odbc_share::free_column_name_str()
{
  DBUG_ENTER("spider_odbc_share::free_column_name_str");
  if (column_name_str)
  {
    delete [] column_name_str;
    column_name_str = NULL;
  }
  DBUG_VOID_RETURN;
}

int spider_odbc_share::convert_key_hint_str()
{
  spider_string *tmp_key_hint;
  int roop_count;
  TABLE_SHARE *table_share = spider_share->table_share;
  DBUG_ENTER("spider_odbc_share::convert_key_hint_str");
  if (spider_share->access_charset->cset != system_charset_info->cset)
  {
    /* need conversion */
    for (roop_count = 0, tmp_key_hint = key_hint;
      roop_count < (int) table_share->keys; roop_count++, tmp_key_hint++)
    {
      tmp_key_hint->length(0);
      if (tmp_key_hint->append(spider_share->key_hint->ptr(),
        spider_share->key_hint->length(), system_charset_info))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
  } else {
    for (roop_count = 0, tmp_key_hint = key_hint;
      roop_count < (int) table_share->keys; roop_count++, tmp_key_hint++)
    {
      if (tmp_key_hint->copy(spider_share->key_hint[roop_count]))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
  }
  DBUG_RETURN(0);
}

int spider_odbc_share::append_show_table_status()
{
  DBUG_ENTER("spider_mysql_append_show_table_status");
  /* nothing to do */
  DBUG_RETURN(0);
}

void spider_odbc_share::free_show_table_status()
{
  DBUG_ENTER("spider_mysql_free_show_table_status");
  /* nothing to do */
  DBUG_VOID_RETURN;
}

int spider_odbc_share::append_show_records()
{
  DBUG_ENTER("spider_odbc_share::append_show_records");
  /* nothing to do */
  DBUG_RETURN(0);
}

void spider_odbc_share::free_show_records()
{
  DBUG_ENTER("spider_odbc_share::free_show_records");
  /* nothing to do */
  DBUG_VOID_RETURN;
}

int spider_odbc_share::append_show_index()
{
  DBUG_ENTER("spider_odbc_share::append_show_index");
  /* nothing to do */
  DBUG_RETURN(0);
}

void spider_odbc_share::free_show_index()
{
  DBUG_ENTER("spider_odbc_share::free_show_index");
  /* nothing to do */
  DBUG_VOID_RETURN;
}

int spider_odbc_share::append_table_select()
{
  Field **field;
  uint field_length;
  spider_string *str = table_select;
  TABLE_SHARE *table_share = spider_share->table_share;
  DBUG_ENTER("spider_odbc_share::append_table_select");

  if (!*table_share->field)
    DBUG_RETURN(0);

  for (field = table_share->field; *field; field++)
  {
    field_length = column_name_str[(*field)->field_index].length();
    if (str->reserve(field_length +
      utility->name_quote_length * 2 + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    append_column_name(str, (*field)->field_index);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(append_from_with_adjusted_table_name(str, &table_select_pos));
}

int spider_odbc_share::append_key_select(
  uint idx
) {
  KEY_PART_INFO *key_part;
  Field *field;
  uint part_num;
  uint field_length;
  spider_string *str = &key_select[idx];
  TABLE_SHARE *table_share = spider_share->table_share;
  const KEY *key_info = &table_share->key_info[idx];
  DBUG_ENTER("spider_odbc_share::append_key_select");

  if (!spider_user_defined_key_parts(key_info))
    DBUG_RETURN(0);

  for (key_part = key_info->key_part, part_num = 0;
    part_num < spider_user_defined_key_parts(key_info); key_part++, part_num++)
  {
    field = key_part->field;
    field_length = column_name_str[field->field_index].length();
    if (str->reserve(field_length +
      utility->name_quote_length * 2 + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    append_column_name(str, field->field_index);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(append_from_with_adjusted_table_name(str, &key_select_pos[idx]));
}

bool spider_odbc_share::need_change_db_table_name()
{
  DBUG_ENTER("spider_odbc_share::need_change_db_table_name");
  DBUG_RETURN(!same_db_table_name);
}

#ifdef SPIDER_HAS_DISCOVER_TABLE_STRUCTURE
int spider_odbc_share::discover_table_structure(
  SPIDER_TRX *trx,
  SPIDER_SHARE *spider_share,
  spider_string *str
) {
  DBUG_ENTER("spider_odbc_share::discover_table_structure");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}
#endif

spider_odbc_handler::spider_odbc_handler(
  ha_spider *spider,
  spider_odbc_share *db_share
) : spider_db_handler(
  spider,
  db_share
),
  utility(&spider_db_odbc_utility),
  where_pos(0),
  order_pos(0),
  limit_pos(0),
  table_name_pos(0),
  ha_read_pos(0),
  ha_next_pos(0),
  ha_where_pos(0),
  ha_limit_pos(0),
  ha_table_name_pos(0),
  insert_pos(0),
  insert_table_name_pos(0),
  upd_tmp_tbl(NULL),
  tmp_sql_pos1(0),
  tmp_sql_pos2(0),
  tmp_sql_pos3(0),
  tmp_sql_pos4(0),
  tmp_sql_pos5(0),
  reading_from_bulk_tmp_table(FALSE),
  union_table_name_pos_first(NULL),
  union_table_name_pos_current(NULL),
  odbc_share(db_share),
  link_for_hash(NULL),
  limit(0)
{
  DBUG_ENTER("spider_odbc_handler::spider_odbc_handler");
  DBUG_PRINT("info",("spider this=%p", this));
  spider_alloc_calc_mem_init(mem_calc, 292);
  spider_alloc_calc_mem(spider_current_trx, mem_calc, sizeof(*this));
  DBUG_VOID_RETURN;
}

spider_odbc_handler::~spider_odbc_handler()
{
  DBUG_ENTER("spider_odbc_handler::~spider_odbc_handler");
  DBUG_PRINT("info",("spider this=%p", this));
  while (union_table_name_pos_first)
  {
    SPIDER_INT_HLD *tmp_pos = union_table_name_pos_first;
    union_table_name_pos_first = tmp_pos->next;
    spider_free(spider_current_trx, tmp_pos, MYF(0));
  }
  if (link_for_hash)
  {
    spider_free(spider_current_trx, link_for_hash, MYF(0));
  }
  spider_free_mem_calc(spider_current_trx, mem_calc_id, sizeof(*this));
  DBUG_VOID_RETURN;
}

int spider_odbc_handler::init()
{
  uint roop_count;
  THD *thd = spider->wide_handler->trx->thd;
  st_spider_share *share = spider->share;
  int init_sql_alloc_size =
    spider_param_init_sql_alloc_size(thd, share->init_sql_alloc_size);
  TABLE *table = spider->get_table();
  DBUG_ENTER("spider_odbc_handler::init");
  DBUG_PRINT("info",("spider this=%p", this));
  sql.init_calc_mem(293);
  sql_part.init_calc_mem(294);
  sql_part2.init_calc_mem(295);
  ha_sql.init_calc_mem(296);
  insert_sql.init_calc_mem(297);
  update_sql.init_calc_mem(298);
  tmp_sql.init_calc_mem(299);
  dup_update_sql.init_calc_mem(300);
  if (
    (sql.real_alloc(init_sql_alloc_size)) ||
    (insert_sql.real_alloc(init_sql_alloc_size)) ||
    (update_sql.real_alloc(init_sql_alloc_size)) ||
    (tmp_sql.real_alloc(init_sql_alloc_size))
  ) {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  sql.set_charset(share->access_charset);
  sql_part.set_charset(share->access_charset);
  sql_part2.set_charset(share->access_charset);
  ha_sql.set_charset(share->access_charset);
  insert_sql.set_charset(share->access_charset);
  update_sql.set_charset(share->access_charset);
  tmp_sql.set_charset(share->access_charset);
  dup_update_sql.set_charset(share->access_charset);
  upd_tmp_tbl_prm.init();
  upd_tmp_tbl_prm.field_count = 1;
  if (!(link_for_hash = (SPIDER_LINK_FOR_HASH *)
    spider_bulk_alloc_mem(spider_current_trx, 301,
      __func__, __FILE__, __LINE__, MYF(MY_WME | MY_ZEROFILL),
      &link_for_hash,
        sizeof(SPIDER_LINK_FOR_HASH) * share->link_count,
      &query,
        sizeof(spider_string *) * share->link_count,
      &minimum_select_bitmap,
        table ? sizeof(uchar) * no_bytes_in_map(table->read_set) : 0,
      NullS))
  ) {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  for (roop_count = 0; roop_count < share->link_count; roop_count++)
  {
    link_for_hash[roop_count].spider = spider;
    link_for_hash[roop_count].link_idx = roop_count;
    link_for_hash[roop_count].db_table_str =
      &odbc_share->db_table_str[roop_count];
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
    link_for_hash[roop_count].db_table_str_hash_value =
      odbc_share->db_table_str_hash_value[roop_count];
#endif
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_index_hint(
  spider_string *str,
  int link_idx,
  ulong sql_type
  )
{
  DBUG_ENTER("spider_odbc_handler::append_index_hint");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_table_name_with_adjusting(
  spider_string *str,
  int link_idx,
  ulong sql_type
) {
  int err = 0;
  DBUG_ENTER("spider_odbc_handler::append_table_name_with_adjusting");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql_type == SPIDER_SQL_TYPE_HANDLER)
  {
    str->q_append(spider->m_handler_cid[link_idx], SPIDER_SQL_HANDLER_CID_LEN);
  } else {
    err = odbc_share->append_table_name_with_adjusting(str,
      spider->conn_link_idx[link_idx]);
  }
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_key_column_types(
  const key_range *start_key,
  spider_string *str
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  KEY *key_info = result_list->key_info;
  uint key_name_length, key_count;
  key_part_map full_key_part_map =
    make_prev_keypart_map(spider_user_defined_key_parts(key_info));
  key_part_map start_key_part_map;
  KEY_PART_INFO *key_part;
  Field *field;
  char tmp_buf[MAX_FIELD_WIDTH];
  spider_string tmp_str(tmp_buf, sizeof(tmp_buf), system_charset_info);
  DBUG_ENTER("spider_odbc_handler::append_key_column_types");
  DBUG_PRINT("info",("spider this=%p", this));
  tmp_str.init_calc_mem(310);

  start_key_part_map = start_key->keypart_map & full_key_part_map;
  DBUG_PRINT("info", ("spider spider_user_defined_key_parts=%u",
    spider_user_defined_key_parts(key_info)));
  DBUG_PRINT("info", ("spider full_key_part_map=%lu", full_key_part_map));
  DBUG_PRINT("info", ("spider start_key_part_map=%lu", start_key_part_map));

  if (!start_key_part_map)
    DBUG_RETURN(0);

  for (
    key_part = key_info->key_part,
    key_count = 0;
    start_key_part_map;
    start_key_part_map >>= 1,
    key_part++,
    key_count++
  ) {
    field = key_part->field;
    key_name_length = my_sprintf(tmp_buf, (tmp_buf, "c%u", key_count));
    if (str->reserve(key_name_length + SPIDER_SQL_SPACE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(tmp_buf, key_name_length);
    str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);

    if (tmp_str.ptr() != tmp_buf)
      tmp_str.set(tmp_buf, sizeof(tmp_buf), system_charset_info);
    else
      tmp_str.set_charset(system_charset_info);
    field->sql_type(*tmp_str.get_str());
    tmp_str.mem_calc();
    str->append(tmp_str);

    if (str->reserve(SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);

  DBUG_RETURN(0);
}

int spider_odbc_handler::append_key_join_columns_for_bka(
  const key_range *start_key,
  spider_string *str,
  const char **table_aliases,
  uint *table_alias_lengths
) {
  KEY *key_info = spider->result_list.key_info;
  uint length, key_name_length, key_count;
  key_part_map full_key_part_map =
    make_prev_keypart_map(spider_user_defined_key_parts(key_info));
  key_part_map start_key_part_map;
  KEY_PART_INFO *key_part;
  Field *field;
  char tmp_buf[MAX_FIELD_WIDTH];
  bool start_where = ((int) str->length() == where_pos);
  DBUG_ENTER("spider_odbc_handler::append_key_join_columns_for_bka");
  DBUG_PRINT("info",("spider this=%p", this));
  start_key_part_map = start_key->keypart_map & full_key_part_map;
  DBUG_PRINT("info", ("spider spider_user_defined_key_parts=%u",
    spider_user_defined_key_parts(key_info)));
  DBUG_PRINT("info", ("spider full_key_part_map=%lu", full_key_part_map));
  DBUG_PRINT("info", ("spider start_key_part_map=%lu", start_key_part_map));

  if (!start_key_part_map)
    DBUG_RETURN(0);

  if (start_where)
  {
    if (str->reserve(SPIDER_SQL_WHERE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_WHERE_STR, SPIDER_SQL_WHERE_LEN);
  } else {
    if (str->reserve(SPIDER_SQL_AND_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
  }

  for (
    key_part = key_info->key_part,
    key_count = 0;
    start_key_part_map;
    start_key_part_map >>= 1,
    key_part++,
    key_count++
  ) {
    field = key_part->field;
    key_name_length =
      odbc_share->column_name_str[field->field_index].length();
    length = my_sprintf(tmp_buf, (tmp_buf, "c%u", key_count));
    if (str->reserve(length + table_alias_lengths[0] + key_name_length +
      utility->name_quote_length * 2 +
      table_alias_lengths[1] + SPIDER_SQL_PF_EQUAL_LEN + SPIDER_SQL_AND_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(table_aliases[0], table_alias_lengths[0]);
    str->q_append(tmp_buf, length);
    str->q_append(SPIDER_SQL_PF_EQUAL_STR, SPIDER_SQL_PF_EQUAL_LEN);
    str->q_append(table_aliases[1], table_alias_lengths[1]);
    odbc_share->append_column_name(str, field->field_index);
    str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
  }
  str->length(str->length() - SPIDER_SQL_AND_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_tmp_table_and_sql_for_bka(
  const key_range *start_key
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::append_tmp_table_and_sql_for_bka");
  DBUG_PRINT("info",("spider this=%p", this));
  char tmp_table_name[MAX_FIELD_WIDTH * 2],
    tgt_table_name[MAX_FIELD_WIDTH * 2];
  int tmp_table_name_length;
  spider_string tgt_table_name_str(tgt_table_name, MAX_FIELD_WIDTH * 2,
    odbc_share->db_names_str[0].charset());
  const char *table_names[2], *table_aliases[2], *table_dot_aliases[2];
  uint table_name_lengths[2], table_alias_lengths[2],
    table_dot_alias_lengths[2];
  tgt_table_name_str.init_calc_mem(302);
  tgt_table_name_str.length(0);
  create_tmp_bka_table_name(tmp_table_name, &tmp_table_name_length,
    first_link_idx);
  if ((err = append_table_name_with_adjusting(&tgt_table_name_str,
    first_link_idx, SPIDER_SQL_TYPE_SELECT_SQL)))
  {
    DBUG_RETURN(err);
  }
  table_names[0] = tmp_table_name;
  table_names[1] = tgt_table_name_str.c_ptr_safe();
  table_name_lengths[0] = tmp_table_name_length;
  table_name_lengths[1] = tgt_table_name_str.length();
  table_aliases[0] = SPIDER_SQL_A_STR;
  table_aliases[1] = SPIDER_SQL_B_STR;
  table_alias_lengths[0] = SPIDER_SQL_A_LEN;
  table_alias_lengths[1] = SPIDER_SQL_B_LEN;
  table_dot_aliases[0] = SPIDER_SQL_A_DOT_STR;
  table_dot_aliases[1] = SPIDER_SQL_B_DOT_STR;
  table_dot_alias_lengths[0] = SPIDER_SQL_A_DOT_LEN;
  table_dot_alias_lengths[1] = SPIDER_SQL_B_DOT_LEN;
  if (
    (err = append_drop_tmp_bka_table(
      &tmp_sql, tmp_table_name, tmp_table_name_length,
      &tmp_sql_pos1, &tmp_sql_pos5, TRUE)) ||
    (err = append_create_tmp_bka_table(
      start_key,
      &tmp_sql, tmp_table_name,
      tmp_table_name_length,
      &tmp_sql_pos2, spider->share->table_share->table_charset)) ||
    (err = append_insert_tmp_bka_table(
      start_key,
      &tmp_sql, tmp_table_name,
      tmp_table_name_length, &tmp_sql_pos3))
  )
    DBUG_RETURN(err);
  tmp_sql_pos4 = tmp_sql.length();
  if ((err = spider_db_append_select(spider)))
    DBUG_RETURN(err);
  if (sql.reserve(SPIDER_SQL_A_DOT_LEN + SPIDER_SQL_ID_LEN +
    SPIDER_SQL_COMMA_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  sql.q_append(SPIDER_SQL_A_DOT_STR, SPIDER_SQL_A_DOT_LEN);
  sql.q_append(SPIDER_SQL_ID_STR, SPIDER_SQL_ID_LEN);
  sql.q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  if (
    (err = append_select_columns_with_alias(&sql,
      SPIDER_SQL_B_DOT_STR, SPIDER_SQL_B_DOT_LEN)) ||
    (err = utility->append_from_with_alias(&sql,
      table_names, table_name_lengths,
      table_aliases, table_alias_lengths, 2,
      &table_name_pos, FALSE))
  )
    DBUG_RETURN(err);
  if (
    odbc_share->key_hint &&
    (err = spider_db_append_hint_after_table(spider,
      &sql, &odbc_share->key_hint[spider->active_index]))
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  where_pos = sql.length();
  if (
    (err = append_key_join_columns_for_bka(
      start_key, &sql,
      table_dot_aliases, table_dot_alias_lengths)) ||
    (err = append_condition_part(
      SPIDER_SQL_B_DOT_STR, SPIDER_SQL_B_DOT_LEN,
      SPIDER_SQL_TYPE_SELECT_SQL, FALSE))
  )
    DBUG_RETURN(err);
  if (spider->result_list.direct_order_limit)
  {
    if ((err = append_key_order_for_direct_order_limit_with_alias(&sql,
      SPIDER_SQL_B_DOT_STR, SPIDER_SQL_B_DOT_LEN)))
      DBUG_RETURN(err);
  }
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  else if (spider->result_list.direct_aggregate)
  {
    if ((err =
      append_group_by(&sql, SPIDER_SQL_B_DOT_STR, SPIDER_SQL_B_DOT_LEN)))
      DBUG_RETURN(err);
  }
#endif

  DBUG_RETURN(0);
}

int spider_odbc_handler::reuse_tmp_table_and_sql_for_bka()
{
  DBUG_ENTER("spider_odbc_handler::reuse_tmp_table_and_sql_for_bka");
  DBUG_PRINT("info",("spider this=%p", this));
  tmp_sql.length(tmp_sql_pos4);
  sql.length(limit_pos);
  ha_sql.length(ha_limit_pos);
  DBUG_RETURN(0);
}

void spider_odbc_handler::create_tmp_bka_table_name(
  char *tmp_table_name,
  int *tmp_table_name_length,
  int link_idx
) {
  uint adjust_length, length;
  DBUG_ENTER("spider_odbc_handler::create_tmp_bka_table_name");
  if (spider_param_bka_table_name_type(current_thd,
    odbc_share->spider_share->
      bka_table_name_types[spider->conn_link_idx[link_idx]]) == 1)
  {
    adjust_length =
      odbc_share->db_nm_max_length -
      odbc_share->db_names_str[spider->conn_link_idx[link_idx]].length() +
      odbc_share->table_nm_max_length -
      odbc_share->table_names_str[spider->conn_link_idx[link_idx]].length();
    *tmp_table_name_length = odbc_share->db_nm_max_length +
      odbc_share->table_nm_max_length;
    memset(tmp_table_name, ' ', adjust_length);
    tmp_table_name += adjust_length;
    memcpy(tmp_table_name, odbc_share->db_names_str[link_idx].c_ptr(),
      odbc_share->db_names_str[link_idx].length());
    tmp_table_name += odbc_share->db_names_str[link_idx].length();
    length = my_sprintf(tmp_table_name, (tmp_table_name,
      "%s%s%p%s", SPIDER_SQL_DOT_STR, SPIDER_SQL_TMP_BKA_STR, spider,
      SPIDER_SQL_UNDERSCORE_STR));
    *tmp_table_name_length += length;
    tmp_table_name += length;
    memcpy(tmp_table_name,
      odbc_share->table_names_str[spider->conn_link_idx[link_idx]].c_ptr(),
      odbc_share->table_names_str[spider->conn_link_idx[link_idx]].length());
  } else {
    adjust_length =
      odbc_share->db_nm_max_length -
      odbc_share->db_names_str[spider->conn_link_idx[link_idx]].length();
    *tmp_table_name_length = odbc_share->db_nm_max_length;
    memset(tmp_table_name, ' ', adjust_length);
    tmp_table_name += adjust_length;
    memcpy(tmp_table_name, odbc_share->db_names_str[link_idx].c_ptr(),
      odbc_share->db_names_str[link_idx].length());
    tmp_table_name += odbc_share->db_names_str[link_idx].length();
    length = my_sprintf(tmp_table_name, (tmp_table_name,
      "%s%s%p", SPIDER_SQL_DOT_STR, SPIDER_SQL_TMP_BKA_STR, spider));
    *tmp_table_name_length += length;
  }
  DBUG_VOID_RETURN;
}

int spider_odbc_handler::append_create_tmp_bka_table(
  const key_range *start_key,
  spider_string *str,
  char *tmp_table_name,
  int tmp_table_name_length,
  int *db_name_pos,
  CHARSET_INFO *table_charset
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::append_create_tmp_bka_table");
  if (str->reserve(SPIDER_SQL_CREATE_TMP_LEN + tmp_table_name_length +
    SPIDER_SQL_OPEN_PAREN_LEN + SPIDER_SQL_ID_LEN + SPIDER_SQL_ID_TYPE_LEN +
    SPIDER_SQL_COMMA_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_CREATE_TMP_STR, SPIDER_SQL_CREATE_TMP_LEN);
  *db_name_pos = str->length();
  str->q_append(tmp_table_name, tmp_table_name_length);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  str->q_append(SPIDER_SQL_ID_STR, SPIDER_SQL_ID_LEN);
  str->q_append(SPIDER_SQL_ID_TYPE_STR, SPIDER_SQL_ID_TYPE_LEN);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  if ((err = append_key_column_types(start_key, str)))
    DBUG_RETURN(err);
  if (str->reserve(SPIDER_SQL_SEMICOLON_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_drop_tmp_bka_table(
  spider_string *str,
  char *tmp_table_name,
  int tmp_table_name_length,
  int *db_name_pos,
  int *drop_table_end_pos,
  bool with_semicolon
) {
  DBUG_ENTER("spider_odbc_handler::append_drop_tmp_bka_table");
  if (str->reserve(SPIDER_SQL_DROP_TMP_LEN + tmp_table_name_length +
    (with_semicolon ? SPIDER_SQL_SEMICOLON_LEN : 0)))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_DROP_TMP_STR, SPIDER_SQL_DROP_TMP_LEN);
  *db_name_pos = str->length();
  str->q_append(tmp_table_name, tmp_table_name_length);
  *drop_table_end_pos = str->length();
  if (with_semicolon)
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_insert_tmp_bka_table(
  const key_range *start_key,
  spider_string *str,
  char *tmp_table_name,
  int tmp_table_name_length,
  int *db_name_pos
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::append_insert_tmp_bka_table");
  if (str->reserve(SPIDER_SQL_INSERT_LEN + SPIDER_SQL_INTO_LEN +
    tmp_table_name_length + SPIDER_SQL_OPEN_PAREN_LEN + SPIDER_SQL_ID_LEN +
    SPIDER_SQL_COMMA_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_INSERT_STR, SPIDER_SQL_INSERT_LEN);
  str->q_append(SPIDER_SQL_INTO_STR, SPIDER_SQL_INTO_LEN);
  *db_name_pos = str->length();
  str->q_append(tmp_table_name, tmp_table_name_length);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  str->q_append(SPIDER_SQL_ID_STR, SPIDER_SQL_ID_LEN);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  if ((err = spider_db_append_key_columns(start_key, spider, str)))
    DBUG_RETURN(err);
  if (str->reserve(SPIDER_SQL_CLOSE_PAREN_LEN + SPIDER_SQL_VALUES_LEN +
    SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  str->q_append(SPIDER_SQL_VALUES_STR, SPIDER_SQL_VALUES_LEN);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_union_table_and_sql_for_bka(
  const key_range *start_key
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::append_union_table_and_sql_for_bka");
  DBUG_PRINT("info",("spider this=%p", this));
  char tgt_table_name[MAX_FIELD_WIDTH * 2];
  spider_string tgt_table_name_str(tgt_table_name, MAX_FIELD_WIDTH * 2,
    odbc_share->db_names_str[0].charset());
  const char *table_names[2], *table_aliases[2], *table_dot_aliases[2];
  uint table_name_lengths[2], table_alias_lengths[2],
    table_dot_alias_lengths[2];
  tgt_table_name_str.init_calc_mem(311);
  tgt_table_name_str.length(0);
  if ((err = append_table_name_with_adjusting(&tgt_table_name_str,
    first_link_idx, SPIDER_SQL_TYPE_SELECT_SQL)))
  {
    DBUG_RETURN(err);
  }
  table_names[0] = "";
  table_names[1] = tgt_table_name_str.c_ptr_safe();
  table_name_lengths[0] = 0;
  table_name_lengths[1] = tgt_table_name_str.length();
  table_aliases[0] = SPIDER_SQL_A_STR;
  table_aliases[1] = SPIDER_SQL_B_STR;
  table_alias_lengths[0] = SPIDER_SQL_A_LEN;
  table_alias_lengths[1] = SPIDER_SQL_B_LEN;
  table_dot_aliases[0] = SPIDER_SQL_A_DOT_STR;
  table_dot_aliases[1] = SPIDER_SQL_B_DOT_STR;
  table_dot_alias_lengths[0] = SPIDER_SQL_A_DOT_LEN;
  table_dot_alias_lengths[1] = SPIDER_SQL_B_DOT_LEN;

  if ((err = spider_db_append_select(spider)))
    DBUG_RETURN(err);
  if (sql.reserve(SPIDER_SQL_A_DOT_LEN + SPIDER_SQL_ID_LEN +
    SPIDER_SQL_COMMA_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  sql.q_append(SPIDER_SQL_A_DOT_STR, SPIDER_SQL_A_DOT_LEN);
  sql.q_append(SPIDER_SQL_ID_STR, SPIDER_SQL_ID_LEN);
  sql.q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  if ((err = append_select_columns_with_alias(&sql,
    SPIDER_SQL_B_DOT_STR, SPIDER_SQL_B_DOT_LEN)))
    DBUG_RETURN(err);
  if (sql.reserve(SPIDER_SQL_FROM_LEN + (SPIDER_SQL_OPEN_PAREN_LEN * 2)))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  sql.q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
  sql.q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  sql.q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  tmp_sql_pos1 = sql.length();

  if (
    (err = utility->append_from_with_alias(&tmp_sql,
      table_names, table_name_lengths,
      table_aliases, table_alias_lengths, 2,
      &table_name_pos, FALSE))
  )
    DBUG_RETURN(err);
  if (
    odbc_share->key_hint &&
    (err = spider_db_append_hint_after_table(spider,
      &tmp_sql, &odbc_share->key_hint[spider->active_index]))
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  where_pos = tmp_sql.length();
  if (
    (err = append_key_join_columns_for_bka(
      start_key, &tmp_sql,
      table_dot_aliases, table_dot_alias_lengths)) ||
    (err = append_condition_part(
      SPIDER_SQL_B_DOT_STR, SPIDER_SQL_B_DOT_LEN,
      SPIDER_SQL_TYPE_TMP_SQL, FALSE))
  )
    DBUG_RETURN(err);
  if (spider->result_list.direct_order_limit)
  {
    if ((err =
      append_key_order_for_direct_order_limit_with_alias(&tmp_sql,
        SPIDER_SQL_B_DOT_STR, SPIDER_SQL_B_DOT_LEN))
    )
      DBUG_RETURN(err);
  }
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  else if (spider->result_list.direct_aggregate)
  {
    if ((err =
      append_group_by(&tmp_sql, SPIDER_SQL_B_DOT_STR, SPIDER_SQL_B_DOT_LEN)))
      DBUG_RETURN(err);
  }
#endif

  DBUG_RETURN(0);
}

int spider_odbc_handler::reuse_union_table_and_sql_for_bka()
{
  DBUG_ENTER("spider_odbc_handler::reuse_union_table_and_sql_for_bka");
  DBUG_PRINT("info",("spider this=%p", this));
  sql.length(tmp_sql_pos1);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_insert_for_recovery(
  ulong sql_type,
  int link_idx
) {
  const TABLE *table = spider->get_table();
  SPIDER_SHARE *share = spider->share;
  Field **field;
  uint field_name_length = 0;
  bool add_value = FALSE;
  spider_string *insert_sql;
  DBUG_ENTER("spider_odbc_handler::append_insert_for_recovery");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql_type == SPIDER_SQL_TYPE_INSERT_SQL)
  {
    insert_sql = &spider->result_list.insert_sqls[link_idx];
    insert_sql->length(0);
  } else {
    insert_sql = &spider->result_list.update_sqls[link_idx];
  }
  if (insert_sql->reserve(
    SPIDER_SQL_INSERT_LEN + SPIDER_SQL_SQL_IGNORE_LEN +
    SPIDER_SQL_INTO_LEN + odbc_share->db_nm_max_length +
    SPIDER_SQL_DOT_LEN + odbc_share->table_nm_max_length +
    utility->name_quote_length * 4 + SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  insert_sql->q_append(SPIDER_SQL_INSERT_STR, SPIDER_SQL_INSERT_LEN);
  insert_sql->q_append(SPIDER_SQL_SQL_IGNORE_STR, SPIDER_SQL_SQL_IGNORE_LEN);
  insert_sql->q_append(SPIDER_SQL_INTO_STR, SPIDER_SQL_INTO_LEN);
  odbc_share->append_table_name(insert_sql, spider->conn_link_idx[link_idx]);
  insert_sql->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  for (field = table->field; *field; field++)
  {
    field_name_length =
      odbc_share->column_name_str[(*field)->field_index].length();
    if (insert_sql->reserve(field_name_length +
      utility->name_quote_length * 2 + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    odbc_share->append_column_name(insert_sql, (*field)->field_index);
    insert_sql->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  if (field_name_length)
    insert_sql->length(insert_sql->length() - SPIDER_SQL_COMMA_LEN);
  if (insert_sql->reserve(SPIDER_SQL_CLOSE_PAREN_LEN + SPIDER_SQL_VALUES_LEN +
    SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  insert_sql->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  insert_sql->q_append(SPIDER_SQL_VALUES_STR, SPIDER_SQL_VALUES_LEN);
  insert_sql->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  for (field = table->field; *field; field++)
  {
    add_value = TRUE;
    if ((*field)->is_null())
    {
      if (insert_sql->reserve(SPIDER_SQL_NULL_LEN + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      insert_sql->q_append(SPIDER_SQL_NULL_STR, SPIDER_SQL_NULL_LEN);
    } else {
      if (
        utility->append_column_value(spider, insert_sql, *field, NULL,
          share->access_charset) ||
        insert_sql->reserve(SPIDER_SQL_COMMA_LEN)
      )
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    insert_sql->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  if (add_value)
    insert_sql->length(insert_sql->length() - SPIDER_SQL_COMMA_LEN);
  if (insert_sql->reserve(SPIDER_SQL_CLOSE_PAREN_LEN, SPIDER_SQL_COMMA_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  insert_sql->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  if (sql_type == SPIDER_SQL_TYPE_INSERT_SQL)
  {
    exec_insert_sql = insert_sql;
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_update(
  const TABLE *table,
  my_ptrdiff_t ptr_diff
) {
  int err;
  spider_string *str = &update_sql;
  DBUG_ENTER("spider_odbc_handler::append_update");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->length() > 0)
  {
    if (str->reserve(SPIDER_SQL_SEMICOLON_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  }

  if (
    (err = append_update(str, 0)) ||
    (err = append_update_set(str)) ||
    (err = append_update_where(str, table, ptr_diff))
  )
    DBUG_RETURN(err);
  filled_up = (str->length() >= (uint) spider->result_list.bulk_update_size);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_update(
  const TABLE *table,
  my_ptrdiff_t ptr_diff,
  int link_idx
) {
  int err;
  SPIDER_SHARE *share = spider->share;
  spider_string *str = &spider->result_list.update_sqls[link_idx];
  DBUG_ENTER("spider_odbc_handler::append_update");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->length() > 0)
  {
    if (str->reserve(SPIDER_SQL_SEMICOLON_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  }

  if (
    (err = append_update(str, link_idx)) ||
    (err = append_update_set(str)) ||
    (err = append_update_where(str, table, ptr_diff))
  )
    DBUG_RETURN(err);

  if (
    spider->pk_update &&
    share->link_statuses[link_idx] == SPIDER_LINK_STATUS_RECOVERY
  ) {
    if (str->reserve(SPIDER_SQL_SEMICOLON_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
    if ((err = append_insert_for_recovery(
      SPIDER_SQL_TYPE_UPDATE_SQL, link_idx)))
      DBUG_RETURN(err);
  }

  if (!filled_up)
    filled_up = (str->length() >= (uint) spider->result_list.bulk_update_size);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_delete(
  const TABLE *table,
  my_ptrdiff_t ptr_diff
) {
  int err;
  spider_string *str = &update_sql;
  DBUG_ENTER("spider_odbc_handler::append_delete");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->length() > 0)
  {
    if (str->reserve(SPIDER_SQL_SEMICOLON_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  }

  if (
    (err = append_delete(str)) ||
    (err = append_from(str, SPIDER_SQL_TYPE_DELETE_SQL,
      first_link_idx)) ||
    (err = append_update_where(str, table, ptr_diff))
  )
    DBUG_RETURN(err);
  filled_up = (str->length() >= (uint) spider->result_list.bulk_update_size);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_delete(
  const TABLE *table,
  my_ptrdiff_t ptr_diff,
  int link_idx
) {
  int err;
  spider_string *str = &spider->result_list.update_sqls[link_idx];
  DBUG_ENTER("spider_odbc_handler::append_delete");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->length() > 0)
  {
    if (str->reserve(SPIDER_SQL_SEMICOLON_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  }

  if (
    (err = append_delete(str)) ||
    (err = append_from(str, SPIDER_SQL_TYPE_DELETE_SQL, link_idx)) ||
    (err = append_update_where(str, table, ptr_diff))
  )
    DBUG_RETURN(err);
  if (!filled_up)
    filled_up = (str->length() >= (uint) spider->result_list.bulk_update_size);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_insert_part()
{
  int err;
  DBUG_ENTER("spider_odbc_handler::append_insert_part");
  DBUG_PRINT("info",("spider this=%p", this));
  err = append_insert(&insert_sql, 0);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_insert(
  spider_string *str,
  int link_idx
) {
  DBUG_ENTER("spider_odbc_handler::append_insert");
  if (str->reserve(SPIDER_SQL_INSERT_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_INSERT_STR, SPIDER_SQL_INSERT_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_update_part()
{
  int err;
  DBUG_ENTER("spider_odbc_handler::append_update_part");
  DBUG_PRINT("info",("spider this=%p", this));
  err = append_update(&update_sql, 0);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_update(
  spider_string *str,
  int link_idx
) {
  DBUG_ENTER("spider_odbc_handler::append_update");
  if (str->reserve(SPIDER_SQL_UPDATE_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_UPDATE_STR, SPIDER_SQL_UPDATE_LEN);
  if (str->reserve(odbc_share->db_nm_max_length +
    SPIDER_SQL_DOT_LEN + odbc_share->table_nm_max_length +
    utility->name_quote_length * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  table_name_pos = str->length();
  append_table_name_with_adjusting(str, link_idx, SPIDER_SQL_TYPE_UPDATE_SQL);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_delete_part()
{
  int err;
  DBUG_ENTER("spider_odbc_handler::append_delete_part");
  DBUG_PRINT("info",("spider this=%p", this));
  err = append_delete(&update_sql);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_delete(
  spider_string *str
) {
  DBUG_ENTER("spider_odbc_handler::append_delete");
  if (str->reserve(SPIDER_SQL_DELETE_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_DELETE_STR, SPIDER_SQL_DELETE_LEN);
  str->length(str->length() - 1);
  DBUG_RETURN(0);
}

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
int spider_odbc_handler::append_increment_update_set_part()
{
  int err;
  DBUG_ENTER("spider_odbc_handler::append_increment_update_set_part");
  DBUG_PRINT("info",("spider this=%p", this));
  err = append_increment_update_set(&update_sql);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_increment_update_set(
  spider_string *str
) {
  uint field_name_length;
  uint roop_count;
  Field *field;
  DBUG_ENTER("spider_odbc_handler::append_increment_update_set");
  if (str->reserve(SPIDER_SQL_SET_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SET_STR, SPIDER_SQL_SET_LEN);
  const SPIDER_HS_STRING_REF *value = hs_upds.ptr();
  for (roop_count = 0; roop_count < hs_upds.size();
    roop_count++)
  {
    if (
      value[roop_count].size() == 1 &&
      *(value[roop_count].begin()) == '0'
    )
      continue;

    Field *top_table_field =
      spider->get_top_table_field(spider->hs_pushed_ret_fields[roop_count]);
    if (!(field = spider->field_exchange(top_table_field)))
      continue;
    field_name_length =
      odbc_share->column_name_str[field->field_index].length();

    if (str->reserve(field_name_length * 2 +
      utility->name_quote_length * 4 +
      SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_HS_INCREMENT_LEN +
      SPIDER_SQL_COMMA_LEN + value[roop_count].size()))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);

    odbc_share->append_column_name(str, field->field_index);
    str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
    odbc_share->append_column_name(str, field->field_index);
    if (spider->hs_increment)
      str->q_append(SPIDER_SQL_HS_INCREMENT_STR,
        SPIDER_SQL_HS_INCREMENT_LEN);
    else
      str->q_append(SPIDER_SQL_HS_DECREMENT_STR,
        SPIDER_SQL_HS_DECREMENT_LEN);
    str->q_append(value[roop_count].begin(), value[roop_count].size());
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}
#endif
#endif

int spider_odbc_handler::append_update_set_part()
{
  int err;
  DBUG_ENTER("spider_odbc_handler::append_update_set_part");
  DBUG_PRINT("info",("spider this=%p", this));
  update_set_pos = update_sql.length();
  err = append_update_set(&update_sql);
  where_pos = update_sql.length();
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_update_set(
  spider_string *str
) {
  uint field_name_length;
  SPIDER_SHARE *share = spider->share;
  TABLE *table = spider->get_table();
  Field **fields;
  DBUG_ENTER("spider_odbc_handler::append_update_set");
  if (str->reserve(SPIDER_SQL_SET_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SET_STR, SPIDER_SQL_SET_LEN);
  for (fields = table->field; *fields; fields++)
  {
    if (bitmap_is_set(table->write_set, (*fields)->field_index))
    {
      field_name_length =
        odbc_share->column_name_str[(*fields)->field_index].length();
      if ((*fields)->is_null())
      {
        if (str->reserve(field_name_length +
          utility->name_quote_length * 2 +
          SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_NULL_LEN +
          SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        odbc_share->append_column_name(str, (*fields)->field_index);
        str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
        str->q_append(SPIDER_SQL_NULL_STR, SPIDER_SQL_NULL_LEN);
      } else {
        if (str->reserve(field_name_length +
          utility->name_quote_length * 2 + SPIDER_SQL_EQUAL_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        odbc_share->append_column_name(str, (*fields)->field_index);
        str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
#ifndef DBUG_OFF
        my_bitmap_map *tmp_map = dbug_tmp_use_all_columns(table,
          table->read_set);
#endif
        if (
          utility->append_column_value(spider, str, *fields, NULL,
            share->access_charset) ||
          str->reserve(SPIDER_SQL_COMMA_LEN)
        ) {
#ifndef DBUG_OFF
          dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
#ifndef DBUG_OFF
        dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
      }
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
int spider_odbc_handler::append_direct_update_set_part()
{
  int err;
  DBUG_ENTER("spider_odbc_handler::append_direct_update_set_part");
  DBUG_PRINT("info",("spider this=%p", this));
  update_set_pos = update_sql.length();
  err = append_direct_update_set(&update_sql);
  where_pos = update_sql.length();
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_direct_update_set(
  spider_string *str
) {
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  uint field_name_length;
  SPIDER_SHARE *share = spider->share;
#ifndef DBUG_OFF
  TABLE *table = spider->get_table();
#endif
#endif
  DBUG_ENTER("spider_odbc_handler::append_direct_update_set");
  if (
    spider->direct_update_kinds == SPIDER_SQL_KIND_SQL &&
    spider->wide_handler->direct_update_fields
  ) {
    if (str->reserve(SPIDER_SQL_SET_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SET_STR, SPIDER_SQL_SET_LEN);
    DBUG_RETURN(spider_db_append_update_columns(spider, str, NULL, 0,
      dbton_id, FALSE, NULL));
  }

  if (
    (spider->direct_update_kinds & SPIDER_SQL_KIND_SQL)
  ) {
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    size_t roop_count;
    Field *field;
    if (str->reserve(SPIDER_SQL_SET_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SET_STR, SPIDER_SQL_SET_LEN);
    for (roop_count = 0; roop_count < spider->hs_pushed_ret_fields_num;
      roop_count++)
    {
      Field *top_table_field =
        spider->get_top_table_field(spider->hs_pushed_ret_fields[roop_count]);
      if (!(field = spider->field_exchange(top_table_field)))
        continue;
      field_name_length =
        odbc_share->column_name_str[field->field_index].length();
      if (top_table_field->is_null())
      {
        if (str->reserve(field_name_length +
          utility->name_quote_length * 2 +
          SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_NULL_LEN +
          SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        odbc_share->append_column_name(str, field->field_index);
        str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
        str->q_append(SPIDER_SQL_NULL_STR, SPIDER_SQL_NULL_LEN);
      } else {
        if (str->reserve(field_name_length +
          utility->name_quote_length * 2 + SPIDER_SQL_EQUAL_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        odbc_share->append_column_name(str, field->field_index);
        str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
#ifndef DBUG_OFF
        my_bitmap_map *tmp_map = dbug_tmp_use_all_columns(table,
          table->read_set);
#endif
        if (
          utility->append_column_value(spider, str, top_table_field, NULL,
            share->access_charset) ||
          str->reserve(SPIDER_SQL_COMMA_LEN)
        ) {
#ifndef DBUG_OFF
          dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
#ifndef DBUG_OFF
        dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
      }
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
#else
    DBUG_ASSERT(0);
#endif
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_dup_update_pushdown_part(
  const char *alias,
  uint alias_length
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::append_dup_update_pushdown_part");
  DBUG_PRINT("info",("spider this=%p", this));
  dup_update_sql.length(0);
  err = append_update_columns(&dup_update_sql, alias, alias_length);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_update_columns_part(
  const char *alias,
  uint alias_length
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::append_update_columns_part");
  DBUG_PRINT("info",("spider this=%p", this));
  err = append_update_columns(&update_sql, alias, alias_length);
  DBUG_RETURN(err);
}

int spider_odbc_handler::check_update_columns_part()
{
  int err;
  DBUG_ENTER("spider_odbc_handler::check_update_columns_part");
  DBUG_PRINT("info",("spider this=%p", this));
  err = append_update_columns(NULL, NULL, 0);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_update_columns(
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  int err;
  List_iterator_fast<Item> fi(*spider->wide_handler->direct_update_fields),
    vi(*spider->wide_handler->direct_update_values);
  Item *field, *value;
  DBUG_ENTER("spider_odbc_handler::append_update_columns");
  while ((field = fi++))
  {
    value = vi++;
    if ((err = spider_db_print_item_type(
      (Item *) field, NULL, spider, str, alias, alias_length,
      spider_dbton_odbc.dbton_id, FALSE, NULL)))
    {
      if (
        err == ER_SPIDER_COND_SKIP_NUM &&
        field->type() == Item::FIELD_ITEM &&
        ((Item_field *) field)->field
      )
        continue;
      DBUG_RETURN(err);
    }
    if (str)
    {
      if (str->reserve(SPIDER_SQL_EQUAL_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
    }
    if ((err = spider_db_print_item_type(
      (Item *) value, ((Item_field *) field)->field, spider, str,
      alias, alias_length, spider_dbton_odbc.dbton_id, FALSE, NULL)))
      DBUG_RETURN(err);
    if (str)
    {
      if (str->reserve(SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
  }
  if (str)
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}
#endif

int spider_odbc_handler::append_select_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_select_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      str = &ha_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_select(str, sql_type);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_select(
  spider_string *str,
  ulong sql_type
) {
  DBUG_ENTER("spider_odbc_handler::append_select");
  if (sql_type == SPIDER_SQL_TYPE_HANDLER)
  {
    if (str->reserve(SPIDER_SQL_HANDLER_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_HANDLER_STR, SPIDER_SQL_HANDLER_LEN);
  } else {
    if (str->reserve(SPIDER_SQL_SELECT_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SELECT_STR, SPIDER_SQL_SELECT_LEN);
    if (spider->result_list.direct_distinct)
    {
      if (str->reserve(SPIDER_SQL_DISTINCT_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_DISTINCT_STR, SPIDER_SQL_DISTINCT_LEN);
    }
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_table_select_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_table_select_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_table_select(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_table_select(
  spider_string *str
) {
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  st_select_lex *select_lex = NULL;
#endif
  DBUG_ENTER("spider_odbc_handler::append_table_select");
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  if (spider->result_list.direct_aggregate)
  {
    select_lex = spider_get_select_lex(spider);
    JOIN *join = select_lex->join;
    if (!(*join->sum_funcs) && !select_lex->group_list.elements)
    {
      select_lex = NULL;
    }
  }
  if (select_lex)
  {
    TABLE *table = spider->get_table();
    Field **field;
    int field_length;
    for (field = table->field; *field; field++)
    {
      bool direct_use = TRUE;
      direct_use = spider_db_check_select_colum_in_group(select_lex, *field);
      if (!direct_use)
      {
        if (str->reserve(SPIDER_SQL_ODBC_MIN_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_ODBC_MIN_STR, SPIDER_SQL_ODBC_MIN_LEN);
      }
      field_length =
        odbc_share->column_name_str[(*field)->field_index].length();
      if (str->reserve(field_length +
        utility->name_quote_length * 2 + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      odbc_share->append_column_name(str, (*field)->field_index);
      if (!direct_use)
      {
        if (str->reserve(SPIDER_SQL_ODBC_CLOSE_FUNC_LEN +
          SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
          SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
      }
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
    DBUG_RETURN(append_from(str, SPIDER_SQL_TYPE_SELECT_SQL, first_link_idx));
  } else {
#endif
    table_name_pos = str->length() + odbc_share->table_select_pos;
    if (str->append(*(odbc_share->table_select)))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  }
#endif
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_key_select_part(
  ulong sql_type,
  uint idx
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_key_select_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_key_select(str, idx);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_key_select(
  spider_string *str,
  uint idx
) {
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  st_select_lex *select_lex = NULL;
#endif
  DBUG_ENTER("spider_odbc_handler::append_key_select");
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  if (spider->result_list.direct_aggregate)
  {
    select_lex = spider_get_select_lex(spider);
    JOIN *join = select_lex->join;
    if (!(*join->sum_funcs) && !select_lex->group_list.elements)
    {
      select_lex = NULL;
    }
  }
  if (select_lex)
  {
    TABLE *table = spider->get_table();
    KEY *key_info = &table->key_info[idx];
    KEY_PART_INFO *key_part;
    Field *field;
    uint part_num;
    int field_length;
    for (key_part = key_info->key_part, part_num = 0;
      part_num < spider_user_defined_key_parts(key_info);
      key_part++, part_num++)
    {
      bool direct_use = TRUE;
      field = key_part->field;
      direct_use = spider_db_check_select_colum_in_group(select_lex, field);
      if (!direct_use)
      {
        if (str->reserve(SPIDER_SQL_ODBC_MIN_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_ODBC_MIN_STR, SPIDER_SQL_ODBC_MIN_LEN);
      }
      field_length = odbc_share->column_name_str[field->field_index].length();
      if (str->reserve(field_length +
        utility->name_quote_length * 2 + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      odbc_share->append_column_name(str, field->field_index);
      if (!direct_use)
      {
        if (str->reserve(SPIDER_SQL_ODBC_CLOSE_FUNC_LEN +
          SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
          SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
      }
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
    DBUG_RETURN(append_from(str, SPIDER_SQL_TYPE_SELECT_SQL, first_link_idx));
  } else {
#endif
    table_name_pos = str->length() + odbc_share->key_select_pos[idx];
    if (str->append(odbc_share->key_select[idx]))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  }
#endif
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_minimum_select_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_minimum_select_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_minimum_select(str, sql_type);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_minimum_select(
  spider_string *str,
  ulong sql_type
) {
  TABLE *table = spider->get_table();
  Field **field;
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  st_select_lex *select_lex = NULL;
#endif
  int field_length;
  bool appended = FALSE;
  DBUG_ENTER("spider_odbc_handler::append_minimum_select");
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  if (spider->result_list.direct_aggregate)
  {
    select_lex = spider_get_select_lex(spider);
    JOIN *join = select_lex->join;
    if (!(*join->sum_funcs) && !select_lex->group_list.elements)
    {
      select_lex = NULL;
    }
  }
#endif
  minimum_select_bitmap_create();
  for (field = table->field; *field; field++)
  {
    if (minimum_select_bit_is_set((*field)->field_index))
    {
/*
      spider_set_bit(minimum_select_bitmap, (*field)->field_index);
*/
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
      bool direct_use = TRUE;
      if (select_lex)
      {
        direct_use = spider_db_check_select_colum_in_group(select_lex, *field);
      }
      if (!direct_use)
      {
        if (str->reserve(SPIDER_SQL_ODBC_MIN_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_ODBC_MIN_STR, SPIDER_SQL_ODBC_MIN_LEN);
      }
#endif
      field_length =
        odbc_share->column_name_str[(*field)->field_index].length();
      if (str->reserve(field_length +
        utility->name_quote_length * 2 + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      odbc_share->append_column_name(str, (*field)->field_index);
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
      if (!direct_use)
      {
        if (str->reserve(SPIDER_SQL_ODBC_CLOSE_FUNC_LEN +
          SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
          SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
      }
#endif
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      appended = TRUE;
    }
  }
  if (appended)
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  else {
    if (str->reserve(SPIDER_SQL_ONE_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_ONE_STR, SPIDER_SQL_ONE_LEN);
  }
  DBUG_RETURN(append_from(str, sql_type, first_link_idx));
}

int spider_odbc_handler::append_table_select_with_alias(
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  TABLE *table = spider->get_table();
  Field **field;
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  st_select_lex *select_lex = NULL;
#endif
  int field_length;
  DBUG_ENTER("spider_odbc_handler::append_table_select_with_alias");
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  if (spider->result_list.direct_aggregate)
  {
    select_lex = spider_get_select_lex(spider);
    JOIN *join = select_lex->join;
    if (!(*join->sum_funcs) && !select_lex->group_list.elements)
    {
      select_lex = NULL;
    }
  }
#endif
  for (field = table->field; *field; field++)
  {
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
    bool direct_use = TRUE;
    if (select_lex)
    {
      direct_use = spider_db_check_select_colum_in_group(select_lex, *field);
    }
    if (!direct_use)
    {
      if (str->reserve(SPIDER_SQL_ODBC_MIN_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_ODBC_MIN_STR, SPIDER_SQL_ODBC_MIN_LEN);
    }
#endif
    field_length =
      odbc_share->column_name_str[(*field)->field_index].length();
    if (str->reserve(alias_length + field_length +
      utility->name_quote_length * 2 + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(alias, alias_length);
    odbc_share->append_column_name(str, (*field)->field_index);
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
    if (!direct_use)
    {
      if (str->reserve(SPIDER_SQL_ODBC_CLOSE_FUNC_LEN +
        SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
        SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
    }
#endif
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_key_select_with_alias(
  spider_string *str,
  const KEY *key_info,
  const char *alias,
  uint alias_length
) {
  KEY_PART_INFO *key_part;
  Field *field;
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  st_select_lex *select_lex = NULL;
#endif
  uint part_num;
  int field_length;
  DBUG_ENTER("spider_odbc_handler::append_key_select_with_alias");
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  if (spider->result_list.direct_aggregate)
  {
    select_lex = spider_get_select_lex(spider);
    JOIN *join = select_lex->join;
    if (!(*join->sum_funcs) && !select_lex->group_list.elements)
    {
      select_lex = NULL;
    }
  }
#endif
  for (key_part = key_info->key_part, part_num = 0;
    part_num < spider_user_defined_key_parts(key_info); key_part++, part_num++)
  {
    field = key_part->field;
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
    bool direct_use = TRUE;
    if (select_lex)
    {
      direct_use = spider_db_check_select_colum_in_group(select_lex, field);
    }
    if (!direct_use)
    {
      if (str->reserve(SPIDER_SQL_ODBC_MIN_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_ODBC_MIN_STR, SPIDER_SQL_ODBC_MIN_LEN);
    }
#endif
    field_length = odbc_share->column_name_str[field->field_index].length();
    if (str->reserve(alias_length + field_length +
      utility->name_quote_length * 2 + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(alias, alias_length);
    odbc_share->append_column_name(str, field->field_index);
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
    if (!direct_use)
    {
      if (str->reserve(SPIDER_SQL_ODBC_CLOSE_FUNC_LEN +
        SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
        SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
    }
#endif
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_minimum_select_with_alias(
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  TABLE *table = spider->get_table();
  Field **field;
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  st_select_lex *select_lex = NULL;
#endif
  int field_length;
  bool appended = FALSE;
  DBUG_ENTER("spider_odbc_handler::append_minimum_select_with_alias");
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  if (spider->result_list.direct_aggregate)
  {
    select_lex = spider_get_select_lex(spider);
    JOIN *join = select_lex->join;
    if (!(*join->sum_funcs) && !select_lex->group_list.elements)
    {
      select_lex = NULL;
    }
  }
#endif
  minimum_select_bitmap_create();
  for (field = table->field; *field; field++)
  {
    if (minimum_select_bit_is_set((*field)->field_index))
    {
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
      bool direct_use = TRUE;
      if (select_lex)
      {
        direct_use = spider_db_check_select_colum_in_group(select_lex, *field);
      }
      if (!direct_use)
      {
        if (str->reserve(SPIDER_SQL_ODBC_MIN_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_ODBC_MIN_STR, SPIDER_SQL_ODBC_MIN_LEN);
      }
#endif
      field_length =
        odbc_share->column_name_str[(*field)->field_index].length();
      if (str->reserve(alias_length + field_length +
        utility->name_quote_length * 2 + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(alias, alias_length);
      odbc_share->append_column_name(str, (*field)->field_index);
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
      if (!direct_use)
      {
        if (str->reserve(SPIDER_SQL_ODBC_CLOSE_FUNC_LEN +
          SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
          SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
      }
#endif
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      appended = TRUE;
    }
  }
  if (appended)
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  else {
    if (str->reserve(SPIDER_SQL_ONE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_ONE_STR, SPIDER_SQL_ONE_LEN);
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_select_columns_with_alias(
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  int err;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_odbc_handler::append_select_columns_with_alias");
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  if (
    result_list->direct_aggregate &&
    (err = append_sum_select(str, alias, alias_length))
  )
    DBUG_RETURN(err);
#endif
  if ((err = append_match_select(str, alias, alias_length)))
    DBUG_RETURN(err);
  if (!spider->select_column_mode)
  {
    if (result_list->keyread)
      DBUG_RETURN(append_key_select_with_alias(
        str, result_list->key_info, alias, alias_length));
    else
      DBUG_RETURN(append_table_select_with_alias(
        str, alias, alias_length));
  }
  DBUG_RETURN(append_minimum_select_with_alias(str, alias, alias_length));
}

int spider_odbc_handler::append_hint_after_table_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_hint_after_table_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      str = &update_sql;
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      str = &ha_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_hint_after_table(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_hint_after_table(
  spider_string *str
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::append_hint_after_table");
  DBUG_PRINT("info",("spider this=%p", this));
  if (
    odbc_share->key_hint &&
    (err = spider_db_append_hint_after_table(spider,
      str, &odbc_share->key_hint[spider->active_index]))
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  DBUG_RETURN(0);
}

void spider_odbc_handler::set_where_pos(
  ulong sql_type
) {
  DBUG_ENTER("spider_odbc_handler::set_where_pos");
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
    case SPIDER_SQL_TYPE_TMP_SQL:
      where_pos = sql.length();
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      where_pos = update_sql.length();
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      ha_read_pos = ha_sql.length();
      break;
    default:
      break;
  }
  DBUG_VOID_RETURN;
}

void spider_odbc_handler::set_where_to_pos(
  ulong sql_type
) {
  DBUG_ENTER("spider_odbc_handler::set_where_to_pos");
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
    case SPIDER_SQL_TYPE_TMP_SQL:
      sql.length(where_pos);
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      update_sql.length(where_pos);
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      ha_sql.length(ha_read_pos);
      break;
    default:
      break;
  }
  DBUG_VOID_RETURN;
}

int spider_odbc_handler::check_item_type(
  Item *item
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::check_item_type");
  DBUG_PRINT("info",("spider this=%p", this));
  err = spider_db_print_item_type(item, NULL, spider, NULL, NULL, 0,
    dbton_id, FALSE, NULL);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_values_connector_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_values_connector_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &tmp_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_values_connector(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_values_connector(
  spider_string *str
) {
  DBUG_ENTER("spider_odbc_handler::append_values_connector");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_CLOSE_PAREN_LEN +
    SPIDER_SQL_COMMA_LEN + SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_values_terminator_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_values_terminator_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &tmp_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_values_terminator(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_values_terminator(
  spider_string *str
) {
  DBUG_ENTER("spider_odbc_handler::append_values_terminator");
  DBUG_PRINT("info",("spider this=%p", this));
  str->length(str->length() -
    SPIDER_SQL_COMMA_LEN - SPIDER_SQL_OPEN_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_union_table_connector_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_union_table_connector_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &tmp_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_union_table_connector(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_union_table_connector(
  spider_string *str
) {
  DBUG_ENTER("spider_odbc_handler::append_union_table_connector");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve((SPIDER_SQL_SPACE_LEN * 2) + SPIDER_SQL_UNION_ALL_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
  str->q_append(SPIDER_SQL_UNION_ALL_STR, SPIDER_SQL_UNION_ALL_LEN);
  str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_union_table_terminator_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_union_table_terminator_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_union_table_terminator(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_union_table_terminator(
  spider_string *str
) {
  DBUG_ENTER("spider_odbc_handler::append_union_table_terminator");
  DBUG_PRINT("info",("spider this=%p", this));
  str->length(str->length() -
    ((SPIDER_SQL_SPACE_LEN * 2) + SPIDER_SQL_UNION_ALL_LEN));
  str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  table_name_pos = str->length() + SPIDER_SQL_SPACE_LEN + SPIDER_SQL_A_LEN +
    SPIDER_SQL_COMMA_LEN;
  if (str->reserve(tmp_sql.length() - SPIDER_SQL_FROM_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(tmp_sql.ptr() + SPIDER_SQL_FROM_LEN,
    tmp_sql.length() - SPIDER_SQL_FROM_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_key_column_values_part(
  const key_range *start_key,
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_key_column_values_part");
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &tmp_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_key_column_values(str, start_key);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_key_column_values(
  spider_string *str,
  const key_range *start_key
) {
  int err;
  const uchar *ptr;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  KEY *key_info = result_list->key_info;
  uint length;
  uint store_length;
  key_part_map full_key_part_map =
    make_prev_keypart_map(spider_user_defined_key_parts(key_info));
  key_part_map start_key_part_map;
  KEY_PART_INFO *key_part;
  Field *field;
  DBUG_ENTER("spider_odbc_handler::append_key_column_values");
  start_key_part_map = start_key->keypart_map & full_key_part_map;
  DBUG_PRINT("info", ("spider spider_user_defined_key_parts=%u",
    spider_user_defined_key_parts(key_info)));
  DBUG_PRINT("info", ("spider full_key_part_map=%lu", full_key_part_map));
  DBUG_PRINT("info", ("spider start_key_part_map=%lu", start_key_part_map));

  if (!start_key_part_map)
    DBUG_RETURN(0);

  for (
    key_part = key_info->key_part,
    length = 0;
    start_key_part_map;
    start_key_part_map >>= 1,
    key_part++,
    length += store_length
  ) {
    store_length = key_part->store_length;
    ptr = start_key->key + length;
    field = key_part->field;
    if ((err = spider_db_append_null_value(str, key_part, &ptr)))
    {
      if (err > 0)
        DBUG_RETURN(err);
    } else {
      if (utility->append_column_value(spider, str, field, ptr,
        share->access_charset))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }

    if (str->reserve(SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_key_column_values_with_name_part(
  const key_range *start_key,
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_key_column_values_with_name_part");
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &tmp_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_key_column_values_with_name(str, start_key);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_key_column_values_with_name(
  spider_string *str,
  const key_range *start_key
) {
  int err;
  const uchar *ptr;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  KEY *key_info = result_list->key_info;
  uint length;
  uint key_name_length, key_count;
  uint store_length;
  key_part_map full_key_part_map =
    make_prev_keypart_map(spider_user_defined_key_parts(key_info));
  key_part_map start_key_part_map;
  KEY_PART_INFO *key_part;
  Field *field;
  char tmp_buf[MAX_FIELD_WIDTH];
  DBUG_ENTER("spider_odbc_handler::append_key_column_values_with_name");
  start_key_part_map = start_key->keypart_map & full_key_part_map;
  DBUG_PRINT("info", ("spider spider_user_defined_key_parts=%u",
    spider_user_defined_key_parts(key_info)));
  DBUG_PRINT("info", ("spider full_key_part_map=%lu", full_key_part_map));
  DBUG_PRINT("info", ("spider start_key_part_map=%lu", start_key_part_map));

  if (!start_key_part_map)
    DBUG_RETURN(0);

  for (
    key_part = key_info->key_part,
    length = 0,
    key_count = 0;
    start_key_part_map;
    start_key_part_map >>= 1,
    key_part++,
    length += store_length,
    key_count++
  ) {
    store_length = key_part->store_length;
    ptr = start_key->key + length;
    field = key_part->field;
    if ((err = spider_db_append_null_value(str, key_part, &ptr)))
    {
      if (err > 0)
        DBUG_RETURN(err);
    } else {
      if (utility->append_column_value(spider, str, field, ptr,
        share->access_charset))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }

    key_name_length = my_sprintf(tmp_buf, (tmp_buf, "c%u", key_count));
    if (str->reserve(SPIDER_SQL_SPACE_LEN + key_name_length +
      SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
    str->q_append(tmp_buf, key_name_length);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_key_where_part(
  const key_range *start_key,
  const key_range *end_key,
  ulong sql_type
) {
  int err;
  spider_string *str, *str_part = NULL, *str_part2 = NULL;
  bool set_order;
  DBUG_ENTER("spider_odbc_handler::append_key_where_part");
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      set_order = FALSE;
      break;
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &tmp_sql;
      set_order = FALSE;
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      str = &update_sql;
      set_order = FALSE;
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      str = &ha_sql;
      ha_read_pos = str->length();
      str_part = &sql_part;
      str_part2 = &sql_part2;
      str_part->length(0);
      str_part2->length(0);
      set_order = TRUE;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_key_where(str, str_part, str_part2, start_key, end_key,
    sql_type, set_order);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_key_where(
  spider_string *str,
  spider_string *str_part,
  spider_string *str_part2,
  const key_range *start_key,
  const key_range *end_key,
  ulong sql_type,
  bool set_order
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::append_key_where");
  err = spider_db_append_key_where_internal(str, str_part, str_part2,
    start_key, end_key, spider, set_order, sql_type,
    dbton_id);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_is_null_part(
  ulong sql_type,
  KEY_PART_INFO *key_part,
  const key_range *key,
  const uchar **ptr,
  bool key_eq,
  bool tgt_final
) {
  int err;
  spider_string *str, *str_part = NULL, *str_part2 = NULL;
  DBUG_ENTER("spider_odbc_handler::append_is_null_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      str = &update_sql;
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      str = &ha_sql;
      str_part = &sql_part;
      str_part2 = &sql_part2;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_is_null(sql_type, str, str_part, str_part2,
    key_part, key, ptr, key_eq, tgt_final);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_is_null(
  ulong sql_type,
  spider_string *str,
  spider_string *str_part,
  spider_string *str_part2,
  KEY_PART_INFO *key_part,
  const key_range *key,
  const uchar **ptr,
  bool key_eq,
  bool tgt_final
) {
  DBUG_ENTER("spider_odbc_handler::append_is_null");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider key_eq=%s", key_eq ? "TRUE" : "FALSE"));
  if (key_part->null_bit)
  {
    if (*(*ptr)++)
    {
      if (sql_type == SPIDER_SQL_TYPE_HANDLER)
      {
        str = str_part;
        if (
          key_eq ||
          key->flag == HA_READ_KEY_EXACT ||
          key->flag == HA_READ_KEY_OR_NEXT
        ) {
          if (str->reserve(SPIDER_SQL_IS_NULL_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_IS_NULL_STR, SPIDER_SQL_IS_NULL_LEN);
        } else {
          str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
          ha_next_pos = str->length();
          if (str->reserve(SPIDER_SQL_FIRST_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_FIRST_STR, SPIDER_SQL_FIRST_LEN);
          spider->result_list.ha_read_kind = 1;
        }
        str = str_part2;
      }
      if (
        key_eq ||
        key->flag == HA_READ_KEY_EXACT ||
        key->flag == HA_READ_KEY_OR_NEXT
      ) {
        if (str->reserve(SPIDER_SQL_IS_NULL_LEN +
          utility->name_quote_length * 2 +
          odbc_share->column_name_str[key_part->field->field_index].length()))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        odbc_share->append_column_name(str, key_part->field->field_index);
        str->q_append(SPIDER_SQL_IS_NULL_STR, SPIDER_SQL_IS_NULL_LEN);
      } else {
        if (str->reserve(SPIDER_SQL_IS_NOT_NULL_LEN +
          utility->name_quote_length * 2 +
          odbc_share->column_name_str[key_part->field->field_index].length()))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        odbc_share->append_column_name(str, key_part->field->field_index);
        str->q_append(SPIDER_SQL_IS_NOT_NULL_STR, SPIDER_SQL_IS_NOT_NULL_LEN);
      }
      DBUG_RETURN(-1);
    }
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_where_terminator_part(
  ulong sql_type,
  bool set_order,
  int key_count
) {
  int err;
  spider_string *str, *str_part = NULL, *str_part2 = NULL;
  DBUG_ENTER("spider_odbc_handler::append_where_terminator_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      str = &update_sql;
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      str = &ha_sql;
      str_part = &sql_part;
      str_part2 = &sql_part2;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_where_terminator(sql_type, str, str_part, str_part2,
    set_order, key_count);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_where_terminator(
  ulong sql_type,
  spider_string *str,
  spider_string *str_part,
  spider_string *str_part2,
  bool set_order,
  int key_count
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_odbc_handler::append_where_terminator");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql_type != SPIDER_SQL_TYPE_HANDLER)
  {
    str->length(str->length() - SPIDER_SQL_AND_LEN);
    if (!set_order)
      result_list->key_order = key_count;
  } else {
    str_part2->length(str_part2->length() - SPIDER_SQL_AND_LEN);

    str_part->length(str_part->length() - SPIDER_SQL_COMMA_LEN);
    if (!result_list->ha_read_kind)
      str_part->q_append(SPIDER_SQL_CLOSE_PAREN_STR,
        SPIDER_SQL_CLOSE_PAREN_LEN);
    if (str->append(*str_part))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    uint clause_length = str->length() - ha_next_pos;
    if (clause_length < SPIDER_SQL_NEXT_LEN)
    {
      int roop_count;
      clause_length = SPIDER_SQL_NEXT_LEN - clause_length;
      if (str->reserve(clause_length))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      for (roop_count = 0; roop_count < (int) clause_length; roop_count++)
        str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
    }
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_match_where_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_match_where_part");
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_ASSERT(0);
      DBUG_RETURN(0);
  }
  err = append_match_where(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_match_where(
  spider_string *str
) {
  DBUG_ENTER("spider_odbc_handler::append_match_where");
  /* TODO: develop later */
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_update_where(
  spider_string *str,
  const TABLE *table,
  my_ptrdiff_t ptr_diff
) {
  uint field_name_length;
  Field **field;
  THD *thd = spider->wide_handler->trx->thd;
  SPIDER_SHARE *share = spider->share;
  bool no_pk = (table->s->primary_key == MAX_KEY);
  DBUG_ENTER("spider_odbc_handler::append_update_where");
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
    if (str->reserve(SPIDER_SQL_CUR_CUR_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_CUR_CUR_STR, SPIDER_SQL_CUR_CUR_LEN);
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

int spider_odbc_handler::append_condition_part(
  const char *alias,
  uint alias_length,
  ulong sql_type,
  bool test_flg
) {
  int err;
  spider_string *str;
  bool start_where = FALSE;
  DBUG_ENTER("spider_odbc_handler::append_condition_part");
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      if (test_flg)
      {
        str = NULL;
      } else {
        str = &sql;
        start_where = ((int) str->length() == where_pos);
      }
      break;
    case SPIDER_SQL_TYPE_TMP_SQL:
      if (test_flg)
      {
        str = NULL;
      } else {
        str = &tmp_sql;
        start_where = ((int) str->length() == where_pos);
      }
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      if (test_flg)
      {
        str = NULL;
      } else {
        str = &update_sql;
        start_where = ((int) str->length() == where_pos);
      }
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      if (test_flg)
      {
        str = NULL;
      } else {
        str = &ha_sql;
        start_where = TRUE;
        if (spider->active_index == MAX_KEY)
        {
          set_where_pos(SPIDER_SQL_TYPE_HANDLER);
          if (str->reserve(SPIDER_SQL_READ_LEN + SPIDER_SQL_FIRST_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_READ_STR, SPIDER_SQL_READ_LEN);
          ha_next_pos = str->length();
          str->q_append(SPIDER_SQL_FIRST_STR, SPIDER_SQL_FIRST_LEN);
          sql_part2.length(0);
        }
        ha_where_pos = str->length();

        if (sql_part2.length())
        {
          str->append(sql_part2);
          start_where = FALSE;
        }
      }
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_condition(str, alias, alias_length, start_where,
    sql_type);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_condition(
  spider_string *str,
  const char *alias,
  uint alias_length,
  bool start_where,
  ulong sql_type
) {
  int err, restart_pos = 0, start_where_pos;
  SPIDER_CONDITION *tmp_cond = spider->wide_handler->condition;
  DBUG_ENTER("spider_odbc_handler::append_condition");
  if (str && start_where)
  {
    start_where_pos = str->length();
  } else {
    start_where_pos = 0;
  }

  if (spider->is_clone && !tmp_cond)
  {
    tmp_cond = spider->pt_clone_source_handler->wide_handler->condition;
  }

  while (tmp_cond)
  {
    if (str)
    {
      restart_pos = str->length();
      if (start_where)
      {
        if (str->reserve(SPIDER_SQL_WHERE_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_WHERE_STR, SPIDER_SQL_WHERE_LEN);
        start_where = FALSE;
      } else {
        if (str->reserve(SPIDER_SQL_AND_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
      }
    }
    if ((err = spider_db_print_item_type(
      (Item *) tmp_cond->cond, NULL, spider, str, alias, alias_length,
      dbton_id, FALSE, NULL)))
    {
      if (str && err == ER_SPIDER_COND_SKIP_NUM)
      {
        DBUG_PRINT("info",("spider COND skip"));
        str->length(restart_pos);
        start_where = (restart_pos == start_where_pos);
      } else
        DBUG_RETURN(err);
    }
    tmp_cond = tmp_cond->next;
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_match_against_part(
  ulong sql_type,
  st_spider_ft_info *ft_info,
  const char *alias,
  uint alias_length
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_match_against_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_match_against(str, ft_info, alias, alias_length);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_match_against(
  spider_string *str,
  st_spider_ft_info  *ft_info,
  const char *alias,
  uint alias_length
) {
  DBUG_ENTER("spider_odbc_handler::append_match_against");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_match_select_part(
  ulong sql_type,
  const char *alias,
  uint alias_length
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_match_select_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_match_select(str, alias, alias_length);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_match_select(
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  DBUG_ENTER("spider_odbc_handler::append_match_select");
  DBUG_PRINT("info",("spider this=%p", this));
  /* TODO: develop later */
  DBUG_RETURN(0);
}

#ifdef HANDLER_HAS_DIRECT_AGGREGATE
int spider_odbc_handler::append_sum_select_part(
  ulong sql_type,
  const char *alias,
  uint alias_length
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_sum_select_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_sum_select(str, alias, alias_length);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_sum_select(
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  int err;
  st_select_lex *select_lex;
  DBUG_ENTER("spider_odbc_handler::append_sum_select");
  DBUG_PRINT("info",("spider this=%p", this));
  select_lex = spider_get_select_lex(spider);
  JOIN *join = select_lex->join;
  Item_sum **item_sum_ptr;
  for (item_sum_ptr = join->sum_funcs; *item_sum_ptr; ++item_sum_ptr)
  {
    if ((err = utility->open_item_sum_func(*item_sum_ptr,
      spider, str, alias, alias_length, FALSE, NULL)))
    {
      DBUG_RETURN(err);
    }
    if (str->reserve(SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  DBUG_RETURN(0);
}
#endif

void spider_odbc_handler::set_order_pos(
  ulong sql_type
) {
  DBUG_ENTER("spider_odbc_handler::set_order_pos");
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
    case SPIDER_SQL_TYPE_TMP_SQL:
      order_pos = sql.length();
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      order_pos = update_sql.length();
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      ha_next_pos = ha_sql.length();
      break;
    default:
      DBUG_ASSERT(0);
      break;
  }
  DBUG_VOID_RETURN;
}

void spider_odbc_handler::set_order_to_pos(
  ulong sql_type
) {
  DBUG_ENTER("spider_odbc_handler::set_order_to_pos");
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
    case SPIDER_SQL_TYPE_TMP_SQL:
      sql.length(order_pos);
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      update_sql.length(order_pos);
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      ha_sql.length(ha_next_pos);
      break;
    default:
      DBUG_ASSERT(0);
      break;
  }
  DBUG_VOID_RETURN;
}

#ifdef HANDLER_HAS_DIRECT_AGGREGATE
int spider_odbc_handler::append_group_by_part(
  const char *alias,
  uint alias_length,
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_group_by_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      str = &update_sql;
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      str = &ha_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_group_by(str, alias, alias_length);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_group_by(
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  int err;
  st_select_lex *select_lex;
  DBUG_ENTER("spider_odbc_handler::append_group_by");
  DBUG_PRINT("info",("spider this=%p", this));
  select_lex = spider_get_select_lex(spider);
  ORDER *group = (ORDER *) select_lex->group_list.first;
  if (group)
  {
    if (str->reserve(SPIDER_SQL_GROUP_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_GROUP_STR, SPIDER_SQL_GROUP_LEN);
    for (; group; group = group->next)
    {
      if ((err = spider_db_print_item_type((*group->item), NULL, spider,
        str, alias, alias_length, dbton_id, FALSE, NULL)))
      {
        DBUG_RETURN(err);
      }
      if (str->reserve(SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  }
  DBUG_RETURN(0);
}
#endif

int spider_odbc_handler::append_key_order_for_merge_with_alias_part(
  const char *alias,
  uint alias_length,
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_key_order_for_merge_with_alias_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      str = &update_sql;
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      str = &ha_sql;
      ha_limit_pos = ha_sql.length();
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_key_order_for_merge_with_alias(str, alias, alias_length);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_key_order_for_merge_with_alias(
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  /* sort for index merge */
  TABLE *table = spider->get_table();
  int length;
  Field *field;
  uint key_name_length;
  DBUG_ENTER("spider_odbc_handler::append_key_order_for_merge_with_alias");
  DBUG_PRINT("info",("spider this=%p", this));
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  if (spider->result_list.direct_aggregate)
  {
    int err;
    if ((err = append_group_by(str, alias, alias_length)))
      DBUG_RETURN(err);
  }
#endif
  if (table->s->primary_key < MAX_KEY)
  {
    /* sort by primary key */
    KEY *key_info = &table->key_info[table->s->primary_key];
    KEY_PART_INFO *key_part;
    for (
      key_part = key_info->key_part,
      length = 1;
      length <= (int) spider_user_defined_key_parts(key_info);
      key_part++,
      length++
    ) {
      field = key_part->field;
      key_name_length =
        odbc_share->column_name_str[field->field_index].length();
      if (length == 1)
      {
        if (str->reserve(SPIDER_SQL_ORDER_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
      }
      if (str->reserve(alias_length + key_name_length +
        utility->name_quote_length * 2 + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(alias, alias_length);
      odbc_share->append_column_name(str, field->field_index);
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
    if (length > 1)
    {
      str->length(str->length() - SPIDER_SQL_COMMA_LEN);
    }
  } else {
    /* sort by all columns */
    Field **fieldp;
    for (
      fieldp = table->field, length = 1;
      *fieldp;
      fieldp++, length++
    ) {
      key_name_length =
        odbc_share->column_name_str[(*fieldp)->field_index].length();
      if (length == 1)
      {
        if (str->reserve(SPIDER_SQL_ORDER_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
      }
      if (str->reserve(alias_length + key_name_length +
        utility->name_quote_length * 2 + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(alias, alias_length);
      odbc_share->append_column_name(str, (*fieldp)->field_index);
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
    if (length > 1)
    {
      str->length(str->length() - SPIDER_SQL_COMMA_LEN);
    }
  }
  limit_pos = str->length();
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_key_order_for_direct_order_limit_with_alias_part(
  const char *alias,
  uint alias_length,
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_key_order_for_direct_order_limit_with_alias_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      str = &update_sql;
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      str = &ha_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_key_order_for_direct_order_limit_with_alias(
    str, alias, alias_length);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_key_order_for_direct_order_limit_with_alias(
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  int err;
  ORDER *order;
  st_select_lex *select_lex;
  longlong select_limit;
  longlong offset_limit;
  DBUG_ENTER("spider_odbc_handler::append_key_order_for_direct_order_limit_with_alias");
  DBUG_PRINT("info",("spider this=%p", this));
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  if (spider->result_list.direct_aggregate)
  {
    if ((err = append_group_by(str, alias, alias_length)))
      DBUG_RETURN(err);
  }
#endif
  spider_get_select_limit(spider, &select_lex, &select_limit,
    &offset_limit);
  if (select_lex->order_list.first)
  {
    if (str->reserve(SPIDER_SQL_ORDER_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
    for (order = (ORDER *) select_lex->order_list.first; order;
      order = order->next)
    {
      if ((err =
        spider_db_print_item_type((*order->item), NULL, spider, str, alias,
          alias_length, dbton_id, FALSE, NULL)))
      {
        DBUG_PRINT("info",("spider error=%d", err));
        DBUG_RETURN(err);
      }
      if (SPIDER_order_direction_is_asc(order))
      {
        if (str->reserve(SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      } else {
        if (str->reserve(SPIDER_SQL_DESC_LEN + SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
        str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      }
    }
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  }
  limit_pos = str->length();
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_key_order_with_alias_part(
  const char *alias,
  uint alias_length,
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_key_order_with_alias_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      str = &update_sql;
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      str = &ha_sql;
      err = append_key_order_for_handler(str, alias, alias_length);
      DBUG_RETURN(err);
    default:
      DBUG_RETURN(0);
  }
  err = append_key_order_with_alias(str, alias, alias_length);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_key_order_for_handler(
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  DBUG_ENTER("spider_odbc_handler::append_key_order_for_handler");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider ha_next_pos=%d", ha_next_pos));
  DBUG_PRINT("info",("spider ha_where_pos=%d", ha_where_pos));
  str->q_append(alias, alias_length);
  memset((char *) str->ptr() + str->length(), ' ',
    ha_where_pos - ha_next_pos - alias_length);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_key_order_with_alias(
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  KEY *key_info = result_list->key_info;
  int length;
  KEY_PART_INFO *key_part;
  Field *field;
  uint key_name_length;
  DBUG_ENTER("spider_odbc_handler::append_key_order_with_alias");
  DBUG_PRINT("info",("spider this=%p", this));
#ifdef HANDLER_HAS_DIRECT_AGGREGATE
  if (spider->result_list.direct_aggregate)
  {
    int err;
    if ((err = append_group_by(str, alias, alias_length)))
      DBUG_RETURN(err);
  }
#endif
  if (result_list->sorted == TRUE)
  {
    if (result_list->desc_flg == TRUE)
    {
      for (
        key_part = key_info->key_part + result_list->key_order,
        length = 1;
        length + result_list->key_order <
          (int) spider_user_defined_key_parts(key_info) &&
        length < result_list->max_order;
        key_part++,
        length++
      ) {
        field = key_part->field;
        key_name_length =
          odbc_share->column_name_str[field->field_index].length();
        if (length == 1)
        {
          if (str->reserve(SPIDER_SQL_ORDER_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
        }
        if (key_part->key_part_flag & HA_REVERSE_SORT)
        {
          if (str->reserve(alias_length + key_name_length +
            utility->name_quote_length * 2 + SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(alias, alias_length);
          odbc_share->append_column_name(str, field->field_index);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        } else {
          if (str->reserve(alias_length + key_name_length +
            utility->name_quote_length * 2 +
            SPIDER_SQL_DESC_LEN + SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(alias, alias_length);
          odbc_share->append_column_name(str, field->field_index);
          str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        }
      }
      if (
        length + result_list->key_order <=
          (int) spider_user_defined_key_parts(key_info) &&
        length <= result_list->max_order
      ) {
        field = key_part->field;
        key_name_length =
          odbc_share->column_name_str[field->field_index].length();
        if (length == 1)
        {
          if (str->reserve(SPIDER_SQL_ORDER_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
        }
        if (key_part->key_part_flag & HA_REVERSE_SORT)
        {
          if (str->reserve(alias_length + key_name_length +
            utility->name_quote_length * 2))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(alias, alias_length);
          odbc_share->append_column_name(str, field->field_index);
        } else {
          if (str->reserve(alias_length + key_name_length +
            utility->name_quote_length * 2 + SPIDER_SQL_DESC_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(alias, alias_length);
          odbc_share->append_column_name(str, field->field_index);
          str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
        }
      }
    } else {
      for (
        key_part = key_info->key_part + result_list->key_order,
        length = 1;
        length + result_list->key_order <
          (int) spider_user_defined_key_parts(key_info) &&
        length < result_list->max_order;
        key_part++,
        length++
      ) {
        field = key_part->field;
        key_name_length =
          odbc_share->column_name_str[field->field_index].length();
        if (length == 1)
        {
          if (str->reserve(SPIDER_SQL_ORDER_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
        }
        if (key_part->key_part_flag & HA_REVERSE_SORT)
        {
          if (str->reserve(alias_length + key_name_length +
            utility->name_quote_length * 2 +
            SPIDER_SQL_DESC_LEN + SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(alias, alias_length);
          odbc_share->append_column_name(str, field->field_index);
          str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        } else {
          if (str->reserve(alias_length + key_name_length +
            utility->name_quote_length * 2 +
            SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(alias, alias_length);
          odbc_share->append_column_name(str, field->field_index);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        }
      }
      if (
        length + result_list->key_order <=
          (int) spider_user_defined_key_parts(key_info) &&
        length <= result_list->max_order
      ) {
        field = key_part->field;
        key_name_length =
          odbc_share->column_name_str[field->field_index].length();
        if (length == 1)
        {
          if (str->reserve(SPIDER_SQL_ORDER_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
        }
        if (key_part->key_part_flag & HA_REVERSE_SORT)
        {
          if (str->reserve(alias_length + key_name_length +
            utility->name_quote_length * 2 +
            SPIDER_SQL_DESC_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(alias, alias_length);
          odbc_share->append_column_name(str, field->field_index);
          str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
        } else {
          if (str->reserve(alias_length + key_name_length +
            utility->name_quote_length * 2))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(alias, alias_length);
          odbc_share->append_column_name(str, field->field_index);
        }
      }
    }
  }
  limit_pos = str->length();
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_limit_part(
  longlong offset,
  longlong limit,
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_limit_part");
  DBUG_PRINT("info",("spider this=%p", this));
  this->limit = limit;
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      limit_pos = str->length();
      break;
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &tmp_sql;
      limit_pos = str->length();
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      str = &update_sql;
      limit_pos = str->length();
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      str = &ha_sql;
      ha_limit_pos = str->length();
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_limit(str, offset, limit);
  DBUG_RETURN(err);
}

int spider_odbc_handler::reappend_limit_part(
  longlong offset,
  longlong limit,
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::reappend_limit_part");
  DBUG_PRINT("info",("spider this=%p", this));
  this->limit = limit;
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      str->length(limit_pos);
      break;
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &tmp_sql;
      str->length(limit_pos);
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      str = &update_sql;
      str->length(limit_pos);
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      str = &ha_sql;
      str->length(ha_limit_pos);
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_limit(str, offset, limit);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_limit(
  spider_string *str,
  longlong offset,
  longlong limit
) {
  DBUG_ENTER("spider_odbc_handler::append_limit");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info", ("spider offset=%lld", offset));
  DBUG_PRINT("info", ("spider limit=%lld", limit));
  /* nothing to do */
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_select_lock_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_select_lock_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_select_lock(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_select_lock(
  spider_string *str
) {
  int lock_mode = spider_conn_lock_mode(spider);
  DBUG_ENTER("spider_odbc_handler::append_select_lock");
  DBUG_PRINT("info",("spider this=%p", this));
  if (lock_mode == SPIDER_LOCK_MODE_EXCLUSIVE)
  {
    if (str->reserve(SPIDER_SQL_FOR_UPDATE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_FOR_UPDATE_STR, SPIDER_SQL_FOR_UPDATE_LEN);
  } else if (lock_mode == SPIDER_LOCK_MODE_SHARED)
  {
    if (str->reserve(SPIDER_SQL_FOR_SHARE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_FOR_SHARE_STR, SPIDER_SQL_FOR_SHARE_LEN);
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_union_all_start_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_union_all_start_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_union_all_start(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_union_all_start(
  spider_string *str
) {
  DBUG_ENTER("spider_odbc_handler::append_union_all_start");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_union_all_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_union_all_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_union_all(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_union_all(
  spider_string *str
) {
  DBUG_ENTER("spider_odbc_handler::append_union_all");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_UNION_ALL_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_UNION_ALL_STR, SPIDER_SQL_UNION_ALL_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_union_all_end_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_union_all_end_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_union_all_end(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_union_all_end(
  spider_string *str
) {
  DBUG_ENTER("spider_odbc_handler::append_union_all_end");
  DBUG_PRINT("info",("spider this=%p", this));
  str->length(str->length() -
    SPIDER_SQL_UNION_ALL_LEN + SPIDER_SQL_CLOSE_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_multi_range_cnt_part(
  ulong sql_type,
  uint multi_range_cnt,
  bool with_comma
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_multi_range_cnt_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &tmp_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_multi_range_cnt(str, multi_range_cnt, with_comma);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_multi_range_cnt(
  spider_string *str,
  uint multi_range_cnt,
  bool with_comma
) {
  int range_cnt_length;
  char range_cnt_str[SPIDER_SQL_INT_LEN];
  DBUG_ENTER("spider_odbc_handler::append_multi_range_cnt");
  DBUG_PRINT("info",("spider this=%p", this));
  range_cnt_length = my_sprintf(range_cnt_str, (range_cnt_str, "%u",
    multi_range_cnt));
  if (with_comma)
  {
    if (str->reserve(range_cnt_length + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(range_cnt_str, range_cnt_length);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  } else {
    if (str->reserve(range_cnt_length))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(range_cnt_str, range_cnt_length);
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_multi_range_cnt_with_name_part(
  ulong sql_type,
  uint multi_range_cnt
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_multi_range_cnt_with_name_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    case SPIDER_SQL_TYPE_TMP_SQL:
      str = &tmp_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_multi_range_cnt_with_name(str, multi_range_cnt);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_multi_range_cnt_with_name(
  spider_string *str,
  uint multi_range_cnt
) {
  int range_cnt_length;
  char range_cnt_str[SPIDER_SQL_INT_LEN];
  DBUG_ENTER("spider_odbc_handler::append_multi_range_cnt_with_name");
  DBUG_PRINT("info",("spider this=%p", this));
  range_cnt_length = my_sprintf(range_cnt_str, (range_cnt_str, "%u",
    multi_range_cnt));
  if (str->reserve(range_cnt_length + SPIDER_SQL_SPACE_LEN +
    SPIDER_SQL_ID_LEN + SPIDER_SQL_COMMA_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(range_cnt_str, range_cnt_length);
  str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
  str->q_append(SPIDER_SQL_ID_STR, SPIDER_SQL_ID_LEN);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_open_handler_part(
  ulong sql_type,
  uint handler_id,
  SPIDER_CONN *conn,
  int link_idx
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_open_handler_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_HANDLER:
      str = &ha_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_open_handler(str, handler_id, conn, link_idx);
  exec_ha_sql = str;
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_open_handler(
  spider_string *str,
  uint handler_id,
  SPIDER_CONN *conn,
  int link_idx
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::append_open_handler");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider link_idx=%d", link_idx));
  DBUG_PRINT("info",("spider m_handler_cid=%s",
    spider->m_handler_cid[link_idx]));
  if (str->reserve(SPIDER_SQL_HANDLER_LEN))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(SPIDER_SQL_HANDLER_STR, SPIDER_SQL_HANDLER_LEN);
  if ((err = odbc_share->append_table_name(str,
      spider->conn_link_idx[link_idx])))
    DBUG_RETURN(err);
  if (str->reserve(SPIDER_SQL_OPEN_LEN + SPIDER_SQL_AS_LEN +
    SPIDER_SQL_HANDLER_CID_LEN))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(SPIDER_SQL_OPEN_STR, SPIDER_SQL_OPEN_LEN);
  str->q_append(SPIDER_SQL_AS_STR, SPIDER_SQL_AS_LEN);
  str->q_append(spider->m_handler_cid[link_idx], SPIDER_SQL_HANDLER_CID_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_close_handler_part(
  ulong sql_type,
  int link_idx
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_close_handler_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_HANDLER:
      str = &ha_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_close_handler(str, link_idx);
  exec_ha_sql = str;
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_close_handler(
  spider_string *str,
  int link_idx
) {
  DBUG_ENTER("spider_odbc_handler::append_close_handler");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_HANDLER_LEN + SPIDER_SQL_CLOSE_LEN +
    SPIDER_SQL_HANDLER_CID_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_HANDLER_STR, SPIDER_SQL_HANDLER_LEN);
  str->q_append(spider->m_handler_cid[link_idx],
    SPIDER_SQL_HANDLER_CID_LEN);
  str->q_append(SPIDER_SQL_CLOSE_STR, SPIDER_SQL_CLOSE_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_insert_terminator_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_insert_terminator_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_INSERT_SQL:
      str = &insert_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_insert_terminator(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_insert_terminator(
  spider_string *str
) {
  DBUG_ENTER("spider_odbc_handler::append_insert_terminator");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider dup_update_sql.length=%u", dup_update_sql.length()));
  if (
    spider->result_list.insert_dup_update_pushdown &&
    dup_update_sql.length()
  ) {
    DBUG_PRINT("info",("spider add duplicate key update"));
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
    if (str->reserve(SPIDER_SQL_DUPLICATE_KEY_UPDATE_LEN +
      dup_update_sql.length()))
    {
      str->length(0);
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    str->q_append(SPIDER_SQL_DUPLICATE_KEY_UPDATE_STR,
      SPIDER_SQL_DUPLICATE_KEY_UPDATE_LEN);
    if (str->append(dup_update_sql))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  } else {
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_insert_values_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_insert_values_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_INSERT_SQL:
      str = &insert_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_insert_values(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_insert_values(
  spider_string *str
) {
  SPIDER_SHARE *share = spider->share;
  TABLE *table = spider->get_table();
  Field **field;
  bool add_value = FALSE;
  DBUG_ENTER("spider_odbc_handler::append_insert_values");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_OPEN_PAREN_LEN))
  {
    str->length(0);
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  for (field = table->field; *field; field++)
  {
    DBUG_PRINT("info",("spider field_index=%u", (*field)->field_index));
    if (
      bitmap_is_set(table->write_set, (*field)->field_index) ||
      bitmap_is_set(table->read_set, (*field)->field_index)
    ) {
#ifndef DBUG_OFF
      my_bitmap_map *tmp_map =
        dbug_tmp_use_all_columns(table, table->read_set);
#endif
      add_value = TRUE;
      DBUG_PRINT("info",("spider is_null()=%s",
        (*field)->is_null() ? "TRUE" : "FALSE"));
      DBUG_PRINT("info",("spider table->next_number_field=%p",
        table->next_number_field));
      DBUG_PRINT("info",("spider *field=%p", *field));
      DBUG_PRINT("info",("spider force_auto_increment=%s",
        (table->next_number_field && spider->force_auto_increment) ?
        "TRUE" : "FALSE"));
      if (
        (*field)->is_null() ||
        (
          table->next_number_field == *field &&
          !table->auto_increment_field_not_null &&
          !spider->force_auto_increment
        )
      ) {
        if (str->reserve(SPIDER_SQL_NULL_LEN + SPIDER_SQL_COMMA_LEN))
        {
#ifndef DBUG_OFF
          dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
          str->length(0);
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
        str->q_append(SPIDER_SQL_NULL_STR, SPIDER_SQL_NULL_LEN);
      } else {
        if (
          utility->append_column_value(spider, str, *field, NULL,
            share->access_charset) ||
          str->reserve(SPIDER_SQL_COMMA_LEN)
        ) {
#ifndef DBUG_OFF
          dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
          str->length(0);
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
      }
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
#ifndef DBUG_OFF
      dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
    }
  }
  if (add_value)
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  if (str->reserve(SPIDER_SQL_CLOSE_PAREN_LEN + SPIDER_SQL_COMMA_LEN))
  {
    str->length(0);
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_into_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_into_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_INSERT_SQL:
      str = &insert_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_into(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_into(
  spider_string *str
) {
  const TABLE *table = spider->get_table();
  Field **field;
  uint field_name_length = 0;
  DBUG_ENTER("spider_odbc_handler::append_into");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_INTO_LEN + odbc_share->db_nm_max_length +
    SPIDER_SQL_DOT_LEN + odbc_share->table_nm_max_length +
    utility->name_quote_length * 4 + SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_INTO_STR, SPIDER_SQL_INTO_LEN);
  insert_table_name_pos = str->length();
  append_table_name_with_adjusting(str, first_link_idx,
    SPIDER_SQL_TYPE_INSERT_SQL);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  for (field = table->field; *field; field++)
  {
    if (
      bitmap_is_set(table->write_set, (*field)->field_index) ||
      bitmap_is_set(table->read_set, (*field)->field_index)
    ) {
      field_name_length =
        odbc_share->column_name_str[(*field)->field_index].length();
      if (str->reserve(field_name_length +
        utility->name_quote_length * 2 + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      odbc_share->append_column_name(str, (*field)->field_index);
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
  }
  if (field_name_length)
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  if (str->reserve(SPIDER_SQL_CLOSE_PAREN_LEN + SPIDER_SQL_VALUES_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  str->q_append(SPIDER_SQL_VALUES_STR, SPIDER_SQL_VALUES_LEN);
  insert_pos = str->length();
  DBUG_RETURN(0);
}

void spider_odbc_handler::set_insert_to_pos(
  ulong sql_type
) {
  DBUG_ENTER("spider_odbc_handler::set_insert_to_pos");
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_INSERT_SQL:
      insert_sql.length(insert_pos);
      break;
    default:
      DBUG_ASSERT(0);
      break;
  }
  DBUG_VOID_RETURN;
}

int spider_odbc_handler::append_from_part(
  ulong sql_type,
  int link_idx
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_from_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_HANDLER:
      str = &ha_sql;
      break;
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      str = &update_sql;
      break;
    default:
      str = &sql;
      break;
  }
  err = append_from(str, sql_type, link_idx);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_from(
  spider_string *str,
  ulong sql_type,
  int link_idx
) {
  DBUG_ENTER("spider_odbc_handler::append_from");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider link_idx=%d", link_idx));
  int err = 0;
  if (sql_type == SPIDER_SQL_TYPE_HANDLER)
  {
    ha_table_name_pos = str->length();
    DBUG_PRINT("info",("spider ha_table_name_pos=%u", ha_table_name_pos));
    ha_sql_handler_id = spider->m_handler_id[link_idx];
    DBUG_PRINT("info",("spider ha_sql_handler_id=%u", ha_sql_handler_id));
    if (str->reserve(SPIDER_SQL_HANDLER_CID_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(spider->m_handler_cid[link_idx], SPIDER_SQL_HANDLER_CID_LEN);
    DBUG_PRINT("info",("spider m_handler_cid=%s",
      spider->m_handler_cid[link_idx]));
  } else {
    if (str->reserve(SPIDER_SQL_FROM_LEN + odbc_share->db_nm_max_length +
      SPIDER_SQL_DOT_LEN + odbc_share->table_nm_max_length +
      utility->name_quote_length * 4 + SPIDER_SQL_OPEN_PAREN_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
    table_name_pos = str->length();
    append_table_name_with_adjusting(str, link_idx, sql_type);
    if(spider_param_index_hint_pushdown(spider->wide_handler->trx->thd))
    {
      if((err = append_index_hint(str, link_idx, sql_type)))
      {
        DBUG_RETURN(err);
      }
    }
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_flush_tables_part(
  ulong sql_type,
  int link_idx,
  bool lock
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_flush_tables_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_OTHER_SQL:
      str = &spider->result_list.sqls[link_idx];
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_flush_tables(str, link_idx, lock);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_flush_tables(
  spider_string *str,
  int link_idx,
  bool lock
) {
  DBUG_ENTER("spider_odbc_handler::append_flush_tables");
  DBUG_PRINT("info",("spider this=%p", this));
  if (lock)
  {
    if (str->reserve(SPIDER_SQL_FLUSH_TABLES_LEN +
      SPIDER_SQL_WITH_READ_LOCK_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_FLUSH_TABLES_STR, SPIDER_SQL_FLUSH_TABLES_LEN);
    str->q_append(SPIDER_SQL_WITH_READ_LOCK_STR,
      SPIDER_SQL_WITH_READ_LOCK_LEN);
  } else {
    if (str->reserve(SPIDER_SQL_FLUSH_TABLES_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_FLUSH_TABLES_STR, SPIDER_SQL_FLUSH_TABLES_LEN);
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_optimize_table_part(
  ulong sql_type,
  int link_idx
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_optimize_table_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_OTHER_SQL:
      str = &spider->result_list.sqls[link_idx];
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_optimize_table(str, link_idx);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_optimize_table(
  spider_string *str,
  int link_idx
) {
  SPIDER_SHARE *share = spider->share;
  int conn_link_idx = spider->conn_link_idx[link_idx];
  int local_length = spider_param_internal_optimize_local(
    spider->wide_handler->trx->thd,
    share->internal_optimize_local) * SPIDER_SQL_SQL_LOCAL_LEN;
  DBUG_ENTER("spider_odbc_handler::append_optimize_table");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_SQL_OPTIMIZE_LEN + SPIDER_SQL_SQL_TABLE_LEN +
    local_length +
    odbc_share->db_names_str[conn_link_idx].length() +
    SPIDER_SQL_DOT_LEN +
    odbc_share->table_names_str[conn_link_idx].length() +
    utility->name_quote_length * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_OPTIMIZE_STR, SPIDER_SQL_SQL_OPTIMIZE_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  odbc_share->append_table_name(str, conn_link_idx);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_analyze_table_part(
  ulong sql_type,
  int link_idx
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_analyze_table_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_OTHER_SQL:
      str = &spider->result_list.sqls[link_idx];
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_analyze_table(str, link_idx);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_analyze_table(
  spider_string *str,
  int link_idx
) {
  SPIDER_SHARE *share = spider->share;
  int conn_link_idx = spider->conn_link_idx[link_idx];
  int local_length = spider_param_internal_optimize_local(
    spider->wide_handler->trx->thd,
    share->internal_optimize_local) * SPIDER_SQL_SQL_LOCAL_LEN;
  DBUG_ENTER("spider_odbc_handler::append_analyze_table");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_SQL_ANALYZE_LEN + SPIDER_SQL_SQL_TABLE_LEN +
    local_length +
    odbc_share->db_names_str[conn_link_idx].length() +
    SPIDER_SQL_DOT_LEN +
    odbc_share->table_names_str[conn_link_idx].length() +
    utility->name_quote_length * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ANALYZE_STR, SPIDER_SQL_SQL_ANALYZE_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  odbc_share->append_table_name(str, conn_link_idx);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_repair_table_part(
  ulong sql_type,
  int link_idx,
  HA_CHECK_OPT* check_opt
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_repair_table_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_OTHER_SQL:
      str = &spider->result_list.sqls[link_idx];
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_repair_table(str, link_idx, check_opt);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_repair_table(
  spider_string *str,
  int link_idx,
  HA_CHECK_OPT* check_opt
) {
  SPIDER_SHARE *share = spider->share;
  int conn_link_idx = spider->conn_link_idx[link_idx];
  int local_length = spider_param_internal_optimize_local(
    spider->wide_handler->trx->thd,
    share->internal_optimize_local) * SPIDER_SQL_SQL_LOCAL_LEN;
  DBUG_ENTER("spider_odbc_handler::append_repair_table");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_SQL_REPAIR_LEN + SPIDER_SQL_SQL_TABLE_LEN +
    local_length +
    odbc_share->db_names_str[conn_link_idx].length() +
    SPIDER_SQL_DOT_LEN +
    odbc_share->table_names_str[conn_link_idx].length() +
    utility->name_quote_length * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_REPAIR_STR, SPIDER_SQL_SQL_REPAIR_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  odbc_share->append_table_name(str, conn_link_idx);
  if (check_opt->flags & T_QUICK)
  {
    if (str->reserve(SPIDER_SQL_SQL_QUICK_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_QUICK_STR, SPIDER_SQL_SQL_QUICK_LEN);
  }
  if (check_opt->flags & T_EXTEND)
  {
    if (str->reserve(SPIDER_SQL_SQL_EXTENDED_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_EXTENDED_STR, SPIDER_SQL_SQL_EXTENDED_LEN);
  }
  if (check_opt->sql_flags & TT_USEFRM)
  {
    if (str->reserve(SPIDER_SQL_SQL_USE_FRM_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_USE_FRM_STR, SPIDER_SQL_SQL_USE_FRM_LEN);
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_check_table_part(
  ulong sql_type,
  int link_idx,
  HA_CHECK_OPT* check_opt
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_check_table_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_OTHER_SQL:
      str = &spider->result_list.sqls[link_idx];
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_check_table(str, link_idx, check_opt);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_check_table(
  spider_string *str,
  int link_idx,
  HA_CHECK_OPT* check_opt
) {
  int conn_link_idx = spider->conn_link_idx[link_idx];
  DBUG_ENTER("spider_odbc_handler::append_check_table");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_SQL_CHECK_TABLE_LEN +
    odbc_share->db_names_str[conn_link_idx].length() +
    SPIDER_SQL_DOT_LEN +
    odbc_share->table_names_str[conn_link_idx].length() +
    utility->name_quote_length * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_CHECK_TABLE_STR,
    SPIDER_SQL_SQL_CHECK_TABLE_LEN);
  odbc_share->append_table_name(str, conn_link_idx);
  if (check_opt->flags & T_QUICK)
  {
    if (str->reserve(SPIDER_SQL_SQL_QUICK_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_QUICK_STR, SPIDER_SQL_SQL_QUICK_LEN);
  }
  if (check_opt->flags & T_FAST)
  {
    if (str->reserve(SPIDER_SQL_SQL_FAST_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_FAST_STR, SPIDER_SQL_SQL_FAST_LEN);
  }
  if (check_opt->flags & T_MEDIUM)
  {
    if (str->reserve(SPIDER_SQL_SQL_MEDIUM_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_MEDIUM_STR, SPIDER_SQL_SQL_MEDIUM_LEN);
  }
  if (check_opt->flags & T_EXTEND)
  {
    if (str->reserve(SPIDER_SQL_SQL_EXTENDED_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_EXTENDED_STR, SPIDER_SQL_SQL_EXTENDED_LEN);
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_enable_keys_part(
  ulong sql_type,
  int link_idx
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_enable_keys_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_OTHER_SQL:
      str = &spider->result_list.sqls[link_idx];
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_enable_keys(str, link_idx);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_enable_keys(
  spider_string *str,
  int link_idx
) {
  int conn_link_idx = spider->conn_link_idx[link_idx];
  DBUG_ENTER("spider_odbc_handler::append_enable_keys");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_SQL_ALTER_TABLE_LEN +
    odbc_share->db_names_str[conn_link_idx].length() +
    SPIDER_SQL_DOT_LEN +
    odbc_share->table_names_str[conn_link_idx].length() +
    utility->name_quote_length * 4 +
    SPIDER_SQL_SQL_ENABLE_KEYS_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ALTER_TABLE_STR,
    SPIDER_SQL_SQL_ALTER_TABLE_LEN);
  odbc_share->append_table_name(str, conn_link_idx);
  str->q_append(SPIDER_SQL_SQL_ENABLE_KEYS_STR,
    SPIDER_SQL_SQL_ENABLE_KEYS_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_disable_keys_part(
  ulong sql_type,
  int link_idx
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_disable_keys_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_OTHER_SQL:
      str = &spider->result_list.sqls[link_idx];
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_disable_keys(str, link_idx);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_disable_keys(
  spider_string *str,
  int link_idx
) {
  int conn_link_idx = spider->conn_link_idx[link_idx];
  DBUG_ENTER("spider_odbc_handler::append_disable_keys");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_SQL_ALTER_TABLE_LEN +
    odbc_share->db_names_str[conn_link_idx].length() +
    SPIDER_SQL_DOT_LEN +
    odbc_share->table_names_str[conn_link_idx].length() +
    utility->name_quote_length * 4 +
    SPIDER_SQL_SQL_DISABLE_KEYS_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ALTER_TABLE_STR,
    SPIDER_SQL_SQL_ALTER_TABLE_LEN);
  odbc_share->append_table_name(str, conn_link_idx);
  str->q_append(SPIDER_SQL_SQL_DISABLE_KEYS_STR,
    SPIDER_SQL_SQL_DISABLE_KEYS_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_delete_all_rows_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_delete_all_rows_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_DELETE_SQL:
      str = &update_sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_delete_all_rows(str, sql_type);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_delete_all_rows(
  spider_string *str,
  ulong sql_type
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::append_delete_all_rows");
  DBUG_PRINT("info",("spider this=%p", this));
  if (spider->wide_handler->sql_command == SQLCOM_TRUNCATE)
  {
    if ((err = append_truncate(str, sql_type, first_link_idx)))
      DBUG_RETURN(err);
  } else {
    if (
      (err = append_delete(str)) ||
      (err = append_from(str, sql_type, first_link_idx))
    )
      DBUG_RETURN(err);
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_truncate(
  spider_string *str,
  ulong sql_type,
  int link_idx
) {
  DBUG_ENTER("spider_odbc_handler::append_truncate");
  if (str->reserve(SPIDER_SQL_TRUNCATE_TABLE_LEN +
    odbc_share->db_nm_max_length +
    SPIDER_SQL_DOT_LEN + odbc_share->table_nm_max_length +
    utility->name_quote_length * 4 + SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_TRUNCATE_TABLE_STR, SPIDER_SQL_TRUNCATE_TABLE_LEN);
  table_name_pos = str->length();
  append_table_name_with_adjusting(str, link_idx, sql_type);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_explain_select_part(
  key_range *start_key,
  key_range *end_key,
  ulong sql_type,
  int link_idx
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_explain_select_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_OTHER_SQL:
      str = &spider->result_list.sqls[link_idx];
      break;
    default:
      DBUG_RETURN(0);
  }
  err =
    append_explain_select(str, start_key, end_key, sql_type, link_idx);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_explain_select(
  spider_string *str,
  key_range *start_key,
  key_range *end_key,
  ulong sql_type,
  int link_idx
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::append_explain_select");
  DBUG_PRINT("info",("spider this=%p", this));
  if (str->reserve(SPIDER_SQL_EXPLAIN_SELECT_LEN))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(SPIDER_SQL_EXPLAIN_SELECT_STR, SPIDER_SQL_EXPLAIN_SELECT_LEN);
  if (
    (err = append_from(str, sql_type, link_idx)) ||
    (err = append_key_where(str, NULL, NULL, start_key, end_key,
      sql_type, FALSE))
  ) {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  DBUG_RETURN(0);
}

/********************************************************************
 * Determine whether the current query's projection list
 * consists solely of the specified column.
 *
 * Params   IN      - field_index:
 *                    Field index of the column of interest within
 *                    its table.
 *
 * Returns  TRUE    - if the query's projection list consists
 *                    solely of the specified column.
 *          FALSE   - otherwise.
 ********************************************************************/
bool spider_odbc_handler::is_sole_projection_field(
  uint16 field_index
) {
  // Determine whether the projection list consists solely of the field of interest
  bool            is_field_in_projection_list = FALSE;
  TABLE*          table                       = spider->get_table();
  uint16          projection_field_count      = 0;
  uint16          projection_field_index;
  Field**         field;
  DBUG_ENTER( "spider_odbc_handler::is_sole_projection_field" );

  for ( field = table->field; *field ; field++ )
  {
    projection_field_index = ( *field )->field_index;

    if ( !( minimum_select_bit_is_set( projection_field_index ) ) )
    {
      // Current field is not in the projection list
      continue;
    }

    projection_field_count++;

    if ( !is_field_in_projection_list )
    {
      if ( field_index == projection_field_index )
      {
        // Field of interest is in the projection list
        is_field_in_projection_list = TRUE;
      }
    }

    if ( is_field_in_projection_list && ( projection_field_count != 1 ) )
    {
      // Field of interest is not the sole column in the projection list
      DBUG_RETURN( FALSE );
    }
  }

  if ( is_field_in_projection_list && ( projection_field_count == 1 ) )
  {
    // Field of interest is the only column in the projection list
    DBUG_RETURN( TRUE );
  }

  DBUG_RETURN( FALSE );
}

bool spider_odbc_handler::is_bulk_insert_exec_period(
  bool bulk_end
) {
  DBUG_ENTER("spider_odbc_handler::is_bulk_insert_exec_period");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider insert_sql.length=%u", insert_sql.length()));
  DBUG_PRINT("info",("spider insert_pos=%d", insert_pos));
  DBUG_PRINT("info",("spider insert_sql=%s", insert_sql.c_ptr_safe()));
  if (
/*
    (bulk_end || (int) insert_sql.length() >= spider->bulk_size) &&
*/
    (int) insert_sql.length() > insert_pos
  ) {
    DBUG_RETURN(TRUE);
  }
  DBUG_RETURN(FALSE);
}

bool spider_odbc_handler::sql_is_filled_up(
  ulong sql_type
) {
  DBUG_ENTER("spider_odbc_handler::sql_is_filled_up");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(filled_up);
}

bool spider_odbc_handler::sql_is_empty(
  ulong sql_type
) {
  bool is_empty;
  DBUG_ENTER("spider_odbc_handler::sql_is_empty");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      is_empty = (sql.length() == 0);
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
      is_empty = (insert_sql.length() == 0);
      break;
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      is_empty = (update_sql.length() == 0);
      break;
    case SPIDER_SQL_TYPE_TMP_SQL:
      is_empty = (tmp_sql.length() == 0);
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      is_empty = (ha_sql.length() == 0);
      break;
    default:
      is_empty = TRUE;
      break;
  }
  DBUG_RETURN(is_empty);
}

bool spider_odbc_handler::support_multi_split_read()
{
  DBUG_ENTER("spider_odbc_handler::support_multi_split_read");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

bool spider_odbc_handler::support_bulk_update()
{
  DBUG_ENTER("spider_odbc_handler::support_bulk_update");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

int spider_odbc_handler::bulk_tmp_table_insert()
{
  int err;
  DBUG_ENTER("spider_odbc_handler::bulk_tmp_table_insert");
  DBUG_PRINT("info",("spider this=%p", this));
  err = store_sql_to_bulk_tmp_table(&update_sql, upd_tmp_tbl);
  DBUG_RETURN(err);
}

int spider_odbc_handler::bulk_tmp_table_insert(
  int link_idx
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::bulk_tmp_table_insert");
  DBUG_PRINT("info",("spider this=%p", this));
  err = store_sql_to_bulk_tmp_table(
    &spider->result_list.update_sqls[link_idx],
    spider->result_list.upd_tmp_tbls[link_idx]);
  DBUG_RETURN(err);
}

int spider_odbc_handler::bulk_tmp_table_end_bulk_insert()
{
  int err;
  DBUG_ENTER("spider_odbc_handler::bulk_tmp_table_end_bulk_insert");
  DBUG_PRINT("info",("spider this=%p", this));
  if ((err = upd_tmp_tbl->file->ha_end_bulk_insert()))
  {
    DBUG_RETURN(err);
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::bulk_tmp_table_rnd_init()
{
  int err;
  DBUG_ENTER("spider_odbc_handler::bulk_tmp_table_rnd_init");
  DBUG_PRINT("info",("spider this=%p", this));
  upd_tmp_tbl->file->extra(HA_EXTRA_CACHE);
  if ((err = upd_tmp_tbl->file->ha_rnd_init(TRUE)))
  {
    DBUG_RETURN(err);
  }
  reading_from_bulk_tmp_table = TRUE;
  DBUG_RETURN(0);
}

int spider_odbc_handler::bulk_tmp_table_rnd_next()
{
  int err;
  DBUG_ENTER("spider_odbc_handler::bulk_tmp_table_rnd_next");
  DBUG_PRINT("info",("spider this=%p", this));
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50200
  err = upd_tmp_tbl->file->ha_rnd_next(upd_tmp_tbl->record[0]);
#else
  err = upd_tmp_tbl->file->rnd_next(upd_tmp_tbl->record[0]);
#endif
  if (!err)
  {
    err = restore_sql_from_bulk_tmp_table(&insert_sql, upd_tmp_tbl);
  }
  DBUG_RETURN(err);
}

int spider_odbc_handler::bulk_tmp_table_rnd_end()
{
  int err;
  DBUG_ENTER("spider_odbc_handler::bulk_tmp_table_rnd_end");
  DBUG_PRINT("info",("spider this=%p", this));
  reading_from_bulk_tmp_table = FALSE;
  if ((err = upd_tmp_tbl->file->ha_rnd_end()))
  {
    DBUG_RETURN(err);
  }
  DBUG_RETURN(0);
}

bool spider_odbc_handler::need_copy_for_update(
  int link_idx
) {
  int all_link_idx = spider->conn_link_idx[link_idx];
  DBUG_ENTER("spider_odbc_handler::need_copy_for_update");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(!odbc_share->same_db_table_name ||
    spider->share->link_statuses[all_link_idx] == SPIDER_LINK_STATUS_RECOVERY);
}

bool spider_odbc_handler::bulk_tmp_table_created()
{
  DBUG_ENTER("spider_odbc_handler::bulk_tmp_table_created");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(upd_tmp_tbl);
}

int spider_odbc_handler::mk_bulk_tmp_table_and_bulk_start()
{
  THD *thd = spider->wide_handler->trx->thd;
  TABLE *table = spider->get_table();
  DBUG_ENTER("spider_odbc_handler::mk_bulk_tmp_table_and_bulk_start");
  DBUG_PRINT("info",("spider this=%p", this));
  if (!upd_tmp_tbl)
  {
#ifdef SPIDER_use_LEX_CSTRING_for_Field_blob_constructor
    LEX_CSTRING field_name = {STRING_WITH_LEN("a")};
    if (!(upd_tmp_tbl = spider_mk_sys_tmp_table(
      thd, table, &upd_tmp_tbl_prm, &field_name, update_sql.charset())))
#else
    if (!(upd_tmp_tbl = spider_mk_sys_tmp_table(
      thd, table, &upd_tmp_tbl_prm, "a", update_sql.charset())))
#endif
    {
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    upd_tmp_tbl->file->extra(HA_EXTRA_WRITE_CACHE);
    upd_tmp_tbl->file->ha_start_bulk_insert((ha_rows) 0);
  }
  DBUG_RETURN(0);
}

void spider_odbc_handler::rm_bulk_tmp_table()
{
  DBUG_ENTER("spider_odbc_handler::rm_bulk_tmp_table");
  DBUG_PRINT("info",("spider this=%p", this));
  if (upd_tmp_tbl)
  {
    spider_rm_sys_tmp_table(spider->wide_handler->trx->thd, upd_tmp_tbl,
      &upd_tmp_tbl_prm);
    upd_tmp_tbl = NULL;
  }
  DBUG_VOID_RETURN;
}

int spider_odbc_handler::store_sql_to_bulk_tmp_table(
  spider_string *str,
  TABLE *tmp_table
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::store_sql_to_bulk_tmp_table");
  DBUG_PRINT("info",("spider this=%p", this));
  tmp_table->field[0]->set_notnull();
  tmp_table->field[0]->store(str->ptr(), str->length(), str->charset());
  if ((err = tmp_table->file->ha_write_row(tmp_table->record[0])))
    DBUG_RETURN(err);
  DBUG_RETURN(0);
}

int spider_odbc_handler::restore_sql_from_bulk_tmp_table(
  spider_string *str,
  TABLE *tmp_table
) {
  DBUG_ENTER("spider_odbc_handler::restore_sql_from_bulk_tmp_table");
  DBUG_PRINT("info",("spider this=%p", this));
  tmp_table->field[0]->val_str(str->get_str());
  str->mem_calc();
  DBUG_RETURN(0);
}

int spider_odbc_handler::insert_lock_tables_list(
  SPIDER_CONN *conn,
  int link_idx
) {
  spider_db_odbc *db_conn = (spider_db_odbc *) conn->db_conn;
  SPIDER_LINK_FOR_HASH *tmp_link_for_hash2 = &link_for_hash[link_idx];
  DBUG_ENTER("spider_odbc_handler::insert_lock_tables_list");
  DBUG_PRINT("info",("spider this=%p", this));
  uint old_elements =
    db_conn->lock_table_hash.array.max_element;
#ifdef HASH_UPDATE_WITH_HASH_VALUE
  if (my_hash_insert_with_hash_value(
    &db_conn->lock_table_hash,
    tmp_link_for_hash2->db_table_str_hash_value,
    (uchar*) tmp_link_for_hash2))
#else
  if (my_hash_insert(&db_conn->lock_table_hash,
    (uchar*) tmp_link_for_hash2))
#endif
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  if (db_conn->lock_table_hash.array.max_element > old_elements)
  {
    spider_alloc_calc_mem(spider_current_trx,
      db_conn->lock_table_hash,
      (db_conn->lock_table_hash.array.max_element - old_elements) *
      db_conn->lock_table_hash.array.size_of_element);
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_lock_tables_list(
  SPIDER_CONN *conn,
  int link_idx,
  int *appended
) {
  int err;
  SPIDER_LINK_FOR_HASH *tmp_link_for_hash, *tmp_link_for_hash2;
  int conn_link_idx = spider->conn_link_idx[link_idx];
  spider_db_odbc *db_conn = (spider_db_odbc *) conn->db_conn;
  DBUG_ENTER("spider_odbc_handler::append_lock_tables_list");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider db_conn=%p", db_conn));
  tmp_link_for_hash2 = &link_for_hash[link_idx];
  tmp_link_for_hash2->db_table_str =
    &odbc_share->db_table_str[conn_link_idx];
  DBUG_PRINT("info",("spider db_table_str=%s",
    tmp_link_for_hash2->db_table_str->c_ptr_safe()));
#ifdef SPIDER_HAS_HASH_VALUE_TYPE
  tmp_link_for_hash2->db_table_str_hash_value =
    odbc_share->db_table_str_hash_value[conn_link_idx];
  if (!(tmp_link_for_hash = (SPIDER_LINK_FOR_HASH *)
    my_hash_search_using_hash_value(
      &db_conn->lock_table_hash,
      tmp_link_for_hash2->db_table_str_hash_value,
      (uchar*) tmp_link_for_hash2->db_table_str->ptr(),
      tmp_link_for_hash2->db_table_str->length())))
#else
  if (!(tmp_link_for_hash = (SPIDER_LINK_FOR_HASH *) my_hash_search(
    &db_conn->lock_table_hash,
    (uchar*) tmp_link_for_hash2->db_table_str->ptr(),
    tmp_link_for_hash2->db_table_str->length())))
#endif
  {
    if ((err = insert_lock_tables_list(conn, link_idx)))
      DBUG_RETURN(err);
    *appended = 1;
  } else {
    if (tmp_link_for_hash->spider->wide_handler->lock_type <
      spider->wide_handler->lock_type)
    {
#ifdef HASH_UPDATE_WITH_HASH_VALUE
      my_hash_delete_with_hash_value(
        &db_conn->lock_table_hash,
        tmp_link_for_hash->db_table_str_hash_value,
        (uchar*) tmp_link_for_hash);
#else
      my_hash_delete(&db_conn->lock_table_hash,
        (uchar*) tmp_link_for_hash);
#endif
      uint old_elements =
        db_conn->lock_table_hash.array.max_element;
#ifdef HASH_UPDATE_WITH_HASH_VALUE
      if (my_hash_insert_with_hash_value(
        &db_conn->lock_table_hash,
        tmp_link_for_hash2->db_table_str_hash_value,
        (uchar*) tmp_link_for_hash2))
#else
      if (my_hash_insert(&db_conn->lock_table_hash,
        (uchar*) tmp_link_for_hash2))
#endif
      {
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
      if (db_conn->lock_table_hash.array.max_element > old_elements)
      {
        spider_alloc_calc_mem(spider_current_trx,
          db_conn->lock_table_hash,
          (db_conn->lock_table_hash.array.max_element - old_elements) *
          db_conn->lock_table_hash.array.size_of_element);
      }
    }
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::realloc_sql(
  ulong *realloced
) {
  THD *thd = spider->wide_handler->trx->thd;
  st_spider_share *share = spider->share;
  int init_sql_alloc_size =
    spider_param_init_sql_alloc_size(thd, share->init_sql_alloc_size);
  DBUG_ENTER("spider_odbc_handler::realloc_sql");
  DBUG_PRINT("info",("spider this=%p", this));
  if ((int) sql.alloced_length() > init_sql_alloc_size * 2)
  {
    sql.free();
    if (sql.real_alloc(init_sql_alloc_size))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    *realloced |= SPIDER_SQL_TYPE_SELECT_SQL;
  }
  if ((int) ha_sql.alloced_length() > init_sql_alloc_size * 2)
  {
    ha_sql.free();
    if (ha_sql.real_alloc(init_sql_alloc_size))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    *realloced |= SPIDER_SQL_TYPE_SELECT_SQL;
  }
  if ((int) dup_update_sql.alloced_length() > init_sql_alloc_size * 2)
  {
    dup_update_sql.free();
    if (dup_update_sql.real_alloc(init_sql_alloc_size))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  if ((int) insert_sql.alloced_length() > init_sql_alloc_size * 2)
  {
    insert_sql.free();
    if (insert_sql.real_alloc(init_sql_alloc_size))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    *realloced |= SPIDER_SQL_TYPE_INSERT_SQL;
  }
  if ((int) update_sql.alloced_length() > init_sql_alloc_size * 2)
  {
    update_sql.free();
    if (update_sql.real_alloc(init_sql_alloc_size))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    *realloced |= (SPIDER_SQL_TYPE_UPDATE_SQL | SPIDER_SQL_TYPE_DELETE_SQL);
  }
  update_sql.length(0);
  if ((int) tmp_sql.alloced_length() > init_sql_alloc_size * 2)
  {
    tmp_sql.free();
    if (tmp_sql.real_alloc(init_sql_alloc_size))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    *realloced |= SPIDER_SQL_TYPE_TMP_SQL;
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::reset_sql(
  ulong sql_type
) {
  DBUG_ENTER("spider_odbc_handler::reset_sql");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql_type & SPIDER_SQL_TYPE_SELECT_SQL)
  {
    sql.length(0);
  }
  if (sql_type & SPIDER_SQL_TYPE_INSERT_SQL)
  {
    insert_sql.length(0);
  }
  if (sql_type & (SPIDER_SQL_TYPE_UPDATE_SQL | SPIDER_SQL_TYPE_DELETE_SQL |
    SPIDER_SQL_TYPE_BULK_UPDATE_SQL))
  {
    update_sql.length(0);
  }
  if (sql_type & SPIDER_SQL_TYPE_TMP_SQL)
  {
    tmp_sql.length(0);
  }
  if (sql_type & SPIDER_SQL_TYPE_HANDLER)
  {
    ha_sql.length(0);
  }
  DBUG_RETURN(0);
}

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
int spider_odbc_handler::reset_keys(
  ulong sql_type
) {
  DBUG_ENTER("spider_odbc_handler::reset_keys");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_odbc_handler::reset_upds(
  ulong sql_type
) {
  DBUG_ENTER("spider_odbc_handler::reset_upds");
  DBUG_PRINT("info",("spider this=%p", this));
  hs_upds.clear();
  DBUG_RETURN(0);
}

int spider_odbc_handler::reset_strs(
  ulong sql_type
) {
  DBUG_ENTER("spider_odbc_handler::reset_strs");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_odbc_handler::reset_strs_pos(
  ulong sql_type
) {
  DBUG_ENTER("spider_odbc_handler::reset_strs_pos");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_ASSERT(0);
  DBUG_RETURN(0);
}

int spider_odbc_handler::push_back_upds(
  SPIDER_HS_STRING_REF &info
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::push_back_upds");
  DBUG_PRINT("info",("spider this=%p", this));
  err = hs_upds.push_back(info);
  DBUG_RETURN(err);
}
#endif

bool spider_odbc_handler::need_lock_before_set_sql_for_exec(
  ulong sql_type
) {
  DBUG_ENTER("spider_odbc_handler::need_lock_before_set_sql_for_exec");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

#ifdef SPIDER_HAS_GROUP_BY_HANDLER
int spider_odbc_handler::set_sql_for_exec(
  ulong sql_type,
  int link_idx,
  SPIDER_LINK_IDX_CHAIN *link_idx_chain
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::set_sql_for_exec");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql_type & SPIDER_SQL_TYPE_SELECT_SQL)
  {
    if ((err = utility->reappend_tables(
      spider->fields, link_idx_chain, &sql)))
      DBUG_RETURN(err);
    exec_sql = &sql;
  }
  DBUG_RETURN(0);
}
#endif

int spider_odbc_handler::set_sql_for_exec(
  ulong sql_type,
  int link_idx
) {
  int err;
  uint tmp_pos;
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  int all_link_idx = spider->conn_link_idx[link_idx];
  DBUG_ENTER("spider_odbc_handler::set_sql_for_exec");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql_type & (SPIDER_SQL_TYPE_SELECT_SQL | SPIDER_SQL_TYPE_TMP_SQL))
  {
    if (odbc_share->same_db_table_name || link_idx == first_link_idx)
    {
      if (sql_type & SPIDER_SQL_TYPE_SELECT_SQL)
        exec_sql = &sql;
      if (sql_type & SPIDER_SQL_TYPE_TMP_SQL)
        exec_tmp_sql = &tmp_sql;
    } else {
      char tmp_table_name[MAX_FIELD_WIDTH * 2],
        tgt_table_name[MAX_FIELD_WIDTH * 2];
      int tmp_table_name_length;
      spider_string tgt_table_name_str(tgt_table_name,
        MAX_FIELD_WIDTH * 2,
        odbc_share->db_names_str[link_idx].charset());
      const char *table_names[2], *table_aliases[2];
      uint table_name_lengths[2], table_alias_lengths[2];
      tgt_table_name_str.init_calc_mem(312);
      tgt_table_name_str.length(0);
      if (result_list->tmp_table_join && spider->bka_mode != 2)
      {
        create_tmp_bka_table_name(tmp_table_name, &tmp_table_name_length,
          link_idx);
        append_table_name_with_adjusting(&tgt_table_name_str, link_idx,
          SPIDER_SQL_TYPE_TMP_SQL);
        table_names[0] = tmp_table_name;
        table_names[1] = tgt_table_name_str.ptr();
        table_name_lengths[0] = tmp_table_name_length;
        table_name_lengths[1] = tgt_table_name_str.length();
        table_aliases[0] = SPIDER_SQL_A_STR;
        table_aliases[1] = SPIDER_SQL_B_STR;
        table_alias_lengths[0] = SPIDER_SQL_A_LEN;
        table_alias_lengths[1] = SPIDER_SQL_B_LEN;
      }
      if (sql_type & SPIDER_SQL_TYPE_SELECT_SQL)
      {
        exec_sql = &result_list->sqls[link_idx];
        if (exec_sql->copy(sql))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        else if (result_list->use_union)
        {
          if ((err = reset_union_table_name(exec_sql, link_idx,
            SPIDER_SQL_TYPE_SELECT_SQL)))
            DBUG_RETURN(err);
        } else {
          tmp_pos = exec_sql->length();
          exec_sql->length(table_name_pos);
          if (result_list->tmp_table_join && spider->bka_mode != 2)
          {
            if ((err = utility->append_from_with_alias(
              exec_sql, table_names, table_name_lengths,
              table_aliases, table_alias_lengths, 2,
              &table_name_pos, TRUE))
            )
              DBUG_RETURN(err);
            exec_sql->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
          } else {
            append_table_name_with_adjusting(exec_sql, link_idx,
              SPIDER_SQL_TYPE_SELECT_SQL);
          }
          exec_sql->length(tmp_pos);
        }
      }
      if (sql_type & SPIDER_SQL_TYPE_TMP_SQL)
      {
        exec_tmp_sql = &result_list->tmp_sqls[link_idx];
        if (result_list->tmp_table_join && spider->bka_mode != 2)
        {
          if (exec_tmp_sql->copy(tmp_sql))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          else {
            tmp_pos = exec_tmp_sql->length();
            exec_tmp_sql->length(tmp_sql_pos1);
            exec_tmp_sql->q_append(tmp_table_name, tmp_table_name_length);
            exec_tmp_sql->length(tmp_sql_pos2);
            exec_tmp_sql->q_append(tmp_table_name, tmp_table_name_length);
            exec_tmp_sql->length(tmp_sql_pos3);
            exec_tmp_sql->q_append(tmp_table_name, tmp_table_name_length);
            exec_tmp_sql->length(tmp_pos);
          }
        }
      }
    }
  }
  if (sql_type & SPIDER_SQL_TYPE_INSERT_SQL)
  {
    if (odbc_share->same_db_table_name || link_idx == first_link_idx)
      exec_insert_sql = &insert_sql;
    else {
      exec_insert_sql = &result_list->insert_sqls[link_idx];
      if (exec_insert_sql->copy(insert_sql))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      DBUG_PRINT("info",("spider exec_insert_sql=%s",
        exec_insert_sql->c_ptr_safe()));
      tmp_pos = exec_insert_sql->length();
      exec_insert_sql->length(insert_table_name_pos);
      append_table_name_with_adjusting(exec_insert_sql, link_idx,
        sql_type);
      exec_insert_sql->length(tmp_pos);
      DBUG_PRINT("info",("spider exec_insert_sql->length=%u",
        exec_insert_sql->length()));
      DBUG_PRINT("info",("spider exec_insert_sql=%s",
        exec_insert_sql->c_ptr_safe()));
    }
  }
  if (sql_type & SPIDER_SQL_TYPE_BULK_UPDATE_SQL)
  {
    if (reading_from_bulk_tmp_table)
    {
      if (
        odbc_share->same_db_table_name &&
        share->link_statuses[all_link_idx] != SPIDER_LINK_STATUS_RECOVERY
      ) {
        exec_update_sql = &insert_sql;
      } else if (!spider->result_list.upd_tmp_tbls[link_idx])
      {
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      } else {
        exec_update_sql = &spider->result_list.insert_sqls[link_idx];
        if ((err = restore_sql_from_bulk_tmp_table(exec_update_sql,
          spider->result_list.upd_tmp_tbls[link_idx])))
        {
          DBUG_RETURN(err);
        }
      }
    } else {
      if (
        odbc_share->same_db_table_name &&
        share->link_statuses[all_link_idx] != SPIDER_LINK_STATUS_RECOVERY
      ) {
        exec_update_sql = &update_sql;
      } else {
        exec_update_sql = &spider->result_list.update_sqls[link_idx];
      }
    }
  } else if (sql_type &
    (SPIDER_SQL_TYPE_UPDATE_SQL | SPIDER_SQL_TYPE_DELETE_SQL))
  {
    if (odbc_share->same_db_table_name || link_idx == first_link_idx)
      exec_update_sql = &update_sql;
    else {
      exec_update_sql = &spider->result_list.update_sqls[link_idx];
      if (exec_update_sql->copy(update_sql))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      tmp_pos = exec_update_sql->length();
      exec_update_sql->length(table_name_pos);
      append_table_name_with_adjusting(exec_update_sql, link_idx,
        sql_type);
      exec_update_sql->length(tmp_pos);
    }
  }
  if (sql_type & SPIDER_SQL_TYPE_HANDLER)
  {
    if (spider->m_handler_id[link_idx] == ha_sql_handler_id)
      exec_ha_sql = &ha_sql;
    else {
      exec_ha_sql = &result_list->sqls[link_idx];
      if (exec_ha_sql->copy(ha_sql))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      else {
        tmp_pos = exec_ha_sql->length();
        exec_ha_sql->length(ha_table_name_pos);
        append_table_name_with_adjusting(exec_ha_sql, link_idx,
          SPIDER_SQL_TYPE_HANDLER);
        exec_ha_sql->length(tmp_pos);
      }
    }
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::set_sql_for_exec(
  spider_db_copy_table *tgt_ct,
  ulong sql_type
) {
  spider_odbc_copy_table *mysql_ct = (spider_odbc_copy_table *) tgt_ct;
  DBUG_ENTER("spider_odbc_handler::set_sql_for_exec");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_INSERT_SQL:
      exec_insert_sql = &mysql_ct->sql;
      break;
    default:
      DBUG_ASSERT(0);
      break;
  }
  DBUG_RETURN(0);
}

#ifdef SPIDER_SQL_TYPE_DDL_SQL
int spider_odbc_handler::set_sql_for_exec(
  spider_db_sql *db_sql,
  int link_idx
) {
  uint tmp_pos;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::set_sql_for_exec");
  DBUG_PRINT("info",("spider this=%p", this));
  query[link_idx] = &result_list->sqls[link_idx];
  str = query[link_idx];
  if (str->reserve(db_sql->sql_end_pos[0] + db_sql->sql_end_pos[1] +
    SPIDER_SQL_SEMICOLON_LEN))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  if (db_sql->sql_end_pos[1])
  {
    str->copy(db_sql->sql_str[1]);
    str->length(db_sql->table_name_pos[1]);
    append_table_name_with_adjusting(str, link_idx, SPIDER_SQL_TYPE_DDL_SQL);
    str->length(db_sql->sql_end_pos[1]);
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
    str->q_append(db_sql->sql_str[0].ptr(), db_sql->sql_end_pos[0]);
    tmp_pos = str->length();
    str->length(str->length() - db_sql->sql_end_pos[0] +
      db_sql->table_name_pos[0]);
    append_table_name_with_adjusting(str, link_idx, SPIDER_SQL_TYPE_DDL_SQL);
    str->length(tmp_pos);
  } else {
    str->copy(db_sql->sql_str[0]);
    str->length(db_sql->table_name_pos[0]);
    append_table_name_with_adjusting(str, link_idx, SPIDER_SQL_TYPE_DDL_SQL);
    str->length(db_sql->sql_end_pos[0]);
  }
  DBUG_PRINT("info",("spider str=%s", str->c_ptr_safe()));
  DBUG_RETURN(0);
}
#endif

int spider_odbc_handler::execute_sql(
  ulong sql_type,
  SPIDER_CONN *conn,
  int quick_mode,
  int *need_mon
) {
  spider_string *tgt_sql;
  uint tgt_length;
  DBUG_ENTER("spider_odbc_handler::execute_sql");
  DBUG_PRINT("info",("spider this=%p", this));
  spider_db_odbc *db_conn = (spider_db_odbc *) conn->db_conn;
  db_conn->set_limit(limit);
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      DBUG_PRINT("info",("spider SPIDER_SQL_TYPE_SELECT_SQL"));
      tgt_sql = exec_sql;
      tgt_length = tgt_sql->length();
      break;
    case SPIDER_SQL_TYPE_INSERT_SQL:
      DBUG_PRINT("info",("spider SPIDER_SQL_TYPE_SELECT_SQL"));
      tgt_sql = exec_insert_sql;
      tgt_length = tgt_sql->length();
      break;
    case SPIDER_SQL_TYPE_UPDATE_SQL:
    case SPIDER_SQL_TYPE_DELETE_SQL:
    case SPIDER_SQL_TYPE_BULK_UPDATE_SQL:
      DBUG_PRINT("info",("spider %s",
        sql_type == SPIDER_SQL_TYPE_UPDATE_SQL ? "SPIDER_SQL_TYPE_UPDATE_SQL" :
        sql_type == SPIDER_SQL_TYPE_DELETE_SQL ? "SPIDER_SQL_TYPE_DELETE_SQL" :
        "SPIDER_SQL_TYPE_BULK_UPDATE_SQL"
      ));
      tgt_sql = exec_update_sql;
      tgt_length = tgt_sql->length();
      break;
    case SPIDER_SQL_TYPE_TMP_SQL:
      DBUG_PRINT("info",("spider SPIDER_SQL_TYPE_TMP_SQL"));
      tgt_sql = exec_tmp_sql;
      tgt_length = tgt_sql->length();
      break;
    case SPIDER_SQL_TYPE_DROP_TMP_TABLE_SQL:
      DBUG_PRINT("info",("spider SPIDER_SQL_TYPE_DROP_TMP_TABLE_SQL"));
      tgt_sql = exec_tmp_sql;
      tgt_length = tmp_sql_pos5;
      break;
    case SPIDER_SQL_TYPE_HANDLER:
      DBUG_PRINT("info",("spider SPIDER_SQL_TYPE_HANDLER"));
      tgt_sql = exec_ha_sql;
      tgt_length = tgt_sql->length();
      break;
#ifdef SPIDER_SQL_TYPE_DDL_SQL
    case SPIDER_SQL_TYPE_DDL_SQL:
      DBUG_PRINT("info",("spider SPIDER_SQL_TYPE_DDL_SQL"));
      tgt_sql = query[conn->link_idx];
      tgt_length = tgt_sql->length();
      break;
#endif
    default:
      /* nothing to do */
      DBUG_PRINT("info",("spider default"));
      DBUG_RETURN(0);
  }
  DBUG_RETURN(spider_db_query(
    conn,
    tgt_sql->ptr(),
    tgt_length,
    quick_mode,
    need_mon
  ));
}

int spider_odbc_handler::reset()
{
  DBUG_ENTER("spider_odbc_handler::reset");
  DBUG_PRINT("info",("spider this=%p", this));
  update_sql.length(0);
  DBUG_RETURN(0);
}

int spider_odbc_handler::get_last_auto_increment(
  int link_idx
) {
  SQLRETURN ret;
  SQLHSTMT hstm;
  spider_string *tnm =
    &odbc_share->table_names_str[spider->conn_link_idx[link_idx]];
  SPIDER_CONN *conn = spider->conns[link_idx];
  spider_db_odbc *db_conn = (spider_db_odbc *) conn->db_conn;
  SPIDER_SHARE *share = spider->share;
  TABLE *table = spider->get_table();
  uint field_index = table->next_number_field->field_index;
  spider_string *str = &sql;
  DBUG_ENTER("spider_odbc_handler::get_last_auto_increment");
  DBUG_PRINT("info",("spider hdbc:%p", db_conn->hdbc));
  stored_error = 0;
  str->length(0);
  if (str->reserve(SPIDER_SQL_SELECT_LEN + SPIDER_SQL_ODBC_MAX_LEN +
    odbc_share->get_column_name_length(field_index) +
    SPIDER_SQL_ODBC_CLOSE_FUNC_LEN + SPIDER_SQL_FROM_LEN + tnm->length()))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  str->q_append(SPIDER_SQL_SELECT_STR, SPIDER_SQL_SELECT_LEN);
  str->q_append(SPIDER_SQL_ODBC_MAX_STR, SPIDER_SQL_ODBC_MAX_LEN);
  (void) odbc_share->append_column_name(str, field_index);
  str->q_append(SPIDER_SQL_ODBC_CLOSE_FUNC_STR,
    SPIDER_SQL_ODBC_CLOSE_FUNC_LEN);
  str->q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
  str->q_append(tnm->ptr(), tnm->length());

  pthread_mutex_lock(&conn->mta_conn_mutex);
  SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
  conn->need_mon = &spider->need_mons[link_idx];
  conn->mta_conn_mutex_lock_already = TRUE;
  conn->mta_conn_mutex_unlock_later = TRUE;
  spider_conn_set_timeout_from_share(conn, link_idx,
    spider->wide_handler->trx->thd,
    share);
  if (spider_db_before_query(conn, (int *) conn->need_mon))
  {
    stored_error = spider_db_errorno(conn);
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    DBUG_RETURN(stored_error);
  }
  ret = SQLAllocHandle(SQL_HANDLE_STMT, db_conn->hdbc, &hstm);
  if (ret != SQL_SUCCESS)
  {
    stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, db_conn->hdbc,
      conn, stored_error_msg);
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    DBUG_RETURN(stored_error);
  }
  ret = SQLExecDirect(hstm, (SQLCHAR *) str->ptr(),
    (SQLINTEGER) str->length());
  if (ret != SQL_SUCCESS && ret != SQL_NO_DATA)
  {
    stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT, hstm,
      conn, stored_error_msg);
    SQLFreeHandle(SQL_HANDLE_STMT, hstm);
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    DBUG_RETURN(stored_error);
  }
  if (ret == SQL_NO_DATA)
  {
    SQLFreeHandle(SQL_HANDLE_STMT, hstm);
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    share->stat.auto_increment_value = 1;
    DBUG_PRINT("info",("spider auto_increment_value:%llu",
      share->stat.auto_increment_value));
    DBUG_RETURN(0);
  }
  SQLINTEGER auto_increment_value;
  SQLLEN auto_increment_value_sz;
  SQLBindCol(hstm, 1, SQL_C_SLONG, &auto_increment_value, 0,
    &auto_increment_value_sz);
  ret = SQLFetch(hstm);
  if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
  {
    stored_error = 0;
    share->stat.auto_increment_value = auto_increment_value + 1;
    DBUG_PRINT("info",("spider auto_increment_value:%llu",
      share->stat.auto_increment_value));
  }
  if (ret == SQL_ERROR || ret == SQL_SUCCESS_WITH_INFO)
  {
    stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT, hstm,
      conn, stored_error_msg);
  }
  SQLFreeStmt(hstm, SQL_CLOSE);
  SQLFreeHandle(SQL_HANDLE_STMT, hstm);
  conn->mta_conn_mutex_lock_already = FALSE;
  conn->mta_conn_mutex_unlock_later = FALSE;
  SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  if ((ulonglong) auto_increment_value >
    share->lgtm_tblhnd_share->auto_increment_value)
  {
    share->lgtm_tblhnd_share->auto_increment_value = auto_increment_value;
    DBUG_PRINT("info",("spider auto_increment_value=%llu",
      share->lgtm_tblhnd_share->auto_increment_value));
  }
  DBUG_RETURN(stored_error);
}

int spider_odbc_handler::get_statistics(
  int link_idx
) {
  SQLRETURN ret;
  SQLHSTMT hstm;
  spider_string *tnm =
    &odbc_share->table_names_str[spider->conn_link_idx[link_idx]];
  SPIDER_CONN *conn = spider->conns[link_idx];
  spider_db_odbc *db_conn = (spider_db_odbc *) conn->db_conn;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_odbc_handler::get_statistics");
  DBUG_PRINT("info",("spider hdbc:%p", db_conn->hdbc));
  stored_error = 0;
  pthread_mutex_lock(&conn->mta_conn_mutex);
  SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
  conn->need_mon = &spider->need_mons[link_idx];
  conn->mta_conn_mutex_lock_already = TRUE;
  conn->mta_conn_mutex_unlock_later = TRUE;
  spider_conn_set_timeout_from_share(conn, link_idx,
    spider->wide_handler->trx->thd,
    share);
  if (spider_db_before_query(conn, (int *) conn->need_mon))
  {
    stored_error = spider_db_errorno(conn);
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    DBUG_RETURN(stored_error);
  }

  ret = SQLAllocHandle(SQL_HANDLE_STMT, db_conn->hdbc, &hstm);
  if (ret != SQL_SUCCESS)
  {
    stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_DBC, db_conn->hdbc,
      conn, stored_error_msg);
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    DBUG_RETURN(stored_error);
  }
  ret = SQLStatistics(hstm, NULL, 0, NULL, 0,
    (SQLCHAR *) tnm->ptr(), (SQLSMALLINT) tnm->length(),
    SQL_INDEX_ALL, SQL_QUICK);
  if (ret != SQL_SUCCESS)
  {
    stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT, hstm,
      conn, stored_error_msg);
    SQLFreeHandle(SQL_HANDLE_STMT, hstm);
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    DBUG_RETURN(stored_error);
  }
  SQLCHAR table_cat[SPIDER_IDENT_SPACE];
  SQLCHAR table_schem[SPIDER_IDENT_SPACE];
  SQLCHAR table_name[SPIDER_IDENT_SPACE];
  SQLCHAR index_qualifier[SPIDER_IDENT_SPACE];
  SQLCHAR index_name[SPIDER_IDENT_SPACE];
  SQLCHAR column_name[SPIDER_IDENT_SPACE];
  SQLCHAR filter_condition[SPIDER_IDENT_SPACE];

  SQLCHAR asc_or_desc[2];

  SQLSMALLINT non_unique;
  SQLSMALLINT type;
  SQLSMALLINT ordinal_position;

  SQLINTEGER cardinality;
  SQLINTEGER pages;

  SQLLEN table_cat_sz;
  SQLLEN table_schem_sz;
  SQLLEN table_name_sz;
  SQLLEN non_unique_sz;
  SQLLEN index_qualifier_sz;
  SQLLEN index_name_sz;
  SQLLEN type_sz;
  SQLLEN ordinal_position_sz;
  SQLLEN column_name_sz;
  SQLLEN asc_or_desc_sz;
  SQLLEN cardinality_sz;
  SQLLEN pages_sz;
  SQLLEN filter_condition_sz;

  SQLBindCol(hstm, 1, SQL_C_CHAR, table_cat, SPIDER_IDENT_SPACE,
    &table_cat_sz);
  SQLBindCol(hstm, 2, SQL_C_CHAR, table_schem, SPIDER_IDENT_SPACE,
    &table_schem_sz);
  SQLBindCol(hstm, 3, SQL_C_CHAR, table_name, SPIDER_IDENT_SPACE,
    &table_name_sz);
  SQLBindCol(hstm, 4, SQL_C_SSHORT, &non_unique, 0, &non_unique_sz);
  SQLBindCol(hstm, 5, SQL_C_CHAR, index_qualifier, SPIDER_IDENT_SPACE,
    &index_qualifier_sz);
  SQLBindCol(hstm, 6, SQL_C_CHAR, index_name, SPIDER_IDENT_SPACE,
    &index_name_sz);
  SQLBindCol(hstm, 7, SQL_C_SSHORT, &type, 0, &type_sz);
  SQLBindCol(hstm, 8, SQL_C_SSHORT, &ordinal_position, 0,
    &ordinal_position_sz);
  SQLBindCol(hstm, 9, SQL_C_CHAR, column_name, SPIDER_IDENT_SPACE,
    &column_name_sz);
  SQLBindCol(hstm, 10, SQL_C_CHAR, asc_or_desc, SPIDER_IDENT_SPACE,
    &asc_or_desc_sz);
  SQLBindCol(hstm, 11, SQL_C_SLONG, &cardinality, 0, &cardinality_sz);
  SQLBindCol(hstm, 12, SQL_C_SLONG, &pages, 0, &pages_sz);
  SQLBindCol(hstm, 13, SQL_C_CHAR, filter_condition, SPIDER_IDENT_SPACE,
    &filter_condition_sz);
  ret = SQLFetch(hstm);
  Field *field;
  memset((uchar *) share->cardinality_upd, 0,
    sizeof(uchar) * share->bitmap_size);
  while (ret == SQL_SUCCESS)
  {
    if (type == SQL_TABLE_STAT)
    {
      if (cardinality_sz > 0)
      {
        share->stat.records = cardinality;
      } else if (!share->stat.records)
      {
        share->stat.records = 2;
      }
      share->stat.mean_rec_length = 65535;
      share->stat.data_file_length = 65535;
      share->stat.max_data_file_length = 65535;
      share->stat.index_file_length = 65535;
      share->stat.create_time = (time_t) 0;
      share->stat.update_time = (time_t) 0;
      share->stat.check_time = (time_t) 0;
      share->stat.auto_increment_value = 1;
    } else {
      if (cardinality_sz > 0 &&
        (field = find_field_in_table_sef(field->table,
          (const char *) column_name)))
      {
        if ((share->cardinality[field->field_index] =
          (longlong) cardinality) <= 0)
        {
          share->cardinality[field->field_index] = 1;
        }
        spider_set_bit(share->cardinality_upd, field->field_index);
        DBUG_PRINT("info", ("spider col_name=%s", column_name));
        DBUG_PRINT("info", ("spider cardinality=%lld",
          share->cardinality[field->field_index]));
      } else {
        DBUG_PRINT("info", ("spider skip col_name=%s", column_name));
      }
    }
    ret = SQLFetch(hstm);
  }
  if (ret == SQL_ERROR || ret == SQL_SUCCESS_WITH_INFO)
  {
    stored_error = spider_db_odbc_get_error(ret, SQL_HANDLE_STMT, hstm,
      conn, stored_error_msg);
  }
  SQLFreeStmt(hstm, SQL_CLOSE);
  SQLFreeHandle(SQL_HANDLE_STMT, hstm);
  conn->mta_conn_mutex_lock_already = FALSE;
  conn->mta_conn_mutex_unlock_later = FALSE;
  SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  if (1 > share->lgtm_tblhnd_share->auto_increment_value)
  {
    share->lgtm_tblhnd_share->auto_increment_value = 1;
    DBUG_PRINT("info",("spider auto_increment_value=%llu",
      share->lgtm_tblhnd_share->auto_increment_value));
  }
  DBUG_RETURN(stored_error);
}

int spider_odbc_handler::sts_mode_exchange(
  int sts_mode
) {
  DBUG_ENTER("spider_odbc_handler::sts_mode_exchange");
  DBUG_PRINT("info",("spider sts_mode=%d", sts_mode));
  DBUG_RETURN(1);
}

int spider_odbc_handler::show_table_status(
  int link_idx,
  int sts_mode,
  uint flag
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::show_table_status");
  if (flag & ~(HA_STATUS_AUTO | HA_STATUS_NO_LOCK))
  {
    if ((err = get_statistics(link_idx)))
    {
      DBUG_RETURN(err);
    }
  }
/*
  if (flag & HA_STATUS_AUTO)
  {
    TABLE *table = spider->get_table();
    if (table->next_number_field &&
      (err = get_last_auto_increment(link_idx)))
    {
      DBUG_RETURN(err);
    }
  }
*/
  DBUG_RETURN(0);
}

int spider_odbc_handler::crd_mode_exchange(
  int crd_mode
) {
  DBUG_ENTER("spider_odbc_handler::crd_mode_exchange");
  DBUG_PRINT("info",("spider crd_mode=%d", crd_mode));
  DBUG_RETURN(1);
}

int spider_odbc_handler::show_index(
  int link_idx,
  int crd_mode
) {
  DBUG_ENTER("spider_odbc_handler::show_index");
  DBUG_PRINT("info",("spider crd_mode=%d", crd_mode));
  DBUG_RETURN(get_statistics(link_idx));
}

int spider_odbc_handler::show_records(
  int link_idx
) {
  int err;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_odbc_handler::show_records");
  err = get_statistics(link_idx);
  if (err)
  {
    DBUG_PRINT("info", ("spider err=%d", err));
    DBUG_RETURN(err);
  }
  spider->table_rows = share->stat.records;
  DBUG_RETURN(0);
}

int spider_odbc_handler::show_last_insert_id(
  int link_idx,
  ulonglong &last_insert_id
) {
  DBUG_ENTER("spider_odbc_handler::show_last_insert_id");
  /* not supported */
  last_insert_id = 0;
  DBUG_RETURN(0);
}

ha_rows spider_odbc_handler::explain_select(
  key_range *start_key,
  key_range *end_key,
  int link_idx
) {
  DBUG_ENTER("spider_odbc_handler::explain_select");
  DBUG_ASSERT(0);
  DBUG_RETURN(HA_POS_ERROR);
}

int spider_odbc_handler::lock_tables(
  int link_idx
) {
  int err;
  bool first = TRUE;
  SPIDER_CONN *conn = spider->conns[link_idx];
  spider_string *str = &sql;
  DBUG_ENTER("spider_odbc_handler::lock_tables");
  do
  {
    str->length(0);
    if ((err = conn->db_conn->append_lock_tables(str)))
    {
      DBUG_RETURN(err);
    }
    if (str->length())
    {
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      conn->need_mon = &spider->need_mons[link_idx];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if (first)
      {
        first = FALSE;
        if ((err = spider_db_set_names(spider, conn, link_idx)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(err);
        }
        spider_conn_set_timeout_from_share(conn, link_idx,
          spider->wide_handler->trx->thd,
          spider->share);
        if (spider_db_query(
          conn,
          SPIDER_SQL_LOCK_TABLE_START_STR,
          SPIDER_SQL_LOCK_TABLE_START_LEN,
          -1,
          &spider->need_mons[link_idx])
        ) {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          DBUG_RETURN(spider_db_errorno(conn));
        }
      }
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
  } while (str->length());
  if (!conn->table_locked)
  {
    conn->table_locked = TRUE;
    spider->wide_handler->trx->locked_connections++;
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::unlock_tables(
  int link_idx
) {
  int err;
  SPIDER_CONN *conn = spider->conns[link_idx];
  DBUG_ENTER("spider_odbc_handler::unlock_tables");
  if (conn->table_locked)
  {
    spider_string *str = &sql;
    conn->table_locked = FALSE;
    spider->wide_handler->trx->locked_connections--;

    str->length(0);
    if ((err = conn->db_conn->append_unlock_tables(str)))
    {
      DBUG_RETURN(err);
    }
    if (str->length())
    {
      spider_conn_set_timeout_from_share(conn, link_idx,
        spider->wide_handler->trx->thd,
        spider->share);
      if (spider_db_query(
        conn,
        str->ptr(),
        str->length(),
        -1,
        &spider->need_mons[link_idx])
      )
        DBUG_RETURN(spider_db_errorno(conn));
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::disable_keys(
  SPIDER_CONN *conn,
  int link_idx
) {
  DBUG_ENTER("spider_odbc_handler::disable_keys");
  DBUG_PRINT("info",("spider this=%p", this));
  /* not supported */
  DBUG_RETURN(0);
}

int spider_odbc_handler::enable_keys(
  SPIDER_CONN *conn,
  int link_idx
) {
  DBUG_ENTER("spider_odbc_handler::enable_keys");
  DBUG_PRINT("info",("spider this=%p", this));
  /* not supported */
  DBUG_RETURN(0);
}

int spider_odbc_handler::check_table(
  SPIDER_CONN *conn,
  int link_idx,
  HA_CHECK_OPT* check_opt
) {
  DBUG_ENTER("spider_odbc_handler::check_table");
  DBUG_PRINT("info",("spider this=%p", this));
  /* not supported */
  DBUG_RETURN(0);
}

int spider_odbc_handler::repair_table(
  SPIDER_CONN *conn,
  int link_idx,
  HA_CHECK_OPT* check_opt
) {
  DBUG_ENTER("spider_odbc_handler::repair_table");
  DBUG_PRINT("info",("spider this=%p", this));
  /* not supported */
  DBUG_RETURN(0);
}

int spider_odbc_handler::analyze_table(
  SPIDER_CONN *conn,
  int link_idx
) {
  DBUG_ENTER("spider_odbc_handler::analyze_table");
  DBUG_PRINT("info",("spider this=%p", this));
  /* not supported */
  DBUG_RETURN(0);
}

int spider_odbc_handler::optimize_table(
  SPIDER_CONN *conn,
  int link_idx
) {
  DBUG_ENTER("spider_odbc_handler::optimize_table");
  DBUG_PRINT("info",("spider this=%p", this));
  /* not supported */
  DBUG_RETURN(0);
}

int spider_odbc_handler::flush_tables(
  SPIDER_CONN *conn,
  int link_idx,
  bool lock
) {
  DBUG_ENTER("spider_odbc_handler::flush_tables");
  DBUG_PRINT("info",("spider this=%p", this));
  /* not supported */
  DBUG_RETURN(0);
}

int spider_odbc_handler::flush_logs(
  SPIDER_CONN *conn,
  int link_idx
) {
  DBUG_ENTER("spider_odbc_handler::flush_logs");
  DBUG_PRINT("info",("spider this=%p", this));
  /* not supported */
  DBUG_RETURN(0);
}

int spider_odbc_handler::insert_opened_handler(
  SPIDER_CONN *conn,
  int link_idx
) {
  spider_db_odbc *db_conn = (spider_db_odbc *) conn->db_conn;
  SPIDER_LINK_FOR_HASH *tmp_link_for_hash = &link_for_hash[link_idx];
  DBUG_ASSERT(tmp_link_for_hash->spider == spider);
  DBUG_ASSERT(tmp_link_for_hash->link_idx == link_idx);
  uint old_elements = db_conn->handler_open_array.max_element;
  DBUG_ENTER("spider_odbc_handler::insert_opened_handler");
  DBUG_PRINT("info",("spider this=%p", this));
  if (insert_dynamic(&db_conn->handler_open_array,
    (uchar*) &tmp_link_for_hash))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  if (db_conn->handler_open_array.max_element > old_elements)
  {
    spider_alloc_calc_mem(spider_current_trx,
      db_conn->handler_open_array,
      (db_conn->handler_open_array.max_element - old_elements) *
      db_conn->handler_open_array.size_of_element);
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::delete_opened_handler(
  SPIDER_CONN *conn,
  int link_idx
) {
  spider_db_odbc *db_conn = (spider_db_odbc *) conn->db_conn;
  uint roop_count, elements = db_conn->handler_open_array.elements;
  SPIDER_LINK_FOR_HASH *tmp_link_for_hash;
  DBUG_ENTER("spider_odbc_handler::delete_opened_handler");
  DBUG_PRINT("info",("spider this=%p", this));
  for (roop_count = 0; roop_count < elements; roop_count++)
  {
    get_dynamic(&db_conn->handler_open_array, (uchar *) &tmp_link_for_hash,
      roop_count);
    if (tmp_link_for_hash == &link_for_hash[link_idx])
    {
      delete_dynamic_element(&db_conn->handler_open_array, roop_count);
      break;
    }
  }
  DBUG_ASSERT(roop_count < elements);
  DBUG_RETURN(0);
}

int spider_odbc_handler::sync_from_clone_source(
  spider_db_handler *dbton_hdl
) {
  DBUG_ENTER("spider_odbc_handler::sync_from_clone_source");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(0);
}

bool spider_odbc_handler::support_use_handler(
  int use_handler
) {
  DBUG_ENTER("spider_odbc_handler::support_use_handler");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_RETURN(FALSE);
}

void spider_odbc_handler::minimum_select_bitmap_create()
{
  TABLE *table = spider->get_table();
  Field **field_p;
  DBUG_ENTER("spider_odbc_handler::minimum_select_bitmap_create");
  DBUG_PRINT("info",("spider this=%p", this));
  memset(minimum_select_bitmap, 0, no_bytes_in_map(table->read_set));
  if (
    spider->use_index_merge ||
#ifdef HA_CAN_BULK_ACCESS
    (spider->is_clone && !spider->is_bulk_access_clone)
#else
    spider->is_clone
#endif
  ) {
    /* need preparing for cmp_ref */
    TABLE_SHARE *table_share = table->s;
    if (
      table_share->primary_key == MAX_KEY
    ) {
      /* need all columns */
      memset(minimum_select_bitmap, 0xFF, no_bytes_in_map(table->read_set));
      DBUG_VOID_RETURN;
    } else {
      /* need primary key columns */
      uint roop_count;
      KEY *key_info;
      KEY_PART_INFO *key_part;
      Field *field;
      key_info = &table_share->key_info[table_share->primary_key];
      key_part = key_info->key_part;
      for (roop_count = 0;
        roop_count < spider_user_defined_key_parts(key_info);
        roop_count++)
      {
        field = key_part[roop_count].field;
        spider_set_bit(minimum_select_bitmap, field->field_index);
      }
    }
  }
  DBUG_PRINT("info",("spider searched_bitmap=%p",
    spider->wide_handler->searched_bitmap));
  for (field_p = table->field; *field_p; field_p++)
  {
    uint field_index = (*field_p)->field_index;
    DBUG_PRINT("info",("spider field_index=%u", field_index));
    DBUG_PRINT("info",("spider ft_discard_bitmap=%s",
      spider_bit_is_set(spider->wide_handler->ft_discard_bitmap, field_index) ?
        "TRUE" : "FALSE"));
    DBUG_PRINT("info",("spider searched_bitmap=%s",
      spider_bit_is_set(spider->wide_handler->searched_bitmap, field_index) ?
        "TRUE" : "FALSE"));
    DBUG_PRINT("info",("spider read_set=%s",
      bitmap_is_set(table->read_set, field_index) ?
        "TRUE" : "FALSE"));
    DBUG_PRINT("info",("spider write_set=%s",
      bitmap_is_set(table->write_set, field_index) ?
        "TRUE" : "FALSE"));
    if (
      spider_bit_is_set(spider->wide_handler->ft_discard_bitmap, field_index) &
      (
        spider_bit_is_set(spider->wide_handler->searched_bitmap, field_index) |
        bitmap_is_set(table->read_set, field_index) |
        bitmap_is_set(table->write_set, field_index)
      )
    ) {
      spider_set_bit(minimum_select_bitmap, field_index);
    }
  }
  DBUG_VOID_RETURN;
}

bool spider_odbc_handler::minimum_select_bit_is_set(
  uint field_index
) {
  DBUG_ENTER("spider_odbc_handler::minimum_select_bit_is_set");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_PRINT("info",("spider field_index=%u", field_index));
  DBUG_PRINT("info",("spider minimum_select_bitmap=%s",
    spider_bit_is_set(minimum_select_bitmap, field_index) ?
      "TRUE" : "FALSE"));
  DBUG_RETURN(spider_bit_is_set(minimum_select_bitmap, field_index));
}

void spider_odbc_handler::copy_minimum_select_bitmap(
  uchar *bitmap
) {
  int roop_count;
  TABLE *table = spider->get_table();
  DBUG_ENTER("spider_odbc_handler::copy_minimum_select_bitmap");
  for (roop_count = 0;
    roop_count < (int) ((table->s->fields + 7) / 8);
    roop_count++)
  {
    bitmap[roop_count] =
      minimum_select_bitmap[roop_count];
    DBUG_PRINT("info",("spider roop_count=%d", roop_count));
    DBUG_PRINT("info",("spider bitmap=%d",
      bitmap[roop_count]));
  }
  DBUG_VOID_RETURN;
}

int spider_odbc_handler::init_union_table_name_pos()
{
  DBUG_ENTER("spider_odbc_handler::init_union_table_name_pos");
  DBUG_PRINT("info",("spider this=%p", this));
  if (!union_table_name_pos_first)
  {
    if (!spider_bulk_malloc(spider_current_trx, 236, MYF(MY_WME),
      &union_table_name_pos_first, (uint) (sizeof(SPIDER_INT_HLD)),
      NullS)
    ) {
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    union_table_name_pos_first->next = NULL;
  }
  union_table_name_pos_current = union_table_name_pos_first;
  union_table_name_pos_current->tgt_num = 0;
  DBUG_RETURN(0);
}

int spider_odbc_handler::set_union_table_name_pos()
{
  DBUG_ENTER("spider_odbc_handler::set_union_table_name_pos");
  DBUG_PRINT("info",("spider this=%p", this));
  if (union_table_name_pos_current->tgt_num >= SPIDER_INT_HLD_TGT_SIZE)
  {
    if (!union_table_name_pos_current->next)
    {
      if (!spider_bulk_malloc(spider_current_trx, 237, MYF(MY_WME),
        &union_table_name_pos_current->next, (uint) (sizeof(SPIDER_INT_HLD)),
        NullS)
      ) {
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
      union_table_name_pos_current->next->next = NULL;
    }
    union_table_name_pos_current = union_table_name_pos_current->next;
    union_table_name_pos_current->tgt_num = 0;
  }
  union_table_name_pos_current->tgt[union_table_name_pos_current->tgt_num] =
    table_name_pos;
  ++union_table_name_pos_current->tgt_num;
  DBUG_RETURN(0);
}

int spider_odbc_handler::reset_union_table_name(
  spider_string *str,
  int link_idx,
  ulong sql_type
) {
  DBUG_ENTER("spider_odbc_handler::reset_union_table_name");
  DBUG_PRINT("info",("spider this=%p", this));
  if (!union_table_name_pos_current)
    DBUG_RETURN(0);

  SPIDER_INT_HLD *tmp_pos = union_table_name_pos_first;
  uint cur_num, pos_backup = str->length();
  while(TRUE)
  {
    for (cur_num = 0; cur_num < tmp_pos->tgt_num; ++cur_num)
    {
      str->length(tmp_pos->tgt[cur_num]);
      append_table_name_with_adjusting(str, link_idx, sql_type);
    }
    if (tmp_pos == union_table_name_pos_current)
      break;
    tmp_pos = tmp_pos->next;
  }
  str->length(pos_backup);
  DBUG_RETURN(0);
}

#ifdef SPIDER_HAS_GROUP_BY_HANDLER
int spider_odbc_handler::append_from_and_tables_part(
  spider_fields *fields,
  ulong sql_type
) {
  int err;
  spider_string *str;
  SPIDER_TABLE_HOLDER *table_holder;
  TABLE_LIST *table_list;
  DBUG_ENTER("spider_odbc_handler::append_from_and_tables_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  fields->set_pos_to_first_table_holder();
  table_holder = fields->get_next_table_holder();
  table_list = table_holder->table->pos_in_table_list;
  err = utility->append_from_and_tables(
    table_holder->spider, fields, str,
    table_list, fields->get_table_count());
  DBUG_RETURN(err);
}

int spider_odbc_handler::reappend_tables_part(
  spider_fields *fields,
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::reappend_tables_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = utility->reappend_tables(fields,
    link_idx_chain, str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_where_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_where_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = utility->append_where(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_having_part(
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_having_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = utility->append_having(str);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_item_type_part(
  Item *item,
  const char *alias,
  uint alias_length,
  bool use_fields,
  spider_fields *fields,
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_item_type_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = spider_db_print_item_type(item, NULL, spider, str,
    alias, alias_length, dbton_id, use_fields, fields);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_list_item_select_part(
  List<Item> *select,
  const char *alias,
  uint alias_length,
  bool use_fields,
  spider_fields *fields,
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_list_item_select_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_list_item_select(select, str, alias, alias_length,
    use_fields, fields);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_list_item_select(
  List<Item> *select,
  spider_string *str,
  const char *alias,
  uint alias_length,
  bool use_fields,
  spider_fields *fields
) {
  int err;
  uint32 length;
  List_iterator_fast<Item> it(*select);
  Item *item;
  Field *field;
  const char *item_name;
  DBUG_ENTER("spider_odbc_handler::append_list_item_select");
  DBUG_PRINT("info",("spider this=%p", this));
  while ((item = it++))
  {
    if ((err = spider_db_print_item_type(item, NULL, spider, str,
      alias, alias_length, dbton_id, use_fields, fields)))
    {
      DBUG_RETURN(err);
    }
    field = *(fields->get_next_field_ptr());
    if (field)
    {
      item_name = SPIDER_field_name_str(field);
      length = SPIDER_field_name_length(field);
    } else {
      item_name = SPIDER_item_name_str(item);
      length = SPIDER_item_name_length(item);
    }
    if (str->reserve(
      SPIDER_SQL_COMMA_LEN + utility->name_quote_length * 2 +
      SPIDER_SQL_SPACE_LEN + length
    ))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
    if ((err = utility->append_escaped_name(str,
      item_name, length)))
    {
      DBUG_RETURN(err);
    }
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_group_by_part(
  ORDER *order,
  const char *alias,
  uint alias_length,
  bool use_fields,
  spider_fields *fields,
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_group_by_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_group_by(order, str, alias, alias_length,
    use_fields, fields);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_group_by(
  ORDER *order,
  spider_string *str,
  const char *alias,
  uint alias_length,
  bool use_fields,
  spider_fields *fields
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::append_group_by");
  DBUG_PRINT("info",("spider this=%p", this));
  if (order)
  {
    if (str->reserve(SPIDER_SQL_GROUP_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_GROUP_STR, SPIDER_SQL_GROUP_LEN);
    for (; order; order = order->next)
    {
      if ((err = spider_db_print_item_type((*order->item), NULL, spider,
        str, alias, alias_length, dbton_id, use_fields, fields)))
      {
        DBUG_RETURN(err);
      }
      if (str->reserve(SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  }
  DBUG_RETURN(0);
}

int spider_odbc_handler::append_order_by_part(
  ORDER *order,
  const char *alias,
  uint alias_length,
  bool use_fields,
  spider_fields *fields,
  ulong sql_type
) {
  int err;
  spider_string *str;
  DBUG_ENTER("spider_odbc_handler::append_order_by_part");
  DBUG_PRINT("info",("spider this=%p", this));
  switch (sql_type)
  {
    case SPIDER_SQL_TYPE_SELECT_SQL:
      str = &sql;
      break;
    default:
      DBUG_RETURN(0);
  }
  err = append_order_by(order, str, alias, alias_length,
    use_fields, fields);
  DBUG_RETURN(err);
}

int spider_odbc_handler::append_order_by(
  ORDER *order,
  spider_string *str,
  const char *alias,
  uint alias_length,
  bool use_fields,
  spider_fields *fields
) {
  int err;
  DBUG_ENTER("spider_odbc_handler::append_order_by");
  DBUG_PRINT("info",("spider this=%p", this));
  if (order)
  {
    if (str->reserve(SPIDER_SQL_ORDER_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
    for (; order; order = order->next)
    {
      if ((err = spider_db_print_item_type((*order->item), NULL, spider,
        str, alias, alias_length, dbton_id, use_fields, fields)))
      {
        DBUG_RETURN(err);
      }
      if (SPIDER_order_direction_is_asc(order))
      {
        if (str->reserve(SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      } else {
        if (str->reserve(SPIDER_SQL_COMMA_LEN + SPIDER_SQL_DESC_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
        str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      }
    }
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  }
  DBUG_RETURN(0);
}
#endif

spider_odbc_copy_table::spider_odbc_copy_table(
  spider_odbc_share *db_share
) : spider_db_copy_table(
  db_share
),
  utility(&spider_db_odbc_utility),
  odbc_share(db_share)
{
  DBUG_ENTER("spider_odbc_copy_table::spider_odbc_copy_table");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

spider_odbc_copy_table::~spider_odbc_copy_table()
{
  DBUG_ENTER("spider_odbc_copy_table::~spider_odbc_copy_table");
  DBUG_PRINT("info",("spider this=%p", this));
  DBUG_VOID_RETURN;
}

int spider_odbc_copy_table::init()
{
  DBUG_ENTER("spider_odbc_copy_table::init");
  DBUG_PRINT("info",("spider this=%p", this));
  sql.init_calc_mem(313);
  DBUG_RETURN(0);
}

void spider_odbc_copy_table::set_sql_charset(
  CHARSET_INFO *cs
) {
  DBUG_ENTER("spider_odbc_copy_table::set_sql_charset");
  DBUG_PRINT("info",("spider this=%p", this));
  sql.set_charset(cs);
  DBUG_VOID_RETURN;
}

int spider_odbc_copy_table::append_select_str()
{
  DBUG_ENTER("spider_odbc_copy_table::append_select_str");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql.reserve(SPIDER_SQL_SELECT_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  sql.q_append(SPIDER_SQL_SELECT_STR, SPIDER_SQL_SELECT_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::append_insert_str(
  int insert_flg
) {
  DBUG_ENTER("spider_odbc_copy_table::append_insert_str");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql.reserve(SPIDER_SQL_INSERT_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  sql.q_append(SPIDER_SQL_INSERT_STR, SPIDER_SQL_INSERT_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::append_table_columns(
  TABLE_SHARE *table_share
) {
  int err;
  Field **field;
  DBUG_ENTER("spider_odbc_copy_table::append_table_columns");
  DBUG_PRINT("info",("spider this=%p", this));
  for (field = table_share->field; *field; field++)
  {
    if (sql.reserve(utility->name_quote_length))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    sql.q_append(utility->name_quote,
      utility->name_quote_length);
    if ((err = spider_db_append_name_with_quote_str(&sql,
      (*field)->field_name, dbton_id)))
      DBUG_RETURN(err);
    if (sql.reserve(utility->name_quote_length +
      SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    sql.q_append(utility->name_quote,
      utility->name_quote_length);
    sql.q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  sql.length(sql.length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::append_from_str()
{
  DBUG_ENTER("spider_odbc_copy_table::append_from_str");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql.reserve(SPIDER_SQL_FROM_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  sql.q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::append_table_name(
  int link_idx
) {
  int err;
  DBUG_ENTER("spider_odbc_copy_table::append_table_name");
  DBUG_PRINT("info",("spider this=%p", this));
  err = odbc_share->append_table_name(&sql, link_idx);
  DBUG_RETURN(err);
}

void spider_odbc_copy_table::set_sql_pos()
{
  DBUG_ENTER("spider_odbc_copy_table::set_sql_pos");
  DBUG_PRINT("info",("spider this=%p", this));
  pos = sql.length();
  DBUG_VOID_RETURN;
}

void spider_odbc_copy_table::set_sql_to_pos()
{
  DBUG_ENTER("spider_odbc_copy_table::set_sql_to_pos");
  DBUG_PRINT("info",("spider this=%p", this));
  sql.length(pos);
  DBUG_VOID_RETURN;
}

int spider_odbc_copy_table::append_copy_where(
  spider_db_copy_table *source_ct,
  KEY *key_info,
  ulong *last_row_pos,
  ulong *last_lengths
) {
  int err, roop_count, roop_count2;
  DBUG_ENTER("spider_odbc_copy_table::append_copy_where");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql.reserve(SPIDER_SQL_WHERE_LEN + SPIDER_SQL_OPEN_PAREN_LEN))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  sql.q_append(SPIDER_SQL_WHERE_STR, SPIDER_SQL_WHERE_LEN);
  sql.q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  Field *field;
  KEY_PART_INFO *key_part = key_info->key_part;
  for (roop_count = spider_user_defined_key_parts(key_info) - 1;
    roop_count >= 0; roop_count--)
  {
    for (roop_count2 = 0; roop_count2 < roop_count; roop_count2++)
    {
      field = key_part[roop_count2].field;
      if ((err = copy_key_row(source_ct,
        field, &last_row_pos[field->field_index],
        &last_lengths[field->field_index],
        SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN)))
      {
        DBUG_RETURN(err);
      }
    }
    field = key_part[roop_count2].field;
    if ((err = copy_key_row(source_ct,
      field, &last_row_pos[field->field_index],
      &last_lengths[field->field_index],
      SPIDER_SQL_GT_STR, SPIDER_SQL_GT_LEN)))
    {
      DBUG_RETURN(err);
    }
    sql.length(sql.length() - SPIDER_SQL_AND_LEN);
    if (sql.reserve(SPIDER_SQL_CLOSE_PAREN_LEN +
      SPIDER_SQL_OR_LEN + SPIDER_SQL_OPEN_PAREN_LEN))
    {
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    sql.q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
    sql.q_append(SPIDER_SQL_OR_STR, SPIDER_SQL_OR_LEN);
    sql.q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  }
  sql.length(sql.length() - SPIDER_SQL_OR_LEN - SPIDER_SQL_OPEN_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::append_key_order_str(
  KEY *key_info,
  int start_pos,
  bool desc_flg
) {
  int length, err;
  KEY_PART_INFO *key_part;
  Field *field;
  DBUG_ENTER("spider_odbc_copy_table::append_key_order_str");
  DBUG_PRINT("info",("spider this=%p", this));
  if ((int) spider_user_defined_key_parts(key_info) > start_pos)
  {
    if (sql.reserve(SPIDER_SQL_ORDER_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    sql.q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
    if (desc_flg == TRUE)
    {
      for (
        key_part = key_info->key_part + start_pos,
        length = 0;
        length + start_pos < (int) spider_user_defined_key_parts(key_info);
        key_part++,
        length++
      ) {
        field = key_part->field;
        if (sql.reserve(utility->name_quote_length))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        sql.q_append(utility->name_quote,
          utility->name_quote_length);
        if ((err = spider_db_append_name_with_quote_str(&sql,
          field->field_name, dbton_id)))
          DBUG_RETURN(err);
        if (key_part->key_part_flag & HA_REVERSE_SORT)
        {
          if (sql.reserve(utility->name_quote_length +
            SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          sql.q_append(utility->name_quote,
            utility->name_quote_length);
          sql.q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        } else {
          if (sql.reserve(utility->name_quote_length +
            SPIDER_SQL_DESC_LEN + SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          sql.q_append(utility->name_quote,
            utility->name_quote_length);
          sql.q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
          sql.q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        }
      }
    } else {
      for (
        key_part = key_info->key_part + start_pos,
        length = 0;
        length + start_pos < (int) spider_user_defined_key_parts(key_info);
        key_part++,
        length++
      ) {
        field = key_part->field;
        if (sql.reserve(utility->name_quote_length))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        sql.q_append(utility->name_quote,
          utility->name_quote_length);
        if ((err = spider_db_append_name_with_quote_str(&sql,
          field->field_name, dbton_id)))
          DBUG_RETURN(err);
        if (key_part->key_part_flag & HA_REVERSE_SORT)
        {
          if (sql.reserve(utility->name_quote_length +
            SPIDER_SQL_DESC_LEN + SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          sql.q_append(utility->name_quote,
            utility->name_quote_length);
          sql.q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
          sql.q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        } else {
          if (sql.reserve(utility->name_quote_length +
            SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          sql.q_append(utility->name_quote,
            utility->name_quote_length);
          sql.q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        }
      }
    }
    sql.length(sql.length() - SPIDER_SQL_COMMA_LEN);
  }
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::append_limit(
  longlong offset,
  longlong limit
) {
  DBUG_ENTER("spider_odbc_copy_table::append_limit");
  DBUG_PRINT("info",("spider this=%p", this));
  this->offset = offset;
  this->limit = limit;
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::append_into_str()
{
  DBUG_ENTER("spider_odbc_copy_table::append_into_str");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql.reserve(SPIDER_SQL_INTO_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  sql.q_append(SPIDER_SQL_INTO_STR, SPIDER_SQL_INTO_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::append_open_paren_str()
{
  DBUG_ENTER("spider_odbc_copy_table::append_open_paren_str");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql.reserve(SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  sql.q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::append_values_str()
{
  DBUG_ENTER("spider_odbc_copy_table::append_values_str");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql.reserve(SPIDER_SQL_CLOSE_PAREN_LEN + SPIDER_SQL_VALUES_LEN +
    SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  sql.q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  sql.q_append(SPIDER_SQL_VALUES_STR, SPIDER_SQL_VALUES_LEN);
  sql.q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::append_select_lock_str(
  int lock_mode
) {
  DBUG_ENTER("spider_odbc_copy_table::append_select_lock_str");
  DBUG_PRINT("info",("spider this=%p", this));
  if (lock_mode == SPIDER_LOCK_MODE_EXCLUSIVE)
  {
    if (sql.reserve(SPIDER_SQL_FOR_UPDATE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    sql.q_append(SPIDER_SQL_FOR_UPDATE_STR, SPIDER_SQL_FOR_UPDATE_LEN);
  } else if (lock_mode == SPIDER_LOCK_MODE_SHARED)
  {
    if (sql.reserve(SPIDER_SQL_FOR_SHARE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    sql.q_append(SPIDER_SQL_FOR_SHARE_STR, SPIDER_SQL_FOR_SHARE_LEN);
  }
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::exec_query(
  SPIDER_CONN *conn,
  int quick_mode,
  int *need_mon
) {
  int err;
  DBUG_ENTER("spider_odbc_copy_table::exec_query");
  DBUG_PRINT("info",("spider this=%p", this));
  err = spider_db_query(conn, sql.ptr(), sql.length(), quick_mode,
    need_mon);
  DBUG_RETURN(err);
}

int spider_odbc_copy_table::copy_key_row(
  spider_db_copy_table *source_ct,
  Field *field,
  ulong *row_pos,
  ulong *length,
  const char *joint_str,
  const int joint_length
) {
  int err;
  spider_string *source_str = &((spider_odbc_copy_table *) source_ct)->sql;
  DBUG_ENTER("spider_odbc_copy_table::copy_key_row");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql.reserve(utility->name_quote_length))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  sql.q_append(utility->name_quote,
    utility->name_quote_length);
  if ((err = spider_db_append_name_with_quote_str(&sql,
    field->field_name, dbton_id)))
    DBUG_RETURN(err);
  if (sql.reserve(utility->name_quote_length +
    joint_length + *length + SPIDER_SQL_AND_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  sql.q_append(utility->name_quote,
    utility->name_quote_length);
  sql.q_append(joint_str, joint_length);
  sql.q_append(source_str->ptr() + *row_pos, *length);
  sql.q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::copy_row(
  Field *field,
  SPIDER_DB_ROW *row
) {
  int err;
  DBUG_ENTER("spider_odbc_copy_table::copy_row");
  DBUG_PRINT("info",("spider this=%p", this));
  if (row->is_null())
  {
    if (sql.reserve(SPIDER_SQL_NULL_LEN + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    sql.q_append(SPIDER_SQL_NULL_STR, SPIDER_SQL_NULL_LEN);
  } else if (field->str_needs_quotes())
  {
    if (sql.reserve(SPIDER_SQL_VALUE_QUOTE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    sql.q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    if ((err = row->append_escaped_to_str(&sql,
      dbton_id)))
      DBUG_RETURN(err);
    if (sql.reserve(SPIDER_SQL_VALUE_QUOTE_LEN + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    sql.q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  } else {
    if ((err = row->append_to_str(&sql)))
      DBUG_RETURN(err);
    if (sql.reserve(SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  sql.q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::copy_rows(
  TABLE *table,
  SPIDER_DB_ROW *row,
  ulong **last_row_pos,
  ulong **last_lengths
) {
  int err;
  Field **field;
  ulong *lengths2, *row_pos2;
  DBUG_ENTER("spider_odbc_copy_table::copy_rows");
  DBUG_PRINT("info",("spider this=%p", this));
  row_pos2 = *last_row_pos;
  lengths2 = *last_lengths;

  for (
    field = table->field;
    *field;
    field++,
    lengths2++
  ) {
    *row_pos2 = sql.length();
    if ((err =
      copy_row(*field, row)))
      DBUG_RETURN(err);
    *lengths2 = sql.length() - *row_pos2 - SPIDER_SQL_COMMA_LEN;
    row->next();
    row_pos2++;
  }
  sql.length(sql.length() - SPIDER_SQL_COMMA_LEN);
  if (sql.reserve(SPIDER_SQL_CLOSE_PAREN_LEN +
    SPIDER_SQL_COMMA_LEN + SPIDER_SQL_OPEN_PAREN_LEN))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  sql.q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  sql.q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  sql.q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::copy_rows(
  TABLE *table,
  SPIDER_DB_ROW *row
) {
  int err;
  Field **field;
  DBUG_ENTER("spider_odbc_copy_table::copy_rows");
  DBUG_PRINT("info",("spider this=%p", this));
  for (
    field = table->field;
    *field;
    field++
  ) {
    if ((err =
      copy_row(*field, row)))
      DBUG_RETURN(err);
    row->next();
  }
  sql.length(sql.length() - SPIDER_SQL_COMMA_LEN);
  if (sql.reserve(SPIDER_SQL_CLOSE_PAREN_LEN +
    SPIDER_SQL_COMMA_LEN + SPIDER_SQL_OPEN_PAREN_LEN))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  sql.q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  sql.q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  sql.q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::append_insert_terminator()
{
  DBUG_ENTER("spider_odbc_copy_table::append_insert_terminator");
  DBUG_PRINT("info",("spider this=%p", this));
  sql.length(sql.length() - SPIDER_SQL_COMMA_LEN - SPIDER_SQL_OPEN_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_odbc_copy_table::copy_insert_values(
  spider_db_copy_table *source_ct
) {
  spider_odbc_copy_table *tmp_ct = (spider_odbc_copy_table *) source_ct;
  spider_string *source_str = &tmp_ct->sql;
  int values_length = source_str->length() - tmp_ct->pos;
  const char *values_ptr = source_str->ptr() + tmp_ct->pos;
  DBUG_ENTER("spider_odbc_copy_table::copy_insert_values");
  DBUG_PRINT("info",("spider this=%p", this));
  if (sql.reserve(values_length))
  {
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  sql.q_append(values_ptr, values_length);
  DBUG_RETURN(0);
}
#endif
