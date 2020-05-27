let $MASTER_1_HS_COMMENT_TMP=
  COMMENT='';
let $MASTER_1_HS_COMMENT_2_1=
  COMMENT='dsn "$ODBC_DSN_HA1", wrapper "odbc", table "$S_2_1_HS_R", uhr "1", uhw "1", hrp "$CHILD2_1_HSRPORT", hwp "$CHILD2_1_HSWPORT"';
let $MASTER_1_HS_COMMENT_P_2_1=
  COMMENT='uhr "1", uhw "1"'
  PARTITION BY RANGE(a) (
    PARTITION pt1 VALUES LESS THAN (4)
    COMMENT='dsn "$ODBC_DSN_HA1", wrapper "odbc", table "$S_2_1_HS_R2", hrp "$CHILD2_1_HSRPORT", hwp "$CHILD2_1_HSWPORT"',
    PARTITION pt2 VALUES LESS THAN MAXVALUE
    COMMENT='dsn "$ODBC_DSN_HA2", wrapper "odbc", table "$S_2_2_HS_R3", hrp "$CHILD2_2_HSRPORT", hwp "$CHILD2_2_HSWPORT"'
  );
