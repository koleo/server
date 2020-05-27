let $S_2_1_TA_R=
  `SELECT CONCAT('ta_', $MASTER_1_MYPORT, 'r')`;
let $S_2_1_TA_R_PK=
  `SELECT CONCAT('pk_s_2_1_ta_', $MASTER_1_MYPORT, 'r')`;
let $S_2_1_TA_R_IDX1=
  `SELECT CONCAT('ta_', $MASTER_1_MYPORT, 'r_idx1')`;
let $S_2_1_TA_R2=
  `SELECT CONCAT('ta_', $MASTER_1_MYPORT, 'r2')`;
let $S_2_1_TA_R2_PK=
  `SELECT CONCAT('pk_s_2_1_ta_', $MASTER_1_MYPORT, 'r2')`;
let $S_2_1_TA_R_NO_IDX=
  `SELECT CONCAT('ta_', $MASTER_1_MYPORT, 'r_no_idx')`;
let $S_2_1_TA_R_AUTO_INC=
  `SELECT CONCAT('ta_', $MASTER_1_MYPORT, 'r_auto_inc')`;
let $S_2_1_TA_R_AUTO_INC_PK=
  `SELECT CONCAT('pk_s_2_1_ta_', $MASTER_1_MYPORT, 'r_auto_inc')`;
let $S_2_1_TA_R_INT=
  `SELECT CONCAT('s_2_1_ta_', $MASTER_1_MYPORT, 'r_int')`;
let $S_2_1_TA_R_INT_PK=
  `SELECT CONCAT('pk_s_2_1_ta_', $MASTER_1_MYPORT, 'r_int')`;
let $S_2_1_TA_R_INT_IDX1=
  `SELECT CONCAT('ta_', $MASTER_1_MYPORT, 'r_int_idx1')`;
let $S_2_1_TA_R_INT_IDX2=
  `SELECT CONCAT('ta_', $MASTER_1_MYPORT, 'r_int_idx2')`;
let $S_2_1_TA_R3=
  `SELECT CONCAT('ta_', $MASTER_1_MYPORT, 'r3')`;
let $S_2_1_FT_R=
  `SELECT CONCAT('s_2_1_ft_', $MASTER_1_MYPORT, 'r')`;
let $S_2_1_FT_R_PK=
  `SELECT CONCAT('pk_s_2_1_ft_', $MASTER_1_MYPORT, 'r')`;
let $S_2_1_FT_R_FT_IDX1=
  `SELECT CONCAT('ft_', $MASTER_1_MYPORT, 'r_ft_idx1')`;
let $S_2_1_FT_R_FT_IDX2=
  `SELECT CONCAT('ft_', $MASTER_1_MYPORT, 'r_ft_idx2')`;
let $S_2_1_FT_R2=
  `SELECT CONCAT('s_2_1_ft_', $MASTER_1_MYPORT, 'r2')`;
let $S_2_1_FT_R2_PK=
  `SELECT CONCAT('pk_s_2_1_ft_', $MASTER_1_MYPORT, 'r2')`;
let $S_2_1_FT_R2_FT_IDX1=
  `SELECT CONCAT('ft_', $MASTER_1_MYPORT, 'r2_ft_idx1')`;
let $S_2_1_FT_R2_FT_IDX2=
  `SELECT CONCAT('ft_', $MASTER_1_MYPORT, 'r2_ft_idx2')`;
let $S_2_1_GM_R=
  `SELECT CONCAT('s_2_1_gm_', $MASTER_1_MYPORT, 'r')`;
let $S_2_1_GM_R_PK=
  `SELECT CONCAT('pk_s_2_1_gm_', $MASTER_1_MYPORT, 'r')`;
let $S_2_1_GM_R_GM_IDX1=
  `SELECT CONCAT('gm_', $MASTER_1_MYPORT, 'r_gm_idx1')`;
let $S_2_1_GM_R_GM_IDX2=
  `SELECT CONCAT('gm_', $MASTER_1_MYPORT, 'r_gm_idx2')`;
let $S_2_1_GM_R2=
  `SELECT CONCAT('s_2_1_gm_', $MASTER_1_MYPORT, 'r2')`;
let $S_2_1_GM_R2_PK=
  `SELECT CONCAT('pk_s_2_1_gm_', $MASTER_1_MYPORT, 'r2')`;
let $S_2_1_GM_R2_GM_IDX1=
  `SELECT CONCAT('gm_', $MASTER_1_MYPORT, 'r2_gm_idx1')`;
let $S_2_1_GM_R2_GM_IDX2=
  `SELECT CONCAT('gm_', $MASTER_1_MYPORT, 'r2_gm_idx2')`;
let $S_2_1_T1_1=
  `SELECT CONCAT('s_2_1_t1_', $MASTER_1_MYPORT, '_1')`;
let $S_2_1_T1_1_PK=
  `SELECT CONCAT('pk_s_2_1_t1_', $MASTER_1_MYPORT, '_1')`;
let $S_2_1_T1_1_SEQ=
  `SELECT CONCAT('s_2_1_t1_', $MASTER_1_MYPORT, '_1_seq')`;
let $S_2_1_T2_2=
  `SELECT CONCAT('s_2_1_t2_', $MASTER_1_MYPORT, '_2')`;
let $S_2_1_T2_2_PK=
  `SELECT CONCAT('pk_s_2_1_t2_', $MASTER_1_MYPORT, '_2')`;
let $S_2_1_T1=
  `SELECT CONCAT('s_2_1_t1_', $MASTER_1_MYPORT)`;
let $S_2_1_T1_PK=
  `SELECT CONCAT('pk_s_2_1_t1_', $MASTER_1_MYPORT)`;
let $S_2_1_T1_IDX1=
  `SELECT CONCAT('t1_', $MASTER_1_MYPORT, '_idx1')`;
let $S_2_1_T1_IDX2=
  `SELECT CONCAT('t1_', $MASTER_1_MYPORT, '_idx2')`;

let $S_2_1_HS_R=
  `SELECT CONCAT('hs_', $MASTER_1_MYPORT, 'r')`;
let $S_2_1_HS_R_PK=
  `SELECT CONCAT('pk_s_2_1_hs_', $MASTER_1_MYPORT, 'r')`;
let $S_2_1_HS_R2=
  `SELECT CONCAT('hs_', $MASTER_1_MYPORT, 'r2')`;
let $S_2_1_HS_R2_PK=
  `SELECT CONCAT('pk_s_2_1_hs_', $MASTER_1_MYPORT, 'r2')`;
