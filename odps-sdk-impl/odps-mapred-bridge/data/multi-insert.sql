     
FROM (
    SELECT reducer_<jobid>(k_col1,k_col2,v_col3,v_col4,v_col5) as (out1_col1,out1_col2,out1_col3 ,out2_col1,out2_col2,out2_col3 ,MULTIDEST_LABEL )
    FROM (
        SELECT k_col1,k_col2,v_col3,v_col4,v_col5
        FROM
        (
            SELECT mapper_<jobid>(key,value ) as (k_col1,k_col2,v_col3,v_col4,v_col5)
            FROM
            (
                            SELECT key as key,value as value  FROM t_in
                                            
            ) open_mr_alias1
        ) open_mr_alias2
        DISTRIBUTE BY k_col1,k_col2 SORT BY k_col1 ASC,k_col2 ASC
    ) open_mr_alias3
) open_mr_alias4
INSERT OVERWRITE TABLE t_out
PARTITION( ds = "20120305" , hr = "18" )
SELECT out1_col1,out1_col2,out1_col3 WHERE MULTIDEST_LABEL = "out1"
INSERT OVERWRITE TABLE t_out1
SELECT out2_col1,out2_col2,out2_col3 WHERE MULTIDEST_LABEL = "out2"
;