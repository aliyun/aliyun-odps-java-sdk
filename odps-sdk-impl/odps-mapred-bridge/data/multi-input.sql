     
FROM (
    SELECT reducer_<jobid>(k_col1,k_col2,v_col3,v_col4,v_col5) as (col1,col2,col3  )
    FROM (
        SELECT k_col1,k_col2,v_col3,v_col4,v_col5
        FROM
        (
            SELECT mapper_<jobid>(key,value ) as (k_col1,k_col2,v_col3,v_col4,v_col5)
            FROM
            (
                            SELECT key as key,value as value  FROM t_in
                                WHERE ds = "20120305"  AND hr = "18"                                 UNION ALL                            SELECT key as key,value as value  FROM t_in1
                                            
            ) open_mr_alias1
        ) open_mr_alias2
        DISTRIBUTE BY k_col1,k_col2 SORT BY k_col1 ASC,k_col2 ASC
    ) open_mr_alias3
) open_mr_alias4
INSERT OVERWRITE TABLE t_out
SELECT col1,col2,col3 ;