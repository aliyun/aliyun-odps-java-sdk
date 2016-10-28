     
FROM (
            SELECT mapper_<jobid>(key,value ) as (nil)
            FROM
            (
                            SELECT key as key,value as value  FROM t_in
                                            
            ) open_mr_alias1
) open_mr_alias4
SELECT *
;