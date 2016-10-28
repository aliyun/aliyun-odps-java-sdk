     
FROM (
            SELECT mapper_<jobid>(key,value ) as (col1,col2,col3  )
            FROM
            (
                            SELECT key as key,value as value  FROM foo.t_in
                                            
            ) open_mr_alias1
) open_mr_alias4
INSERT OVERWRITE TABLE foo.t_out
SELECT col1,col2,col3 ;
