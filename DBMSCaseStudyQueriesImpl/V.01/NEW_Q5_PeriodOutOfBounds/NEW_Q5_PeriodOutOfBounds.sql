SELECT 	*
FROM "DBMS_EMS_Schema"."New_Q12_PeriodBetweenDatastreamTuples"
WHERE rank = 1 
   AND NOT('00:00:55' <= delta  AND  delta <= '00:01:05')