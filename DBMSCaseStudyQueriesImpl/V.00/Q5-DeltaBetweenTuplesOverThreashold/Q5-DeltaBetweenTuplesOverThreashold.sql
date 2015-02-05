SELECT 	*
FROM "DBMS_EMS_Schema"."Q12_DeltaBetweenTuples"
WHERE  delta < '00:00:50' 
   OR  delta > '00:01:10'
