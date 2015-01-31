SELECT 	dga.device_pk,
	dga.measure_timestamp,	
	(dga.measure/(avg(dga.measure) 	OVER w +0.00001) - 1) 	AS variation,	
	dga.measure 						AS current_measure,
	avg(dga.measure) 		OVER w 			AS win_measure,
	dga.device_location,
	rank() 		 		OVER w 			AS rank
	
FROM 	"DBMS_EMS_Schema"."DenormalizedAggPhases" 	AS dga,
	(SELECT "DenormalizedAggPhases".device_pk,
		max("DenormalizedAggPhases".measure_timestamp) AS ts
	FROM "DBMS_EMS_Schema"."DenormalizedAggPhases"
	GROUP BY "DenormalizedAggPhases".device_pk) 	AS dga_last_ts
	
WHERE dga.device_pk = dga_last_ts.device_pk 
	AND dga.measure_timestamp > (dga_last_ts.ts - '00:05:00'::interval)
	
WINDOW w AS 	(PARTITION BY dga.device_pk 	
		ORDER BY dga.measure_timestamp 
		DESC RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
