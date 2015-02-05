SELECT 	relA.device_pk,
		relA.device_location,
		relA.measure_timestamp AS measure_timestamp_last,
		relB.measure_timestamp AS measure_timestamp_2nd_last,
		relA.measure_timestamp - relB.measure_timestamp AS delta
FROM	(SELECT *, rank() OVER (PARTITION BY device_pk 
								ORDER BY measure_timestamp DESC)
	 	FROM "DBMS_EMS_Schema"."DenormalizedAggPhases") AS relA
	 	INNER JOIN
		(SELECT *, rank() OVER (PARTITION BY device_pk 
								ORDER BY measure_timestamp DESC)
	 	FROM "DBMS_EMS_Schema"."DenormalizedAggPhases") AS relB
	 	ON relA.device_pk = relB.device_pk 
	    	AND relA.rank + 1 = relB.rank
WHERE relA.rank = 1




