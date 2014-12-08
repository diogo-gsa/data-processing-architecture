
SELECT 	relA.device_pk,
	relA.measure_timestamp,
	relB.measure_timestamp,
	relA.measure_timestamp - relB.measure_timestamp AS delta
FROM	(SELECT *, rank() OVER (PARTITION BY device_pk 
				ORDER BY measure_timestamp DESC)
	 FROM "DBMS_EMS_Schema"."DenormalizedAggPhases") AS relA
	 INNER JOIN
	(SELECT *, rank() OVER (PARTITION BY device_pk 
				ORDER BY measure_timestamp DESC)
	 FROM "DBMS_EMS_Schema"."DenormalizedAggPhases") AS relB
	 ON relA.rank + 1 = relB.rank
	    AND relA.device_pk = relB.device_pk
--WHERE relA.rank = 1




