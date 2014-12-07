SELECT 	relA.device_pk,
	relA.measure_timestamp,
	relB.measure_timestamp,
	relA.measure_timestamp - relB.measure_timestamp AS delta
FROM	(SELECT *, rank() OVER (PARTITION BY device_pk 
				ORDER BY measure_timestamp DESC)
	 FROM "DBMS_EMS_Schema"."DenormalizedAggPhases") AS relA,
	(SELECT *, rank() OVER (PARTITION BY device_pk 
				ORDER BY measure_timestamp DESC)
	 FROM "DBMS_EMS_Schema"."DenormalizedAggPhases") AS relB
WHERE relB.rank = (relA.rank + 1) 
  AND relA.device_pk = relB.device_pk
--LIMIT 1

	




