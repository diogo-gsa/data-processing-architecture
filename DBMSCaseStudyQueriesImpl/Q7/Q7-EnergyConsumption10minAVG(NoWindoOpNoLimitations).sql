SELECT 	R1.device_pk,
	R1.measure_timestamp,
	R1.measure,
	R2.device_pk,
	R2.measure_timestamp,
	R2.measure
FROM 	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS R1
	INNER JOIN
	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS R2
	ON R1.device_pk = R2.device_pk
	  AND R2.measure_timestamp >= R1.measure_timestamp - interval '3 minutes'
ORDER BY R1.device_pk ASC, R1.measure_timestamp DESC, R2.measure_timestamp DESC
	 