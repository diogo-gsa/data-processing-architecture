SELECT 	r1.device_pk,
		r1.measure_timestamp,
		AVG(r2.measure) AS measure_avg_10min,
		'WATT.HOUR/m^2' AS measure_unit,
		'EnergyConsumptionSliding10minAVG' AS measure_description,
		r1.device_location,
		r1.location_area_m2
		
FROM 	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS r1
		INNER JOIN
		"DBMS_EMS_Schema"."DenormalizedAggPhases" AS r2
		ON r1.device_pk = r2.device_pk
	 	AND (r2.measure_timestamp >= r1.measure_timestamp - interval '10 minutes'
	      	AND r2.measure_timestamp <= r1.measure_timestamp)	

GROUP BY r1.device_pk, 
		 r1.measure_timestamp, 
		 r1.device_location, 
		 r1.location_area_m2
--ORDER BY r1.device_pk ASC, r1.measure_timestamp DESC -- Just for DEBUG
		