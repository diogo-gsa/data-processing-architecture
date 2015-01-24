-- NEW_Q7
SELECT 	r1.device_pk,
	r1.measure_timestamp,
	avg(r2.measure) 			AS measure_avg_10min,
	'WATT.HOUR' 				AS measure_unit,
	'EnergyConsumptionSliding10minAVG' 	AS measure_description,
	r1.device_location,
	r1.location_area_m2,
	rank()	OVER w

FROM 	"DBMS_EMS_Schema"."DenormalizedAggPhases" r1
	JOIN 
	"DBMS_EMS_Schema"."DenormalizedAggPhases" r2 
	ON r1.device_pk = r2.device_pk 
	AND r2.measure_timestamp >= (r1.measure_timestamp - '00:10:00'::interval) 
	AND r2.measure_timestamp <= r1.measure_timestamp

GROUP BY r1.device_pk, r1.measure_timestamp, r1.device_location, r1.location_area_m2

WINDOW w AS (PARTITION BY r1.device_pk
	     ORDER BY 	  r1.measure_timestamp DESC)
--DEBUG
--ORDER BY device_pk ASC, measure_timestamp DESC 
		
	
