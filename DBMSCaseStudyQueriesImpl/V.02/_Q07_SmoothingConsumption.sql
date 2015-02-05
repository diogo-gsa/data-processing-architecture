SELECT 	r1.device_pk,
	r1.measure_timestamp,
	avg(r2.measure) 						  AS measure,
	'WATT'	 							  AS measure_unit,
	'Smoothed Power Consumption through 10 minutes sliding average.'  AS measure_description,
	r1.device_location,
	r1.location_area_m2,
	rank() OVER w 							  AS index

FROM 	"DBMS_EMS_Schema"."_Q00_DataAggregation" r1
	JOIN 
	"DBMS_EMS_Schema"."_Q00_DataAggregation" r2 
	ON  r1.device_pk = r2.device_pk 
	AND r2.measure_timestamp >= (r1.measure_timestamp - '00:10:00'::interval) 
	AND r2.measure_timestamp <= r1.measure_timestamp

GROUP BY r1.device_pk, 
	 r1.measure_timestamp, 
	 r1.device_location, 
	 r1.location_area_m2

WINDOW w AS (PARTITION BY r1.device_pk 
	     ORDER BY r1.measure_timestamp DESC)