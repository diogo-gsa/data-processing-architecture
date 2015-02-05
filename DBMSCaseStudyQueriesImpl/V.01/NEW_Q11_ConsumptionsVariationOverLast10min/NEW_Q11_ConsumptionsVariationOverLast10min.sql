-- New Q11
SELECT  r1.device_pk,
	r1.measure_timestamp,
	(((r1.measure/(avg(r2.measure)+0.00001))-1)*100)	AS variation,	
	r1.measure 					AS current_measure,
	avg(r2.measure) 				AS win_measure,
	r1.measure_unit::text,
	r1.measure_description::text,
	r1.device_location,
	r1.location_area_m2,
	rank()	OVER w
	
FROM 	"DBMS_EMS_Schema"."New_Q0_JoinDatastreamWithStaticData" 	AS r1
	INNER JOIN
	"DBMS_EMS_Schema"."New_Q0_JoinDatastreamWithStaticData" 	AS r2
	ON  r1.device_pk = r2.device_pk 
	AND r2.measure_timestamp > (r1.measure_timestamp - '00:05:00'::interval) 
	AND r2.measure_timestamp <= r1.measure_timestamp

GROUP BY r1.device_pk, 
	 r1.measure_timestamp, 
	 r1.measure, 
	 r1.measure_unit::text, 
	 r1.measure_description::text, 
	 r1.device_location, 
	 r1.location_area_m2
		
WINDOW w AS (PARTITION BY r1.device_pk 	
	     ORDER BY r1.measure_timestamp 
	     DESC RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)