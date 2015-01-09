SELECT 	all_measures.device_pk,
	all_measures.measure_timestamp, 
	all_measures.measure_avg_10min, 
	all_measures.measure_unit, 
	all_measures.measure_description, 
	all_measures.device_location, 
	all_measures.location_area_m2
FROM 	"DBMS_EMS_Schema"."Q7_10minAVG" AS all_measures
	INNER JOIN 
	(SELECT device_pk, 
		max(measure_timestamp) AS current_ts
	 FROM 	"DBMS_EMS_Schema"."Q7_10minAVG"
	 GROUP BY device_pk) AS most_recent_measure
	ON  most_recent_measure.device_pk = all_measures.device_pk
	AND all_measures.measure_timestamp >= most_recent_measure.current_ts  - interval '2 minutes'
	
--GROUP BY all_measures.device_pk
ORDER BY  all_measures.device_pk ASC, all_measures.measure_timestamp DESC --DEBUG

