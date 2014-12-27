SELECT  MAX(measure_timestamp) 		  AS measure_timestamp,
	MIN(building_normalized_measure)
	/MAX(building_normalized_measure) AS min_max_measure_ratio,
	MAX(building_normalized_measure)  AS max_measure,
	MIN(building_normalized_measure)  AS min_measure,
	AVG(covered_devices)	  	  AS covered_devices,
	AVG(covered_area_m2)	          AS covered_area_m2	
FROM 	"DBMS_EMS_Schema"."Q8_NormalizationAllBuilding" 		AS r1,
	(SELECT  MAX(measure_timestamp) AS max_ts
	 FROM 	"DBMS_EMS_Schema"."Q8_NormalizationAllBuilding") 	AS r2
WHERE 	r1.measure_timestamp >= r2.max_ts - interval '60 minutes'
