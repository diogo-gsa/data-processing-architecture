/*SELECT  measure_timestamp, 
	building_normalized_measure, 
	measure_unit, 
	measure_description, 
	covered_locations, covered_area_m2
FROM 	"DBMS_EMS_Schema"."Q8_TotalAreaNormalization"

ORDER BY measure_timestamp DESC --DEBUG*/

/*SELECT  MAX(measure_timestamp) AS max_ts
FROM 	"DBMS_EMS_Schema"."Q8_TotalAreaNormalization"*/

SELECT  MAX(measure_timestamp) 		  AS measure_timestamp,
	MIN(building_normalized_measure)
	/MAX(building_normalized_measure) AS ratio,
	MAX(building_normalized_measure)  AS max_measure,
	MIN(building_normalized_measure)  AS min_measure,
	AVG(covered_locations)	  	  AS covered_locations,
	AVG(covered_area_m2)	          AS covered_area_m2

	
FROM 	"DBMS_EMS_Schema"."Q8_TotalAreaNormalization" 		AS r1,
	(SELECT  MAX(measure_timestamp) AS max_ts
	 FROM 	"DBMS_EMS_Schema"."Q8_TotalAreaNormalization") 	AS r2
WHERE 	r1.measure_timestamp >= r2.max_ts - interval '5 minutes'



ORDER BY measure_timestamp DESC --DEBUG*/
