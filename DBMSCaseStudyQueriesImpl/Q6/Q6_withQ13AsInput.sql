SELECT	device_pk, 
		measure_timestamp, 
		(measure/(cluster_expected_measure+0.0001) - 1)*100 							AS measure,
		measure																			AS current_cosnumption,
		cluster_expected_measure			    										AS expected_consumption, 
		'%percent' 																		AS measure_unit,
		'Percent variation between current and expected consumption greater than 10%' 	AS measure_description, 
		device_location
FROM 	"DBMS_EMS_Schema"."Q13_CurrentAndExpectedHourClusterMeasure"
WHERE 	(measure/(cluster_expected_measure+0.0001) - 1)*100 > 10 
-- +0.0001 to avoid division-by-zero when expected_measure = 0