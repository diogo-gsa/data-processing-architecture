SELECT	device_pk, 
		measure_timestamp, 
		(measure/(expected_measure+0.0001) - 1)*100 									AS measure,
		measure					 														AS current_consumption,
		expected_measure+0.0001 														AS expected_consumption, 
		'%percent' 																		AS measure_unit,
		'Percent variation between current and expected consumption greater than 10%' 	AS measure_description, 
		device_location
FROM 	"DBMS_EMS_Schema"."Q14_CurrentAndExpectedUDFMeasure"
WHERE 	(measure/(expected_measure+0.0001) - 1)*100 > 10 
-- +0.0001 to avoid division-by-zero when expected_measure = 0 