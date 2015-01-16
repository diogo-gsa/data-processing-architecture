SELECT 	device_pk, 
	measure_timestamp, 
	normalized_measure_avg_10min 				    				    AS measure,
	get_expected_measure(device_pk::integer, measure_timestamp) 				    AS expected_measure,
	normalized_measure_avg_10min - get_expected_measure(device_pk::integer, measure_timestamp)  AS delta,
	measure_unit, 
        measure_description, 
        device_location
FROM "DBMS_EMS_Schema"."Q7+8_WindowOp";