SELECT 	device_pk,
    	measure_timestamp,
    	measure 											AS measure,
    	"DBMS_EMS_Schema".get_expected_measure_udf(device_pk::integer, measure_timestamp)		AS expected_measure,
    	measure - "DBMS_EMS_Schema".get_expected_measure_udf(device_pk::integer, measure_timestamp) 	AS delta,
    	measure_unit,
    	measure_description,
    	device_location,
    	rank
FROM "DBMS_EMS_Schema"."New_Q8_NormalizeConsumptionsByLocationSquareMeters"