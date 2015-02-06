SELECT  device_pk,
        measure_timestamp,
        measure 									  AS current_measure,
        "DBMS_EMS_Schema".get_expected_measure_udf(device_pk::integer, measure_timestamp) AS expected_measure,
        'WATT/m^2'									  AS measure_unit,
        'Current and Expected Power consumption given by a User Defined Function (UDF).'  AS measure_description,
        device_location
FROM    "DBMS_EMS_Schema"."_Q08_SquareMeterNormalization"