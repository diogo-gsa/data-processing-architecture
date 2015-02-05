SELECT  rela.device_pk,
        rela.measure_timestamp                                    AS measure_timestamp,
        rela.measure_timestamp - relb.measure_timestamp           AS measure,
        'Time Seconds'                                            AS measure_unit,
        'Period between two last power consumption measurements.' AS measure_description,
        rela.device_location,
        rela.location_area_m2,
        rela.index

FROM    "DBMS_EMS_Schema"."_Q00_DataAggregation" rela
        JOIN 
        "DBMS_EMS_Schema"."_Q00_DataAggregation" relb 
        ON rela.device_pk = relb.device_pk 
        AND (rela.index + 1) = relb.index