SELECT  device_pk,
        measure_timestamp,
        measure,
        'Time Seconds' AS measure_unit,
        'Period between two last power consumption measurements is out of range: [55, 65] seconds.' AS meausre_description,
        device_location,
        location_area_m2

FROM    "DBMS_EMS_Schema"."_Q12_DataStreamPeriodicity"

WHERE   index = 1 
    AND NOT('00:00:55' <= measure  AND  measure <= '00:01:05')