-- _Q0_
SELECT  dev.device_pk,
        dpr.measure_timestamp,
        sum(dpr.measure)          AS measure,
        'WATT'                    AS measure_unit,
        'Power Consumption: 3-Phase Aggregate.' AS measure_description,
        dl.location               AS device_location,
        dl.area_m2                AS location_area_m2,
        rank() OVER w             AS index

FROM "DBMS_EMS_Schema"."DataPoint" dp
      JOIN "DBMS_EMS_Schema"."DataPointReading" dpr 
      ON dpr.datapoint_fk = dp.datapoint_pk
      JOIN "DBMS_EMS_Schema"."Device" dev 
      ON dp.device_fk = dev.device_pk
      JOIN "DBMS_EMS_Schema"."DeviceLocation" dl 
      ON dev.device_location_fk = dl.device_location_pk
      JOIN "DBMS_EMS_Schema"."DataPointDescription" dpd 
      ON dp.datapoint_description_fk = dpd.datapoint_description_pk
      JOIN "DBMS_EMS_Schema"."DataPointUnit" dpu 
      ON dp.datapoint_unit_fk = dpu.datapoint_unit_pk

WHERE dpd.description::text = 'Phase1_EnergyConsumption'::text 
   OR dpd.description::text = 'Phase2_EnergyConsumption'::text 
   OR dpd.description::text = 'Phase3_EnergyConsumption'::text

GROUP BY dev.device_pk, dpr.measure_timestamp, dl.location, dl.area_m2

HAVING count(dpr.measure) = 3

WINDOW w AS (PARTITION BY dev.device_pk 
             ORDER BY dpr.measure_timestamp DESC)