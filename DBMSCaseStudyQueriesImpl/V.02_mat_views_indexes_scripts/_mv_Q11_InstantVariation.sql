 CREATE MATERIALIZED VIEW "DBMS_EMS_Schema"."_mv_Q11_InstantVariation" AS 

 SELECT r1.device_pk,
        r1.measure_timestamp,
        (r1.measure / (avg(r2.measure) + 0.00001::double precision) - 1::double precision) * 100::double precision AS measure,
        r1.measure                                                                  AS current_power_consumption,
        'Percentage%'                                                               AS measure_unit,
        'Variation between current and last 5 minutes average power consumption.'   AS measure_description,
        r1.device_location,
        r1.location_area_m2,
        rank() OVER w                                                               AS index

FROM "DBMS_EMS_Schema"."_mv_Q00_DataAggregation" r1
     JOIN 
     "DBMS_EMS_Schema"."_mv_Q00_DataAggregation" r2 
     ON r1.device_pk = r2.device_pk 
      AND r2.measure_timestamp > (r1.measure_timestamp - '00:05:00'::interval) 
      AND r2.measure_timestamp <= r1.measure_timestamp

GROUP BY r1.device_pk, r1.measure_timestamp, r1.measure, r1.measure_unit::text, r1.measure_description::text, r1.device_location, r1.location_area_m2
WINDOW w AS (PARTITION BY r1.device_pk 
             ORDER BY r1.measure_timestamp DESC 
             RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)

ALTER TABLE "DBMS_EMS_Schema"."_mv_Q11_InstantVariation"
  OWNER TO postgres;