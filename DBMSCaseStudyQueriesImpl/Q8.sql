SELECT	*,(measure/location_area_m2) AS normalized_measure
FROM "DBMS_EMS_Schema"."DenormalizedAggPhases"
--ORDER BY measure_timestamp
--LIMIT 10

