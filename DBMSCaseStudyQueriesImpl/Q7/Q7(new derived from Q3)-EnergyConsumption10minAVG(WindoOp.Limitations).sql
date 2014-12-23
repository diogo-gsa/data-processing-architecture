SELECT 	device_pk, 
	measure_timestamp,
	measure,
	avg(measure)		OVER w AS  measure_avg_10min,
	measure_unit::text,
	measure_description::text,
	device_location,
	location_area_m2,
	rank() 			OVER w 
FROM 	"DBMS_EMS_Schema"."DenormalizedAggPhases"
WINDOW w AS (PARTITION BY device_pk
	     ORDER BY measure_timestamp DESC
	     -- 1(own row) + 9 = 10 minutes
	     ROWS BETWEEN CURRENT ROW AND 9 FOLLOWING)
	     