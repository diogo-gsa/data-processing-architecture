-- Correct Output
-- measure_timestamp; ratio; locatio
-- "2014-03-17 10:02:05"; -0.0093; "LECTUREHALL_A4"
-- "2014-03-17 11:59:05"; -0.0139; "LIBRARY"

-----------------------------------------------------
-- Q4 with WINDOW clause + ROW (Integration Query) --
-----------------------------------------------------
SELECT	now.measure_timestamp, 
	ROUND(((now.measureNow::float / timeWin10min.measureAVG10min::float) -1 )::numeric, 4) AS ratio,
	now.device_location
FROM   	(SELECT measure_timestamp, measureAVG10min, measure_unit, measure_description, device_location, location_area_m2 --GET avg over last 10 measures
	FROM 	(SELECT measure_timestamp, measure, measure_unit, measure_description, device_location, location_area_m2, avg(measure) OVER w AS measureAVG10min, rank() over w
		FROM "DBMS_EMS_Schema"."DenormalizedAggPhases"
		WINDOW w AS (	PARTITION BY device_pk 
				ORDER BY measure_timestamp DESC
				ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING)
		) AS rel
	WHERE rel.rank = 1
	)AS timeWin10min,
	(SELECT measure_timestamp, measure AS measureNow, measure_unit, measure_description, device_location, location_area_m2 -- GET last measure 
	FROM	(SELECT measure_timestamp, measure, measure_unit, measure_description, device_location, location_area_m2, rank() OVER w
		FROM "DBMS_EMS_Schema"."DenormalizedAggPhases"
		WINDOW w AS (	PARTITION BY device_pk 
				ORDER BY measure_timestamp DESC)
		) AS rel
		WHERE rel.rank = 1
	)AS now
WHERE now.device_location = timeWin10min.device_location
