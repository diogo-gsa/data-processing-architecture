-- Correct Output
-- measure_timestamp; ratio; locatio
-- "2014-03-17 10:02:05"; -0.0093; "LECTUREHALL_A4"
-- "2014-03-17 11:59:05"; -0.0139; "LIBRARY"

-----------------------------------------------
-- Q4 with WINDOW clause (Integration Query) --
-----------------------------------------------
SELECT	now.measure_timestamp, 
	ROUND(((now.measureNow::float / timeWin10min.measureAVG10min::float) -1 )::numeric, 4) AS ratio,
	now.location
FROM   	(SELECT measure_timestamp, measureAVG10min, unit, description, location, area_m2 --GET avg over last 10 measures
	FROM 	(SELECT measure_timestamp, measure, unit, description, location, area_m2, avg(measure) OVER w AS measureAVG10min, rank() over w
		FROM "DBMS_EMS_Schema"."DenormalizedAggPhases"
		WINDOW w AS (	PARTITION BY location 
				ORDER BY measure_timestamp DESC
				ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING)
		) AS rel
	WHERE rel.rank = 1
	)AS timeWin10min,
	(SELECT measure_timestamp, measure AS measureNow, unit, description, location, area_m2 -- GET last measure 
	FROM	(SELECT measure_timestamp, measure, unit, description, location, area_m2, rank() OVER w
		FROM "DBMS_EMS_Schema"."DenormalizedAggPhases"
		WINDOW w AS (	PARTITION BY location 
				ORDER BY measure_timestamp DESC)
		) AS rel
		WHERE rel.rank = 1
	)AS now
WHERE now.location = timeWin10min.location
