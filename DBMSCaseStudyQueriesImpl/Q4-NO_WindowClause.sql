---------------------------------------------------
-- Q11 : Integration Query ( *NO* window clause) --
---------------------------------------------------

SELECT  now_measure.measure_timestamp, 
	ROUND(((now_measure.measure/min10avg_measure.measure_10minutes_avg) - 1)::numeric, 5) AS ratio, 
	now_measure.device_location
FROM	(
		SELECT 	rel1.measure_timestamp, measure, rel1.device_location
		FROM	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS rel1,
		(	SELECT device_location, MAX(measure_timestamp) AS max_ts
			FROM "DBMS_EMS_Schema"."DenormalizedAggPhases" 
			GROUP BY device_location
		) AS rel2
		WHERE	rel1.device_location = rel2.device_location	
		AND rel1.measure_timestamp = rel2.max_ts
	) AS now_measure,
	(
		SELECT  rel1.device_location, avg(measure) AS measure_10minutes_avg
		FROM	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS rel1,
			(	SELECT device_location, MAX(measure_timestamp) AS max_ts
				FROM "DBMS_EMS_Schema"."DenormalizedAggPhases" 
				GROUP BY device_location
			) AS rel2
		WHERE	rel1.device_location = rel2.device_location
		AND rel1.measure_timestamp >= rel2.max_ts - interval '10 minutes'
		GROUP BY rel1.device_location
	) AS min10avg_measure
WHERE 	now_measure.device_location = min10avg_measure.device_location

-- Correct Output
--"2014-03-17 11:59:05"|-0.01393|"LIBRARY"
--"2014-03-17 10:02:05"|-0.00925|"LECTUREHALL_A4"

