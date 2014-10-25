---------------------------------------------------------
-- Q4 : Integration Query (with WINDOW clause + RANGE) --
---------------------------------------------------------

SELECT device_pk, device_location, measure_timestamp, ROUND(((last_measure / win_avg_measure) - 1)::numeric, 5) AS variation_10min_win
FROM	(SELECT  rel1.device_pk, rel1.device_location, rel1.measure_timestamp, rel1.measure AS last_measure, avg(rel1.measure) OVER w AS win_avg_measure, rank() OVER w
	FROM	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS rel1,
		(	SELECT device_pk, MAX(measure_timestamp) AS max_ts
			FROM "DBMS_EMS_Schema"."DenormalizedAggPhases" 
			GROUP BY device_pk
		) AS rel2
	WHERE	rel1.device_pk = rel2.device_pk
		AND rel1.measure_timestamp >= rel2.max_ts - interval '10 minutes'
	WINDOW w AS 
		(PARTITION BY rel1.device_pk 
		ORDER BY measure_timestamp DESC
		RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS rel
WHERE rank = 1



-- Correct Output
-- 1|"LIBRARY" 	     |"2014-03-17 11:59:05"|-0.01393
-- 2|"LECTUREHALL_A4"|"2014-03-17 10:02:05"|-0.00925