----------------------------------------------------------------------
-- Q4 : Integration Query (with WINDOW clause + RANGE"workaounded") --
----------------------------------------------------------------------

SELECT 	device_pk, 
	device_location, 
	measure_timestamp, 
	ROUND(((last_measure / win_avg_measure) - 1)::numeric, 5) AS variation_10min_win
FROM	(SELECT dga.device_pk,
		dga.device_location,
		dga.measure_timestamp, 
		dga.measure 	  AS last_measure, 
		avg(dga.measure)  OVER w AS win_avg_measure, 
		rank() 		  OVER w
	FROM	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS dga,
		(	SELECT device_pk, MAX(measure_timestamp) AS ts
			FROM "DBMS_EMS_Schema"."DenormalizedAggPhases" 
			GROUP BY device_pk
		) AS dga_last_ts
	WHERE	dga.device_pk = dga_last_ts.device_pk
		AND dga.measure_timestamp >= dga_last_ts.ts - interval '10 minutes'
	WINDOW w AS 
		(PARTITION BY dga.device_pk 
		ORDER BY measure_timestamp DESC
		RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
	) AS rel
WHERE rank = 1

-- Correct Output
-- 1|"LIBRARY" 	     |"2014-03-17 11:59:05"|-0.01393
-- 2|"LECTUREHALL_A4"|"2014-03-17 10:02:05"|-0.00925