--------------------------------------------------------
-- Q11 : Integration Query (with WINDOW clause + ROW) --
--------------------------------------------------------

SELECT 	rel.device_pk,
	rel.device_location,
	rel.measure_timestamp, 
	ROUND(((rel.last_measure / rel.last10min_avg_measure) -1 )::numeric, 5) AS win_ratio
FROM 	(SELECT *, 
		measure 	AS last_measure, 
		avg(measure) 	OVER w 	AS last10min_avg_measure, 
		rank()	 	OVER w
	FROM "DBMS_EMS_Schema"."DenormalizedAggPhases"
	WINDOW w AS (	PARTITION BY device_pk 
			ORDER BY measure_timestamp DESC
			ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING)
	) AS rel
WHERE rel.rank = 1

-- Correct Output
-- "2014-03-17 11:59:05"|-0.01393|"LIBRARY"
-- "2014-03-17 10:02:05"|-0.00925|"LECTUREHALL_A4"

	 