 --resultado correcto = -1,393216585
/*SELECT ((now.val::float / timeWin10min.val::float) -1)*100
FROM   (
		SELECT avg(measure) as val
		FROM library_aggegrated_phases AS rel
		WHERE rel.measure_timestamp >=  timestamp '2014-03-17 11:59:05' - interval '10 minutes'  --time windows

	)AS timeWin10min,

	(	SELECT measure as val
		FROM library_aggegrated_phases AS rel
		ORDER BY rel.measure_timestamp DESC
		LIMIT 1

	) AS now;
*/

-----------------------------------------------
-- Q4.2 GET last timestamp for each location --
-----------------------------------------------
/*SELECT 	rel1.measure_timestamp, measure, rel1.location
FROM	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS rel1,
	(	SELECT location, MAX(measure_timestamp) AS max_ts
		FROM "DBMS_EMS_Schema"."DenormalizedAggPhases" 
		GROUP BY location
	) AS rel2
WHERE	rel1.location = rel2.location
	AND rel1.measure_timestamp = rel2.max_ts */



-----------------------------------------------------------
-- Q4.1 GET last 10 minutes timestamps for each location --
-----------------------------------------------------------
/*--SELECT 	rel1.measure_timestamp, measure, rel1.location

SELECT  rel1.location, avg(measure) AS measure_10minutes_avg
FROM	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS rel1,
	(	SELECT location, MAX(measure_timestamp) AS max_ts
		FROM "DBMS_EMS_Schema"."DenormalizedAggPhases" 
		GROUP BY location
	) AS rel2
WHERE	rel1.location = rel2.location
    AND rel1.measure_timestamp >= rel2.max_ts - interval '10 minutes'
GROUP BY rel1.location

--ORDER BY measure_timestamp DESC */


SELECT  now_measure.measure_timestamp, now_measure.locatsion, ((now_measure.measure/min10avg_measure.measure_10minutes_avg) - 1) AS ratio
FROM	(
		SELECT 	rel1.measure_timestamp, measure, rel1.location
		FROM	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS rel1,
		(	SELECT location, MAX(measure_timestamp) AS max_ts
			FROM "DBMS_EMS_Schema"."DenormalizedAggPhases" 
			GROUP BY location
		) AS rel2
		WHERE	rel1.location = rel2.location
		AND rel1.measure_timestamp = rel2.max_ts
	) AS now_measure,
	(
		SELECT  rel1.location, avg(measure) AS measure_10minutes_avg
		FROM	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS rel1,
			(	SELECT location, MAX(measure_timestamp) AS max_ts
				FROM "DBMS_EMS_Schema"."DenormalizedAggPhases" 
				GROUP BY location
			) AS rel2
		WHERE	rel1.location = rel2.location
		AND rel1.measure_timestamp >= rel2.max_ts - interval '10 minutes'
		GROUP BY rel1.location
	) AS min10avg_measure
WHERE 	now_measure.location = min10avg_measure.location
