--resultado correcto = -1,393216585
/*
SELECT *--((now.measure::float / timeWin10min.measure::float) -1)*100

FROM   	(	SELECT location, avg(measure) AS measure
		FROM "DBMS_EMS_Schema"."DenormalizedAggPhases"
		WHERE measure_timestamp >=  timestamp '2014-03-17 11:59:05' - interval '10 minutes'  --time windows
		GROUP BY location

	)AS timeWin10min,

	(	SELECT location, measure -- Q4.1
		FROM "DBMS_EMS_Schema"."DenormalizedAggPhases"
		GROUP BY location
		ORDER BY measure_timestamp DESC
		LIMIT 1
	)AS now; 
*/
-- Q4 = Q4.1 + Q4.2

-- Q4.1
/*
SELECT measure_timestamp, measure, unit, description, location, area_m2
FROM	(SELECT measure_timestamp, measure, unit, description, location, area_m2, rank() OVER w
	FROM "DBMS_EMS_Schema"."DenormalizedAggPhases"
	WINDOW w AS (PARTITION BY location ORDER BY measure_timestamp DESC)) AS rel
WHERE rel.rank = 1
*/

-- Q4.2
SELECT measure_timestamp, measure, unit, description, location, area_m2, avg(measure) OVER w, rank() over w
FROM "DBMS_EMS_Schema"."DenormalizedAggPhases"
WINDOW w AS (	PARTITION BY location 
		ORDER BY measure_timestamp DESC
		ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING -- ESTOU AQUI: RANGE WITH INTERVAL IS NOT YET IMPLEMENTED, WHAT A FUCK???
	    )
	    