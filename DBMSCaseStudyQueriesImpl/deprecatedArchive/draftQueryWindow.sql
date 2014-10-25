/* --resultado correcto = -1,393216585
SELECT ((now.val::float / timeWin10min.val::float) -1)*100
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

/*
SELECT * --avg(measure) as val
FROM library_aggegrated_phases AS rel
WHERE rel.measure_timestamp >=  timestamp '2014-03-17 11:59:05' - interval '10 minutes';  --time windows
*/

SELECT measure_timestamp, measure, avg(measure) OVER win AS val
FROM library_aggegrated_phases AS rel
--WHERE rel.measure_timestamp >=  timestamp '2014-03-17 11:59:05' - interval '10 minutes'  --time windows
WINDOW win AS
	( --PARTITION location
	  ORDER BY measure_timestamp DESC
	  ROWS BETWEEN CURRENT ROW  AND 3 FOLLOWING)
--LIMIT 1;
	  