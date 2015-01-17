SELECT device_pk,
	date_part('hour',measure_timestamp),
	avg(measure_avg_10min),
	sum(measure_avg_10min),
	count(measure_avg_10min)
	--,avg(measure_avg_10min) 	OVER w AS expected,
	--rank() 			OVER w
FROM "DBMS_EMS_Schema"."Q7_10minAVG"
--WINDOW w AS (PARTITION BY device_pk
--	      ORDER BY measure_timestamp DESC
--	      RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
GROUP BY device_pk, date_part('hour',measure_timestamp)
ORDER BY device_pk, date_part('hour',measure_timestamp)
--ORDER BY device_pk, measure_timestamp