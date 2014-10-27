---------------------------------------------------
-- Q11 : Integration Query ( *NO* window clause) --
---------------------------------------------------

SELECT 	last_reading.device_pk,
	last_reading.device_location,
	last_reading.measure_timestamp, 
	ROUND(((last_reading.measure/last10min_avg_reading.measure) - 1)::numeric, 5) AS variation_10min_win

FROM	(SELECT	dap.device_pk, dap.measure_timestamp, dap.measure, dap.device_location
	FROM	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS dap,
		(SELECT device_pk, MAX(measure_timestamp) AS measure_timestamp
		FROM "DBMS_EMS_Schema"."DenormalizedAggPhases" 
		GROUP BY device_pk ) AS dap_last_ts
	WHERE	dap.device_pk = dap_last_ts.device_pk	
		AND dap.measure_timestamp = dap_last_ts.measure_timestamp
	) AS last_reading,
	
	(SELECT  dap.device_pk, AVG(dap.measure) AS measure, dap.device_location
	FROM	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS dap,
		(SELECT device_pk, MAX(measure_timestamp) AS measure_timestamp
		FROM "DBMS_EMS_Schema"."DenormalizedAggPhases" 
		GROUP BY device_pk ) AS dap_last_ts
	WHERE	dap.device_pk = dap_last_ts.device_pk
		AND dap.measure_timestamp >= dap_last_ts.measure_timestamp - interval '10 minutes'
	GROUP BY dap.device_pk, dap.device_location
	) AS last10min_avg_reading
	
WHERE 	last_reading.device_pk = last10min_avg_reading.device_pk

-- Correct Output
--"2014-03-17 11:59:05"|-0.01393|"LIBRARY"
--"2014-03-17 10:02:05"|-0.00925|"LECTUREHALL_A4"
