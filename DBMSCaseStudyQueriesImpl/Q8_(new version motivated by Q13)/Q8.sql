--to_timestamp(
SELECT 	to_timestamp(
		date_part('year',  measure_timestamp)::text    ||'-'||
		date_part('month', measure_timestamp)::text    ||'-'||
		date_part('day',   measure_timestamp)::text    ||' '||
		date_part('hour',  measure_timestamp)::text    ||':'||
		date_part('minute',measure_timestamp)::text    ,
		'YYYY-MM-DD HH24:MI:SS'
	)::timestamp without time zone 		     AS measure_timestamp,
	sum(measure_avg_10min)/sum(location_area_m2) AS building_normalized_measure,
	count(device_pk)        		     AS covered_devices,
	sum(location_area_m2) 			     AS covered_area
	 
FROM "DBMS_EMS_Schema"."Q7_10minAVG"
GROUP BY date_part('year',  measure_timestamp),
	 date_part('month', measure_timestamp),
	 date_part('day',  measure_timestamp),
	 date_part('hour', measure_timestamp),
	 date_part('minute', measure_timestamp) 
ORDER BY covered_devices, measure_timestamp DESC 


/*
SELECT 	*
FROM "DBMS_EMS_Schema"."Q7_10minAVG"
ORDER BY device_pk, measure_timestamp DESC
*/
/*
SELECT 	r1.device_pk,
	r1.measure_timestamp,
	r2.measure_timestamp,
	r2.measure AS measure_avg_10min,
	'WATT.HOUR/m^2' AS measure_unit,
	'EnergyConsumptionSliding10minAVG' AS measure_description,
	r1.device_location,
	r1.location_area_m2
FROM 	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS r1
	INNER JOIN
	"DBMS_EMS_Schema"."DenormalizedAggPhases" AS r2
	ON r1.device_pk = r2.device_pk
	 AND (r2.measure_timestamp >= r1.measure_timestamp - interval '10 minutes'
	      AND r2.measure_timestamp <= r1.measure_timestamp)	
ORDER BY device_pk, r1.measure_timestamp DESC, r2.measure_timestamp DESC
--GROUP BY r1.device_pk, r1.measure_timestamp, r1.device_location, r1.location_area_m2
*/