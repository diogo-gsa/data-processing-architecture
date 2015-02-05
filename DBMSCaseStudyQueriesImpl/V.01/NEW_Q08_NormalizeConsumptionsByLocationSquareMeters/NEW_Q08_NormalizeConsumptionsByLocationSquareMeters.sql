SELECT 	device_pk,
	measure_timestamp,
	measure_avg_10min/location_area_m2 					AS measure,
	'WATT/m^2'::text			      				AS measure_unit,
	'Energy consumption Normalized by energy meter location area'::text   	AS measure_description,
	device_location::text,
	rank
FROM 	"DBMS_EMS_Schema"."New_Q7_SmoothConsumptionsWith10MinSlindingAvg"
UNION
SELECT 	*,
	rank()	OVER w
FROM	(SELECT  0 								AS device_pk,
		to_timestamp(
			date_part('year',  measure_timestamp)::text ||'-'||
			date_part('month', measure_timestamp)::text ||'-'||
			date_part('day',   measure_timestamp)::text ||' '||
			date_part('hour',  measure_timestamp)::text ||':'||
			date_part('minute',measure_timestamp)::text ,
			'YYYY-MM-DD HH24:MI:SS'
		)::timestamp without time zone 		      				AS measure_timestamp,
		sum(measure_avg_10min)/sum(location_area_m2)  				AS measure,
		'WATT/m^2'::text 				      			AS measure_unit,
		'Energy consumption Normalized by energy meter location area'::text   	AS measure_description,
		'ALL_BUILDING'::text 							AS device_location
	FROM 	"DBMS_EMS_Schema"."New_Q7_SmoothConsumptionsWith10MinSlindingAvg"
	GROUP BY date_part('year',  measure_timestamp),	
		 date_part('month', measure_timestamp),
		 date_part('day',   measure_timestamp),
		 date_part('hour',  measure_timestamp),
		 date_part('minute',measure_timestamp)
	HAVING count(device_pk) = 8
	) AS rel
WINDOW w AS (ORDER BY measure_timestamp DESC)