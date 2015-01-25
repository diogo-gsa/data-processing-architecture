SELECT 	device_pk,
	measure_timestamp,
	measure_avg_10min/location_area_m2 				AS measure,
	'WATT/m^2' 				      			AS measure_unit,
	'Energy consumption Normalized by energy meter location area'   AS measure_description,
	device_location,
	rank
FROM 	"DBMS_EMS_Schema"."New_Q7_SmoothConsumptionsWith10MinSlindingAvg"
UNION
SELECT  0 								AS device_pk,
	to_timestamp(
		date_part('year',  measure_timestamp)::text ||'-'||
	  	date_part('month', measure_timestamp)::text ||'-'||
          	date_part('day',   measure_timestamp)::text ||' '||
	  	date_part('hour',  measure_timestamp)::text ||':'||
	  	date_part('minute',measure_timestamp)::text ,
          	'YYYY-MM-DD HH24:MI:SS'
	)::timestamp without time zone 		      			AS measure_timestamp,
	sum(measure_avg_10min)/sum(location_area_m2)  			AS measure,
	'WATT/m^2' 				      			AS measure_unit,
	'Energy consumption Normalized by energy meter location area'   AS measure_description,
	'ALL_BUILDING' 							AS device_location,
	rank()	OVER w
FROM 	"DBMS_EMS_Schema"."New_Q7_SmoothConsumptionsWith10MinSlindingAvg"
GROUP BY date_part('year',  measure_timestamp),	
	 date_part('month', measure_timestamp),
	 date_part('day',   measure_timestamp),
	 date_part('hour',  measure_timestamp),
	 date_part('minute',measure_timestamp)
HAVING count(device_pk) = 8
WINDOW w AS (ORDER BY 	date_part('year',  measure_timestamp),	
			date_part('month', measure_timestamp),
			date_part('day',   measure_timestamp),
			date_part('hour',  measure_timestamp),
			date_part('minute',measure_timestamp) DESC)

--ORDER BY measure_timestamp DESC --DEBUG
	

