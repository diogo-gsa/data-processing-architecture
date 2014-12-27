SELECT 	to_timestamp(
	  date_part('year',  measure_timestamp)::text ||'-'||
	  date_part('month', measure_timestamp)::text ||'-'||
          date_part('day',   measure_timestamp)::text ||' '||
	  date_part('hour',  measure_timestamp)::text ||':'||
	  date_part('minute',measure_timestamp)::text ,
          'YYYY-MM-DD HH24:MI:SS'
	)::timestamp without time zone 		      AS measure_timestamp,
	sum(measure_avg_10min)/sum(location_area_m2)  AS building_normalized_measure,
	'WATT.HOUR/m2' 				      AS measure_unit,
	'EnergyConsumption_NormalizedByTotalArea'     AS measure_description,
	count(device_pk)        		      AS covered_locations,
	sum(location_area_m2) 			      AS covered_area_m2
	
FROM "DBMS_EMS_Schema"."Q7_10minAVG"
GROUP BY date_part('year',  measure_timestamp),	
	 date_part('month', measure_timestamp),
	 date_part('day',   measure_timestamp),
	 date_part('hour',  measure_timestamp),
	 date_part('minute',measure_timestamp) 
--ORDER BY covered_locations, measure_timestamp DESC -- DEBUG
