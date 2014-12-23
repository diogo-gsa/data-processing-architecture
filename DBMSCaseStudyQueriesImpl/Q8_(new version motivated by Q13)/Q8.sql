SELECT 	count(device_pk),
	 date_part('year',  measure_timestamp),
	 date_part('month', measure_timestamp),
	 date_part('day',  measure_timestamp),
	 date_part('hour', measure_timestamp),
	 date_part('minute', measure_timestamp)
	--date_part('hour', measure_timestamp)

	--measure_avg_10min, 
	--measure_unit, 
	--measure_description, 
	--device_location, location_area_m2
FROM "DBMS_EMS_Schema"."Q7_10minAVG"
GROUP BY date_part('year',  measure_timestamp),
	 date_part('month', measure_timestamp),
	 date_part('day',  measure_timestamp),
	 date_part('hour', measure_timestamp),
	 date_part('minute', measure_timestamp)

--ORDER BY device_pk, measure_timestamp DESC

