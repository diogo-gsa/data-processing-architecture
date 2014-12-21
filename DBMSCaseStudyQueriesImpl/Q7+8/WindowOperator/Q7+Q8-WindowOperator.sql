SELECT 	device_pk,
    	ts AS measure_timestamp,
    	measure_avg_10min,
    	'WATT.HOUR' AS measure_unit,
    	'AVG10minEnergyConsumption' AS measure_description,
    	device_location,
    	location_area_m2

FROM	(SELECT all_measures.device_pk, 
				last_measure.ts,
				avg(all_measures.measure)				OVER w AS  measure_avg_10min,
				all_measures.measure_unit::text,
				all_measures.measure_description::text,
				all_measures.device_location,
				all_measures.location_area_m2,
				rank() OVER w 

		FROM 	"DBMS_EMS_Schema"."DenormalizedAggPhases"	AS all_measures,
				(SELECT device_pk, MAX(measure_timestamp) 	AS ts
				FROM "DBMS_EMS_Schema"."DenormalizedAggPhases"
				GROUP BY device_pk) 						AS last_measure
	
		WHERE	all_measures.device_pk = last_measure.device_pk 
			AND	all_measures.measure_timestamp > last_measure.ts - interval '10 minutes'
    
		WINDOW w AS	(PARTITION BY all_measures.device_pk
					ORDER BY all_measures.measure_timestamp DESC
					RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
		) AS rel
WHERE rank = 1		
