SELECT 	all_measures.device_pk, 
	last_measure.ts,
	avg(all_measures.measure) AS  measure_avg_10min,
	all_measures.measure_unit::text,
	all_measures.measure_description::text,
	all_measures.device_location,
	all_measures.location_area_m2,
	avg(all_measures.measure)/all_measures.location_area_m2 AS normalized_measure_avg_10min

FROM 	"DBMS_EMS_Schema"."DenormalizedAggPhases"	AS all_measures,
	(SELECT device_pk, 
		MAX(measure_timestamp) 	AS ts
	FROM "DBMS_EMS_Schema"."DenormalizedAggPhases"
	GROUP BY device_pk) 				AS last_measure
	
WHERE	all_measures.device_pk = last_measure.device_pk 
    AND	all_measures.measure_timestamp > last_measure.ts - interval '10 minutes'

GROUP BY all_measures.device_pk,
	 last_measure.ts,
	 all_measures.measure_unit::text,
	 all_measures.measure_description::text,
	 all_measures.device_location,
	 all_measures.location_area_m2
	
