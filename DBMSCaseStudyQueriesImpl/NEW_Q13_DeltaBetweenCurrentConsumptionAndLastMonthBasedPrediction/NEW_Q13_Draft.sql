SELECT	all_measures.device_pk,
	most_recent_measure.measure_timestamp		    AS most_recent_ts,
	all_measures.measure_timestamp		            AS all_measures_ts,
	date_part('hour', all_measures.measure_timestamp)   AS cluster_pivot_hour,
	all_measures.measure 				    AS all_measure,
	avg(all_measures.measure) 	OVER w	  AS cluster_historic_measure,
	/*count(all_measures.measure)	OVER w	  AS cluster_dataset_size,
	rank()				OVER w */
	most_recent_measure.rank,
	all_measures.rank
	
FROM 	"DBMS_EMS_Schema"."New_Q8_NormalizeConsumptionsByLocationSquareMeters"   	AS all_measures
	INNER JOIN 
	"DBMS_EMS_Schema"."New_Q8_NormalizeConsumptionsByLocationSquareMeters"		AS most_recent_measure
	ON  /*most_recent_measure.rank 	    = 1
	AND */most_recent_measure.device_pk   = all_measures.device_pk	
	AND all_measures.measure_timestamp > most_recent_measure.measure_timestamp  - interval '3 minutes'
	AND all_measures.measure_timestamp <= most_recent_measure.measure_timestamp

WINDOW 	w AS (PARTITION BY all_measures.device_pk, 
			   date_part('hour', all_measures.measure_timestamp),
			   most_recent_measure.rank
	     ORDER BY all_measures.measure_timestamp DESC --NOTA que em principio tbm vais ter de partir por most_recent_measure.rank,
	     RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)

ORDER BY device_pk, most_recent_measure.measure_timestamp DESC, all_measures.measure_timestamp DESC