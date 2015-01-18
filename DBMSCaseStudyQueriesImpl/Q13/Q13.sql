SELECT 	all_measures.device_pk,
	all_measures.measure_timestamp,
	all_measures.measure_avg_10min						AS measure,
	clustered_measures.cluster_avg_measure					AS cluster_expected_measure,
	all_measures.measure_avg_10min - clustered_measures.cluster_avg_measure	AS delta,
	clustered_measures.cluster_pivot_hour,					
	clustered_measures.cluster_timestamp					AS cluster_last_update,
	clustered_measures.cluster_dataset_size/60				AS cluster_dataset_days,
	measure_unit,
	'Delta between current and last month avg consumption '			AS measure_description,
	device_location
	
FROM	(SELECT	all_measures.device_pk,
		most_recent_measure.ts				  AS measure_timestamp,
		all_measures.measure_timestamp		          AS cluster_timestamp,
		date_part('hour', all_measures.measure_timestamp) AS cluster_pivot_hour,
		avg(all_measures.measure_avg_10min) 	OVER w	  AS cluster_avg_measure,
		count(all_measures.measure_avg_10min)	OVER w	  AS cluster_dataset_size,
		rank()					OVER w
	
	FROM 	"DBMS_EMS_Schema"."Q7_10minAVG"   	AS all_measures
		INNER JOIN 
		(SELECT device_pk, 
			max(measure_timestamp) 	AS ts
		FROM 	"DBMS_EMS_Schema"."Q7_10minAVG"
		GROUP BY device_pk) 			AS most_recent_measure
		ON  most_recent_measure.device_pk   = all_measures.device_pk	
		AND all_measures.measure_timestamp >= most_recent_measure.ts  - interval '3 day'

	WINDOW 	w AS (PARTITION BY all_measures.device_pk, 
				  date_part('hour', all_measures.measure_timestamp)
		     ORDER BY all_measures.measure_timestamp DESC
		     RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
	) AS clustered_measures
	INNER JOIN 
	"DBMS_EMS_Schema"."Q7_10minAVG"	AS all_measures
	ON  clustered_measures.rank = 1 
	AND all_measures.device_pk = clustered_measures.device_pk
	AND all_measures.measure_timestamp = clustered_measures.measure_timestamp	
	AND date_part('hour', all_measures.measure_timestamp) = clustered_measures.cluster_pivot_hour

