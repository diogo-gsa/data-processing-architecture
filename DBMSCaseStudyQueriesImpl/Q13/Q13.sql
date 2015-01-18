SELECT 	cluster_rel.device_pk,
	cluster_rel.measure_timestamp,
	all_measures.measure_avg_10min,
	cluster_rel.cluster_timestamp,
	cluster_rel.date_part,
	cluster_rel.avg,
	cluster_rel.sum,
	cluster_rel.count,
	cluster_rel.rank
FROM	(SELECT	all_measures.device_pk,
		most_recent_measure.ts					AS measure_timestamp,
		all_measures.measure_timestamp				AS cluster_timestamp,
		date_part('hour', all_measures.measure_timestamp),
		avg(all_measures.measure_avg_10min) 			OVER w,
		sum(all_measures.measure_avg_10min) 			OVER w,
		count(all_measures.measure_avg_10min) 			OVER w,
		rank()							OVER w
	
	FROM 	"DBMS_EMS_Schema"."Q7_10minAVG"   		AS all_measures
		INNER JOIN 
		(SELECT device_pk, 
			max(measure_timestamp) 	AS ts
		FROM 	"DBMS_EMS_Schema"."Q7_10minAVG"
		GROUP BY device_pk) 		   		AS most_recent_measure
	
	ON  most_recent_measure.device_pk = all_measures.device_pk	
	AND all_measures.measure_timestamp >=  most_recent_measure.ts  - interval '3 day'

	WINDOW w AS (PARTITION BY all_measures.device_pk, date_part('hour', all_measures.measure_timestamp)
		 ORDER BY all_measures.measure_timestamp DESC
		 RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
	) 				AS cluster_rel
	INNER JOIN 
	"DBMS_EMS_Schema"."Q7_10minAVG"	AS all_measures
	ON  cluster_rel.rank = 1 
	AND all_measures.device_pk = cluster_rel.device_pk
	AND all_measures.measure_timestamp = cluster_rel.measure_timestamp	
	AND date_part('hour', all_measures.measure_timestamp) = cluster_rel.date_part


/*SELECT *
FROM	(SELECT	all_measures.device_pk,
		most_recent_measure.ts					AS measure_timestamp,
		all_measures.measure_timestamp				AS cluster_timestamp,
		date_part('hour', all_measures.measure_timestamp),
		avg(all_measures.measure_avg_10min) 			OVER w,
		sum(all_measures.measure_avg_10min) 			OVER w,
		count(all_measures.measure_avg_10min) 			OVER w,
		rank()							OVER w
	
	FROM 	"DBMS_EMS_Schema"."Q7_10minAVG"   		AS all_measures
		INNER JOIN 
		(SELECT device_pk, 
			max(measure_timestamp) 	AS ts
		FROM 	"DBMS_EMS_Schema"."Q7_10minAVG"
		GROUP BY device_pk) 		   		AS most_recent_measure
	
	ON  most_recent_measure.device_pk = all_measures.device_pk	
	AND all_measures.measure_timestamp >=  most_recent_measure.ts  - interval '3 day'

	WINDOW w AS (PARTITION BY all_measures.device_pk, date_part('hour', all_measures.measure_timestamp)
		 ORDER BY all_measures.measure_timestamp DESC
		 RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
	) AS cluster_rel
WHERE cluster_rel.rank = 1*/
