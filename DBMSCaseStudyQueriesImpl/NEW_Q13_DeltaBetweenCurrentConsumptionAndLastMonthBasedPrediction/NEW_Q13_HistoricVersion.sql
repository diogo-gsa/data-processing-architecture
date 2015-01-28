SELECT	device_pk,
	measure_timestamp_pivot, -- ALTEREI ISTO 
	measure,
	expecetd_measure,
	measure - expecetd_measure AS delta,
	measure_description,
	measure_unit,		
	device_location,
	rank()	OVER w2
	
FROM	(SELECT	pivot_measures.device_pk,
		pivot_measures.measure_timestamp		AS measure_timestamp_pivot,   -- ALTEREI ISTO 
		cluster_measures.measure_timestamp		AS measure_timestamp_cluster, -- ALTEREI ISTO
		pivot_measures.measure 				AS measure,
		avg(cluster_measures.measure)	     OVER w1	AS expecetd_measure,
		pivot_measures.measure_description,
		pivot_measures.measure_unit,
		pivot_measures.device_location,	
		rank()				     OVER w1
		
	FROM 	"DBMS_EMS_Schema"."New_Q8_NormalizeConsumptionsByLocationSquareMeters"   	AS cluster_measures
		INNER JOIN 
		"DBMS_EMS_Schema"."New_Q8_NormalizeConsumptionsByLocationSquareMeters"		AS pivot_measures
		ON  cluster_measures.device_pk		= pivot_measures.device_pk
		AND cluster_measures.measure_timestamp <= pivot_measures.measure_timestamp
		AND cluster_measures.measure_timestamp  > pivot_measures.measure_timestamp  - interval '1 month'
				
		WINDOW 	w1 AS 	(PARTITION BY 	cluster_measures.device_pk, 
						date_part('hour', cluster_measures.measure_timestamp),
				   		pivot_measures.rank
				ORDER BY	cluster_measures.measure_timestamp DESC
				RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
	) AS rel 	

WHERE 	rel.rank = 1 
	AND date_part('hour', measure_timestamp_pivot) = date_part('hour', measure_timestamp_cluster) -- ALTEREI ISTO 

WINDOW 	w2 AS 	(PARTITION BY 	device_pk
		 ORDER BY	measure_timestamp_pivot DESC) -- ALTEREI ISTO 