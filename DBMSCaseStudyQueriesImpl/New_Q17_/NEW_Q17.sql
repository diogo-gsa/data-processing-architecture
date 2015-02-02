SELECT 	r2.device_pk, 
	max(r2.measure_timestamp) AS measure_timestamp,
	count(r2.measure) 	  AS count_measure_above_expected
	
FROM 	"DBMS_EMS_Schema"."New_Q14_DeltaBetweenCurrentConsumptionAndUDFBasedPrediction" r1
	INNER JOIN
	"DBMS_EMS_Schema"."New_Q14_DeltaBetweenCurrentConsumptionAndUDFBasedPrediction" r2
	 ON r1.device_pk = r2.device_pk
	AND r1.rank = 1 
	AND r2.measure_timestamp > (r1.measure_timestamp - '01:00:00'::interval) 

WHERE  r2.measure > r2.expected_measure
GROUP BY r2.device_pk, r2.expected_measure
HAVING 5 <= COUNT(r2.measure) AND COUNT(r2.measure) <= 10
