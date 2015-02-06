SELECT 	r2.device_pk,  																			
	max(r2.measure_timestamp) 	AS measure_timestamp,  											
	count(r2.current_measure) 	AS measure,
	'Positive Integer'::text	AS measure_unit,
	'Counter of times that, in last hour, current consumption as exceeded the expected one. Being the counter limited by a min and max value.' AS measure_description,
	r2.device_location
FROM 	"DBMS_EMS_Schema"."_Q14_ExpectedConsumptionByUDF" r1  	
	INNER JOIN  																				
	"DBMS_EMS_Schema"."_Q14_ExpectedConsumptionByUDF" r2  	
	ON r1.device_pk = r2.device_pk  															
	AND r1.index = 1  																			
	AND r2.measure_timestamp > (r1.measure_timestamp - '01:00:00'::interval)  				 
WHERE  r2.current_measure > r2.expected_measure  															 
GROUP BY r2.device_pk, r2.expected_measure, r2.device_location 														
HAVING 5 <= COUNT(r2.current_measure) AND COUNT(r2.current_measure) <= 10