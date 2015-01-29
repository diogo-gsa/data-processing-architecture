SELECT  r1.device_pk,
	r1.measure_timestamp,
	min(r2.measure)/max(r2.measure) AS measure,
	min(r2.measure) AS min_measure,	
	max(r2.measure) AS max_measure,
	r1.measure_unit,
	'Min/Max Ratio over last 60 minutes'::text AS measure_description,
	r1.device_location

FROM   "DBMS_EMS_Schema"."New_Q8_NormalizeConsumptionsByLocationSquareMeters"   AS r1
	INNER JOIN
       "DBMS_EMS_Schema"."New_Q8_NormalizeConsumptionsByLocationSquareMeters"	AS r2  
	ON   r1.device_pk  = 0  -- All Building consumption only
	AND  r1.rank       = 1  -- All Building most recent measure 
	AND  r2.device_pk  = r1.device_pk
	AND  r2.measure_timestamp > r1.measure_timestamp - interval '60 minutes' -- 60min Time Window
	
GROUP BY r1.device_pk, 
	 r1.measure_timestamp, 
	 r1.measure_unit, 
	 r1.measure_description, 
	 r1.device_location

  