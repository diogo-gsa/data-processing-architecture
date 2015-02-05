SELECT 	device_pk, 
	device_location, 
	measure_timestamp, 
	variation, 
	current_measure, 
	win_measure, 
	rank
FROM "DBMS_EMS_Schema"."New_Q11_ConsumptionsVariationOverLast5min"
WHERE rank = 1
  AND ((device_pk = 1 AND variation >= -1000)  									
    OR (device_pk = 2 AND variation >= -1000)  									
    OR (device_pk = 3 AND variation >= -1000)  									
    OR (device_pk = 4 AND variation >= -1000)  									
    OR (device_pk = 5 AND variation >= -1000)  									
    OR (device_pk = 6 AND variation >= -1000)  									
    OR (device_pk = 7 AND variation >= -1000)  									
    OR (device_pk = 8 AND variation >= -1000))