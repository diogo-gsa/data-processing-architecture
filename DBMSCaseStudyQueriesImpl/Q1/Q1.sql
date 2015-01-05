/*SELECT 	measure_timestamp, 
	building_normalized_measure,
	measure_unit, 
	measure_description, 
	covered_devices, covered_area_m2
FROM 	"DBMS_EMS_Schema"."Q8_NormalizationAllBuilding"
WHERE building_normalized_measure >= 0
ORDER BY measure_timestamp DESC 
LIMIT 1
*/
 --NEW ONE ---

(SELECT  null::bigint 			AS device_pk,
	 measure_timestamp,
	 building_normalized_measure 	AS measure,
	 measure_unit::text, 
	 measure_description::text,
	 'AllDevices'::varchar(100)	AS device_location
FROM 	 "DBMS_EMS_Schema"."Q8_NormalizationAllBuilding"
WHERE 	 building_normalized_measure >= 0
ORDER BY measure_timestamp DESC 
LIMIT 1)
UNION
(SELECT device_pk, 
	measure_timestamp, 
	normalized_measure_avg_10min AS measure, 
	measure_unit::text, 
	measure_description::text, 
	device_location
FROM 	"DBMS_EMS_Schema"."Q7+8_NoWindowOp"
WHERE 	(device_pk = 1 AND normalized_measure_avg_10min >= 00)
     OR (device_pk = 2 AND normalized_measure_avg_10min >= 00)
     OR	(device_pk = 3 AND normalized_measure_avg_10min >= 00)
     OR	(device_pk = 4 AND normalized_measure_avg_10min >= 00)
     OR	(device_pk = 5 AND normalized_measure_avg_10min >= 00)
     OR	(device_pk = 6 AND normalized_measure_avg_10min >= 00)
     OR	(device_pk = 7 AND normalized_measure_avg_10min >= 00)
     OR	(device_pk = 8 AND normalized_measure_avg_10min >= 00)
)	
