-- Esta query pode/deve ser dfinida como uma VIEW (views são stored queries: Views são Queries) 
-- Queries da Data Integraton component podem ser todas definidas como views

DROP VIEW IF EXISTS library_aggegrated_phases;
CREATE VIEW library_aggegrated_phases AS
SELECT measure_timestamp, SUM(measure) AS measure
FROM 	"DBMS_EMS_Schema"."DataPointReading" 	 AS dpr,
	"DBMS_EMS_Schema"."DataPoint" 		 AS dp,
	"DBMS_EMS_Schema"."Device" 		 AS dev,
	"DBMS_EMS_Schema"."DeviceLocation" 	 AS dl,
	"DBMS_EMS_Schema"."DataPointDescription" AS dpd
WHERE	dpr.DataPoint_fk = dp.datapoint_pk 
    AND dp.device_fk = dev.device_pk
    AND dev.device_location_fk = dl.device_location_pk
    AND dl.location = 'LIBRARY'
    AND dp.datapoint_description_fk = dpd.datapoint_description_pk
    --AND dpr.measure_timestamp >=  timestamp '2014-03-17 11:59:05' - interval '10 minutes'  --time window
GROUP BY dpr.measure_timestamp;

--ORDER BY measure_timestamp DESC; /* Ease of Debug */
 
    
    
	

	
	
	
