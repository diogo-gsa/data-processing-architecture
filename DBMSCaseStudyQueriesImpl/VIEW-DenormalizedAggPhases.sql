SELECT 	dpr.measure_timestamp, 
	SUM(dpr.measure)	 AS measure, 
	'WATT.HOUR' 	 	 AS unit, 
	'EnergyConsumptionPh123' AS description, 
	dl.location, 
	dl.area_m2 
FROM 	"DBMS_EMS_Schema"."DataPointReading" 	 AS dpr,
	"DBMS_EMS_Schema"."DataPoint" 		 AS dp,
	"DBMS_EMS_Schema"."Device" 		 AS dev,
	"DBMS_EMS_Schema"."DeviceLocation" 	 AS dl,
	"DBMS_EMS_Schema"."DataPointDescription" AS dpd,
	"DBMS_EMS_Schema"."DataPointUnit" 	 AS dpu
WHERE	dpr.DataPoint_fk = dp.datapoint_pk 
    AND dp.device_fk = dev.device_pk
    AND dev.device_location_fk = dl.device_location_pk
    AND dp.datapoint_description_fk = dpd.datapoint_description_pk
    AND dp.datapoint_unit_fk = dpu.datapoint_unit_pk
GROUP BY dl.location, dpr.measure_timestamp, dl.area_m2;