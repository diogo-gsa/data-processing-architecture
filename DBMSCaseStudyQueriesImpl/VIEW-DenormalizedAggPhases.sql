SELECT	dev.device_pk,
		dpr.measure_timestamp,
		sum(dpr.measure) 	 		AS measure,
		'WATT.HOUR' 		 		AS measure_unit,
		'EnergyConsumptionPh123'	AS measure_description,
		dpl.location 		 		AS device_location,
		dl.area_m2 		 			AS location_area_m2
	
FROM	"DBMS_EMS_Schema"."DataPointReading" 	 dpr,
		"DBMS_EMS_Schema"."DataPoint" 		 dp,
		"DBMS_EMS_Schema"."Device" 		 dev,
		"DBMS_EMS_Schema"."DeviceLocation" 	 dl,
		"DBMS_EMS_Schema"."DataPointDescription" dpd,
		"DBMS_EMS_Schema"."DataPointUnit" 	 dpu
	
WHERE 	dpr.datapoint_fk = dp.datapoint_pk 
    AND dp.device_fk = dev.device_pk 
    AND dev.device_location_fk = dl.device_location_pk 
    AND dp.datapoint_description_fk = dpd.datapoint_description_pk 
    AND dp.datapoint_unit_fk = dpu.datapoint_unit_pk
    AND (    dpd.description = 'Phase1_EnergyConsumption' 
	  OR dpd.description = 'Phase2_EnergyConsumption'
	  OR dpd.description = 'Phase3_EnergyConsumption')			
	  
GROUP BY dev.device_pk, dpr.measure_timestamp, dl.location, dl.area_m2