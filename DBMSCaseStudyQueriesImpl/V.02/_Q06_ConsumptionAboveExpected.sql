SELECT	device_pk,
	measure_timestamp,
	(current_measure/(expecetd_measure+0.0001) - 1)*100                                 	  AS measure,                                                               
	'Percentage%'::text                                                                       AS measure_unit,
	'Delta between current and expecetd power consumption exceeded a given threshold.'::text  AS measure_description,
	device_location,
	current_measure 									  AS current_power_consumption,                                                             
	expecetd_measure 									  AS expected_power_consumption	
FROM   "DBMS_EMS_Schema"."_Q13_ExpectedConsumptionByMonthlyHourAvg"
WHERE  index = 1 AND (current_measure/(expecetd_measure+0.0001) - 1)*00 >= 00
/*IMPORTANT: (current_measure/(expecetd_measure+0.0001) - 1)*0 >= 0 Universal Condition/Worst Case*/