SELECT 	device_pk,
	measure_timestamp,
	(normalized_measure_avg_10min/SUM(normalized_measure_avg_10min) OVER winTotal)*100 AS measure,
	'percentage' AS measure_unit, 
	'%ofTotalNomaizedEnergyCconsumption' measure_description,
	device_location
FROM 	"DBMS_EMS_Schema"."Q7+8_NoWindowOp"
WINDOW 	winTotal AS (PARTITION BY null)




