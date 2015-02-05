 SELECT device_pk,
	measure_timestamp,
	measure / (sum(measure) OVER wintotal) * 100 AS measure,
	'percentage' AS measure_unit,
	'%ofTotalNormalizedEnergyConsumption' AS measure_description,
	device_location
FROM "DBMS_EMS_Schema"."New_Q8_NormalizeConsumptionsByLocationSquareMeters"
WHERE device_pk != 0 AND rank = 1
WINDOW wintotal AS (PARTITION BY NULL)


