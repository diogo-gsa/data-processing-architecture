SELECT 	device_pk,
	measure_timestamp,
	measure,
	measure_unit,
	measure_description,
	device_location,
	rank() OVER sortedwindow AS rank
FROM "DBMS_EMS_Schema"."New_Q8_NormalizeConsumptionsByLocationSquareMeters"
WHERE rank = 1
WINDOW sortedwindow AS (PARTITION BY NULL
			ORDER BY measure DESC);
