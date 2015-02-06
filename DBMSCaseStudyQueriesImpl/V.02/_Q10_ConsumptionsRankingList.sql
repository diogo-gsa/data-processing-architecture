SELECT 	device_pk,
    	measure_timestamp,
	rank() OVER sortedwindow AS measrure,
	measure AS current_power_consumption,
	'Ranking List Position' 						AS measure_unit,
	'Descendig Ranking List of each Location by its power consumption.' 	AS measure_description,
	device_location

FROM "DBMS_EMS_Schema"."_Q08_SquareMeterNormalization"

WHERE index = 1

WINDOW sortedwindow AS (PARTITION BY NULL::text 
			ORDER BY measure DESC)
