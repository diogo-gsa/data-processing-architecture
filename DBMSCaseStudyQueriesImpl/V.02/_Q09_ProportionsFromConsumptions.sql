SELECT	device_pk,
	measure_timestamp,
	measure/sum(measure) OVER wintotal * 100::double precision 				   AS measure,
	'Percentage%' 						   				   AS measure_unit,
	'%Proportion of each location power consumption by comparation with all other locations.'  AS measure_description,
	device_location
FROM 	"DBMS_EMS_Schema"."_Q08_SquareMeterNormalization"
WHERE 	device_pk <> 0 AND index = 1
WINDOW 	wintotal AS (PARTITION BY NULL::text)