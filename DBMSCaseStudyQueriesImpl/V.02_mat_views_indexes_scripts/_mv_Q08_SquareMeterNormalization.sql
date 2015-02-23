CREATE MATERIALIZED VIEW "DBMS_EMS_Schema"."_mv_Q08_SquareMeterNormalization" AS 

SELECT 	device_pk,
	measure_timestamp,
	measure / location_area_m2::double precision AS measure,
	'WATT/m^2'::text AS measure_unit,
	'Power Consumption Normalized by each location square meter area and all building as a whole.' AS measure_description,
	device_location,
	index
FROM 	"DBMS_EMS_Schema"."_mv_Q07_SmoothingConsumption"
UNION
SELECT 	rel.device_pk,
        rel.measure_timestamp,
        rel.measure,
        'WATT/m^2'::text AS measure_unit,
        'Power Consumption Normalized by each location square meter area and all building as a whole.' AS measure_description,
        rel.device_location,
        rank() OVER w AS rank
FROM 	(SELECT 0 AS device_pk,
		to_timestamp((((((((
			date_part('year'::text,   measure_timestamp)::text  || '-'::text) || 
			date_part('month'::text,  measure_timestamp)::text) || '-'::text) || 
			date_part('day'::text,    measure_timestamp)::text) || ' '::text) || 
			date_part('hour'::text,   measure_timestamp)::text) || ':'::text) || 
			date_part('minute'::text, measure_timestamp)::text, 'YYYY-MM-DD HH24:MI:SS'::text)::timestamp without time zone AS measure_timestamp,
		sum(measure) / sum(location_area_m2)::double precision AS measure,
		'WATT/m^2'::text AS measure_unit,
		'Energy consumption Normalized by energy meter location area'::text AS measure_description,
		'ALL_BUILDING'::text AS device_location
	FROM "DBMS_EMS_Schema"."_mv_Q07_SmoothingConsumption"
	GROUP BY date_part('year'::text, measure_timestamp), 
		 date_part('month'::text, measure_timestamp), 
		 date_part('day'::text, measure_timestamp), 
		 date_part('hour'::text, measure_timestamp), 
		 date_part('minute'::text, measure_timestamp)
	HAVING count(device_pk) = 8) rel

WINDOW w AS (ORDER BY rel.measure_timestamp DESC)

ALTER TABLE "DBMS_EMS_Schema"."_mv_Q08_SquareMeterNormalization"
  OWNER TO postgres;