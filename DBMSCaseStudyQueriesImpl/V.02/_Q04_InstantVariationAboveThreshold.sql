SELECT  device_pk,
	measure_timestamp,
	measure,
	current_power_consumption,
	'Percentage%' 												  AS measure_unit,
	'Variation between current and last 5 minutes average power consumption that exceeded a given threshold.' AS measure_description,
	device_location,
	location_area_m2

FROM "DBMS_EMS_Schema"."_Q11_InstantVariation"

WHERE index = 1
AND ((device_pk = 1 AND measure >= -100)
  OR (device_pk = 2 AND measure >= -100)
  OR (device_pk = 3 AND measure >= -100)
  OR (device_pk = 4 AND measure >= -100)
  OR (device_pk = 5 AND measure >= -100)
  OR (device_pk = 6 AND measure >= -100)
  OR (device_pk = 7 AND measure >= -100)
  OR (device_pk = 8 AND measure >= -100))