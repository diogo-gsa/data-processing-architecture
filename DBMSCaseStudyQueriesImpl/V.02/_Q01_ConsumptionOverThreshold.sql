SELECT  device_pk,
	measure_timestamp,
	measure,
	measure_unit,
	'Power consumption above a given threshold.'::text AS measure_description,
	device_location

FROM   "DBMS_EMS_Schema"."_Q08_SquareMeterNormalization"

WHERE   index = 1
  AND 	((device_pk = 0 AND measure >= 00)
      OR (device_pk = 1 AND measure >= 00)
      OR (device_pk = 2 AND measure >= 00)
      OR (device_pk = 3 AND measure >= 00)
      OR (device_pk = 4 AND measure >= 00)
      OR (device_pk = 5 AND measure >= 00)
      OR (device_pk = 6 AND measure >= 00)
      OR (device_pk = 7 AND measure >= 00)
      OR (device_pk = 8 AND measure >= 00))