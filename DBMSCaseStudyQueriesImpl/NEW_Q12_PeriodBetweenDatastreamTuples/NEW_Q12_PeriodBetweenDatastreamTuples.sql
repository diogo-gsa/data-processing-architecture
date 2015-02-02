--NEW_Q12_PeriodBetweenDatastreamTuples
SELECT 	rela.device_pk,
	rela.device_location,
	rela.measure_timestamp 				AS measure_timestamp_last,
	relb.measure_timestamp 				AS measure_timestamp_2nd_last,
	rela.measure_timestamp - relb.measure_timestamp AS delta,
	rela.rank
   FROM ( SELECT device_pk,
		 measure_timestamp,
		 measure,
		 measure_unit,
		 measure_description,
		 device_location,
		 location_area_m2,
		 rank() OVER (PARTITION BY device_pk 
			      ORDER BY 	measure_timestamp DESC) AS rank
           FROM "DBMS_EMS_Schema"."New_Q0_JoinDatastreamWithStaticData") rela
   JOIN ( SELECT device_pk,
		 measure_timestamp,
		 measure,
		 measure_unit,
		 measure_description,
		 device_location,
		 location_area_m2,
		 rank() OVER (PARTITION BY device_pk 
			      ORDER BY measure_timestamp DESC) AS rank
           FROM "DBMS_EMS_Schema"."New_Q0_JoinDatastreamWithStaticData") relb 
    ON rela.device_pk = relb.device_pk 
    AND (rela.rank + 1) = relb.rank
 