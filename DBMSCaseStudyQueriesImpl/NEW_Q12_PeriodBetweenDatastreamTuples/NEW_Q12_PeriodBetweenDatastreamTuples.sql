--NEW_Q12_PeriodBetweenDatastreamTuples
SELECT 	rela.device_pk,
	rela.device_location,
	rela.measure_timestamp 				AS measure_timestamp_last,
	relb.measure_timestamp 				AS measure_timestamp_2nd_last,
	rela.measure_timestamp - relb.measure_timestamp AS delta,
	rela.rank
FROM 	"DBMS_EMS_Schema"."New_Q0_JoinDatastreamWithStaticData" rela
	INNER JOIN
	"DBMS_EMS_Schema"."New_Q0_JoinDatastreamWithStaticData"relb 
	ON  rela.device_pk = relb.device_pk 
	AND (rela.rank + 1) = relb.rank
 