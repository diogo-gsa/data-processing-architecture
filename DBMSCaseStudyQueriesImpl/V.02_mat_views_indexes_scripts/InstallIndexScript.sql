--List Installed Indexs
select	t.relname as table_name,
		i.relname as index_name,
		a.attname as column_name
from	pg_class t,
		pg_class i,
		pg_index ix,
		pg_attribute a
where	t.oid = ix.indrelid
    and i.oid = ix.indexrelid
    and a.attrelid = t.oid
    and a.attnum = ANY(ix.indkey)
    and t.relname = '_mv_Q00_DataAggregation'
order by t.relname,
	 	 i.relname;

----------------------------------------------------------------------------------------------
-- 	INDEX das DPR Table
---------------------------------------------------------------------------------------------- 

-- Index: "DBMS_EMS_Schema"."NotClusteredIndex_ON_DataPoint_AND_TS"
-- DROP INDEX "DBMS_EMS_Schema"."NotClusteredIndex_ON_DataPoint_AND_TS";
CREATE INDEX "NotClusteredIndex_ON_DataPoint_AND_TS"
  ON "DBMS_EMS_Schema"."DataPointReading" USING btree (datapoint_fk, measure_timestamp DESC);


-- Index: "DBMS_EMS_Schema"."NotClusteredIndex_ON_TS_AND_DataPoint"
-- DROP INDEX "DBMS_EMS_Schema"."NotClusteredIndex_ON_TS_AND_DataPoint";
CREATE INDEX "NotClusteredIndex_ON_TS_AND_DataPoint"
  ON "DBMS_EMS_Schema"."DataPointReading" USING btree (measure_timestamp DESC, datapoint_fk);


-- Index: "DBMS_EMS_Schema"."NotClusteredIndex_ON_DataPoint"
-- DROP INDEX "DBMS_EMS_Schema"."NotClusteredIndex_ON_DataPoint";
CREATE INDEX "NotClusteredIndex_ON_DataPoint"                       -- Este é o que está em produção/instalado
  ON "DBMS_EMS_Schema"."DataPointReading" USING btree (datapoint_fk);


-- Index: "DBMS_EMS_Schema"."NotClusteredIndex_ON_measure_timestamp"
-- DROP INDEX "DBMS_EMS_Schema"."NotClusteredIndex_ON_measure_timestamp";
CREATE INDEX "NotClusteredIndex_ON_measure_timestamp"
  ON "DBMS_EMS_Schema"."DataPointReading" USING btree (measure_timestamp DESC);

--- COmando para fazer clustering manualmente
CLUSTER VERBOSE "DBMS_EMS_Schema"."DataPointReading" USING "NotClusteredIndex_ON_DataPoint_AND_TS"

----------------------------------------------------------------------------------------------
-- 	INDEX da Q0
---------------------------------------------------------------------------------------------- 

-- DROP INDEX "DBMS_EMS_Schema"."NotClusteredIndex_ON_Device";
CREATE INDEX "Q0_NotClusteredIndex_ON_Device"
  ON "DBMS_EMS_Schema"."_mv_Q00_DataAggregation" USING btree (device_pk);

--DROP INDEX "DBMS_EMS_Schema"."Q0_NotClusteredIndex_ON_TS";
CREATE INDEX "Q0_NotClusteredIndex_ON_TS"
  ON "DBMS_EMS_Schema"."_mv_Q00_DataAggregation" USING btree (measure_timestamp DESC);

--DROP INDEX "DBMS_EMS_Schema"."Q0_NotClusteredIndex_ON_Device_AND_TS"; Este é o que está em produção/instalado
CREATE INDEX "Q0_NotClusteredIndex_ON_Device_AND_TS"
  ON "DBMS_EMS_Schema"."_mv_Q00_DataAggregation" USING btree (device_pk, measure_timestamp DESC);

--DROP INDEX "DBMS_EMS_Schema"."Q0_NotClusteredIndex_ON_TS_AND_Device";
CREATE INDEX "Q0_NotClusteredIndex_ON_TS_AND_Device"
  ON "DBMS_EMS_Schema"."_mv_Q00_DataAggregation" USING btree (measure_timestamp DESC, device_pk);

-- INDEX Q11
--DROP INDEX "DBMS_EMS_Schema"."Q11_NotClusteredIndex_ON_Device";
CREATE INDEX "Q11_NotClusteredIndex_ON_Index"
  ON "DBMS_EMS_Schema"."_mv_Q11_InstantVariation" USING btree (index DESC);

-- INDEX Q12
CREATE INDEX "Q12_NotClusteredIndex_ON_Index"
  ON "DBMS_EMS_Schema"."_mv_Q12_DataStreamPeriodicity" USING btree (index ASC);

-- INDEX Q07
CREATE INDEX "Q07_NotClusteredIndex_ON_Device_AND_TS"
  ON "DBMS_EMS_Schema"."_mv_Q07_SmoothingConsumption" USING btree (device_pk, measure_timestamp DESC);

-- INDEX Q08
CREATE INDEX "Q08_NotClusteredIndex_ON_Device_AND_TS"
  ON "DBMS_EMS_Schema"."_mv_Q08_SquareMeterNormalization" USING btree (device_pk, measure_timestamp DESC);

-- INDEX Q14
CREATE INDEX "Q14_NotClusteredIndex_ON_Device_AND_TS"
  ON "DBMS_EMS_Schema"."_mv_Q14_ExpectedConsumptionByUDF" USING btree (device_pk, measure_timestamp DESC);

-- INDEX Q13
CREATE INDEX "Q13_NotClusteredIndex_ON_Index"
  ON "DBMS_EMS_Schema"."_mv_Q13_ExpectedConsumptionByMonthlyHourAvg" USING btree (index ASC);