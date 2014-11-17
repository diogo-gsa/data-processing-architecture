package msc_thesis.diogo_anjos.DBMS_Version;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import msc_thesis.diogo_anjos.DBMS_Version.exceptions.ThereIsNoDataPoint_PKwithThisLocaionException;
import msc_thesis.diogo_anjos.simulator.EnergyMeasureTupleDTO;
import msc_thesis.diogo_anjos.simulator.EnergyMeter;
import msc_thesis.diogo_anjos.util.DButil;
import msc_thesis.diogo_anjos.util.DataPoint_PK;

public class DB_CRUD_Query_API {

	private final String className = "BD_CRUD_Query_API"; //debug purposes
	private final Connection database = DButil.connectToDB("localhost", "5432", "lumina_db", "postgres", "root", className);
		
	/*
	 *  INSERT a the given record into DBMS_EMS_Schema.DataPointReading
	 */
	public void insertInto_DatapointReadingTable(EnergyMeasureTupleDTO reading){	
		String queryStatement = "";		
		String measure_ts = reading.getMeasureTS();
		
		DataPoint_PK dpPK = null;
		try{
			dpPK = DataPoint_PK.getDataPoint_PKByLocation(reading.getMeterLocation());
		}catch(ThereIsNoDataPoint_PKwithThisLocaionException e){
			e.printStackTrace();
			System.exit(1); //non-zero status program = program terminate with errors 
		}
	
		Map<Integer,Double> datapointPKToConsumptionValueMap = new TreeMap<Integer,Double>();
		datapointPKToConsumptionValueMap.put(dpPK.getPh1_PK(), reading.getPh1Consumption());
		datapointPKToConsumptionValueMap.put(dpPK.getPh2_PK(), reading.getPh2Consumption());
		datapointPKToConsumptionValueMap.put(dpPK.getPh3_PK(), reading.getPh3Consumption());
		
		
	   try {
		   for(Integer pk : datapointPKToConsumptionValueMap.keySet()){
			   queryStatement =  "INSERT INTO \"DBMS_EMS_Schema\".\"DataPointReading\"(measure_timestamp, measure, datapoint_fk)"
					   			+ "VALUES ('"+measure_ts+"',"+datapointPKToConsumptionValueMap.get(pk)+","+pk+")";
			   DButil.executeUpdate(queryStatement, database);
		   }
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	/*
	 *  INSERT a the the specified specified batch of readings into DBMS_EMS_Schema.DataPointReading
	 */
	public void insertInto_DatapointReadingTable_BatchMode(String initialMeasure_ts, String finalMeasure_ts, EnergyMeter meterDBtable){
		String queryStatement = "SELECT * " + 
								"FROM " + meterDBtable.getDatabaseTable() + 
								" WHERE measure_timestamp >= '"+initialMeasure_ts+"' AND " +
								" measure_timestamp <= '"+finalMeasure_ts+"'"; 
		
		ResultSet batchResult = null;
		try{
			batchResult = DButil.executeQuery(queryStatement, database);
		}catch(SQLException e){
			e.printStackTrace();
		}	
		
		List<EnergyMeasureTupleDTO> listDTOs = buildDtoListFromResultSet(batchResult);
		for(EnergyMeasureTupleDTO dto : listDTOs){
			this.insertInto_DatapointReadingTable(dto);
		}
  }
	
	
	/*
	 *  TRUNCATE ALL records from table DBMS_EMS_Schema.DataPointReading
	 */
	public void truncateAll_DatapointReadingTable() {
		String queryStatement = "TRUNCATE TABLE \"DBMS_EMS_Schema\".\"DataPointReading\"";
		try{
			DButil.executeUpdate(queryStatement, database);
		}catch(SQLException e){
			e.printStackTrace();
		}
	}

	/*
	 *  DELETE from DBMS_EMS_Schema.DataPointReading the (unique) record 
	 *  that match the measure_timestamp AND the datapoint_pk  
	 */
	public void deleteSpecificRow_DatapointReadingTable(String measure_ts, int datapoint_pk) {
		String queryStatement =	"DELETE FROM \"DBMS_EMS_Schema\".\"DataPointReading\""+
								" WHERE measure_timestamp = '"+measure_ts+"' AND datapoint_fk ="+datapoint_pk+";";
		try{
			DButil.executeUpdate(queryStatement, database);
		}catch(SQLException e){
			e.printStackTrace();
		}
	}

	
	/*
	 *  DELETE from DBMS_EMS_Schema.DataPointReading the records 
	 *  that match the initialMeasure_ts <= measure_timestamp <= finalMeasure_ts 
	 *  AND the datapoint_pk  
	 */
	public void deleteSpecificInterval_DatapointReadingTable(String initialMeasure_ts, String finalMeasure_ts, int datapoint_pk) {
		String queryStatement =	"DELETE FROM \"DBMS_EMS_Schema\".\"DataPointReading\""+
								" WHERE measure_timestamp >= '"+initialMeasure_ts+"' AND " +
									  " measure_timestamp <= '"+finalMeasure_ts+"' AND " +
									  " datapoint_fk ="+datapoint_pk+";";
		try{
			DButil.executeUpdate(queryStatement, database);
		}catch(SQLException e){
			e.printStackTrace();
		}
	}
	
	/*
	 *  DELETE from DBMS_EMS_Schema.DataPointReading the records 
	 *  that match the initialMeasure_ts <= measure_timestamp <= finalMeasure_ts  
	 */
	public void deleteSpecificInterval_DatapointReadingTable(String initialMeasure_ts, String finalMeasure_ts) {
		String queryStatement =	"DELETE FROM \"DBMS_EMS_Schema\".\"DataPointReading\""+
								" WHERE measure_timestamp >= '"+initialMeasure_ts+"' AND " +
									  " measure_timestamp <= '"+finalMeasure_ts+"';";
		try{
			DButil.executeUpdate(queryStatement, database);
		}catch(SQLException e){
			e.printStackTrace();
		}	
	}
	
// 	==========================================================================================
//								Case Study Queries Implementation 
//	==========================================================================================
	public QueryEvaluationReport executeEvaluationQuery_Q11_NoWindows_10min(){
		String queryStatement =	  "SELECT * "
								+ "FROM \"DBMS_EMS_Schema\".\"Q11_NO_Win_10min\"";
		return executeEvaluationQuery(queryStatement);	
	}
	
	public QueryEvaluationReport executeEvaluationQuery_Q11_NoWindows_60min(){
		String queryStatement =	  "SELECT * "
								+ "FROM \"DBMS_EMS_Schema\".\"Q11_NO_Win_60min\"";
		return executeEvaluationQuery(queryStatement);	
	}
	
	
	public QueryEvaluationReport executeEvaluationQuery_Q11_SizeWindows_10min(){
		String queryStatement =	  "SELECT * "
								+ "FROM \"DBMS_EMS_Schema\".\"Q11_Size_Win_10min\"";
		return executeEvaluationQuery(queryStatement);
	}
	
	public QueryEvaluationReport executeEvaluationQuery_Q11_SizeWindows_60min(){
		String queryStatement =	  "SELECT * "
								+ "FROM \"DBMS_EMS_Schema\".\"Q11_Size_Win_60min\"";
		return executeEvaluationQuery(queryStatement);
	}
	
	
	public QueryEvaluationReport executeEvaluationQuery_Q11_TimeWindows_10min(){
		String queryStatement =	  "SELECT * "
								+ "FROM \"DBMS_EMS_Schema\".\"Q11_Time_Win_10min\"";
		return executeEvaluationQuery(queryStatement);
	}
	
	public QueryEvaluationReport executeEvaluationQuery_Q11_TimeWindows_60min(){
		String queryStatement =	  "SELECT * "
								+ "FROM \"DBMS_EMS_Schema\".\"Q11_Time_Win_60min\"";
		return executeEvaluationQuery(queryStatement);
	}
	
	
	public QueryEvaluationReport executeEvaluationQuery_Q4_NoWindows_10min(){
		String queryStatement =	  "SELECT * "
								+ "FROM \"DBMS_EMS_Schema\".\"Q11_NO_Win_10min\""
//										"DBMS_EMS_Schema"."Q11_Size_Win_10min" 		//Available Windows (views) to "feed" this Query
//										"DBMS_EMS_Schema"."Q11_Time_Win_10min"
								+ "WHERE 	variation_10min_win > 0.05";
		return executeEvaluationQuery(queryStatement);	
	}
	
	public QueryEvaluationReport executeIntegrationQuery_Q8_7_10minAVG_NoWindowOperator(){
		String queryStatement =	"SELECT all_measures.device_pk, " 																		+
										"last_measure.ts, "																				+
										"avg(all_measures.measure) AS  measure_avg_10min, "												+
										"all_measures.measure_unit::text, "																+
										"all_measures.measure_description::text, "														+
										"all_measures.device_location, "																+
										"all_measures.location_area_m2, "																+
										"avg(all_measures.measure)/all_measures.location_area_m2 AS normalized_measure_avg_10min " 		+
					
								"FROM 	\"DBMS_EMS_Schema\".\"DenormalizedAggPhases\"	AS all_measures, "								+
										"(SELECT device_pk, "																			+
												"MAX(measure_timestamp) 	AS ts "														+
										"FROM \"DBMS_EMS_Schema\".\"DenormalizedAggPhases\" "											+
										"GROUP BY device_pk) 				AS last_measure " 											+
				
								"WHERE	 all_measures.device_pk = last_measure.device_pk "												+
									"AND all_measures.measure_timestamp > last_measure.ts - interval '10 minutes' " 					+
								
								"GROUP BY 	all_measures.device_pk, "																	+
											"last_measure.ts, "																			+
											"all_measures.measure_unit::text, "															+
											"all_measures.measure_description::text, "													+
											"all_measures.device_location, "															+
											"all_measures.location_area_m2" ;
		
		System.out.println(queryStatement);
		return executeEvaluationQuery(queryStatement);
	}
	

	public QueryEvaluationReport executeIntegrationQuery_Q8_7_10minAVG_WithWindowOperator(){
		String queryStatement =	"SELECT device_pk, "																						+
										"ts, " 																								+
										"measure_avg_10min, " 																				+
										"measure_unit, "																					+
										"measure_description, " 																			+
										"device_location, "																					+
										"location_area_m2, "																				+
										"measure_avg_10min/location_area_m2 AS normalized_measure_avg_10min "								+
										
								"FROM(	SELECT 	all_measures.device_pk, "																	+
												"last_measure.ts, "																			+
												"all_measures.measure_unit::text, "															+
												"all_measures.measure_description::text, "													+
												"all_measures.device_location, "															+
												"all_measures.location_area_m2, "															+
												"avg(all_measures.measure)	OVER w AS  measure_avg_10min, "									+
												"rank() 						OVER w "													+

												"FROM 	\"DBMS_EMS_Schema\".\"DenormalizedAggPhases\"	AS all_measures, "					+
														"(SELECT device_pk, "																+
																"MAX(measure_timestamp) 	AS ts "											+
														"FROM \"DBMS_EMS_Schema\".\"DenormalizedAggPhases\" "								+
														"GROUP BY device_pk) 				AS last_measure "								+
				
												"WHERE	all_measures.device_pk = last_measure.device_pk "									+
													"AND	all_measures.measure_timestamp > last_measure.ts - interval '10 minutes' "		+
			    
												"WINDOW w AS "																				+
													"(PARTITION BY all_measures.device_pk "													+
													"ORDER BY all_measures.measure_timestamp DESC "											+
													"RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) "									+
									") AS rel "																								+
							"WHERE rank = 1	";		
		System.out.println(queryStatement);
		return executeEvaluationQuery(queryStatement);
	}
	
	
	
	
//	==========================================================================================
//						End Of Case Study Queries Implementation Zone 
//	==========================================================================================

	
	
	private QueryEvaluationReport executeEvaluationQuery(String queryStatement){
		ResultSet queryExecutionResultSet = null;
		double queryExecutionTime = 0;
		try{
			long initTS = System.nanoTime();
			queryExecutionResultSet = DButil.executeQuery(queryStatement, database);
			// 1 nanoSecond / (10^6) = 1 milliSecond
	    	// measure with nano resolution, but present the result in milliseconds 
			queryExecutionTime = (double) (System.nanoTime() - initTS)/1000000;
		}catch(SQLException e){
			e.printStackTrace();
		}
		return new QueryEvaluationReport(queryStatement, queryExecutionResultSet, queryExecutionTime);
	}
	
	
	
	private List<EnergyMeasureTupleDTO> buildDtoListFromResultSet(ResultSet rs) {
		
		List<EnergyMeasureTupleDTO> resListofDTOs = new ArrayList<EnergyMeasureTupleDTO>();
		EnergyMeasureTupleDTO auxDTO = null;
		try {
			while(rs.next()) { 
				String measure_ts = rs.getString(1);
				String location = rs.getString(2);
				auxDTO = new EnergyMeasureTupleDTO(measure_ts, location);
				auxDTO.setPh1Ampere(rs.getString(3));
				auxDTO.setPh1PowerFactor(rs.getString(4));
				auxDTO.setPh1Volt(rs.getString(5));
				auxDTO.setPh2Ampere(rs.getString(6));
				auxDTO.setPh2PowerFactor(rs.getString(7));
				auxDTO.setPh2Volt(rs.getString(8));
				auxDTO.setPh3Ampere(rs.getString(9));
				auxDTO.setPh3PowerFactor(rs.getString(10));
				auxDTO.setPh3Volt(rs.getString(11));
				resListofDTOs.add(auxDTO);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return resListofDTOs;
	}

	
	
	
}
