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
		String queryStatement =	"SELECT * " 										+					
								"FROM 	\"DBMS_EMS_Schema\".\"Q7+8_NoWindowOp\" ";
		return executeEvaluationQuery(queryStatement);
	}
	

	public QueryEvaluationReport executeIntegrationQuery_Q8_7_10minAVG_WindowOperator(){
		String queryStatement =	"SELECT * " 										+					
								"FROM 	\"DBMS_EMS_Schema\".\"Q7+8_WindowOp\" ";
		return executeEvaluationQuery(queryStatement);
	}
	
	public QueryEvaluationReport executeIntegrationQuery_Q9_Percentage(){
		String queryStatement =	"SELECT * " 										+					
								"FROM 	\"DBMS_EMS_Schema\".\"Q9_Percentage\" ";	
		return executeEvaluationQuery(queryStatement);
	}
	
	public QueryEvaluationReport executeIntegrationQuery_Q10_SortedMeasures(){
		String queryStatement =	"SELECT * " 										+					
								"FROM 	\"DBMS_EMS_Schema\".\"Q10_SortedMeasures\" ";	
		return executeEvaluationQuery(queryStatement);	
	}

	public QueryEvaluationReport executeIntegrationQuery_Q12_DeltaBetweenTuples(){
		String queryStatement =	"SELECT * " 										+	
								"FROM \"DBMS_EMS_Schema\".\"Q12_DeltaBetweenTuples\"";
		return executeEvaluationQuery(queryStatement);	
	}
	
	public QueryEvaluationReport executeEvaluationQuery_Q5_DeltaBetweenTuplesOverThreashold(){
		String queryStatement =	"SELECT 	* "											+
								"FROM \"DBMS_EMS_Schema\".\"Q12_DeltaBetweenTuples\" "	+
								"WHERE  delta < '00:00:50' "							+ 
								   "OR  delta > '00:01:10' "; 
		//catch periods between measures out of [50,70] seconds range
		return executeEvaluationQuery(queryStatement);	
	}
	
	public QueryEvaluationReport executeEvaluationQuery_Q3_MinMaxRatio(){
		String queryStatement =	"SELECT  MAX(measure_timestamp) 		  	AS measure_timestamp, "     	  +
										"MIN(building_normalized_measure) "									  +
										"/MAX(building_normalized_measure) 	AS min_max_measure_ratio, "		  +
										"MAX(building_normalized_measure)  	AS max_measure, "				  +
										"MIN(building_normalized_measure) 	AS min_measure, "				  +
										"AVG(covered_devices)	  	  		AS covered_devices, "			  +
										"AVG(covered_area_m2)	          	AS covered_area_m2 "			  +
								"FROM 	\"DBMS_EMS_Schema\".\"Q8_NormalizationAllBuilding\" 	      AS r1, "+
										"(SELECT MAX(measure_timestamp) AS max_ts "							  +
										 "FROM 	\"DBMS_EMS_Schema\".\"Q8_NormalizationAllBuilding\") AS r2 " +
								"WHERE 	r1.measure_timestamp >= r2.max_ts - interval '60 minutes'";
		return executeEvaluationQuery(queryStatement);	
	}
	
	public QueryEvaluationReport executeEvaluationQuery_Q1_BuildingNormalizedConsumptionOverThreshold(){
		// Note: measure >= 0 means worst case scenario simulation. 
		// That is, query always produces an output.
		String queryStatement =	"(SELECT null::bigint 					AS device_pk, "				+
				 						"measure_timestamp,	"										+
				 						"building_normalized_measure 	AS measure, "				+
				 						"measure_unit::text, "										+
				 						"measure_description::text, "								+
				 						"'AllDevices'::varchar(100)	AS device_location "			+
				 				 "FROM 	 \"DBMS_EMS_Schema\".\"Q8_NormalizationAllBuilding\" "		+
				 				 "WHERE  building_normalized_measure >= 0 "						+
				 				 "ORDER BY measure_timestamp DESC "									+
				 				"LIMIT 1) "															+
				 				"UNION "															+
				 				"(SELECT device_pk, "												+ 
				 						"measure_timestamp, " 										+
				 						"normalized_measure_avg_10min AS measure, " 				+
				 						"measure_unit::text, "										+
				 						"measure_description::text, " 								+
				 						"device_location "											+
				 				"FROM 	\"DBMS_EMS_Schema\".\"Q7+8_NoWindowOp\" "					+
				 				"WHERE 	 (device_pk = 1 AND normalized_measure_avg_10min >= 00) "	+
				 					 "OR (device_pk = 2 AND normalized_measure_avg_10min >= 00) "	+
			                         "OR (device_pk = 3 AND normalized_measure_avg_10min >= 00) "	+
			                         "OR (device_pk = 4 AND normalized_measure_avg_10min >= 00) "	+
			                         "OR (device_pk = 5 AND normalized_measure_avg_10min >= 00) "	+
			                         "OR (device_pk = 6 AND normalized_measure_avg_10min >= 00) "	+
			                         "OR (device_pk = 7 AND normalized_measure_avg_10min >= 00) "	+
			                         "OR (device_pk = 8 AND normalized_measure_avg_10min >= 00) "	+
				 				")";
		return executeEvaluationQuery(queryStatement);	
	}
	
	public QueryEvaluationReport executeEvaluationQuery_Q16_MeasuresPercentHigherThanAverageThresold(){
		String queryStatement =		"SELECT  device_pk, "																								+
											"measure_timestamp, "																						+
											"current_measure 				AS measure, "																+
											"measure_sliding24h_avg*1.25	AS measure_threshold, "														+
											"device_location, "																							+
											"measure_unit, "																							+
											"'Measures 25% higher than the past 24h average' AS measure_description "									+
									"FROM	(SELECT all_measures.device_pk, "																			+
													"all_measures.measure_timestamp, " 																	+
													"all_measures.measure_avg_10min	  AS current_measure, "												+
													"all_measures.measure_unit, " 																		+
													"all_measures.measure_description, " 																+
													"all_measures.device_location, " 																	+
													"all_measures.location_area_m2, "																	+
													"avg(measure_avg_10min)  over w 	  AS measure_sliding24h_avg, "									+
													"rank() 				 over w " 																	+
											"FROM 	\"DBMS_EMS_Schema\".\"Q7_10minAVG\"   AS all_measures "												+
													"INNER JOIN (SELECT device_pk, "																	+
																		"max(measure_timestamp) AS current_ts "											+
																 "FROM 	\"DBMS_EMS_Schema\".\"Q7_10minAVG\" "											+
																 "GROUP BY device_pk "																	+
																") AS most_recent_measure "																+
													"ON  most_recent_measure.device_pk = all_measures.device_pk "										+
													"AND all_measures.measure_timestamp >= most_recent_measure.current_ts  - interval '24 hours' "		+
											"WINDOW w AS (PARTITION BY	 all_measures.device_pk "															+
														 "ORDER BY all_measures.measure_timestamp DESC "												+
														 "RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) "											+
											") AS rel "																									+
									"WHERE current_measure > measure_sliding24h_avg*1.25 AND rank = 1 ";
		return executeEvaluationQuery(queryStatement);	
	}
	public QueryEvaluationReport executeIntegrationQuery_Q14_CurrentAndExpectedUDFMeasure(){
		String queryStatement =	"SELECT * " +
								"FROM \"DBMS_EMS_Schema\".\"Q14_CurrentAndExpectedUDFMeasure\"";
		return executeEvaluationQuery(queryStatement);	
	}
	public QueryEvaluationReport executeIntegrationQuery_Q13_CurrentAndExpectedHourClusterMeasure(){
		String queryStatement =	"SELECT * " +
								"FROM \"DBMS_EMS_Schema\".\"Q13_CurrentAndExpectedHourClusterMeasure\"";
		return executeEvaluationQuery(queryStatement);	
	}
	
	public QueryEvaluationReport executeEvaluationQuery_Q6_withQ13AsInput_CurrentAndExpectedConsumptionAboveGivenPercentage(){
		
		String queryStatement =	"SELECT	device_pk, " 																								+
										"measure_timestamp, " 																						+
										"(measure/(cluster_expected_measure+0.0001) - 1)*100 							AS measure, "				+
										"measure																		AS current_cosnumption, "	+
										"cluster_expected_measure			    										AS expected_consumption, "	+
										"'%percent' 																	AS measure_unit, "			+
										"'Percent variation between current and expected consumption greater than 10%' 	AS measure_description, " 	+
										"device_location "																							+
								"FROM 	\"DBMS_EMS_Schema\".\"Q13_CurrentAndExpectedHourClusterMeasure\" "											+
								"WHERE 	(measure/(cluster_expected_measure+0.0001) - 1)*100 > 10 ";	
								// +0.0001 to avoid division-by-zero when expected_measure = 0
		
		return executeEvaluationQuery(queryStatement);	
	}
	
	public QueryEvaluationReport executeEvaluationQuery_Q6_withQ14AsInput_CurrentAndExpectedConsumptionAboveGivenPercentage(){
		
		String queryStatement =	"SELECT	device_pk, " 																								+
										"measure_timestamp," 																						+
										"(measure/(expected_measure+0.0001) - 1)*100 									AS measure, "				+
										"measure					 													AS current_consumption, "	+
										"expected_measure+0.0001 														AS expected_consumption, "	+
										"'%percent' 																	AS measure_unit, "			+
										"'Percent variation between current and expected consumption greater than 10%' 	AS measure_description, " 	+
										"device_location "																							+
								"FROM 	\"DBMS_EMS_Schema\".\"Q14_CurrentAndExpectedUDFMeasure\" "													+
								"WHERE 	(measure/(expected_measure+0.0001) - 1)*100 > 10 ";
								// +0.0001 to avoid division-by-zero when expected_measure = 0
		
		return executeEvaluationQuery(queryStatement);	
	}
	
	
	public QueryEvaluationReport executeIntegrationQuery_New_Q9_FractionateConsumptions(){
		String queryStatement =	"SELECT * " 														+					
								"FROM 	\"DBMS_EMS_Schema\".\"New_Q9_FractionateConsumptions\" ";			
		return executeEvaluationQuery(queryStatement);	
	}
	
	public QueryEvaluationReport executeIntegrationQuery_New_Q10_OrderByConsumptions(){
		String queryStatement =	"SELECT * " 														+					
								"FROM 	\"DBMS_EMS_Schema\".\"New_Q10_OrderByConsumptions\" ";			
		return executeEvaluationQuery(queryStatement);	
	}
	
	public QueryEvaluationReport executeIntegrationQuery_New_Q14_DeltaBetweenCurrentConsumptionAndUDFBasedPrediction(){
		String queryStatement =	"SELECT * " +
								"FROM \"DBMS_EMS_Schema\".\"New_Q14_DeltaBetweenCurrentConsumptionAndUDFBasedPrediction\"";
		return executeEvaluationQuery(queryStatement);	
	}
	
	
	public QueryEvaluationReport executeIntegrationQuery_New_Q13_DeltaBetweenCurrentConsumptionAndLastMonthBasedPrediction(){
		String queryStatement =	"SELECT * " +
								"FROM \"DBMS_EMS_Schema\".\"New_Q13_DeltaBetweenCurrentConsumptionAndLastMonthBasedPredicti\" ";
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
