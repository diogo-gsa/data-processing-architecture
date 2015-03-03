package msc_thesis.diogo_anjos.DSMS_Version;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import msc_thesis.diogo_anjos.DBMS_Version.exceptions.ThereIsNoDataPoint_PKwithThisLocaionException;
import msc_thesis.diogo_anjos.simulator.EnergyMeter;
import msc_thesis.diogo_anjos.simulator.EnergyMeasureTupleDTO;
import msc_thesis.diogo_anjos.simulator.SimulatorClient;
import msc_thesis.diogo_anjos.util.DataPoint_PK;
import Datastream.Measure;

public class DSMS_VersionImpl implements SimulatorClient, Runnable{

	//Note the comparison/analogy between components 
	//	EsperEngine			--> DSMS_versionImpl
	//	DBMS_CRUD_Query_API --> DBMS_VersionImpl		

	// DUMP Configuration Flags ======================================
	private boolean DUMP_PUSHED_INPUT 		= false;
	private boolean DUMP_INPUTBUFFER_LENGTH = true;
	//=================================================================

	
	private EsperEngine esperEngine = new EsperEngine();
	private volatile boolean simulationHasFinished = false;

	
	//producerConsumerQueueOfTuples
	private LinkedList<EnergyMeasureTupleDTO> bufferOfTuples = new LinkedList<EnergyMeasureTupleDTO>(); 
	
	Map<EnergyMeter, Boolean> simulationStartStopFlags = new TreeMap<EnergyMeter, Boolean>(); 
	
/*=========================================================================================================== 
 * 			Push Datastream into DBMS and Execute Data Integration/Evaluation Queries 
 *=========================================================================================================*/		
	public DSMS_VersionImpl(){
		Thread bufferConsumerThread = new Thread(this);
		bufferConsumerThread.start();
		//=== Query to be Executed ============
//		install_Q0();
//		install_Q1();
//		install_Q3();
//		install_Q4();
//		install_Q5();
		install_Q6();
//		install_Q7();
//		install_Q8();
//		install_Q9();
//		install_Q10();
//		install_Q11();
//		install_Q12();	
//		install_Q13();
//		install_Q14();
//		install_Q16();
//		install_Q17();

		//=== Query to be Executed ============
	}
	
	
//	TODO: Attention: remover *synchronized* deste metodo para que a "velocidade" com que o simulator
//			preenche o byffer seja completamente independente da velocidade do Esper para processar esses tuplos.
//			C/ synch: SpeedTimeFactors Altos => Buffer Não enche demasiado  => tempo de simulação é muito maior do que o esperado.
//			S/ synch: SpeedTimeFactors Altos => Buffer Enche demasiado  => tempo de simulação é igual ao esperado.
	private /*synchronized*/ void processConsumedTuple(EnergyMeasureTupleDTO tuple){
		List<Measure> datastreamTuples = inputAdapter(tuple);
		for(Measure m : datastreamTuples){
			if(DUMP_PUSHED_INPUT){
				System.out.println("Input:"+m+"\n");
			}
			esperEngine.pushInput(m);
		}
	}
/* EOF Push Datastream and Queries execution ==============================================================*/
	
	// Queries Installation Functions	
	public void install_Q0(){
		install_Q00_DataAggregation(true);
	}
	
	public void install_Q11(){
		install_Q00_DataAggregation(false);
		install_Q11_InstantVariation(true);
	}
	
	public void install_Q12(){
		install_Q00_DataAggregation(false);
		install_Q12_DataStreamPeriodicity(true);
	}
	
	public void install_Q7(){
		install_Q00_DataAggregation(false);
		install_Q07_SmoothingConsumption(true);
	}
	
	public void install_Q8(){
		install_Q00_DataAggregation(false);
		install_Q07_SmoothingConsumption(false);
		install_Q08_SquareMeterNormalization(true);
	}
	
	public void install_Q9(){
		install_Q00_DataAggregation(false);
		install_Q07_SmoothingConsumption(false);
		install_Q08_SquareMeterNormalization(false);
		install_Q09_ProportionsFromConsumptions(true);
	}
	
	public void install_Q10(){
		install_Q00_DataAggregation(false);
		install_Q07_SmoothingConsumption(false);
		install_Q08_SquareMeterNormalization(false);
		install_Q10_ConsumptionsRankingList(true);
	}
	
	public void install_Q14(){
		install_Q00_DataAggregation(false);
		install_Q07_SmoothingConsumption(false);
		install_Q08_SquareMeterNormalization(false);
		install_Q14_ExpectedConsumptionByUDF(true);
	}
	
	public void install_Q13(){
		install_Q00_DataAggregation(false);
		install_Q07_SmoothingConsumption(false);
		install_Q08_SquareMeterNormalization(false);
		install_Q13_ExpectedConsumptionByMonthlyHourAvg(true);
	}
	
	public void install_Q4(){
		install_Q00_DataAggregation(false);	
		install_Q11_InstantVariation(false);
		install_Q04_InstantVariationAboveThreshold(true);
	}
	
	public void install_Q5(){
		install_Q00_DataAggregation(false);
		install_Q12_DataStreamPeriodicity(false);
		install_Q05_StreamPeriodicityOutOfRange(true);
	}
	
	public void install_Q1(){
		install_Q00_DataAggregation(false);
		install_Q07_SmoothingConsumption(false);
		install_Q08_SquareMeterNormalization(false);
		install_Q01_ConsumptionOverThreshold(true);
	}
	
	public void install_Q3(){
		install_Q00_DataAggregation(false);
		install_Q07_SmoothingConsumption(false);
		install_Q08_SquareMeterNormalization(false);
		install_Q03_MinMaxConsumptionRatio(true);
	}
	
	@Deprecated
	public void install_Q6_with_Q14_asInput(){
		install_Q00_DataAggregation(false);
		install_Q07_SmoothingConsumption(false);
		install_Q08_SquareMeterNormalization(false);
		install_Q14_ExpectedConsumptionByUDF(false);
		install_New_Q6_withQ14AsInput_DeltaAbove(true);
	}
	
	public void install_Q6(){
		install_Q00_DataAggregation(false);
		install_Q07_SmoothingConsumption(false);
		install_Q08_SquareMeterNormalization(false);
		install_Q13_ExpectedConsumptionByMonthlyHourAvg(false);
		install_Q06_ConsumptionAboveExpected(true);
	}
	
	public void install_Q17(){
		install_Q00_DataAggregation(false);
		install_Q07_SmoothingConsumption(false);
		install_Q08_SquareMeterNormalization(false);
		install_Q14_ExpectedConsumptionByUDF(false);
		install_Q17_ConsumptionAboveExpectedCounter(true);
	}
	
	public void install_Q16(){
		install_Q00_DataAggregation(false);
		install_Q07_SmoothingConsumption(false);
		install_Q16_ConsumptionAboveSlidingAvgThreshold(true);
	}
	
/* ==========================================================================================================
 * 										Data Integration and Evaluation Queries
 * ========================================================================================================*/
	
	public void install_Q00_DataAggregation(boolean addListener){
		
		String sqlQuery="SELECT  dev.device_pk 			 AS device_pk, "						 +
								"dpu.unit                AS measure_unit, "						 +
								"dpd.description         AS measure_description, "				 +
								"dl.location             AS device_location,"					 +
								"dl.area_m2              AS location_area_m2 "					 +
						
						"FROM    \"DSMS_EMS_Schema\".\"DataPoint\"         		   dp " 		 +
								"JOIN \"DSMS_EMS_Schema\".\"Device\"               dev " 		 +
								"ON dp.device_fk = dev.device_pk "								 +
								"JOIN \"DSMS_EMS_Schema\".\"DeviceLocation\"       dl  " 		 +
								"ON dev.device_location_fk = dl.device_location_pk "			 +
								"JOIN \"DSMS_EMS_Schema\".\"DataPointDescription\" dpd " 		 +
								"ON dp.datapoint_description_fk = dpd.datapoint_description_pk " +
								"JOIN \"DSMS_EMS_Schema\".\"DataPointUnit\"        dpu " 		 +
								"ON dp.datapoint_unit_fk = dpu.datapoint_unit_pk " 				 +
						//testing PK because with dpd.description=".." weirdly does NOT work		
						"WHERE   ${datapointPk} = dp.datapoint_pk " 							 +
						  "AND (   dpd.datapoint_description_pk = 10 "							 +
						       "OR dpd.datapoint_description_pk = 11 " 							 +
						       "OR dpd.datapoint_description_pk = 12) " 						 ;
						
		String statement = 	"INSERT INTO _Q00_DataAggregation "														+
							"SELECT 	bd.device_pk 									AS device_pk, "				+
										"convertStringTSformatToLong(stream.measureTS)	AS measure_timestamp_long, " + 
										"stream.measureTS          						AS measure_timestamp, "		+
										"sum(stream.measure) 							AS measure, "				+
										"\"WATT\"	 									AS measure_unit, "			+
										"\"Power Consumption: 3-Phase Aggregate.\" 		AS measure_description, "	+
										"bd.device_location  							AS device_location, "		+		
										"bd.location_area_m2        					AS location_area_m2 "		+
							"FROM		Datastream.Measure 				AS stream , " 								+
										"sql:database ['"+sqlQuery+"'] 	AS bd "										+
							"GROUP BY	bd.device_pk," 																+
										"stream.measureTS "															+
							"HAVING		count(stream.measureTS) = 3"; //3 Phases
		
		esperEngine.installQuery(statement,addListener);
	}
	
	public void install_Q11_InstantVariation(boolean addListener){
		/* Query foi reescrita de uma forma mais simples e mais eficiente:
		 *
		 *		SELECT (measure/avg(measure) - 1) 	AS variation,
		 *				device_pk 					AS device_pk,
		 *				device_location 			AS device_location,
		 *				measure_timestamp 			AS measure_timestamp
		 *		FROM	DenormalizedAggPhases.win:time(60 min)
		 *		GROUP BY device_pk
		 *
		 * NO select projeccões "singulares" são sempre feitas sobre o tuplo mais recente,
		 * que acabou de entrar no engien, e fez triggered à query.
		 * Agregações são avaliados sobre janelas
		 * Por isso: measure      -> avaliado sobre tuplo mais recente.
		 * 			 avg(measure) -> avaliado sobre a ajanela win:time
		 * Assim, evitas um "SELF-JOIN" e um "OUTPUT LAST", no entanto tudo isto
		 * carece de validação impirica.
		 *
		 * Antiga (e Ineficiente) implemenntação da query que não tira
		 * partido da caracteristica especial dos DSMS de a query ser orientada ao
		 * tuplo, e não ao dataset inteiro:
		String statement = 	"INSERT INTO Q11_VariationStream " 							+
							"SELECT (now.measure/avg(win.measure) - 1) AS variation, "	+
									"now.device_pk AS device_pk, "						+
									"now.device_location AS device_location, "			+
									"now.measure_timestamp AS measure_timestamp "		+
							"FROM	DenormalizedAggPhases.win:time(60 min)	AS win, " 	+
							"		DenormalizedAggPhases.std:lastevent()	AS now "	+
							"WHERE 	win.device_pk = now.device_pk "						+
							"OUTPUT LAST EVERY 1 EVENTS ";
		*/
		
		String statement = 	"INSERT INTO _Q11_InstantVariation " 										 													+
							"SELECT 	device_pk 								  										AS device_pk, "		 				+
										"measure_timestamp 						  										AS measure_timestamp, " 			+
										"(measure/(avg(measure)+0.00001) - 1)*100 										AS measure, "		 				+
										"measure 								  										AS current_power_consumption, "  	+
										"\"Percentage%\" 						  										AS measure_unit, " 					+
										"\"Variation between current and last 5 minutes average power consumption.\"	AS measure_description, " 			+
										"device_location 						  										AS device_location, "	 			+
										"location_area_m2 "																									+
							"FROM		_Q00_DataAggregation.win:ext_timed(measure_timestamp_long, 5 min) "													+
							"GROUP BY 	device_pk ";
	
		esperEngine.installQuery(statement, addListener);
	}
	
	public void install_Q12_DataStreamPeriodicity(boolean addListener){		
		String statement = 	"INSERT INTO _Q12_DataStreamPeriodicity "	 																		  +
							"SELECT device_pk, " 																								  +									
									"last(measure_timestamp, 0) 														AS measure_timestamp, "	  +
									"(DateTime.toMillisec(last(measure_timestamp, 0),\"yyyy-MM-dd HH:mm:ss\") " 								  +
									" - DateTime.toMillisec(last(measure_timestamp, 1),\"yyyy-MM-dd HH:mm:ss\"))/1000 	AS measure, "			  +
									"\"Time Seconds\" 																	AS measure_unit," 		  +
									" \"Period between two last power consumption measurements.\" 						AS measure_description, " + 
									"device_location, "																							  +
									"location_area_m2 "																							  +
				 			"FROM _Q00_DataAggregation.std:groupwin(device_pk).win:length(2) "													  +
				 			"GROUP BY device_pk " 																								  +
				 			"HAVING count(*) > 1";		
		esperEngine.installQuery(statement, addListener);
	}
	
	
	public void install_Q07_SmoothingConsumption(boolean addListener){
		String statement = 	"INSERT INTO _Q07_SmoothingConsumption "	 															+
							"SELECT device_pk, " 																					+
									"measure_timestamp_long 											AS measure_timestamp_long, "+
									"measure_timestamp, " 																			+
									"avg(measure)														 AS measure, " 				+
									"\"WATT\" 															 AS measure_unit, "			+
									"\"Smoothed Power Consumption through 10 minutes sliding average.\"	 AS measure_description, "	+
									"device_location,  "																			+
									"location_area_m2 "																				+
							"FROM  _Q00_DataAggregation.win:ext_timed(measure_timestamp_long, 10 min) "								+
							"GROUP BY device_pk";
		esperEngine.installQuery(statement, addListener);
	}
	
	
		
	public void install_Q08_SquareMeterNormalization(boolean addListener){		
		
		String subStatement1 =	"INSERT INTO _Q08_Aux_SquareMeterNormalization "														+
								"SELECT device_pk, "																					+
										"measure_timestamp_long 											AS measure_timestamp_long, "+
										"measure_timestamp, "																			+
										"measure/location_area_m2											AS measure, "				+
										"\"WATT/m^2\"	 													AS measure_unit, "			+
										"\"Power Consumption Normalized by each location square " 										+
										"meter area and all building as a whole.\" 							AS measure_description, "	+
										"device_location "																				+
								"FROM	_Q07_SmoothingConsumption ";
		
		String subStatement2 =	"INSERT INTO _Q08_Aux_SquareMeterNormalization "										+
								"SELECT  0L	 																AS device_pk, "				+
										"min(measure_timestamp_long) 										AS measure_timestamp_long, "+
										"min(measure_timestamp)												AS measure_timestamp, " 	+
										"sum(measure)/sum(location_area_m2) 								AS measure, " 				+
										"\"WATT/m^2\"	 													AS measure_unit, " 			+
										"\"Energy consumption Normalized by energy meter location area\"	AS measure_description, "	+
										"\"ALL_BUILDING\" 													AS device_location " 		+					
								"FROM 	_Q07_SmoothingConsumption.std:unique(device_pk) " 												+
								"HAVING count(device_pk) = 8 " 																			+
								"OUTPUT LAST EVERY 8 EVENTS"; 

		String statement =	"INSERT INTO _Q08_SquareMeterNormalization "																+
							"SELECT	 device_pk, "			 					   														+
									"measure_timestamp_long, " 																			+
									"measure_timestamp, "																				+
									"measure, "																							+
									"measure_unit, "																					+
									"measure_description, "																				+
									"device_location "																					+	
							"FROM  _Q08_Aux_SquareMeterNormalization ";	

		esperEngine.installQuery(subStatement1, false);
		esperEngine.installQuery(subStatement2, false);
		esperEngine.installQuery(statement, addListener);
		
	}
	
	public void install_Q09_ProportionsFromConsumptions(boolean addListener){		
		String statement = 	"SELECT device_pk, " 																			+																	
									"measure_timestamp, " 																	+
									"(measure/SUM(measure))*100 						AS measure, "						+
									"\"Percentage%\" 									AS measure_unit, "					+						
									"\"Proportion of each location power consumption " 										+
									"by comparation with all other locations.\"  		AS measure_description, "			+
									"device_location "																		+
							"FROM _Q08_SquareMeterNormalization.std:unique(device_pk) "										+
							"WHERE device_pk != 0 "																			+
							"OUTPUT SNAPSHOT EVERY 1 EVENTS";
		esperEngine.installQuery(statement, addListener);
	}
	
	public void install_Q10_ConsumptionsRankingList(boolean addListener){		
		String statement = 	"SELECT device_pk," 																					+
									"measure_timestamp, "																			+
									"measure, "																						+
									"\"WATT\" 															   AS measure_unit, "		+
									"\"Descendig Ranking List of each Location by its power consumption.\" AS measure_description, "+
									"device_location "																				+	
							"FROM _Q08_SquareMeterNormalization.std:unique(device_pk) "												+
							"OUTPUT SNAPSHOT EVERY 1 EVENTS "																		+
							"ORDER BY measure DESC";
		esperEngine.installQuery(statement, addListener);
	}
	
	
	public void install_Q14_ExpectedConsumptionByUDF(boolean addListener){							
		String statement =  "INSERT INTO _Q14_ExpectedConsumptionByUDF "                    	                   		+
							"SELECT device_pk, "  																		+
									"measure_timestamp_long 								AS measure_timestamp_long, "+
			       					"measure_timestamp, "                                                          		+                    
			       					"measure                                                AS current_measure, "  		+
			       					"getExpectedMeasure(device_pk, measure_timestamp)       AS expected_measure, "  	+
			       					"\"WATT/m^2\" 											AS measure_unit, "         	+                     
			       					"\"Current and Expected Power consumption given by a " 								+
			       					"User Defined Function (UDF).\" 						AS measure_description, " 	+                    
			       					"device_location "                                                              	+            
			       			"FROM _Q08_SquareMeterNormalization ";
		esperEngine.installQuery(statement, addListener);
	}
	
	public void install_Q13_ExpectedConsumptionByMonthlyHourAvg(boolean addListener){
		
		String statement = 	"INSERT INTO _Q13_ExpectedConsumptionByMonthlyHourAvg "                                         +      
							"SELECT  device_pk, "                                                                    		+
		        					"measure_timestamp, "                                                                   +         
		        					"measure					 								AS current_measure, "		+
		        					"avg(measure)                								AS expected_measure, "		+
		        					"\"WATT/m^2\" 												AS measure_unit, "          +       
		        					"\"Current and Expected Power consumption given by last " 								+
		        					"month average consumption of the current hour.\" 			AS measure_description, "  	+
		        					"device_location "                                                                      +                          
							"FROM _Q08_SquareMeterNormalization.win:ext_timed(measure_timestamp_long, 1 month)	"			+
							"GROUP BY device_pk, DateTime.toDate(measure_timestamp, \"yyyy-MM-dd HH:mm:ss\").getHours()"; 
		
		esperEngine.installQuery(statement, addListener);
	}
	
	
// 	----------------------------- Evaluation Queries -------------------------------------	
	
	public void install_Q04_InstantVariationAboveThreshold(boolean addListener){
		String statement = 	"SELECT  device_pk, " 																					+
									"measure_timestamp, " 																			+
									"measure, "																						+
									"current_power_consumption, "																	+
									"\"Percentage%\" 												AS measure_unit, "				+
									"\"Variation between current and last 5 minutes average power " 								+
									"consumption that exceeded a given threshold.\" 				AS measure_description, "		+
									"device_location,  "																			+
									"location_area_m2  "																			+
							"FROM	_Q11_InstantVariation	"																		+	
							"WHERE ((device_pk = 1 AND measure*0 >= 0) " 																+  									
							   "OR  (device_pk = 2 AND measure*0 >= 0) "																+								
							   "OR  (device_pk = 3 AND measure*0 >= 0) "																+								
							   "OR  (device_pk = 4 AND measure*0 >= 0) "																+								
							   "OR  (device_pk = 5 AND measure*0 >= 0) "																+								
							   "OR  (device_pk = 6 AND measure*0 >= 0) "																+								
							   "OR  (device_pk = 7 AND measure*0 >= 0) "																+								
							   "OR  (device_pk = 8 AND measure*0 >= 0)) ";
			//IMPORTANT: Use device_pk = X AND measure*0 >= 0 for universal condition
		esperEngine.installQuery(statement, addListener);
	}
	
	public void install_Q05_StreamPeriodicityOutOfRange(boolean addListener){
		String statement = 	"SELECT device_pk, "							+
									"measure_timestamp, " +
									"measure, " +
									"\"Time Seconds\" 																				AS measure_unit, " +
									"\"Period between two last power consumption measurements is out of range: [55, 65] seconds.\" 	AS measure_description, " +
									"device_location, " +
									"location_area_m2 " +
				 			"FROM _Q12_DataStreamPeriodicity "  			+
				 			//"WHERE NOT(55 <= measure  AND  measure <= 65) ";   		// Real Condition
							"WHERE (-9999999 <= measure  AND  measure <= 9999999) ";	// Universal Condition
		
							//Important: Universal condition required for worst case scenario
		esperEngine.installQuery(statement, addListener);
	}
	
	public void install_Q01_ConsumptionOverThreshold(boolean addListener){
		String statement = 	"SELECT  device_pk, "																+
									"measure_timestamp, "														+
									"measure, "																	+
									"\"WATT/m^2\" 									AS measure_unit, "			+
									"\"Power consumption above a given threshold.\" AS measure_description, "	+
									"device_location "                              							+                 
							"FROM  _Q08_SquareMeterNormalization "												+
							"WHERE  (device_pk = 0 AND measure >= 0) "											+
		   					   "OR  (device_pk = 1 AND measure >= 0) "											+
		   					   "OR  (device_pk = 2 AND measure >= 0) "											+
		   					   "OR  (device_pk = 3 AND measure >= 0) "											+
		   					   "OR  (device_pk = 4 AND measure >= 0) "											+
		   					   "OR  (device_pk = 5 AND measure >= 0) "											+
		   					   "OR  (device_pk = 6 AND measure >= 0) "											+
		   					   "OR  (device_pk = 7 AND measure >= 0) "											+
		   					   "OR  (device_pk = 8 AND measure >= 0) "											+
		   					   "OR  (device_pk = 0 AND measure >= 0)";	
		//Important: Universal condition required for worst case scenario	
		esperEngine.installQuery(statement, addListener);
	}
	
	public void install_Q03_MinMaxConsumptionRatio(boolean addListener){
		String statement = 	"SELECT  device_pk, "																					+
        							"measure_timestamp, "																			+
        							"min(measure)/(max(measure)+0.000001)            		AS measure, "							+
        							"\"Ratio = [0,1]\" 										AS measure_unit, "						+
        							"'Min/Max Power Consumption Ratio during last hour.' 	AS measure_description, "				+
        							"device_location, "																				+
        							"min(measure)                         					AS min_last_hour_power_consumption, "	+
        							"max(measure)                         					AS max_last_hour_power_consumption "	+
        					"FROM   _Q08_SquareMeterNormalization.win:ext_timed(measure_timestamp_long, 60 min) " +
        					"GROUP BY device_pk";
		esperEngine.installQuery(statement, addListener);
	}
	
	public void install_Q06_ConsumptionAboveExpected(boolean addListener){
		String statement = 	"SELECT  device_pk, "																											+                                                                                                 
		        					"measure_timestamp, "																									+
		        					"(current_measure/(expected_measure+0.0001) - 1)*100							 	  AS measure, "              		+							   
							        "current_measure                                        						 	  AS current_power_consumption, " 	+
							        "expected_measure                               								 	  AS expected_power_consumption, "	+
							        "\"Percentage%\"                                                                 	  AS measure_unit, " 				+
							        "\"Delta between current and expecetd power consumption exceeded a given threshold.\" AS measure_description, " 		+
							        "device_location "                                                                      								+    
							"FROM   _Q13_ExpectedConsumptionByMonthlyHourAvg "																				+ 
							"WHERE  (current_measure/(expected_measure+0.0001) - 1)*0 >= 0";	//Universal Condition
//							"WHERE  (current_measure/(expected_measure+0.0001) - 1)*100 >= 10"; //Real Condition
							//Important: Universal condition required for worst case scenario
		esperEngine.installQuery(statement, addListener);
	}
			
	public void install_Q17_ConsumptionAboveExpectedCounter(boolean addListener){
		String statement = 	"SELECT  device_pk, " 																		+
		        					"measure_timestamp, " 																+
		        					"count(current_measure) 								AS measure, " 				+
		        					"\"Positive Integer\" 									AS measure_unit, " 			+
		        					"\"Number of times that, in last hour,  current " 									+
		        					"consumption as exceeded the expected one. " 										+
		        					"Being the counter limited by a min and max threshold.\" 	AS measure_description, " 	+
		        					"device_location " 																	+ 
		        			"FROM   _Q14_ExpectedConsumptionByUDF.win:ext_timed(measure_timestamp_long, 60 min) " 		+
		        			"WHERE  current_measure >= expected_measure*0 " 											+	//Universal Condition
//		        			"WHERE  current_measure > expected_measure " 												+	//Real condition
		        			"GROUP BY device_pk " 																		+
		        			"HAVING 0 <= count(current_measure) AND count(current_measure) <= 999999 ";		// Universal condition
//		        			"HAVING 5 <= count(current_measure) AND count(current_measure) <= 10 "; 	// Real condition
							//Important: Universal condition required for worst case scenario
		
		esperEngine.installQuery(statement, addListener);
	}
	
	public void install_Q16_ConsumptionAboveSlidingAvgThreshold(boolean addListener){		
		String statement = 	"SELECT device_pk, "	 																					 +
									"measure_timestamp, "																				 +
									"measure, "																							 +
									"avg(measure)*1.20 															AS measure_threshold, "	 +
									"\"WATT\" 																	AS measure_unit, "	 	 +
									"\"Power consumption 20% above the average consumption of last 24 hours.\" 	AS measure_description, "+
									"device_location "																					 +
							"FROM _Q07_SmoothingConsumption.win:ext_timed(measure_timestamp_long, 24 hours) " +
							"GROUP BY device_pk "																						 +
							"HAVING measure >= avg(measure)*0.0 ";
							//Important: Universal condition required for worst case scenario
		esperEngine.installQuery(statement, addListener);
	}

// =================================== DEPRECATED FUNCTIONS ==================================
	@Deprecated
	public void installHelloWorldQuery(boolean addListener){
		String statement = 	"SELECT * " +
							"FROM Datastream.Measure";
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void installHelloWorldDatabaseQuery(boolean addListener){
		String statement = 	"SELECT measureTS, measure, datapointPk, datapoint_description_fk "	+
							"FROM	Datastream.Measure, " 										+
									"sql:database ['SELECT datapoint_description_fk " 			+
									             "FROM \"DSMS_EMS_Schema\".\"DataPoint\" "		+	
									             "WHERE datapoint_pk = 82']";			
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void install_Q0_BaseView(boolean addListener){
		String sqlQuery="SELECT  dev.device_pk 			 AS device_pk, "						+
								"dpu.unit                AS measure_unit, "						+
								"dpd.description         AS measure_description, "				+
								"dl.location             AS device_location,"					+
								"dl.area_m2              AS location_area_m2 "					+
						
						"FROM    \"DSMS_EMS_Schema\".\"DataPoint\"            dp, "				+ 
								"\"DSMS_EMS_Schema\".\"Device\"               dev, "			+
								"\"DSMS_EMS_Schema\".\"DeviceLocation\"       dl, " 			+
								"\"DSMS_EMS_Schema\".\"DataPointDescription\" dpd, "			+
								"\"DSMS_EMS_Schema\".\"DataPointUnit\"        dpu "				+
								
						//testing PK because with dpd.description=".." weirdly does NOT work
						"WHERE   ${datapointPk} = dp.datapoint_pk " 							+
							"AND dp.device_fk = dev.device_pk " 								+
							"AND dev.device_location_fk = dl.device_location_pk "				+
							"AND dp.datapoint_description_fk = dpd.datapoint_description_pk "	+ 
							"AND dp.datapoint_unit_fk = dpu.datapoint_unit_pk "					+
							"AND (dpd.datapoint_description_pk = 10 " 							+ 
									"OR dpd.datapoint_description_pk = 11 " 					+
									"OR dpd.datapoint_description_pk = 12) ";
		
        						
		String statement = 	"INSERT INTO DenormalizedAggPhases "								+
							"SELECT 	bd.device_pk 				AS device_pk, "				+
										"stream.measureTS          	AS measure_timestamp, "		+
										"sum(stream.measure) 		AS measure, "				+
										"\"WATT.HOUR\"	 			AS measure_unit, "			+
										"\"EnergyConsumptionPh123\" AS measure_description, "	+
										"bd.device_location  		AS device_location, "		+		
										"bd.location_area_m2        AS location_area_m2 "		+
										
							"FROM		Datastream.Measure 				AS stream , " 			+
										"sql:database ['"+sqlQuery+"'] 	AS bd "					+
							
							"GROUP BY	bd.device_pk," 											+
										"stream.measureTS "										+
							
							"HAVING		count(stream.measureTS) = 3"; //3 Phases
		 			
		esperEngine.installQuery(statement,addListener);
	}
	
	@Deprecated
	public void install_Q11_IntegrationQuery(boolean addListener){
		/* Query foi reescrita de uma forma mais simples e mais eficiente:
		 *
		 *		SELECT (measure/avg(measure) - 1) 	AS variation,
		 *				device_pk 					AS device_pk,
		 *				device_location 			AS device_location,
		 *				measure_timestamp 			AS measure_timestamp
		 *		FROM	DenormalizedAggPhases.win:time(60 min)
		 *		GROUP BY device_pk
		 *
		 * NO select projeccões "singulares" são sempre feitas sobre o tuplo mais recente,
		 * que acabou de entrar no engien, e fez triggered à query.
		 * Agregações são avaliados sobre janelas
		 * Por isso: measure      -> avaliado sobre tuplo mais recente.
		 * 			 avg(measure) -> avaliado sobre a ajanela win:time
		 * Assim, evitas um "SELF-JOIN" e um "OUTPUT LAST", no entanto tudo isto
		 * carece de validação impirica.
		 *
		 * Antiga (e Ineficiente) implemenntação da query que não tira
		 * partido da caracteristica especial dos DSMS de a query ser orientada ao
		 * tuplo, e não ao dataset inteiro:
		String statement = 	"INSERT INTO Q11_VariationStream " 							+
							"SELECT (now.measure/avg(win.measure) - 1) AS variation, "	+
									"now.device_pk AS device_pk, "						+
									"now.device_location AS device_location, "			+
									"now.measure_timestamp AS measure_timestamp "		+
							"FROM	DenormalizedAggPhases.win:time(60 min)	AS win, " 	+
							"		DenormalizedAggPhases.std:lastevent()	AS now "	+
							"WHERE 	win.device_pk = now.device_pk "						+
							"OUTPUT LAST EVERY 1 EVENTS ";
		*/
		
		String statement = 	"INSERT INTO Q11_VariationStream " 										 +
							"SELECT 	(measure/avg(measure) - 1)	 			  AS variation, "		 +
										"device_pk 								  AS device_pk, "		 +
										"device_location 						  AS device_location, "	 +
										"measure_timestamp 						  AS measure_timestamp " +
							"FROM		DenormalizedAggPhases.win:time(10 min) " 						 +
							"GROUP BY 	device_pk ";
	
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void install_Q4_EvaluationQuery(boolean addListener){
		String statement = 	"SELECT variation          		AS variationAlarm, "		+
									"device_pk         		AS device_pk, "				+
									"device_location   		AS device_location, "		+
									"measure_timestamp 		AS measure_timestamp "		+
							"FROM	Q11_VariationStream	"								+
							"WHERE 	variation > 0.01 ";			
	
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void install_Q7_8_Normalization_IntegrationQuery(boolean addListener){
		String statement = 	"INSERT INTO LocationNormalizedMeasures "	 +
							"SELECT device_pk, " 																+
									"measure_timestamp, " 														+
									"avg(measure)/location_area_m2			AS normalized_measure_avg_10min, " 	+
									"\"WATT.HOUR/m^2\"	 					AS measure_unit, "					+
									"\"NormalizedEnergyConsumptionPh123\"	AS measure_description, "			+
									"device_location "															+
							"FROM  DenormalizedAggPhases.win:time(10 min) " 									+
							"GROUP BY device_pk";
	
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void install_Q8_BuildingConsumptionNormalized_IntegrationQuery(boolean addListener){
		String statement = 	"INSERT INTO Q8_AllBuildingNormalization " 													+
							"SELECT min(measure_timestamp)							AS measure_timestamp, "				+
									"sum(measure_avg_10min)/sum(location_area_m2) 	AS building_normalized_measure, "	+
									"\"WATT.HOUR/m2\" 				      			AS measure_unit, "					+
									"\"EnergyConsumption_NormalizedByTotalArea\"    AS measure_description, "			+
									"count(device_pk) 								AS covered_devices, "				+
									"sum(location_area_m2)							AS covered_area_m2 "				+
							"FROM 	Q7_Sliding10minAVGbyDevice.std:unique(device_pk).win:time(1 min) "					+
							"HAVING count(device_pk) = 8 "	 															+
							"OUTPUT LAST EVERY 8 EVENTS ";
	
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void install_Q3_MinMaxRatioQuery(boolean addListener){
		String statement = 	"SELECT max(measure_timestamp) 			   AS measure_timestamp, "	    + 
								   "min(building_normalized_measure)" 								+
								   "/max(building_normalized_measure)  AS min_max_measure_ratio, "  +
								   "max(building_normalized_measure)   AS max_measure, "			+
								   "max(building_normalized_measure)   AS min_measure, "			+
								   "covered_devices, "												+
								   "covered_area_m2 "												+
							"FROM	Q8_AllBuildingNormalization.win:time(60 min) ";
	
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void install_Q9_Percentage(boolean addListener){		
		String statement = 	"SELECT device_pk, " 																		+
									"measure_timestamp, "																+
									"(normalized_measure_avg_10min/SUM(normalized_measure_avg_10min))*100 AS measure, "	+
									"\"percentage\" AS measure_unit, "													+
									"\"%ofTotalNormalizedEnergyConsumption\" AS measure_description, "					+
									"device_location "																	+	
							"FROM LocationNormalizedMeasures.std:unique(device_pk).win:time(2 min) "					+
							"OUTPUT SNAPSHOT EVERY 1 EVENTS ";
	
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void install_Q10_OrderBy(boolean addListener){		
		String statement = 	"SELECT device_pk," 														+
									"measure_timestamp, "												+
									"normalized_measure_avg_10min AS normalized_measure, "				+
									"measure_unit, "													+
									"measure_description, "												+
									"device_location "													+	
							"FROM LocationNormalizedMeasures.std:unique(device_pk).win:time(2 min) "	+
							"OUTPUT SNAPSHOT EVERY 1 EVENTS "											+
							"ORDER BY normalized_measure_avg_10min DESC";
	
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void install_Q12_DeltaBetweenTuples(boolean addListener){		
		String statement = 	"INSERT INTO DeltaBetweenTuples "	 													+
							"SELECT device_pk, " 																	+
									"device_location, " 															+
									"last(measure_timestamp, 0) AS measure_timestamp_last, " 						+
									"last(measure_timestamp, 1) AS measure_timestamp_2nd_Last, " 					+
									"(DateTime.toMillisec(last(measure_timestamp, 0),\"yyyy-MM-dd HH:mm:ss\") " 							+
									" - DateTime.toMillisec(last(measure_timestamp, 1),\"yyyy-MM-dd HH:mm:ss\"))/1000 AS delta_seconds "	+
				 			"FROM DenormalizedAggPhases.std:groupwin(device_pk).win:length(2) "						+
				 			"GROUP BY device_pk " 																	+
				 			"HAVING count(*) > 1";		
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void install_Q5_DeltaBetweenTuplesOverThreashold(boolean addListener){		
		String statement = 	"SELECT * "					+
				 			"FROM DeltaBetweenTuples "  +
							"WHERE delta_seconds < 50 " +
							"	OR delta_seconds > 70";
		esperEngine.installQuery(statement, addListener);
	}
		
	@Deprecated
	public void install_Q1_AllAndEachDevicesNormalizedConsumptionOverThreshold(boolean addListener){		
		
		String statementAdapterQ78 ="INSERT INTO NormalizedMeasureQ1Input "								+
									"SELECT	device_pk						AS device_pk, "				+
											"measure_timestamp 				AS measure_timestamp, "		+
											"normalized_measure_avg_10min	AS measure, "				+
											"measure_unit					AS measure_unit, "			+
											"measure_description			AS measure_description, "	+
											"device_location 				AS device_location "		+
									"FROM  LocationNormalizedMeasures ";
		
		String statementAdapterQ8 =	"INSERT INTO NormalizedMeasureQ1Input "								+
									"SELECT	0L								AS device_pk, "				+
											"measure_timestamp				AS measure_timestamp, " 	+
											"building_normalized_measure	AS measure, "				+
											"measure_unit					AS measure_unit, "			+
											"measure_description			AS measure_description, "	+
									 		"\"AllDevices\"					AS device_location "		+
									 "FROM  Q8_AllBuildingNormalization ";

		String statementAdapterQ1 =	"SELECT	* "															+
									"FROM  NormalizedMeasureQ1Input "									+	
									"WHERE  (device_pk = 1    AND measure >= 0) "						+
									   "OR  (device_pk = 2    AND measure >= 0) "						+
									   "OR	(device_pk = 3    AND measure >= 0) "						+
									   "OR	(device_pk = 4    AND measure >= 0) "						+
									   "OR	(device_pk = 5    AND measure >= 0) "						+
									   "OR	(device_pk = 6    AND measure >= 0) "						+
									   "OR	(device_pk = 7    AND measure >= 0) "						+
									   "OR	(device_pk = 8 	  AND measure >= 0) "						+
									   "OR	(device_pk = 0 	  AND measure >= 0) "						;
		
		
		esperEngine.installQuery(statementAdapterQ78, false);
		esperEngine.installQuery(statementAdapterQ8,  false);
		esperEngine.installQuery(statementAdapterQ1,  addListener);
		
	}
	@Deprecated
	public void install_Q16_MeasuresPercentHigherThanAverageThresold(boolean addListener){		
		String statement = 	"SELECT device_pk, "	 																+
									"measure_timestamp, "															+
									"measure_avg_10min 									AS measure, "				+
									"avg(measure_avg_10min)*1.25 						AS threshold_measure, "		+
									"device_location, "																+
									"measure_unit, "																+
									"\"Measures 25% higher than the past 24h average\" AS measure_description "		+
							"FROM Q7_Sliding10minAVGbyDevice.win:time(24 hours) "									+
							"GROUP BY device_pk "																	+
							"HAVING measure_avg_10min >= avg(measure_avg_10min)*1.25 ";
		
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void install_Q14_RealAndExpectedMeasureDelta(boolean addListener){		
		
		String statement =  "INSERT INTO Q14_CurrentAndExpectedMeasure "																	+ 	
							"SELECT device_pk, " 																							+
									"measure_timestamp, "																					+
									"normalized_measure_avg_10min 				    				    			 AS measure, "			+
									"getExpectedMeasure(device_pk, measure_timestamp) 				    			 AS expected_measure, "	+
									"normalized_measure_avg_10min - getExpectedMeasure(device_pk, measure_timestamp) AS delta, "			+
									"measure_unit, "																						+
									"measure_description, " 																				+
									"device_location "																						+
							"FROM LocationNormalizedMeasures";
		
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void install_Q13_CurrentAndExpectedHourClusterMeasure(boolean addListener){
		
		String statement = 	"INSERT INTO Q13_CurrentAndExpectedMeasure "																	+
							"SELECT device_pk, " 																							+
									"measure_timestamp, "																					+
									"measure_avg_10min	 													AS measure, "					+
									"avg(measure_avg_10min) 												AS clusterExpectedMeasure, "	+
									"measure_avg_10min - avg(measure_avg_10min) 							AS delta, " 					+
									"DateTime.toDate(measure_timestamp, \"yyyy-MM-dd HH:mm:ss\").getHours() AS clusterPivotHour, " 			+
									"count(measure_avg_10min)/60											AS clusterDatasetDays, "		+
									"measure_unit, "																						+
									"measure_description, "																					+
									"device_location "																						+			
							"FROM Q7_Sliding10minAVGbyDevice.win:time(1 month)"																+
							"GROUP BY device_pk, DateTime.toDate(measure_timestamp, \"yyyy-MM-dd HH:mm:ss\").getHours()";
		
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void install_Q6_withQ13AsInput_CurrentAndExpectedConsumptionAboveGivenPercentage(boolean addListener){
		String statement = 	"SELECT device_pk, "																								+
									"measure_timestamp, "																						+
									"(measure/(clusterExpectedMeasure+0.0001) - 1)*100 								 AS measure, "				+
									"measure 																		 AS current_cosnumption, "	+
									"clusterExpectedMeasure 														 AS expected_consumption, "	+
									"\"%percent\" 																	 AS measure_unit, "			+
									"\"Percent variation between current and expected consumption greater than 10%\" AS measure_description, "	+
									"device_location "																							+
							"FROM	Q13_CurrentAndExpectedMeasure "																				+
							"WHERE 	(measure/(clusterExpectedMeasure+0.0001) - 1)*100 > 0 ";
		
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void install_Q6_withQ14AsInput_CurrentAndExpectedConsumptionAboveGivenPercentage(boolean addListener){
		
		String statement = 	"SELECT device_pk, "																								+
									"measure_timestamp, "																						+
									"(measure/(expected_measure+0.0001) - 1)*100 								 	 AS measure, "				+
									"measure 																		 AS current_cosnumption, "	+
									"expected_measure 														 		 AS expected_consumption, "	+
									"\"%percent\" 																	 AS measure_unit, "			+
									"\"Percent variation between current and expected consumption greater than 10%\" AS measure_description, "	+
									"device_location "																							+
							"FROM	Q14_CurrentAndExpectedMeasure "																				+
							"WHERE 	(measure/(expected_measure+0.0001) - 1)*0 = 0 ";
		
		esperEngine.installQuery(statement, addListener);
	}
	
	@Deprecated
	public void install_New_Q6_withQ14AsInput_DeltaAbove(boolean addListener){
		String statement = 	"SELECT  device_pk, " 																								+
		        					"measure_timestamp, "																						+
		        					"(measure/(expected_measure+0.0001) - 1)*100                                     AS measure, " 				+
		        					"measure                                                                         AS current_consumption, " 	+
		        					"expected_measure                                                                AS expected_consumption, " +
		        					"\"%percent\"                                                                    AS measure_unit, " 		+
		        					"\"Percent variation between current and expected consumption greater than 10%\" AS measure_description, " 	+
		        					"device_location " 																							+                                                                             
		        			"FROM   New_Q14_CurrentAndExpectedMeasure " 																		+
							//IMPORTANT: (measure/(expected_measure+0.0001) - 1)*0 >= 0 Universal Condition
		        			"WHERE  (measure/(expected_measure+0.0001) - 1)*0 >= 0 ";	
		esperEngine.installQuery(statement, addListener);
	}
	
/* EOF Data Integration and Evaluation Queries ==============================================================*/	
	
	
/*=========================================================================================================== 
 * 			SimulatorClient's Interface Implementation and Producer and Consumer Buffer Code
 *=========================================================================================================*/		
	@Override
	public void receiveDatastream(EnergyMeasureTupleDTO tuple) {
		produceTuple(tuple);
	}
	
	
	private synchronized void produceTuple(EnergyMeasureTupleDTO tuple){
		bufferOfTuples.addLast(tuple);
		notifyAll();
	}
	
	
	private void consumeTuple(){
		EnergyMeasureTupleDTO tuple;
		while(true){
			synchronized (this) {			
				while(bufferOfTuples.isEmpty()){
					//1st IF eval. is redundant but helps to clearly understand the END condition
					if(bufferOfTuples.isEmpty() && simulationHasFinished){
						return; //otherwise thread will be waiting forever
					}
					try {
						wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				tuple = bufferOfTuples.pollFirst();
			}
			processConsumedTuple(tuple);
			if(DUMP_INPUTBUFFER_LENGTH){
				System.out.print("Input_Buffer(remaining events)= "+bufferOfTuples.size()+" | ");
			}
		}
	}
	
	@Override
	public synchronized void simulationHasStartedNotification(EnergyMeter em) {
		simulationStartStopFlags.put(em, true); //true  = simulation started
	}
	
	@Override
	public synchronized void simulationHasFinishedNotification(EnergyMeter em) {
		
		// Simulator "em" has finished its simulation work (false = simulation finished)
		simulationStartStopFlags.put(em, false);
		
		// Check if all simulators/sensors have already finished their work
		for(boolean startStopFlag : simulationStartStopFlags.values()){
			if(startStopFlag == true){
				return; //nop, there is at least one simulator/simulation still working 
			}
		}
		
		// all started simulator have finished their work, lets stop producer/consumer buffer thread
		simulationHasFinished = true;
		notifyAll(); //wake up waiting threads so they can check the flag
	}

	@Override
	public void run() {
		consumeTuple();
	}
	
	private List<Measure> inputAdapter(EnergyMeasureTupleDTO dto){
		
		DataPoint_PK dpPK = null;
		try{
			dpPK = DataPoint_PK.getDataPoint_PKByLocation(dto.getMeterLocation());
		}catch(ThereIsNoDataPoint_PKwithThisLocaionException e){
			e.printStackTrace();
			System.exit(1); //non-zero status program = program terminate with errors 
		}
		
		ArrayList<Measure> measures = new ArrayList<>();
		measures.add(new Measure(dto.getMeasureTS(), dto.getPh1Consumption(), dpPK.getPh1_PK()));
		measures.add(new Measure(dto.getMeasureTS(), dto.getPh2Consumption(), dpPK.getPh2_PK()));
		measures.add(new Measure(dto.getMeasureTS(), dto.getPh3Consumption(), dpPK.getPh3_PK()));
	
		return measures;
	}
/* EOF Producer and Consumer Buffer Code ====================================================*/

}
