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

	private EsperEngine esperEngine = new EsperEngine();
	private volatile boolean simulationHasFinished = false;

	//TODO turn on/off some dump flags (verbose mode)
	private boolean DUMP_PUSHED_INPUT = false;
	private boolean DUMP_INPUTBUFFER_LENGTH = true;
	
	//producerConsumerQueueOfTuples
	private LinkedList<EnergyMeasureTupleDTO> bufferOfTuples = new LinkedList<EnergyMeasureTupleDTO>(); 
	
	Map<EnergyMeter, Boolean> simulationStartStopFlags = new TreeMap<EnergyMeter, Boolean>(); 
	
/*=========================================================================================================== 
 * 			Push Datastream into DBMS and Execute Data Integration/Evaluation Queries 
 *=========================================================================================================*/		
	public DSMS_VersionImpl(){
		Thread bufferConsumerThread = new Thread(this);
		bufferConsumerThread.start();
		install_Q0_BaseView(false);
		install_Q12_DeltaBetweenTuples(true);
//		install_Q11_IntegrationQuery(true); //install_Q4_EvaluationQuery(true);
//		install_Q7_8_Normalization_IntegrationQuery(false);
//		install_Q9_Percentage(true);
//		install_Q10_OrderBy(true);
	}
		
	private synchronized void processConsumedTuple(EnergyMeasureTupleDTO tuple){
		List<Measure> datastreamTuples = inputAdapter(tuple);
		for(Measure m : datastreamTuples){
			if(DUMP_PUSHED_INPUT){
				System.out.println("Pushing into Esper's engine -> "+m+"\n");
			}	
			esperEngine.pushInput(m);
		}	
	}
/* EOF Push Datastream and Queries execution ==============================================================*/
	

/* ==========================================================================================================
 * 										Data Integration and Evaluation Queries
 * ========================================================================================================*/
	
	public void installHelloWorldQuery(boolean addListener){
		String statement = 	"SELECT * " +
							"FROM Datastream.Measure";
		esperEngine.installQuery(statement, addListener);
	}
	
	public void installHelloWorldDatabaseQuery(boolean addListener){
		String statement = 	"SELECT measureTS, measure, datapointPk, datapoint_description_fk "	+
							"FROM	Datastream.Measure, " 										+
									"sql:database ['SELECT datapoint_description_fk " 			+
									             "FROM \"DSMS_EMS_Schema\".\"DataPoint\" "		+	
									             "WHERE datapoint_pk = 82']";			
		esperEngine.installQuery(statement, addListener);
	}
	
	
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
	
	
	public void install_Q11_IntegrationQuery(boolean addListener){
		String statement = 	"INSERT INTO Q11_VariationStream " +
							"SELECT (now.measure/avg(win.measure) - 1) AS variation, "	+
									"now.device_pk AS device_pk, "						+
									"now.device_location AS device_location, "			+
									"now.measure_timestamp AS measure_timestamp "		+
							"FROM	DenormalizedAggPhases.win:time(60 min)	AS win, " 	+
							"		DenormalizedAggPhases.std:lastevent()	AS now "	+
							"WHERE 	win.device_pk = now.device_pk "						+
							"OUTPUT LAST EVERY 1 EVENTS ";			
	
		esperEngine.installQuery(statement, addListener);
	}
	
	public void install_Q4_EvaluationQuery(boolean addListener){
		String statement = 	"SELECT variation          		AS variationAlarm, "		+
									"device_pk         		AS device_pk, "				+
									"device_location   		AS device_location, "		+
									"measure_timestamp 		AS measure_timestamp "		+
							"FROM	Q11_VariationStream	"								+
							"WHERE 	variation > 0.01 ";			
	
		esperEngine.installQuery(statement, addListener);
	}
	
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
	
	public void install_Q12_DeltaBetweenTuples(boolean addListener){		
		String statement = 	"SELECT device_pk, " +
									"device_location, " +
									"last(measure_timestamp, 0) AS measure_timestamp_last, " +
									"last(measure_timestamp, 1) AS measure_timestamp_2nd_Last, " +
									"(DateTime.toMillisec(last(measure_timestamp, 0),\"yyyy-MM-dd HH:mm:ss\") " +
									" - DateTime.toMillisec(last(measure_timestamp, 1),\"yyyy-MM-dd HH:mm:ss\"))/1000 AS delta_seconds "	+
				 			"FROM DenormalizedAggPhases.std:groupwin(device_pk).win:length(2) "			+
				 			"GROUP BY device_pk " 													+
				 			"HAVING count(*) > 1";		
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
				if(DUMP_INPUTBUFFER_LENGTH){
					System.out.print("Input_Buffer(remaining events): "+bufferOfTuples.size()+" | ");
				}
			}
			processConsumedTuple(tuple);
		}
	}
	
	@Override
	public synchronized void simulationHasStartedNotification(EnergyMeter em) {
		simulationStartStopFlags.put(em, true); //true  = simulation started
	}
	
	@Override
	public synchronized void simulationHasFinishedNotification(EnergyMeter em) {
		
		// Simulator "em" has finished its simulation work (false = simulation finished)
		simulationStartStopFlags.put(em, false); //
		
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
