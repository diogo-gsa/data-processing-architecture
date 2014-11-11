package msc_thesis.diogo_anjos.DSMS_Version;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import msc_thesis.diogo_anjos.DBMS_Version.exceptions.ThereIsNoDataPoint_PKwithThisLocaionException;
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

	//TODO to turn off some input event push
	private boolean printPushedInput = false;
	
	//producerConsumerQueueOfTuples
	private LinkedList<EnergyMeasureTupleDTO> bufferOfTuples = new LinkedList<EnergyMeasureTupleDTO>(); 
		
	
	public DSMS_VersionImpl(){
		Thread bufferConsumerThread = new Thread(this);
		bufferConsumerThread.start();
		install_Q0_BaseView(false);
		install_Q11_IntegrationQuery(true);
	}
	
	
// 	==========================================================================================
//	Case Study Queries Implementation 
//	==========================================================================================
	
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
		String statement = 	"SELECT (now.measure/avg(win.measure) - 1) AS variation, "	+
									"now.device_pk AS device_pk, "						+
									"now.device_location AS device_location, "			+
									"now.measure_timestamp AS measure_timestamp "		+
							"FROM	DenormalizedAggPhases.win:time(60 min)	AS win, " 	+
							"		DenormalizedAggPhases.std:lastevent()	AS now "	+
							"WHERE 	win.device_pk = now.device_pk "						+
							"OUTPUT LAST EVERY 1 EVENTS ";			
		
		
		esperEngine.installQuery(statement, addListener);
	}
	
//	==========================================================================================
//	End Of Case Study Queries Implementation Zone 
//	==========================================================================================
	 
	
	
	
	
	/*
	 * SimulatorClient's Interface Implementation
	 * And Producer and consumer Functions
	 */
	
	private synchronized void processConsumedTuple(EnergyMeasureTupleDTO tuple){
		List<Measure> datastreamTuples = inputAdapter(tuple);
		for(Measure m : datastreamTuples){
			if(printPushedInput){
				System.out.println("Pushing into Esper's engine -> "+m+"\n");
			}
			
			esperEngine.pushInput(m);
		}	
	}
	
	
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
		}
	}
	
	
	@Override
	public synchronized void simulationHasFinishedNotification() {
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

}
