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

	
	//producerConsumerQueueOfTuples
	private LinkedList<EnergyMeasureTupleDTO> bufferOfTuples = new LinkedList<EnergyMeasureTupleDTO>(); 
		
	
	public DSMS_VersionImpl(){
		Thread bufferConsumerThread = new Thread(this);
		bufferConsumerThread.start();
//		installHelloWorldQuery();
//		installHelloWorldDatabaseQuery();
		installQ0_BaseView();
		
	}
	
	
	/*  TODO Implement QueryDeployment Methods here! ===============================*/
	public void installHelloWorldQuery(){
		String statement = 	"SELECT * " +
							"FROM Datastream.Measure";
		esperEngine.installQuery(statement);
	}
	
	public void installHelloWorldDatabaseQuery(){
		String statement = 	"SELECT measureTS, measure, datapointPk, datapoint_description_fk "		+
							"FROM	Datastream.Measure, " 										+
									"sql:database ['SELECT datapoint_description_fk " 			+
									             "FROM \"DSMS_EMS_Schema\".\"DataPoint\" "		+	
									             "WHERE datapoint_pk = 82']";			
		esperEngine.installQuery(statement);
	}
	
	
	public void installQ0_BaseView(){
		
		String sqlQuery =	"SELECT  dev.device_pk 			 AS device_pk, "						+
        							"dpu.unit                AS measure_unit, "						+
        							"dpd.description         AS measure_description, "				+
        							"dl.location             AS device_location, "					+
        							"dl.area_m2              AS location_area_m2 "					+

        					"FROM    \"DBMS_EMS_Schema\".\"DataPoint\"            dp, "				+
        							"\"DBMS_EMS_Schema\".\"Device\"               dev, "			+
        							"\"DBMS_EMS_Schema\".\"DeviceLocation\"       dl, " 			+
        							"\"DBMS_EMS_Schema\".\"DataPointDescription\" dpd, "			+
        							"\"DBMS_EMS_Schema\".\"DataPointUnit\"        dpu "				+
        					"WHERE   ${datapointPk} = dp.datapoint_pk " 							+
        						"AND dp.device_fk = dev.device_pk " 								+
        						"AND dev.device_location_fk = dl.device_location_pk  "				+
        						"AND dp.datapoint_description_fk = dpd.datapoint_description_pk " 	+ 
        						"AND dp.datapoint_unit_fk = dpu.datapoint_unit_pk ";			  	/*+
        						"AND (dpd.description::text   = 'Phase1_EnergyConsumption'::text " 	+
        							"OR dpd.description::text = 'Phase2_EnergyConsumption'::text " 	+
        							"OR dpd.description::text = 'Phase3_EnergyConsumption'::text)";*/
		
		String statement = 	"SELECT rel.device_location "							+
							"FROM	Datastream.Measure, " 				+
									"sql:database ['"+sqlQuery+"'] AS rel";
        					
		
		
		
		
		esperEngine.installQuery(statement);
	}
	
	
	/* EOF Implement QueryDeployment Methods ===============================*/
	 
	
	
	
	
	/*
	 * SimulatorClient's Interface Implementation
	 * And Producer and consumer Functions
	 */
	
	private synchronized void processConsumedTuple(EnergyMeasureTupleDTO tuple){
		List<Measure> datastreamTuples = inputAdapter(tuple);
		for(Measure m : datastreamTuples){
			System.out.println("Pushing into Esper's engine -> "+m+"\n");
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
