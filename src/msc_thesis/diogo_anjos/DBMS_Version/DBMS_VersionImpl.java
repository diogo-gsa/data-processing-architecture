package msc_thesis.diogo_anjos.DBMS_Version;

import java.util.LinkedList;

import msc_thesis.diogo_anjos.simulator.EnergyMeasureTupleDTO;
import msc_thesis.diogo_anjos.simulator.EnergyMeter;
import msc_thesis.diogo_anjos.simulator.SimulatorClient;

public class DBMS_VersionImpl implements SimulatorClient, Runnable {


	private DB_CRUD_Query_API dbAPI = new DB_CRUD_Query_API();
	private volatile boolean simulationHasFinished = false;
	
	
	//producerConsumerQueueOfTuples
	private LinkedList<EnergyMeasureTupleDTO> bufferOfTuples = new LinkedList<EnergyMeasureTupleDTO>(); 
	
	
	public DBMS_VersionImpl(){
		Thread bufferConsumerThread = new Thread(this);
		bufferConsumerThread.start();
	}
	
/* ==========================================================================================================
 * 											Database CRUD operation
 * ========================================================================================================*/
	
	 // INSERT a the given record into DBMS_EMS_Schema.DataPointReading
	public void insertInto_DatapointReadingTable(EnergyMeasureTupleDTO dto){
		dbAPI.insertInto_DatapointReadingTable(dto);
	}
	
	 // INSERT a the the specified specified batch of readings into DBMS_EMS_Schema.DataPointReading
	public void insertInto_DatapointReadingTable_BatchMode(String initialMeasure_ts, String finalMeasure_ts, EnergyMeter meterDBtable){
		dbAPI.insertInto_DatapointReadingTable_BatchMode(initialMeasure_ts, finalMeasure_ts, meterDBtable);
	} 
	
	 //  TRUNCATE ALL records from table DBMS_EMS_Schema.DataPointReading
	public void truncateAll_DatapointReadingTable(){
		dbAPI.truncateAll_DatapointReadingTable();
	}
	
	 //  DELETE from DBMS_EMS_Schema.DataPointReading the (unique) record 
	 //  that match the measure_timestamp AND the datapoint_pk  
	public void deleteSpecificRow_DatapointReadingTable(String measure_ts, int datapoint_pk){
		dbAPI.deleteSpecificRow_DatapointReadingTable(measure_ts, datapoint_pk);
	}
	
	//   DELETE from DBMS_EMS_Schema.DataPointReading the records 
	//   that match the initialMeasure_ts <= measure_timestamp <= finalMeasure_ts AND the datapoint_pk  
	public void deleteSpecificInterval_DatapointReadingTable(String initialMeasure_ts, String finalMeasure_ts , int datapoint_pk){
		dbAPI.deleteSpecificInterval_DatapointReadingTable(initialMeasure_ts, finalMeasure_ts, datapoint_pk);
	}
	
	 //  DELETE from DBMS_EMS_Schema.DataPointReading the records 
	 //  that match the initialMeasure_ts <= measure_timestamp <= finalMeasure_ts  
	public void deleteSpecificInterval_DatapointReadingTable(String initialMeasure_ts, String finalMeasure_ts){
		dbAPI.deleteSpecificInterval_DatapointReadingTable(initialMeasure_ts, finalMeasure_ts);
	}
/* EOF Database CRUD operation ============================================================================*/

	
	
/* ==========================================================================================================
 * 										Data Integration and Evaluation Queries
 * ========================================================================================================*/	
	public QueryEvaluationReport executeEvaluationQuery_Q11_NoWindows_10min(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q11_NoWindows_10min();
		return report;
	}

	public QueryEvaluationReport executeEvaluationQuery_Q11_NoWindows_60min(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q11_NoWindows_60min();
		return report;
	}
	
	public QueryEvaluationReport executeEvaluationQuery_Q11_SizeWindows_10min(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q11_SizeWindows_10min();
		return report;
	}
	
	public QueryEvaluationReport executeEvaluationQuery_Q11_SizeWindows_60min(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q11_SizeWindows_60min();
		return report;
	}
	
	public QueryEvaluationReport executeEvaluationQuery_Q11_TimeWindows_10min(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q11_TimeWindows_10min();
		return report;
	}
	
	public QueryEvaluationReport executeEvaluationQuery_Q11_TimeWindows_60min(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q11_TimeWindows_60min();
		return report;
	}
	
	public QueryEvaluationReport executeIntegrationQuery_Q8_7_10minAVG_NoWindowOp(){
		QueryEvaluationReport report = dbAPI.executeIntegrationQuery_Q8_7_10minAVG_NoWindowOperator();
		return report;
	}
	
	public QueryEvaluationReport executeIntegrationQuery_Q8_7_10minAVG_WindowOp(){
		QueryEvaluationReport report = dbAPI.executeIntegrationQuery_Q8_7_10minAVG_WindowOperator();
		return report;
	}
	
	public QueryEvaluationReport executeIntegrationQuery_Q9_Percentage(){
		QueryEvaluationReport report = dbAPI.executeIntegrationQuery_Q9_Percentage();
		return report;
	}
	
	public QueryEvaluationReport executeIntegrationQuery_Q10_SortedMeasures(){
		QueryEvaluationReport report = dbAPI.executeIntegrationQuery_Q10_SortedMeasures();
		return report;
	}
/* EOF Data Integration and Evaluation Queries ==============================================================*/
	

	
/*=========================================================================================================== 
 * 			Push Datastream into DBMS and Execute Data Integration/Evaluation Queries 
 *=========================================================================================================*/	
	private synchronized void processConsumedTuple(EnergyMeasureTupleDTO tuple){
		System.out.println("Received: "+tuple); //DEBUG
		this.insertInto_DatapointReadingTable(tuple);

		// Execute QUERY
		QueryEvaluationReport report = this.executeIntegrationQuery_Q10_SortedMeasures();
		System.out.println(report);	
	}
/* EOF Push Datastream and Queries execution ==============================================================*/
	
	
	
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
		}
	}
	
	@Override
	public void run() {
		consumeTuple();
	}

	@Override
	public synchronized void simulationHasFinishedNotification() {
		simulationHasFinished = true;
		notifyAll(); //wake up waiting threads so they can check the flag
	}
/* EOF Producer and Consumer Buffer Code ====================================================*/

	
	
}
