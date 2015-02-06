package msc_thesis.diogo_anjos.DBMS_Version;

import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import msc_thesis.diogo_anjos.simulator.EnergyMeasureTupleDTO;
import msc_thesis.diogo_anjos.simulator.EnergyMeter;
import msc_thesis.diogo_anjos.simulator.SimulatorClient;

public class DBMS_VersionImpl implements SimulatorClient, Runnable {

	// DUMP Configuration Flags ======================================
		private boolean DUMP_PUSHED_INPUT 		= false;
		private boolean DUMP_INPUTBUFFER_LENGTH = true;
	//=================================================================
		
	private DB_CRUD_Query_API dbAPI = new DB_CRUD_Query_API();
	private volatile boolean simulationHasFinished = false;
			
	//producerConsumerQueueOfTuples
	private LinkedList<EnergyMeasureTupleDTO> bufferOfTuples = new LinkedList<EnergyMeasureTupleDTO>(); 
	
	Map<EnergyMeter, Boolean> simulationStartStopFlags = new TreeMap<EnergyMeter, Boolean>();
	
		
	public DBMS_VersionImpl(){
		Thread bufferConsumerThread = new Thread(this);
		bufferConsumerThread.start();
	}
	
/*=========================================================================================================== 
 * 			Push Datastream into DBMS and Execute Data Integration/Evaluation Queries 
 *=========================================================================================================*/	
	
//	TODO: Attention: remover *synchronized* deste metodo para que a "velocidade" com que o simulator
//			preenche o byffer seja completamente independente da velocidade do DBMS para processar esses tuplos.
//			C/ synch: SpeedTimeFactors Altos => Buffer Não enche demasiado  => tempo de simulação é muito maior do que o esperado.
//			S/ synch: SpeedTimeFactors Altos => Buffer Enche demasiado  => tempo de simulação é igual ao esperado.
	private synchronized void processConsumedTuple(EnergyMeasureTupleDTO tuple){
		
		if(DUMP_PUSHED_INPUT){
			System.out.println("Received: "+tuple); //DEBUG
		}
		this.insertInto_DatapointReadingTable(tuple);
// ============= Query to be Executed ========================================================================= 
//		QueryEvaluationReport report = this.execute_New_Q4_VariationsAboveThreshold();
//		QueryEvaluationReport report = this.execute_New_Q5_PeriodOutOfBounds();
//		QueryEvaluationReport report = this.execute_New_Q1_ConsumptionsAboveThreshold();
//		QueryEvaluationReport report = this.execute_Q03_MinMaxConsumptionRatio();
//		QueryEvaluationReport report = this.execute_Q06_ConsumptionAboveExpected();
//		QueryEvaluationReport report = this.execute_New_Q6_DeltaAboveThreshold_WithQ14AsInput();
		QueryEvaluationReport report = this.execute_Q17_ConsumptionAboveExpectedCounter();
//		QueryEvaluationReport report = this.execute_New_Q16_CurrentConsumptions20percentAbove24hrsSlidingAvg();
//		QueryEvaluationReport report = this.execute_New_Q09_ProportionsFromConsumptions();
//		QueryEvaluationReport report = this.execute_Q10_ConsumptionsRankingList();	
//		QueryEvaluationReport report = this.execute_Q01_ConsumptionOverThreshold();
//============================================================================================================= 
		
		
		//report.dump(dumpStatement, dumpResult, dumpElapsedTime)
		System.out.println(report.dump(false, true, true));	//dumpStatement, dumpResult, dumpElapsedTime
//		System.out.println(report.dumpElapsedTime());	//dumpStatement, dumpResult, dumpElapsedTime
	}
	
/* EOF Push Datastream and Queries execution ==============================================================*/
	
	
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
 * 										Data Evaluation Queries
 * ========================================================================================================*/	
	
	public QueryEvaluationReport execute_New_Q4_VariationsAboveThreshold(){
		QueryEvaluationReport report = dbAPI.execute_Q04_InstantVariationAboveThreshold();
		return report;
	}
	
	public QueryEvaluationReport execute_New_Q5_PeriodOutOfBounds(){
		QueryEvaluationReport report = dbAPI.execute_Q05_StreamPeriodicityOutOfRange();
		return report;
	}
	
	public QueryEvaluationReport execute_New_Q09_ProportionsFromConsumptions(){
		QueryEvaluationReport report = dbAPI.execute_Q09_ProportionsFromConsumptions();
		return report;
	}
	
	public QueryEvaluationReport execute_Q10_ConsumptionsRankingList(){
		QueryEvaluationReport report = dbAPI.execute_Q10_ConsumptionsRankingList();
		return report;
	}
	
	public QueryEvaluationReport execute_Q01_ConsumptionOverThreshold(){
		QueryEvaluationReport report = dbAPI.execute_Q01_ConsumptionOverThreshold();
		return report;
	}
	
	public QueryEvaluationReport execute_Q03_MinMaxConsumptionRatio(){
		QueryEvaluationReport report = dbAPI.execute_Q03_MinMaxConsumptionRatio();
		return report;
	}
	
	public QueryEvaluationReport execute_Q06_ConsumptionAboveExpected(){
		QueryEvaluationReport report = dbAPI.execute_Q06_ConsumptionAboveExpected();
		return report;
	}
	
	public QueryEvaluationReport execute_New_Q6_DeltaAboveThreshold_WithQ14AsInput(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_New_Q6_DeltaAboveThreshold_WithQ14AsInput();
		return report;
	}
	
	
	public QueryEvaluationReport execute_Q17_ConsumptionAboveExpectedCounter(){
		QueryEvaluationReport report = dbAPI.execute_Q17_ConsumptionAboveExpectedCounter();
		return report;
	}
	
	
	public QueryEvaluationReport execute_New_Q16_CurrentConsumptions20percentAbove24hrsSlidingAvg(){
		QueryEvaluationReport report = dbAPI.execute_Q16_ConsumptionAboveSlidingAvgThreshold();
		return report;
	}
	
	
// =================================== DEPRECATED FUNCTIONS ==================================
	
	@Deprecated
	public QueryEvaluationReport execute_New_Q12_PeriodBetweenDatastreamTuples(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q12_PeriodBetweenDatastreamTuples();
		return report;
	}

	@Deprecated
	public QueryEvaluationReport executeIntegrationQuery_New_Q11_ConsumptionsVariationOverLast5min(){
		QueryEvaluationReport report = dbAPI.executeIntegrationQuery_New_Q11_ConsumptionsVariationOverLast5min();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeEvaluationQuery_Q11_NoWindows_10min(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q11_NoWindows_10min();
		return report;
	}

	@Deprecated
	public QueryEvaluationReport executeEvaluationQuery_Q11_NoWindows_60min(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q11_NoWindows_60min();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeEvaluationQuery_Q11_SizeWindows_10min(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q11_SizeWindows_10min();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeEvaluationQuery_Q11_SizeWindows_60min(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q11_SizeWindows_60min();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeEvaluationQuery_Q11_TimeWindows_10min(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q11_TimeWindows_10min();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeEvaluationQuery_Q11_TimeWindows_60min(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q11_TimeWindows_60min();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeIntegrationQuery_Q8_7_10minAVG_NoWindowOp(){
		QueryEvaluationReport report = dbAPI.executeIntegrationQuery_Q8_7_10minAVG_NoWindowOperator();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeIntegrationQuery_Q8_7_10minAVG_WindowOp(){
		QueryEvaluationReport report = dbAPI.executeIntegrationQuery_Q8_7_10minAVG_WindowOperator();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeIntegrationQuery_Q9_Percentage(){
		QueryEvaluationReport report = dbAPI.executeIntegrationQuery_Q9_Percentage();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeIntegrationQuery_Q10_SortedMeasures(){
		QueryEvaluationReport report = dbAPI.executeIntegrationQuery_Q10_SortedMeasures();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeIntegrationQuery_Q12_DeltaBetweenTuples(){
		QueryEvaluationReport report = dbAPI.executeIntegrationQuery_Q12_DeltaBetweenTuples();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeEvaluationQuery_Q5_DeltaBetweenTuplesOverThreashold(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q5_DeltaBetweenTuplesOverThreashold();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeEvaluationQuery_Q3_MinMaxRatio(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q3_MinMaxRatio();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeEvaluationQuery_Q1_AllAndEachDevicesNormalizedConsumptionOverThreshold(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q1_BuildingNormalizedConsumptionOverThreshold();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeEvaluationQuery_Q16_MeasuresPercentHigherThanAverageThresold(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q16_MeasuresPercentHigherThanAverageThresold();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeIntegrationQuery_Q14_CurrentAndExpectedUDFMeasure(){
		QueryEvaluationReport report = dbAPI.executeIntegrationQuery_Q14_CurrentAndExpectedUDFMeasure();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeIntegrationQuery_Q13_CurrentAndExpectedHourClusterMeasure(){
		QueryEvaluationReport report = dbAPI.executeIntegrationQuery_Q13_CurrentAndExpectedHourClusterMeasure();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeEvaluationQuery_Q6_withQ13AsInput_CurrentAndExpectedConsumptionAboveGivenPercentage(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q6_withQ13AsInput_CurrentAndExpectedConsumptionAboveGivenPercentage();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeEvaluationQuery_Q6_withQ14AsInput_CurrentAndExpectedConsumptionAboveGivenPercentage(){
		QueryEvaluationReport report = dbAPI.executeEvaluationQuery_Q6_withQ14AsInput_CurrentAndExpectedConsumptionAboveGivenPercentage();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeIntegrationQuery_New_Q9_FractionateConsumptions(){
		QueryEvaluationReport report = dbAPI. executeIntegrationQuery_New_Q9_FractionateConsumptions();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeIntegrationQuery_New_Q10_OrderByConsumptions(){
		QueryEvaluationReport report = dbAPI. executeIntegrationQuery_New_Q10_OrderByConsumptions();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport executeIntegrationQuery_New_Q14_DeltaBetweenCurrentConsumptionAndUDFBasedPrediction(){
		QueryEvaluationReport report = dbAPI.executeIntegrationQuery_New_Q14_DeltaBetweenCurrentConsumptionAndUDFBasedPrediction();
		return report;
	}
	
	@Deprecated
	public QueryEvaluationReport execute_New_Q13_DeltaBetweenCurrentConsumptionAndLastMonthBasedPrediction(){
		QueryEvaluationReport report = dbAPI.executeIntegrationQuery_New_Q13_DeltaBetweenCurrentConsumptionAndLastMonthBasedPrediction();
		return report;
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
	public void run() {
		consumeTuple();
	}

	
	@Override
	public void simulationHasStartedNotification(EnergyMeter em) {
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
	
/* EOF Producer and Consumer Buffer Code ====================================================*/
}
