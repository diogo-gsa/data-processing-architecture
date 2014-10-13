package msc_thesis.diogo_anjos.DBMS_Version;

import msc_thesis.diogo_anjos.simulator.EnergyMeasureTupleDTO;
import msc_thesis.diogo_anjos.simulator.SimulatorClient;

public class DBMS_VersionImpl implements SimulatorClient {


	DB_CRUD_Query_API dbAPI = new DB_CRUD_Query_API();
	
	

	public void truncateAll_DatapointReadingTable(){
		dbAPI.truncateAll_DatapointReadingTable();
	}
		
	public void deleteSpecificRow_DatapointReadingTable(String measure_ts, int datapoint_pk){
		dbAPI.deleteSpecificRow_DatapointReadingTable(measure_ts, datapoint_pk);
	}
	
	public void deleteSpecificInterval_DatapointReadingTable(String initialMeasure_ts, String finalMeasure_ts , int datapoint_pk){
		dbAPI.deleteSpecificInterval_DatapointReadingTable(initialMeasure_ts, finalMeasure_ts, datapoint_pk);
	}
	
	/*
	 * SimulatorClient's Interface Implementation
	 */
	@Override
	public void receiveDatastream(EnergyMeasureTupleDTO tuple) {
		System.out.println("Received: "+tuple); //DEBUG
		dbAPI.insertInto_DatapointReadingTable(tuple);
	}
	
}
