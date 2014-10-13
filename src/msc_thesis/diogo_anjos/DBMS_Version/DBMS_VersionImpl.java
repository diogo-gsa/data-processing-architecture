package msc_thesis.diogo_anjos.DBMS_Version;

import msc_thesis.diogo_anjos.simulator.EnergyMeasureTupleDTO;
import msc_thesis.diogo_anjos.simulator.SimulatorClient;

public class DBMS_VersionImpl implements SimulatorClient {


	DB_CRUD_Query_API dbAPI = new DB_CRUD_Query_API();
	
	

	
	/*
	 * SimulatorClient's Interface Implementation
	 */
	@Override
	public void receiveDatastream(EnergyMeasureTupleDTO tuple) {
		System.out.println("Received: "+tuple);
		
		//TODO DEBUG TUNCATE TABLE before TEST insertInto ----
		dbAPI.truncateAllTable_DatapointReading();
		//TODO DEBUG TUNCATE TABLE before TEST insertInto ----
		
		
		//dbAPI.insertInto_DatapointReading(tuple);
	}
	
}
