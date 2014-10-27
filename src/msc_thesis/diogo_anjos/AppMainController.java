package msc_thesis.diogo_anjos;

import msc_thesis.diogo_anjos.DBMS_Version.DBMS_VersionImpl;
import msc_thesis.diogo_anjos.simulator.EnergyMeter;
import msc_thesis.diogo_anjos.simulator.Simulator;
import msc_thesis.diogo_anjos.simulator.SimulatorClient;
import msc_thesis.diogo_anjos.simulator.impl.SimulatorImpl;

/*
 * @author Diogo Anjos (diogo.silva.anjos@tecnico.ulisboa.pt)
 * 
 */

public class AppMainController {

	
	public static void main(String args[]){


		DBMS_VersionImpl dbms_versionImpl = new DBMS_VersionImpl(); 
		
//		Simulator simTestTable = new SimulatorImpl(EnergyMeter.TEST_FIRST);
//		System.out.println(simTestTable);
		
//		simTestTable.registerNewClient(dbms_versionImpl);
		
//		dbms_versionImpl.truncateAll_DatapointReadingTable();
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17 10:00:00", "2014-03-17 12:00:00", EnergyMeter.LIBRARY);
		
		dbms_versionImpl.executeEvaluationQuery_Q11_NoWindows_10min();
		
		
//		simTestTable.start();

		
	}
	
	
}
