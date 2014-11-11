package msc_thesis.diogo_anjos;

import msc_thesis.diogo_anjos.DBMS_Version.DBMS_VersionImpl;
import msc_thesis.diogo_anjos.DSMS_Version.DSMS_VersionImpl;
import msc_thesis.diogo_anjos.simulator.EnergyMeter;
import msc_thesis.diogo_anjos.simulator.Simulator;
import msc_thesis.diogo_anjos.simulator.impl.SimulatorImpl;

/*
 * @author Diogo Anjos (diogo.silva.anjos@tecnico.ulisboa.pt)
 * 
 */

public class AppMainController {

	public static void main(String args[]) throws Exception{
		execute_DSMS_experiment();
//		execute_DBMS_experiment();
	}	
	
	
	public static void execute_DSMS_experiment() throws Exception{
		// Prepare DSMS	===========================================================
		DSMS_VersionImpl dsms_versionImpl = new DSMS_VersionImpl();
		
		//  Prepare Simulator  ====================================================
		Simulator simulatorBib = new SimulatorImpl(EnergyMeter.LIBRARY, "2014-03-17  12:01:05", "2014-03-17  13:20:05");		//60min
		
		// Simulator simulatorLibrary = new SimulatorImpl(EnergyMeter.LIBRARY, "2014-03-19 10:01:00", "2014-03-19 10:10:05");		//48h
		simulatorBib.setSpeedTimeFactor(1);
		System.out.println(simulatorBib);
			
		// Init Simulation  ====================================================
		simulatorBib.registerNewClient(dsms_versionImpl);
		simulatorBib.start();
	}

	public static void execute_DBMS_experiment() throws Exception{
		// Prepare Database  ====================================================
		DBMS_VersionImpl dbms_versionImpl = new DBMS_VersionImpl(); 
		dbms_versionImpl.truncateAll_DatapointReadingTable();
		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17 10:00:00", "2014-03-17 12:00:05", EnergyMeter.LIBRARY); 	// 2h
		// dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17 10:00:00", "2014-03-19 10:00:06", EnergyMeter.LIBRARY); 	// 48h
				
		//  Prepare Simulator  ====================================================
		Simulator simulator = new SimulatorImpl(EnergyMeter.LIBRARY, "2014-03-17  12:01:05", "2014-03-17  12:10:05");				//2h
		//Simulator simulatorLibrary = new SimulatorImpl(EnergyMeter.LIBRARY, "2014-03-19 10:01:00", "2014-03-19 10:10:05");				//48h		
		simulator.setSpeedTimeFactor(600);
		System.out.println(simulator); 
		
		// Init Simulation  ====================================================
		simulator.registerNewClient(dbms_versionImpl);
		simulator.start();
	}

}
