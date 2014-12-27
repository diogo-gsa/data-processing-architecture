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
//		execute_DSMS_experiment();
		execute_DBMS_experiment();	
	}	
	
	
	public static void execute_DSMS_experiment() throws Exception{
		// Prepare DSMS	===========================================================
		DSMS_VersionImpl dsms_versionImpl = new DSMS_VersionImpl();
		
		//  Prepare Simulator  ====================================================
		Simulator simLIB 		= new SimulatorImpl(EnergyMeter.LIBRARY, 		"2014-05-21  00:25:00", "2014-05-21  09:35:00");			
		Simulator simA4 		= new SimulatorImpl(EnergyMeter.LECTUREHALL_A4, "2014-05-21  00:25:00", "2014-05-21  09:35:00");			
//		Simulator simA5 		= new SimulatorImpl(EnergyMeter.LECTUREHALL_A5, "2014-07-04  00:00:00", "2014-08-03  00:00:00");			
//		Simulator sim1_17 		= new SimulatorImpl(EnergyMeter.CLASSROOM_1_17, "2014-07-04  00:00:00", "2014-08-03  00:00:00");			
//		Simulator sim1_19 		= new SimulatorImpl(EnergyMeter.CLASSROOM_1_19, "2014-07-04  00:00:00", "2014-08-03  00:00:00");			
//		Simulator simDEPT_14 	= new SimulatorImpl(EnergyMeter.DEPARTMENT_14, 	"2014-07-04  00:00:00", "2014-08-03  00:00:00");			
//		Simulator simDEPT_16 	= new SimulatorImpl(EnergyMeter.DEPARTMENT_16,	"2014-07-04  00:00:00", "2014-08-03  00:00:00");			
//		Simulator simMIT_LAB 	= new SimulatorImpl(EnergyMeter.LAB_1_58_MIT, 	"2014-07-04  00:00:00", "2014-08-03  00:00:00");			
		
		simLIB.setSpeedTimeFactor(1000);
		simA4.setSpeedTimeFactor(1000);
//		simA5.setSpeedTimeFactor(8000);
//		sim1_17.setSpeedTimeFactor(8000);
//		sim1_19.setSpeedTimeFactor(8000);
//		simDEPT_14.setSpeedTimeFactor(8000);
//		simDEPT_16.setSpeedTimeFactor(8000);
//		simMIT_LAB.setSpeedTimeFactor(8000);

		// Init Simulation  ====================================================
		simLIB.registerNewClient(dsms_versionImpl); 	simLIB.start();
		simA4.registerNewClient(dsms_versionImpl); 		simA4.start();
//		simA5.registerNewClient(dsms_versionImpl); 		simA5.start();
//		sim1_17.registerNewClient(dsms_versionImpl); 	sim1_17.start();
//		sim1_19.registerNewClient(dsms_versionImpl); 	sim1_19.start();
//		simDEPT_14.registerNewClient(dsms_versionImpl); simDEPT_14.start();
//		simDEPT_16.registerNewClient(dsms_versionImpl); simDEPT_16.start();
//		simMIT_LAB.registerNewClient(dsms_versionImpl); simMIT_LAB.start();
	}

	public static void execute_DBMS_experiment() throws Exception{
		// Prepare Database  ====================================================
		DBMS_VersionImpl dbms_versionImpl = new DBMS_VersionImpl();
		dbms_versionImpl.truncateAll_DatapointReadingTable();
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17 05:00:00", "2014-03-17 06:59:30", EnergyMeter.LIBRARY); 		
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17 07:00:00", "2014-03-17 07:10:06", EnergyMeter.LECTUREHALL_A4); 	
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17 07:00:00", "2014-03-17 07:10:06", EnergyMeter.LECTUREHALL_A5); 	
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17 07:00:00", "2014-03-17 07:10:06", EnergyMeter.CLASSROOM_1_17); 	
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17 07:00:00", "2014-03-17 07:10:06", EnergyMeter.CLASSROOM_1_19);
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17 07:00:00", "2014-03-17 07:10:06", EnergyMeter.DEPARTMENT_14);
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17 07:00:00", "2014-03-17 07:10:06", EnergyMeter.DEPARTMENT_16);
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17 07:00:00", "2014-03-17 07:10:06", EnergyMeter.LAB_1_58_MIT);
				
		//  Prepare Simulator  ====================================================
		Simulator simLIB 		= new SimulatorImpl(EnergyMeter.LIBRARY, 		"2014-03-17  00:00:00", "2014-03-17  23:59:00");				
		Simulator simA4 		= new SimulatorImpl(EnergyMeter.LECTUREHALL_A4, "2014-03-17  00:00:00", "2014-03-17  23:59:00");
		Simulator simA5 		= new SimulatorImpl(EnergyMeter.LECTUREHALL_A5, "2014-03-17  00:00:00", "2014-03-17  23:59:00");
		Simulator sim1_17 		= new SimulatorImpl(EnergyMeter.CLASSROOM_1_17, "2014-03-17  00:00:00", "2014-03-17  23:59:00");
		Simulator sim1_19 		= new SimulatorImpl(EnergyMeter.CLASSROOM_1_19, "2014-03-17  00:00:00", "2014-03-17  23:59:00");
		Simulator simDEPT_14 	= new SimulatorImpl(EnergyMeter.DEPARTMENT_14, 	"2014-03-17  00:00:00", "2014-03-17  23:59:00");
		Simulator simDEPT_16 	= new SimulatorImpl(EnergyMeter.DEPARTMENT_16, 	"2014-03-17  00:00:00", "2014-03-17  23:59:00");
		Simulator simMIT_LAB 	= new SimulatorImpl(EnergyMeter.LAB_1_58_MIT,	"2014-03-17  00:00:00", "2014-03-17  23:59:00");
		
		simLIB.setSpeedTimeFactor(2000); 	 System.out.println(simLIB);
		simA4.setSpeedTimeFactor(2000);		 System.out.println(simA4);
		simA5.setSpeedTimeFactor(2000); 	 System.out.println(simA5);
		sim1_17.setSpeedTimeFactor(2000);	 System.out.println(sim1_17);
		sim1_19.setSpeedTimeFactor(2000); 	 System.out.println(sim1_19);
		simDEPT_14.setSpeedTimeFactor(2000); System.out.println(simDEPT_14);
		simDEPT_16.setSpeedTimeFactor(2000); System.out.println(simDEPT_16);
		simMIT_LAB.setSpeedTimeFactor(2000); System.out.println(simMIT_LAB);
		
		
		// Init Simulation  ====================================================
		simLIB.registerNewClient(dbms_versionImpl); 	simLIB.start();
		simA4.registerNewClient(dbms_versionImpl); 		simA4.start();
		simA5.registerNewClient(dbms_versionImpl); 		simA5.start();
		sim1_17.registerNewClient(dbms_versionImpl); 	sim1_17.start();
		sim1_19.registerNewClient(dbms_versionImpl); 	sim1_19.start();
		simDEPT_14.registerNewClient(dbms_versionImpl); simDEPT_14.start();
		simDEPT_16.registerNewClient(dbms_versionImpl); simDEPT_16.start();
		simMIT_LAB.registerNewClient(dbms_versionImpl); simMIT_LAB.start();
	}

}
