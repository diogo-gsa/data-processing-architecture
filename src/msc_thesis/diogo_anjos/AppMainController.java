package msc_thesis.diogo_anjos;

import msc_thesis.diogo_anjos.DBMS_Version.DBMS_VersionImpl;
import msc_thesis.diogo_anjos.DSMS_Version.DSMS_VersionImpl;
import msc_thesis.diogo_anjos.simulator.EnergyMeter;
import msc_thesis.diogo_anjos.simulator.Simulator;
import msc_thesis.diogo_anjos.simulator.impl.SimulatorImpl;
import msc_thesis.diogo_anjos.util.AppUtil;

/*
 * @author Diogo Anjos (diogo.silva.anjos@tecnico.ulisboa.pt)
 * 
 */

public class AppMainController {
	
	public static void main(String args[]) throws Exception{

		execute_DSMS_experiment(true);
		execute_DBMS_experiment(true);
		
		doMemoryMonitoring(10);
		
	}	
	
	public static void execute_DSMS_experiment(boolean checkMemorySettings) throws Exception{
		if(checkMemorySettings){checkMemorySettings("DSMS");}
		// Prepare DSMS	===========================================================
		DSMS_VersionImpl dsms_versionImpl = new DSMS_VersionImpl();
		
		//  Prepare Simulator  ====================================================
		String beginTime = "2014-05-01  00:00:00";
		String endTime	 = "2014-07-30  00:00:00";
		int simulatorSpeedFactor = 4;
		
		Simulator simLIB 		= new SimulatorImpl(EnergyMeter.LIBRARY, 		beginTime, endTime);			
		Simulator simA4 		= new SimulatorImpl(EnergyMeter.LECTUREHALL_A4, beginTime, endTime);			
		Simulator simA5 		= new SimulatorImpl(EnergyMeter.LECTUREHALL_A5, beginTime, endTime);			
		Simulator sim1_17 		= new SimulatorImpl(EnergyMeter.CLASSROOM_1_17, beginTime, endTime);			
		Simulator sim1_19 		= new SimulatorImpl(EnergyMeter.CLASSROOM_1_19, beginTime, endTime);			
		Simulator simDEPT_14 	= new SimulatorImpl(EnergyMeter.DEPARTMENT_14, 	beginTime, endTime);			
		Simulator simDEPT_16 	= new SimulatorImpl(EnergyMeter.DEPARTMENT_16,	beginTime, endTime);			
		Simulator simMIT_LAB 	= new SimulatorImpl(EnergyMeter.LAB_1_58_MIT, 	beginTime, endTime);			
		
		simLIB.setSpeedTimeFactor(simulatorSpeedFactor);
		simA4.setSpeedTimeFactor(simulatorSpeedFactor);
		simA5.setSpeedTimeFactor(simulatorSpeedFactor);
		sim1_17.setSpeedTimeFactor(simulatorSpeedFactor);
		sim1_19.setSpeedTimeFactor(simulatorSpeedFactor);
		simDEPT_14.setSpeedTimeFactor(simulatorSpeedFactor);
		simDEPT_16.setSpeedTimeFactor(simulatorSpeedFactor);
		simMIT_LAB.setSpeedTimeFactor(simulatorSpeedFactor);

		// Init Simulation  ====================================================
		simLIB.registerNewClient(dsms_versionImpl); 	simLIB.start();
		simA4.registerNewClient(dsms_versionImpl); 		simA4.start();
		simA5.registerNewClient(dsms_versionImpl); 		simA5.start();
		sim1_17.registerNewClient(dsms_versionImpl); 	sim1_17.start();
		sim1_19.registerNewClient(dsms_versionImpl); 	sim1_19.start();
		simDEPT_14.registerNewClient(dsms_versionImpl); simDEPT_14.start();
		simDEPT_16.registerNewClient(dsms_versionImpl); simDEPT_16.start();
		simMIT_LAB.registerNewClient(dsms_versionImpl); simMIT_LAB.start();
	}

	public static void execute_DBMS_experiment(boolean checkMemorySettings) throws Exception{
		if(checkMemorySettings){checkMemorySettings("DBMS");}
		// Prepare Database  ====================================================
		DBMS_VersionImpl dbms_versionImpl = new DBMS_VersionImpl();		
		dbms_versionImpl.truncateAll_DatapointReadingTable();
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17  00:55:00", "2014-03-17  01:05:00", EnergyMeter.LIBRARY); 		
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17  00:55:00", "2014-03-17  01:05:00", EnergyMeter.LECTUREHALL_A4); 	
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17  00:55:00", "2014-03-17  01:05:00", EnergyMeter.LECTUREHALL_A5); 	
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17  00:55:00", "2014-03-17  01:05:00", EnergyMeter.CLASSROOM_1_17); 	
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17  00:55:00", "2014-03-17  01:05:00", EnergyMeter.CLASSROOM_1_19);
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17  00:55:00", "2014-03-17  01:05:00", EnergyMeter.DEPARTMENT_14);
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17  00:55:00", "2014-03-17  01:05:00", EnergyMeter.DEPARTMENT_16);
//		dbms_versionImpl.insertInto_DatapointReadingTable_BatchMode("2014-03-17  00:55:00", "2014-03-17  01:05:00", EnergyMeter.LAB_1_58_MIT);

		//  Prepare Simulator  ====================================================
		//TODO Dataset of All Night Long Tests
		String beginTime = "2014-05-01  00:00:00";
		String endTime	 = "2014-07-30  00:00:00";
		int simulatorSpeedFactor = 8000;
		// Dataset de testes normais
//		String beginTime = "2014-05-01  00:00:00"; //"2014-05-01  00:00:00"; //TODO Dataset of All Night Long Tests
//		String endTime	 = "2014-05-01  00:15:00"; //"2014-07-30  00:00:00"; //TODO Dataset of All Night Long Tests
		
		Simulator simLIB 		= new SimulatorImpl(EnergyMeter.LIBRARY, 		beginTime, endTime); 				
		Simulator simA4 		= new SimulatorImpl(EnergyMeter.LECTUREHALL_A4, beginTime, endTime); 
		Simulator simA5 		= new SimulatorImpl(EnergyMeter.LECTUREHALL_A5, beginTime, endTime);
		Simulator sim1_17 		= new SimulatorImpl(EnergyMeter.CLASSROOM_1_17, beginTime, endTime);
		Simulator sim1_19 		= new SimulatorImpl(EnergyMeter.CLASSROOM_1_19, beginTime, endTime);
		Simulator simDEPT_14 	= new SimulatorImpl(EnergyMeter.DEPARTMENT_14, 	beginTime, endTime);
		Simulator simDEPT_16 	= new SimulatorImpl(EnergyMeter.DEPARTMENT_16, 	beginTime, endTime);
		Simulator simMIT_LAB 	= new SimulatorImpl(EnergyMeter.LAB_1_58_MIT,	beginTime, endTime);

		simLIB.setSpeedTimeFactor(simulatorSpeedFactor); 	 	System.out.println(simLIB);
		simA4.setSpeedTimeFactor(simulatorSpeedFactor);		 	System.out.println(simA4);
		simA5.setSpeedTimeFactor(simulatorSpeedFactor); 		System.out.println(simA5);
		sim1_17.setSpeedTimeFactor(simulatorSpeedFactor);	 	System.out.println(sim1_17);
		sim1_19.setSpeedTimeFactor(simulatorSpeedFactor); 	 	System.out.println(sim1_19);
		simDEPT_14.setSpeedTimeFactor(simulatorSpeedFactor);  	System.out.println(simDEPT_14);
		simDEPT_16.setSpeedTimeFactor(simulatorSpeedFactor);  	System.out.println(simDEPT_16);
		simMIT_LAB.setSpeedTimeFactor(simulatorSpeedFactor);  	System.out.println(simMIT_LAB);		
		
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

	
	
	private static void doMemoryMonitoring(int monitoringPeriodInSeconds){
		System.err.println("MaxMemory:" + AppUtil.getMaxMemory() +" MB");
		while(true){
			System.err.println(AppUtil.getMemoryStatus());
			try {
				Thread.sleep(1000*monitoringPeriodInSeconds);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static void checkMemorySettings(String solution){
		int dsmsJVMlimit = 512; //512MB max memory allowed per query engine
		if(solution.equals("DSMS") && AppUtil.getMaxMemory() > dsmsJVMlimit){
			System.err.println("ERROR: DSMS is beeing started with wrong memory settings: "+AppUtil.getMaxMemory()+" MB");
			System.err.println("\t Adjust memory settings as follow: RunConfigurations >> VMarguments(heapMaxSize setting) >> add flag -Xmx512m");
			System.exit(1);
		}
		if(solution.equals("DBMS") && AppUtil.getMaxMemory() < dsmsJVMlimit){
			System.err.println("ERROR: DBMS is beeing started with wrong memory settings: "+AppUtil.getMaxMemory()+" MB");
			System.err.println("\t Adjust memory settings as follow: RunConfigurations >> VMarguments(heapMaxSize setting) >> remove flag -Xmx512m");
			System.exit(1);
		}	
	}
	
}
