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


		SimulatorClient dbms_versionImpl = new DBMS_VersionImpl();
		
		Simulator simTest = new SimulatorImpl(EnergyMeter.TEST_FIRST);
		System.out.println(simTest);
		
		simTest.registerNewClient(dbms_versionImpl);
		simTest.start();

		
	}
	
	
}
