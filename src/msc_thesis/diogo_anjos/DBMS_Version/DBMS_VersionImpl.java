package msc_thesis.diogo_anjos.DBMS_Version;

import msc_thesis.diogo_anjos.simulator.EnergyMeasureTupleDTO;
import msc_thesis.diogo_anjos.simulator.SimulatorClient;

public class DBMS_VersionImpl implements SimulatorClient {

	
	
	

	
	/*
	 * SimulatorClient's Interface Implementation
	 */
	@Override
	public void receiveDatastream(EnergyMeasureTupleDTO tuple) {
		System.out.println("Received: "+tuple);
	}

}
