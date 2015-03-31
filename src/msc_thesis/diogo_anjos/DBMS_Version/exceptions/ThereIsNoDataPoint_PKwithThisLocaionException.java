package msc_thesis.diogo_anjos.DBMS_Version.exceptions;

/*
 * @author Diogo Anjos (diogo.silva.anjos@tecnico.ulisboa.pt)
 * MScThesis Solution:  Real-Time Data Processing Architecture 
 * 						for Energy Management Applications
 */

public class ThereIsNoDataPoint_PKwithThisLocaionException extends Exception {

	private String errorLocation = null;
	
	public ThereIsNoDataPoint_PKwithThisLocaionException(String location){
		errorLocation = location;
	}
	
	@Override
	public String toString(){
		return "[FATAL ERROR] Location=s'"+errorLocation+"' cannot match any (Enum instance) DataPoint_PK.";
	}
}
