package Datastream;

/*
 * @author Diogo Anjos (diogo.silva.anjos@tecnico.ulisboa.pt)
 */

public class Measure{

	// Unlike the DBMS, the DSMS will NOT use this timestamp.
	// We only keep TS to maintain the consistency between the two solution implementations. 
	private String measure_ts;
	private double measure;
	private int datapoint_pk;
	
	public Measure(String measure_ts, double measure, int datapoint_pk){
		this.measure_ts = measure_ts;
		this.measure = measure;
		this.datapoint_pk = datapoint_pk;
	}
	
	public String getMeasureTS(){
		return measure_ts;
	}
	
	public double getMeasure(){
		return measure;
	}
	
	public int getDatapointPK(){
		return datapoint_pk;
	}
	
    @Override
	public String toString() {
		return "Datastream.Measure:<ts="+measure_ts+", measure="+measure+", datapoint_pk="+datapoint_pk+">";
	}
	
	
}
