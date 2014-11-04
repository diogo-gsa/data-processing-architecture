package Datastream;

/*
 * @author Diogo Anjos (diogo.silva.anjos@tecnico.ulisboa.pt)
 */

public class Measure{

	// Unlike the DBMS, the DSMS will NOT use this timestamp.
	// We only keep TS to maintain the consistency between the two solution implementations. 
	private String measureTS;
	private double measure;
	private int datapointPk;
	
	public Measure(String ts, double measure, int datapoint_pk){
		this.measureTS = ts;
		this.measure = measure;
		this.datapointPk = datapoint_pk;
	}
	
	public String getMeasureTS(){
		return measureTS;
	}
	
	public double getMeasure(){
		return measure;
	}
	
	public int getDatapointPk(){
		return datapointPk;
	}
	
    @Override
	public String toString() {
		return "Datastream.Measure:<ts="+measureTS+", measure="+measure+", datapoint_pk="+datapointPk+">";
	}
	
	
}
