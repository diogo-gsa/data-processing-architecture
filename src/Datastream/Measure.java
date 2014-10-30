package Datastream;

/*
 * @author Diogo Anjos (diogo.silva.anjos@tecnico.ulisboa.pt)
 */

public class Measure{

    private String meterId; 
    private double measure;
    
    public Measure(String id,/* long ts,*/ double measure) {
        this.meterId = id;
        this.measure = measure;        
    }
    
    public double getMeasure() {
        return measure;
    }
    
    public String getMeterId() {
        return meterId;
    }

    public String toString(){        
        return "[meterId: "+meterId+" | measure: "+measure+"]";
    }

}
