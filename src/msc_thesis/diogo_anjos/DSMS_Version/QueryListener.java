package msc_thesis.diogo_anjos.DSMS_Version;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

/*
 * @author Diogo Anjos (diogo.silva.anjos@tecnico.ulisboa.pt)
 */

public class QueryListener implements UpdateListener {

    private QueryMetadata qMD;    
    private EsperEngine esperEngine;
    private boolean printElapsedTime = true;
    private boolean printQueryResult = false;
    private double queryExecutionTime = 0;
    
    public QueryListener(QueryMetadata metadata, EsperEngine engine) {
        qMD = metadata;
        esperEngine = engine;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
    	
    	// 1 nanoSecond / (10^6) = 1 milliSecond
    	// measure with nano resolution, but present the result in milliseconds
    	queryExecutionTime = (double) (System.nanoTime() - esperEngine.lastPushedEventSystemTS)/1000000; 
    	
    	if (newEvents != null) {
            printOutput(newEvents,"NEW");
        }
        if (oldEvents != null) {
            printOutput(oldEvents,"OLD");
        }
    }
    
    private void printOutput(EventBean[] events, String typeOfEvent){
    	String res;
    	
    	if(printElapsedTime){
    		res = "Query with id=" + qMD.getQueryID() + " OUTPUT " + typeOfEvent + " Events, ElapsedTime = "+queryExecutionTime+" ms";
    	}
    	else{
    		res = "Query with id=" + qMD.getQueryID() + " OUTPUT " + typeOfEvent + " Events, ElapsedTime = not measured";
    	}
    	
    	if(printQueryResult){
    		for (EventBean eb : events) {
    			res += "\n| " + eb.getUnderlying();
    		}
    	}
        System.out.println(res);
    }       
}
