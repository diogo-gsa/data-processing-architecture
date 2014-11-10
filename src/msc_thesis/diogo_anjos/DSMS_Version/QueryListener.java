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
    long queryExecutionTime;
    
    public QueryListener(QueryMetadata metadata, EsperEngine engine) {
        qMD = metadata;
        esperEngine = engine;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
    	
    	queryExecutionTime = System.currentTimeMillis() - esperEngine.lastPushedEventSystemTS;
    	
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
    	
    	for (EventBean eb : events) {
            res += "\n| " + eb.getUnderlying();
        }
        System.out.println(res);
    }       
}
