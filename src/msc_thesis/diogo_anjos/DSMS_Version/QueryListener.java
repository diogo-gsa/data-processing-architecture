package msc_thesis.diogo_anjos.DSMS_Version;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

/*
 * @author Diogo Anjos (diogo.silva.anjos@tecnico.ulisboa.pt)
 */

public class QueryListener implements UpdateListener {

    private QueryMetadata qMD;    
    
    
    public QueryListener(QueryMetadata metadata) {
        qMD = metadata;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
    	if (newEvents != null) {
            printOutput(newEvents,"NEW");
        }
        if (oldEvents != null) {
            printOutput(oldEvents,"OLD");
        }
    }
    
    private void printOutput(EventBean[] events, String typeOfEvent){
        String res = "Query with id=" + qMD.getQueryID() + " OUTPUT " + typeOfEvent + " Events:";
        for (EventBean eb : events) {
            res += "\n| " + eb.getUnderlying();
        }
        System.out.println(res);
    }       
}
