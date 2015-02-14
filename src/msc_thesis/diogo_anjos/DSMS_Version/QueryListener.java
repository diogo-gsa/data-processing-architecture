package msc_thesis.diogo_anjos.DSMS_Version;

import java.util.Map;
import java.util.TreeMap;

import msc_thesis.diogo_anjos.util.DataPoint_PK;

import Datastream.Measure;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

/*
 * @author Diogo Anjos (diogo.silva.anjos@tecnico.ulisboa.pt)
 */

public class QueryListener implements UpdateListener {

	// DUMP Configuration Flags ======================================
		private boolean DUMP_ELAPSED_TIME = true;
		private boolean DUMP_QUERY_RESULT = true;
	//=================================================================
	
    private QueryMetadata qMD;    
    
    // contains all events that entered in engine but had not yet left the engine
    // Key = device_pk +"$"+measureTS e.g "1$2014-03-17 00:05:04"
    // Value = system unix timestamp e.g 1423917410 
    // rational: tuple of devicePK=1 with measureTS=2014-03-17 00:05:04 entered the engine at 1423917410.
    private Map<String,Long> inputEventsMapForElapsedTime; 
    
    
    //TODO: estes 2 atributos vão desaparecer
    private EsperEngine esperEngine;
    private double queryExecutionTime = 0;
    
    //TODO: Listener vai deixar de receber engine pq o mapa vai estar no listener.
    public QueryListener(QueryMetadata metadata, EsperEngine engine) {
    	inputEventsMapForElapsedTime = new TreeMap<String, Long>();
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
    	computeElapsedTime(events);
    	
    	String res;
    	if(DUMP_ELAPSED_TIME){
    		res = "Query with id=" + qMD.getQueryID() + " OUTPUT " + typeOfEvent + " Events, DeprecatedElapsedTime = "+queryExecutionTime+" ms";
    	}
    	else{
    		res = "Query with id=" + qMD.getQueryID() + " OUTPUT " + typeOfEvent + " Events, DeprecatedElapsedTime = not measured";
    	}
    	
    	if(DUMP_QUERY_RESULT){
    		for (EventBean eb : events) {
//    			System.out.println("teste:"+eb.get("measure_timestamp"));
    			res += "\n| " + eb.getUnderlying();
    		}
    	}
        System.out.println(res);
    }

    private synchronized long computeElapsedTime(EventBean[] events){
    	Long elapsedTime = null;
    	long endTS = System.nanoTime();
    	boolean firstMatch = true;
    	
    	
    	//Check all Rows from ResultSet and compare them with InputEventLog
    	for (EventBean eb : events) {
    		String devicePK$measureTS_key = Long.toString(((Long) eb.get("device_pk"))) + "$" + eb.get("measure_timestamp");
    		if(inputEventsMapForElapsedTime.containsKey(devicePK$measureTS_key)){
    			Long beginTS = inputEventsMapForElapsedTime.get(devicePK$measureTS_key);
    			elapsedTime = endTS - beginTS;
    			inputEventsMapForElapsedTime.remove(devicePK$measureTS_key);
    			if(firstMatch){
    				System.out.println("True ET = "+(double)(elapsedTime/1000000)+"ms : "+devicePK$measureTS_key);
    				firstMatch = false;
    			}else{
    				System.err.println("??ERROR?? - Have found more than 1 match between logEt and output resultset rows, is that normal?");
    				System.err.println("True ET = "+(double)(elapsedTime/1000000)+"ms : "+devicePK$measureTS_key);
    			}
    		}
    		
		}
    	
    	return elapsedTime; //if (et==null) exit(1)
    }
    
    
	public synchronized void logNewInputEvent(Measure event) {
		int device_pk = DataPoint_PK.getDevice_PKByDatapoint_PK(event.getDatapointPk());
		String measureTS = event.getMeasureTS();
		
		String devicePK$measureTS_key = device_pk +"$"+measureTS;
		long beginTS_value = System.nanoTime();
		
		if(!inputEventsMapForElapsedTime.containsKey(devicePK$measureTS_key)){
			inputEventsMapForElapsedTime.put(devicePK$measureTS_key, beginTS_value);
		}
	}       
	
	public synchronized void dumpInputEventslog(){
		System.out.println("=== Dump InputEventsLog ===");
		for(String key : inputEventsMapForElapsedTime.keySet()){
			System.out.println(key+" | "+inputEventsMapForElapsedTime.get(key));
		}
		System.out.println("EOD");
	}
}
