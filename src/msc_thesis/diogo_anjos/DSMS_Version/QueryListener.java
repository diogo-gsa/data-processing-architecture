package msc_thesis.diogo_anjos.DSMS_Version;

/*
 * @author Diogo Anjos (diogo.silva.anjos@tecnico.ulisboa.pt)
 * MScThesis Solution:  Real-Time Data Processing Architecture 
 * 						for Energy Management Applications
 */

import java.util.Map;
import java.util.TreeMap;

import msc_thesis.diogo_anjos.util.DataPoint_PK;
import Datastream.Measure;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class QueryListener implements UpdateListener {
	// DUMP Configuration Flags ======================================
		private boolean DUMP_ELAPSED_TIME = true;
		private boolean DUMP_QUERY_RESULT = false;
	//=================================================================

    // contains all events that entered in engine but had not yet left the engine
    // Key = device_pk +"$"+measureTS e.g "1$2014-03-17 00:05:04"
    // Value = system unix timestamp e.g 1423917410 
    // rational: tuple of devicePK=1 with measureTS=2014-03-17 00:05:04 entered the engine at 1423917410.
    private Map<String,Long> inputEventsMapForElapsedTime;
    private QueryMetadata qMD;
    private long processedTuples;
    
    
    public QueryListener(QueryMetadata metadata) {
    	inputEventsMapForElapsedTime = new TreeMap<String, Long>();
        qMD = metadata;
        processedTuples = 0;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
    	if (newEvents != null) {
            printOutputAndComputeElapsedTime(newEvents,"NEW");
        }
        if (oldEvents != null) {
            printOutputAndComputeElapsedTime(oldEvents,"OLD");
        }
    }
    
    private void printOutputAndComputeElapsedTime(EventBean[] events, String typeOfEvent){
    	if(DUMP_ELAPSED_TIME){
//	    	dumpInputEventslog();
	    	computeElapsedTime(events);
//	    	dumpInputEventslog();
    	}   	
    	String res = "QId:"+qMD.getQueryID()+" OUTPUT " + typeOfEvent +" Events";
    	if(DUMP_QUERY_RESULT){
    		for (EventBean eb : events) {
    			res += "\n| " + eb.getUnderlying();
    		}
    	  System.out.println(res);
    	}
        
    }

    private synchronized void computeElapsedTime(EventBean[] events){
    	long endTS = System.nanoTime();
    	long elapsedTime;    	
    	int matchCounter = 0;
    	//Check all Rows from ResultSet and compare them with InputEventLog
    	//for the unique founded match--there must always be one match--compute the elapsed time
    	//and remove this pair from log
    	for (EventBean eb : events) {
    		String devicePK$measureTS_key = Long.toString(((Long) eb.get("device_pk"))) + "$" + eb.get("measure_timestamp");
    		if(inputEventsMapForElapsedTime.containsKey(devicePK$measureTS_key)){
    			long beginTS = inputEventsMapForElapsedTime.get(devicePK$measureTS_key);
    			elapsedTime = endTS - beginTS;
    			inputEventsMapForElapsedTime.remove(devicePK$measureTS_key);
    			if(matchCounter==0){
    				//System.out.println("ScenarioLatency="+nanoToMilliSeconds(elapsedTime)+"ms ("+devicePK$measureTS_key+") | ProcessedTuples = "+processedTuples); // debug
    				System.out.println("ScenarioLatency="+nanoToMilliSeconds(elapsedTime)+"ms"
    								  +" | ProcessedTuples="+processedTuples
    								  +" | ProcessedMeasurements="+(processedTuples/3)
    								  );
    				matchCounter++;
    			}else{
    				System.err.println("??ERROR?? - Have found more than 1 match between ElapsedTimeLog and output resultset rows, is that normal?");
    				System.err.println("ET = "+nanoToMilliSeconds(elapsedTime)+" ms : "+devicePK$measureTS_key);
    			}
    		}
		}
    }
    
    
	public synchronized void logNewInputEvent(Measure event) {
		int device_pk = DataPoint_PK.getDevice_PKByDatapoint_PK(event.getDatapointPk());
		String measureTS = event.getMeasureTS();
		String devicePK$measureTS_key = device_pk +"$"+measureTS;
		long beginTS_value = System.nanoTime();
		if(!inputEventsMapForElapsedTime.containsKey(devicePK$measureTS_key)){
			inputEventsMapForElapsedTime.put(devicePK$measureTS_key, beginTS_value);
		}
		processedTuples++;
	}       
	
	public synchronized void dumpInputEventslog(){
		System.out.println("=== Dump InputEventsLog ===");
		for(String key : inputEventsMapForElapsedTime.keySet()){
			System.out.println(key+" | "+inputEventsMapForElapsedTime.get(key));
		}
		System.out.println("EOD");
	}

	private double nanoToMilliSeconds(long nanoValue){
		// 1 nanoSecond / (10^6) = 1 milliSecond
    	// measure with nano resolution, but present the result in milliseconds
		return (((double)nanoValue)/((double)1000000));
	}
}
