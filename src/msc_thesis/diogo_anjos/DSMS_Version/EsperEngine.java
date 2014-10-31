package msc_thesis.diogo_anjos.DSMS_Version;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import Datastream.Measure;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EPStatementException;

/*
 * @author Diogo Anjos (diogo.silva.anjos@tecnico.ulisboa.pt)
 * 
 */

public class EsperEngine {

    EPServiceProvider   esperEngine;
    EPRuntime           engineRuntime;
    EPAdministrator     engineAdmin;
    
    int countInitializedQueries;// serves as QueryID/key in the map
    Map<Integer,QueryMetadata> queryCatalog; 
    
     
    
    public EsperEngine(){
        esperEngine = EPServiceProviderManager.getDefaultProvider();
        engineRuntime = esperEngine.getEPRuntime();
        engineAdmin = esperEngine.getEPAdministrator();
        
        queryCatalog = new TreeMap<Integer,QueryMetadata>(); 
        countInitializedQueries = 0;        
    }
       
    public void pushInput(Measure event){        
    	if(countInitializedQueries==0){
    		System.out.println("There is any query installed in the Esper's Engine");
    	}
        engineRuntime.sendEvent(event);
    }
    
    public void installQuery(String eplQueryExpression) throws EPStatementException {
        
        EPStatement queryEngineObject = engineAdmin.createEPL(eplQueryExpression);
        countInitializedQueries++; //get queryID        
        QueryMetadata qmd = new QueryMetadata(countInitializedQueries, eplQueryExpression, queryEngineObject);
        
        QueryListener listener = new QueryListener(qmd);
        queryEngineObject.addListener(listener);
            
        queryCatalog.put(qmd.getQueryID(), qmd);        
    }          

    
    public boolean dropQuery(int queryID){
        try{ 
            queryCatalog.get(queryID).destroyQuery();
            queryCatalog.remove(queryID);
            return true;
        }catch(NullPointerException | ClassCastException e){
            System.out.println("Error: Query with the id="+queryID+" does not exist");
            return false;            
        }
    }
    
    public int dropAllQueries(){
        int droppedQueries = 0;
        try{           
            List<Integer>  keyset = new ArrayList<Integer>(queryCatalog.keySet());
            for(int queryID : keyset){
                queryCatalog.get(queryID).destroyQuery();
                queryCatalog.remove(queryID);
                droppedQueries++;
            }
        }catch(Exception e){
            System.out.println("Error: something went wrong during dropAllQueries Commmand");
        }
        return droppedQueries;
    }

    public void dumpInstalledQueries(){
        System.out.println("========== Installed Queries ("+queryCatalog.keySet().size()+") ==========");
        for(int queryId : queryCatalog.keySet()){
            System.out.println(queryCatalog.get(queryId));
            System.out.println("-------------------------------------------");            
        }        
    }


}
