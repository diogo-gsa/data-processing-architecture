package msc_thesis.diogo_anjos.DSMS_Version;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import Datastream.Measure;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.ConfigurationDBRef;
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

    private EPServiceProvider   esperEngine;
    private EPRuntime           engineRuntime;
    private EPAdministrator     engineAdmin;
    
    private int countInitializedQueries;// serves as QueryID/key in the map
    private Map<Integer,QueryMetadata> queryCatalog; 
    
    //public field (to be accesible by the listeners) to measure elapse time 
    // between the time that the engine takes to evaluate an pushed event
    // TODO:Esta solução apenas funionas para cliente isolados,
    //		i.e. se existirem dois sensores a enviar measures para o engine
    //		esta abordagem deixa de funionar. Nessa altura tem de se passar 
    //		para um Map<DatapointReadingPK:key, TS:value> 
    public long lastPushedEventSystemTS = 0;
     
    
    public EsperEngine(){ 
        ConfigurationDBRef dbConfig = new ConfigurationDBRef();
        //configure Esper's connection to lumina_db to enable join streams with static data
        dbConfig.setDriverManagerConnection("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/lumina_db", "postgres", "root");
        
        Configuration engineConfig = new Configuration();
        engineConfig.addDatabaseReference("database", dbConfig);
  
        esperEngine = EPServiceProviderManager.getDefaultProvider(engineConfig); 
  
        engineRuntime = esperEngine.getEPRuntime();
        engineAdmin = esperEngine.getEPAdministrator();
        
        queryCatalog = new TreeMap<Integer,QueryMetadata>(); 
        countInitializedQueries = 0;        
    }
       
    public void pushInput(Measure event){        
    	if(countInitializedQueries==0){
    		System.out.println("There is any query installed in the Esper's Engine");
    	}
    	lastPushedEventSystemTS = System.nanoTime();
        engineRuntime.sendEvent(event);
    }
    
    public void installQuery(String eplQueryExpression, boolean addListener) throws EPStatementException {
        
        EPStatement queryEngineObject = engineAdmin.createEPL(eplQueryExpression);
        countInitializedQueries++; //get queryID        
        QueryMetadata qmd = new QueryMetadata(countInitializedQueries, eplQueryExpression, queryEngineObject);
        
        // I may don't want to listen the output of integration/intermediate queries 
        if(addListener){
        	QueryListener listener = new QueryListener(qmd, this);
        	queryEngineObject.addListener(listener);
        }
            
        queryCatalog.put(qmd.getQueryID(), qmd);
        System.out.println("Query: "+ eplQueryExpression + " installed\n");
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
