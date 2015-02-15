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
    private QueryListener queryScenarioListener = null;
    
    
    public EsperEngine(){ 
        ConfigurationDBRef dbConfig = new ConfigurationDBRef();
        //configure Esper's connection to lumina_db to enable join streams with static data
        dbConfig.setDriverManagerConnection("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/lumina_db", "postgres", "root");
        
        Configuration engineConfig = new Configuration();
        
        // set-up database configuration
        engineConfig.addDatabaseReference("database", dbConfig);
       
        // import auxiliar functions (to deal with TS manipulation at Q12)
        engineConfig.addImport("com.espertech.esper.client.util.DateTime");
        
        // import Single Row User Defined Function (UDF) - getExpectedMeasure(..) for Q14
        engineConfig.addPlugInSingleRowFunction("getExpectedMeasure", "msc_thesis.diogo_anjos.util.DSMS_UserDefinedFunctions", "getExpectedMeasure");
        
        esperEngine = EPServiceProviderManager.getDefaultProvider(engineConfig); 
  
        engineRuntime = esperEngine.getEPRuntime();
        engineAdmin = esperEngine.getEPAdministrator();
        
        queryCatalog = new TreeMap<Integer,QueryMetadata>(); 
        countInitializedQueries = 0;        
    }
       
    public void pushInput(Measure event){        
    	if(countInitializedQueries==0){
    		System.out.println("There is any query installed in the Esper Engine");
    	}
    	
    	queryScenarioListener.logNewInputEvent(event);
    	engineRuntime.sendEvent(event);

    }
    
    public void installQuery(String eplQueryExpression, boolean addListener) throws EPStatementException {
        
        EPStatement queryEngineObject = engineAdmin.createEPL(eplQueryExpression);
        countInitializedQueries++; //get queryID        
        QueryMetadata qmd = new QueryMetadata(countInitializedQueries, eplQueryExpression, queryEngineObject);
        
        // We just want to have *ONE* listener per scenario (for ET measurement purposes), then we have to choose is which 
        // query (that composes the scenario) will the listener be installed.    
        if(addListener){
        	if(queryScenarioListener == null){
        		queryScenarioListener = new QueryListener(qmd);
            	queryEngineObject.addListener(queryScenarioListener);
        	}else{
        		throw new RuntimeException("\n\n[FATAL ERROR]:One of the pipeline queries that compose this scenario already has a listener.\n" +
        									"\tCan NOT be installed more than one listener per scenario.\n" +
        									"\tThis situation must be fixed on Engine's Query/Insatallation phase (at compile time).\n" +
        									"\tThe program will be terminated.\n");
        	}
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
