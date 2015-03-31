package msc_thesis.diogo_anjos.DSMS_Version;

/*
 * @author Diogo Anjos (diogo.silva.anjos@tecnico.ulisboa.pt)
 * MScThesis Solution:  Real-Time Data Processing Architecture 
 * 						for Energy Management Applications
 */

import com.espertech.esper.client.EPStatement;

public class QueryMetadata {

    private int queryID;
    private String queryExpression;
    private EPStatement queryEngineObject;

    public QueryMetadata(int queryID, String queryStatement, EPStatement queryEngineObject){
        this.queryID = queryID;
        this.queryExpression = queryStatement;
        this.queryEngineObject = queryEngineObject;
    }
       
    public int getQueryID() {
        return queryID;
    }

    public String getQueryStatement() {
        return queryExpression;
    }

    public void destroyQuery(){
        queryEngineObject.destroy();
    }
    
    @Override
    public String toString(){
        return "QueryID: " + queryID + "Statement:\n"+ queryExpression;
    }
}
