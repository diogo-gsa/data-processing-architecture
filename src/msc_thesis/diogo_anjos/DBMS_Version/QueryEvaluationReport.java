package msc_thesis.diogo_anjos.DBMS_Version;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DecimalFormat;

public class QueryEvaluationReport {

	private String executedQueryStatement = null;
	private String resultSetDump = null;
	private double queryExecutionTime = 0;

	public QueryEvaluationReport(String queryStatement, ResultSet resultSet){
		this.executedQueryStatement = queryStatement;
		this.resultSetDump = buildResulSetDump(resultSet);
	}

	public void setExecutedQueryStatement(String executedQueryStatement) {
		this.executedQueryStatement = executedQueryStatement;
	}
	
	public void setResultSetDump(ResultSet resultSet) {
		this.resultSetDump = buildResulSetDump(resultSet);
	}

	
	public void setQueryExecutionTime(double queryExecutionTime) {
		this.queryExecutionTime = queryExecutionTime;
	}
	
	public String getExecutedQueryStatement() {
		return executedQueryStatement;
	}

	public String getResultSetDump() {
		return resultSetDump;
	}

	public double getQueryExecutionTime() {
		return queryExecutionTime;
	}
	


	private String buildResulSetDump(ResultSet rs){
		String res = "";
		boolean emptyRS = true;
		try {ResultSetMetaData rsMetaData = rs.getMetaData();
			int columnCount = rsMetaData.getColumnCount();
			for(int i = 1; i <= columnCount; i++){
				res += "|"+rsMetaData.getColumnName(i); 
			}res +=  "\n";
			while(rs.next()) {
				for(int i = 1; i <= columnCount; i++){
					res +="|" + rs.getString(i);
				}emptyRS = false;
				res +=  "\n";
			}
		} catch (SQLException e) {e.printStackTrace();}
		//return null for empty ResultSets
		return (emptyRS == true) ? null : res;
	}
	
	public String dump(boolean dumpStatement, boolean dumpResult, boolean dumpElapsedTime, long insertStreamNanoElapsedTime){
		String res = "  ";
		if(dumpStatement){
			res += "Statement: " 	+	getExecutedQueryStatement() + "\n";
		}
		if(dumpElapsedTime){
			// ET = QueryExectionET + DatabaseInsertionTupleET
			double ET = getQueryExecutionTime()+nanoToMilliSeconds(insertStreamNanoElapsedTime);
			res += "ET="+ET+" ms "+"< In:"+nanoToMilliSeconds(insertStreamNanoElapsedTime)+" + Q:"+getQueryExecutionTime()+">\n";
		}
		if(dumpResult){
			res += getResultSetDump();
		}
		return res; 
	}
	
	public String dumpElapsedTime(){
		return "ET= " + getQueryExecutionTime() + " ms"; 
	}
	
	@Override
	public String toString(){ 
		return	"===== Query Evaluation [DEPRECATED] Report =====\n" +
				"Statement: " 	+	getExecutedQueryStatement() + "\n" +
				"ElapsedTime: " + 	getQueryExecutionTime()     + " ms \n" +
									getResultSetDump()        	+ "\n" +
				"=========== End of Report =========\n";
	}
	private double nanoToMilliSeconds(long nanoValue){
		// 1 nanoSecond / (10^6) = 1 milliSecond
    	// measure with nano resolution, but present the result in milliseconds
		return (((double)nanoValue)/((double)1000000));
	}
	
}
