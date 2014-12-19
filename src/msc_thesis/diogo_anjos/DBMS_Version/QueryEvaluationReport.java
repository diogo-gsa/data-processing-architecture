package msc_thesis.diogo_anjos.DBMS_Version;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class QueryEvaluationReport {

	private String executedQueryStatement = null;
	private String resultSetDump = null;
	private double queryExecutionTime = 0;

	public QueryEvaluationReport(String queryStatement, ResultSet resultSet, double executionTime){
		this.executedQueryStatement = queryStatement;
		this.resultSetDump = buildResulSetDump(resultSet);
		this.queryExecutionTime = executionTime;
	}

	public void setExecutedQueryStatement(String executedQueryStatement) {
		this.executedQueryStatement = executedQueryStatement;
	}
	
	public void setResultSetDump(ResultSet resultSet) {
		this.resultSetDump = buildResulSetDump(resultSet);
	}

	
	public void setQueryExecutionTime(long queryExecutionTime) {
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
	
	public String dump(boolean dumpStatement, boolean dumpResult, boolean dumpElapsedTime){
		String res = "===== Query Evaluation Report =====\n";
		if(dumpStatement){
			res += "Statement: " 	+	getExecutedQueryStatement() + "\n";
		}
		if(dumpElapsedTime){
			res += "ElapsedTime: " + 	getQueryExecutionTime()     + " ms \n";
		}
		if(dumpResult){
			res += getResultSetDump() + "\n";
		}
		return res += "=========== End of Report =========\n"; 
	}
	
	public String dumpElapsedTime(){
		return "ElapsedTime: " + getQueryExecutionTime() + " ms"; 
	}
	
	@Override
	public String toString(){ 
		return	"===== Query Evaluation [DEPRECATED] Report =====\n" +
				"Statement: " 	+	getExecutedQueryStatement() + "\n" +
				"ElapsedTime: " + 	getQueryExecutionTime()     + " ms \n" +
									getResultSetDump()        	+ "\n" +
				"=========== End of Report =========\n";
	}
	
}
