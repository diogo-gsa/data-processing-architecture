package msc_thesis.diogo_anjos.DBMS_Version;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import msc_thesis.diogo_anjos.util.DButil;

public class BD_CRUD_Query_API {

	private final String className = "BD_CRUD_Query_API"; //debug purposes
	private final Connection database = DButil.connectToDB("localhost", "5432", "lumina_db", "postgres", "root", className);;
	
	public void insertInto_DatapointReading(){	
		String queryStatement = "";		
		try {
			
//			INSERT INTO "DBMS_EMS_Schema"."DataPointReading"(measure_timestamp, measure, datapoint_fk)
//			VALUES ('2014-02-05 12:33:28', 17, 4);
			
			 DButil.executeQuery(queryStatement, database);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	

}
