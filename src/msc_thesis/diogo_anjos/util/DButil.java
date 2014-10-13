package msc_thesis.diogo_anjos.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DButil {
	
	public static Connection connectToDB(String hostname, String port, String dbname, String username, String password, String className) {
		System.out.print(className+"is Connecting to DB: " + hostname + ":" + port + "/" + dbname + " with user/pass=" + username + "/" + password + "... ");
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("\n[ERROR]: Cannot find Simulator's JDBC driver.");
			e.printStackTrace();
			return null;
		}
		try {
			String database_URI = "jdbc:postgresql://" + hostname + ":" + port + "/" + dbname;
			Connection db_connection = DriverManager.getConnection(database_URI, username, password);
			System.out.println("Done! "+className+" successfully connected to DB");
			return db_connection;
		} catch (SQLException e) {
			System.out.println("\n[ERROR]: Cannot connect to Database.");
			e.printStackTrace();
			return null;
		}
	}

	public static ResultSet executeQuery(String queryStatement, Connection database) throws SQLException {
		return database.createStatement().executeQuery(queryStatement);			
	}

	//Returns: either 	(1) the row count for SQL Data Manipulation Language (DML) statements or 
	//					(2) 0 for SQL statements that return nothing
	public static int executeUpdate(String queryStatement, Connection database) throws SQLException{
		return database.createStatement().executeUpdate(queryStatement);
	}

}
