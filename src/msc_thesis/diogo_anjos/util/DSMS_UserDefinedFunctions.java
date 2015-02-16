package msc_thesis.diogo_anjos.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.sql.Timestamp;

public class DSMS_UserDefinedFunctions {
	
	public static double getExpectedMeasure(long device_pk, String measure_timestamp){
		
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(measure_timestamp));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		int ts_hour = cal.get(Calendar.HOUR_OF_DAY);
				
		double reference_map[][] = 
			// 00h  01h  02h  03h  04h  05h  06h  07h  08h  09h  10h  11h  12h  13h  14h  15h  16h  17h  18h  19h  20h  21h  22h  23h
			{{ 4.5, 4.4, 4.4, 4.4, 4.3, 4.5, 5.3, 6.1, 5.8, 6.8, 9.0, 9.6, 9.2, 8.7, 9.1, 9.5, 7.4, 7.0, 6.3, 5.7, 5.5, 4.8, 4.7, 4.6},
			 {10.2,10.1, 9.9, 9.9, 9.9,10.3,12.3,12.3,12.4,13.4,14.4,15.5,14.6,13.9,14.5,15.2,15.0,14.3,13.6,13.0,12.5,10.5,10.4,10.5},
			 { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 4.1, 0.0, 0.2,17.3,16.8,17.2,17.8,18.5,17.2, 0.4, 0.4, 0.4, 0.1, 0.0, 0.4, 0.4, 0.2},
			 { 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.1, 6.8, 3.9, 9.8, 9.8, 9.8, 9.8,10.0, 9.4, 8.6, 0.2, 0.1, 0.2, 0.0, 0.0, 0.1, 0.0, 0.0},
			 { 0.9, 0.8, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 2.9, 2.2, 1.1, 3.1, 2.6, 2.7, 3.6, 4.8, 4.1, 3.0, 1.5, 1.4, 1.3, 1.6, 1.9, 1.1},
			 { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.8, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.8, 1.7, 0.2, 0.2, 0.2, 0.0, 0.0, 0.0, 0.0},
			 { 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 1.0, 2.4, 4.6, 5.2, 4.4, 3.1, 4.0, 4.8, 5.1, 5.2, 3.5, 1.5, 1.5, 1.7, 1.5, 0.7},
			 { 2.2, 2.1, 2.3, 2.3, 2.2, 2.1, 2.3, 2.3, 2.2, 2.3, 3.5, 3.5, 3.8, 3.3, 3.3, 3.4, 3.3, 3.2, 2.7, 2.5, 2.3, 2.4, 2.2, 2.3},	
			 { 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 3.9, 3.9, 3.9, 4.0, 4.0, 4.0, 4.0, 4.1, 4.0, 4.1, 4.1, 4.1, 4.1, 4.1, 4.1, 4.1, 4.1, 4.0}};  
		
		return reference_map[(int) device_pk][ts_hour];
	}	

	public static long convertStringTSformatToLong(String measure_timestamp) throws ParseException{
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Date parsedDate = dateFormat.parse(measure_timestamp);
		Timestamp timestamp = new Timestamp(parsedDate.getTime());
		return timestamp.getTime(); //13digitsTS (micro TS)
	}

}