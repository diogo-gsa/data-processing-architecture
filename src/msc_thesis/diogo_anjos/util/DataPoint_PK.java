package msc_thesis.diogo_anjos.util;

import msc_thesis.diogo_anjos.DBMS_Version.exceptions.ThereIsNoDataPoint_PKwithThisLocaionException;

public enum DataPoint_PK {
	
	//---------------------------------------------------
	//DataPoint_PK(Ph1_PK,Ph2_PK,Ph3_PK,location)
	LIBRARY_Ph1			(81,82,83,"Biblioteca"),		
	LECTUREHALL_A4_Ph1	(85,86,87,"Anfiteatro A4"),	
    LECTUREHALL_A5_Ph1	(89,90,91, "Anfiteatro A5"),
    CLASSROOM_1_17_Ph1	(93,94,95, "1.17"),    
    CLASSROOM_1_19_Ph1	(97,98,99, "1.19"),    
    DEPARTMENT_14_Ph1	(101,102,103, "2N-14"),    
    DEPARTMENT_16_Ph1	(105,106,107, "2N-16"),    
    LAB_1_58_MIT_Ph1	(109,110,111, "Lab MIT (1.58)"),
    UTA_A4_Ph1			(113,114,115, "UTA A4 (inov)"),
  
	TEST_METER			(117,118,119, "Test_Meter");
	//---------------------------------------------------
	
    // Datapoint PK for each datapoint/location
 	private int Ph1_PK;
 	private int Ph2_PK;
 	private int Ph3_PK;
 	
 	private String location;
 	
 	DataPoint_PK(int Ph1_PK, int Ph2_PK, int Ph3_PK, String location){
 		this.Ph1_PK = Ph1_PK;
 		this.Ph2_PK = Ph2_PK;
 		this.Ph3_PK = Ph3_PK;
 		this.location = location;
 	}
    
 	public int getPh1_PK(){
 		return Ph1_PK;
 	}
 	
 	public int getPh2_PK(){
 		return Ph1_PK;
 	}
 	
 	public int getPh3_PK(){
 		return Ph1_PK;
 	}
 	
 	public static DataPoint_PK getDataPoint_PKByLocation(String location) throws ThereIsNoDataPoint_PKwithThisLocaionException{
 		for(DataPoint_PK dpk : DataPoint_PK.values()){
 			if(dpk.location.equals(location)){
 				return dpk;
 			}
 		}
		throw new ThereIsNoDataPoint_PKwithThisLocaionException(location);
 	}
 	
}
