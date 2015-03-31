package msc_thesis.diogo_anjos.util;

/*
 * @author Diogo Anjos (diogo.silva.anjos@tecnico.ulisboa.pt)
 * MScThesis Solution:  Real-Time Data Processing Architecture 
 * 						for Energy Management Applications
 */

import javax.management.RuntimeErrorException;

import msc_thesis.diogo_anjos.DBMS_Version.exceptions.ThereIsNoDataPoint_PKwithThisLocaionException;

public enum DataPoint_PK {
	
	//---------------------------------------------------
	//DataPoint_PK(Ph1_PK,Ph2_PK,Ph3_PK,location)
	LIBRARY_Ph1			(81,82,83,	  "Biblioteca",		1),		
	LECTUREHALL_A4_Ph1	(85,86,87,	  "Anfiteatro A4",	2),	
    LECTUREHALL_A5_Ph1	(89,90,91, 	  "Anfiteatro A5",	3),
    CLASSROOM_1_17_Ph1	(93,94,95, 	  "1.17",			4),    
    CLASSROOM_1_19_Ph1	(97,98,99, 	  "1.19",			5),    
    DEPARTMENT_14_Ph1	(101,102,103, "2N-14",			6),    
    DEPARTMENT_16_Ph1	(105,106,107, "2N-16",			7),    
    LAB_1_58_MIT_Ph1	(109,110,111, "Lab MIT (1.58)",	8),
    UTA_A4_Ph1			(113,114,115, "UTA A4 (inov)",	9),
  
	TEST_METER			(117,118,119, "Test_Meter",		10);
	//---------------------------------------------------
	
    // Datapoint PK for each datapoint/location
 	private int Ph1_PK;
 	private int Ph2_PK;
 	private int Ph3_PK;
 	private String location;
 	private int device_PK; 
 	
 	
 	DataPoint_PK(int Ph1_PK, int Ph2_PK, int Ph3_PK, String location, int device_PK){
 		this.Ph1_PK = Ph1_PK;		//each Phase is a new and unique datapoint_pk that  
 		this.Ph2_PK = Ph2_PK;		//belongs to a unique device_pk (relation datapoint:device = 3:1)
 		this.Ph3_PK = Ph3_PK;
 		this.location = location;
 		this.device_PK = device_PK;
 	}
    
 	public int getPh1_PK(){
 		return Ph1_PK;
 	}
 	
 	public int getPh2_PK(){
 		return Ph2_PK;
 	}
 	
 	public int getPh3_PK(){
 		return Ph3_PK;
 	}
 	
 	public int getDevice_PK(){
 		return device_PK;
 	}
 	
 	public static DataPoint_PK getDataPoint_PKByLocation(String location) throws ThereIsNoDataPoint_PKwithThisLocaionException{
 		for(DataPoint_PK dpk : DataPoint_PK.values()){
 			if(dpk.location.equals(location)){
 				return dpk;
 			}
 		}
		throw new ThereIsNoDataPoint_PKwithThisLocaionException(location);
 	}
 	
 	public static int getDevice_PKByDatapoint_PK(int datapointPK) {
 		for(DataPoint_PK dpk : DataPoint_PK.values()){
 			if(dpk.Ph1_PK == datapointPK || dpk.Ph2_PK == datapointPK || dpk.Ph3_PK == datapointPK){
 				return dpk.device_PK;
 			}
 		}
		throw new RuntimeException("[FATAL ERROR] Datapoint_PK "+datapointPK+
				" cannot match any (Enum instance) DataPoint_PK to return its DevicePK.");
 	}
}