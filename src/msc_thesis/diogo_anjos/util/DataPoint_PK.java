package msc_thesis.diogo_anjos.util;

public enum DataPoint_PK {

	LIBRARY_Ph1			(81),	
	LIBRARY_Ph2			(82),
	LIBRARY_Ph3			(83),
	
	LECTUREHALL_A4_Ph1	(85),
	LECTUREHALL_A4_Ph2	(86),
	LECTUREHALL_A4_Ph3	(87),
	
    LECTUREHALL_A5_Ph1	(89),
    LECTUREHALL_A5_Ph2	(90),
    LECTUREHALL_A5_Ph3	(91),
    
    CLASSROOM_1_17_Ph1	(93),
    CLASSROOM_1_17_Ph2	(94),
    CLASSROOM_1_17_Ph3	(95),
    
    CLASSROOM_1_19_Ph1	(97),
    CLASSROOM_1_19_Ph2	(98),
    CLASSROOM_1_19_Ph3	(99),
    
    DEPARTMENT_14_Ph1	(101),
    DEPARTMENT_14_Ph2	(102),
    DEPARTMENT_14_Ph3	(103),
    
    DEPARTMENT_16_Ph1	(105),
    DEPARTMENT_16_Ph2	(106),
    DEPARTMENT_16_Ph3	(107),
    
    LAB_1_58_MIT_Ph1	(109),
    LAB_1_58_MIT_Ph2	(110),
    LAB_1_58_MIT_Ph3	(111),
    
    UTA_A4_Ph1			(113),
    UTA_A4_Ph2			(114),
    UTA_A4_Ph3			(115);
	
    // Datapoint PK for each datapoint/location
 	private int PK; 
 	
 	DataPoint_PK(int PK){
 		this.PK = PK;
 	}
    
}
