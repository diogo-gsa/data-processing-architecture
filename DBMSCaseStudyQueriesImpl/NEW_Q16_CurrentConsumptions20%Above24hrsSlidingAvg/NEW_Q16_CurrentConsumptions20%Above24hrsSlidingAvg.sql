SELECT  device_pk,                                                                                               
        measure_timestamp,                                                                                        
        current_measure                AS measure,                                                                
        measure_sliding24h_avg*1.25    AS measure_threshold,                                                      
        device_location,                                                                                          
        measure_unit,                                                                                             
        'Measures 25% higher than the past 24h average' AS measure_description                                    
FROM   (SELECT  all_measures.device_pk,                                                                            
                all_measures.measure_timestamp,                                                                   
                all_measures.measure_avg_10min   AS current_measure,                                              
                all_measures.measure_unit,                                                                        
                all_measures.measure_description,                                                                 
                all_measures.device_location,                                                                     
                all_measures.location_area_m2,                                                                    
                avg(all_measures.measure_avg_10min)  over w       AS measure_sliding24h_avg,                                   
                rank()                  over w                                                                    
        FROM   "DBMS_EMS_Schema"."New_Q7_SmoothConsumptionsWith10MinSlindingAvg"   AS all_measures                                              
                INNER JOIN 
                "DBMS_EMS_Schema"."New_Q7_SmoothConsumptionsWith10MinSlindingAvg" AS most_recent_measure
                ON   most_recent_measure.rank = 1
                AND  most_recent_measure.device_pk = all_measures.device_pk                                        
                AND  all_measures.measure_timestamp >= most_recent_measure.measure_timestamp  - interval '24 hours'             
        WINDOW w AS (PARTITION BY   all_measures.device_pk                                                            
                     ORDER BY all_measures.measure_timestamp DESC                                                 
                     RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)                                           
        ) AS rel                                                                                                  
WHERE current_measure >= measure_sliding24h_avg*0.0 AND rank = 1
