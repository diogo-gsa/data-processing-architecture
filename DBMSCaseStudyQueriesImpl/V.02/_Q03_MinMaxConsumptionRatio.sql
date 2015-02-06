SELECT  r1.device_pk,                                         
        r1.measure_timestamp,                                     
        min(r2.measure)/max(r2.measure) AS measure,                           
        'Ratio = [0,1]' AS measure_unit,                                        
        'Min/Max Power Consumption Ratio during last hour.'::text AS measure_description,              
        r1.device_location,
        min(r2.measure) AS min_last_hour_power_consumption,                                 
        max(r2.measure) AS max_last_hour_power_consumption                                      

FROM  "DBMS_EMS_Schema"."_Q08_SquareMeterNormalization"   AS r1       
      INNER JOIN                                          
      "DBMS_EMS_Schema"."_Q08_SquareMeterNormalization"   AS r2            
      ON   r1.device_pk  = 0       /* All Building consumption only */                
      AND r1.index        = 1       /* All Building most recent measure */               
      AND r2.device_pk   = r1.device_pk                               
      AND r2.measure_timestamp > r1.measure_timestamp - interval '60 minutes'  /*60min Time Window*/

GROUP BY r1.device_pk,                                        
         r1.measure_timestamp,                                      
         r1.measure_unit,                                       
         r1.measure_description,                                    
         r1.device_location