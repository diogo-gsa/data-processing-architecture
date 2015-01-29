ConsumptionsAboveThreshold
SELECT  device_pk,                                                 
        measure_timestamp,                                        
        measure,                  
        measure_unit,                                       
        measure_description,                                
        device_location                                         
FROM   "DBMS_EMS_Schema"."New_Q8_NormalizeConsumptionsByLocationSquareMeters"                    
WHERE   rank = 1
        AND (  (device_pk = 0 AND measure >= 00)  
            OR (device_pk = 1 AND measure >= 00)    
            OR (device_pk = 2 AND measure >= 00)    
            OR (device_pk = 3 AND measure >= 00)    
            OR (device_pk = 4 AND measure >= 00)    
            OR (device_pk = 5 AND measure >= 00)    
            OR (device_pk = 6 AND measure >= 00)    
            OR (device_pk = 7 AND measure >= 00)    
            OR (device_pk = 8 AND measure >= 00))