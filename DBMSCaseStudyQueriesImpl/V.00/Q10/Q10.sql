SELECT	*, rank() over sortedWindow
FROM "DBMS_EMS_Schema"."Q7+8_NoWindowOp"
WINDOW sortedWindow AS 
	(PARTITION BY null
	ORDER BY normalized_measure_avg_10min DESC)