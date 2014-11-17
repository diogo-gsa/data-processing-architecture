SELECT 	* 
FROM 	"DBMS_EMS_Schema"."Q11_NO_Win_10min"
		--Available Windows (views) to "feed" this Query
		--"DBMS_EMS_Schema"."Q11_Size_Win_10min" 
		--"DBMS_EMS_Schema"."Q11_Time_Win_10min"
WHERE 	variation_10min_win > 0.05


