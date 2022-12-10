Declare @QRY	        	NVARCHAR(MAX) = ''
       ,@QADatabaseName		NVARCHAR(MAX) = 'QAData'
	   ,@EXISTING_TABLE 	NVARCHAR(MAX) = 'TR_ClaimStatus_1022022101001'
;

SELECT @QRY = 'USE ' + @QADatabasename                                                                                      + CHAR(10) + 
              
              'IF EXISTS (SELECT [name] FROM QAData.sys.external_tables WHERE [name] = ' + ''''+ @EXISTING_Table +'''' +')' + CHAR(10) +
			  'BEGIN '                                                                                                      + CHAR(10) + 
			  '      DROP EXTERNAL Table ' + @EXISTING_TABLE                                                                + CHAR(10) + 
			  '  END;' 
print @QRY		
EXECUTE sp_executesql @QRY