DECLARE @QRY                        NVARCHAR(MAX) = ''
       ,@Parquet					NVARCHAR(MAX) = 'PARQUET'
	   ,@DBName                     NVARCHAR(MAX) = ''
	   ,@TableName                  NVARCHAR(MAX) = ''

	   --,@Delta                      NVARCHAR(MAX) = 'Delta'
	   --,@Bronze_Stage_Path          NVARCHAR(MAX) = '' 
	   
;

--SET @Bronze_Stage_Path = 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/'   + 'dbo_' + @TableName + '/*/**' --using Blob storage access
SET @TableName = 'BusinessCode'
SET @DBName    = 'EclipseBronze'

       SELECT 
		      @QRY = 'With '                                                                                                                                                          + CHAR(10) + 
                    --Building query for Staging data from PARQUET file
                    ' CTE_Bronze_Stage as ('                                                                                                                                          + CHAR(10) + 
						 '     SELECT ' + '''' + t.name + '''' + ' as TableName' + ',' + 'COUNT(*) as Stage_Count'                                                                    + CHAR(10) + 
						 '       FROM OPENROWSET ( '                                                                                                                                  + CHAR(10) + 
						 '            BULK '     + '''' + 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_' + t.name + '/*/**' +'''' + ','  + CHAR(10) + 
                         '            FORMAT = ' + '''' + @Parquet + ''''	                                                                                                          + CHAR(10) +
						 '                          ) as RS'                                                                                                                          + CHAR(10) +    
							')'                                                                                                                                                       + CHAR(10) +    

                    --Building query for Bronze Delta data from Synopse view
					',CTE_Bronze_Delta as ('                                                                                                                                          + CHAR(10) + 
						 '     SELECT ' + '''' + t.name + '''' + ' as TableName' + ',' + 'COUNT(*) as Delta_Count'                                                                    + CHAR(10) + 
						 '       FROM [EclipseBronze].[dbo].' + t.name                                                                                                                + CHAR(10) + 
							')'                                                                                                                                                       + CHAR(10) + 

                    --Building Comparison Query between Stage & Delta
                                                                                                                                                                                      + CHAR(10) +
						 '     SELECT STG.TableName,Stage_Count,Delta_Count '                                                                                                         + CHAR(10) +
                         '           ,Case WHEN Stage_Count = Delta_Count '                                                                                                           + CHAR(10) +
	                     '                 THEN ' + '''Passed'''                                                                                                                      + CHAR(10) + 
			             '                 ELSE ' + '''Failed'''                                                                                                                      + CHAR(10) +
		                 '             End as TestResult'                                                                                                                             + CHAR(10) + 
						 '           ,GetDate() ' + 'as TestedOn '                                                                                                                    + CHAR(10) +
						 '       FROM ' + ' CTE_Bronze_Stage STG '                                                                                                                    + CHAR(10) +
						 '       FULL JOIN  CTE_Bronze_Delta TGT '                                                                                                                    + CHAR(10) + 
						 '              on  STG.TableName = TGT.TableName '                                                                         
               
         FROM [EclipseBronze].[sys].[schemas] s
         JOIN [EclipseBronze].[sys].[views]   t  ON s.schema_id = t.schema_id
         JOIN [EclipseBronze].[sys].[columns] c  ON t.object_id = c.object_id 
     
        WHERE c.name NOT IN ('EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID') 
		  AND t.name = @TableName
          --AND t.name IN ( 'ClaimStatus','BusinessCode')

PRINT @QRY
EXEC(@QRY)

