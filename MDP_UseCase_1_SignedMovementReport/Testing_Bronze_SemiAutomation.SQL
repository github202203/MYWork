--POC Script for Semi Automation Bronze Layer WIP Script

USE [EclipseBronze];
DECLARE 
	    @Delta                      NVARCHAR(MAX) = 'Delta'
	   ,@Parquet					NVARCHAR(MAX) = 'PARQUET'
	   ,@BronzeCOLUMNList	        NVARCHAR(MAX) = ''
	   ,@BronzeColumnsForHash		NVARCHAR(MAX) = '' 
	   ,@Bronze_Stage_Path          NVARCHAR(MAX) = '' 
	   ,@Bronze_Delta_Path          NVARCHAR(MAX) = ''
	   ,@TableName					NVARCHAR(MAX) = 'BusinessCode' 
	   ,@QRY				        NVARCHAR(MAX) = ''

;		

SET @Bronze_Stage_Path = 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/'   + 'dbo_' + @TableName + '/*/**'
SET @Bronze_Delta_Path = 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/DeltaLake/' + 'dbo_' + @TableName + '/'



PRINT @Bronze_Stage_Path
PRINT @Bronze_Delta_Path
	   
SELECT @BronzeColumnsForHash  = @BronzeColumnsForHash + '[' + ColumnName    + ']' + ',' + '''%''' +','
      ,@BronzeCOLUMNList      = @BronzeCOLUMNList     + '[' + ColumnName    + ']' + ','
  from (
select 
        s.schema_id as SchemaId
	   ,s.name      as SchemName 
	   ,t.name      as TableName
	   ,c.column_id as ColumnId
	   ,c.name      as ColumnName
  from [EclipseBronze].[sys].[schemas] s
  JOIN [EclipseBronze].[sys].[views]   t  ON s.schema_id = t.schema_id
  JOIN [EclipseBronze].[sys].[columns] c  ON t.object_id = c.object_id 
  )T
 --WHERE TableName = 'ClaimStatus'
 WHERE TableName = @TableName
   AND ColumnName NOT IN ('EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID')

SELECT @BronzeColumnsForHash  = SUBSTRING (@BronzeColumnsForHash   ,1 ,LEN(@BronzeColumnsForHash)-5)
--print @BronzeColumnsForHash 

SELECT @BronzeCOLUMNList      = SUBSTRING (@BronzeCOLUMNList,1 ,LEN(@BronzeCOLUMNList)-1)
--print @BronzeCOLUMNList

--SELECT @QRY = 'With CTE_Bronze_Stage as ('                                                                                                                       + CHAR(10) + 
SELECT @QRY = 'With '                                                                                                                                            + CHAR(10) + 
                    ' CTE_Bronze_Stage as ('                                                                                                                     + CHAR(10) + 
						('     SELECT ' + @BronzeCOLUMNList )                                                                                                    + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1''' + ',' + 'CONCAT(' + @BronzeColumnsForHash + ')' + ')' + 'as Bronze_Stage_HashBytes'               + CHAR(10) + 
						 '       FROM OPENROWSET ( '                                                                                                             + CHAR(10) + 
						 '             BULK ' + '''' + @Bronze_Stage_Path + ''''+ ','                                                                            + CHAR(10) + 
                         '             FORMAT = ' + ''''+@Parquet+''''	                                                                                         + CHAR(10) +
						 '                          ) as RS'                                                                                                     + CHAR(10) +    
							')'                                                         
							+ CHAR(10) +    
							
/****************************************************************************************************************************************************************************

--This is NOT working for CONCAT operator picking up the columns from the Synapse view
						',CTE_Bronze_Delta as ('                                                                                                    + CHAR(10) + 
						 ('     SELECT ' + @BronzeCOLUMNList )                                                                                      + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1''' + ',' + 'CONCAT(' + @BronzeColumnsForHash + ')' + ')' + 'as Bronze_Delta_HashBytes'  + CHAR(10) + 
						 '       FROM [EclipseBronze].[dbo].[ClaimStatus] '                                                                         + CHAR(10) + 
							')'                                                                                                                     + CHAR(10) +
************************************************************************************************************************************************************************** */


						',CTE_Bronze_Delta as ('                                                                                                                 + CHAR(10) + 
						('     SELECT ' + @BronzeCOLUMNList )                                                                                                    + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1'''       + ','      + 'CONCAT(' + @BronzeColumnsForHash + ')' + ')' + 'as Bronze_Delta_HashBytes'    + CHAR(10) + 
						 '       FROM OPENROWSET ( '                                                                                                             + CHAR(10) + 
						 '             BULK ' + '''' + @Bronze_Delta_Path + ''''+ ','                                                                            + CHAR(10) + 
                         '             FORMAT = ' + ''''+ @Delta +''''	                                                                                         + CHAR(10) +
						 '                          ) as RS'                                                                                                     + CHAR(10) +    
							')'                                                                                                                                  + CHAR(10) +

--Building Comparison Query between Prod & Dev

							'SELECT STG.*,TGT.* '                                                                                                                + CHAR(10) +
                            '           ,Case WHEN Bronze_Stage_HashBytes = Bronze_Delta_HashBytes '                                                             + CHAR(10) +
	                        '                 THEN ' + '''Passed'''                                                                                              + CHAR(10) + 
			                '                 ELSE ' + '''Failed'''                                                                                              + CHAR(10) +
		                    '             End as TestResult'                                                                                                     + CHAR(10) + 
							'           ,GetDate() ' + 'as TestedOn '                                                                                            + CHAR(10) +
																																					             
							'FROM ' + ' CTE_Bronze_Stage STG'                                                                                                    + CHAR(10) +
							'FULL JOIN  CTE_Bronze_Delta TGT'                                                                                                    + CHAR(10) + 
							'       on Bronze_Stage_HashBytes = Bronze_Delta_HashBytes '                                                                         


print @QRY
--EXECUTE sp_executesql @QRY
EXEC (@QRY)

