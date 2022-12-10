/* ---------------------------------------------------------------------------------------------------------------------------- */
/* Version  |  Date        | Author              | Description                                                                  */
/* ---------------------------------------------------------------------------------------------------------------------------- */
/* 0.1      |  05/10/2022  | Ram Baskar          | Parameterised query to compare data in Bronze Layer between Staging & Delta   */

/* ---------------------------------------------------------------------------------------------------------------------------- */

USE [EclipseBronze];
DECLARE 
	    @Delta                      NVARCHAR(MAX) = 'Delta'
	   ,@Parquet					NVARCHAR(MAX) = 'PARQUET'
	   ,@BronzeCOLUMNList	        NVARCHAR(MAX) = ''
	   ,@BronzeColumnsForHash		NVARCHAR(MAX) = '' 
	   ,@Bronze_Stage_Path          NVARCHAR(MAX) = '' 
	   ,@Bronze_Delta_Path          NVARCHAR(MAX) = ''
	   --,@TableName					NVARCHAR(MAX) = 'BusinessCode' 
	   ,@TableName					NVARCHAR(MAX) = 'ClaimStatus' 
	   ,@QRY				        NVARCHAR(MAX) = ''

;	


SELECT TOP 1 * FROM [EclipseBronze].dbo.ClaimStatus;

--exec sys.sp_refreshview @viewname='dbo.ClaimStatus' -- Not supported in Synapse with Serverless Pool	

--Access via File Store
--SET @Bronze_Stage_Path = 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/'   + 'dbo_' + @TableName + '/*/**'
--SET @Bronze_Delta_Path = 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/DeltaLake/' + 'dbo_' + @TableName + '/'

--Access via Blob Storage URL
SET @Bronze_Stage_Path = 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/'   + 'dbo_' + @TableName + '/*/**' --using Blob storage access
SET @Bronze_Delta_Path = 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/DeltaLake/' + 'dbo_' + @TableName + '/'--using Blob storage access

PRINT @Bronze_Stage_Path
PRINT @Bronze_Delta_Path
	   
--SELECT @BronzeColumnsForHash  = @BronzeColumnsForHash + '[' + ColumnName    + ']' + ',' + '''%''' +',' -- without Collation 
SELECT @BronzeColumnsForHash  = @BronzeColumnsForHash       + '[' + ColumnName    + ']' + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_100_BIN2_UTF8') + ',' + '''%''' +','
      ,@BronzeCOLUMNList      = @BronzeCOLUMNList           + '[' + ColumnName    + ']' + ','
  FROM (
        SELECT 
               s.schema_id       as SchemaId
        	  ,s.name            as SchemName 
        	  ,t.name            as TableName
        	  ,c.column_id       as ColumnId
        	  ,c.collation_name  as CollationName
        	  ,c.name            AS ColumnName
          FROM [EclipseBronze].[sys].[schemas] s
          JOIN [EclipseBronze].[sys].[views]   t  ON s.schema_id = t.schema_id
          JOIN [EclipseBronze].[sys].[columns] c  ON t.object_id = c.object_id 
          )T
 --WHERE TableName = 'ClaimStatus'
 WHERE TableName = @TableName
   AND ColumnName NOT IN ('EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID')

SELECT @BronzeColumnsForHash  = SUBSTRING (@BronzeColumnsForHash ,1 ,LEN(@BronzeColumnsForHash)-5)
--print @BronzeColumnsForHash 

SELECT @BronzeCOLUMNList      = SUBSTRING (@BronzeCOLUMNList,1 ,LEN(@BronzeCOLUMNList)-1)
--print @BronzeCOLUMNList

--Building CTE query for Bronze Staging & Delta table

SELECT @QRY = 'With '                                                                                                                                            + CHAR(10) + 
                    --Building query for Staging data from PARQUET file
                    ' CTE_Bronze_Stage as ('                                                                                                                     + CHAR(10) + 
						('     SELECT ' + @BronzeCOLUMNList )                                                                                                    + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1''' + ',' + 'CONCAT(' + @BronzeColumnsForHash + ')' + ')' + 'as Bronze_Stage_HashBytes'               + CHAR(10) + 
						 '       FROM OPENROWSET ( '                                                                                                             + CHAR(10) + 
						 '             BULK ' + '''' + @Bronze_Stage_Path + ''''+ ','                                                                            + CHAR(10) + 
                         '             FORMAT = ' + ''''+@Parquet+''''	                                                                                         + CHAR(10) +
						 '                          ) as RS'                                                                                                     + CHAR(10) +    
							')'                                                                                                                                  + CHAR(10) +    

                    --Building query for Bronze Delta data from Synopse view
					',CTE_Bronze_Delta as ('                                                                                                                     + CHAR(10) + 
						('     SELECT ' + @BronzeCOLUMNList )                                                                                                    + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1''' + ',' + 'CONCAT(' + @BronzeColumnsForHash + ')' + ')' + 'as Bronze_Delta_HashBytes'               + CHAR(10) + 
						 '       FROM [EclipseBronze].[dbo].' + @TableName                                                                                       + CHAR(10) + 
							')'                                                                                                                                  + CHAR(10) +

/****************************************************************************************************************************************************************************
                    --Building query for Bronze Delta from PARQUET File
						',CTE_Bronze_Delta as ('                                                                                                                 + CHAR(10) + 
						('     SELECT ' + @BronzeCOLUMNList )                                                                                                    + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1'''       + ','      + 'CONCAT(' + @BronzeColumnsForHash + ')' + ')' + 'as Bronze_Delta_HashBytes'    + CHAR(10) + 
						 '       FROM OPENROWSET ( '                                                                                                             + CHAR(10) + 
						 '             BULK ' + '''' + @Bronze_Delta_Path + ''''+ ','                                                                            + CHAR(10) + 
                         '             FORMAT = ' + ''''+ @Delta +''''	                                                                                         + CHAR(10) +
						 '                          ) as RS'                                                                                                     + CHAR(10) +    
							')'                                                                                                                                  + CHAR(10) +
*****************************************************************************************************************************************************************************/
                    --Building Comparison Query between Stage & Delta
                                                                                                                                                                 + CHAR(10) +
						 '     SELECT STG.*,TGT.* '                                                                                                              + CHAR(10) +
                         '           ,Case WHEN Bronze_Stage_HashBytes = Bronze_Delta_HashBytes '                                                                + CHAR(10) +
	                     '                 THEN ' + '''Passed'''                                                                                                 + CHAR(10) + 
			             '                 ELSE ' + '''Failed'''                                                                                                 + CHAR(10) +
		                 '             End as TestResult'                                                                                                        + CHAR(10) + 
						 '           ,GetDate() ' + 'as TestedOn '                                                                                               + CHAR(10) +
						 '       FROM ' + ' CTE_Bronze_Stage STG'                                                                                                + CHAR(10) +
						 '  FULL JOIN  CTE_Bronze_Delta TGT'                                                                                                     + CHAR(10) + 
						 '         on Bronze_Stage_HashBytes = Bronze_Delta_HashBytes '                                                                         


print @QRY
--EXECUTE sp_executesql @QRY
EXEC (@QRY)

