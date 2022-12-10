/********************************************************************************************************************************************
--------------------------------------------------------------------------------------------------------------------------------------------- 
   Version  |  Date        | Author              | Description                                                                  
--------------------------------------------------------------------------------------------------------------------------------------------- 
   0.1      |  05/10/2022  | Ram Baskar          | Parameterised query (TableName)to compare data in Bronze Layer between Staging & Delta 
   0.2      |  12/10/2022  | Ram Baskar          | Included second parameter (Bronze SystemLoadID 
   
 ***********************     Status:- Sprint 2 delivery done    ******************************

   Parameter 1 - TableName                                                                                                      
   Parameter 2 - Bronze SystemLoadId                                                                                            

--------------------------------------------------------------------------------------------------------------------------------------------- 
********************************************************************************************************************************************/

USE [EclipseBronze];

DECLARE 
	    @Delta                      NVARCHAR(MAX) = 'Delta'
	   ,@Parquet					NVARCHAR(MAX) = 'PARQUET'
	   ,@BronzeCOLUMNList	        NVARCHAR(MAX) = ''
	   ,@BronzeCOLUMNListSTAGE      NVARCHAR(MAX) = ''
	   ,@BronzeCOLUMNListDELTA      NVARCHAR(MAX) = ''
	   ,@BronzeColumnsForHash		NVARCHAR(MAX) = '' 
	   ,@StagePath                  NVARCHAR(MAX) = 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/'
	   ,@DeltaPath                  NVARCHAR(MAX) = 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/DeltaLake/'
	   ,@Bronze_Stage_Path          NVARCHAR(MAX) = '' 
	   ,@Bronze_Delta_Path          NVARCHAR(MAX) = ''
	   ,@Schemaname                 NVARCHAR(MAX) = 'dbo'
--Claim Status
	   --,@TableName				NVARCHAR(MAX) = 'ClaimStatus' 
   	   --,@SystemLoadId             NVARCHAR(MAX) = '1022022090801'

--Lloyds Broker
--	   ,@TableName					NVARCHAR(MAX) = 'LloydsBroker' 
--	   ,@SystemLoadId               NVARCHAR(MAX) = '1022022092302'

--BusinessCode
	   ,@TableName					NVARCHAR(MAX) = 'BusinessCode' 
	   ,@SystemLoadId               NVARCHAR(MAX) = '1022022090801'

	   ,@QRY				        NVARCHAR(MAX) = ''

;	


--SET @SystemLoadId = '1022022090801';
--SET @SchemaName   = 'dbo'

SELECT TOP 1 * FROM [EclipseBronze].dbo.ClaimStatus;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--exec sys.sp_refreshview @viewname='dbo.ClaimStatus' -- Not supported in Synapse with Serverless Pool	
--Access via File Store
--SET @Bronze_Stage_Path = 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/'   + 'dbo_' + @TableName + '/*/**'
--SET @Bronze_Delta_Path = 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/DeltaLake/' + 'dbo_' + @TableName + '/'

--Access via Blob Storage URL
--SET @Bronze_Stage_Path = 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/'   + 'dbo_' + @TableName + '/*/**' --using Blob storage access
--SET @Bronze_Stage_Path = 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/'   + 'dbo_' + @TableName + '/SystemLoadID=' + @SystemLoadId + '/**' --using Blob storage access
--SET @Bronze_Delta_Path = 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/DeltaLake/' + 'dbo_' + @TableName + '/'--using Blob storage access
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--Setting up with additional parameter with SystemLoadId for staging

SET @Bronze_Stage_Path = @StagePath  + @SchemaName + '_' + @TableName + '/SystemLoadID=' + @SystemLoadId + '/**' 
SET @Bronze_Delta_Path = @DeltaPath  + @SchemaName + '_' + @TableName + '/'--using Blob storage access

PRINT @Bronze_Stage_Path
PRINT @Bronze_Delta_Path
	   
--SELECT @BronzeColumnsForHash  = @BronzeColumnsForHash + '[' + ColumnName    + ']' + ',' + '''%''' +',' -- without Collation 
SELECT @BronzeColumnsForHash    = @BronzeColumnsForHash       + '[' + ColumnName      + ']' + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_100_BIN2_UTF8') + ',' + '''%''' +','
      ,@BronzeCOLUMNListSTAGE   = @BronzeCOLUMNListSTAGE      + '[' + ColumnNameSTAGE + ']' + ','
	  ,@BronzeCOLUMNListDELTA   = @BronzeCOLUMNListDELTA      + '[' + ColumnNameDELTA + ']' + ','
	  ,@BronzeCOLUMNList        = @BronzeCOLUMNList           + '[' + ColumnName      + ']' + ','
  FROM (
        SELECT 
               s.schema_id                                       as SchemaId
        	  ,s.name                                            as SchemaName 
        	  ,t.name                                            as TableName
        	  ,c.column_id                                       as ColumnId
        	  ,c.collation_name                                  as CollationName
        	  ,c.name                                            as ColumnName
			  ,c.name +']' + ' as '  +  '[' + c.name + 'STAGE'   as ColumnNameSTAGE
			  ,c.name +']' + ' as '  +  '[' + c.name + 'DELTA'   as ColumnNameDELTA
          FROM [EclipseBronze].[sys].[schemas] s
          join [EclipseBronze].[sys].[views]   t  on s.schema_id = t.schema_id
          join [EclipseBronze].[sys].[columns] c  on t.object_id = c.object_id 
		--  WHERe t.name = 'ClaimStatus'
          )T
 
 WHERE SchemaName = @Schemaname
   AND TableName = @TableName
   AND ColumnName NOT IN ('RowNumber','EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID')


--Tidying up all comma and extra characters
SELECT @BronzeColumnsForHash   = SUBSTRING (@BronzeColumnsForHash,1   ,LEN(@BronzeColumnsForHash)-5)
SELECT @BronzeCOLUMNList       = SUBSTRING (@BronzeCOLUMNList,1       ,LEN(@BronzeCOLUMNList)-1)
SELECT @BronzeCOLUMNListSTAGE  = SUBSTRING (@BronzeCOLUMNListSTAGE,1  ,LEN(@BronzeCOLUMNListSTAGE)-1)
SELECT @BronzeCOLUMNListDELTA  = SUBSTRING (@BronzeCOLUMNListDELTA,1  ,LEN(@BronzeCOLUMNListDELTA)-1)

--print 'Column List STAGE = ' + @BronzeCOLUMNListSTAGE
--print @BronzeCOLUMNListSTAGE
--print @BronzeColumnsForHash 
--print @BronzeCOLUMNList


--Building CTE query for Bronze Staging & Bronze Delta

SELECT @QRY = 'With '                                                                                                                                            + CHAR(10) + 
                    --Building query for Staging data from PARQUET file
                    ' CTE_Bronze_Stage as ('                                                                                                                     + CHAR(10) + 
						--('     SELECT ' + @BronzeCOLUMNListSTAGE )                                                                                               + CHAR(10) + 
						 '     SELECT ' + @BronzeCOLUMNListSTAGE                                                                                                 + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1''' + ',' + 'CONCAT(' + @BronzeColumnsForHash + ')' + ')' + 'as Bronze_Stage_HashBytes'               + CHAR(10) + 
						 '       FROM OPENROWSET ( '                                                                                                             + CHAR(10) + 
						 '             BULK ' + '''' + @Bronze_Stage_Path + ''''+ ','                                                                            + CHAR(10) + 
                         '             FORMAT = ' + ''''+@Parquet+''''	                                                                                         + CHAR(10) +
						 '                          ) as RS'                                                                                                     + CHAR(10) +    
							')'                                                                                                                                  + CHAR(10) +    

                    --Building query for Bronze Delta from Synopse view
					',CTE_Bronze_Delta as ('                                                                                                                     + CHAR(10) + 
						('     SELECT ' + @BronzeCOLUMNListDELTA )                                                                                               + CHAR(10) + 
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
						 '     SELECT STAGE.*,DELTA.* '                                                                                                          + CHAR(10) +
                         '           ,Case WHEN Bronze_Stage_HashBytes = Bronze_Delta_HashBytes '                                                                + CHAR(10) +
	                     '                 THEN ' + '''Passed'''                                                                                                 + CHAR(10) + 
			             '                 ELSE ' + '''Failed'''                                                                                                 + CHAR(10) +
		                 '             End as TestResult'                                                                                                        + CHAR(10) + 
						 '           ,GetDate() ' + 'as TestedOn '                                                                                               + CHAR(10) +
						 '       FROM ' + ' CTE_Bronze_Stage STAGE'                                                                                              + CHAR(10) +
						 '       left join  CTE_Bronze_Delta DELTA'                                                                                              + CHAR(10) + 
						 '              on Bronze_Stage_HashBytes = Bronze_Delta_HashBytes '                                                              


print @QRY
EXECUTE sp_executesql @QRY
--EXEC (@QRY)





