/********************************************************************************************************************************************
--------------------------------------------------------------------------------------------------------------------------------------------- 
   Version  |  Date        | Author              | Description                                                                  
--------------------------------------------------------------------------------------------------------------------------------------------- 

-- ***************************************************    Status:- Sprint 2 delivery done    ***********************************************
   0.1      |  05/10/2022  | Ram Baskar          | Parameterised query (TableName)to compare data in Bronze Layer between Staging & Delta 
   0.2      |  12/10/2022  | Ram Baskar          | Included second parameter (Bronze SystemLoadID 

*****************************************************     Status:- Sprint 3 delivery (Work In Progress)    ************************************************ 
   0.3      |  17/10/2022  | Ram Baskar          | Amended with QAData.dbo.ConfigData table for Stage & Delta Path
   0.4      |  18/10/2022  | Ram Baskar          | Amended with Create External Table for test Result based n SystemLoadId
   0.5      |  18/10/2022  | Ram Baskar          | Amended with Drop External Table statement manually

   Parameter 1 - TableName                                                                                                      
   Parameter 2 - Bronze SystemLoadId                                                                                            

--------------------------------------------------------------------------------------------------------------------------------------------- 
********************************************************************************************************************************************/



USE [EclipseBronze];

--select @@SERVERNAME

DECLARE 
       /*************************************************************************************************************************************
       ,@StagePath                  NVARCHAR(MAX) = 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/'
	   ,@DeltaPath                  NVARCHAR(MAX) = 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/DeltaLake/'
	   ,@Location                   NVARCHAR(MAX) =  'N' +'''TestResult/ClaimStatus/tr7.parquet''' -- To be parameterised(Done)
       *********** **************************************************************************************************************************/

	    @Delta                      NVARCHAR(MAX) = 'Delta'
	   ,@Parquet					NVARCHAR(MAX) = 'parquet'
	   ,@BronzeCOLUMNList	        NVARCHAR(MAX) = ''
	   ,@BronzeCOLUMNListSTAGE      NVARCHAR(MAX) = ''
	   ,@BronzeCOLUMNListDELTA      NVARCHAR(MAX) = ''
	   ,@BronzeColumnsForHash		NVARCHAR(MAX) = '' 
	   ,@StagePath                  NVARCHAR(MAX) = ''
	   ,@DeltaPath                  NVARCHAR(MAX) = ''
	   ,@Bronze_Stage_Path          NVARCHAR(MAX) = '' 
	   ,@Bronze_Delta_Path          NVARCHAR(MAX) = ''
	   ,@QADatabasename             NVARCHAR(MAX) = 'QAData'
	   ,@QASchemaname               NVARCHAR(MAX) = 'dbo'
	   ,@QAFileFormat               NVARCHAR(MAX) = '[SynapseParquetFormat]'
	   ,@DataSource                 NVARCHAR(MAX) = '[qadata_dldpdev01_dfs_core_windows_net]' -- To be Parameterised
	   ,@TRFile                     NVARCHAR(MAX) = ''
	   ,@FileTimeStamp	            NVARCHAR(MAX) = ''
	   ,@Schemaname                 NVARCHAR(MAX) = 'dbo'
	   ,@TableName					NVARCHAR(MAX) = 'ClaimStatus' 
	   --,@TableName					NVARCHAR(MAX) = 'BusinessCode' 
   	   ,@SystemLoadId				NVARCHAR(MAX) = '1022022101001'
	   ,@Location                   NVARCHAR(MAX) =  ''
	   ,@QRY				        NVARCHAR(MAX) = ''
	   ,@EXISTING_Table             NVARCHAR(MAX) = ''
;	

--N'TestResult/ClaimStatus/tr7.parquet'
--SELECT CONVERT(char(10), GetDate(),112) - THis will give format of YYYYMMDD
--SET @SystemLoadId = '1022022090801';
--SET @SchemaName   = 'dbo'
SELECT TOP 1 * FROM [EclipseBronze].dbo.ClaimStatus;

Set @EXISTING_Table = @QADatabasename + '.' + @QASchemaName + '.' + 'TR_'+ @TableName + '_' + @SystemLoadId

print 'Existing Table = ' + @EXISTING_TABLE
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
print 'TableName = ' + @TableName

SET @FileTimeStamp = (SELECT FORMAT(GetDate(), 'yyyymmddhhmmss'))
SET @TRFile        = (SELECT @TableName + '_' + @SystemLoadId + '_'+ @FileTimeStamp + '.' + @Parquet)
SET @Location      = (SELECT 'N' + '''TestResult/' + @TRFile + '''')

print 'TRFile   = ' + @TRFile
print 'Location = ' + @Location

--Setting up with additional parameter with SystemLoadId for staging

SET @StagePath = (SELECT StagePath from QAData.dbo.ConfigData where Environment = 'Dev')
SET @DeltaPath = (SELECT DeltaPath from QAData.dbo.ConfigData where Environment = 'Dev')

SET @Bronze_Stage_Path = @StagePath  + @SchemaName + '_' + @TableName + '/SystemLoadID=' + @SystemLoadId + '/**' 
SET @Bronze_Delta_Path = @DeltaPath  + @SchemaName + '_' + @TableName + '/'

PRINT 'Bronze Stage Path = ' + @Bronze_Stage_Path
PRINT 'Bronze Delta Path = ' + @Bronze_Delta_Path
	   
--SELECT @BronzeColumnsForHash  = @BronzeColumnsForHash + '[' + ColumnName    + ']' + ',' + '''%''' +',' -- without Collation to be used for SQL DB/Azure SQL DB

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


--Building Create External Table & CTE query for Bronze Staging & Bronze Delta Data validation

--SELECT @QRY = 'Use QAData'                                                       + CHAR(10) + 


/*
IF EXISTS (SELECT [name] FROM QAData.sys.tables WHERE [name] = @QADatabasename + '.' + @QASchemaName + '.' + 'TR_'+ @TableName + '_' + @SystemLoadId) 
BEGIN
   DROP TABLE   @EXISTING_Table
END;
*/

--Including Create External Table code
SELECT @QRY = 'USE ' + @QADatabasename                                                   + CHAR(10) + 
              /*
              'IF EXISTS (SELECT [name] FROM QAData.sys.tables WHERE [name] = ' + @EXISTING_Table +')' + CHAR(10) +
			  'BEGIN ' + CHAR(10) + 
			  'DROP EXTERNAL Table ' + @EXISTING_TABLE + CHAR(10) + 
			  'END;' + CHAR(10) +
			  */
              'DROP EXTERNAL TABLE ' +   @EXISTING_Table + CHAR(10) +
			  --'CREATE EXTERNAL TABLE ' + @QADatabasename + '.' + @QASchemaName + '.' + 'TR_'+ @TableName + '_' + @SystemLoadId + '_' + @FileTimeStamp  + CHAR(10) + 
			  'CREATE EXTERNAL TABLE ' + @QADatabasename + '.' + @QASchemaName + '.' + 'TR_'+ @TableName + '_' + @SystemLoadId   + CHAR(10) + 
              'WITH ( '                                                          + CHAR(10) +                          
              '      DATA_SOURCE = ' + @DataSource          	                 + CHAR(10) + 
			  '     ,LOCATION    = ' + @Location                                 + CHAR(10) +
	          '     ,FILE_FORMAT = ' + @QAFileFormat                             + CHAR(10) +
              '    ) AS '   
--print @QRY
SELECT @QRY = @QRY                                                                                                                                               + CHAR(10) + 
              'With '                                                                                                                                            + CHAR(10) + 
                    --Building query for Staging data from PARQUET file
                    ' CTE_Bronze_Stage as ('                                                                                                                     + CHAR(10) + 
						 '     SELECT ' + @BronzeCOLUMNListSTAGE                                                                                                 + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1''' + ',' + 'CONCAT(' + @BronzeColumnsForHash + ')' + ')' + 'as Bronze_Stage_HashBytes'               + CHAR(10) + 
						 '       FROM OPENROWSET ( '                                                                                                             + CHAR(10) + 
						 '             BULK ' + '''' + @Bronze_Stage_Path + ''''+ ','                                                                            + CHAR(10) + 
                         '             FORMAT = ' + ''''+ @Parquet +''''	                                                                                         + CHAR(10) +
						 '                          ) as RS'                                                                                                     + CHAR(10) +    
							')'                                                                                                                                  + CHAR(10) +    

                    --Building query for Bronze Delta from Synopse view
					',CTE_Bronze_Delta as ('                                                                                                                     + CHAR(10) + 
						('     SELECT ' + @BronzeCOLUMNListDELTA )                                                                                               + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1''' + ',' + 'CONCAT(' + @BronzeColumnsForHash + ')' + ')' + 'as Bronze_Delta_HashBytes'               + CHAR(10) + 
						 '       FROM [EclipseBronze].[dbo].' + @TableName                                                                                       + CHAR(10) + 
							')'                                                                                                                                  + CHAR(10) +


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

--SELECT * FROM QAData.sys.external_tables