--USE [QAData];

/********************************************************************************************************************************************
--------------------------------------------------------------------------------------------------------------------------------------------- 
   Version  |  Date        | Author              | Description                                                                  
--------------------------------------------------------------------------------------------------------------------------------------------- 

-- ***********************************************************    Status:- Sprint 2 delivery done    ******************************************************
   0.1      |  05/10/2022  | Ram Baskar          | Parameterised query (TableName)to compare data in Bronze Layer between Staging & Delta 
   0.2      |  12/10/2022  | Ram Baskar          | Included second parameter (Bronze SystemLoadID 

*****************************************************     Status:- Sprint 3 delivery (Work In Progress)    ************************************************ 
   0.3      |  17/10/2022  | Ram Baskar          | Amended with QAData.dbo.ConfigData table for Stage & Delta Path
   0.4      |  18/10/2022  | Ram Baskar          | Amended with Create External Table for test Result based n SystemLoadId
   0.5      |  18/10/2022  | Ram Baskar          | Amended with Drop External Table statement manually
   0.6      |  19/10/2022  | Ram Baskar          | Amended with Drop External Table statement dynamically and additional columns in TestResult table
   0.7      |  20/10/2022  | Ram Baskar          | Updated with Additional parameters from Control Framework - SystemLoadId/SystemDescription/Run Id etc.
   0.8      |  21/10/2022  | Ram Baskar          | Converted the SQL script to Stored Procedure
   0.9      |  26/10/2022  | Ram Baskar          | Changed the columns for HashBytes with CONVERT function due to mismatch of DataType between STAGE/DELTA
   0.10     |  29/10/2022  | Ram Baskar          | Changed the Query to use JoinCondition from Control Framework
   0.11     |  30/10/2022  | Ram Baskar          | Changed the Query to derive JoinCondition from Control Framework for table with composite Unique Columns
   0.12     |  04/11/2022  | Ram Baskar          | Changed the query to compare between Bronze Staging Parquet to Bronze Delta Parquet
   0.13     |  11/11/2022  | Ram Baskar          | Changed the query to pick up only BronzeStaging - BronzeDelta related SystemLoadId's for data comparision

   Parameter 1 - TableName                                                                                                      
   Syntax to execute this code

   --EXEC QAData.dbo.ValidateBronzeStageToDeltaData @TableName = 'ClaimStatus'
   
   *********************************** This script will work only for tables with less than 254 columns ********************************************
----------------------------------------------------------------------------------------------------------------------------------------------------------- 
**********************************************************************************************************************************************************/

--USE [QAData];


--select @@ServerName, Substring(@@SERVERNAME,8,3)

CREATE or ALTER Procedure [dbo].[ValidateBronzeStageToBronzeDeltaData_usingParquet] (@TableName VARCHAR(100))
AS 
BEGIN



DECLARE 
       /*************************************************************************************************************************************
       ,@StagePath                  NVARCHAR(MAX) = 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/'
	   ,@DeltaPath                  NVARCHAR(MAX) = 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/DeltaLake/'
	   ,@DataSource                 NVARCHAR(MAX) = '[qadata_dldpdev01_dfs_core_windows_net]' -- To be Parameterised (Done)
	   ,@Location                   NVARCHAR(MAX) =  'N' +'''TestResult/ClaimStatus/tr7.parquet''' -- To be parameterised(Done)
	   ,@Environment                NVARCHAR(MAX) = 'Dev' -- To be parameterised (Done)
	   ,@SystemLoadId				NVARCHAR(MAX) = '1022022101001' -- To be Parameterised (Done)
       *********** **************************************************************************************************************************/

	    @Delta                      NVARCHAR(MAX) = 'Delta'
	   ,@Parquet					NVARCHAR(MAX) = 'parquet'
	   --,@TableName					NVARCHAR(MAX) = 'ClaimStatus' 
	   --,@TableName					NVARCHAR(MAX) = 'BusinessCode' 
	   --,@TableName					NVARCHAR(MAX) = 'ApplicationUser' 
	   --,@TableName					NVARCHAR(MAX) = 'PolicyPrem' 
	   --,@TableName					NVARCHAR(MAX) = 'ReportingClass' 
	   ,@SourceSystemId             NVARCHAR(MAX) = ''
	   ,@SourceObjectId             NVARCHAR(MAX) = ''
	   ,@SourceObjectName           NVARCHAR(MAX) = ''
	   ,@ObjectRunId				NVARCHAR(MAX) = ''
	   ,@SystemName                 NVARCHAR(MAX) = ''
	   ,@UniqueColumn               NVARCHAR(MAX) = ''
	   ,@LoadTypeDescription        NVARCHAR(MAX) = ''
	   ,@JoinCondition              NVARCHAR(MAX) = ''
	   ,@Tags                       NVARCHAR(MAX) = ''
	   ,@SystemDescription          NVARCHAR(MAX) = ''
	   
	   ,@QADatabasename             NVARCHAR(MAX) = 'QAData'
	   ,@QASchemaname               NVARCHAR(MAX) = 'dbo'
	   ,@QAFileFormat               NVARCHAR(MAX) = '[SynapseParquetFormat]'
	   ,@Schemaname                 NVARCHAR(MAX) = 'dbo'
	   ,@ServerName                 NVARCHAR(MAX) = ''
	   ,@Environment                NVARCHAR(MAX) = '' 

   	   
	   --,@SystemLoadId				NVARCHAR(MAX) = '1022022101001'
	   ,@SystemLoadId				NVARCHAR(MAX) = ''

	   ,@BronzeCOLUMNList	        NVARCHAR(MAX) = ''
	   ,@BronzeCOLUMNListSTAGE      NVARCHAR(MAX) = ''
	   ,@BronzeCOLUMNListDELTA      NVARCHAR(MAX) = ''
	   ,@BronzeColumnsForHash		NVARCHAR(MAX) = '' 
	   ,@BronzeColumnsForHashSTG	NVARCHAR(MAX) = '' 
	   ,@BronzeColumnsForHashTGT	NVARCHAR(MAX) = '' 
	   ,@StagePath                  NVARCHAR(MAX) = ''
	   ,@DeltaPath                  NVARCHAR(MAX) = ''
	   ,@Bronze_Stage_Path          NVARCHAR(MAX) = '' 
	   ,@Bronze_Delta_Path          NVARCHAR(MAX) = ''
	   ,@DataSource                 NVARCHAR(MAX) = ''
	   ,@TRFile                     NVARCHAR(MAX) = ''
	   ,@FileTimeStamp	            NVARCHAR(MAX) = ''
	   ,@Location                   NVARCHAR(MAX) = ''
	   ,@QRY				        NVARCHAR(MAX) = ''
	   ,@EXT				        NVARCHAR(MAX) = ''
	   ,@FINAL				        NVARCHAR(MAX) = ''
	   ,@EXISTING_External_Table    NVARCHAR(MAX) = ''
;	

SELECT TOP 1 * FROM [EclipseBronze].dbo.ClaimStatus;

IF 
    Substring(@@SERVERNAME,8,3) = 'dev'
    SET @Environment = 'Dev'

ELSE IF 
    Substring(@@SERVERNAME,8,3) = 'tst'
    SET @Environment = 'Tst'

ELSE IF 
    Substring(@@SERVERNAME,8,3) = 'uat'
    SET @Environment = 'Uat'
ELSE 
    SET @Environment = 'Prd'

--Print 'Environment = ' + @Environment
   
/***************************************************************************************************************************************
SET @SystemName       = (SELECT SystemName       FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName)
SET @SourceObjectId   = (SELECT SourceObjectId   FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName)
SET @SourceObjectName = (SELECT SourceObjectName FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName)
SET @SystemLoadId     = (SELECT SystemLoadId     FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName)
SET @UniqueColumn     = (SELECT UniqueColumn     FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName)
****************************************************************************************************************************************/
--Changes from v0.13 as below to pick up only for Bronze Stage to Bronze Delta SystemLoadId's
SET @SystemName       = (SELECT SystemName       FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and SystemLoadId like '102%')
SET @SourceObjectId   = (SELECT SourceObjectId   FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and SystemLoadId like '102%')
SET @SourceObjectName = (SELECT SourceObjectName FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and SystemLoadId like '102%')
SET @SystemLoadId     = (SELECT SystemLoadId     FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and SystemLoadId like '102%')
SET @UniqueColumn     = (SELECT UniqueColumn     FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and SystemLoadId like '102%')

/* This condition works OK if the unique column is a single column for the enrity being tested 
SET @JoinCondition     = (SELECT REPLACE(REPLACE(JoinCondition,'P.','STAGE.STAGE'),'CT.','DELTA.DELTA')
                            FROM QAData.dbo.QAControlData 
						   WHERE SourceObjectName = @SchemaName + '.' + @TableName)
*/
SET @Tags              = (SELECT UniqueColumn      FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and SystemLoadId like '102%')
SET @ObjectRunId       = (SELECT ObjectRunId       FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and SystemLoadId like '102%')
SET @SystemDescription = (SELECT SystemDescription FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and SystemLoadId like '102%')

--print 'System Description =' + @SystemDescription
--print 'Join Condition = ' + @JoinCondition



--SET @EXISTING_External_Table = @QADatabasename + '.' + @QASchemaName + '.' + 'TR_'+ @TableName + '_' + @SystemLoadId
--SET @EXISTING_External_Table = 'TR_'+ @TableName + '_' + @SystemLoadId
SET @EXISTING_External_Table = 'TR_'+ @TableName 

--print 'Existing Table = ' + @EXISTING_External_Table
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

SET @Bronze_Stage_Path = 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/'   + 'dbo_' + @TableName + '/SystemLoadID=' + @SystemLoadId + '/**' --using Blob storage access
SET @Bronze_Delta_Path = 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/DeltaLake/' + 'dbo_' + @TableName + '/'--using Blob storage access

print 'Stage Path = ' + @Bronze_Stage_Path
print 'Delta Path = ' + @Bronze_Delta_Path

--print 'TableName = ' + @TableName



SET @FileTimeStamp = (SELECT FORMAT(GetDate(), 'yyyymmddhhmmss'))
SET @TRFile        = (SELECT @TableName + '_' + @SystemLoadId + '_'+ @FileTimeStamp + '.' + @Parquet)
SET @Location      = (SELECT 'N' + '''TestResult/' + @TableName + '/' + @TRFile + '''')

--print 'TRFile   = ' + @TRFile
--print 'Location = ' + @Location

--Setting up with additional parameter with SystemLoadId for staging

--SET @StagePath  = (SELECT StagePath  from QAData.dbo.ConfigData where Environment = 'Dev')
--SET @DeltaPath  = (SELECT DeltaPath  from QAData.dbo.ConfigData where Environment = 'Dev')
--SET @DataSource = (SELECT DataSource from QAData.dbo.ConfigData where Environment = 'Dev')

SET @StagePath  = (SELECT StagePath  from QAData.dbo.ConfigData where Environment =  + @Environment )
SET @DeltaPath  = (SELECT DeltaPath  from QAData.dbo.ConfigData where Environment =  + @Environment )
SET @DataSource = (SELECT DataSource from QAData.dbo.ConfigData where Environment =  + @Environment )
--PRINT 'Stage Path = ' + @StagePath

SET @Bronze_Stage_Path = @StagePath  + @SchemaName + '_' + @TableName + '/SystemLoadID=' + @SystemLoadId + '/**' 
SET @Bronze_Delta_Path = @DeltaPath  + @SchemaName + '_' + @TableName + '/'

PRINT 'Bronze Stage Path from Config = ' + @Bronze_Stage_Path
PRINT 'Bronze Delta Path from Config = ' + @Bronze_Delta_Path

/**************************************************************************************************************************************************************	   
SELECT @BronzeColumnsForHash  = @BronzeColumnsForHash + '[' + ColumnName    + ']' + ',' + '''%''' +',' -- without Collation to be used for SQL DB/Azure SQL DB
**************************************************************************************************************************************************************/
SELECT @BronzeColumnsForHash    = @BronzeColumnsForHash       + '[' + ColumnName      + ']' + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_100_BIN2_UTF8') + ',' + '''%''' +','
      ,@BronzeColumnsForHashSTG = @BronzeColumnsForHashSTG    + '[' + ColumnName      + ']' + ',' + '''%''' +',' -- without Collation to be used for SQL DB/Azure SQL DB
      ,@BronzeColumnsForHashTGT = @BronzeColumnsForHashTGT    + '[' + ColumnName      + ']' + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_100_BIN2_UTF8') + ',' + '''%''' +','
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
			  ,c.name +']' + ' as '  +  '[' + 'STAGE' + c.name   as ColumnNameSTAGE
			  ,c.name +']' + ' as '  +  '[' + 'DELTA' + c.name   as ColumnNameDELTA
			  
          FROM [EclipseBronze].[sys].[schemas] s
          join [EclipseBronze].[sys].[views]   t  on s.schema_id = t.schema_id
          join [EclipseBronze].[sys].[columns] c  on t.object_id = c.object_id 
        )T
 
 WHERE SchemaName = @Schemaname
   AND TableName = @TableName
   AND ColumnName NOT IN ('RowNumber','EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID')


--Tidying up all comma and extra characters
SELECT @BronzeColumnsForHash      = SUBSTRING (@BronzeColumnsForHash,1   ,LEN(@BronzeColumnsForHash)-5)
SELECT @BronzeColumnsForHashSTG   = SUBSTRING (@BronzeColumnsForHashSTG,1   ,LEN(@BronzeColumnsForHashSTG)-5)
SELECT @BronzeColumnsForHashTGT   = SUBSTRING (@BronzeColumnsForHashTGT,1   ,LEN(@BronzeColumnsForHashTGT)-5)
SELECT @BronzeCOLUMNList          = SUBSTRING (@BronzeCOLUMNList,1       ,LEN(@BronzeCOLUMNList)-1)
SELECT @BronzeCOLUMNListSTAGE     = SUBSTRING (@BronzeCOLUMNListSTAGE,1  ,LEN(@BronzeCOLUMNListSTAGE)-1)
SELECT @BronzeCOLUMNListDELTA     = SUBSTRING (@BronzeCOLUMNListDELTA,1  ,LEN(@BronzeCOLUMNListDELTA)-1)


print 'BRONZE Column List = ' + @BronzeCOLUMNList
print 'Bronze STG Hash Columns = ' + @BronzeColumnsForHashSTG

SELECT @JoinCondition = @JoinCondition + JoinConditionNew + CHAR(10) + ' and '  
  FROM (

SELECT --STR_Column, T.ColumnName,CollationName,
       'STAGE.STAGE' + T.ColumnName + ' ' + REPLACE(ISNULL(T.CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE DATABASE_DEFAULT') + ' = ' + 
	   'DELTA.DELTA' + T.ColumnName + ' ' + REPLACE(ISNULL(T.CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE DATABASE_DEFAULT') as JoinConditionNew

	   --query to separate the list of composite columns for join condition derivation
  FROM (
         SELECT value as STR_Column
           FROM STRING_SPLIT(@tags, ',')
           WHERE RTRIM(value) <> ''
		)STR

Left Join 

        (
         SELECT 
                s.schema_id                                       as SchemaId
               ,s.name                                            as SchemaName 
               ,t.name                                            as TableName
               ,c.column_id                                       as ColumnId
               ,c.collation_name                                  as CollationName
               ,c.name                                            as ColumnName
               ,c.name +']' + ' as '  +  '[' + 'STAGE' + c.name   as ColumnNameSTAGE
         	   ,c.name +']' + ' as '  +  '[' + 'DELTA' + c.name   as ColumnNameDELTA
           FROM [EclipseBronze].[sys].[schemas] s
           join [EclipseBronze].[sys].[views]   t  on s.schema_id = t.schema_id
           join [EclipseBronze].[sys].[columns] c  on t.object_id = c.object_id 
           )T
 on STR.STR_Column = T.ColumnName
--and TableName = 'ReportingClass'
and TableName = @TableName
)TT


SELECT @JoinCondition      = SUBSTRING (@JoinCondition,1   ,LEN(@JoinCondition)-4)
print 'Join Condition = ' + @JoinCondition



SELECT @QRY = 'USE ' + @QADatabasename                                                                                                                           + CHAR(10) + 
              'IF EXISTS (SELECT [name] FROM QAData.sys.external_tables WHERE [name] = ' + ''''+ @EXISTING_External_Table +'''' +')'                             + CHAR(10) +
			  'BEGIN '                                                                                                                                           + CHAR(10) + 
			  '      DROP EXTERNAL Table ' + @EXISTING_External_Table                                                                                            + CHAR(10) + 
			  '  END;'                                                                                                                                           + CHAR(10) +

			  'CREATE EXTERNAL TABLE ' + @QADatabasename + '.' + @QASchemaName + '.' + 'TR_'+ @TableName                                                         + CHAR(10) + 
              'WITH ( '                                                                                                                                          + CHAR(10) +                          
              '      DATA_SOURCE =   ' + @DataSource          	                                                                                                 + CHAR(10) + 
			  '     ,LOCATION    =   ' + @Location                                                                                                               + CHAR(10) +
	          '     ,FILE_FORMAT =   ' + @QAFileFormat                                                                                                           + CHAR(10) +
              '    ) AS '                                                                                                                                        + CHAR(10) +
			  

              'With '                                                                                                                                            + CHAR(10) + 
--Building query for Staging data from PARQUET file
                    ' CTE_Bronze_Stage as ('                                                                                                                     + CHAR(10) + 
						 --'     SELECT ' + @BronzeCOLUMNListSTAGE                                                                                                 + CHAR(10) + 
						 '     SELECT ' + @BronzeCOLUMNListSTAGE                                                                                                 + CHAR(10) + 
						 --'           ,HASHBYTES(' + '''SHA1''' + ',' + 'CONVERT(NVARCHAR(MAX),CONCAT(' + @BronzeColumnsForHashSTG + ')'+ ')' + ')' + 'as Bronze_Stage_HashBytes'               + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1''' + ',' + 'CONVERT(NVARCHAR(MAX),CONCAT(' + @BronzeColumnsForHashSTG + ')'+ ')' + ')' + 'as Bronze_Stage_HashBytes'               + CHAR(10) + 
						 '       FROM OPENROWSET ( '                                                                                                             + CHAR(10) + 
						 '             BULK '     + '''' + @Bronze_Stage_Path + ''''+ ','                                                                        + CHAR(10) + 
                         '             FORMAT = ' + ''''+ @Parquet +''''	                                                                                     + CHAR(10) +
						 '                          ) as RS'                                                                                                     + CHAR(10) +    
							')'                                                                                                                                  + CHAR(10) +    

--Building query for Bronze Delta from Synopse view
					',CTE_Bronze_Delta as ('                                                                                                                     + CHAR(10) + 
                         '     SELECT ' + @BronzeCOLUMNListDELTA                                                                                                 + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1''' + ',' + 'CONVERT(NVARCHAR(MAX),CONCAT(' + @BronzeColumnsForHashSTG + ')'+ ')' + ')' + 'as Bronze_Delta_HashBytes'               + CHAR(10) + 
						 --'       FROM [EclipseBronze].[dbo].' + @TableName                                                                                       + CHAR(10) + 
						 '       FROM OPENROWSET ( '                                                                                                             + CHAR(10) + 
						 '             BULK '     + '''' + @Bronze_Delta_Path + ''''+ ','                                                                        + CHAR(10) + 
                         '             FORMAT = ' + '''' + @Parquet +''''	                                                                                     + CHAR(10) +
						 '                          ) as TR'                                                                                                     + CHAR(10) +    
						 '    WHERE [BronzeStagingSystemLoadID] = ' + @SystemLoadId                                                                                + CHAR(10) +    
							')'                                                                                                                                  + CHAR(10) +
							
----------------------Building Comparison Query between Stage & Delta
                         '     SELECT ' + '''' + @@ServerName       + '''' + ' as SynapseServerName  '                                                           + CHAR(10) +
						 '           ,     ''' + @SystemName        + '''' + ' as SystemName '                                                                   + CHAR(10) +
						 '           ,     ''' + @SourceObjectName  + '''' + ' as SynapseViewName '                                                              + CHAR(10) +
						 '           ,     ''' + @SystemLoadId      + '''' + ' as SystemLoadId '                                                                 + CHAR(10) +
						 '           ,     ''' + @SystemDescription + '''' + ' as SystemDescription '                                                            + CHAR(10) +
						 '           ,     ''' + @ObjectRunId       + '''' + ' as ObjectRunId '                                                                  + CHAR(10) +
						 '           ,     ''' + @TRFile            + '''' + ' as TestResultFileName '                                                                   + CHAR(10) +
						 '           ,STAGE.*,DELTA.* '                                                                                                          + CHAR(10) +
                         '           ,Case WHEN Bronze_Stage_HashBytes = Bronze_Delta_HashBytes '                                                                + CHAR(10) +
	                     '                 THEN ' + '''Passed'''                                                                                                 + CHAR(10) + 
			             '                 ELSE ' + '''Failed'''                                                                                                 + CHAR(10) +
		                 '             End as TestResult'                                                                                                        + CHAR(10) + 
						 '           ,GetDate() ' + 'as TestedOn '                                                                                               + CHAR(10) +
						 '       FROM ' + ' CTE_Bronze_Stage STAGE'                                                                                              + CHAR(10) +
						 '       left join  CTE_Bronze_Delta DELTA'                                                                                              + CHAR(10) + 
--						 '              on Bronze_Stage_HashBytes = Bronze_Delta_HashBytes '                                                              
						 '              on ' + @JoinCondition

--print @QRY
EXECUTE sp_executesql @QRY

END;
GO

/*********************************************************************************************************************************************************************************************************************************************************************************************************************

EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_usingParquet] @TableName = 'ClaimStatus' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_usingParquet] @TableName = 'BusinessCode' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_usingParquet] @TableName = 'Org' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_usingParquet] @TableName = 'PolicyOrg' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_usingParquet] @TableName = 'LloydsBroker' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_usingParquet] @TableName = 'ApplicationUser' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_usingParquet] @TableName = 'PolicyLine' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_usingParquet] @TableName = 'PolicyLimit' 

SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_ClaimStatus GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_BusinessCode GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_ApplicationUser GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_PolicyLine GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_PolicyLimit GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_Org GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_PolicyOrg GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_LloydsBroker GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)
********************************************************************************************************************************************************************************************************************************************************************************************************************/