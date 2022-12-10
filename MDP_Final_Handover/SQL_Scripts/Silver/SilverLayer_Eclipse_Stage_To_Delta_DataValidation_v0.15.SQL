--USE [QAData];

/********************************************************************************************************************************************
--------------------------------------------------------------------------------------------------------------------------------------------- 
   Version  |  Date        | Author              | Description                                                                  
--------------------------------------------------------------------------------------------------------------------------------------------- 


   0.1      |  05/10/2022  | Ram Baskar          | Parameterised query (TableName)to compare data in Bronze Layer between Staging & Delta 
   0.2      |  12/10/2022  | Ram Baskar          | Included second parameter (Bronze SystemLoadID 
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
   0.14     |  16/11/2022  | Ram Baskar          | Changed the Procedure name with meaningful name

   0.15     |  26/11/2022  | Ram Baskar          | Changed the code to test Silver Layer stage - delta 

   Parameter 1 - TableName                                                                                                      
   Syntax to execute this code

   --EXEC [QAData].[dbo].[SilverLayer_Eclipse_Stage_To_Delta_DataValidation] @TableName = 'Policy'
   
   *********************************** This script will work only for tables with less than 254 columns ********************************************
----------------------------------------------------------------------------------------------------------------------------------------------------------- 
**********************************************************************************************************************************************************/

--USE [QAData];



CREATE or ALTER Procedure [dbo].[SilverLayer_Eclipse_Stage_To_Delta_DataValidation] (@TableName VARCHAR(100))
AS 
BEGIN



DECLARE 

	    @Delta                      NVARCHAR(MAX) = 'Delta'
	   ,@Parquet					NVARCHAR(MAX) = 'parquet'
	   --,@TableName					NVARCHAR(MAX) = 'Policy' 
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
	   ,@Schemaname                 NVARCHAR(MAX) = 'delta'
	   ,@QSchemaname                NVARCHAR(MAX) = 'dbo'
	   ,@ServerName                 NVARCHAR(MAX) = ''
	   ,@Environment                NVARCHAR(MAX) = '' 
	   ,@SystemLoadId				NVARCHAR(MAX) = ''
	   ,@SilverCOLUMNList	        NVARCHAR(MAX) = ''
	   ,@SilverCOLUMNListSTAGE      NVARCHAR(MAX) = ''
	   ,@SilverCOLUMNListDELTA      NVARCHAR(MAX) = ''
	   ,@SilverColumnsForHash		NVARCHAR(MAX) = '' 
	   ,@SilverColumnsForHashSTG	NVARCHAR(MAX) = '' 
	   ,@SilverColumnsForHashTGT	NVARCHAR(MAX) = '' 
	   ,@StagePath                  NVARCHAR(MAX) = ''
	   ,@DeltaPath                  NVARCHAR(MAX) = ''
	   ,@Silver_Stage_Path          NVARCHAR(MAX) = '' 
	   ,@Silver_Delta_Path          NVARCHAR(MAX) = ''
	   ,@DataSource                 NVARCHAR(MAX) = ''
	   ,@TRFile                     NVARCHAR(MAX) = ''
	   ,@FileTimeStamp	            NVARCHAR(MAX) = ''
	   ,@Location                   NVARCHAR(MAX) = ''
	   ,@QRY				        NVARCHAR(MAX) = ''
	   ,@EXT				        NVARCHAR(MAX) = ''
	   ,@FINAL				        NVARCHAR(MAX) = ''
	   ,@EXISTING_External_Table    NVARCHAR(MAX) = ''
	   ,@TestType                    VARCHAR(200) = 'Silver Stage --> Silver Delta Data Reconciliiation/Data Completeness/Data Correctness'
	   ,@Layer                       VARCHAR(10)  = 'Silver'
;	

SELECT TOP 1 * FROM [EclipseBronze].dbo.Policy;

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

SET @EXISTING_External_Table    = 'TR_Silver_'+ @TableName 
SET @SystemName                 = (SELECT SystemName        FROM QAData.dbo.QAControlData WHERE SourceObjectName = @QSchemaName + '.' + @TableName and SystemLoadId like '104%')
SET @SourceObjectId             = (SELECT SourceObjectId    FROM QAData.dbo.QAControlData WHERE SourceObjectName = @QSchemaName + '.' + @TableName and SystemLoadId like '104%')
SET @SourceObjectName           = (SELECT SourceObjectName  FROM QAData.dbo.QAControlData WHERE SourceObjectName = @QSchemaName + '.' + @TableName and SystemLoadId like '104%')
SET @SystemLoadId               = (SELECT SystemLoadId      FROM QAData.dbo.QAControlData WHERE SourceObjectName = @QSchemaName + '.' + @TableName and SystemLoadId like '104%')
SET @UniqueColumn               = (SELECT UniqueColumn      FROM QAData.dbo.QAControlData WHERE SourceObjectName = @QSchemaName + '.' + @TableName and SystemLoadId like '104%')
SET @Tags                       = (SELECT UniqueColumn      FROM QAData.dbo.QAControlData WHERE SourceObjectName = @QSchemaName + '.' + @TableName and SystemLoadId like '104%')
SET @ObjectRunId                = (SELECT ObjectRunId       FROM QAData.dbo.QAControlData WHERE SourceObjectName = @QSchemaName + '.' + @TableName and SystemLoadId like '104%')
SET @SystemDescription          = (SELECT SystemDescription FROM QAData.dbo.QAControlData WHERE SourceObjectName = @QSchemaName + '.' + @TableName and SystemLoadId like '104%')
SET @FileTimeStamp              = (SELECT format(GetDate(), 'yyyyMMddHHmmss'))
SET @TRFile                     = (SELECT @TableName + '_' + @SystemLoadId + '_'+ @FileTimeStamp + '.' + @Parquet)
SET @Location                   = (SELECT 'N' + '''SITResult/' + @Layer + '/' + @TableName +  '/' + @TRFile + '''')
SET @StagePath                  = (SELECT SilverStagePath  from QAData.dbo.ConfigData where Environment =  + @Environment )
SET @DeltaPath                  = (SELECT SilverDeltaPath  from QAData.dbo.ConfigData where Environment =  + @Environment )
SET @DataSource                 = (SELECT DataSource from QAData.dbo.ConfigData where Environment =  + @Environment )
SET @Silver_Stage_Path          = @StagePath  + @QSchemaName + '_' + @TableName + '/SystemLoadID=' + @SystemLoadId + '/**' 
SET @Silver_Delta_Path          = @DeltaPath  + @QSchemaName + '_' + @TableName + '/'


SELECT @SilverColumnsForHash    = @SilverColumnsForHash       + '[' + ColumnName      + ']' + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_100_BIN2_UTF8') + ',' + '''%''' +','
      ,@SilverColumnsForHashSTG = @SilverColumnsForHashSTG    + '[' + ColumnName      + ']' + ',' + '''%''' +',' -- without Collation to be used for SQL DB/Azure SQL DB
      ,@SilverColumnsForHashTGT = @SilverColumnsForHashTGT    + '[' + ColumnName      + ']' + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_100_BIN2_UTF8') + ',' + '''%''' +','
      ,@SilverCOLUMNListSTAGE   = @SilverCOLUMNListSTAGE      + '[' + ColumnNameSTAGE + ']' + ','
	  ,@SilverCOLUMNListDELTA   = @SilverCOLUMNListDELTA      + '[' + ColumnNameDELTA + ']' + ','
	  ,@SilverCOLUMNList        = @SilverCOLUMNList           + '[' + ColumnName      + ']' + ','
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
			  
          FROM [EclipseSilver].[sys].[schemas] s
          join [EclipseSilver].[sys].[views]   t  on s.schema_id = t.schema_id
          join [EclipseSilver].[sys].[columns] c  on t.object_id = c.object_id 
        )T
 
 WHERE SchemaName = @Schemaname
   AND TableName = 'dbo_' + @TableName
   AND ColumnName NOT IN ('RowNumber','EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID','SilverStagingSystemLoadID','SilverSystemLoadID')

SELECT @SilverColumnsForHash    = SUBSTRING (@SilverColumnsForHash,1   ,LEN(@SilverColumnsForHash)-5)
SELECT @SilverColumnsForHashSTG = SUBSTRING (@SilverColumnsForHashSTG,1,LEN(@SilverColumnsForHashSTG)-5)
SELECT @SilverColumnsForHashTGT = SUBSTRING (@SilverColumnsForHashTGT,1,LEN(@SilverColumnsForHashTGT)-5)
SELECT @SilverCOLUMNList        = SUBSTRING (@SilverCOLUMNList,1       ,LEN(@SilverCOLUMNList)-1)
SELECT @SilverCOLUMNListSTAGE   = SUBSTRING (@SilverCOLUMNListSTAGE,1  ,LEN(@SilverCOLUMNListSTAGE)-1)
SELECT @SilverCOLUMNListDELTA   = SUBSTRING (@SilverCOLUMNListDELTA,1  ,LEN(@SilverCOLUMNListDELTA)-1)


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
           FROM [EclipseSilver].[sys].[schemas] s
           join [EclipseSilver].[sys].[views]   t  on s.schema_id = t.schema_id
           join [EclipseSilver].[sys].[columns] c  on t.object_id = c.object_id 
           )T
      on STR.STR_Column = T.ColumnName
     and SchemaName = @Schemaname
     and TableName  = @QSchemaName +'_' + @TableName
)TT


SELECT @JoinCondition      = SUBSTRING (@JoinCondition,1   ,LEN(@JoinCondition)-4)

SELECT @QRY = 'USE ' + @QADatabasename                                                                                                                           + CHAR(10) + 
              'IF EXISTS (SELECT [name] FROM QAData.sys.external_tables WHERE [name] = ' + ''''+ @EXISTING_External_Table +'''' +')'                             + CHAR(10) +
			  'BEGIN '                                                                                                                                           + CHAR(10) + 
			  '      DROP EXTERNAL Table ' + @EXISTING_External_Table                                                                                            + CHAR(10) + 
			  '  END;'                                                                                                                                           + CHAR(10) +

			  'CREATE EXTERNAL TABLE ' + @QADatabasename + '.' + @QASchemaName + '.' + 'TR_Silver_'+ @TableName                                                  + CHAR(10) + 
              'WITH ( '                                                                                                                                          + CHAR(10) +                          
              '      DATA_SOURCE =   ' + @DataSource          	                                                                                                 + CHAR(10) + 
			  '     ,LOCATION    =   ' + @Location                                                                                                               + CHAR(10) +
	          '     ,FILE_FORMAT =   ' + @QAFileFormat                                                                                                           + CHAR(10) +
              '    ) AS '                                                                                                                                        + CHAR(10) +
			  

              'With '                                                                                                                                            + CHAR(10) + 
--Building query for Silver Staging data from PARQUET file
                    ' CTE_Silver_Stage as ('                                                                                                                     + CHAR(10) + 
						 '     SELECT ' + @SilverCOLUMNListSTAGE                                                                                                 + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1''' + ',' + 'CONVERT(NVARCHAR(MAX),CONCAT(' + @SilverColumnsForHashSTG + ')'+ ')' + ')' + 'as Silver_Stage_HashBytes'               + CHAR(10) + 
						 '       FROM OPENROWSET ( '                                                                                                             + CHAR(10) + 
						 '             BULK '     + '''' + @Silver_Stage_Path + ''''+ ','                                                                        + CHAR(10) + 
                         '             FORMAT = ' + ''''+ @Parquet +''''	                                                                                     + CHAR(10) +
						 '                          ) as RS'                                                                                                     + CHAR(10) +    
							')'                                                                                                                                  + CHAR(10) +    

--Building query for Silver Delta from Synopse view
					',CTE_Silver_Delta as ('                                                                                                                     + CHAR(10) + 
                         '     SELECT ' + @SilverCOLUMNListDELTA                                                                                                 + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1''' + ',' + 'CONVERT(NVARCHAR(MAX),CONCAT(' + @SilverColumnsForHashSTG + ')'+ ')' + ')' + 'as Silver_Delta_HashBytes'               + CHAR(10) + 
						 '       FROM OPENROWSET ( '                                                                                                             + CHAR(10) + 
						 '             BULK '     + '''' + @Silver_Delta_Path + ''''+ ','                                                                        + CHAR(10) + 
                         '             FORMAT = ' + '''' + @Parquet +''''	                                                                                     + CHAR(10) +
						 '                          ) as TR'                                                                                                     + CHAR(10) +    
						 '    WHERE [SilverStagingSystemLoadID] = ' + @SystemLoadId                                                                              + CHAR(10) +    
							')'                                                                                                                                  + CHAR(10) +
							
----------------------Building Comparison Query between SilverStage & SilverDelta
                         '     SELECT ' + '''' + @@ServerName       + '''' + '                  as SynapseServerName  '                                          + CHAR(10) +
						 '           ,     ''' + @Layer             + '''' + '                  as Layer '                                                       + CHAR(10) +
						 '           ,     ''' + REPLACE(@SourceObjectName,'.','_')  + '''' + ' as Entity '                                                      + CHAR(10) +
						 '           ,     ''' + @TestType          + '''' + '                  as TestType '                                                    + CHAR(10) +
						 '           ,     ''' + @SystemName        + '''' + '                  as SystemName '                                                  + CHAR(10) +
						 '           ,     ''' + @SystemLoadId      + '''' + '                  as SystemLoadId '                                                + CHAR(10) +
						 '           ,     ''' + @ObjectRunId       + '''' + '                  as ObjectRunId '                                                 + CHAR(10) +
						 '           ,     ''' + @TRFile            + '''' + '                  as TestResultFileName '                                          + CHAR(10) +
                         '           ,Case WHEN Silver_Stage_HashBytes = Silver_Delta_HashBytes '                                                                + CHAR(10) +
	                     '                 THEN ' + '''Passed'''                                                                                                 + CHAR(10) + 
			             '                 ELSE ' + '''Failed'''                                                                                                 + CHAR(10) +
		                 '             End                                                      as TestResult'                                                   + CHAR(10) + 
						 '           ,GetDate() ' + '                                           as TestedOn '                                                    + CHAR(10) +
						 '           ,STAGE.*,DELTA.* '                                                                                                          + CHAR(10) +
						 '       FROM ' + ' CTE_Silver_Stage STAGE'                                                                                              + CHAR(10) +
						 '       left join  CTE_Silver_Delta DELTA'                                                                                              + CHAR(10) + 
						 '              on ' + @JoinCondition

--print @QRY
EXECUTE sp_executesql @QRY

END;
GO
