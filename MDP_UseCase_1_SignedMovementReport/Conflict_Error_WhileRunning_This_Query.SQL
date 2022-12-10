select 
        s.schema_id as SchemaId
	   ,s.name      as SchemName 
	   ,t.name      as TableName
	   ,c.column_id AS ColumnId
	   ,c.collation_name
	   ,c.name      AS ColumnName
  FROM [EclipseBronze].[sys].[schemas] s
  JOIN [EclipseBronze].[sys].[views]   t  ON s.schema_id = t.schema_id
  JOIN [EclipseBronze].[sys].[columns] c  ON t.object_id = c.object_id 

--WHERE t.name = 'Country'
WHERE t.name = 'ClaimStatus'
--This SQL file works for CTE to SELECT Stage Data from Parquet file and it's WORKING OK 

USE [EclipseBronze];
--exec sys.sp_refreshview @viewname='dbo.ClaimStatus'

DECLARE 
	    @BronzeCOLUMNS		NVARCHAR(MAX) = '' 
	   ,@BronzeCOLUMNList	NVARCHAR(MAX) = ''
	   	   
SELECT @BronzeCOLUMNS        = @BronzeCOLUMNS       + '[' + ColumnName    + ']' + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_100_BIN2_UTF8') + ',' + '''%''' +','
      ,@BronzeCOLUMNList     = @BronzeCOLUMNList    + '[' + ColumnName    + ']' + ','
  FROM (
SELECT 
        s.schema_id AS SchemaId
	   ,s.name      AS SchemName 
	   ,t.name      AS TableName
	   ,c.column_id AS ColumnId
	   ,c.collation_name AS CollationName
	   ,c.name      AS ColumnName
  FROM [EclipseBronze].[sys].[schemas] s
  JOIN [EclipseBronze].[sys].[views]   t  ON s.schema_id = t.schema_id
  JOIN [EclipseBronze].[sys].[columns] c  ON t.object_id = c.object_id 
  )T
 WHERE TableName = 'ClaimStatus'
   AND ColumnName NOT IN ('EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID')


   PRINT @BronzeCOLUMNS

SELECT REPLACE('Ram Baskar','Ram Basar','Correct')

--   ,HASHBYTES('SHA1',CONCAT([ClaimStatus] COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc] COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy]  COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]))as Bronze_Target_HashBytes

--------------------------------------------------

--This SQL file works for CTE to SELECT Stage Data from Parquet file and it's WORKING OK 

USE [EclipseBronze];
--exec sys.sp_refreshview @viewname='dbo.ClaimStatus'

DECLARE 
	    @BronzeCOLUMNS		NVARCHAR(MAX) = '' 
	   ,@BronzeCOLUMNList	NVARCHAR(MAX) = ''
	   ,@QRY				NVARCHAR(MAX) = ''
	   ,@Bronze_Delta       NVARCHAR(MAX) = 'underwriting/Internal/Eclipse/DeltaLake/dbo_ClaimStatus/*.parquet'
       ,@Bronze_Stage       NVARCHAR(MAX) = 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_ClaimStatus/*/**'
	   ,@DATASource         NVARCHAR(MAX) = 'Bronze'
	   ,@DATASource1        NVARCHAR(MAX) = 'Bronze'
	   ,@FORMAT             NVARCHAR(MAX) = 'Delta'
	   ,@Parquet            NVARCHAR(MAX) = 'PARQUET'
	   
--SELECT @BronzeCOLUMNS        = @BronzeCOLUMNS       + '[' + ColumnName    + ']' + ',' + '''%''' +','
SELECT @BronzeCOLUMNS        = @BronzeCOLUMNS       + '[' + ColumnName    + ']' + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_100_BIN2_UTF8') + ',' + '''%''' +','
      ,@BronzeCOLUMNList     = @BronzeCOLUMNList    + '[' + ColumnName    + ']' + ','
  from (
select 
        s.schema_id as SchemaId
	   ,s.name      as SchemName 
	   ,t.name      as TableName
	   ,c.column_id AS ColumnId
	   ,c.collation_name AS CollationName
	   ,c.name      AS ColumnName
  FROM [EclipseBronze].[sys].[schemas] s
  JOIN [EclipseBronze].[sys].[views]   t  ON s.schema_id = t.schema_id
  JOIN [EclipseBronze].[sys].[columns] c  ON t.object_id = c.object_id 
  )T
 WHERE TableName = 'ClaimStatus'
   AND ColumnName NOT IN ('EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID')

SELECT @BronzeCOLUMNS       = SUBSTRING (@BronzeCOLUMNS   ,1 ,LEN(@BronzeCOLUMNS)-5)
--print @BronzeCOLUMNS 
SELECT @BronzeCOLUMNList    = SUBSTRING (@BronzeCOLUMNList,1 ,LEN(@BronzeCOLUMNList)-1)
--print @BronzeCOLUMNList

SELECT @QRY = 'With CTE_Bronze_Source as ('                                                                                                         + CHAR(10) + 
						 ('     SELECT ' + @BronzeCOLUMNList )                                                                                      + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1''' + ',' + 'CONCAT(' + @BronzeCOLUMNS + ')' + ')' + 'as Bronze_Stage_HashBytes'     + CHAR(10) + 
						 '       FROM OPENROWSET ( '                                                                                                + CHAR(10) + 
						 '             BULK ' + '''' + @Bronze_Stage + ''''+ ','                                                                      + CHAR(10) + 
                         '             FORMAT = ' + ''''+@Parquet+''''	                                                                            + CHAR(10) +
						 '                          ) as RS'                                                                                        + CHAR(10) +    
							')'                                                                                                                     + CHAR(10) +    
						',CTE_Bronze_Target as ('                                                                                                   + CHAR(10) + 
						 ('     SELECT ' + @BronzeCOLUMNList )                                                                                      + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1''' + ',' + 'CONCAT(' + @BronzeCOLUMNS + ')' + ')' + 'as Bronze_Target_HashBytes'    + CHAR(10) + 
						 '       FROM [EclipseBronze].[dbo].[ClaimStatus] '                                                                                                + CHAR(10) + 
						    
							')'                                                                                                                     + CHAR(10) +
							
--Building Comparison Query between Prod & Dev

							'SELECT STG.*,TGT.*,Bronze_Stage_HashBytes,Bronze_Target_HashBytes'                                                                   + CHAR(10) +
							'FROM ' + ' CTE_Bronze_Source STG'                                                                                      + CHAR(10) +
							'FULL JOIN  CTE_Bronze_Target TGT'                                                                                      + CHAR(10) + 
							'       on Bronze_Stage_HashBytes = Bronze_Target_HashBytes '                                                                                 -- + CHAR(10) +
											

	

print @QRY
--EXECUTE sp_executesql @QRY
--EXEC (@QRY)



With CTE_Bronze_Source as (
     SELECT [ClaimStatus],[ClaimStatusID],[Dsc],[OwnerID],[InsDate],[DelDate],[LastUpd],[UpdBy],[SYS_CHANGE_OPERATION]
           ,HASHBYTES('SHA1',CONCAT([ClaimStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8))as Bronze_Stage_HashBytes
       FROM OPENROWSET ( 
             BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_ClaimStatus/*/**',
             FORMAT = 'PARQUET'
                          ) as RS
)
,CTE_Bronze_Target as (
     SELECT [ClaimStatus],[ClaimStatusID],[Dsc],[OwnerID],[InsDate],[DelDate],[LastUpd],[UpdBy],[SYS_CHANGE_OPERATION]
           ,HASHBYTES('SHA1',CONCAT([ClaimStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8))as Bronze_Target_HashBytes
       FROM [EclipseBronze].[dbo].[ClaimStatus] 
)
SELECT STG.*,TGT.*,Bronze_Stage_HashBytes,Bronze_Target_HashBytes
FROM  CTE_Bronze_Source STG
FULL JOIN  CTE_Bronze_Target TGT
       on Bronze_Stage_HashBytes = Bronze_Target_HashBytes 