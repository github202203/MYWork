use QAData;

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[testresult]') AND type in (N'U'))
DROP EXTERNAL TABLE [dbo].[testresult]
GO

CREATE EXTERNAL TABLE QAData.dbo.testresult
WITH (
      DATA_SOURCE = [qadata_dldpdev01_dfs_core_windows_net]
	 ,LOCATION    = N'TestResult/ClaimStatus/tr7.parquet'
	 --,FILE_FORMAT = [SynapseDelimitedTextFormat]
	 ,FILE_FORMAT = [SynapseParquetFormat]
      )  
AS

With 
 CTE_Bronze_Stage as (
     SELECT [ClaimStatus] as [ClaimStatusSTAGE],[ClaimStatusID] as [ClaimStatusIDSTAGE],[Dsc] as [DscSTAGE],[OwnerID] as [OwnerIDSTAGE],[InsDate] as [InsDateSTAGE],[DelDate] as [DelDateSTAGE],[LastUpd] as [LastUpdSTAGE],[UpdBy] as [UpdBySTAGE],[SYS_CHANGE_OPERATION] as [SYS_CHANGE_OPERATIONSTAGE]
           ,HASHBYTES('SHA1',CONCAT([ClaimStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8))as Bronze_Stage_HashBytes
       FROM OPENROWSET ( 
             BULK 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_ClaimStatus/SystemLoadID=1022022101001/**',
             FORMAT = 'PARQUET'
                          ) as RS
)
,CTE_Bronze_Delta as (
     SELECT [ClaimStatus] as [ClaimStatusDELTA],[ClaimStatusID] as [ClaimStatusIDDELTA],[Dsc] as [DscDELTA],[OwnerID] as [OwnerIDDELTA],[InsDate] as [InsDateDELTA],[DelDate] as [DelDateDELTA],[LastUpd] as [LastUpdDELTA],[UpdBy] as [UpdByDELTA],[SYS_CHANGE_OPERATION] as [SYS_CHANGE_OPERATIONDELTA]
           ,HASHBYTES('SHA1',CONCAT([ClaimStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8))as Bronze_Delta_HashBytes
       FROM [EclipseBronze].[dbo].ClaimStatus
)

,ExpectedResult as (
     SELECT STAGE.*,DELTA.* 
           ,Case WHEN Bronze_Stage_HashBytes = Bronze_Delta_HashBytes 
                 THEN 'Passed'
                 ELSE 'Failed'
             End as TestResult
           ,GetDate() as TestedOn 
       FROM  CTE_Bronze_Stage STAGE
       left join  CTE_Bronze_Delta DELTA
              --on Bronze_Stage_HashBytes = Bronze_Delta_HashBytes 
	    on ClaimStatusIDSTAGE = ClaimStatusIDDELTA
)

SELECT * FROM ExpectedResult

select * from QAData.dbo.testresult



