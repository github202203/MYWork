--Query works fine with DateTime2 datatype ( Script for Demo)

With 
 CTE_Bronze_Stage as (
     SELECT [ClaimStatus] as [ClaimStatusSTAGE],[ClaimStatusID] as [ClaimStatusIDSTAGE],[Dsc] as [DscSTAGE],[OwnerID] as [OwnerIDSTAGE],[InsDate] as [InsDateSTAGE],[DelDate] as [DelDateSTAGE],[LastUpd] as [LastUpdSTAGE],[UpdBy] as [UpdBySTAGE],[SYS_CHANGE_OPERATION] as [SYS_CHANGE_OPERATIONSTAGE]
		   ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([ClaimStatus],'%',[ClaimStatusID],'%',[Dsc],'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy],'%',[SYS_CHANGE_OPERATION])))as Bronze_Stage_HashBytes
       FROM OPENROWSET ( 
             BULK 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_ClaimStatus/SystemLoadID=1022022101001/**',
             FORMAT = 'parquet'
                          ) as RS
)
,CTE_Bronze_Delta as (
     SELECT [ClaimStatus] as [ClaimStatusDELTA],[ClaimStatusID] as [ClaimStatusIDDELTA],[Dsc] as [DscDELTA],[OwnerID] as [OwnerIDDELTA],[InsDate] as [InsDateDELTA],[DelDate] as [DelDateDELTA],[LastUpd] as [LastUpdDELTA],[UpdBy] as [UpdByDELTA],[SYS_CHANGE_OPERATION] as [SYS_CHANGE_OPERATIONDELTA]
           ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([ClaimStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8)))as Bronze_Delta_HashBytes
       --FROM [EclipseBronze].[dbo].ClaimStatus
	   FROM [QAData].[dbo].[QAClaimStatus] -- works fine with DateTime2 datatype for all the date related attributes
)
     SELECT STAGE.*,DELTA.* 
           ,Case WHEN Bronze_Stage_HashBytes = Bronze_Delta_HashBytes 
                 THEN 'Passed'
                 ELSE 'Failed'
             End as TestResult
           ,GetDate() as TestedOn 
       FROM  CTE_Bronze_Stage STAGE
       left join  CTE_Bronze_Delta DELTA
              on ClaimStatusIDSTAGE = ClaimStatusIDDELTA