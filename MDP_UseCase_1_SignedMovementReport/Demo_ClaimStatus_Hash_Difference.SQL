--Quick Demo for Tahir 

--Compare Hash Bytes Stage Parquet /Delta Parquet using Parquet files


With 
    CTE_Bronze_Source as (
        SELECT [ClaimStatus],[ClaimStatusID],[Dsc],[OwnerID],[InsDate],[DelDate],[LastUpd],[UpdBy],[SYS_CHANGE_OPERATION]
              ,HASHBYTES('SHA1',CONCAT([ClaimStatus],'%',[ClaimStatusID],'%',[Dsc],'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy],'%',[SYS_CHANGE_OPERATION]))as Bronze_Source_HashBytes
          FROM OPENROWSET ( 
               BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_ClaimStatus/*/**',
               FORMAT = 'PARQUET'
                          ) as SR
    )
   ,CTE_Bronze_Target as (
        SELECT [ClaimStatus],[ClaimStatusID],[Dsc],[OwnerID],[InsDate],[DelDate],[LastUpd],[UpdBy],[SYS_CHANGE_OPERATION]
              ,HASHBYTES('SHA1',CONCAT([ClaimStatus],'%',[ClaimStatusID],'%',[Dsc],'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy],'%',[SYS_CHANGE_OPERATION]))as Bronze_Delta_HashBytes
          FROM
               OPENROWSET (
               BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/DeltaLake/dbo_ClaimStatus/',
               FORMAT = 'Delta'
                           ) as TR
    )

        SELECT STG.*,TGT.*
		      ,CASE WHEN Bronze_Source_HashBytes = Bronze_Delta_HashBytes THEN 'Passed' ELSE 'Failed' END AS TestResult
          FROM  CTE_Bronze_Source STG
          Left Join CTE_Bronze_Target TGT on Bronze_Source_HashBytes = Bronze_Delta_HashBytes 
		  --Left Join CTE_Bronze_Target TGT on STG.ClaimStatusId = TGT.ClaimStatusId 
;

--Compare Hash Bytes Stage Parquet/Delta Synapse view 
With 
 CTE_Bronze_Stage as (
     SELECT [ClaimStatus] as [ClaimStatusSTAGE],[ClaimStatusID] as [ClaimStatusIDSTAGE],[Dsc] as [DscSTAGE],[OwnerID] as [OwnerIDSTAGE],[InsDate] as [InsDateSTAGE],[DelDate] as [DelDateSTAGE],[LastUpd] as [LastUpdSTAGE],[UpdBy] as [UpdBySTAGE],[SYS_CHANGE_OPERATION] as [SYS_CHANGE_OPERATIONSTAGE]
           --,HASHBYTES('SHA1',CONCAT([ClaimStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8))as Bronze_Stage_HashBytes
		   ,HASHBYTES('SHA1',CONCAT([ClaimStatus],'%',[ClaimStatusID],'%',[Dsc],'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy],'%',[SYS_CHANGE_OPERATION]))as Bronze_Stage_HashBytes
       FROM OPENROWSET ( 
             BULK 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_ClaimStatus/SystemLoadID=1022022101001/**',
             FORMAT = 'parquet'
                          ) as RS
)
,CTE_Bronze_Delta as (
     SELECT [ClaimStatus] as [ClaimStatusDELTA],[ClaimStatusID] as [ClaimStatusIDDELTA],[Dsc] as [DscDELTA],[OwnerID] as [OwnerIDDELTA],[InsDate] as [InsDateDELTA],[DelDate] as [DelDateDELTA],[LastUpd] as [LastUpdDELTA],[UpdBy] as [UpdByDELTA],[SYS_CHANGE_OPERATION] as [SYS_CHANGE_OPERATIONDELTA]
           ,HASHBYTES('SHA1',CONCAT([ClaimStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8))as Bronze_Delta_HashBytes
		   --,HASHBYTES('SHA1',CONCAT([ClaimStatus],'%',[ClaimStatusID],'%',[Dsc],'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy],'%',[SYS_CHANGE_OPERATION])) as Bronze_Delta_HashBytes
       FROM [EclipseBronze].[dbo].[ClaimStatus]
	   --FROM [QAData].[dbo].[QAClaimStatus]
)
     SELECT 
            STAGE.*,DELTA.* 
           ,Case WHEN Bronze_Stage_HashBytes = Bronze_Delta_HashBytes 
                 THEN 'Passed'
                 ELSE 'Failed'
             End as TestResult
           ,GetDate() as TestedOn 
       FROM  CTE_Bronze_Stage STAGE
       left join  CTE_Bronze_Delta DELTA
              on ClaimStatusIDSTAGE = ClaimStatusIDDELTA


--Compare Hash Bytes Stage Parquet/Delta Synapse view under QA Database
With 
 CTE_Bronze_Stage as (
     SELECT [ClaimStatus] as [ClaimStatusSTAGE],[ClaimStatusID] as [ClaimStatusIDSTAGE],[Dsc] as [DscSTAGE],[OwnerID] as [OwnerIDSTAGE],[InsDate] as [InsDateSTAGE],[DelDate] as [DelDateSTAGE],[LastUpd] as [LastUpdSTAGE],[UpdBy] as [UpdBySTAGE],[SYS_CHANGE_OPERATION] as [SYS_CHANGE_OPERATIONSTAGE]
           --,HASHBYTES('SHA1',CONCAT([ClaimStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8))as Bronze_Stage_HashBytes
		   --,HASHBYTES('SHA1',CONCAT([ClaimStatus],'%',[ClaimStatusID],'%',[Dsc],'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy],'%',[SYS_CHANGE_OPERATION]))as Bronze_Stage_HashBytes
		   ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([ClaimStatus],'%',[ClaimStatusID],'%',[Dsc],'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy],'%',[SYS_CHANGE_OPERATION]))) as Bronze_Stage_HashBytes
		   
		   --Hash with No date columns in Staging
		   ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([ClaimStatus],'%',[ClaimStatusID],'%',[Dsc],'%',[OwnerID],'%',[UpdBy],'%',[SYS_CHANGE_OPERATION]))) as HBSTG
       FROM OPENROWSET ( 
             BULK 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_ClaimStatus/SystemLoadID=1022022101001/**',
             FORMAT = 'parquet'
                          ) as RS
)
,CTE_Bronze_Delta as (
     SELECT [ClaimStatus] as [ClaimStatusDELTA],[ClaimStatusID] as [ClaimStatusIDDELTA],[Dsc] as [DscDELTA],[OwnerID] as [OwnerIDDELTA],[InsDate] as [InsDateDELTA],[DelDate] as [DelDateDELTA],[LastUpd] as [LastUpdDELTA],[UpdBy] as [UpdByDELTA],[SYS_CHANGE_OPERATION] as [SYS_CHANGE_OPERATIONDELTA]
           --,HASHBYTES('SHA1',CONCAT([ClaimStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8))as Bronze_Delta_HashBytes
           --,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([ClaimStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8)))as HBTGT
		   --Hash with all columns
           ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([ClaimStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8)))as Bronze_Delta_HashBytes
		   --Hash with No date columns
		   ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([ClaimStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[UpdBy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8)))as HBTGT
		   --,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([ClaimStatus],'%',[ClaimStatusID],'%',[Dsc],'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy],'%',[SYS_CHANGE_OPERATION]))) as HBTGT
		   --,HASHBYTES('SHA1',CONCAT([ClaimStatus],'%',[ClaimStatusID],'%',[Dsc],'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy],'%',[SYS_CHANGE_OPERATION])) as Bronze_Delta_HashBytes
       --FROM [EclipseBronze].[dbo].[ClaimStatus]
	   FROM [QAData].[dbo].[QAClaimStatus]
)
     SELECT 
            STAGE.*,DELTA.* 
		   ,CASE WHEN Bronze_Stage_HashBytes = Bronze_Delta_HashBytes then 'Passed' Else 'failed' End as TestResultALLCols
           ,Case WHEN HBSTG = HBTGT THEN 'Passed' ELSE 'Failed' End as TestResultNoDateColumns
           ,GetDate() as TestedOn 
       FROM  CTE_Bronze_Stage STAGE
       left join  CTE_Bronze_Delta DELTA
              on ClaimStatusIDSTAGE = ClaimStatusIDDELTA