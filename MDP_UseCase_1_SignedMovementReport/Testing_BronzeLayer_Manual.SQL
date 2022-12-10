--SELECT GetDate(), CAST(GetDate() as Date) DateValue, CAST(GetDate() as DateTime) as DateTimeValue, CAST(GetDate() as SmallDateTime) as SmallDateTimeValue,CAST(GetDate() as DateTime2) as DateTime2Value

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
          --Left Join CTE_Bronze_Target TGT on Bronze_Source_HashBytes = Bronze_Delta_HashBytes 
		  Left Join CTE_Bronze_Target TGT on STG.ClaimStatusId = TGT.ClaimStatusId 