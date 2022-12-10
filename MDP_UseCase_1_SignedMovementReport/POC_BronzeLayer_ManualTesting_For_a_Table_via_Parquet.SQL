/* ---------------------------------------------------------------------------------------------------------------------------- */
/* Version  |  Date        | Author              | Description                                                                  */
/* ---------------------------------------------------------------------------------------------------------------------------- */
/* 0.1      |  03/10/2022  | Ram Baskar          | Manual Testing - Data comparison in Bronze Layer between Staging & Delta     */

-- This script compare data between Stage Parquet to Delta Parquet 

/* ---------------------------------------------------------------------------------------------------------------------------- */

With 
    CTE_Bronze_Source as (
        SELECT [ClaimStatus],[ClaimStatusID],[Dsc],[OwnerID],[InsDate],[DelDate],[LastUpd],[UpdBy],[SYS_CHANGE_OPERATION]
              ,HASHBYTES('SHA1',CONCAT([ClaimStatus],'%',[ClaimStatusID],'%',[Dsc],'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy],'%',[SYS_CHANGE_OPERATION]))as Bronze_Source_HashBytes
          FROM OPENROWSET ( 
               BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_ClaimStatus/*/**',
               FORMAT = 'PARQUET'
                          ) as SR
    )
	
   ,CTE_Bronze_Target AS (
        SELECT [ClaimStatus],[ClaimStatusID],[Dsc],[OwnerID],[InsDate],[DelDate],[LastUpd],[UpdBy],[SYS_CHANGE_OPERATION]
              ,HASHBYTES('SHA1',CONCAT([ClaimStatus],'%',[ClaimStatusID],'%',[Dsc],'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy],'%',[SYS_CHANGE_OPERATION]))AS Bronze_Delta_HashBytes
          FROM
               OPENROWSET (
               BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/DeltaLake/dbo_ClaimStatus/',
               FORMAT = 'Delta'
                           ) AS TR
    )

        SELECT @@SERVERNAME AS Environment, STG.*,TGT.*
		      ,CASE WHEN Bronze_Source_HashBytes = Bronze_Delta_HashBytes THEN 'Passed' ELSE 'Failed' END AS TestResult
			  ,GETDATE() AS TestedOn
          FROM  CTE_Bronze_Source STG
          FULL JOIN CTE_Bronze_Target TGT
                 on Bronze_Source_HashBytes = Bronze_Delta_HashBytes 

