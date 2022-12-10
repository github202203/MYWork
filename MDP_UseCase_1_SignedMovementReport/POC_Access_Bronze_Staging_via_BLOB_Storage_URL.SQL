--access via blob storage URL

With 
 CTE_Bronze_Stage as (
     SELECT [ClaimStatus],[ClaimStatusID],[Dsc],[OwnerID],[InsDate],[DelDate],[LastUpd],[UpdBy],[SYS_CHANGE_OPERATION]
           ,HASHBYTES('SHA1',CONCAT([ClaimStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8))as Bronze_Stage_HashBytes
       FROM OPENROWSET ( 
             --BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_ClaimStatus/*/**', -- using File Store
			 BULK 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_ClaimStatus/*/**', -- using Blob storage access
             FORMAT = 'PARQUET'
                          ) as RS
)

SELECT * FROM CTE_Bronze_Stage

