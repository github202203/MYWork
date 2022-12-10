--This script is working 
WITH
CTE_Bronze_Target as (
     SELECT [ClaimStatus],[ClaimStatusID],[Dsc],[OwnerID],[InsDate],[DelDate],[LastUpd],[UpdBy],[SYS_CHANGE_OPERATION]
     ,HASHBYTES('SHA1',CONCAT([ClaimStatus] COLLATE Latin1_General_100_BIN2_UTF8,'%',[ClaimStatusID],'%',[Dsc] COLLATE Latin1_General_100_BIN2_UTF8,'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd],'%',[UpdBy]  COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]))as Bronze_Target_HashBytes
     --,HASHBYTES('SHA1',CONCAT([ClaimStatusID],'%',[OwnerID],'%',[InsDate],'%',[DelDate],'%',[LastUpd]))as HashBytes1
     --,HASHBYTES('SHA1',CONCAT([ClaimStatus],'%',[Dsc],'%',[UpdBy],'%',[SYS_CHANGE_OPERATION]))as HashBytes2
       FROM [EclipseBronze].[dbo].[ClaimStatus]
)

SELECT * FROM CTE_Bronze_Target