
/**********************************************************************************************************************************************************
   Version  |  Date        | Author              | Description                                                                  
--------------------------------------------------------------------------------------------------------------------------------------------- 


   0.1      |  23/11/2022  | Ram Baskar          | Script to validate Transformation Rule between Bronze Staging - Silver Staging for PolicyLine entity - Initial Version
   0.2      |  24/11/2022  | Ram Baskar          | Amended the script with DelDate attribute
   0.3      |  25/11/2022  | Ram Baskar          | Amended the script in sync with the data mapping document as at 25th Nov, 2022@12 Noon
   0.4      |  25/11/2022  | Ram Baskar          | Amended the script to pick up the SystemLoadId from Source dynamically based on ControlQAData
   

	 Layer:Silver 
	Entity:PolicyLine

    Script Type - 100% manual
    Script neexs to be amended 

**********************************************************************************************************************************************************/

DECLARE @SystemLoadID	BIGINT = ''
       ,@Layer          VARCHAR(20)   = 'Silver'
       ,@TestType       NVARCHAR(MAX) = 'Transformation Rule Testng'
       ,@SchemaName	    NVARCHAR(MAX) = 'dbo'
       ,@TableName	    NVARCHAR(MAX) = 'PolicyLine'
;

SET @SystemLoadId      = (SELECT SystemLoadId  FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName AND SystemLoadId like '102%') 

;


WITH 
CTE_Source AS (--PolicyLine Source Data from Eclipse Bronze Staging PARQUET
SELECT [PolicyLineId]                                                                                 AS [SRC_PolicyLineID]
      ,[PolicyId]                                                                                     AS [SRC_PolicyID]
      ,CAST([SignedDate] as DATE  )                                                                   AS [SRC_SignedDate]  
      ,CAST([DelDate] as datetime2)                                                                   AS [SRC_DelDate]
      ,ISNULL([WholePartOrder],0)                                                                     AS [SRC_WholePartOrder]
      ,[WrittenOrder]                                                                                 AS [SRC_WrittenOrder]
	  ,ISNULL(CASE WHEN [WholePartOrder] = 'O' THEN [WrittenOrder] ELSE 100  END ,0)                  AS [SRC_WrittenOrderPct]
      ,[WrittenLine]                                                                                  AS [SRC_WrittenLine]
      ,[WrittenLineInd]                                                                               AS [SRC_WrittenLineInd]
      ,ISNULL([EstSigningDown],0)                                                                     AS [SRC_EstimatedSignedPct]
	  ,ISNULL([SignedLine],0)                                                                         AS [SRC_SignedLinePct]
	  ,ISNULL([SignedOrder],0)                                                                        AS [SRC_SignedOrderPct]
	  ,(SELECT mdmCode FROM ProfiseeBronze.dbo.Ref_SourceSystem WHERE name = 'Eclipse')               AS [SRC_SourceSystemCode]
      ,UPPER(TRIM([Synd]))                                                                            AS [SRC_Syndicate]
      ,CASE WHEN ISNULL([ActualSigningDown],0) > 999 THEN 999 ELSE ISNULL([ActualSigningDown],0) END  AS [SRC_ActualSigningDown]
	  ,ProducingTeam                                                                                  AS [SRC_ProducingTeam]
	  ,LineStatus                                                                                     AS [SRC_LineStatus]
	  ,EstSigningDown                                                                                 AS [SRC_EstSigningDown]
	  ,SignedLine                                                                                     AS [SRC_SignedLine]
	  ,SignedOrder                                                                                    AS [SRC_SignedOrder]
	  ,SYS_CHANGE_OPERATION                                                                           AS [SRC_Sys_Change_Operation]
	  ,CAST(SPQ.filepath(1) AS BIGINT)                                                                AS [SRC_SystemLoadID]

  --FROM  OPENROWSET(BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_PolicyLine/SystemLoadID=1022022111501/dbo_PolicyLine_1022022111501.parquet', FORMAT = 'PARQUET') AS SPQ
    FROM  OPENROWSET(BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_PolicyLine/SystemLoadID=*/*.parquet', FORMAT = 'PARQUET') AS SPQ
  )

,CTE_Target AS (--PolicyLine Source Data from Eclipse Siler Staging PARQUET
SELECT [PolicyLineID]                                                                                 AS [TGT_PolicyLineID]
      ,[PolicyID]                                                                                     AS [TGT_PolicyID]
      ,[SignedDate]                                                                                   AS [TGT_SignedDate]  
      ,[DelDate]                                                                                      AS [TGT_DelDate]
      ,ISNULL([WholePartOrder],0)                                                                     AS [TGT_WholePartOrder]
      ,[WrittenOrder]                                                                                 AS [TGT_WrittenOrder]
	  ,ISNULL(CASE WHEN [WholePartOrder] = 'O' THEN [WrittenOrder] ELSE 100  END ,0)                  AS [TGT_WrittenOrderPct]
      ,[WrittenLine]                                                                                  AS [TGT_WrittenLine]
      ,[WrittenLineInd]                                                                               AS [TGT_WrittenLineInd]
      ,ISNULL([EstSigningDown],0)                                                                     AS [TGT_EstimatedSignedPct]
	  ,ISNULL([SignedLine],0)                                                                         AS [TGT_SignedLinePct]
	  ,ISNULL([SignedOrder],0)                                                                        AS [TGT_SignedOrderPct]
	  ,(SELECT mdmCode FROM ProfiseeBronze.dbo.Ref_SourceSystem WHERE name = 'Eclipse')               AS [TGT_SourceSystemCode]
      ,[Syndicate]                                                                                    AS [TGT_Syndicate]
      ,CASE WHEN ISNULL([ActualSigningDown],0) > 999 THEN 999 ELSE ISNULL([ActualSigningDown],0) END  AS [TGT_ActualSigningDown]
	  ,ProducingTeam                                                                                  AS [TGT_ProducingTeam]
	  ,LineStatus                                                                                     AS [TGT_LineStatus]
	  ,EstSigningDown                                                                                 AS [TGT_EstSigningDown]
	  ,SignedLine                                                                                     AS [TGT_SignedLine]
	  ,SignedOrder                                                                                    AS [TGT_SignedOrder]
	  ,SYS_CHANGE_OPERATION                                                                           AS [TGT_Sys_Change_Operation]
	  ,CAST(TPQ.filepath(1) AS BIGINT)                                                                AS [TGT_SystemLoadID]
  --FROM  OPENROWSET(BULK 'https://dldpdev01.dfs.core.windows.net/silver/underwriting/Internal/Eclipse/Staging/dbo_PolicyLine/SystemLoadID=1042022111501/dbo_PolicyLine_1042022111501.parquet',        FORMAT = 'PARQUET') AS TPQ
  FROM  OPENROWSET(BULK 'https://dldpdev01.dfs.core.windows.net/silver/underwriting/Internal/Eclipse/Staging/dbo_PolicyLine/SystemLoadID=*/*.parquet' ,FORMAT = 'PARQUET') AS TPQ
  )

,ExpectedResult as (

SELECT  @@ServerName as Server,  @Layer as Layer, @SchemaName + '.' + @TableName as Entity, @TestType as TestType,GetDate() as TestedOn
       ,Case when SRC_HB = TGT_HB then 'Passed' else 'Failed' End as TestResult
	   ,SRC.*,TGT.*
  FROM (

        SELECT 
               [SRC_PolicyLineID]
              ,[SRC_PolicyID]
              ,[SRC_SignedDate]  
              ,[SRC_DelDate]
              ,[SRC_WholePartOrder]
              ,[SRC_WrittenOrder]
              ,[SRC_WrittenOrderPct]
              ,[SRC_WrittenLine]
              ,[SRC_WrittenLineInd]
              ,[SRC_EstimatedSignedPct]
              ,[SRC_SignedLinePct]
              ,[SRC_SignedOrderPct]
              ,[SRC_SourceSystemCode]
              ,[SRC_Syndicate]
              ,[SRC_ActualSigningDown]
              ,[SRC_ProducingTeam]
              ,[SRC_LineStatus]
              ,[SRC_EstSigningDown]
              ,[SRC_SignedLine]
              ,[SRC_SignedOrder]
			  ,SRC_SystemLoadID
               --,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([SRC_PolicyLineID],'%',[SRC_PolicyID],'%',[SRC_SignedDate],'%',[SRC_DelDate],'%',[SRC_WholePartOrder],'%',[SRC_WrittenOrder],'%',[SRC_WrittenOrderPct],'%',[SRC_WrittenLine],'%',[SRC_WrittenLineInd],'%',[SRC_EstimatedSignedPct],'%',[SRC_SignedLinePct],'%',[SRC_SignedOrderPct],'%',[SRC_SourceSystemCode] COLLATE SQL_Latin1_General_CP1_CI_AS,'%',[SRC_Syndicate],'%',[SRC_ActualSigningDown],'%',[SRC_ProducingTeam],'%',[SRC_LineStatus],'%',[SRC_EstSigningDown],'%',[SRC_SignedLine],'%',[SRC_SignedOrder] ))) AS SRC_HB
              --Below hash Bytes just removed COLLATE for SourceSystemCode
              ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([SRC_PolicyLineID],'%',[SRC_PolicyID],'%',[SRC_SignedDate],'%',[SRC_DelDate],'%',[SRC_WholePartOrder],'%',[SRC_WrittenOrder],'%',[SRC_WrittenOrderPct],'%',[SRC_WrittenLine],'%',[SRC_WrittenLineInd],'%',[SRC_EstimatedSignedPct],'%',[SRC_SignedLinePct],'%',[SRC_SignedOrderPct],'%',[SRC_SourceSystemCode],'%',[SRC_Syndicate],'%',[SRC_ActualSigningDown],'%',[SRC_ProducingTeam],'%',[SRC_LineStatus],'%',[SRC_EstSigningDown],'%',[SRC_SignedLine],'%',[SRC_SignedOrder] ))) AS SRC_HB
			          FROM CTE_Source
		WHERE SRC_SystemLoadID = @SystemLoadID
        ) SRC

  LEFT JOIN 
 
       (
        SELECT 
               [TGT_PolicyLineID]
              ,[TGT_PolicyID]
              ,[TGT_SignedDate]  
              ,[TGT_DelDate]
              ,[TGT_WholePartOrder]
              ,[TGT_WrittenOrder]
              ,[TGT_WrittenOrderPct]
              ,[TGT_WrittenLine]
              ,[TGT_WrittenLineInd]
              ,[TGT_EstimatedSignedPct]
              ,[TGT_SignedLinePct]
              ,[TGT_SignedOrderPct]
              ,[TGT_SourceSystemCode]
              ,[TGT_Syndicate]
              ,[TGT_ActualSigningDown]
              ,[TGT_ProducingTeam]
              ,[TGT_LineStatus]
              ,[TGT_EstSigningDown]
              ,[TGT_SignedLine]
              ,[TGT_SignedOrder]

			  --,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([TGT_PolicyLineID],'%',[TGT_PolicyID],'%',[TGT_SignedDate],'%',[TGT_DelDate],'%',[TGT_WholePartOrder],'%',[TGT_WrittenOrder],'%',[TGT_WrittenOrderPct],'%',[TGT_WrittenLine],'%',[TGT_WrittenLineInd],'%',[TGT_EstimatedSignedPct],'%',[TGT_SignedLinePct],'%',[TGT_SignedOrderPct],'%',[TGT_SourceSystemCode] COLLATE SQL_Latin1_General_CP1_CI_AS,'%',[TGT_Syndicate],'%',[TGT_ActualSigningDown],'%',[TGT_ProducingTeam],'%',[TGT_LineStatus],'%',[TGT_EstSigningDown],'%',[TGT_SignedLine],'%',[TGT_SignedOrder] ))) AS TGT_HB
			  --Below hash Bytes just removed COLLATE for SourceSystemCode
              ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([TGT_PolicyLineID],'%',[TGT_PolicyID],'%',[TGT_SignedDate],'%',[TGT_DelDate],'%',[TGT_WholePartOrder],'%',[TGT_WrittenOrder],'%',[TGT_WrittenOrderPct],'%',[TGT_WrittenLine],'%',[TGT_WrittenLineInd],'%',[TGT_EstimatedSignedPct],'%',[TGT_SignedLinePct],'%',[TGT_SignedOrderPct],'%',[TGT_SourceSystemCode],'%',[TGT_Syndicate],'%',[TGT_ActualSigningDown],'%',[TGT_ProducingTeam],'%',[TGT_LineStatus],'%',[TGT_EstSigningDown],'%',[TGT_SignedLine],'%',[TGT_SignedOrder] ))) AS TGT_HB
        FROM CTE_Target
        ) TGT
    ON SRC_PolicyLineID = TGT_PolicyLineID
   
   )

SELECT * 
  FROM  ExpectedResult

