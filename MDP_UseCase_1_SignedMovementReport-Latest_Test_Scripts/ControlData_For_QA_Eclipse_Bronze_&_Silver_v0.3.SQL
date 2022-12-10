--USE [QAData];

/********************************************************************************************************************************************
--------------------------------------------------------------------------------------------------------------------------------------------- 
   Version  |  Date        | Author              | Description                                                                  
--------------------------------------------------------------------------------------------------------------------------------------------- 
   0.1      |  05/10/2022  | Ram Baskar          | Control data for Eclipse Bronze Layer SystemLoadId
   0.2      |  12/10/2022  | Ram Baskar          | Script amended to bring Profisee Source System Data
   0.3      |  12/11/2022  | Ram Baskar          | Script amended to bring Eclipse Silver Layer SystemLoadId
---------------------------------------------------------------------------------------------------------------------------------------------    
--Data to be bring it to QA database via Synapse Pipeline (Query used in Synapse Pipeline - PL_GetControlDataForQa)
*********************************************************************************************************************************************/


SELECT * 
 FROM (
        --bring SystemLoadId's for Eclipse Bronze
        SELECT 
               SO.SourceSystemId,MS.SystemName, MS.SystemDescription, SO.SourceObjectId, SO.BronzeStagingSystemId as BronzeStage,SO.BronzeSystemId as BronzeDelta
        	  ,SilverStagingSystemID,SilverSystemID
              ,SourceObjectName,SO.UniqueColumn, SO.[Join] as JoinCondition
        	  ,LT.LoadTypeDescription
        	  ,POR.ObjectRunId,POR.SystemLoadId,POR.ObjectId,POR.Status,POR.StartDateUTC,POR.EndDateUTC
        	  ,Rank () over (partition by SourceObjectId ORDER BY SystemLoadId DESC) as RN
          FROM control.metadata.SourceObject SO
          join control.metadata.System       MS on SO.SourceSystemID = MS.SystemID
          join control.process.ObjectRun    POR on SO.SourceObjectID = POR.ObjectID
          join control.metadata.LoadType     LT on SO.LoadTypeID = LT.LoadTypeID
         WHERE POR.Status = 'Succeeded'                                                                                                                                                                        
           AND  SystemLoadID like '102%' --bring only Eclipse Bronze Stage related run 
          --AND SO.SourceObjectName = 'dbo.Policy'
		  
        UNION 
        --bring SystemLoadId's for Profisee
        SELECT 
               SO.SourceSystemId,MS.SystemName, MS.SystemDescription, SO.SourceObjectId, SO.BronzeStagingSystemId as BronzeStage,SO.BronzeSystemId as BronzeDelta
        	  ,SilverStagingSystemID,SilverSystemID
              ,SourceObjectName,SO.UniqueColumn, SO.[Join] as JoinCondition
        	  ,LT.LoadTypeDescription
        	  ,POR.ObjectRunId,POR.SystemLoadId,POR.ObjectId,POR.Status,POR.StartDateUTC,POR.EndDateUTC
        	  ,Rank () over (partition by SourceObjectId,BronzeStagingSystemId, BronzeSystemId,SilverStagingSystemID,SilverSystemId ORDER BY ObjectRunId DESC) as RN
          FROM control.metadata.SourceObject SO
          join control.metadata.System       MS on SO.SourceSystemID = MS.SystemID
          join control.process.ObjectRun    POR on SO.SourceObjectID = POR.ObjectID
          join control.metadata.LoadType     LT on SO.LoadTypeID = LT.LoadTypeID
         WHERE POR.Status = 'Succeeded'                                                                                                                                                                        
           AND  SystemLoadID like '119%' --bring only Eclipse Silver related run 
          --AND SO.SourceObjectName = 'dbo.Policy'		  

        UNION 
        --bring SystemLoadId's for Eclipse Silver
        SELECT 
               SO.SourceSystemId,MS.SystemName, MS.SystemDescription, SO.SourceObjectId, SO.BronzeStagingSystemId as BronzeStage,SO.BronzeSystemId as BronzeDelta
        	  ,SilverStagingSystemID,SilverSystemID
              ,SourceObjectName,SO.UniqueColumn, SO.[Join] as JoinCondition
        	  ,LT.LoadTypeDescription
        	  ,POR.ObjectRunId,POR.SystemLoadId,POR.ObjectId,POR.Status,POR.StartDateUTC,POR.EndDateUTC
        	  ,Rank () over (partition by SourceObjectId,BronzeStagingSystemId, BronzeSystemId,SilverStagingSystemID,SilverSystemId ORDER BY ObjectRunId DESC) as RN
          FROM control.metadata.SourceObject SO
          join control.metadata.System       MS on SO.SourceSystemID = MS.SystemID
          join control.process.ObjectRun    POR on SO.SourceObjectID = POR.ObjectID
          join control.metadata.LoadType     LT on SO.LoadTypeID = LT.LoadTypeID
         WHERE POR.Status = 'Succeeded'                                                                                                                                                                        
           AND  SystemLoadID like '104%' --bring only Eclipse Silver related run 
          --AND SO.SourceObjectName = 'dbo.Policy'
 )T

 WHERE RN = 1



