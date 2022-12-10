--USE [QAData];

/********************************************************************************************************************************************
--------------------------------------------------------------------------------------------------------------------------------------------- 
   Version  |  Date        | Author              | Description                                                                  
--------------------------------------------------------------------------------------------------------------------------------------------- 
   0.1      |  05/10/2022  | Ram Baskar          | Control data for Bronze Layer Eclipse Source System
   0.2      |  12/10/2022  | Ram Baskar          | Script amended to bring Profisee Source System Data
---------------------------------------------------------------------------------------------------------------------------------------------    
--Data to be bring it to QA database via Synapse Pipeline (Query used in Synapse Pipeline - PL_GetControlDataForQa)
*********************************************************************************************************************************************/
SELECT TOP 1 with TIES 
       SO.SourceSystemId,MS.SystemName, MS.SystemDescription, SO.SourceObjectId, SO.BronzeStagingSystemId as BronzeStage,SO.BronzeSystemId as BronzeDelta
      ,SourceObjectName,SO.UniqueColumn, SO.[Join] as JoinCondition
	  ,LT.LoadTypeDescription
	  ,POR.ObjectRunId,POR.SystemLoadId,POR.ObjectId,POR.Status,POR.StartDateUTC,POR.EndDateUTC
  FROM control.metadata.SourceObject SO
  join control.metadata.System       MS on SO.SourceSystemID = MS.SystemID
  join control.process.ObjectRun    POR on SO.SourceObjectID = POR.ObjectID
  join control.metadata.LoadType     LT on SO.LoadTypeID = LT.LoadTypeID
 WHERE ( SystemLoadID like '102%'  --bring only Bronze Stage related tables (Eclipse)
         or 
		 SystemLoadID like '119%'  --bring only Bronze Stage related tables (Profisee)
		 )
   AND POR.Status = 'Succeeded' 
 --ORDER BY ROW_NUMBER () Over (Partition by ObjectId ORDER BY SystemLoadId ASC) -- To get the SystemLoadId for an Initial Load
 ORDER BY ROW_NUMBER () Over (Partition by ObjectId ORDER BY SystemLoadId DESC) -- To get the SystemLoadId for latest Delta Load

