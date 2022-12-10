--Data to be bring it to QA database via Synapse Pipeline (Query used in Synapse Pipeline - PL_GetControlDataForQa
--102 BronzeStaging, 104 SilverStagingEclipse, 105 SilverDelta Eclipse

SELECT * 
  FROM (
SELECT TOP 1 with TIES 
       SO.SourceSystemId,MS.SystemName, MS.SystemDescription, SO.SourceObjectId, SO.BronzeStagingSystemId as BronzeStage,SO.BronzeSystemId as BronzeDelta
      ,SourceObjectName,SO.UniqueColumn, SO.[Join] as JoinCondition
	  ,LT.LoadTypeDescription
	  ,POR.ObjectRunId,POR.SystemLoadId,POR.ObjectId,POR.Status,POR.StartDateUTC,POR.EndDateUTC
  FROM control.metadata.SourceObject SO
  join control.metadata.System       MS on SO.SourceSystemID = MS.SystemID
  join control.process.ObjectRun    POR on SO.SourceObjectID = POR.ObjectID
  join control.metadata.LoadType     LT on SO.LoadTypeID = LT.LoadTypeID
 WHERE SystemLoadID like '102%' --bring only Eclipse Bronze Stage related run
   AND POR.Status = 'Succeeded' 
 --AND SO.SourceObjectName = 'dbo.Policy'
   
 --ORDER BY ROW_NUMBER () Over (Partition by ObjectId ORDER BY SystemLoadId ASC) -- To get the SystemLoadId for an Initial Load
 ORDER BY ROW_NUMBER () Over (Partition by ObjectId ORDER BY SystemLoadId DESC) -- To get the SystemLoadId for an Initial Load
 
 UNION ALL
 
 SELECT TOP 1 with TIES 
       SO.SourceSystemId,MS.SystemName, MS.SystemDescription, SO.SourceObjectId, SO.BronzeStagingSystemId as BronzeStage,SO.BronzeSystemId as BronzeDelta
      ,SourceObjectName,SO.UniqueColumn, SO.[Join] as JoinCondition
	  ,LT.LoadTypeDescription
	  ,POR.ObjectRunId,POR.SystemLoadId,POR.ObjectId,POR.Status,POR.StartDateUTC,POR.EndDateUTC
  FROM control.metadata.SourceObject SO
  join control.metadata.System       MS on SO.SourceSystemID = MS.SystemID
  join control.process.ObjectRun    POR on SO.SourceObjectID = POR.ObjectID
  join control.metadata.LoadType     LT on SO.LoadTypeID = LT.LoadTypeID
 WHERE SystemLoadID like '104%' --bring only Bronze Stage related tables 
 --102 BronzeStaging, 104 SilverStagingEclipse, 105 SilverDelta Eclipse
   AND POR.Status = 'Succeeded' 
 --AND SO.SourceObjectName = 'dbo.Policy'
   
 --ORDER BY ROW_NUMBER () Over (Partition by ObjectId ORDER BY SystemLoadId ASC) -- To get the SystemLoadId for an Initial Load
 ORDER BY ROW_NUMBER () Over (Partition by ObjectId ORDER BY SystemLoadId DESC) -- To get the SystemLoadId for an Initial Load

 UNION ALL
 
 SELECT TOP 1 with TIES 
       SO.SourceSystemId,MS.SystemName, MS.SystemDescription, SO.SourceObjectId, SO.BronzeStagingSystemId as BronzeStage,SO.BronzeSystemId as BronzeDelta
      ,SourceObjectName,SO.UniqueColumn, SO.[Join] as JoinCondition
	  ,LT.LoadTypeDescription
	  ,POR.ObjectRunId,POR.SystemLoadId,POR.ObjectId,POR.Status,POR.StartDateUTC,POR.EndDateUTC
  FROM control.metadata.SourceObject SO
  join control.metadata.System       MS on SO.SourceSystemID = MS.SystemID
  join control.process.ObjectRun    POR on SO.SourceObjectID = POR.ObjectID
  join control.metadata.LoadType     LT on SO.LoadTypeID = LT.LoadTypeID
 WHERE SystemLoadID like '119%' --bring profisee related run details
   AND POR.Status = 'Succeeded' 
 --AND SO.SourceObjectName = 'dbo.Policy'
 --ORDER BY ROW_NUMBER () Over (Partition by ObjectId ORDER BY SystemLoadId ASC) -- To get the SystemLoadId for an Initial Load
 ORDER BY ROW_NUMBER () Over (Partition by ObjectId ORDER BY SystemLoadId DESC) -- To get the SystemLoadId for an Initial Load

 )T ORDER BY ObjectRunId DESC
