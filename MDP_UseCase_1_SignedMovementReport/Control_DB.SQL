
SELECT * FROM metadata.System

SELECT * FROM control.metadata.LoadType

SELECT * 
  FROM control.metadata.SourceObject SO
 WHERE SO.BronzeStagingSystemID = 102
   AND SO.SourceObjectName = 'dbo.ClaimStatus'

SELECT * 
  FROM control.process.ObjectRun
 WHERE ObjectID = 1010006
 ORDER BY 1 DESC

SELECT * 
  FROM control.process.SystemLoad
 ORDER BY 1 DESC

SELECT POR.ObjectRunId,POR.SystemLoadId,POR.ObjectId,POR.Status,POR.StartDateUTC,POR.EndDateUTC
      ,SO.SourceObjectName, SO.UniqueColumn, SO.[Join], SO.LoadTypeID, LT.LoadTypeDescription
  FROM control.process.ObjectRun     POR
  Join control.metadata.SourceObject SO on POR.ObjectID  = SO.SourceObjectID
  join control.metadata.LoadType     LT on SO.LoadTypeID = LT.LoadTypeID
 WHERE POR.ObjectID = 1010006
   AND SystemLoadID like '102%'
 ORDER BY POR.ObjectRunId DESC
 
--Linked Services 
--LS_ASQL_DP to access control database
--DS_ADLS_Parquet - to create file in Blob Storage 

 
 --Data to be bring it to QA database via Synapse Pipeline (Query used in Synapse Pipeline - PL_GetControlDataForQa
SELECT TOP 1 with TIES 
       SO.SourceSystemId,MS.SystemName, /*MS.SystemDescription,*/ SO.SourceObjectId, SO.BronzeStagingSystemId as BronzeStage,SO.BronzeSystemId as BronzeDelta
      ,SourceObjectName,SO.UniqueColumn, SO.[Join] as JoinCondition
	  ,LT.LoadTypeDescription
	  ,POR.ObjectRunId,POR.SystemLoadId,POR.ObjectId,POR.Status,POR.StartDateUTC,POR.EndDateUTC
  FROM control.metadata.SourceObject SO
  join control.metadata.System       MS on SO.SourceSystemID = MS.SystemID
  join control.process.ObjectRun    POR on SO.SourceObjectID = POR.ObjectID
  join control.metadata.LoadType     LT on SO.LoadTypeID = LT.LoadTypeID
 WHERE SystemLoadID like '102%' --bring only Bronze Stage related tables 
   AND POR.Status = 'Succeeded' 
 --ORDER BY ROW_NUMBER () Over (Partition by ObjectId ORDER BY SystemLoadId ASC) -- To get the SystemLoadId for an Initial Load
 ORDER BY ROW_NUMBER () Over (Partition by ObjectId ORDER BY SystemLoadId DESC) -- To get the SystemLoadId for latest Delta Load