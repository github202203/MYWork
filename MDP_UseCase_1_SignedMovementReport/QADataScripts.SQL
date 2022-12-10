IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
	WITH ( FORMAT_TYPE = PARQUET)
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'qadata_dldpdev01_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [qadata_dldpdev01_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://qadata@dldpdev01.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE QAControlData (
	[SourceSystemId] 		int,
	[SystemName] 			nvarchar(4000),
	[SystemDescription] 	nvarchar(4000),
	[SourceObjectId] 		int,
	[BronzeStage] 			int,
	[BronzeDelta] 			int,
	[SourceObjectName] 		nvarchar(4000),
	[UniqueColumn] 			nvarchar(4000),
	[JoinCondition] 		nvarchar(4000),
	[LoadTypeDescription] 	nvarchar(4000),
	[ObjectRunId] 			int,
	[SystemLoadId] 			bigint,
	[ObjectId] 				int,
	[Status] 				nvarchar(4000),
	[StartDateUTC] 			datetime2(7),
	[EndDateUTC] 			datetime2(7)
	)
	WITH (
	LOCATION = 'controldata/**',
	DATA_SOURCE = [qadata_dldpdev01_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO


SELECT TOP 100 * FROM [dbo].QAControlData
GO