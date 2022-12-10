use QAData
DROP External Table QAData.dbo.TR_Summary
Create EXTERNAL table dbo.TR_Summary (

Layer	    	Varchar(100),
ViewName		Varchar(150),
TestResult  	Varchar(50),
TotalRecords	INT,
TestedOn		DateTime
)
WITH (
       DATA_SOURCE = [qadata_dldpdev01_dfs_core_windows_net],LOCATION = N'TestResult/QASummary/**',FILE_FORMAT = [SynapseParquetFormat]
     )
