SELECT 'BronzeLayer Data Validation Test Started at:- ' + CONVERT(VARCHAR, getdate(), 120) as BronzeLayerTestStart
 
EXEC [QAData].[dbo].[BronzeLayer_Eclipse_Stage_To_Delta_DataValidation]        @TableName = 'Addr' -- --Can't see this table
EXEC [QAData].[dbo].[BronzeLayer_Eclipse_Stage_To_Delta_DataValidation]        @TableName = 'ApplicationUser' 
EXEC [QAData].[dbo].[BronzeLayer_Eclipse_Stage_To_Delta_DataValidation]        @TableName = 'LloydsBroker' 
EXEC [QAData].[dbo].[BronzeLayer_Eclipse_Stage_To_Delta_DataValidation]        @TableName = 'Org' 
EXEC [QAData].[dbo].[BronzeLayer_Eclipse_Stage_To_Delta_DataValidation]        @TableName = 'OrgName' --Can't see this table
EXEC [QAData].[dbo].[BronzeLayer_Eclipse_Stage_To_Delta_DataValidation_Policy] @TableName = 'Policy' 
EXEC [QAData].[dbo].[BronzeLayer_Eclipse_Stage_To_Delta_DataValidation]        @TableName = 'PolicyBroker' --Can't see this table
EXEC [QAData].[dbo].[BronzeLayer_Eclipse_Stage_To_Delta_DataValidation]        @TableName = 'PolicyLimit' 
EXEC [QAData].[dbo].[BronzeLayer_Eclipse_Stage_To_Delta_DataValidation]        @TableName = 'PolicyLine' 
EXEC [QAData].[dbo].[BronzeLayer_Eclipse_Stage_To_Delta_DataValidation]        @TableName = 'PolicyOrg' 
EXEC [QAData].[dbo].[BronzeLayer_Eclipse_Stage_To_Delta_DataValidation]        @TableName = 'ReportingClass' 
EXEC [QAData].[dbo].[BronzeLayer_Eclipse_Stage_To_Delta_DataValidation]        @TableName = 'Role' --Can't see any Parquet file for this view

SELECT 'BronzeLayer Data Validation Test Completed at:- ' + CONVERT(VARCHAR, getdate(), 120) as BronzeLayerTestEnd


