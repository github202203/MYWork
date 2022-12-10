--EXEC [QAData].[dbo].[ValidateBronzeStageToBronzeDeltaData_usingParquet] @TableName = 'ClaimStatus' 
--EXEC [QAData].[dbo].[ValidateBronzeStageToBronzeDeltaData_usingParquet] @TableName = 'BusinessCode' 
--EXEC [QAData].[dbo].[ValidateBronzeStageToBronzeDeltaData_usingParquet] @TableName = 'OrgName' 
--SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_ClaimStatus GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
--SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_BusinessCode GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
--SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_ApplicationUser GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
--SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_PolicyLine GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
--SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_PolicyLimit GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
--SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_PolicyOrg GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
--SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_LloydsBroker GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)



EXEC [QAData].[dbo].[ValidateBronzeStageToBronzeDeltaData_usingParquet] @TableName = 'Org' 
EXEC [QAData].[dbo].[ValidateBronzeStageToBronzeDeltaData_usingParquet] @TableName = 'PolicyOrg' 
EXEC [QAData].[dbo].[ValidateBronzeStageToBronzeDeltaData_usingParquet] @TableName = 'LloydsBroker' 
EXEC [QAData].[dbo].[ValidateBronzeStageToBronzeDeltaData_usingParquet] @TableName = 'ApplicationUser' 
EXEC [QAData].[dbo].[ValidateBronzeStageToBronzeDeltaData_usingParquet] @TableName = 'PolicyLine' 
EXEC [QAData].[dbo].[ValidateBronzeStageToBronzeDeltaData_usingParquet] @TableName = 'PolicyLimit' 
EXEC [QAData].[dbo].[ValidateBronzeStageToBronzeDeltaDataPolicy_usingParquet] @TableName = 'Policy' 

SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_ApplicationUser GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_Policy GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_PolicyLine GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_PolicyLimit GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_Org GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_PolicyOrg GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_LloydsBroker GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)




select 'DROP External Table ' + name  from sys.external_tables
 where name like 'TR_%'

DROP External Table TR_Org
DROP External Table TR_PolicyOrg
DROP External Table TR_LloydsBroker
DROP External Table TR_ApplicationUser
DROP External Table TR_PolicyLine
DROP External Table TR_PolicyLimit
DROP External Table TR_Policy
DROP External Table TR_Summary
DROP External Table TR_ClaimStatus
DROP External Table TR_BusinessCode