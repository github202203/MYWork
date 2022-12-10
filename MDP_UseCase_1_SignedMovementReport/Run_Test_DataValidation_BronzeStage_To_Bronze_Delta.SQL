--test data validation bronze stage to delta synapse view 
SELECT GetDate() as StartTime 


--Below are the list of scope for Use Case1 - Signed Movement Report
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_Policy] @TableName = 'Policy'
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn] @TableName = 'Org' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn] @TableName = 'PolicyOrg' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn] @TableName = 'LloydsBroker' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn] @TableName = 'ApplicationUser' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn] @TableName = 'PolicyLine' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn] @TableName = 'PolicyLimit' 

--Out of Scope List for Use Case 1
--EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn] @TableName = 'FinTransCategory' 
--EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn] @TableName = 'PolicyDeduction' 
--EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn] @TableName = 'FinTransCategoryRole' 
--EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn] @TableName = 'ReportingClass' 
--EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn] @TableName = 'PolicyPrem'     
--EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData] @TableName = 'BusinessCode'
--EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData] @TableName = 'ClaimStatus' 

SELECT GetDate() as EndTime


--To Check the Test Result
SELECT 'SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords' + CHAR(10) + 
       'FROM ' + name + CHAR(10) + 
       'GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)' + CHAR(10) + ' UNION ' 
  FROM qadata.sys.external_tables
 WHERE name like 'TR_%'

--SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_Policy GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION  
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_ApplicationUser GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_PolicyLine GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_PolicyLimit GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_Org GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_PolicyOrg GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)  UNION 
SELECT SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date) as TestedOn, Count(*) as TotalRecords FROM QAData.dbo.TR_LloydsBroker GROUP By SystemName,SynapseViewName,SystemLoadId,ObjectRunId,TestResultFileName,TestResult,CAST(TestedOn as Date)



EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn_SDT] @TableName = 'Org' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn_SDT] @TableName = 'PolicyOrg' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn_SDT] @TableName = 'LloydsBroker' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn_SDT] @TableName = 'ApplicationUser' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn_SDT] @TableName = 'PolicyLine' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn_SDT] @TableName = 'PolicyLimit' 



EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn_SDT_NoBinary] @TableName = 'Org' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn_SDT_NoBinary] @TableName = 'PolicyOrg' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn_SDT_NoBinary] @TableName = 'LloydsBroker' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn_SDT_NoBinary] @TableName = 'ApplicationUser' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn_SDT_NoBinary] @TableName = 'PolicyLine' 
EXEC [QAData].[dbo].[ValidateBronzeStageToDeltaData_UniqueColumn_SDT_NoBinary] @TableName = 'PolicyLimit' 