

/**********************************************************************************************************************************************************
   Version  |  Date        | Author              | Description                                                                  
--------------------------------------------------------------------------------------------------------------------------------------------- 


   0.1      |  19/11/2022  | Ram Baskar          | Script to validate Transformation Rule between Bronze Staging - Silver Staging
   0.2      |  21/11/2022  | Ram Baskar          | Amended the script with Hash Bytes for validation
   0.3      |  22/11/2022  | Ram Baskar          | Amended the script to compare the data between Parquet to Parquet
   0.4      |  25/11/2022  | Ram Baskar          | Amended the script in sync with the data mapping document as at 25th Nov, 2022@12 Noon
   0.5      |  25/11/2022  | Ram Baskar          | Amended the script to pick up the SystemLoadId from Source dynamically based on ControlQAData
   0.6      |  26/11/2022  | Ram Baskar          | Amended the script with few variables to display in the TestResult
    
	 Layer:Silver 
	Entity:Policy

    Script Type - 100% manual
    Script neexs to be amended 

**********************************************************************************************************************************************************/

DECLARE @SystemLoadID	BIGINT = ''
       ,@Layer          VARCHAR(20)   = 'Silver'
       ,@TestType       NVARCHAR(MAX) = 'Transformation Rule Testng'
       ,@SchemaName	    NVARCHAR(MAX) = 'dbo'
       ,@TableName	    NVARCHAR(MAX) = 'Policy'
;

SET @SystemLoadId      = (SELECT SystemLoadId  FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and SystemLoadId like '102%')
;

With 
 CTE_Source as (--Policy Source Data from Eclipse Bronze Staging PARQUET
 SELECT  
        PolicyRef                                                                                                   as SRC_PolicyReference 
	   ,Case when ProgramRef is NULL then SUBSTRING(PolicyRef,1,8) else ProgramRef End                              as SRC_ProgrammeReference 
	   ,PolicyId                                                                                                    as SRC_PolicyID
	   ,ISNULL(YOA,0)                                                                                               as SRC_YOA
	   ,Case when InceptionDate is NULL then CAST('1900-01-01' as Date) else CAST(InceptionDate as Date) End        as SRC_InceptionDate
	   ,Case when ExpiryDate    is NULL then CAST('1900-01-01' as Date) else CAST(ExpiryDate    as Date) End        as SRC_ExpiryDate
	   ,Case when DateWritten   is NULL then CAST('1900-01-01' as Date) else CAST(DateWritten   as Datetime2) End   as SRC_WrittenDate
	   ,ISNULL(UPPER(PlacingType), '')                                                                              as SRC_PlacingBasis 
	   ,cpb.conformedCode                                                                                           as SRC_PlacingBasisCode
	   ,cpb.name                                                                                                    as SRC_PlacingBasisName
	   ,ISNULL(UniqueMarketRef,'')                                                                                  as SRC_UniqueMarketRef
	   ,DelDate                                                                                                     as SRC_DelDate
	   ,Class1                                                                                                      as SRC_Class1
	   ,Class2                                                                                                      as SRC_Class2
	   ,Class3                                                                                                      as SRC_Class3
       ,Class4                                                                                                      as SRC_Class4
	   ,EntryType                                                                                                   as SRC_EntryType
	   ,ParentPolicyId                                                                                              as SRC_ParentPolicyID
	   --,UW                                                                                                          as SRC_UW Attribute available in Bronze
       ,(SELECT mdmCode FROM ProfiseeBronze.dbo.Ref_SourceSystem WHERE name = 'Eclipse')                            as SRC_SourceSystemCode
	   ,SYS_CHANGE_OPERATION                                                                                        as SRC_Sys_Change_Operation
	   --,SubclassCode                                                                                                as SRC_SubclassCode --Column does not exists in Bronze Staging
	   ,CAST(SPQ.filepath(1) AS BIGINT)                                                                             as SRC_SystemLoadID
	   
   FROM  OPENROWSET(
        --BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_Policy/SystemLoadID=1022022112402/dbo_Policy_1022022112402.parquet',
		BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_Policy/SystemLoadID=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS SPQ
	
   Left Join ProfiseeBronze.dbo.Ref_PlacingBasis pb            on CONCAT('ECL_',PlacingType) = pb.code  COLLATE SQL_Latin1_General_CP1_CI_AS
   Left Join ProfiseeBronze.dbo.Ref_Conformed_PlacingBasis cpb on pb.conformedPlacingBasis   = cpb.code COLLATE SQL_Latin1_General_CP1_CI_AS
  --WHERE PolicyId = 28
    )

,CTE_Target as (--Policy Target Data from Eclipse Silver Staging PARQUET
 SELECT 
       PolicyReference                                                                                         as TGT_PolicyReference 
      ,ProgrammeReference                                                                                      as TGT_ProgrammeReference
      ,PolicyID                                                                                                as TGT_PolicyID
      ,YOA                                                                                                     as TGT_YOA
      ,InceptionDate                                                                                           as TGT_InceptionDate
      ,ExpiryDate                                                                                              as TGT_ExpiryDate
      ,WrittenDate                                                                                             as TGT_WrittenDate
      ,PlacingBasis                                                                                            as TGT_PlacingBasis 
      ,PlacingBasisCode                                                                                        as TGT_PlacingBasisCode
      ,PlacingBasisName                                                                                        as TGT_PlacingBasisName
      ,UniqueMarketRef                                                                                         as TGT_UniqueMarketRef
	  ,DelDate                                                                                                 as TGT_DelDate
	  ,Class1                                                                                                  as TGT_Class1
      ,Class2                                                                                                  as TGT_Class2
      ,Class3                                                                                                  as TGT_Class3
      ,Class4                                                                                                  as TGT_Class4
      ,EntryType                                                                                               as TGT_EntryType
      ,ParentPolicyID                                                                                          as TGT_ParentPolicyID
      --,UW  --This attribute missing in the EclipseSilverStaging.dbo.Policy view                              as TGT_UW --Attribute not available in Silver staging
      ,(SELECT mdmCode FROM ProfiseeBronze.dbo.Ref_SourceSystem WHERE name = 'Eclipse')                        as TGT_SourceSystemCode
	  ,SYS_CHANGE_OPERATION                                                                                    as TGT_Sys_Change_Operation
	  --,SubclassCode                                                                                            as TGT_SubclassCode --Column does not exists in Bronze Staging
	  ,CAST(TPQ.filepath(1) AS BIGINT)                                                                         as TGT_SystemLoadID
   FROM OPENROWSET(
        --BULK 'https://dldpdev01.dfs.core.windows.net/silver/underwriting/Internal/Eclipse/Staging/dbo_Policy/SystemLoadID=1042022112402/dbo_Policy_1042022112402.parquet',
		BULK 'https://dldpdev01.dfs.core.windows.net/silver/underwriting/Internal/Eclipse/Staging/dbo_Policy/SystemLoadID=*/*.parquet',
        FORMAT = 'PARQUET'
    ) AS TPQ
  --WHERE PolicyID = 28
)

,ExpectedResult AS (

SELECT @@ServerName as Server,  @Layer as Layer, @SchemaName + '.' + @TableName as Entity, @TestType as TestType,GetDate() as TestedOn
       ,Case when SRC_HB = TGT_HB then 'Passed' else 'Failed' End as TestResult
	   ,SRC.*,TGT.*
  FROM (

        SELECT 
               SRC_PolicyReference 
              ,SRC_ProgrammeReference
              ,SRC_PolicyID
              ,SRC_YOA
              ,SRC_InceptionDate
              ,SRC_ExpiryDate
              ,SRC_WrittenDate
              ,SRC_PlacingBasis 
              ,SRC_PlacingBasisCode
              ,SRC_PlacingBasisName
              ,SRC_UniqueMarketRef
			  ,SRC_DelDate
			  ,SRC_Class1
			  ,SRC_Class2
			  ,SRC_Class3
              ,SRC_Class4
			  ,SRC_EntryType
			  ,SRC_ParentPolicyID
			  --,SRC_UW
              ,SRC_SourceSystemCode
			  ,SRC_Sys_Change_Operation
			  --,SRC_SubclassCode
			  ,SRC_SystemLoadID
			  --,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT(SRC_PolicyReference ,'%',SRC_ProgrammeReference,'%',SRC_PolicyID,'%',SRC_YOA,'%',SRC_InceptionDate,'%',SRC_ExpiryDate,'%',SRC_WrittenDate,'%',SRC_PlacingBasis,'%',SRC_PlacingBasisCode COLLATE SQL_Latin1_General_CP1_CI_AS,'%',SRC_PlacingBasisName COLLATE SQL_Latin1_General_CP1_CI_AS,'%',SRC_UniqueMarketRef,'%',SRC_DelDate,'%',SRC_Class1,'%',SRC_Class2,'%',SRC_Class3,'%',SRC_Class4,'%',SRC_EntryType,'%',SRC_ParentPolicyID,'%',/*SRC_UW,'%',*/SRC_SourceSystemCode COLLATE SQL_Latin1_General_CP1_CI_AS,'%',SRC_Sys_Change_Operation))) as SRC_HB
              	--with NO Collation
                ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT(SRC_PolicyReference ,'%',SRC_ProgrammeReference,'%',SRC_PolicyID,'%',SRC_YOA,'%',SRC_InceptionDate,'%',SRC_ExpiryDate,'%',SRC_WrittenDate,'%',SRC_PlacingBasis,'%',SRC_PlacingBasisCode,'%',SRC_PlacingBasisName,'%',SRC_UniqueMarketRef,'%',SRC_DelDate,'%',SRC_Class1,'%',SRC_Class2,'%',SRC_Class3,'%',SRC_Class4,'%',SRC_EntryType,'%',SRC_ParentPolicyID,'%',/*SRC_UW,'%',*/SRC_SourceSystemCode,'%',SRC_Sys_Change_Operation))) as SRC_HB
          FROM CTE_Source
		  WHERE SRC_SystemLoadID = @SystemLoadID
        ) SRC

  Left Join 

       (
        SELECT 
               TGT_PolicyReference 
              ,TGT_ProgrammeReference
              ,TGT_PolicyID
              ,TGT_YOA
              ,TGT_InceptionDate
              ,TGT_ExpiryDate
              ,TGT_WrittenDate
              ,TGT_PlacingBasis 
              ,TGT_PlacingBasisCode
              ,TGT_PlacingBasisName
              ,TGT_UniqueMarketRef
			  ,TGT_DelDate
			  ,TGT_Class1
			  ,TGT_Class2
			  ,TGT_Class3
              ,TGT_Class4
			  ,TGT_EntryType
			  ,TGT_ParentPolicyID
              --,TGT_UW
              ,TGT_SourceSystemCode
			  ,TGT_Sys_Change_Operation
			  --,TGT_SubclassCode
			  --,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT(TGT_PolicyReference,'%',TGT_ProgrammeReference,'%',TGT_PolicyID,'%',TGT_YOA,'%',TGT_InceptionDate,'%',TGT_ExpiryDate,'%',TGT_WrittenDate,'%',TGT_PlacingBasis,'%',TGT_PlacingBasisCode ,'%',TGT_PlacingBasisName ,'%',TGT_UniqueMarketRef,'%',TGT_DelDate,'%',TGT_Class1,'%',TGT_Class2,'%',TGT_Class3,'%',TGT_Class4,'%',TGT_EntryType,'%',TGT_ParentPolicyID,'%',/*SRC_UW,'%',*/TGT_SourceSystemCode COLLATE SQL_Latin1_General_CP1_CI_AS,'%',TGT_Sys_Change_Operation)))  as TGT_HB
              --with NO Collation
              ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT(TGT_PolicyReference,'%',TGT_ProgrammeReference,'%',TGT_PolicyID,'%',TGT_YOA,'%',TGT_InceptionDate,'%',TGT_ExpiryDate,'%',TGT_WrittenDate,'%',TGT_PlacingBasis,'%',TGT_PlacingBasisCode ,'%',TGT_PlacingBasisName ,'%',TGT_UniqueMarketRef,'%',TGT_DelDate,'%',TGT_Class1,'%',TGT_Class2,'%',TGT_Class3,'%',TGT_Class4,'%',TGT_EntryType,'%',TGT_ParentPolicyID,'%',/*SRC_UW,'%',*/TGT_SourceSystemCode,'%',TGT_Sys_Change_Operation)))  as TGT_HB
          FROM CTE_Target
        )TGT 

    on SRC_PolicyID = TGT_PolicyID
       ) 
  
SELECT *
  FROM ExpectedResult

