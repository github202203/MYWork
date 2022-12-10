--Script to verify only Bronze Layet Synapse view existence

use [EclipseBronze]

SELECT SRC.TableName, TGT.BronzeSchemaName,TGT.BronzeViewName, TGT.ObjectName
      ,Case when SRC.TableName = TGT.ObjectName 
	        then 'Bronze Layer - Eclipse Delta view exists - Passed' 
	        else 'Bronze Layer - Eclipse Delta view doens''t exists - Failed'
	    End as TestResult
	  ,GetDate() as TestedOn

  FROM (
        --List of Bronze Layer tables/view for Use Case1
        select 'dbo_Policy'               as TableName union
        select 'dbo_Org'                  as TableName union
        select 'dbo_PolicyOrg'            as TableName union
        select 'dbo_LloydsBroker'         as TableName union
        select 'dbo_ApplicationUser'      as TableName union
        select 'dbo_PolicyLine'           as TableName union
        select 'dbo_PolicyLimit'          as TableName union
        select 'dbo_PolicyPrem'           as TableName union
        select 'dbo_FinTransCategory'     as TableName union
        select 'dbo_ReportingClass'       as TableName union
        select 'dbo_PolicyDeduction'      as TableName union
        select 'dbo_FinTransCategoryRole' as TableName union
		select 'dbo_FinTransCategoryRole' as TableName union
        select 'dbo_RESPCODE'             as TableName union
        select 'dbo_DETAILATTDATA'        as TableName

	   ) SRC

  left Join

      (
        select 
                s.schema_id           as SchemaId
        	   ,s.name                as BronzeSchemaName 
        	   ,t.name                as BronzeViewName
        	   ,s.name + '_' + t.name as ObjectName
          from [EclipseBronze].[sys].[schemas] s
          join [EclipseBronze].[sys].[views]   t  
            on s.schema_id = t.schema_id
		) TGT

	on SRC.TableName = TGT.ObjectName
 
