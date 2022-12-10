SELECT * 
  FROM (
        --Eclipse Data
        SELECT 
               s.schema_id                                       as SchemaId
        	  ,s.name                                            as SchemaName 
        	  ,t.name                                            as TableName
        	  ,c.column_id                                       as ColumnId
        	  ,c.collation_name                                  as CollationName
        	  ,c.name                                            as ColumnName
			  ,c.name +']' + ' as '  +  '[' + c.name + 'STAGE'   as ColumnNameSTAGE
			  ,c.name +']' + ' as '  +  '[' + c.name + 'DELTA'   as ColumnNameDELTA
          FROM [EclipseBronze].[sys].[schemas] s
          join [EclipseBronze].[sys].[views]   t  on s.schema_id = t.schema_id
          join [EclipseBronze].[sys].[columns] c  on t.object_id = c.object_id 
        UNION
        --Profisee Data
        SELECT 
               s.schema_id                                       as SchemaId
        	  ,s.name                                            as SchemaName 
        	  ,t.name                                            as TableName
        	  ,c.column_id                                       as ColumnId
        	  ,c.collation_name                                  as CollationName
        	  ,c.name                                            as ColumnName
			  ,c.name +']' + ' as '  +  '[' + c.name + 'STAGE'   as ColumnNameSTAGE
			  ,c.name +']' + ' as '  +  '[' + c.name + 'DELTA'   as ColumnNameDELTA
          FROM [ProfiseeBronze].[sys].[schemas] s
          join [ProfiseeBronze].[sys].[views]   t  on s.schema_id = t.schema_id
          join [ProfiseeBronze].[sys].[columns] c  on t.object_id = c.object_id 
		  )T 
 WHERE TableName NOT LIKE '%test%' 
   and CollationName is NOT NULL 
   and CollationName not like 'SQL_Latin1_General_CP1_CI_AS%'