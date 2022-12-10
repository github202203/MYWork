select
       s.schema_id as SchemaId
	  ,s.name      as SchemName 
	  ,t.name      as TableName
	  ,c.column_id as ColumnId
	  ,c.name      as ColumnName
	  ,c.name      as ColumnNameMYMI
	  ,c.name      as ColumnNameDAL
from sys.[schemas] s
join sys.[views]   t  on s.schema_id = t.schema_id
join sys.[columns] c  on t.object_id = c.object_id 

use EclipseBronze;
select * 
  from [dbo].[ClaimStatus]

use EclipseSilver;
select * 
  from [dbo].[ClaimStatus]
  
--List of Columns from Data Dictionary in Synapse for multiple database  
select * 
  from (
select 
        s.schema_id as SchemaId
	   ,s.name      as SchemName 
	   ,t.name      as TableName
	   ,c.column_id as ColumnId
	   ,c.name      as ColumnName
	   --,c.name      as ColumnNameMYMI
	   --,c.name      as ColumnNameDAL
  from [EclipseBronze].[sys].[schemas] s
  join [EclipseBronze].[sys].[views]   t  on s.schema_id = t.schema_id
  join [EclipseBronze].[sys].[columns] c  on t.object_id = c.object_id 
  )T
 where TableName = 'ClaimStatus'

select * 
  from (
select 
        s.schema_id as SchemaId
	   ,s.name      as SchemName 
	   ,t.name      as TableName
	   ,c.column_id as ColumnId
	   ,c.name      as ColumnName
	   --,c.name      as ColumnNameMYMI
	   --,c.name      as ColumnNameDAL
  from [EclipseSilver].[sys].[schemas] s
  join [EclipseSilver].[sys].[views]   t  on s.schema_id = t.schema_id
  join [EclipseSilver].[sys].[columns] c  on t.object_id = c.object_id 
  )T
 where TableName = 'ClaimStatus'  