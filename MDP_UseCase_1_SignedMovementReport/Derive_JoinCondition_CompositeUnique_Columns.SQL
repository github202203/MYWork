Declare @JoinCondition  NVARCHAR(MAX) = '';
Declare @tags	NVARCHAR(400) = (SELECT  UniqueColumn  FROM QAData.dbo.QAControlData WHERE SourceObjectName = 'dbo.ReportingClass')

--print @tags
print 'JoinCondition = ' + @JoinCondition

SELECT @JoinCondition = @JoinCondition + JoinConditionNew + CHAR(10) + ' and '  FROM (

SELECT --STR_Column, T.ColumnName,CollationName,
       'STAGE.STAGE' + T.ColumnName + ' ' + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE DATABASE_DEFAULT') + ' = ' + 
	   'DELTA.DELTA' + T.ColumnName + ' ' + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE DATABASE_DEFAULT') as JoinConditionNew
  FROM (
         SELECT value as STR_Column
           FROM STRING_SPLIT(@tags, ',')
           WHERE RTRIM(value) <> ''
		)STR

left Join 

        (
         SELECT 
                s.schema_id                                       as SchemaId
               ,s.name                                            as SchemaName 
               ,t.name                                            as TableName
               ,c.column_id                                       as ColumnId
               ,c.collation_name                                  as CollationName
               ,c.name                                            as ColumnName
               ,c.name +']' + ' as '  +  '[' + 'STAGE' + c.name   as ColumnNameSTAGE
         	   ,c.name +']' + ' as '  +  '[' + 'DELTA' + c.name   as ColumnNameDELTA
           FROM [EclipseBronze].[sys].[schemas] s
           join [EclipseBronze].[sys].[views]   t  on s.schema_id = t.schema_id
           join [EclipseBronze].[sys].[columns] c  on t.object_id = c.object_id 
           )T
 on STR.STR_Column = T.ColumnName
and TableName = 'ReportingClass'
)TT

print 'Join Condition = ' + @JoinCondition
SELECT @JoinCondition      = SUBSTRING (@JoinCondition,1   ,LEN(@JoinCondition)-4)
print 'Join Condition = ' + @JoinCondition