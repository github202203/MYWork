
SELECT [ClaimStatus]
      ,[ClaimStatusID]
      ,[Dsc]
      ,[OwnerID]
      ,[InsDate]
      ,[DelDate]
      ,[LastUpd]
      ,[UpdBy]
      ,[SYS_CHANGE_OPERATION]
      ,[EffectiveDateUTC]
      ,[EndDateUTC]
	  ,LEAD(EffectiveDateUTC) over (Partition by ClaimStatusID Order by EffectiveDateUTC) as NXT_EffectiveStartDateUTC
	  ,Case when LEAD(EffectiveDateUTC) over ( Partition By ClaimStatusID ORDER by EffectiveDateUTC) is NOT NULL 
	         and LEAD(EffectiveDateUTC) over ( Partition By ClaimStatusID ORDER by EffectiveDateUTC) = EndDateUTC 
			 then 'Passed'
            when LEAD(EffectiveDateUTC) over ( Partition By ClaimStatusID Order by EffectiveDateUTC) is NULL 
			 and ISNULL(CAST(EndDateUTC as Date),'2099-12-31') = '2099-12-31' 
			then 'Passed'
            Else 'Failed'
        End as SCDType2TestResult

  FROM [dbo].[ClaimStatus]
  
  
 ----WIP Script for SCD Type2 using Dynamic SQL
 
 Declare 

 @ColumnList	NVARCHAR(MAX) = ''
,@SchemaName    NVARCHAR(MAX) = 'dbo'
,@TableName     NVARCHAR(MAX) = 'ClaimStatus'
,@QRY           NVARCHAR(MAX) = ''

SELECT 
	  @ColumnList        = @ColumnList           + '[' + ColumnName      + ']' + ','
  FROM (
        SELECT 
               s.schema_id                                       as SchemaId
        	  ,s.name                                            as SchemaName 
        	  ,t.name                                            as TableName
        	  ,c.column_id                                       as ColumnId
        	  ,c.collation_name                                  as CollationName
        	  ,c.name                                            as ColumnName
			  --,c.name +']' + ' as '  +  '[' + 'STAGE' + c.name   as ColumnNameSTAGE
			  --,c.name +']' + ' as '  +  '[' + 'DELTA' + c.name   as ColumnNameDELTA
			  
          FROM [EclipseBronze].[sys].[schemas] s
          join [EclipseBronze].[sys].[views]   t  on s.schema_id = t.schema_id
          join [EclipseBronze].[sys].[columns] c  on t.object_id = c.object_id 
        )T
 
 WHERE SchemaName = @Schemaname
   AND TableName = @TableName
   AND ColumnName NOT IN ('RowNumber','EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID')

@QRY = print @ColumnList


,LEAD(EffectiveDateUTC) over (Partition by ClaimStatusID Order by EffectiveDateUTC) as NXT_EffectiveStartDateUTC
	  ,Case when LEAD(EffectiveDateUTC) over ( Partition By ClaimStatusID ORDER by EffectiveDateUTC) is NOT NULL 
	         and LEAD(EffectiveDateUTC) over ( Partition By ClaimStatusID ORDER by EffectiveDateUTC) = EndDateUTC 
			 then 'Passed'
            when LEAD(EffectiveDateUTC) over ( Partition By ClaimStatusID Order by EffectiveDateUTC) is NULL 
			 and ISNULL(CAST(EndDateUTC as Date),'2099-12-31') = '2099-12-31' 
			then 'Passed'
            Else 'Failed'
        End as SCDType2TestResult