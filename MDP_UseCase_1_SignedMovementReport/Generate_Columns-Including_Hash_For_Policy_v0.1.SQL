--This script will convert only smalldatetime columns not the datetime columns
--50% of the data is failing 

DECLARE

 @BronzeCOLUMNListSTAGE100      NVARCHAR(MAX) = ''
,@BronzeColumnsForHashSTG100	NVARCHAR(MAX) = '' 
,@BronzeCOLUMNListDELTA100      NVARCHAR(MAX) = ''
,@BronzeColumnsForHashTGT100    NVARCHAR(MAX) = ''

;

SELECT @BronzeCOLUMNListSTAGE100   = @BronzeCOLUMNListSTAGE100 + ColumnNameSTAGE100  + ','
      ,@BronzeColumnsForHashSTG100 = @BronzeColumnsForHashSTG100 + ColumnName100       + ',' + '''%''' +',' 
	  ,@BronzeCOLUMNListDELTA100   = @BronzeCOLUMNListDELTA100 + ColumnNameDELTA100  + ','
	  --,@BronzeColumnsForHashTGT100 = @BronzeColumnsForHashTGT100    + '[' + ColumnName100      + ']' + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_100_BIN2_UTF8') + ',' + '''%''' +','
	  ,@BronzeColumnsForHashTGT100 = @BronzeColumnsForHashTGT100    +  ColumnNameDELTAHASH100     + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_100_BIN2_UTF8') + ',' + '''%''' +','

  FROM (
        SELECT 
              Table_Schema                                      as SchemaName 
             ,Table_Name                                        as TableName
             ,Ordinal_Position                                  as ColumnId
             ,Collation_name                                  as CollationName
	         ,Data_Type
	         ,Case when Data_Type = 'smalldatetime' 
	               then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '104' + ')' + ' as ' + Column_Name
			       else Column_Name
		       End as ColumnNameSTAGE100
             ,Case when Data_Type = 'smalldatetime' 
	               then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '104' + ')'
			       else Column_Name
		       End as ColumnName100
	  ,'['+Column_Name +']' + ' as '  +  '[' + 'DELTA' + Column_Name +']' as ColumnNameDELTA100
	  ,'['+Column_Name+']'   as ColumnNameDELTAHASH100
	  --,Column_Name  + ' as '  +  'DELTA' + Column_Name   as ColumnNameDELTA100
  FROM EclipseBronze.INFORMATION_SCHEMA.COLUMNS
 WHERE Table_Schema = 'dbo' and Table_Name = 'Policy' 
   AND Ordinal_Position between 101 and 100
   AND Column_Name NOT IN ('RowNumber','EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID')
	   ) T1

print 'STG 100 Columns      = ' + @BronzeCOLUMNListSTAGE100   
print 'STG 100 Columns Hash = ' + @BronzeColumnsForHashSTG100
print 'DELTA 100 Columns = ' + @BronzeCOLUMNListDELTA100 
print 'TGT 100 Columns Hash = ' + @BronzeColumnsForHashTGT100

DECLARE

 @BronzeCOLUMNListSTAGE200      NVARCHAR(MAX) = ''
,@BronzeColumnsForHashSTG200	NVARCHAR(MAX) = '' 
,@BronzeCOLUMNListDELTA200      NVARCHAR(MAX) = ''
,@BronzeColumnsForHashTGT200    NVARCHAR(MAX) = ''

;

SELECT @BronzeCOLUMNListSTAGE200   = @BronzeCOLUMNListSTAGE200 + ColumnNameSTAGE200  + ','
      ,@BronzeColumnsForHashSTG200 = @BronzeColumnsForHashSTG200 + ColumnName200       + ',' + '''%''' +',' 
	  ,@BronzeCOLUMNListDELTA200   = @BronzeCOLUMNListDELTA200 + ColumnNameDELTA200  + ','
	  --,@BronzeColumnsForHashTGT200 = @BronzeColumnsForHashTGT200    + '[' + ColumnName200      + ']' + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_200_BIN2_UTF8') + ',' + '''%''' +','
	  ,@BronzeColumnsForHashTGT200 = @BronzeColumnsForHashTGT200    +  ColumnNameDELTAHASH200     + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_100_BIN2_UTF8') + ',' + '''%''' +','

  FROM (
        SELECT 
              Table_Schema                                      as SchemaName 
             ,Table_Name                                        as TableName
             ,Ordinal_Position                                  as ColumnId
             ,Collation_name                                  as CollationName
	         ,Data_Type
	         ,Case when Data_Type = 'smalldatetime' 
	               then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '104' + ')' + ' as ' + Column_Name
			       else Column_Name
		       End as ColumnNameSTAGE200
             ,Case when Data_Type = 'smalldatetime' 
	               then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '104' + ')'
			       else Column_Name
		       End as ColumnName200
	  ,'['+Column_Name +']' + ' as '  +  '[' + 'DELTA' + Column_Name +']' as ColumnNameDELTA200
	  ,'['+Column_Name+']'   as ColumnNameDELTAHASH200
	  --,Column_Name  + ' as '  +  'DELTA' + Column_Name   as ColumnNameDELTA200
  FROM EclipseBronze.INFORMATION_SCHEMA.COLUMNS
 WHERE Table_Schema = 'dbo' and Table_Name = 'Policy' 
   AND Ordinal_Position between 101 and 200
   AND Column_Name NOT IN ('RowNumber','EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID')
	   ) T1

print 'STG 200 Columns      = ' + @BronzeCOLUMNListSTAGE200   
print 'STG 200 Columns Hash = ' + @BronzeColumnsForHashSTG200
print 'DELTA 200 Columns = ' + @BronzeCOLUMNListDELTA200 
print 'TGT 200 Columns Hash = ' + @BronzeColumnsForHashTGT200

DECLARE

 @BronzeCOLUMNListSTAGE300      NVARCHAR(MAX) = ''
,@BronzeColumnsForHashSTG300	NVARCHAR(MAX) = '' 
,@BronzeCOLUMNListDELTA300      NVARCHAR(MAX) = ''
,@BronzeColumnsForHashTGT300    NVARCHAR(MAX) = ''

;

SELECT @BronzeCOLUMNListSTAGE300   = @BronzeCOLUMNListSTAGE300 + ColumnNameSTAGE300  + ','
      ,@BronzeColumnsForHashSTG300 = @BronzeColumnsForHashSTG300 + ColumnName300       + ',' + '''%''' +',' 
	  ,@BronzeCOLUMNListDELTA300   = @BronzeCOLUMNListDELTA300 + ColumnNameDELTA300  + ','
	  --,@BronzeColumnsForHashTGT300 = @BronzeColumnsForHashTGT300    + '[' + ColumnName300      + ']' + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_300_BIN2_UTF8') + ',' + '''%''' +','
	  ,@BronzeColumnsForHashTGT300 = @BronzeColumnsForHashTGT300    +  ColumnNameDELTAHASH300     + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_100_BIN2_UTF8') + ',' + '''%''' +','

  FROM (
        SELECT 
              Table_Schema                                      as SchemaName 
             ,Table_Name                                        as TableName
             ,Ordinal_Position                                  as ColumnId
             ,Collation_name                                  as CollationName
	         ,Data_Type
	         ,Case when Data_Type = 'smalldatetime' 
	               then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '104' + ')' + ' as ' + Column_Name
			       else Column_Name
		       End as ColumnNameSTAGE300
             ,Case when Data_Type = 'smalldatetime' 
	               then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '104' + ')'
			       else Column_Name
		       End as ColumnName300
	  ,'['+Column_Name +']' + ' as '  +  '[' + 'DELTA' + Column_Name +']' as ColumnNameDELTA300
	  ,'['+Column_Name+']'   as ColumnNameDELTAHASH300
	  --,Column_Name  + ' as '  +  'DELTA' + Column_Name   as ColumnNameDELTA300
  FROM EclipseBronze.INFORMATION_SCHEMA.COLUMNS
 WHERE Table_Schema = 'dbo' and Table_Name = 'Policy' 
   AND Ordinal_Position > 200 
   AND Column_Name NOT IN ('RowNumber','EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID')
	   ) T1

print 'STG 300 Columns      = ' + @BronzeCOLUMNListSTAGE300   
print 'STG 300 Columns Hash = ' + @BronzeColumnsForHashSTG300
print 'DELTA 300 Columns = ' + @BronzeCOLUMNListDELTA300 
print 'TGT 300 Columns Hash = ' + @BronzeColumnsForHashTGT300