--This SQL file works for CTE to SELECT Stage Data from Parquet file and it's WORKING OK 

DECLARE 
	    @BronzeCOLUMNS		NVARCHAR(MAX) = '' 
	   ,@BronzeCOLUMNList	NVARCHAR(MAX) = ''
	   ,@QRY				NVARCHAR(MAX) = ''
	   ,@BULK               NVARCHAR(MAX) = 'underwriting/Internal/Eclipse/DeltaLake/dbo_ClaimStatus/'
	   ,@BULK1               NVARCHAR(MAX) = 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/DeltaLake/dbo_ClaimStatus/*.parquet'
	   ,@DATASource         NVARCHAR(MAX) = 'Bronze'
	   ,@DATASource1         NVARCHAR(MAX) = 'Bronze'
	   ,@FORMAT             NVARCHAR(MAX) = 'Delta'
	   ,@FORMAT1             NVARCHAR(MAX) = 'PARQUET'
	   
select @BronzeCOLUMNS        = @BronzeCOLUMNS       + '[' + ColumnName    + ']' + ',' + '''%''' +','
      ,@BronzeCOLUMNList     = @BronzeCOLUMNList    + '[' + ColumnName    + ']' + ','
  from (
select 
        s.schema_id as SchemaId
	   ,s.name      as SchemName 
	   ,t.name      as TableName
	   ,c.column_id as ColumnId
	   ,c.name      as ColumnName
  from [EclipseBronze].[sys].[schemas] s
  join [EclipseBronze].[sys].[views]   t  on s.schema_id = t.schema_id
  join [EclipseBronze].[sys].[columns] c  on t.object_id = c.object_id 
  )T
 where TableName = 'ClaimStatus'

SELECT @BronzeCOLUMNS       = SUBSTRING (@BronzeCOLUMNS   ,1 ,LEN(@BronzeCOLUMNS)-5)
print @BronzeCOLUMNS 
SELECT @BronzeCOLUMNList    = SUBSTRING (@BronzeCOLUMNList,1 ,LEN(@BronzeCOLUMNList)-1)
print @BronzeCOLUMNList


SELECT @QRY = 'With CTE_Bronze_Delta as ('                                                                                                                   + CHAR(10) + 
		                   ('     SELECT ' + @BronzeCOLUMNList)                                                                                        + CHAR(10) + 
		                    '           ,HASHBYTES(' + '''SHA1'''       + ','      + 'CONCAT(' + @BronzeCOLUMNS + ')' + ')' + 'as Delta_HashBytes'          + CHAR(10) + 
						    '       FROM OPENROWSET( ' + CHAR(10) + 
                            '            BULK ' + '''' +@BULK+ ''''+ ',' +  CHAR(10) + 
                            '            DATA_SOURCE = ' + '''' +@DATASource+ '''' + ',' + CHAR(10) + 
                            '            FORMAT = ' + ''''+@FORMAT+''''	+ CHAR(10) +
							'                     ) as RS' + CHAR(10) +    
							')'                             + CHAR(10) +

						 ,'CTE_Bronze_Stage as ('                                                                                                      + CHAR(10) + 
						 '('     SELECT ' + @BronzeCOLUMNList)                                                                                         + CHAR(10) + 
						 '           ,HASHBYTES(' + '''SHA1'''       + ','      + 'CONCAT(' + @BronzeCOLUMNS + ')' + ')' + 'as Stage_HashBytes'           + CHAR(10) + 
						 '       FROM OPENROWSET( ' + CHAR(10) + 
						    '            BULK ' + '''' +@BULK1+ ''''+ ',' +  CHAR(10) + 
                            '            FORMAT1 = ' + ''''+@FORMAT+''''	+ CHAR(10) +
							'                     ) as RS' + CHAR(10) +    
							')'                             + CHAR(10) +
							')'


							'SELECT * FROM CTE_Bronze_Delta'

print @QRY
--EXECUTE sp_executesql @QRY
EXEC (@QRY)

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://dldpdev01.dfs.core.windows.net/bronze/underwriting/Internal/Eclipse/DeltaLake/dbo_ClaimStatus/*.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]