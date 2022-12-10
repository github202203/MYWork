/* ***************************************************************************************************************************************************************

/*************************************************************************************************/
/*                                         Building CTE Query from Bronze Delta tables                              */
/* --------------------------------------------------------------------------------------------- */
/* Date        | Author              | Description                                               */
/* --------------------------------------------------------------------------------------------- */
/* 01/10/2022  | Ram Baskar          | Building CTE Query from Bronze Layer view                 */

/*************************************************************************************************/
 
DECLARE 
	    @BronzeCOLUMNS		NVARCHAR(MAX) = '' 
	   ,@BronzeCOLUMNList	NVARCHAR(MAX) = ''
	   ,@QRY				NVARCHAR(MAX) = ''
	   ,@BULK               NVARCHAR(MAX) = 'underwriting/Internal/Eclipse/DeltaLake/dbo_ClaimStatus/'
	   ,@DATASource         NVARCHAR(MAX) = 'Bronze'
	   ,@FORMAT             NVARCHAR(MAX) = 'Delta'
	   
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

select @BronzeCOLUMNS    = SUBSTRING (@BronzeCOLUMNS   ,1 ,LEN(@BronzeCOLUMNS)-5)
--print  @BronzeCOLUMNS 
select @BronzeCOLUMNList = SUBSTRING (@BronzeCOLUMNList,1 ,LEN(@BronzeCOLUMNList)-1)
--print @BronzeCOLUMNList

--Building CTE

select @QRY = 'With CTE_Bronze as ('                                                                                                                   + CHAR(10) + 
		                   ('     select  ' + @BronzeCOLUMNList)                                                                                       + CHAR(10) + 
		                    '            ,HASHBYTES(' + '''SHA1'''       + ','      + 'CONCAT(' + @BronzeCOLUMNS + ')' + ')' + 'as Eclipse_HashBytes'  + CHAR(10) + 
						    '       from OPENROWSET( '                                                                                                 + CHAR(10) + 
                            '            BULK ' + '''' + @BULK + ''''+ ','                                                                             + CHAR(10) + 
                            '            DATA_SOURCE = ' + '''' + @DATASource + '''' + ','                                                             + CHAR(10) + 
                            '            FORMAT = ' + ''''+ @FORMAT +''''	                                                                           + CHAR(10) +
							'                     ) as RS'                                                                                             + CHAR(10) +    
							')'                                                                                                                        + CHAR(10) +
							
							'select * from CTE_Bronze'

--print @QRY
--EXECUTE sp_executesql @QRY
EXEC (@QRY)
--Either EXEC sp_executesql @QRY or EXEC(@QRY) - both will work