--Option 1
DECLARE @Counter INT
DECLARE @Iterations INT
DECLARE @MinNumberofColumns INT = 0
--DECLARE @MaxNumberofColumns INT = 80 --Harish
DECLARE @MaxNumberofColumns INT = 100
DECLARE @TableName VARCHAR(255) = 'Policy'


--SELECT @Iterations = CEILING(TRY_CONVERT(DECIMAL, MAX(ORDINAL_POSITION)) / 80)--Harish
SELECT @Iterations = CEILING(TRY_CONVERT(DECIMAL, MAX(ORDINAL_POSITION)) / 100)
  FROM INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_NAME = @TableName

SET @Counter = 1

WHILE (@Counter <= @Iterations)
BEGIN
    SELECT STRING_AGG(COLUMN_HASHLIST,',') as ALLHashList
	  FROM (
    SELECT STRING_AGG(COLUMN_NAME, ',') AS COLUMN_LIST 
	      ,'CONCAT(' + STRING_AGG(COLUMN_NAME, ',''%'',') + ')' AS COLUMN_HASHLIST
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = @TableName
        --AND ORDINAL_POSITION BETWEEN @MinNumberofColumns AND @MaxNumberofColumns --Harish
		AND ORDINAL_POSITION BETWEEN @MinNumberofColumns AND @MaxNumberofColumns-1
		) T

/*--Harish
    SET @Counter = @Counter + 1
    SET @MinNumberofColumns = @MinNumberofColumns + 80
    SET @MaxNumberofColumns = @MaxNumberofColumns + 80
*/
	SET @Counter = @Counter + 1
    SET @MinNumberofColumns = @MinNumberofColumns + 100
    SET @MaxNumberofColumns = @MaxNumberofColumns + 100
END


--Option2
SELECT STRING_AGG(CONVERT(NVARCHAR(MAX), C1),',') 
  FROM (
         SELECT 'HASHBYTES(' + '''SHA1''' + ',' + 'CONCAT(' + STRING_AGG(COLUMN_NAME, ',''%'',') + ')) as HB100' as C1 FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME = 'Policy' AND ORDINAL_POSITION BETWEEN 1 and 100
		 UNION ALL
         SELECT 'HASHBYTES(' + '''SHA1''' + ',' + 'CONCAT(' + STRING_AGG(COLUMN_NAME, ',''%'',') + ')) as HB200' as C1 FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME = 'Policy' AND ORDINAL_POSITION BETWEEN 101 and 200
		 UNION ALL
		 SELECT 'HASHBYTES(' + '''SHA1''' + ',' + 'CONCAT(' + STRING_AGG(COLUMN_NAME, ',''%'',') + ')) as HB300' as C1 FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME = 'Policy' AND ORDINAL_POSITION BETWEEN 201 and 300
		 )T
		 
		 
--Option 3


DECLARE @SQL NVARCHAR(MAX) = '' 

SELECT @Sql = @Sql + CASE LEN(@Sql) WHEN 0 THEN '' ELSE ',' END + COLUMN_NAME 
  FROM INFORMATION_SCHEMA.COLUMNS 
 WHERE TABLE_NAME = 'Policy'

print @SQL		 