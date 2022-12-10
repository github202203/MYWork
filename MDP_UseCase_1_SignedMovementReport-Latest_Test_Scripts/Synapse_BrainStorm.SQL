Declare @QRY           NVARCHAR(MAX) = ''
;

SELECT @QRY = 'SELECT ' + CHAR(10) + 
       --STRING_AGG(CONCAT('`', column_name, '`'), ',')  
       STRING_AGG(CONCAT('', column_name, ''),   ',')
	  --STRING_AGG(CONCAT('', column_name, '%'),  ',') 
  FROM INFORMATION_SCHEMA.COLUMNS
 WHERE table_name = 'ClaimStatus'
 AND Column_Name NOT IN ('RowNumber','EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID')

Print @QRY