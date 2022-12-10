DECLARE @tags NVARCHAR(400) = 'Synd,PIMYear,Class1,Class2,Class3,Class4,ProducingTeam'
--DECLARE @tags NVARCHAR(400) = 'ClaimStatusId'
SELECT value
  FROM STRING_SPLIT(@tags, ',')
 WHERE RTRIM(value) <> '';