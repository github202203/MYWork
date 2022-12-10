With 
    CTE_SRC as (
    SELECT PolicyRef                              as PolicyReference
		  ,ISNULL(ProgramRef, LEFT(PolicyRef, 8)) as ProgrammeReference
    	  ,PolicyId                               as PolicyId
    	  ,ISNULL(YOA,0)                          as YOA
    	  ,InceptionDate                          as InceptionDate
    	  ,ExpiryDate                             as ExpiryDate
    	  ,DateWritten                            as WrittenDate
    	  ,PlacingType                            as PlacingBasis
    	  ,ISNULL(UniqueMarketRef,'')             as UniqueMarketRef
          ,EffectiveDateUTC                       as EffectiveDateUTC
    	  ,EndDateUTC                             as EndDateUTC
      FROM EclipseBronze.dbo.Policy
  )

,ExpectedResult as (

 SELECT 
        PolicyReference
	   ,ProgrammeReference
	   ,PolicyId
	   ,YOA
	   ,InceptionDate
	   ,ExpiryDate
	   ,WrittenDate
	   ,PlacingBasis
	   ,UniqueMarketRef
   FROM CTE_SRC
   )

SELECT * 
  FROM ExpectedResult
      



