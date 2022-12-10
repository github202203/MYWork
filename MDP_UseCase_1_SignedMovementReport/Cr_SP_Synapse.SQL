CREATE or ALTER Procedure [dbo].[CTE_Test]
AS 
BEGIN
With 
    CTE_ClaimEvent as ( 
        select * 
          from [EclipseBronze].[dbo].[ClaimStatus]
    )

   ,ExpectedResult as (
       select * 
         from CTE_ClaimEvent 
   )

   select * 
     from ExpectedResult
END;
Go