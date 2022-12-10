select  * from [EclipseBronze].[dbo].[ClaimStatus]

select  * from [EclipseSilverStaging].[dbo].[ClaimStatus]

select  * from [EclipseSilver].[dbo].[ClaimStatus]

--Run from Azure SQL Studio / Azure Synapse
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


