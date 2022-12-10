/*
DECLARE

 @BronzeCOLUMNListSTAGE100      NVARCHAR(MAX) = ''
,@BronzeColumnsForHashSTG100	NVARCHAR(MAX) = '' 
,@BronzeColumnsForHashTGT100    NVARCHAR(MAX) = ''

;

SELECT @BronzeColumnsForHashSTG100 = @BronzeColumnsForHashSTG100     + ColumnName100       + ',' + '''%''' +',' 
	  ,@BronzeCOLUMNListSTAGE100   = @BronzeCOLUMNListSTAGE100       + ColumnNameSTAGE100  + ','
	  ,@BronzeColumnsForHashTGT100 = @BronzeColumnsForHashTGT100    + '[' + ColumnName100      + ']' + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_100_BIN2_UTF8') + ',' + '''%''' +','

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
	  --,Column_Name + ' as '  + 'STAGE' + Column_Name               as ColumnNameSTAGE100
	  ,Column_Name +']' + ' as '  +  '[' + 'DELTA' + Column_Name   as ColumnNameDELTA
  FROM EclipseBronze.INFORMATION_SCHEMA.COLUMNS
 WHERE Table_Schema = 'dbo' and Table_Name = 'Policy' 
   AND Ordinal_Position <= 100
   --AND Ordinal_Position between 101 and 200
   --AND Ordinal_Position > 101 
   AND Column_Name NOT IN ('RowNumber','EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID')
	   ) T1

print 'STG 100 Columns      = ' + @BronzeCOLUMNListSTAGE100   
print 'STG 100 Columns Hash = ' + @BronzeColumnsForHashSTG100
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

*/


--v0.2 script to include both datetime and smalldatetime conversion

DECLARE

 @BronzeCOLUMNListSTAGE100      NVARCHAR(MAX) = ''
,@BronzeColumnsForHashSTG100	NVARCHAR(MAX) = '' 
,@BronzeCOLUMNListDELTA100      NVARCHAR(MAX) = ''
,@BronzeColumnsForHashTGT100    NVARCHAR(MAX) = ''

;

SELECT @BronzeCOLUMNListSTAGE100   = @BronzeCOLUMNListSTAGE100       + ColumnNameSTAGE100  + ','
      ,@BronzeColumnsForHashSTG100 = @BronzeColumnsForHashSTG100     + ColumnName100       + ',' + '''%''' +',' 
	  ,@BronzeCOLUMNListDELTA100   = @BronzeCOLUMNListDELTA100 + ColumnNameDELTA100  + ','
	  ,@BronzeColumnsForHashTGT100 = @BronzeColumnsForHashTGT100    +  ColumnNameDELTAHASH100     + REPLACE(ISNULL(CollationName,''),'SQL_Latin1_General_CP1_CI_AS','COLLATE Latin1_General_100_BIN2_UTF8') + ',' + '''%''' +','

  FROM (
        SELECT 
              Table_Schema                                      as SchemaName 
             ,Table_Name                                        as TableName
             ,Ordinal_Position                                  as ColumnId
             ,Collation_name                                  as CollationName
	         ,Data_Type
	         ,Case when Data_Type =  'smalldatetime' then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '104' + ')' + ' as ' + Column_Name
			       when Data_Type =  'datetime'      then 'CONVERT(datetime,'     + Column_Name + ',' + '127' + ')' + ' as ' + Column_Name
			       else Column_Name
		       End as ColumnNameSTAGE100
             ,Case when Data_Type = 'smalldatetime' then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '104' + ')'
			       when Data_Type = 'datetime'      then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '127' + ')'
			       else Column_Name
		       End as ColumnName100
	         ,'[' + Column_Name + ']' + ' as '  +  '[' + 'DELTA' + Column_Name  + ']' as ColumnNameDELTA100
	         ,'[' + Column_Name + ']'   as ColumnNameDELTAHASH100
	  
          FROM EclipseBronze.INFORMATION_SCHEMA.COLUMNS
         WHERE Table_Schema = 'dbo' and Table_Name = 'Policy' 
           AND Ordinal_Position <= 100
           AND Column_Name NOT IN ('RowNumber','EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID')
	    ) T1

print 'STG 100 Columns      = ' + @BronzeCOLUMNListSTAGE100   
print 'STG 100 Columns Hash = ' + @BronzeColumnsForHashSTG100
print 'DELTA 100 Columns = '    + @BronzeCOLUMNListDELTA100 
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
	         ,Case when Data_Type =  'smalldatetime' then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '104' + ')' + ' as ' + Column_Name
			       when Data_Type =  'datetime'      then 'CONVERT(datetime,'     + Column_Name + ',' + '127' + ')' + ' as ' + Column_Name
			       else Column_Name
		       End as ColumnNameSTAGE200
             ,Case when Data_Type = 'smalldatetime' then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '104' + ')'
			       when Data_Type = 'datetime'      then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '127' + ')'
			       else Column_Name
		       End as ColumnName200
	         ,'['+Column_Name +']' + ' as '  +  '[' + 'DELTA' + Column_Name +']' as ColumnNameDELTA200
	         ,'['+Column_Name+']'   as ColumnNameDELTAHASH200
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
	         ,Case when Data_Type =  'smalldatetime' then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '104' + ')' + ' as ' + Column_Name
			       when Data_Type =  'datetime'      then 'CONVERT(datetime,'     + Column_Name + ',' + '127' + ')' + ' as ' + Column_Name
			       else Column_Name
		       End as ColumnNameSTAGE300
             ,Case when Data_Type = 'smalldatetime' then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '104' + ')'
			       when Data_Type = 'datetime'      then 'CONVERT(smalldatetime,'+ Column_Name + ',' + '127' + ')'
			       else Column_Name
		       End as ColumnName300
	         ,'[' + Column_Name + ']' + ' as '  +  '[' + 'DELTA' + Column_Name +']' as ColumnNameDELTA300
	         ,'[' + Column_Name+ ']'   as ColumnNameDELTAHASH300
         FROM EclipseBronze.INFORMATION_SCHEMA.COLUMNS
        WHERE Table_Schema = 'dbo' and Table_Name = 'Policy' 
          AND Ordinal_Position > 200 
          AND Column_Name NOT IN ('RowNumber','EffectiveDateUTC','EndDateUTC','Current','BronzeStagingSystemLoadID','BronzeSystemLoadID')
	   ) T1

print 'STG 300 Columns      = ' + @BronzeCOLUMNListSTAGE300   
print 'STG 300 Columns Hash = ' + @BronzeColumnsForHashSTG300
print 'DELTA 300 Columns = ' + @BronzeCOLUMNListDELTA300 
print 'TGT 300 Columns Hash = ' + @BronzeColumnsForHashTGT300


/*
use EclipseBronze

SELECT 
     CONVERT(smalldatetime,SettDueDate,104) as SettDueDate,CONVERT(smalldatetime,QuoteExpiryDate,104) as QuoteExpiryDate,CONVERT(smalldatetime,QuoteInceptionDate,104) as QuoteInceptionDate,BureauSettledInd,VenueCity,LORSCode,NoticeToBrokerInsured,CONVERT(smalldatetime,NoticeToBrokerInsuredDate,104) as NoticeToBrokerInsuredDate,AssuredSignOff,CONVERT(smalldatetime,AssuredSignOffDate,104) as AssuredSignOffDate,TerrorismAPCcy,TerrorismAPAmount,TerrorismAPReceived,CONVERT(smalldatetime,TerrorismAPReceivedDate,104) as TerrorismAPReceivedDate,ZoneA,ZoneB,ZoneC,ZoneD,BusinessInterruption,ManyFacInd,EventPeriod,CONVERT(smalldatetime,EventPeriodInception,104) as EventPeriodInception,CONVERT(smalldatetime,EventPeriodExpiry,104) as EventPeriodExpiry,CONVERT(smalldatetime,PremiumSwitchDate,104) as PremiumSwitchDate,PremiumSwitchedInd,CONVERT(smalldatetime,RetroDate,104) as RetroDate,MinimumRate,MaximumRate,LossesLoaded,QuotedROL,CONVERT(smalldatetime,RetroactiveDate,104) as RetroactiveDate,OldStatsRef,Description,ExternalRef,OrigPRI,OrigCI,UpdPRI,UpdCI,PRIInd,ConfiscationInd,OutwardPolicyInd,CONVERT(smalldatetime,PeerReviewDate,104) as PeerReviewDate,PeerReviewInd,ContractCertainty,ContractCertaintyExcpt,CONVERT(smalldatetime,ContractCertaintyDate,104) as ContractCertaintyDate,AggRiskData,ExternalRef2,BUAAInd,PRILossRatioOrig,PRILossRatioUpd,PRINewOldPremOrig,PRINewOldPremUpd,DeficitClause,TruePrice,AdditionalLines,AnticipatedLossRatio,RiskTransfer,LeadInd,ManualMatchingInd,ProcessingType,Indexed,CONVERT(smalldatetime,CommutedDate,104) as CommutedDate,CONVERT(smalldatetime,CommutationOptDate,104) as CommutationOptDate,VerificationStatus,CashLossLimit,ClaimsAgreement,TradeCode,Market,BrokerSettledInd,ScheduleTypeId,DealReference,ExternalRef_COMP,ExternalRef_ForUniqueConstraint,DecRef_ForUniqueConstraint,MasterPolicy,ExternallyProvidedInd,PartVIIInd,SYS_CHANGE_OPERATION
	 ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT(CONVERT(smalldatetime,SettDueDate,104),'%',CONVERT(smalldatetime,QuoteExpiryDate,104),'%',CONVERT(smalldatetime,QuoteInceptionDate,104),'%',BureauSettledInd,'%',VenueCity,'%',LORSCode,'%',NoticeToBrokerInsured,'%',CONVERT(smalldatetime,NoticeToBrokerInsuredDate,104),'%',AssuredSignOff,'%',CONVERT(smalldatetime,AssuredSignOffDate,104),'%',TerrorismAPCcy,'%',TerrorismAPAmount,'%',TerrorismAPReceived,'%',CONVERT(smalldatetime,TerrorismAPReceivedDate,104),'%',ZoneA,'%',ZoneB,'%',ZoneC,'%',ZoneD,'%',BusinessInterruption,'%',ManyFacInd,'%',EventPeriod,'%',CONVERT(smalldatetime,EventPeriodInception,104),'%',CONVERT(smalldatetime,EventPeriodExpiry,104),'%',CONVERT(smalldatetime,PremiumSwitchDate,104),'%',PremiumSwitchedInd,'%',CONVERT(smalldatetime,RetroDate,104),'%',MinimumRate,'%',MaximumRate,'%',LossesLoaded,'%',QuotedROL,'%',CONVERT(smalldatetime,RetroactiveDate,104),'%',OldStatsRef,'%',Description,'%',ExternalRef,'%',OrigPRI,'%',OrigCI,'%',UpdPRI,'%',UpdCI,'%',PRIInd,'%',ConfiscationInd,'%',OutwardPolicyInd,'%',CONVERT(smalldatetime,PeerReviewDate,104),'%',PeerReviewInd,'%',ContractCertainty,'%',ContractCertaintyExcpt,'%',CONVERT(smalldatetime,ContractCertaintyDate,104),'%',AggRiskData,'%',ExternalRef2,'%',BUAAInd,'%',PRILossRatioOrig,'%',PRILossRatioUpd,'%',PRINewOldPremOrig,'%',PRINewOldPremUpd,'%',DeficitClause,'%',TruePrice,'%',AdditionalLines,'%',AnticipatedLossRatio,'%',RiskTransfer,'%',LeadInd,'%',ManualMatchingInd,'%',ProcessingType,'%',Indexed,'%',CONVERT(smalldatetime,CommutedDate,104),'%',CONVERT(smalldatetime,CommutationOptDate,104),'%',VerificationStatus,'%',CashLossLimit,'%',ClaimsAgreement,'%',TradeCode,'%',Market,'%',BrokerSettledInd,'%',ScheduleTypeId,'%',DealReference,'%',ExternalRef_COMP,'%',ExternalRef_ForUniqueConstraint,'%',DecRef_ForUniqueConstraint,'%',MasterPolicy,'%',ExternallyProvidedInd,'%',PartVIIInd,'%',SYS_CHANGE_OPERATION))) AS HB300STG
  FROM OPENROWSET ( 
      BULK 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_Policy/SystemLoadID=1022022101001/**',
      FORMAT = 'parquet'
     ) as RS
WHERE PolicyId = 683602	 
	 
SELECT	 
     [SettDueDate] as [DELTASettDueDate],[QuoteExpiryDate] as [DELTAQuoteExpiryDate],[QuoteInceptionDate] as [DELTAQuoteInceptionDate],[BureauSettledInd] as [DELTABureauSettledInd],[VenueCity] as [DELTAVenueCity],[LORSCode] as [DELTALORSCode],[NoticeToBrokerInsured] as [DELTANoticeToBrokerInsured],[NoticeToBrokerInsuredDate] as [DELTANoticeToBrokerInsuredDate],[AssuredSignOff] as [DELTAAssuredSignOff],[AssuredSignOffDate] as [DELTAAssuredSignOffDate],[TerrorismAPCcy] as [DELTATerrorismAPCcy],[TerrorismAPAmount] as [DELTATerrorismAPAmount],[TerrorismAPReceived] as [DELTATerrorismAPReceived],[TerrorismAPReceivedDate] as [DELTATerrorismAPReceivedDate],[ZoneA] as [DELTAZoneA],[ZoneB] as [DELTAZoneB],[ZoneC] as [DELTAZoneC],[ZoneD] as [DELTAZoneD],[BusinessInterruption] as [DELTABusinessInterruption],[ManyFacInd] as [DELTAManyFacInd],[EventPeriod] as [DELTAEventPeriod],[EventPeriodInception] as [DELTAEventPeriodInception],[EventPeriodExpiry] as [DELTAEventPeriodExpiry],[PremiumSwitchDate] as [DELTAPremiumSwitchDate],[PremiumSwitchedInd] as [DELTAPremiumSwitchedInd],[RetroDate] as [DELTARetroDate],[MinimumRate] as [DELTAMinimumRate],[MaximumRate] as [DELTAMaximumRate],[LossesLoaded] as [DELTALossesLoaded],[QuotedROL] as [DELTAQuotedROL],[RetroactiveDate] as [DELTARetroactiveDate],[OldStatsRef] as [DELTAOldStatsRef],[Description] as [DELTADescription],[ExternalRef] as [DELTAExternalRef],[OrigPRI] as [DELTAOrigPRI],[OrigCI] as [DELTAOrigCI],[UpdPRI] as [DELTAUpdPRI],[UpdCI] as [DELTAUpdCI],[PRIInd] as [DELTAPRIInd],[ConfiscationInd] as [DELTAConfiscationInd],[OutwardPolicyInd] as [DELTAOutwardPolicyInd],[PeerReviewDate] as [DELTAPeerReviewDate],[PeerReviewInd] as [DELTAPeerReviewInd],[ContractCertainty] as [DELTAContractCertainty],[ContractCertaintyExcpt] as [DELTAContractCertaintyExcpt],[ContractCertaintyDate] as [DELTAContractCertaintyDate],[AggRiskData] as [DELTAAggRiskData],[ExternalRef2] as [DELTAExternalRef2],[BUAAInd] as [DELTABUAAInd],[PRILossRatioOrig] as [DELTAPRILossRatioOrig],[PRILossRatioUpd] as [DELTAPRILossRatioUpd],[PRINewOldPremOrig] as [DELTAPRINewOldPremOrig],[PRINewOldPremUpd] as [DELTAPRINewOldPremUpd],[DeficitClause] as [DELTADeficitClause],[TruePrice] as [DELTATruePrice],[AdditionalLines] as [DELTAAdditionalLines],[AnticipatedLossRatio] as [DELTAAnticipatedLossRatio],[RiskTransfer] as [DELTARiskTransfer],[LeadInd] as [DELTALeadInd],[ManualMatchingInd] as [DELTAManualMatchingInd],[ProcessingType] as [DELTAProcessingType],[Indexed] as [DELTAIndexed],[CommutedDate] as [DELTACommutedDate],[CommutationOptDate] as [DELTACommutationOptDate],[VerificationStatus] as [DELTAVerificationStatus],[CashLossLimit] as [DELTACashLossLimit],[ClaimsAgreement] as [DELTAClaimsAgreement],[TradeCode] as [DELTATradeCode],[Market] as [DELTAMarket],[BrokerSettledInd] as [DELTABrokerSettledInd],[ScheduleTypeId] as [DELTAScheduleTypeId],[DealReference] as [DELTADealReference],[ExternalRef_COMP] as [DELTAExternalRef_COMP],[ExternalRef_ForUniqueConstraint] as [DELTAExternalRef_ForUniqueConstraint],[DecRef_ForUniqueConstraint] as [DELTADecRef_ForUniqueConstraint],[MasterPolicy] as [DELTAMasterPolicy],[ExternallyProvidedInd] as [DELTAExternallyProvidedInd],[PartVIIInd] as [DELTAPartVIIInd],[SYS_CHANGE_OPERATION] as [DELTASYS_CHANGE_OPERATION]
	 ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([SettDueDate],'%',[QuoteExpiryDate],'%',[QuoteInceptionDate],'%',[BureauSettledInd],'%',[VenueCity]COLLATE Latin1_General_100_BIN2_UTF8,'%',[LORSCode]COLLATE Latin1_General_100_BIN2_UTF8,'%',[NoticeToBrokerInsured]COLLATE Latin1_General_100_BIN2_UTF8,'%',[NoticeToBrokerInsuredDate],'%',[AssuredSignOff]COLLATE Latin1_General_100_BIN2_UTF8,'%',[AssuredSignOffDate],'%',[TerrorismAPCcy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[TerrorismAPAmount],'%',[TerrorismAPReceived]COLLATE Latin1_General_100_BIN2_UTF8,'%',[TerrorismAPReceivedDate],'%',[ZoneA],'%',[ZoneB],'%',[ZoneC],'%',[ZoneD],'%',[BusinessInterruption],'%',[ManyFacInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[EventPeriod]COLLATE Latin1_General_100_BIN2_UTF8,'%',[EventPeriodInception],'%',[EventPeriodExpiry],'%',[PremiumSwitchDate],'%',[PremiumSwitchedInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[RetroDate],'%',[MinimumRate],'%',[MaximumRate],'%',[LossesLoaded],'%',[QuotedROL],'%',[RetroactiveDate],'%',[OldStatsRef]COLLATE Latin1_General_100_BIN2_UTF8,'%',[Description]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ExternalRef]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OrigPRI],'%',[OrigCI],'%',[UpdPRI],'%',[UpdCI],'%',[PRIInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ConfiscationInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[OutwardPolicyInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[PeerReviewDate],'%',[PeerReviewInd],'%',[ContractCertainty]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ContractCertaintyExcpt]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ContractCertaintyDate],'%',[AggRiskData]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ExternalRef2]COLLATE Latin1_General_100_BIN2_UTF8,'%',[BUAAInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[PRILossRatioOrig],'%',[PRILossRatioUpd],'%',[PRINewOldPremOrig],'%',[PRINewOldPremUpd],'%',[DeficitClause]COLLATE Latin1_General_100_BIN2_UTF8,'%',[TruePrice],'%',[AdditionalLines]COLLATE Latin1_General_100_BIN2_UTF8,'%',[AnticipatedLossRatio],'%',[RiskTransfer]COLLATE Latin1_General_100_BIN2_UTF8,'%',[LeadInd],'%',[ManualMatchingInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ProcessingType]COLLATE Latin1_General_100_BIN2_UTF8,'%',[Indexed]COLLATE Latin1_General_100_BIN2_UTF8,'%',[CommutedDate],'%',[CommutationOptDate],'%',[VerificationStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[CashLossLimit],'%',[ClaimsAgreement]COLLATE Latin1_General_100_BIN2_UTF8,'%',[TradeCode]COLLATE Latin1_General_100_BIN2_UTF8,'%',[Market]COLLATE Latin1_General_100_BIN2_UTF8,'%',[BrokerSettledInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ScheduleTypeId],'%',[DealReference]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ExternalRef_COMP]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ExternalRef_ForUniqueConstraint]COLLATE Latin1_General_100_BIN2_UTF8,'%',[DecRef_ForUniqueConstraint]COLLATE Latin1_General_100_BIN2_UTF8,'%',[MasterPolicy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ExternallyProvidedInd],'%',[PartVIIInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8))) as HB300TGT
  FROM [EclipseBronze].[dbo].[Policy]
 WHERE PolicyId = 683602


STG 200 Columns      = LossOfUseDays,CONVERT(smalldatetime,InsDate,104) as InsDate,CONVERT(smalldatetime,DelDate,104) as DelDate,OtherDeductionsApplyInd,ProfitCommApplyInd,PolicyType,BenefitsType,NoOfLives,Grantee,CONVERT(smalldatetime,ProfitCommAdj,104) as ProfitCommAdj,PolicyStatus,ParentPolicyId,CancBasis,RemindFreq,CONVERT(smalldatetime,RemindDate,104) as RemindDate,RemindMess,RemindEmail,TerritoryIdAssured,BestCode,NAICCode,Producer,Class1,Class2,Class3,GeogCode,TotalSumInsured,CalcFromWritten,BrokerageDeductionsInd,MaxLimit,MinXS,RateFixedInd,TopLimitOrigCcyISO,TopLimitOrigAmount,TopLimitSettAmount,TopLimitConsAmount,TopXSOrigAmount,TopXSSettAmount,TopXSConsAmount,ConsCcyROE,TopLimitSettCcyISO,FILCode,UserName,AggLimit,PaymentMethod,PaymentType,AdjFreq,AdjPcnt,AdjDsc,CONVERT(smalldatetime,AdjDate,104) as AdjDate,CONVERT(smalldatetime,DWAgreed,104) as DWAgreed,ClaimFundInd,NeedReCalcInd,ProfitCommAgreed,Class4,FrontingInd,FinanceUWInd,NOCInd,FullTermInd,EndorsemntInd,FacReinsInd,MonitorPIMInd,CONVERT(smalldatetime,DateEntered,104) as DateEntered,UpdBy,MainLayerInd,QuoteDurationDays,UltimateLossRatio,AccountsPayable,BenchMarkRating,InstalmentPeriod,InitialTOT,DeferredTOT,FinalTOT,ConfidentialNotes,LOCInd,CONVERT(smalldatetime,ReviewDate,104) as ReviewDate,EventName,EventMultiple,OutdoorEventInd,LTPInd,TerrorMiles,TerrorDays,ZipCode,SurveyFees,VetsFees,ConsultancyFees,ExpertFees,AnticipatedLR,PremWarrantyInd,CONVERT(smalldatetime,WordingDate,104) as WordingDate,WordingNote,NettLineInd,ReinstBrokerage,LossWarrantyTriggers,LossReviewPct,WarInd,StrikesInd,ExpensesInd,SurveyInd,CONVERT(smalldatetime,SurveyDate,104) as SurveyDate,NumOfInstalment,
STG 200 Columns Hash = LossOfUseDays,'%',CONVERT(smalldatetime,InsDate,104),'%',CONVERT(smalldatetime,DelDate,104),'%',OtherDeductionsApplyInd,'%',ProfitCommApplyInd,'%',PolicyType,'%',BenefitsType,'%',NoOfLives,'%',Grantee,'%',CONVERT(smalldatetime,ProfitCommAdj,104),'%',PolicyStatus,'%',ParentPolicyId,'%',CancBasis,'%',RemindFreq,'%',CONVERT(smalldatetime,RemindDate,104),'%',RemindMess,'%',RemindEmail,'%',TerritoryIdAssured,'%',BestCode,'%',NAICCode,'%',Producer,'%',Class1,'%',Class2,'%',Class3,'%',GeogCode,'%',TotalSumInsured,'%',CalcFromWritten,'%',BrokerageDeductionsInd,'%',MaxLimit,'%',MinXS,'%',RateFixedInd,'%',TopLimitOrigCcyISO,'%',TopLimitOrigAmount,'%',TopLimitSettAmount,'%',TopLimitConsAmount,'%',TopXSOrigAmount,'%',TopXSSettAmount,'%',TopXSConsAmount,'%',ConsCcyROE,'%',TopLimitSettCcyISO,'%',FILCode,'%',UserName,'%',AggLimit,'%',PaymentMethod,'%',PaymentType,'%',AdjFreq,'%',AdjPcnt,'%',AdjDsc,'%',CONVERT(smalldatetime,AdjDate,104),'%',CONVERT(smalldatetime,DWAgreed,104),'%',ClaimFundInd,'%',NeedReCalcInd,'%',ProfitCommAgreed,'%',Class4,'%',FrontingInd,'%',FinanceUWInd,'%',NOCInd,'%',FullTermInd,'%',EndorsemntInd,'%',FacReinsInd,'%',MonitorPIMInd,'%',CONVERT(smalldatetime,DateEntered,104),'%',UpdBy,'%',MainLayerInd,'%',QuoteDurationDays,'%',UltimateLossRatio,'%',AccountsPayable,'%',BenchMarkRating,'%',InstalmentPeriod,'%',InitialTOT,'%',DeferredTOT,'%',FinalTOT,'%',ConfidentialNotes,'%',LOCInd,'%',CONVERT(smalldatetime,ReviewDate,104),'%',EventName,'%',EventMultiple,'%',OutdoorEventInd,'%',LTPInd,'%',TerrorMiles,'%',TerrorDays,'%',ZipCode,'%',SurveyFees,'%',VetsFees,'%',ConsultancyFees,'%',ExpertFees,'%',AnticipatedLR,'%',PremWarrantyInd,'%',CONVERT(smalldatetime,WordingDate,104),'%',WordingNote,'%',NettLineInd,'%',ReinstBrokerage,'%',LossWarrantyTriggers,'%',LossReviewPct,'%',WarInd,'%',StrikesInd,'%',ExpensesInd,'%',SurveyInd,'%',CONVERT(smalldatetime,SurveyDate,104),'%',NumOfInstalment,'%',
DELTA 200 Columns = [LossOfUseDays] as [DELTALossOfUseDays],[InsDate] as [DELTAInsDate],[DelDate] as [DELTADelDate],[OtherDeductionsApplyInd] as [DELTAOtherDeductionsApplyInd],[ProfitCommApplyInd] as [DELTAProfitCommApplyInd],[PolicyType] as [DELTAPolicyType],[BenefitsType] as [DELTABenefitsType],[NoOfLives] as [DELTANoOfLives],[Grantee] as [DELTAGrantee],[ProfitCommAdj] as [DELTAProfitCommAdj],[PolicyStatus] as [DELTAPolicyStatus],[ParentPolicyId] as [DELTAParentPolicyId],[CancBasis] as [DELTACancBasis],[RemindFreq] as [DELTARemindFreq],[RemindDate] as [DELTARemindDate],[RemindMess] as [DELTARemindMess],[RemindEmail] as [DELTARemindEmail],[TerritoryIdAssured] as [DELTATerritoryIdAssured],[BestCode] as [DELTABestCode],[NAICCode] as [DELTANAICCode],[Producer] as [DELTAProducer],[Class1] as [DELTAClass1],[Class2] as [DELTAClass2],[Class3] as [DELTAClass3],[GeogCode] as [DELTAGeogCode],[TotalSumInsured] as [DELTATotalSumInsured],[CalcFromWritten] as [DELTACalcFromWritten],[BrokerageDeductionsInd] as [DELTABrokerageDeductionsInd],[MaxLimit] as [DELTAMaxLimit],[MinXS] as [DELTAMinXS],[RateFixedInd] as [DELTARateFixedInd],[TopLimitOrigCcyISO] as [DELTATopLimitOrigCcyISO],[TopLimitOrigAmount] as [DELTATopLimitOrigAmount],[TopLimitSettAmount] as [DELTATopLimitSettAmount],[TopLimitConsAmount] as [DELTATopLimitConsAmount],[TopXSOrigAmount] as [DELTATopXSOrigAmount],[TopXSSettAmount] as [DELTATopXSSettAmount],[TopXSConsAmount] as [DELTATopXSConsAmount],[ConsCcyROE] as [DELTAConsCcyROE],[TopLimitSettCcyISO] as [DELTATopLimitSettCcyISO],[FILCode] as [DELTAFILCode],[UserName] as [DELTAUserName],[AggLimit] as [DELTAAggLimit],[PaymentMethod] as [DELTAPaymentMethod],[PaymentType] as [DELTAPaymentType],[AdjFreq] as [DELTAAdjFreq],[AdjPcnt] as [DELTAAdjPcnt],[AdjDsc] as [DELTAAdjDsc],[AdjDate] as [DELTAAdjDate],[DWAgreed] as [DELTADWAgreed],[ClaimFundInd] as [DELTAClaimFundInd],[NeedReCalcInd] as [DELTANeedReCalcInd],[ProfitCommAgreed] as [DELTAProfitCommAgreed],[Class4] as [DELTAClass4],[FrontingInd] as [DELTAFrontingInd],[FinanceUWInd] as [DELTAFinanceUWInd],[NOCInd] as [DELTANOCInd],[FullTermInd] as [DELTAFullTermInd],[EndorsemntInd] as [DELTAEndorsemntInd],[FacReinsInd] as [DELTAFacReinsInd],[MonitorPIMInd] as [DELTAMonitorPIMInd],[DateEntered] as [DELTADateEntered],[UpdBy] as [DELTAUpdBy],[MainLayerInd] as [DELTAMainLayerInd],[QuoteDurationDays] as [DELTAQuoteDurationDays],[UltimateLossRatio] as [DELTAUltimateLossRatio],[AccountsPayable] as [DELTAAccountsPayable],[BenchMarkRating] as [DELTABenchMarkRating],[InstalmentPeriod] as [DELTAInstalmentPeriod],[InitialTOT] as [DELTAInitialTOT],[DeferredTOT] as [DELTADeferredTOT],[FinalTOT] as [DELTAFinalTOT],[ConfidentialNotes] as [DELTAConfidentialNotes],[LOCInd] as [DELTALOCInd],[ReviewDate] as [DELTAReviewDate],[EventName] as [DELTAEventName],[EventMultiple] as [DELTAEventMultiple],[OutdoorEventInd] as [DELTAOutdoorEventInd],[LTPInd] as [DELTALTPInd],[TerrorMiles] as [DELTATerrorMiles],[TerrorDays] as [DELTATerrorDays],[ZipCode] as [DELTAZipCode],[SurveyFees] as [DELTASurveyFees],[VetsFees] as [DELTAVetsFees],[ConsultancyFees] as [DELTAConsultancyFees],[ExpertFees] as [DELTAExpertFees],[AnticipatedLR] as [DELTAAnticipatedLR],[PremWarrantyInd] as [DELTAPremWarrantyInd],[WordingDate] as [DELTAWordingDate],[WordingNote] as [DELTAWordingNote],[NettLineInd] as [DELTANettLineInd],[ReinstBrokerage] as [DELTAReinstBrokerage],[LossWarrantyTriggers] as [DELTALossWarrantyTriggers],[LossReviewPct] as [DELTALossReviewPct],[WarInd] as [DELTAWarInd],[StrikesInd] as [DELTAStrikesInd],[ExpensesInd] as [DELTAExpensesInd],[SurveyInd] as [DELTASurveyInd],[SurveyDate] as [DELTASurveyDate],[NumOfInstalment] as [DELTANumOfInstalment],
TGT 200 Columns Hash = [LossOfUseDays],'%',[InsDate],'%',[DelDate],'%',[OtherDeductionsApplyInd],'%',[ProfitCommApplyInd],'%',[PolicyType]COLLATE Latin1_General_100_BIN2_UTF8,'%',[BenefitsType]COLLATE Latin1_General_100_BIN2_UTF8,'%',[NoOfLives],'%',[Grantee]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ProfitCommAdj],'%',[PolicyStatus]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ParentPolicyId],'%',[CancBasis]COLLATE Latin1_General_100_BIN2_UTF8,'%',[RemindFreq]COLLATE Latin1_General_100_BIN2_UTF8,'%',[RemindDate],'%',[RemindMess]COLLATE Latin1_General_100_BIN2_UTF8,'%',[RemindEmail]COLLATE Latin1_General_100_BIN2_UTF8,'%',[TerritoryIdAssured],'%',[BestCode]COLLATE Latin1_General_100_BIN2_UTF8,'%',[NAICCode]COLLATE Latin1_General_100_BIN2_UTF8,'%',[Producer]COLLATE Latin1_General_100_BIN2_UTF8,'%',[Class1]COLLATE Latin1_General_100_BIN2_UTF8,'%',[Class2]COLLATE Latin1_General_100_BIN2_UTF8,'%',[Class3]COLLATE Latin1_General_100_BIN2_UTF8,'%',[GeogCode]COLLATE Latin1_General_100_BIN2_UTF8,'%',[TotalSumInsured],'%',[CalcFromWritten],'%',[BrokerageDeductionsInd],'%',[MaxLimit],'%',[MinXS],'%',[RateFixedInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[TopLimitOrigCcyISO]COLLATE Latin1_General_100_BIN2_UTF8,'%',[TopLimitOrigAmount],'%',[TopLimitSettAmount],'%',[TopLimitConsAmount],'%',[TopXSOrigAmount],'%',[TopXSSettAmount],'%',[TopXSConsAmount],'%',[ConsCcyROE],'%',[TopLimitSettCcyISO]COLLATE Latin1_General_100_BIN2_UTF8,'%',[FILCode]COLLATE Latin1_General_100_BIN2_UTF8,'%',[UserName]COLLATE Latin1_General_100_BIN2_UTF8,'%',[AggLimit],'%',[PaymentMethod]COLLATE Latin1_General_100_BIN2_UTF8,'%',[PaymentType]COLLATE Latin1_General_100_BIN2_UTF8,'%',[AdjFreq]COLLATE Latin1_General_100_BIN2_UTF8,'%',[AdjPcnt],'%',[AdjDsc]COLLATE Latin1_General_100_BIN2_UTF8,'%',[AdjDate],'%',[DWAgreed],'%',[ClaimFundInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[NeedReCalcInd],'%',[ProfitCommAgreed]COLLATE Latin1_General_100_BIN2_UTF8,'%',[Class4]COLLATE Latin1_General_100_BIN2_UTF8,'%',[FrontingInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[FinanceUWInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[NOCInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[FullTermInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[EndorsemntInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[FacReinsInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[MonitorPIMInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[DateEntered],'%',[UpdBy]COLLATE Latin1_General_100_BIN2_UTF8,'%',[MainLayerInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[QuoteDurationDays],'%',[UltimateLossRatio],'%',[AccountsPayable]COLLATE Latin1_General_100_BIN2_UTF8,'%',[BenchMarkRating],'%',[InstalmentPeriod]COLLATE Latin1_General_100_BIN2_UTF8,'%',[InitialTOT],'%',[DeferredTOT],'%',[FinalTOT],'%',[ConfidentialNotes]COLLATE Latin1_General_100_BIN2_UTF8,'%',[LOCInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ReviewDate],'%',[EventName]COLLATE Latin1_General_100_BIN2_UTF8,'%',[EventMultiple],'%',[OutdoorEventInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[LTPInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[TerrorMiles],'%',[TerrorDays],'%',[ZipCode]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SurveyFees],'%',[VetsFees],'%',[ConsultancyFees],'%',[ExpertFees],'%',[AnticipatedLR],'%',[PremWarrantyInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[WordingDate],'%',[WordingNote]COLLATE Latin1_General_100_BIN2_UTF8,'%',[NettLineInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ReinstBrokerage]COLLATE Latin1_General_100_BIN2_UTF8,'%',[LossWarrantyTriggers]COLLATE Latin1_General_100_BIN2_UTF8,'%',[LossReviewPct],'%',[WarInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[StrikesInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ExpensesInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SurveyInd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SurveyDate],'%',[NumOfInstalment],'%',


HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT(LossOfUseDays,'%',CONVERT(smalldatetime,InsDate,104),'%',CONVERT(smalldatetime,DelDate,104),'%',OtherDeductionsApplyInd,'%',ProfitCommApplyInd,'%',PolicyType,'%',BenefitsType,'%',NoOfLives,'%',Grantee,'%',CONVERT(smalldatetime,ProfitCommAdj,104),'%',PolicyStatus,'%',ParentPolicyId,'%',CancBasis,'%',RemindFreq,'%',CONVERT(smalldatetime,RemindDate,104),'%',RemindMess,'%',RemindEmail,'%',TerritoryIdAssured,'%',BestCode,'%',NAICCode,'%',Producer,'%',Class1,'%',Class2,'%',Class3,'%',GeogCode,'%',TotalSumInsured,'%',CalcFromWritten,'%',BrokerageDeductionsInd,'%',MaxLimit,'%',MinXS,'%',RateFixedInd,'%',TopLimitOrigCcyISO,'%',TopLimitOrigAmount,'%',TopLimitSettAmount,'%',TopLimitConsAmount,'%',TopXSOrigAmount,'%',TopXSSettAmount,'%',TopXSConsAmount,'%',ConsCcyROE,'%',TopLimitSettCcyISO,'%',FILCode,'%',UserName,'%',AggLimit,'%',PaymentMethod,'%',PaymentType,'%',AdjFreq,'%',AdjPcnt,'%',AdjDsc,'%',CONVERT(smalldatetime,AdjDate,104),'%',CONVERT(smalldatetime,DWAgreed,104),'%',ClaimFundInd,'%',NeedReCalcInd,'%',ProfitCommAgreed,'%',Class4,'%',FrontingInd,'%',FinanceUWInd,'%',NOCInd,'%',FullTermInd,'%',EndorsemntInd,'%',FacReinsInd,'%',MonitorPIMInd,'%',CONVERT(smalldatetime,DateEntered,104),'%',UpdBy,'%',MainLayerInd,'%',QuoteDurationDays,'%',UltimateLossRatio,'%',AccountsPayable,'%',BenchMarkRating,'%',InstalmentPeriod,'%',InitialTOT,'%',DeferredTOT,'%',FinalTOT,'%',ConfidentialNotes,'%',LOCInd,'%',CONVERT(smalldatetime,ReviewDate,104),'%',EventName,'%',EventMultiple,'%',OutdoorEventInd,'%',LTPInd,'%',TerrorMiles,'%',TerrorDays,'%',ZipCode,'%',SurveyFees,'%',VetsFees,'%',ConsultancyFees,'%',ExpertFees,'%',AnticipatedLR,'%',PremWarrantyInd,'%',CONVERT(smalldatetime,WordingDate,104),'%',WordingNote,'%',NettLineInd,'%',ReinstBrokerage,'%',LossWarrantyTriggers,'%',LossReviewPct,'%',WarInd,'%',StrikesInd,'%',ExpensesInd,'%',SurveyInd,'%',CONVERT(smalldatetime,SurveyDate,104),'%',NumOfInstalment)))
*/