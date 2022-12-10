--USE [QAData];

/********************************************************************************************************************************************
--------------------------------------------------------------------------------------------------------------------------------------------- 
   Version  |  Date        | Author              | Description                                                                  
--------------------------------------------------------------------------------------------------------------------------------------------- 

   0.1      |  07/11/2022  | Ram Baskar          | Initial Manual script for Policy entity - Bronze Layer
   0.2      |  08/11/2022  | Ram Baskar          | Amended the script to include Config Data from QAData DB
   0.3      |  09/11/2022  | Ram Baskar          | Amended the script to Parameterise with SystemLoadID
   0.4      |  15/11/2022  | Ram Baskar          | Amended the script to Capture the result as other Bronze entities
   0.5      |  16/11/2022  | Ram Baskar          | Changed to Stored Procedure
   0.6      |  16/11/2022  | Ram Baskar          | Amended the code to handle BIGINT/VARCHAR for SystemLoadID attribute which is a parameter for this script
   0.7      |  16/11/2022  | Ram Baskar          | Amended the code to change the parameter from SystemLoadID to TableName
   0.8      |  16/11/2022  | Ram Baskar          | Amended the code to include additional System columns for reporting
   0.9      |  17/11/2022  | Ram Baskar          | Changed the procedure name to meanigful name
   0.10     |  27/11/2022  | Ram Baskar          | Amended the code to save the result in new folder structure


This script will work ONLY for Policy Entity in Bronze Layer

**********************************************************************************************************************************************************/

CREATE or ALTER Procedure [dbo].[BronzeLayer_Eclipse_Stage_To_Delta_DataValidation_Policy] (@TableName VARCHAR(100))
AS 
BEGIN

DECLARE @SchemaName              VARCHAR(10)   = 'dbo'
	   ,@SystemLoadID			 BIGINT		   = '' --1022022111502
	   --,@TableName				 VARCHAR(100)  = 'Policy'
       ,@OBjectName				 VARCHAR(100)  = 'dbo_Policy'
       ,@QRY				     VARCHAR(MAX)  = ''
       ,@StageFileName			 VARCHAR(MAX)  = '' 
	   ,@DeltaFileName			 VARCHAR(MAX)  = '' 
	   ,@Environment			 VARCHAR(MAX)  = ''
	   ,@DataSource				 VARCHAR(MAX)  = ''
	   ,@StagePath				 VARCHAR(MAX)  = ''
	   ,@DeltaPath				 VARCHAR(MAX)  = ''
	   ,@QADBName				 VARCHAR(MAX)  = 'QAData'
	   ,@Parquet			    NVARCHAR(MAX)  = 'parquet'
	   ,@QRY1	        		NVARCHAR(MAX)  = ''
       ,@QADatabaseName			NVARCHAR(MAX)  = 'QAData'
	   ,@SystemDescription      NVARCHAR(MAX)  = ''
	   ,@EXISTING_TABLE 		NVARCHAR(MAX)  = ''
	   ,@QAFileFormat           NVARCHAR(MAX)  = '[SynapseParquetFormat]'
	   ,@TRFile                 NVARCHAR(MAX)  = ''
	   ,@FileTimeStamp	        NVARCHAR(MAX)  = ''
	   ,@Location               NVARCHAR(MAX)  = ''
       ,@SystemName       	    NVARCHAR(MAX)  = ''
       ,@SourceObjectId   	    NVARCHAR(MAX)  = ''
       ,@SourceObjectName 	    NVARCHAR(MAX)  = ''
       ,@UniqueColumn           NVARCHAR(MAX)  = ''
       ,@ObjectRunId            NVARCHAR(MAX)  = ''
	   ,@EXISTING_External_Table    NVARCHAR(MAX) = ''
	   ,@TestType                   VARCHAR(200) = '(Policy) - Bronze Stage --> Bronze Delta Data Reconciliiation/Data Completeness/Data Correctness'
	   ,@Layer                      VARCHAR(10)  = 'Bronze'	   
	   ,@SysID                  NVARCHAR(MAX)  = ''
	   
	   
--Setting up the Environment 
IF 
    Substring(@@SERVERNAME,8,3) = 'dev'
    SET @Environment = 'Dev'
ELSE IF 
    Substring(@@SERVERNAME,8,3) = 'tst'
    SET @Environment = 'Tst'
ELSE IF 
    Substring(@@SERVERNAME,8,3) = 'uat'
    SET @Environment = 'Uat'
ELSE IF 
    Substring(@@SERVERNAME,8,3) = 'prd'
    SET @Environment = 'Prd'
ELSE 
    SET @Environment = ''

--select * from qadata.dbo.QAControlData

SET @EXISTING_External_Table    = 'TR' + '_' + @Layer + '_' + @TableName 

SET @SystemName        = (SELECT SystemName         FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and CAST(SystemLoadId as VARCHAR(20)) like '102%')
SET @SourceObjectId    = (SELECT SourceObjectId     FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and CAST(SystemLoadId as VARCHAR(20)) like '102%')
SET @SourceObjectName  = (SELECT SourceObjectName   FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and CAST(SystemLoadId as VARCHAR(20)) like '102%')
SET @SystemDescription = (SELECT SystemDescription  FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and CAST(SystemLoadId as VARCHAR(20)) like '102%')
SET @UniqueColumn      = (SELECT UniqueColumn       FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and CAST(SystemLoadId as VARCHAR(20)) like '102%')
SET @ObjectRunId       = (SELECT ObjectRunId        FROM QAData.dbo.QAControlData WHERE SourceObjectName = @SchemaName + '.' + @TableName and CAST(SystemLoadId as VARCHAR(20)) like '102%')
SET @FileTimeStamp     = (SELECT format(GetDate(), 'yyyyMMddHHmmss'))

--SET @Location          = (SELECT 'N' + '''SITResult/' + @Layer + '/' + @TableName +  '/' + @TRFile + '''')

SET @StagePath         = (SELECT StagePath    from QAData.dbo.ConfigData    where Environment =  + @Environment )
SET @DeltaPath         = (SELECT DeltaPath    from QAData.dbo.ConfigData    where Environment =  + @Environment )
SET @DataSource        = (SELECT DataSource   from QAData.dbo.ConfigData    where Environment =  + @Environment )
SET @SystemLoadId      = (SELECT SystemLoadId from QAData.dbo.QAControlData where SourceObjectName = @SchemaName + '.' + @TableName and CAST(SystemLoadId as VARCHAR(20)) like '102%')
SET @TRFile            = (SELECT @TableName + '_' + CAST(@SystemLoadId as VARCHAR(20))+ '_'+ @FileTimeStamp + '.' + @Parquet)
SET @Location          = (SELECT 'N' + '''SITResult/' + @Layer + '/' + @TableName +  '/' + @TRFile + '''')
SET @StageFileName     = @StagePath + @SchemaName + '_' + @TableName + '/SystemLoadID='  + CAST(@SystemLoadID AS VARCHAR(20)) + '/' + @SchemaName + '_' + @TableName + '_'+ CAST(@SystemLoadID AS VARCHAR(20)) + '.parquet'
SET @DeltaFileName     = @DeltaPath + @SchemaName + '_' + @TableName + '/*.parquet'
SET @SysID             = CAST(@SystemLoadId as VARCHAR(20))


SELECT @QRY1 = 'USE ' + @QADatabasename                                                                                               + CHAR(10) + 
              
              'IF EXISTS (SELECT [name] FROM QAData.sys.external_tables WHERE [name] = ' + ''''+ @EXISTING_External_Table +'''' +')'  + CHAR(10) +
			  'BEGIN '                                                                                                                + CHAR(10) + 
			  '      DROP EXTERNAL Table ' + @EXISTING_External_Table                                                                 + CHAR(10) + 
			  '  END;'                                                                                                                + CHAR(10) +

			  'CREATE EXTERNAL TABLE ' + @QADatabaseName + '.' + @SchemaName +'.' + @EXISTING_External_Table                          + CHAR(10) +
              ' WITH ( '                                                                                                              + CHAR(10) +
			  ' DATA_SOURCE =  ' + @DataSource                                                                                        + CHAR(10) +
			  ',LOCATION    =  ' + @Location                                                                                          + CHAR(10) + 
	          ',FILE_FORMAT =  ' + @QAFileFormat + ' ) AS '
      
--print @QRY1	


SET @QRY = @QRY1 + N' 


with CTESTG as (
     SELECT ProgramRef,LayerNum,StatsCcyROE,AnnivDate,DaysNotice,UW,Interest,PolicyId,PolicyRef,EntryType,DecRef,LloydsBrokerId,LloydsBrokerRef,LloydsBrokerContactId,BucketInd,PeriodType,InceptionDate,ExpiryDate,TermsOfTrade,DateWritten,FirstWritten,ClaimInd,Brokerage,Deductions,CancelDate,StatsCcyISO,ConsCcyISO,ConsTotalSyndEPI,ConsTotalSyndExposure,Notes,RenewedFromRef,RenewedToRef,GrossPremIncome,Comm,CommMisc,CoverType,UniqueMarketRef,PlacingType,WordingStatus,PortfolioTransInd,LossOfUseDaily,LastUpd,SectionCode,Period,FullTermPeriod,FullTermInceptionDate,FullTermExpiryDate,FullTermResignInd,ProfitComm,ProfitComm2,CommDeductionsInd,ProfitCommExpense,OtherDeductions,PremFreq,FirstSettDueDate,SettTermOfTrade,AdjTermsOfTrade,CoverMaxPeriod,CommSlideAt,BordereauPremTermsOfTrade,TreatyReinsInd,CoIns,SysLastUpd,NoClaimBonus,EarnPattern,LossOfUseLimit,LossOfUseXs,LossOfUseExposure,LossOfUseCcyISO,LossOfUseXsDsc,CommSlide,CommSlideProv,CommSlideLR,CommSlideMin,BordereauClaimTermsOfTrade,WaitPeriod,SmokerInd,BordereauClaimFreq,Sex,DateOfBirth,QuotaSharePct,BordereauPremFreq,QuotaShareLine,ConvertedInd,IPTPct,IPTPremApplic,RepGroup,SysPolicyType,TotalSyndEPI,TotalSyndExposure,UWDivision,WriteBackPct,LloydsBrokerCode,LloydsBrokerPseud,YOA,PerilGroupId,TerritoryId,TopLimit,TopLimitCcyISO,TopXs,LossOfUseDays,InsDate,DelDate,OtherDeductionsApplyInd,ProfitCommApplyInd,PolicyType,BenefitsType,NoOfLives,Grantee,ProfitCommAdj,PolicyStatus,ParentPolicyId,CancBasis,RemindFreq,RemindDate,RemindMess,RemindEmail,TerritoryIdAssured,BestCode,NAICCode,Producer,Class1,Class2,Class3,GeogCode,TotalSumInsured,CalcFromWritten,BrokerageDeductionsInd,MaxLimit,MinXS,RateFixedInd,TopLimitOrigCcyISO,TopLimitOrigAmount,TopLimitSettAmount,TopLimitConsAmount,TopXSOrigAmount,TopXSSettAmount,TopXSConsAmount,ConsCcyROE,TopLimitSettCcyISO,FILCode,UserName,AggLimit,PaymentMethod,PaymentType,AdjFreq,AdjPcnt,AdjDsc,AdjDate,DWAgreed,ClaimFundInd,NeedReCalcInd,ProfitCommAgreed,Class4,FrontingInd,FinanceUWInd,NOCInd,FullTermInd,EndorsemntInd,FacReinsInd,MonitorPIMInd,DateEntered,UpdBy,MainLayerInd,QuoteDurationDays,UltimateLossRatio,AccountsPayable,BenchMarkRating,InstalmentPeriod,InitialTOT,DeferredTOT,FinalTOT,ConfidentialNotes,LOCInd,ReviewDate,EventName,EventMultiple,OutdoorEventInd,LTPInd,TerrorMiles,TerrorDays,ZipCode,SurveyFees,VetsFees,ConsultancyFees,ExpertFees,AnticipatedLR,PremWarrantyInd,WordingDate,WordingNote,NettLineInd,ReinstBrokerage,LossWarrantyTriggers,LossReviewPct,WarInd,StrikesInd,ExpensesInd,SurveyInd,SurveyDate,NumOfInstalment,SettDueDate,QuoteExpiryDate,QuoteInceptionDate,BureauSettledInd,VenueCity,LORSCode,NoticeToBrokerInsured,NoticeToBrokerInsuredDate,AssuredSignOff,AssuredSignOffDate,TerrorismAPCcy,TerrorismAPAmount,TerrorismAPReceived,TerrorismAPReceivedDate,ZoneA,ZoneB,ZoneC,ZoneD,BusinessInterruption,ManyFacInd,EventPeriod,EventPeriodInception,EventPeriodExpiry,PremiumSwitchDate,PremiumSwitchedInd,RetroDate,MinimumRate,MaximumRate,LossesLoaded,QuotedROL,RetroactiveDate,OldStatsRef,Description,ExternalRef,OrigPRI,OrigCI,UpdPRI,UpdCI,PRIInd,ConfiscationInd,OutwardPolicyInd,PeerReviewDate,PeerReviewInd,ContractCertainty,ContractCertaintyExcpt,ContractCertaintyDate,AggRiskData,ExternalRef2,BUAAInd,PRILossRatioOrig,PRILossRatioUpd,PRINewOldPremOrig,PRINewOldPremUpd,DeficitClause,TruePrice,AdditionalLines,AnticipatedLossRatio,RiskTransfer,LeadInd,ManualMatchingInd,ProcessingType,Indexed,CommutedDate,CommutationOptDate,VerificationStatus,CashLossLimit,ClaimsAgreement,TradeCode,Market,BrokerSettledInd,ScheduleTypeId,DealReference,ExternalRef_COMP,ExternalRef_ForUniqueConstraint,DecRef_ForUniqueConstraint,MasterPolicy,ExternallyProvidedInd,PartVIIInd,SYS_CHANGE_OPERATION     
	       ,HASHBYTES(''SHA1'',CONCAT(ProgramRef,''%'',LayerNum,''%'',StatsCcyROE,''%'',AnnivDate,''%'',DaysNotice,''%'',UW,''%'',Interest,''%'',PolicyId,''%'',PolicyRef,''%'',EntryType,''%'',DecRef,''%'',LloydsBrokerId,''%'',LloydsBrokerRef,''%'',LloydsBrokerContactId,''%'',BucketInd,''%'',PeriodType,''%'',InceptionDate,''%'',ExpiryDate,''%'',TermsOfTrade,''%'',DateWritten,''%'',FirstWritten,''%'',ClaimInd,''%'',Brokerage,''%'',Deductions,''%'',CancelDate,''%'',StatsCcyISO,''%'',ConsCcyISO,''%'',ConsTotalSyndEPI,''%'',ConsTotalSyndExposure,''%'',Notes,''%'',RenewedFromRef,''%'',RenewedToRef,''%'',GrossPremIncome,''%'',Comm,''%'',CommMisc,''%'',CoverType,''%'',UniqueMarketRef,''%'',PlacingType,''%'',WordingStatus,''%'',PortfolioTransInd,''%'',LossOfUseDaily,''%'',LastUpd,''%'',SectionCode,''%'',Period,''%'',FullTermPeriod,''%'',FullTermInceptionDate,''%'',FullTermExpiryDate,''%'',FullTermResignInd,''%'',ProfitComm,''%'',ProfitComm2,''%'',CommDeductionsInd,''%'',ProfitCommExpense,''%'',OtherDeductions,''%'',PremFreq,''%'',FirstSettDueDate,''%'',SettTermOfTrade,''%'',AdjTermsOfTrade,''%'',CoverMaxPeriod,''%'',CommSlideAt,''%'',BordereauPremTermsOfTrade,''%'',TreatyReinsInd,''%'',CoIns,''%'',SysLastUpd,''%'',NoClaimBonus,''%'',EarnPattern,''%'',LossOfUseLimit,''%'',LossOfUseXs,''%'',LossOfUseExposure,''%'',LossOfUseCcyISO,''%'',LossOfUseXsDsc,''%'',CommSlide,''%'',CommSlideProv,''%'',CommSlideLR,''%'',CommSlideMin,''%'',BordereauClaimTermsOfTrade,''%'',WaitPeriod,''%'',SmokerInd,''%'',BordereauClaimFreq,''%'',Sex,''%'',DateOfBirth,''%'',QuotaSharePct,''%'',BordereauPremFreq,''%'',QuotaShareLine,''%'',ConvertedInd,''%'',IPTPct,''%'',IPTPremApplic,''%'',RepGroup,''%'',SysPolicyType,''%'',TotalSyndEPI,''%'',TotalSyndExposure,''%'',UWDivision,''%'',WriteBackPct,''%'',LloydsBrokerCode,''%'',LloydsBrokerPseud,''%'',YOA,''%'',PerilGroupId,''%'',TerritoryId,''%'',TopLimit,''%'',TopLimitCcyISO,''%'',TopXs)) as HB100     
		   ,HASHBYTES(''SHA1'',CONCAT(LossOfUseDays,''%'',InsDate,''%'',DelDate,''%'',OtherDeductionsApplyInd,''%'',ProfitCommApplyInd,''%'',PolicyType,''%'',BenefitsType,''%'',NoOfLives,''%'',Grantee,''%'',ProfitCommAdj,''%'',PolicyStatus,''%'',ParentPolicyId,''%'',CancBasis,''%'',RemindFreq,''%'',RemindDate,''%'',RemindMess,''%'',RemindEmail,''%'',TerritoryIdAssured,''%'',BestCode,''%'',NAICCode,''%'',Producer,''%'',Class1,''%'',Class2,''%'',Class3,''%'',GeogCode,''%'',TotalSumInsured,''%'',CalcFromWritten,''%'',BrokerageDeductionsInd,''%'',MaxLimit,''%'',MinXS,''%'',RateFixedInd,''%'',TopLimitOrigCcyISO,''%'',TopLimitOrigAmount,''%'',TopLimitSettAmount,''%'',TopLimitConsAmount,''%'',TopXSOrigAmount,''%'',TopXSSettAmount,''%'',TopXSConsAmount,''%'',ConsCcyROE,''%'',TopLimitSettCcyISO,''%'',FILCode,''%'',UserName,''%'',AggLimit,''%'',PaymentMethod,''%'',PaymentType,''%'',AdjFreq,''%'',AdjPcnt,''%'',AdjDsc,''%'',AdjDate,''%'',DWAgreed,''%'',ClaimFundInd,''%'',NeedReCalcInd,''%'',ProfitCommAgreed,''%'',Class4,''%'',FrontingInd,''%'',FinanceUWInd,''%'',NOCInd,''%'',FullTermInd,''%'',EndorsemntInd,''%'',FacReinsInd,''%'',MonitorPIMInd,''%'',DateEntered,''%'',UpdBy,''%'',MainLayerInd,''%'',QuoteDurationDays,''%'',UltimateLossRatio,''%'',AccountsPayable,''%'',BenchMarkRating,''%'',InstalmentPeriod,''%'',InitialTOT,''%'',DeferredTOT,''%'',FinalTOT,''%'',ConfidentialNotes,''%'',LOCInd,''%'',ReviewDate,''%'',EventName,''%'',EventMultiple,''%'',OutdoorEventInd,''%'',LTPInd,''%'',TerrorMiles,''%'',TerrorDays,''%'',ZipCode,''%'',SurveyFees,''%'',VetsFees,''%'',ConsultancyFees,''%'',ExpertFees,''%'',AnticipatedLR,''%'',PremWarrantyInd,''%'',WordingDate,''%'',WordingNote,''%'',NettLineInd,''%'',ReinstBrokerage,''%'',LossWarrantyTriggers,''%'',LossReviewPct,''%'',WarInd,''%'',StrikesInd,''%'',ExpensesInd,''%'',SurveyInd,''%'',SurveyDate,''%'',NumOfInstalment)) as HB200     
		   ,HASHBYTES(''SHA1'',CONCAT(SettDueDate,''%'',QuoteExpiryDate,''%'',QuoteInceptionDate,''%'',BureauSettledInd,''%'',VenueCity,''%'',LORSCode,''%'',NoticeToBrokerInsured,''%'',NoticeToBrokerInsuredDate,''%'',AssuredSignOff,''%'',AssuredSignOffDate,''%'',TerrorismAPCcy,''%'',TerrorismAPAmount,''%'',TerrorismAPReceived,''%'',TerrorismAPReceivedDate,''%'',ZoneA,''%'',ZoneB,''%'',ZoneC,''%'',ZoneD,''%'',BusinessInterruption,''%'',ManyFacInd,''%'',EventPeriod,''%'',EventPeriodInception,''%'',EventPeriodExpiry,''%'',PremiumSwitchDate,''%'',PremiumSwitchedInd,''%'',RetroDate,''%'',MinimumRate,''%'',MaximumRate,''%'',LossesLoaded,''%'',QuotedROL,''%'',RetroactiveDate,''%'',OldStatsRef,''%'',Description,''%'',ExternalRef,''%'',OrigPRI,''%'',OrigCI,''%'',UpdPRI,''%'',UpdCI,''%'',PRIInd,''%'',ConfiscationInd,''%'',OutwardPolicyInd,''%'',PeerReviewDate,''%'',PeerReviewInd,''%'',ContractCertainty,''%'',ContractCertaintyExcpt,''%'',ContractCertaintyDate,''%'',AggRiskData,''%'',ExternalRef2,''%'',BUAAInd,''%'',PRILossRatioOrig,''%'',PRILossRatioUpd,''%'',PRINewOldPremOrig,''%'',PRINewOldPremUpd,''%'',DeficitClause,''%'',TruePrice,''%'',AdditionalLines,''%'',AnticipatedLossRatio,''%'',RiskTransfer,''%'',LeadInd,''%'',ManualMatchingInd,''%'',ProcessingType,''%'',Indexed,''%'',CommutedDate,''%'',CommutationOptDate,''%'',VerificationStatus,''%'',CashLossLimit,''%'',ClaimsAgreement,''%'',TradeCode,''%'',Market,''%'',BrokerSettledInd,''%'',ScheduleTypeId,''%'',DealReference,''%'',ExternalRef_COMP,''%'',ExternalRef_ForUniqueConstraint,''%'',DecRef_ForUniqueConstraint,''%'',MasterPolicy,''%'',ExternallyProvidedInd,''%'',PartVIIInd,''%'',SYS_CHANGE_OPERATION)) as HB300
      FROM OPENROWSET( BULK ''' + @StageFileName + ''', FORMAT = ''PARQUET'') AS [STAGEresult]
     --WHERE PolicyId = 1 
	 )

    ,CTETGT as (
     SELECT ProgramRef,LayerNum,StatsCcyROE,AnnivDate,DaysNotice,UW,Interest,PolicyId,PolicyRef,EntryType,DecRef,LloydsBrokerId,LloydsBrokerRef,LloydsBrokerContactId,BucketInd,PeriodType,InceptionDate,ExpiryDate,TermsOfTrade,DateWritten,FirstWritten,ClaimInd,Brokerage,Deductions,CancelDate,StatsCcyISO,ConsCcyISO,ConsTotalSyndEPI,ConsTotalSyndExposure,Notes,RenewedFromRef,RenewedToRef,GrossPremIncome,Comm,CommMisc,CoverType,UniqueMarketRef,PlacingType,WordingStatus,PortfolioTransInd,LossOfUseDaily,LastUpd,SectionCode,Period,FullTermPeriod,FullTermInceptionDate,FullTermExpiryDate,FullTermResignInd,ProfitComm,ProfitComm2,CommDeductionsInd,ProfitCommExpense,OtherDeductions,PremFreq,FirstSettDueDate,SettTermOfTrade,AdjTermsOfTrade,CoverMaxPeriod,CommSlideAt,BordereauPremTermsOfTrade,TreatyReinsInd,CoIns,SysLastUpd,NoClaimBonus,EarnPattern,LossOfUseLimit,LossOfUseXs,LossOfUseExposure,LossOfUseCcyISO,LossOfUseXsDsc,CommSlide,CommSlideProv,CommSlideLR,CommSlideMin,BordereauClaimTermsOfTrade,WaitPeriod,SmokerInd,BordereauClaimFreq,Sex,DateOfBirth,QuotaSharePct,BordereauPremFreq,QuotaShareLine,ConvertedInd,IPTPct,IPTPremApplic,RepGroup,SysPolicyType,TotalSyndEPI,TotalSyndExposure,UWDivision,WriteBackPct,LloydsBrokerCode,LloydsBrokerPseud,YOA,PerilGroupId,TerritoryId,TopLimit,TopLimitCcyISO,TopXs,LossOfUseDays,InsDate,DelDate,OtherDeductionsApplyInd,ProfitCommApplyInd,PolicyType,BenefitsType,NoOfLives,Grantee,ProfitCommAdj,PolicyStatus,ParentPolicyId,CancBasis,RemindFreq,RemindDate,RemindMess,RemindEmail,TerritoryIdAssured,BestCode,NAICCode,Producer,Class1,Class2,Class3,GeogCode,TotalSumInsured,CalcFromWritten,BrokerageDeductionsInd,MaxLimit,MinXS,RateFixedInd,TopLimitOrigCcyISO,TopLimitOrigAmount,TopLimitSettAmount,TopLimitConsAmount,TopXSOrigAmount,TopXSSettAmount,TopXSConsAmount,ConsCcyROE,TopLimitSettCcyISO,FILCode,UserName,AggLimit,PaymentMethod,PaymentType,AdjFreq,AdjPcnt,AdjDsc,AdjDate,DWAgreed,ClaimFundInd,NeedReCalcInd,ProfitCommAgreed,Class4,FrontingInd,FinanceUWInd,NOCInd,FullTermInd,EndorsemntInd,FacReinsInd,MonitorPIMInd,DateEntered,UpdBy,MainLayerInd,QuoteDurationDays,UltimateLossRatio,AccountsPayable,BenchMarkRating,InstalmentPeriod,InitialTOT,DeferredTOT,FinalTOT,ConfidentialNotes,LOCInd,ReviewDate,EventName,EventMultiple,OutdoorEventInd,LTPInd,TerrorMiles,TerrorDays,ZipCode,SurveyFees,VetsFees,ConsultancyFees,ExpertFees,AnticipatedLR,PremWarrantyInd,WordingDate,WordingNote,NettLineInd,ReinstBrokerage,LossWarrantyTriggers,LossReviewPct,WarInd,StrikesInd,ExpensesInd,SurveyInd,SurveyDate,NumOfInstalment,SettDueDate,QuoteExpiryDate,QuoteInceptionDate,BureauSettledInd,VenueCity,LORSCode,NoticeToBrokerInsured,NoticeToBrokerInsuredDate,AssuredSignOff,AssuredSignOffDate,TerrorismAPCcy,TerrorismAPAmount,TerrorismAPReceived,TerrorismAPReceivedDate,ZoneA,ZoneB,ZoneC,ZoneD,BusinessInterruption,ManyFacInd,EventPeriod,EventPeriodInception,EventPeriodExpiry,PremiumSwitchDate,PremiumSwitchedInd,RetroDate,MinimumRate,MaximumRate,LossesLoaded,QuotedROL,RetroactiveDate,OldStatsRef,Description,ExternalRef,OrigPRI,OrigCI,UpdPRI,UpdCI,PRIInd,ConfiscationInd,OutwardPolicyInd,PeerReviewDate,PeerReviewInd,ContractCertainty,ContractCertaintyExcpt,ContractCertaintyDate,AggRiskData,ExternalRef2,BUAAInd,PRILossRatioOrig,PRILossRatioUpd,PRINewOldPremOrig,PRINewOldPremUpd,DeficitClause,TruePrice,AdditionalLines,AnticipatedLossRatio,RiskTransfer,LeadInd,ManualMatchingInd,ProcessingType,Indexed,CommutedDate,CommutationOptDate,VerificationStatus,CashLossLimit,ClaimsAgreement,TradeCode,Market,BrokerSettledInd,ScheduleTypeId,DealReference,ExternalRef_COMP,ExternalRef_ForUniqueConstraint,DecRef_ForUniqueConstraint,MasterPolicy,ExternallyProvidedInd,PartVIIInd,SYS_CHANGE_OPERATION,BronzeStagingSystemLoadID
	       ,HASHBYTES(''SHA1'',CONCAT(ProgramRef,''%'',LayerNum,''%'',StatsCcyROE,''%'',AnnivDate,''%'',DaysNotice,''%'',UW,''%'',Interest,''%'',PolicyId,''%'',PolicyRef,''%'',EntryType,''%'',DecRef,''%'',LloydsBrokerId,''%'',LloydsBrokerRef,''%'',LloydsBrokerContactId,''%'',BucketInd,''%'',PeriodType,''%'',InceptionDate,''%'',ExpiryDate,''%'',TermsOfTrade,''%'',DateWritten,''%'',FirstWritten,''%'',ClaimInd,''%'',Brokerage,''%'',Deductions,''%'',CancelDate,''%'',StatsCcyISO,''%'',ConsCcyISO,''%'',ConsTotalSyndEPI,''%'',ConsTotalSyndExposure,''%'',Notes,''%'',RenewedFromRef,''%'',RenewedToRef,''%'',GrossPremIncome,''%'',Comm,''%'',CommMisc,''%'',CoverType,''%'',UniqueMarketRef,''%'',PlacingType,''%'',WordingStatus,''%'',PortfolioTransInd,''%'',LossOfUseDaily,''%'',LastUpd,''%'',SectionCode,''%'',Period,''%'',FullTermPeriod,''%'',FullTermInceptionDate,''%'',FullTermExpiryDate,''%'',FullTermResignInd,''%'',ProfitComm,''%'',ProfitComm2,''%'',CommDeductionsInd,''%'',ProfitCommExpense,''%'',OtherDeductions,''%'',PremFreq,''%'',FirstSettDueDate,''%'',SettTermOfTrade,''%'',AdjTermsOfTrade,''%'',CoverMaxPeriod,''%'',CommSlideAt,''%'',BordereauPremTermsOfTrade,''%'',TreatyReinsInd,''%'',CoIns,''%'',SysLastUpd,''%'',NoClaimBonus,''%'',EarnPattern,''%'',LossOfUseLimit,''%'',LossOfUseXs,''%'',LossOfUseExposure,''%'',LossOfUseCcyISO,''%'',LossOfUseXsDsc,''%'',CommSlide,''%'',CommSlideProv,''%'',CommSlideLR,''%'',CommSlideMin,''%'',BordereauClaimTermsOfTrade,''%'',WaitPeriod,''%'',SmokerInd,''%'',BordereauClaimFreq,''%'',Sex,''%'',DateOfBirth,''%'',QuotaSharePct,''%'',BordereauPremFreq,''%'',QuotaShareLine,''%'',ConvertedInd,''%'',IPTPct,''%'',IPTPremApplic,''%'',RepGroup,''%'',SysPolicyType,''%'',TotalSyndEPI,''%'',TotalSyndExposure,''%'',UWDivision,''%'',WriteBackPct,''%'',LloydsBrokerCode,''%'',LloydsBrokerPseud,''%'',YOA,''%'',PerilGroupId,''%'',TerritoryId,''%'',TopLimit,''%'',TopLimitCcyISO,''%'',TopXs)) as HB100     
		   ,HASHBYTES(''SHA1'',CONCAT(LossOfUseDays,''%'',InsDate,''%'',DelDate,''%'',OtherDeductionsApplyInd,''%'',ProfitCommApplyInd,''%'',PolicyType,''%'',BenefitsType,''%'',NoOfLives,''%'',Grantee,''%'',ProfitCommAdj,''%'',PolicyStatus,''%'',ParentPolicyId,''%'',CancBasis,''%'',RemindFreq,''%'',RemindDate,''%'',RemindMess,''%'',RemindEmail,''%'',TerritoryIdAssured,''%'',BestCode,''%'',NAICCode,''%'',Producer,''%'',Class1,''%'',Class2,''%'',Class3,''%'',GeogCode,''%'',TotalSumInsured,''%'',CalcFromWritten,''%'',BrokerageDeductionsInd,''%'',MaxLimit,''%'',MinXS,''%'',RateFixedInd,''%'',TopLimitOrigCcyISO,''%'',TopLimitOrigAmount,''%'',TopLimitSettAmount,''%'',TopLimitConsAmount,''%'',TopXSOrigAmount,''%'',TopXSSettAmount,''%'',TopXSConsAmount,''%'',ConsCcyROE,''%'',TopLimitSettCcyISO,''%'',FILCode,''%'',UserName,''%'',AggLimit,''%'',PaymentMethod,''%'',PaymentType,''%'',AdjFreq,''%'',AdjPcnt,''%'',AdjDsc,''%'',AdjDate,''%'',DWAgreed,''%'',ClaimFundInd,''%'',NeedReCalcInd,''%'',ProfitCommAgreed,''%'',Class4,''%'',FrontingInd,''%'',FinanceUWInd,''%'',NOCInd,''%'',FullTermInd,''%'',EndorsemntInd,''%'',FacReinsInd,''%'',MonitorPIMInd,''%'',DateEntered,''%'',UpdBy,''%'',MainLayerInd,''%'',QuoteDurationDays,''%'',UltimateLossRatio,''%'',AccountsPayable,''%'',BenchMarkRating,''%'',InstalmentPeriod,''%'',InitialTOT,''%'',DeferredTOT,''%'',FinalTOT,''%'',ConfidentialNotes,''%'',LOCInd,''%'',ReviewDate,''%'',EventName,''%'',EventMultiple,''%'',OutdoorEventInd,''%'',LTPInd,''%'',TerrorMiles,''%'',TerrorDays,''%'',ZipCode,''%'',SurveyFees,''%'',VetsFees,''%'',ConsultancyFees,''%'',ExpertFees,''%'',AnticipatedLR,''%'',PremWarrantyInd,''%'',WordingDate,''%'',WordingNote,''%'',NettLineInd,''%'',ReinstBrokerage,''%'',LossWarrantyTriggers,''%'',LossReviewPct,''%'',WarInd,''%'',StrikesInd,''%'',ExpensesInd,''%'',SurveyInd,''%'',SurveyDate,''%'',NumOfInstalment)) as HB200     
		   ,HASHBYTES(''SHA1'',CONCAT(SettDueDate,''%'',QuoteExpiryDate,''%'',QuoteInceptionDate,''%'',BureauSettledInd,''%'',VenueCity,''%'',LORSCode,''%'',NoticeToBrokerInsured,''%'',NoticeToBrokerInsuredDate,''%'',AssuredSignOff,''%'',AssuredSignOffDate,''%'',TerrorismAPCcy,''%'',TerrorismAPAmount,''%'',TerrorismAPReceived,''%'',TerrorismAPReceivedDate,''%'',ZoneA,''%'',ZoneB,''%'',ZoneC,''%'',ZoneD,''%'',BusinessInterruption,''%'',ManyFacInd,''%'',EventPeriod,''%'',EventPeriodInception,''%'',EventPeriodExpiry,''%'',PremiumSwitchDate,''%'',PremiumSwitchedInd,''%'',RetroDate,''%'',MinimumRate,''%'',MaximumRate,''%'',LossesLoaded,''%'',QuotedROL,''%'',RetroactiveDate,''%'',OldStatsRef,''%'',Description,''%'',ExternalRef,''%'',OrigPRI,''%'',OrigCI,''%'',UpdPRI,''%'',UpdCI,''%'',PRIInd,''%'',ConfiscationInd,''%'',OutwardPolicyInd,''%'',PeerReviewDate,''%'',PeerReviewInd,''%'',ContractCertainty,''%'',ContractCertaintyExcpt,''%'',ContractCertaintyDate,''%'',AggRiskData,''%'',ExternalRef2,''%'',BUAAInd,''%'',PRILossRatioOrig,''%'',PRILossRatioUpd,''%'',PRINewOldPremOrig,''%'',PRINewOldPremUpd,''%'',DeficitClause,''%'',TruePrice,''%'',AdditionalLines,''%'',AnticipatedLossRatio,''%'',RiskTransfer,''%'',LeadInd,''%'',ManualMatchingInd,''%'',ProcessingType,''%'',Indexed,''%'',CommutedDate,''%'',CommutationOptDate,''%'',VerificationStatus,''%'',CashLossLimit,''%'',ClaimsAgreement,''%'',TradeCode,''%'',Market,''%'',BrokerSettledInd,''%'',ScheduleTypeId,''%'',DealReference,''%'',ExternalRef_COMP,''%'',ExternalRef_ForUniqueConstraint,''%'',DecRef_ForUniqueConstraint,''%'',MasterPolicy,''%'',ExternallyProvidedInd,''%'',PartVIIInd,''%'',SYS_CHANGE_OPERATION)) as HB300
       FROM OPENROWSET( BULK ''' + @DeltaFileName + ''', FORMAT = ''PARQUET'') AS [DELTAresult]
	  )
     SELECT * 
	   FROM (
	   
     SELECT @@ServerName                                          as SynapseServerName 
	       ,'''  + @Layer +                                  '''  as Layer
		   ,'''  + @SourceObjectName +                       '''  as Entity 
	       ,'''  + @TestType +                               '''  as TestType
	       ,'''  + @SystemName +                             '''  as SystemName
		   ,'''  + CAST(@SystemLoadID as VARCHAR(20)) +      '''  as SystemLoadId
		   ,'''  + @ObjectRunId  +                           '''  as ObjectRunId		   
 	       ,'''  + @TRFile       +                           '''  as TestResultFileName
		   ,CAST(TGT.BronzeStagingSystemLoadID  as varchar(20))   as BronzeStagingSystemLoadID
	       ,Case when STG.HB100 = TGT.HB100 and STG.HB200 = TGT.HB200 and STG.HB300 = TGT.HB300
		         then ''Passed'' else ''Failed''
			 End as TestResult
		   ,GetDate() as TestedOn
	       ,STG.PolicyId as STGPolicyID
		   ,TGT.PolicyId as TGTPolicyID
	       ,STG.HB100 as STGHB100,TGT.HB100 as TGTHB100
		   ,STG.HB200 as STGHB200,TGT.HB200 as TGTHB200
		   ,STG.HB300 as STGHB300,TGT.HB300 as TGTHB300

       FROM CTESTG STG
       left join CTETGT TGT 
         on STG.PolicyId = TGT.PolicyId
		 ) TTT WHERE SystemLoadId = BronzeStagingSystemLoadID
'
EXEC (@QRY)

END
GO


