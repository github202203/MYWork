With 
 CTE_Bronze_Stage as (
     SELECT [Synd] as [SyndSTAGE],[PIMYear] as [PIMYearSTAGE],[Class1] as [Class1STAGE],[Class2] as [Class2STAGE],[Class3] as [Class3STAGE],[Class4] as [Class4STAGE],[ReportingClass1] as [ReportingClass1STAGE],[ReportingClass2] as [ReportingClass2STAGE],[NewDivision] as [NewDivisionSTAGE],[NewDivisionCode] as [NewDivisionCodeSTAGE],[SysLastUpd] as [SysLastUpdSTAGE],[LastUpd] as [LastUpdSTAGE],[InsDate] as [InsDateSTAGE],[ReportingClassId] as [ReportingClassIdSTAGE],[ProducingTeam] as [ProducingTeamSTAGE],[SYS_CHANGE_OPERATION] as [SYS_CHANGE_OPERATIONSTAGE]
           ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([Synd],'%',[PIMYear],'%',[Class1],'%',[Class2],'%',[Class3],'%',[Class4],'%',[ReportingClass1],'%',[ReportingClass2],'%',[NewDivision],'%',[NewDivisionCode],'%',[SysLastUpd],'%',[LastUpd],'%',[InsDate],'%',[ReportingClassId],'%',[ProducingTeam],'%',[SYS_CHANGE_OPERATION])))as Bronze_Stage_HashBytes
       FROM OPENROWSET ( 
             BULK 'https://dldpdev01.blob.core.windows.net/bronze/underwriting/Internal/Eclipse/Staging/dbo_ReportingClass/SystemLoadID=1022022101001/**',
             FORMAT = 'parquet'
                          ) as RS
)
,CTE_Bronze_Delta as (
     SELECT [Synd] as [SyndDELTA],[PIMYear] as [PIMYearDELTA],[Class1] as [Class1DELTA],[Class2] as [Class2DELTA],[Class3] as [Class3DELTA],[Class4] as [Class4DELTA],[ReportingClass1] as [ReportingClass1DELTA],[ReportingClass2] as [ReportingClass2DELTA],[NewDivision] as [NewDivisionDELTA],[NewDivisionCode] as [NewDivisionCodeDELTA],[SysLastUpd] as [SysLastUpdDELTA],[LastUpd] as [LastUpdDELTA],[InsDate] as [InsDateDELTA],[ReportingClassId] as [ReportingClassIdDELTA],[ProducingTeam] as [ProducingTeamDELTA],[SYS_CHANGE_OPERATION] as [SYS_CHANGE_OPERATIONDELTA]
           ,HASHBYTES('SHA1',CONVERT(NVARCHAR(MAX),CONCAT([Synd]COLLATE Latin1_General_100_BIN2_UTF8,'%',[PIMYear],'%',[Class1]COLLATE Latin1_General_100_BIN2_UTF8,'%',[Class2]COLLATE Latin1_General_100_BIN2_UTF8,'%',[Class3]COLLATE Latin1_General_100_BIN2_UTF8,'%',[Class4]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ReportingClass1]COLLATE Latin1_General_100_BIN2_UTF8,'%',[ReportingClass2]COLLATE Latin1_General_100_BIN2_UTF8,'%',[NewDivision]COLLATE Latin1_General_100_BIN2_UTF8,'%',[NewDivisionCode]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SysLastUpd],'%',[LastUpd],'%',[InsDate],'%',[ReportingClassId],'%',[ProducingTeam]COLLATE Latin1_General_100_BIN2_UTF8,'%',[SYS_CHANGE_OPERATION]COLLATE Latin1_General_100_BIN2_UTF8)))as Bronze_Delta_HashBytes
       FROM [EclipseBronze].[dbo].ReportingClass
)
     SELECT 'syn-dp-dev-01-ondemand' as SynapseServerName  
           ,     'Eclipse' as SystemName 
           ,     'dbo.ReportingClass' as SynapseViewName 
           ,     '1022022101001' as SystemLoadId 
           ,     'Replicated copy of Eclipse with SQL Change Tracking' as SystemDescription 
           ,     '5271' as ObjectRunId 
           ,     'ReportingClass_1022022101001_20222028082037.parquet' as TRFileName 
           ,STAGE.*,DELTA.* 
           ,Case WHEN Bronze_Stage_HashBytes = Bronze_Delta_HashBytes 
                 THEN 'Passed'
                 ELSE 'Failed'
             End as TestResult
           ,GetDate() as TestedOn 
       FROM  CTE_Bronze_Stage STAGE
       left join  CTE_Bronze_Delta DELTA
              --on Synd,PIMYear,Class1,Class2,Class3,Class4,ProducingTeamSTAGE = Synd,PIMYear,Class1,Class2,Class3,Class4,ProducingTeamDELTA
			  on SyndSTAGE COLLATE DATABASE_DEFAULT = SyndDELTA COLLATE DATABASE_DEFAULT
			 and PIMYearSTAGE  = PIMYearDELTA 
			 and Class1STAGE COLLATE DATABASE_DEFAULT= Class1DELTA COLLATE DATABASE_DEFAULT
			 and Class2STAGE COLLATE DATABASE_DEFAULT= Class2DELTA COLLATE DATABASE_DEFAULT
			 and Class3STAGE COLLATE DATABASE_DEFAULT= Class3DELTA COLLATE DATABASE_DEFAULT
			 and Class4STAGE COLLATE DATABASE_DEFAULT= Class4DELTA COLLATE DATABASE_DEFAULT
			 and ProducingTeamSTAGE COLLATE DATABASE_DEFAULT= ProducingTeamDELTA COLLATE DATABASE_DEFAULT
