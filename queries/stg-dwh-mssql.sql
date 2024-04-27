CREATE DATABASE dwhdev;
CREATE USER dwhwriter WITH PASSWORD 'Ebaadataeng'
GRANT ALL PRIVILEGES ON DATABASE dwhdev TO dwhwriter;

CREATE ROLE admin;
CREATE ROLE dwhwriter WITH 
LOGIN
SUPERUSER
CREATEDB
CREATEROLE
IN GROUP admin
PASSWORD 'EbaaDataEng'
;

----------  stagging

IF OBJECT_ID('[stagging].[Asset]') >0 
	DROP TABLE [stagging].[Asset]
CREATE TABLE [stagging].[Asset](
	Id                     INT
	CONSTRAINT DF_sta_Asset_Id                  DEFAULT 0 NOT NULL,
	AssetId                INT             
	CONSTRAINT DF_sta_Asset_AssetId             DEFAULT 0 NOT NULL,
	ProjectId		       VARCHAR(100)		   
	CONSTRAINT DF_sta_Asset_ProjectId            DEFAULT 'None' NOT NULL, 
    Project			       VARCHAR(250)		   
	CONSTRAINT DF_sta_Asset_Project              DEFAULT 'None' NOT NULL,
	Technology		       VARCHAR(200)		   
	CONSTRAINT DF_sta_Asset_Technology          DEFAULT 'None' NOT NULL,
	Cod				       DATE		           
	CONSTRAINT DF_sta_Asset_Cod                 DEFAULT '1901-01-01' NOT NULL,
	MW      		       DECIMAL(5,2)	       
	CONSTRAINT DF_sta_Asset_MW                  DEFAULT   0 NOT NULL,
	SuccessPct		       DECIMAL(7,5)	       
	CONSTRAINT DF_sta_Asset_SuccessPct          DEFAULT   0 NOT NULL,
	InstalledPower	       DECIMAL(7,5)	       
	CONSTRAINT DF_sta_Asset_InstalledPower      DEFAULT   0 NOT NULL,
	Eoh				       DECIMAL(10,3)       
	CONSTRAINT DF_sta_Asset_Eoh                DEFAULT   0,                            
	DateMerchant	       DATE		           
	CONSTRAINT DF_sta_Asset_DateMerchant        DEFAULT '1901-01-01' NOT NULL,
	DismantleDate          DATE                
	CONSTRAINT DF_sta_Asset_DismentleDate       DEFAULT '1901-01-01',
	Repowering             VARCHAR(100)        
	CONSTRAINT DF_sta_Asset_Repowering          DEFAULT 'None',
	DateMsi                DATE                
	CONSTRAINT DF_sta_Asset_MsiDate             DEFAULT '1901-01-01',
	InPlanif               BIT,
	P50                    DECIMAL(10,3)       
	CONSTRAINT DF_sta_Asset_P50                 DEFAULT   0,
	P90                    DECIMAL(10,3)       
	CONSTRAINT DF_sta_Asset_P90                 DEFAULT   0,
	LastUpdated             DATETIME             
	CONSTRAINT  DF_sta_Asset_LastUpdated        DEFAULT GETDATE() NOT NULL,
	UpdatedBy               VARCHAR(50)          
    CONSTRAINT DF_sta_Asset_UpdatedBy           DEFAULT SUSER_SNAME() NOT NULL
)
GO


IF OBJECT_ID('[staging].[Hedge]') >0 
	DROP TABLE [staging].[Hedge]
CREATE TABLE [stagging].[Hedge](
	HedgeId				    INTEGER			    NOT NULL,
	ProjetId			    VARCHAR(50)		    
	CONSTRAINT DF_odsHedge_ProjetId             DEFAULT 'N/A',
	Projet			        VARCHAR(250)        
	CONSTRAINT DF_odsHedge_Projet               DEFAULT 'N/A', 
	TypeHedge		        VARCHAR(50)         
	CONSTRAINT DF_odsHedge_TypeHedge            DEFAULT 'N/A',
	ContractStartDate       DATE  
	CONSTRAINT DF_odsHedge_ContractStartDate    DEFAULT '1900-01-01',
	ContractEndDate		    DATE  
	CONSTRAINT DF_odsHedge_ContractEndDate      DEFAULT '1900-01-01',
	Profil			        VARCHAR(100)		
	CONSTRAINT DF_odsHedge_Profil               DEFAULT 'N/A',
	HedgePct                DECIMAL(7, 2)       
	CONSTRAINT DF_odsHedge_HedgePct             DEFAULT   0,
	Counterparty            VARCHAR(100)        
	CONSTRAINT DF_odsHedge_Counterparty         DEFAULT 'N/A',
	CountryCounterparty		VARCHAR(100)        
	CONSTRAINT DF_odsHedge_CountryCounterparty  DEFAULT 'N/A',
	DimensionCheckSum INT NOT NULL 
    CONSTRAINT DF_odsHedge_DimensionCheckSum    DEFAULT -1,
    LastUpdated DATETIME  NOT NULL 
    CONSTRAINT DF_odsHedge_LastUpdated DEFAULT GETDATE(),
    UpdatedBy varchar(50) NOT NULL
    CONSTRAINT DF_odsHedge_UpdatedBy DEFAULT SUSER_SNAME()
);

---------  warehouse

IF OBJECT_ID('[dwh].[DimAsset]') >0 
	DROP TABLE [dwh].[DimAsset]
CREATE TABLE [dwh].[DimAsset](
    Id                     INT             IDENTITY(1, 1)  NOT NULL, 
	AssetId                INT             
	CONSTRAINT DF_dwh_DimAsset_AssetId          DEFAULT 0 NOT NULL,
	ProjectId		       VARCHAR(100)		   
	CONSTRAINT DF_dwh_DimAsset_ProjectId        DEFAULT 'None' NOT NULL, 
    Project			       VARCHAR(250)		   
	CONSTRAINT DF_dwh_DimAsset_Project          DEFAULT 'None' NOT NULL,
	Technology		       VARCHAR(200)		   
	CONSTRAINT DF_dwh_DimAsset_Technology       DEFAULT 'None' NOT NULL,
	Cod				       DATE		           
	CONSTRAINT DF_dwh_DimAsset_Cod              DEFAULT '1901-01-01' NOT NULL,
	MW      		       DECIMAL(5,2)	       
	CONSTRAINT DF_dwh_DimAsset_MW               DEFAULT   0 NOT NULL,
	SuccesPct		       DECIMAL(7,5)	       
	CONSTRAINT DF_dwh_DimAsset_SuccesPct        DEFAULT   0 NOT NULL,
	InstalledPower	       DECIMAL(7,5)	       
	CONSTRAINT DF_dwh_DimAsset_InstalledPower   DEFAULT   0 NOT NULL,
	Eoh				       DECIMAL(10,3)       
	CONSTRAINT DF_dwh_DimAsset_Eoh              DEFAULT   0,                            
	DateMerchant	       DATE		           
	CONSTRAINT DF_dwh_DimAsset_DateMerchant     DEFAULT '1901-01-01' NOT NULL,
	DismantleDate          DATE                
	CONSTRAINT DF_dwh_DimAsset_DismantleDate    DEFAULT '1901-01-01',
	Repowering             VARCHAR(100)        
	CONSTRAINT DF_dwh_DimAsset_Repowering       DEFAULT 'None',
	DateMsi                DATE                
	CONSTRAINT DF_dwh_DimAsset_DateMsi          DEFAULT '1901-01-01',
	InPlanif               VARCHAR(50)         
	CONSTRAINT DF_dwh_DimAsset_InPlanif         DEFAULT 'None' NOT NULL,
	P50                    DECIMAL(10,3)       
	CONSTRAINT DF_dwh_DimAsset_P50              DEFAULT   0,
	P90                    DECIMAL(10,3)       
	CONSTRAINT DF_dwh_DimAsset_P90              DEFAULT   0,
    LastUpdated             DATETIME           NOT NULL 
	CONSTRAINT  DF_dwh_DimAsset_LastUpdated     DEFAULT GETDATE(),
	UpdatedBy               VARCHAR(50)        NOT NULL 
    CONSTRAINT DF_dwh_DimAsset_UpdatedBy        DEFAULT SUSER_SNAME(),
	CONSTRAINT PK_dwh_DimAsset PRIMARY KEY CLUSTERED
	(
 [AssetId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

IF OBJECT_ID('[dwh].[DimHedge]') >0 
	DROP TABLE [dwh].[DimHedge]
CREATE TABLE [dwh].[DimHedge](

	ID					    INTEGER    IDENTITY(1,1) NOT NULL,
	HedgeId				    INTEGER
	CONSTRAINT DF_dwh_DimHedge_ProjetId          NOT NULL,
	ProjetId			    VARCHAR(50)		    
	CONSTRAINT DF_dwh_DimHedge_ProjetId          DEFAULT 'N/A',
	Projet			        VARCHAR(250)        
	CONSTRAINT DF_dwh_DimHedge_Projet            DEFAULT 'N/A', 
	TypeHedge		        VARCHAR(50)         
	CONSTRAINT DF_dwh_DimHedge_TypeHedge         DEFAULT 'N/A',
	ContractStartDate       DATE  
	CONSTRAINT DF_dwh_DimHedge_ContractStartDate DEFAULT  '1900-01-01',
	ContractEndDate		    DATE  
	CONSTRAINT DF_dwh_DimHedge_ContractEndDate   DEFAULT  '1900-01-01',
	Profil			        VARCHAR(100)		
	CONSTRAINT DF_dwh_DimHedge_Profil            DEFAULT 'N/A',
	HedgePct                DECIMAL(7, 2)       
	CONSTRAINT DF_dwh_DimHedge_HedgePct          DEFAULT   0,
	Counterparty            VARCHAR(100)        
	CONSTRAINT DF_dwh_DimHedge_Counterparty      DEFAULT 'N/A',
	CountryCounterparty		VARCHAR(100)        
	CONSTRAINT DF_dwh_DimHedge_CountryCounterparty DEFAULT 'N/A',
	DimensionCheckSum       INT NOT NULL 
    CONSTRAINT DF_dwh_DimHedge_DimensionCheckSum DEFAULT -1,
	EffectiveDate           DATE NOT NULL 
    CONSTRAINT DF_dwh_DimHedge_EffectiveDate DEFAULT GETDATE(),
	EndDate                 DATE NOT NULL 
    CONSTRAINT DF_dwh_DimHedge_EndDate DEFAULT '12/31/9999',
	CurrentRecord           CHAR(1) NOT NULL 
    CONSTRAINT DF_dwh_DimHedge_CurrentRecord DEFAULT 'Y',
	LastUpdated             DATETIME  NOT NULL 
	CONSTRAINT  DF_dwh_DimHedge_LastUpdated DEFAULT GETDATE(),
	UpdatedBy               VARCHAR(50) NOT NULL 
    CONSTRAINT DF_dwh_DimHedge_UpdatedBy DEFAULT SUSER_SNAME()
	CONSTRAINT PK_dwh_DimHedge PRIMARY KEY CLUSTERED
	(
 [HedgeId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE schema dw;
ALTER SCHEMA DW
TRANSFER dwh.DimDate;

/****** DimDate ********/
BEGIN TRY
    DROP TABLE [dw].[DimDate]
END TRY
BEGIN CATCH
    /* No Action */
END CATCH
/**********************************************************************************/
IF OBJECT_ID('[dwh].[DimDate]') > 0
DROP TABLE [dwh].[DimDate]
CREATE TABLE [dwh].[DimDate]
(
    [DateKey] INT PRIMARY KEY,
    [Date] DATETIME,
    [FullDateUK] CHAR(10), -- Date in dd-MM-yyyy format
    [FullDateUSA] CHAR(10), -- Date in MM-dd-yyyy format
    [DayOfMonth] VARCHAR(2), -- Field will hold day number of Month
    [DaySuffix] VARCHAR(4), -- Apply suffix as 1st, 2nd, 3rd, etc.
    [DayName] VARCHAR(9), -- Contains name of the day, Sunday, Monday
    [DayOfWeekUSA] CHAR(1), -- First Day Sunday=1 and Saturday=7
    [DayOfWeekUK] CHAR(1), -- First Day Monday=1 and Sunday=7
    [DayOfWeekInMonth] VARCHAR(2), -- 1st Monday or 2nd Monday in Month
    [DayOfWeekInYear] VARCHAR(2),
    [DayOfQuarter] VARCHAR(3),
    [DayOfYear] VARCHAR(3),
    [WeekOfMonth] VARCHAR(1), -- Week Number of Month
    [WeekOfQuarter] VARCHAR(2), -- Week Number of the Quarter
    [WeekOfYear] VARCHAR(2), -- Week Number of the Year
    [Month] VARCHAR(2), -- Number of the Month 1 to 12
    [MonthName] VARCHAR(9), -- January, February, etc.
    [MonthOfQuarter] VARCHAR(2), -- Month Number belongs to Quarter
    [Quarter] CHAR(1),
    [QuarterName] VARCHAR(9), -- First, Second, etc.
    [Year] CHAR(4), -- Year value of Date stored in Row
    [YearName] CHAR(7), -- CY 2012, CY 2013
    [MonthYear] CHAR(10), -- Jan-2013, Feb-2013
    [MMYYYY] CHAR(6),
    [FirstDayOfMonth] DATE,
    [LastDayOfMonth] DATE,
    [FirstDayOfQuarter] DATE,
    [LastDayOfQuarter] DATE,
    [FirstDayOfYear] DATE,
    [LastDayOfYear] DATE,
    [IsHolidayUSA] BIT, -- Flag 1=National Holiday, 0=No National Holiday
    [IsWeekday] BIT, -- 0=Weekend, 1=Weekday
    [HolidayUSA] VARCHAR(50), -- Name of Holiday in US
    [IsHolidayUK] BIT NULL, -- Flag 1=National Holiday, 0=No National Holiday
    [HolidayUK] VARCHAR(50) NULL -- Name of Holiday in UK
)
GO

/********************************************************************************************/
--Specify Start Date and End date here
--Value of Start Date Must be Less than Your End Date

DECLARE @StartDate DATETIME = '01/01/2020' --Starting value of Date Range
DECLARE @EndDate DATETIME = '31/12/2030' --End Value of Date Range

--Temporary Variables To Hold the Values During Processing of Each Date of Year
DECLARE
	@DayOfWeekInMonth INT,
	@DayOfWeekInYear INT,
	@DayOfQuarter INT,
	@WeekOfMonth INT,
	@CurrentYear INT,
	@CurrentMonth INT,
	@CurrentQuarter INT

/*Table Data type to store the day of week count for the month and year*/
DECLARE @DayOfWeek TABLE (DOW INT, MonthCount INT, QuarterCount INT, YearCount INT)

INSERT INTO @DayOfWeek VALUES (1, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (2, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (3, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (4, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (5, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (6, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (7, 0, 0, 0)

--Extract and assign various parts of Values from Current Date to Variable

DECLARE @CurrentDate AS DATETIME = @StartDate
SET @CurrentMonth = DATEPART(MM, @CurrentDate)
SET @CurrentYear = DATEPART(YY, @CurrentDate)
SET @CurrentQuarter = DATEPART(QQ, @CurrentDate)

/********************************************************************************************/
--Proceed only if Start Date(Current date ) is less than End date you specified above

WHILE @CurrentDate < @EndDate
BEGIN
 
/*Begin day of week logic*/

         /*Check for Change in Month of the Current date if Month changed then 
          Change variable value*/
	IF @CurrentMonth != DATEPART(MM, @CurrentDate) 
	BEGIN
		UPDATE @DayOfWeek
		SET MonthCount = 0
		SET @CurrentMonth = DATEPART(MM, @CurrentDate)
	END

        /* Check for Change in Quarter of the Current date if Quarter changed then change 
         Variable value*/

	IF @CurrentQuarter != DATEPART(QQ, @CurrentDate)
	BEGIN
		UPDATE @DayOfWeek
		SET QuarterCount = 0
		SET @CurrentQuarter = DATEPART(QQ, @CurrentDate)
	END
       
        /* Check for Change in Year of the Current date if Year changed then change 
         Variable value*/
	

	IF @CurrentYear != DATEPART(YY, @CurrentDate)
	BEGIN
		UPDATE @DayOfWeek
		SET YearCount = 0
		SET @CurrentYear = DATEPART(YY, @CurrentDate)
	END
	
        -- Set values in table data type created above from variables 

	UPDATE @DayOfWeek
	SET 
		MonthCount = MonthCount + 1,
		QuarterCount = QuarterCount + 1,
		YearCount = YearCount + 1
	WHERE DOW = DATEPART(DW, @CurrentDate)

	SELECT
		@DayOfWeekInMonth = MonthCount,
		@DayOfQuarter = QuarterCount,
		@DayOfWeekInYear = YearCount
	FROM @DayOfWeek
	WHERE DOW = DATEPART(DW, @CurrentDate)
	
/*End day of week logic*/


/* Populate Your Dimension Table with values*/
	
	INSERT INTO [dwh].[DimDate]
	SELECT
		
		CONVERT (char(8),@CurrentDate,112) as DateKey,
		@CurrentDate AS Date,
		CONVERT (char(10),@CurrentDate,103) as FullDateUK,
		CONVERT (char(10),@CurrentDate,101) as FullDateUSA,
		DATEPART(DD, @CurrentDate) AS DayOfMonth,
		--Apply Suffix values like 1st, 2nd 3rd etc..
		CASE 
			WHEN DATEPART(DD,@CurrentDate) IN (11,12,13) _
			THEN CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'th'
			WHEN RIGHT(DATEPART(DD,@CurrentDate),1) = 1 _
			THEN CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'st'
			WHEN RIGHT(DATEPART(DD,@CurrentDate),1) = 2 _
			THEN CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'nd'
			WHEN RIGHT(DATEPART(DD,@CurrentDate),1) = 3 _
			THEN CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'rd'
			ELSE CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'th' 
			END AS DaySuffix,
		
		DATENAME(DW, @CurrentDate) AS DayName,
		DATEPART(DW, @CurrentDate) AS DayOfWeekUSA,

		-- check for day of week as Per US and change it as per UK format 
		CASE DATEPART(DW, @CurrentDate)
			WHEN 1 THEN 7
			WHEN 2 THEN 1
			WHEN 3 THEN 2
			WHEN 4 THEN 3
			WHEN 5 THEN 4
			WHEN 6 THEN 5
			WHEN 7 THEN 6
			END 
			AS DayOfWeekUK,
		
		@DayOfWeekInMonth AS DayOfWeekInMonth,
		@DayOfWeekInYear AS DayOfWeekInYear,
		@DayOfQuarter AS DayOfQuarter,
		DATEPART(DY, @CurrentDate) AS DayOfYear,
		DATEPART(WW, @CurrentDate) + 1 - DATEPART(WW, CONVERT(VARCHAR, _
		DATEPART(MM, @CurrentDate)) + '/1/' + CONVERT(VARCHAR, _
		DATEPART(YY, @CurrentDate))) AS WeekOfMonth,
		(DATEDIFF(DD, DATEADD(QQ, DATEDIFF(QQ, 0, @CurrentDate), 0), _
		@CurrentDate) / 7) + 1 AS WeekOfQuarter,
		DATEPART(WW, @CurrentDate) AS WeekOfYear,
		DATEPART(MM, @CurrentDate) AS Month,
		DATENAME(MM, @CurrentDate) AS MonthName,
		CASE
			WHEN DATEPART(MM, @CurrentDate) IN (1, 4, 7, 10) THEN 1
			WHEN DATEPART(MM, @CurrentDate) IN (2, 5, 8, 11) THEN 2
			WHEN DATEPART(MM, @CurrentDate) IN (3, 6, 9, 12) THEN 3
			END AS MonthOfQuarter,
		DATEPART(QQ, @CurrentDate) AS Quarter,
		CASE DATEPART(QQ, @CurrentDate)
			WHEN 1 THEN 'First'
			WHEN 2 THEN 'Second'
			WHEN 3 THEN 'Third'
			WHEN 4 THEN 'Fourth'
			END AS QuarterName,
		DATEPART(YEAR, @CurrentDate) AS Year,
		'CY ' + CONVERT(VARCHAR, DATEPART(YEAR, @CurrentDate)) AS YearName,
		LEFT(DATENAME(MM, @CurrentDate), 3) + '-' + CONVERT(VARCHAR, _
		DATEPART(YY, @CurrentDate)) AS MonthYear,
		RIGHT('0' + CONVERT(VARCHAR, DATEPART(MM, @CurrentDate)),2) + _
		CONVERT(VARCHAR, DATEPART(YY, @CurrentDate)) AS MMYYYY,
		CONVERT(DATETIME, CONVERT(DATE, DATEADD(DD, - (DATEPART(DD, _
		@CurrentDate) - 1), @CurrentDate))) AS FirstDayOfMonth,
		CONVERT(DATETIME, CONVERT(DATE, DATEADD(DD, - (DATEPART(DD, _
		(DATEADD(MM, 1, @CurrentDate)))), DATEADD(MM, 1, _
		@CurrentDate)))) AS LastDayOfMonth,
		DATEADD(QQ, DATEDIFF(QQ, 0, @CurrentDate), 0) AS FirstDayOfQuarter,
		DATEADD(QQ, DATEDIFF(QQ, -1, @CurrentDate), -1) AS LastDayOfQuarter,
		CONVERT(DATETIME, '01/01/' + CONVERT(VARCHAR, DATEPART(YY, _
		@CurrentDate))) AS FirstDayOfYear,
		CONVERT(DATETIME, '12/31/' + CONVERT(VARCHAR, DATEPART(YY, _
		@CurrentDate))) AS LastDayOfYear,
		NULL AS IsHolidayUSA,
		CASE DATEPART(DW, @CurrentDate)
			WHEN 1 THEN 0
			WHEN 2 THEN 1
			WHEN 3 THEN 1
			WHEN 4 THEN 1
			WHEN 5 THEN 1
			WHEN 6 THEN 1
			WHEN 7 THEN 0
			END AS IsWeekday,
		NULL AS HolidayUSA, Null, Null

	SET @CurrentDate = DATEADD(DD, 1, @CurrentDate)
END

/********************************************************************************************/
-- Step 3.
-- Update Values of Holiday as per UK Government Declaration for National Holiday.

/* Update HOLIDAY fields of UK as per Govt. Declaration of National Holiday */

-- Good Friday April 18
UPDATE [dwh].[DimDate]
SET HolidayUK = 'Good Friday'
WHERE [Month] = 4 AND [DayOfMonth] = 18;

-- Easter Monday April 21
UPDATE [dwh].[DimDate]
SET HolidayUK = 'Easter Monday'
WHERE [Month] = 4 AND [DayOfMonth] = 21;

-- Early May Bank Holiday May 5
UPDATE [dwh].[DimDate]
SET HolidayUK = 'Early May Bank Holiday'
WHERE [Month] = 5 AND [DayOfMonth] = 5;

-- Spring Bank Holiday May 26
UPDATE [dwh].[DimDate]
SET HolidayUK = 'Spring Bank Holiday'
WHERE [Month] = 5 AND [DayOfMonth] = 26;

-- Summer Bank Holiday August 25
UPDATE [dwh].[DimDate]
SET HolidayUK = 'Summer Bank Holiday'
WHERE [Month] = 8 AND [DayOfMonth] = 25;

-- Boxing Day December 26
UPDATE [dwh].[DimDate]
SET HolidayUK = 'Boxing Day'
WHERE [Month] = 12 AND [DayOfMonth] = 26;

-- CHRISTMAS
UPDATE [dwh].[DimDate]
SET HolidayUK = 'Christmas Day'
WHERE [Month] = 12 AND [DayOfMonth] = 25;

-- New Year's Day
UPDATE [dwh].[DimDate]
SET HolidayUK = 'New Year''s Day'
WHERE [Month] = 1 AND [DayOfMonth] = 1;

-- Update flag for UK Holidays 1 = Holiday, 0 = No Holiday
UPDATE [dwh].[DimDate]
SET IsHolidayUK = CASE
    WHEN HolidayUK IS NULL THEN 0
    WHEN HolidayUK IS NOT NULL THEN 1
END;

-- Step 4.
-- Update Values of Holiday as per USA Govt. Declaration for National Holiday.

/* Update HOLIDAY Field of USA In dimension */

-- Thanksgiving - Fourth THURSDAY in November
UPDATE [dwh].[DimDate]
SET HolidayUSA = 'Thanksgiving Day'
WHERE
    [Month] = 11
    AND [DayOfWeekUSA] = 'Thursday'
    AND DayOfWeekInMonth = 4;

-- CHRISTMAS
UPDATE [dwh].[DimDate]
SET HolidayUSA = 'Christmas Day'
WHERE [Month] = 12 AND [DayOfMonth] = 25;

-- 4th of July
UPDATE [dwh].[DimDate]
SET HolidayUSA = 'Independence Day'
WHERE [Month] = 7 AND [DayOfMonth] = 4;

-- New Year's Day
UPDATE [dwh].[DimDate]
SET HolidayUSA = 'New Year''s Day'
WHERE [Month] = 1 AND [DayOfMonth] = 1;

-- Memorial Day - Last Monday in May
UPDATE [dwh].[DimDate]
SET HolidayUSA = 'Memorial Day'
WHERE DateKey IN (
    SELECT MAX(DateKey)
    FROM [dwh].[DimDate]
    WHERE [MonthName] = 'May'
    AND [DayOfWeekUSA] = 'Monday'
    GROUP BY [Year], [Month]
);

-- Labor Day - First Monday in September
UPDATE [dwh].[DimDate]
SET HolidayUSA = 'Labor Day'
WHERE DateKey IN (
    SELECT MIN(DateKey)
    FROM [dwh].[DimDate]
    WHERE [MonthName] = 'September'
    AND [DayOfWeekUSA] = 'Monday'
    GROUP BY [Year], [Month]
);

-- Valentine's Day
UPDATE [dwh].[DimDate]
SET HolidayUSA = 'Valentine''s Day'
WHERE [Month] = 2 AND [DayOfMonth] = 14;

-- Saint Patrick's Day
UPDATE [dwh].[DimDate]
SET HolidayUSA = 'Saint Patrick''s Day'
WHERE [Month] = 3 AND [DayOfMonth] = 17;

-- Martin Luther King Day - Third Monday in January starting in 1983
UPDATE [dwh].[DimDate]
SET HolidayUSA = 'Martin Luther King Jr Day'
WHERE [Month] = 1 AND [DayOfWeekUSA] = 'Monday'
AND [Year] >= 1983
AND DayOfWeekInMonth = 3;

-- President's Day - Third Monday in February
UPDATE [dwh].[DimDate]
SET HolidayUSA = 'President''s Day'
WHERE [Month] = 2 AND [DayOfWeekUSA] = 'Monday'
AND DayOfWeekInMonth = 3;

-- Mother's Day - Second Sunday of May
UPDATE [dwh].[DimDate]
SET HolidayUSA = 'Mother''s Day'
WHERE [Month] = 5 AND [DayOfWeekUSA] = 'Sunday'
AND DayOfWeekInMonth = 2;

-- Father's Day - Third Sunday of June
UPDATE [dwh].[DimDate]
SET HolidayUSA = 'Father''s Day'
WHERE [Month] = 6 AND [DayOfWeekUSA] = 'Sunday'
AND DayOfWeekInMonth = 3;

-- Halloween 10/31
UPDATE [dwh].[DimDate]
SET HolidayUSA = 'Halloween'
WHERE [Month] = 10 AND [DayOfMonth] = 31;

-- Election Day - The first Tuesday after the first Monday in November
BEGIN
    DECLARE @Holidays TABLE (ID INT IDENTITY(1, 1), DateID INT, Week TINYINT, YEAR CHAR(4), DAY CHAR(2))

    INSERT INTO @Holidays(DateID, [Year], [Day])
    SELECT DateKey, [Year], [DayOfMonth]
    FROM [dwh].[DimDate]
    WHERE [Month] = 11 AND [DayOfWeekUSA] = 'Monday'
    ORDER BY YEAR, DayOfMonth

    DECLARE @CNTR INT, @POS INT, @STARTYEAR INT, @ENDYEAR INT, @MINDAY INT

    SELECT @CURRENTYEAR = MIN([Year]), @STARTYEAR = MIN([Year]), @ENDYEAR = MAX([Year])
    FROM @Holidays

    WHILE @CURRENTYEAR <= @ENDYEAR
    BEGIN
        SELECT @CNTR = COUNT([Year])
        FROM @Holidays
        WHERE [Year] = @CURRENTYEAR

        SET @POS = 1

        WHILE @POS <= @CNTR
        BEGIN
            SELECT @MINDAY = MIN(DAY)
            FROM @Holidays
            WHERE [Year] = @CURRENTYEAR AND [Week] IS NULL

            UPDATE @Holidays
            SET [Week] = @POS
            WHERE [Year] = @CURRENTYEAR AND [Day] = @MINDAY

            SELECT @POS = @POS + 1
        END

        SELECT @CURRENTYEAR = @CURRENTYEAR + 1
    END

    UPDATE [dwh].[DimDate]
    SET HolidayUSA = 'Election Day'
    FROM [dwh].[DimDate] DT
    JOIN @Holidays HL ON (HL.DateID + 1) = DT.DateKey
    WHERE [Week] = 1
END

-- Set flag for USA holidays in Dimension
UPDATE [dwh].[DimDate]
SET IsHolidayUSA = CASE WHEN HolidayUSA IS NULL THEN 0 WHEN HolidayUSA IS NOT NULL THEN 1 END;

-- Add Fiscal Calendar columns into table DimDate
ALTER TABLE [dwh].[DimDate]
ADD [FiscalDayOfYear] VARCHAR(3),
    [FiscalWeekOfYear] VARCHAR(3),
    [FiscalMonth] VARCHAR(2), 
    [FiscalQuarter] CHAR(1),
    [FiscalQuarterName] VARCHAR(9),
    [FiscalYear] CHAR(4),
    [FiscalYearName] CHAR(7),
    [FiscalMonthYear] CHAR(10),
    [FiscalMMYYYY] CHAR(6),
    [FiscalFirstDayOfMonth] DATE,
    [FiscalLastDayOfMonth] DATE,
    [FiscalFirstDayOfQuarter] DATE,
    [FiscalLastDayOfQuarter] DATE,
    [FiscalFirstDayOfYear] DATE,
    [FiscalLastDayOfYear] DATE;

-- The following section needs to be populated for defining the fiscal calendar

DECLARE
    @dtFiscalYearStart SMALLDATETIME = 'January 01, 1995',
    @FiscalYear INT = 1995,
    @LastYear INT = 2025,
    @FirstLeapYearInPeriod INT = 1996;

DECLARE
    @iTemp INT,
    @LeapWeek INT,
    @CurrentDate DATETIME,
    @FiscalDayOfYear INT,
    @FiscalWeekOfYear INT,
    @FiscalMonth INT,
    @FiscalQuarter INT,
    @FiscalQuarterName VARCHAR(10),
    @FiscalYearName VARCHAR(7),
    @LeapYear INT,
    @FiscalFirstDayOfYear DATE,
    @FiscalFirstDayOfQuarter DATE,
    @FiscalFirstDayOfMonth DATE,
    @FiscalLastDayOfYear DATE,
    @FiscalLastDayOfQuarter DATE,
    @FiscalLastDayOfMonth DATE;

-- Holds the years that have 455 in the last quarter

DECLARE @LeapTable TABLE (leapyear INT);

-- TABLE to contain the fiscal year calendar

DECLARE @tb TABLE(
    PeriodDate DATETIME,
    [FiscalDayOfYear] VARCHAR(3),
    [FiscalWeekOfYear] VARCHAR(3),
    [FiscalMonth] VARCHAR(2), 
    [FiscalQuarter] VARCHAR(1),
    [FiscalQuarterName] VARCHAR(9),
    [FiscalYear] VARCHAR(4),
    [FiscalYearName] VARCHAR(7),
    [FiscalMonthYear] VARCHAR(10),
    [FiscalMMYYYY] VARCHAR(6),
    [FiscalFirstDayOfMonth] DATE,
    [FiscalLastDayOfMonth] DATE,
    [FiscalFirstDayOfQuarter] DATE,
    [FiscalLastDayOfQuarter] DATE,
    [FiscalFirstDayOfYear] DATE,
    [FiscalLastDayOfYear] DATE);

-- Populate the table with all leap years

SET @LeapYear = @FirstLeapYearInPeriod;
WHILE (@LeapYear < @LastYear)
BEGIN
    INSERT INTO @leapTable VALUES (@LeapYear);
    SET @LeapYear = @LeapYear + 5;
END

-- Initiate parameters before the loop

SET @CurrentDate = @dtFiscalYearStart;
SET @FiscalDayOfYear = 1;
SET @FiscalWeekOfYear = 1;
SET @FiscalMonth = 1;
SET @FiscalQuarter = 1;
SET @FiscalWeekOfYear = 1;

IF (EXISTS (SELECT * FROM @LeapTable WHERE @FiscalYear = leapyear))
BEGIN
    SET @LeapWeek = 1;
END
ELSE
BEGIN
    SET @LeapWeek = 0;
END

-- Loop on days in interval
WHILE (DATEPART(yy,@CurrentDate) <= @LastYear)
BEGIN
    -- SET fiscal Month
    SELECT @FiscalMonth = CASE 
        /* Use this section for a 4-5-4 calendar.  
        Every leap year the result will be a 4-5-5 */
        WHEN @FiscalWeekOfYear BETWEEN 1 AND 4 THEN 1 -- 4 weeks
        WHEN @FiscalWeekOfYear BETWEEN 5 AND 9 THEN 2 -- 5 weeks
        WHEN @FiscalWeekOfYear BETWEEN 10 AND 13 THEN 3 -- 4 weeks
        WHEN @FiscalWeekOfYear BETWEEN 14 AND 17 THEN 4 -- 4 weeks
        WHEN @FiscalWeekOfYear BETWEEN 18 AND 22 THEN 5 -- 5 weeks
        WHEN @FiscalWeekOfYear BETWEEN 23 AND 26 THEN 6 -- 4 weeks
        WHEN @FiscalWeekOfYear BETWEEN 27 AND 30 THEN 7 -- 4 weeks
        WHEN @FiscalWeekOfYear BETWEEN 31 AND 35 THEN 8 -- 5 weeks
        WHEN @FiscalWeekOfYear BETWEEN 36 AND 39 THEN 9 -- 4 weeks
        WHEN @FiscalWeekOfYear BETWEEN 40 AND 43 THEN 10 -- 4 weeks
        WHEN @FiscalWeekOfYear BETWEEN 44 AND (48+@LeapWeek) THEN 11 -- 5 weeks
        WHEN @FiscalWeekOfYear BETWEEN (49+@LeapWeek) AND (52+@LeapWeek) THEN 12 -- 4 weeks (5 weeks on leap year)

        -- Use this section for a 4-4-5 calendar.  
        -- Every leap year the result will be a 4-5-5
        -- Additional section commented out for clarity
    END

    -- SET Fiscal Quarter
    SELECT @FiscalQuarter = CASE 
        WHEN @FiscalMonth BETWEEN 1 AND 3 THEN 1
        WHEN @FiscalMonth BETWEEN 4 AND 6 THEN 2
        WHEN @FiscalMonth BETWEEN 7 AND 9 THEN 3
        WHEN @FiscalMonth BETWEEN 10 AND 12 THEN 4
    END

    SELECT @FiscalQuarterName = CASE 
        WHEN @FiscalMonth BETWEEN 1 AND 3 THEN 'First'
        WHEN @FiscalMonth BETWEEN 4 AND 6 THEN 'Second'
        WHEN @FiscalMonth BETWEEN 7 AND 9 THEN 'Third'
        WHEN @FiscalMonth BETWEEN 10 AND 12 THEN 'Fourth'
    END

    -- Set Fiscal Year Name
    SELECT @FiscalYearName = 'FY ' + CONVERT(VARCHAR, @FiscalYear)

    INSERT INTO @tb (PeriodDate, FiscalDayOfYear, FiscalWeekOfYear, FiscalMonth, FiscalQuarter, FiscalQuarterName, FiscalYear, FiscalYearName) VALUES 
    (@CurrentDate, @FiscalDayOfYear, @FiscalWeekOfYear, @FiscalMonth, @FiscalQuarter, @FiscalQuarterName, @FiscalYear, @FiscalYearName)

    -- SET next day
    SET @CurrentDate = DATEADD(dd, 1, @CurrentDate)
    SET @FiscalDayOfYear = @FiscalDayOfYear + 1
    SET @FiscalWeekOfYear = ((@FiscalDayOfYear-1) / 7) + 1

    IF (@FiscalWeekOfYear > (52+@LeapWeek))
    BEGIN
        -- Reset a new year
        SET @FiscalDayOfYear = 1
        SET @FiscalWeekOfYear = 1
        SET @FiscalYear = @FiscalYear + 1

        IF (EXISTS (SELECT * FROM @leapTable WHERE @FiscalYear = leapyear))
        BEGIN
            SET @LeapWeek = 1
        END
        ELSE
        BEGIN
            SET @LeapWeek = 0
        END
    END
END

-- Set first and last days of the fiscal months
UPDATE @tb
SET FiscalFirstDayOfMonth = minmax.StartDate,
    FiscalLastDayOfMonth = minmax.EndDate
FROM @tb t,
    (
    SELECT FiscalMonth, FiscalQuarter, FiscalYear,
        MIN(PeriodDate) AS StartDate, MAX(PeriodDate) AS EndDate
    FROM @tb
    GROUP BY FiscalMonth, FiscalQuarter, FiscalYear
    ) minmax
WHERE t.FiscalMonth = minmax.FiscalMonth AND
    t.FiscalQuarter = minmax.FiscalQuarter AND
    t.FiscalYear = minmax.FiscalYear 

-- Set first and last days of the fiscal quarters
UPDATE @tb
SET FiscalFirstDayOfQuarter = minmax.StartDate,
    FiscalLastDayOfQuarter = minmax.EndDate
FROM @tb t
   
    
/****** Object:  Table [stagging].[ProdPrices]    Script Date: 17-10-2022 13:26:13 ******/
USE DWH;
CREATE SCHEMA stagging;
IF OBJECT_ID('[stagging].ProdPrices') >0 
	DROP TABLE [stagging].[ProdPrices]
CREATE TABLE [stagging].ProdPrices(
	Id				       INTEGER             IDENTITY(1,1),
	DateId                INTEGER        	   DEFAULT '0',
	AssetId                INTEGER			   DEFAULT  0 NULL,
    HedgeId                INTEGER			   DEFAULT  0 NULL,
	ProjetId               VARCHAR(100)		   DEFAULT 'None' NULL,
    P50Asset     		   DECIMAL(10,3)	   DEFAULT  0 NULL,
	P90Asset		       DECIMAL(10,3)	   DEFAULT  0 NULL,
	P50Hedge     		   DECIMAL(10,3)	   DEFAULT  0 NULL,
	P90Hedge		       DECIMAL(10,3)	   DEFAULT  0 NULL,
	ContractPrice          DECIMAL(7, 3)	   DEFAULT  0 NULL,
	SettlementPrice        DECIMAL(7, 3)	   DEFAULT  0 NULL,
	LastUpdated            DATETIME            DEFAULT GETDATE()
)ON [PRIMARY]
GO

/****** Object:  Table [dwh].[FactProdPrices]    Script Date: 17-10-2022 13:26:13 ******/
IF OBJECT_ID('[dwh].FactProdPrices') >0 
	DROP TABLE [dwh].[FactProdPrices]
CREATE TABLE [dwh].[FactProdPrices](
	Id				       INTEGER             IDENTITY(1,1),
	HedgeId		           INTEGER        	   DEFAULT '0',
	DateId                 INTEGER        	   DEFAULT '0',
	ProjetId               VARCHAR(100)		   DEFAULT 'None' NULL,
    P50Asset     		   DECIMAL(10,3)	   DEFAULT  0 NULL,
	P90Asset		       DECIMAL(10,3)	   DEFAULT  0 NULL,
	P50Hedge     		   DECIMAL(10,3)	   DEFAULT  0 NULL,
	P90Hedge		       DECIMAL(10,3)	   DEFAULT  0 NULL,
	ContractPrice          DECIMAL(7, 3)	   DEFAULT  0 NULL,
	SettlementPrice        DECIMAL(7, 3)	   DEFAULT  0 NULL,
	LastUpdated       DATETIME            DEFAULT GETDATE()
	CONSTRAINT PK_FactProdPrices PRIMARY KEY(Id)
)ON [PRIMARY]
GO



