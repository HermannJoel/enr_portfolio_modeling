/* dwh tables */
CREATE OR REPLACE WAREHOUSE BLX_WH;
SELECT CURRENT_WAREHOUSE();

CREATE DATABASE DWH;
CREATE SCHEMA DWH.DW;

CREATE OR REPLACE TABLE DWH.DW.DIMDATE(
    ID int IDENTITY(1,1) NOT NULL,
    DateKey int PRIMARY KEY,
	Date date NULL,
	CalenderYear int NULL,
	QuarterNumberOfYear int NULL,
	MonthNumberOfYear int NULL,
	MonthNameOfYear STRING NULL,
	DayNumberOfMonth int NULL,
	DayNameOfWeek STRING NULL,
	DayNumberOfWeek int NULL,
	WeekNumberOfYear int NULL,
	DayNumberOfYear int NULL);

CREATE TABLE DWH.DW.DIMDATE (
    Date date NULL,
	CalenderYear int NULL,
	QuarterNumberOfYear int NULL,
	MonthNumberOfYear int NULL,
	MonthNameOfYear nvarchar(30) NULL,
	DayNumberOfMonth int NULL,
	DayNameOfWeek nvarchar(30) NULL,
	DayNumberOfWeek int NULL,
	WeekNumberOfYear [int NULL,
	DayNumberOfYear int NULL,
	DateKey int NOT NULL,
	ID int IDENTITY(1,1) NOT NULL,
  );

  CREATE TABLE DWH.DW.DIMASSET(
	ID int IDENTITY(1,1) NOT NULL,
	AssetId int NOT NULL PRIMARY KEY,
	ProjectId varchar(100) NOT NULL,
	Project varchar(250) NOT NULL,
	Technology varchar(200) NOT NULL,
	Cod date NOT NULL,
	Mw decimal(5, 2) NOT NULL,
	SuccesPct decimal(7, 5) NOT NULL,
	InstalledPower decimal(7, 5) NOT NULL,
	Eoh decimal(10, 3) NULL,
	DateMerchant date NOT NULL,
	DismentleDate date NULL,
	Repowering varchar(100) NULL,
	DateMsi date NULL,
	InPlanif varchar(50) NOT NULL,
	P50 decimal(10, 3) NULL,
	P90 decimal(10, 3) NULL
)

CREATE TABLE DWH.DW.DIMHEDGE(
	ID int IDENTITY(1,1) NOT NULL,
    AssetKey int NOT NULL,
	HedgeId int NOT NULL PRIMARY KEY ,
	ProjectId varchar(100) NOT NULL,
	TypeHedge varchar(50) NULL,
	ContractStartDate date NOT NULL,
	ContractEndDate date NULL,
	Profil varchar(100) NULL,
	HedgePct decimal(4, 2) NOT NULL,
	Counterparty varchar(100) NULL,
	CountryCounterparty varchar(100) NULL
    );

CREATE TABLE DWH.DW.FACTPRODPRICES(
	ID int IDENTITY(1,1) NOT NULL PRIMARY KEY ,
	Hedgekey int NOT NULL,
	DateKey int NOT NULL,
	ProjetId varchar(100) NULL,
	P50Asset decimal(10, 3) NULL,
	P90Asset decimal(10, 3) NULL,
	P50Hedge decimal(10, 3) NULL,
	P90Hedge decimal(10, 3) NULL,
	ContractPrice decimal(7, 3) NULL,
	SettlementPrice decimal(7, 3) NULL);

/* staging tables */

CREATE TABLE STAGING.STG.DATE (
    Date date NULL,
	CalenderYear int NULL,
	QuarterNumberOfYear int NULL,
	MonthNumberOfYear int NULL,
	MonthNameOfYear nvarchar(30) NULL,
	DayNumberOfMonth int NULL,
	DayNameOfWeek nvarchar(30) NULL,
	DayNumberOfWeek int NULL,
	WeekNumberOfYear [int NULL,
	DayNumberOfYear int NULL,
	DateKey int NOT NULL,
	ID int IDENTITY(1,1) NOT NULL,
  );

CREATE TABLE STAGING.STG.ASSET(
	ID int IDENTITY(1,1) NOT NULL,
	AssetId int NOT NULL PRIMARY KEY,
	ProjectId varchar(100) NOT NULL,
	Project varchar(250) NOT NULL,
	Technology varchar(200) NOT NULL,
	Cod date NOT NULL,
	Mw decimal(5, 2) NOT NULL,
	SuccesPct decimal(7, 5) NOT NULL,
	InstalledPower decimal(7, 5) NOT NULL,
	Eoh decimal(10, 3) NULL,
	DateMerchant date NOT NULL,
	DismentleDate date NULL,
	Repowering varchar(100) NULL,
	DateMsi date NULL,
	InPlanif varchar(50) NOT NULL,
	P50 decimal(10, 3) NULL,
	P90 decimal(10, 3) NULL
);

CREATE TABLE STAGING.STG.HEDGE(
	ID int IDENTITY(1,1) NOT NULL,
    AssetKey int NOT NULL,
	HedgeId int NOT NULL PRIMARY KEY ,
	ProjectId varchar(100) NOT NULL,
	TypeHedge varchar(50) NULL,
	ContractStartDate date NOT NULL,
	ContractEndDate date NULL,
	Profil varchar(100) NULL,
	HedgePct decimal(4, 2) NOT NULL,
	Counterparty varchar(100) NULL,
	CountryCounterparty varchar(100) NULL
    );

CREATE TABLE STAGING.STG.PRODPRICES(
	ID int IDENTITY(1,1) NOT NULL PRIMARY KEY ,
	Hedgekey int NOT NULL,
	DateKey int NOT NULL,
	ProjetId varchar(100) NULL,
	P50Asset decimal(10, 3) NULL,
	P90Asset decimal(10, 3) NULL,
	P50Hedge decimal(10, 3) NULL,
	P90Hedge decimal(10, 3) NULL,
	ContractPrice decimal(7, 3) NULL,
	SettlementPrice decimal(7, 3) NULL);

USE SCHEMA DWH.DW;
 
CREATE STORAGE INTEGRATION GCS_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://mdp-data-lake');


CREATE OR REPLACE FILE format MY_CSV_FORMAT
TYPE = csv
FIELD_DELIMITER= ';'
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null')
EMPTY_FIELD_AS_NULL = true
COMPRESSION= AUTO;

USE SCHEMA DWH.DW;

CREATE OR REPLACE STAGE MDP_DW
  URL = 'gcs://mdp-data-lake'
  STORAGE_INTEGRATION = GCS_INT
  FILE_FORMAT = MY_CSV_FORMAT;