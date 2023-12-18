create role jojo_dataeng 
WITH 
LOGIN
CREATEDB 
CREATEROLE
SUPERUSER;
CREATE USER dwh_writer WITH PASSWORD 'EbaaDataEngWr!t3r10';

GRANT ALL PRIVILEGES
ON DATABASE warehouse TO dwh_writer;
/*
 ***************************************
 ***********     stagging     **********
 ***************************************
*/


/*
 *********** stagging."Asset" **********
*/

DROP TABLE IF EXISTS warehouse.stagging."Asset";
CREATE TABLE warehouse.stagging."Asset"(
	"Id"                     serial                 NOT NULL,
	"AssetId"                INTEGER                 DEFAULT 0 NOT NULL,
	"ProjectId"		       VARCHAR(100)            DEFAULT 'None' NOT NULL, 
    "Project"			       VARCHAR(250)		       DEFAULT 'None' NOT NULL,
	"Technology"		       VARCHAR(200)            DEFAULT 'None' NOT NULL,
	"Cod"				       DATE	                   DEFAULT '1901-01-01' NOT NULL,
	"MW"      		       DECIMAL(5,2)             DEFAULT   0 NOT NULL,
	"SuccessPct"		       DECIMAL(7,5)	            DEFAULT   0 NOT NULL,
	"InstalledPower"	       DECIMAL(7,5)	            DEFAULT   0 NOT NULL,
	"Eoh"				       DECIMAL(10,3)            DEFAULT   0,                            
	"DateMerchant"	       DATE		                DEFAULT '1901-01-01' NOT NULL,
	"DismantleDate"          DATE                     DEFAULT '1901-01-01',
	"Repowering"             VARCHAR(100)             DEFAULT 'None',
	"DateMsi"                DATE                     DEFAULT '1901-01-01',
	"InPlanif"               BOOLEAN,
	"P50"                   DECIMAL(10,3)            DEFAULT   0,
	"P90"                    DECIMAL(10,3)            DEFAULT   0,
	"LastUpdated"            timestamp                default current_timestamp NOT NULL
);

/*
 *********** stagging."Hedge" **********
*/
DROP TABLE IF exists warehouse.stagging."Hedge";
CREATE TABLE warehouse.stagging."Hedge"(
	"Id"                     	SERIAL,
	"HedgeId"				    INT4			    NOT NULL,
    "AssetId"                  INT4                 NOT null,
	"ProjectId"			    	VARCHAR(50)		    DEFAULT 'N/A',
	"Project"			        VARCHAR(250)              DEFAULT 'N/A',
	"Technology"		       	VARCHAR(200)            DEFAULT 'None' NOT NULL,
	"TypeHedge"		        	VARCHAR(50)                     DEFAULT 'N/A',
	"ContractStartDate"       	DATE      DEFAULT '1900-01-01',
	"ContractEndDate"		    DATE       DEFAULT '1900-01-01',
	"DismantleDate"				DATE       DEFAULT '1900-01-01',
	"InstalledPower"	       	DECIMAL(7,5)	            DEFAULT   0 NOT NULL,
	"InPlanif"               	BOOLEAN,
	"Profil"			        VARCHAR(100)		              DEFAULT 'N/A',
	"HedgePct"                	DECIMAL(7, 2)                   DEFAULT   0,
	"Counterparty"            	VARCHAR(100)                 DEFAULT 'N/A',
	"CountryCounterparty"		VARCHAR(100)          DEFAULT 'N/A',
	"DimensionCheckSum"         INT4       NOT NULL          DEFAULT -1,
    "LastUpdated"             	TIMESTAMP  NOT NULL  DEFAULT current_timestamp
);


/*
 *********** stagging."ProdPrices" **********
*/
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'ProdPrices' AND table_schema = 'stagging') THEN
        DROP TABLE "stagging"."ProdPrices";
    END IF;
END $$;

CREATE TABLE "stagging"."ProdPrices" (
    ----"Id" SERIAL PRIMARY KEY,
    "AssetId" INTEGER not null DEFAULT 0,
    "HedgeId" INTEGER not null DEFAULT 0,
    "ProjectId" VARCHAR(100) not null DEFAULT 'None',
    "P50Asset" DECIMAL(10, 3) null DEFAULT 0,
    "P90Asset" DECIMAL(10, 3) null DEFAULT 0,
    "P50Hedge" DECIMAL(10, 3) null DEFAULT 0,
    "P90Hedge" DECIMAL(10, 3) null DEFAULT 0,
    "ContractPrice" DECIMAL(7, 3) null DEFAULT 0,
    "SettlementPrice" DECIMAL(7, 3) null DEFAULT 0,
    "LastUpdated" TIMESTAMP DEFAULT NOW()
);

/*
 *********** stagging."ProductionAsset" **********
*/
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'ProductionAsset' AND table_schema = 'stagging') THEN
        DROP TABLE "stagging"."ProductionAsset";
    END IF;
END $$;
CREATE TABLE "stagging"."ProductionAsset" (
    "AssetId" INTEGER NOT NULL,
    "ProjectId" VARCHAR(50) NOT NULL,
    "DateId"    INT4 not null  default 0,
    "Project" VARCHAR(250) DEFAULT 'N/A',
    "Date" DATE DEFAULT '1901-01-01',
    "Year" INTEGER DEFAULT 0,
    "Quarter" VARCHAR(50) DEFAULT 'N/A',
    "Month" INTEGER DEFAULT 0,
    "P50" DECIMAL(10,5) DEFAULT 0,
    "P90" DECIMAL(10,5) DEFAULT 0
);

/*
 *********** stagging."VolumeHedge" **********
*/
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'VolumeHedge' AND table_schema = 'stagging') THEN
        DROP TABLE "stagging"."VolumeHedge";
    END IF;
END $$;
CREATE TABLE "stagging"."VolumeHedge" (
    "HedgeId" INT4 NOT NULL,
    "ProjectId" VARCHAR(50) NOT NULL,
    "DateId"    INT4  default 0,
    "Project" VARCHAR(250) DEFAULT 'N/A',
    "TypeHedge" VARCHAR(50) DEFAULT 'N/A',
    "Date" DATE DEFAULT '1901-01-01',
    "Year" INT4 DEFAULT 0,
    "Quarter" VARCHAR(50) DEFAULT 'N/A',
    "Month" INT4 DEFAULT 0,
    "P50" DECIMAL(10,5) DEFAULT 0,
    "P90" DECIMAL(10,5) DEFAULT 0
);

/*
 *********** stagging."SettlementPricesCurve" **********
*/
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'SettlementPricesCurve' AND table_schema = 'stagging') THEN
        DROP TABLE "stagging"."SettlementPricesCurve";
    END IF;
END $$;

CREATE TABLE "stagging"."SettlementPricesCurve"( 
	"DeliveryPeriod"     DATE              DEFAULT '1999-01-01' NOT NULL,			
	"SettlementPrice"    DECIMAL(7, 3)     DEFAULT '0',
	"CotationDate"       DATE              DEFAULT '1999-01-01' NOT NULL,
	"CurrentVersion"     BOOLEAN           NOT NULL  DEFAULT true,
	"LastUpdated"        TIMESTAMP         NOT NULL  DEFAULT current_timestamp
);


DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'MarketPrices' AND table_schema = 'stagging') THEN
        DROP TABLE "stagging"."MarketPrices";
    END IF;
END $$;

CREATE TABLE "stagging"."MarketPrices"( 
    "HedgeId"            INT4              DEFAULT 0 NOT NULL,
	"ProjectId"          VARCHAR(50)       DEFAULT 'N/A' NOT NULL,
	"DateId"             INT4              default 0,
	"DeliveryPeriod"     DATE              DEFAULT '1999-01-01' NOT NULL,
	"Year"               INT4              DEFAULT 0,
	"Quarter"		     INT4 	           DEFAULT 0,
	"Month"              INT4              DEFAULT 0,
	"SettlementPrice"    DECIMAL(7, 3)     DEFAULT 0
);

/*
 *********** stagging."ContractPrices" **********
*/
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'ContractPrices' AND table_schema = 'stagging') THEN
        DROP TABLE "stagging"."ContractPrices";
    END IF;
END $$;
CREATE TABLE "stagging"."ContractPrices" (
	"HedgeId" int4 NOT NULL DEFAULT 0,
	"DateId"  int4 NOT NULL DEFAULT 0,
	"ProjectId" VARCHAR(50) DEFAULT 'N/A' NOT NULL,
	"Project" VARCHAR(250) DEFAULT 'N/A',
	"TypeHedge" VARCHAR(250) DEFAULT 'N/A',
	"Date" date NOT NULL DEFAULT '1901-01-01'::date,
	"Year" int4 NOT NULL DEFAULT 0,
	"Quarter" int4 NOT NULL DEFAULT 0,
	"Month" int4 NOT NULL DEFAULT 0,
	"ContractPrice" DECIMAL(7, 3) NULL DEFAULT 0
);

/*
 *********** stagging."MtM" **********
*/

CREATE TABLE "stagging"."MtM" (
	"Id" SERIAL,
	"CotationDate" date NOT NULL DEFAULT '1900-01-01'::date,
	"MtM" decimal(10, 5) NULL DEFAULT 0
);


/*
 ***************************************
 ***********     DWH          **********
 ***************************************
*/

/*
 *********** dwh."DimAsset" **********
*/
DROP TABLE IF EXISTS warehouse.dwh."DimAsset" Cascade;
CREATE TABLE warehouse.dwh."DimAsset"(
	"Id"                     INT4                 NOT null,
	"AssetId"                INT4                 NOT null,
	"ProjectId"		       VARCHAR(100)            DEFAULT 'None' NOT NULL, 
    "Project"			       VARCHAR(250)		       DEFAULT 'None' NOT NULL,
	"Technology"		       VARCHAR(200)            DEFAULT 'None' NOT NULL,
	"Cod"				       DATE	                   DEFAULT '1901-01-01' NOT NULL,
	"MW"      		       DECIMAL(5,2)             DEFAULT   0 NOT NULL,
	"SuccessPct"		       DECIMAL(7,5)	            DEFAULT   0 NOT NULL,
	"InstalledPower"	       DECIMAL(7,5)	            DEFAULT   0 NOT NULL,
	"Eoh"				       DECIMAL(10,3)            DEFAULT   0,                            
	"DateMerchant"	       DATE		                DEFAULT '1901-01-01' NOT NULL,
	"DismantleDate"          DATE                     DEFAULT '1901-01-01',
	"Repowering"             VARCHAR(100)             DEFAULT 'None',
	"DateMsi"                DATE                     DEFAULT '1901-01-01',
	"InPlanif"               BOOLEAN,
	"P50"                   DECIMAL(10,3)            DEFAULT   0,
	"P90"                    DECIMAL(10,3)            DEFAULT   0,
	"LastUpdated"            timestamp                default current_timestamp NOT null,
	CONSTRAINT PK_dwh_DimAsset PRIMARY KEY ("AssetId")
);

DROP SEQUENCE IF exists dwh.dim_asset_seq_id cascade;
CREATE sequence dwh.dim_asset_seq_id
START 1
INCREMENT 1
MINVALUE 1
OWNED BY dwh."DimAsset"."Id";
ALTER TABLE dwh."DimAsset" ALTER COLUMN "Id" SET DEFAULT nextval('dim_asset_seq_id'::regclass);

/*
 *********** dwh."DimHedge" **********
*/
DROP TABLE IF EXISTS warehouse.dwh."DimHedge" cascade;
CREATE TABLE warehouse.dwh."DimHedge"(
	"Id"                     	INT4                 NOT null,
	"HedgeId"				    INT4			     NOT null,
	"AssetId"                   INT4                 NOT null,
	"ProjectId"			    	VARCHAR(50)		     DEFAULT 'N/A',
	"Project"			        VARCHAR(250)         DEFAULT 'N/A',
	"Technology"		       	VARCHAR(200)         DEFAULT 'None' NOT NULL,
	"TypeHedge"		        	VARCHAR(50)          DEFAULT 'N/A',
	"ContractStartDate"       	DATE                 DEFAULT '1900-01-01',
	"ContractEndDate"		    DATE                 DEFAULT '1900-01-01',
	"DismantleDate"				DATE                 DEFAULT '1900-01-01',
	"InstalledPower"	       	DECIMAL(7,5)	     DEFAULT   0 NOT NULL,
	"InPlanif"               	BOOLEAN,
	"Profil"			        VARCHAR(100)		 DEFAULT 'N/A',
	"HedgePct"                	DECIMAL(7, 2)        DEFAULT   0,
	"Counterparty"            	VARCHAR(100)         DEFAULT 'N/A',
	"CountryCounterparty"		VARCHAR(100)         DEFAULT 'N/A',
	"DimensionCheckSum"         INT4       NOT NULL          DEFAULT -1,
	"EffectiveDate"             DATE NOT NULL DEFAULT current_date,
	"EndDate"                   DATE NOT NULL DEFAULT '9999/12/31',
	"CurrentRecord"             BOOLEAN              DEFAULT TRUE,
    "LastUpdated"             	TIMESTAMP  NOT NULL  DEFAULT current_timestamp,
    CONSTRAINT PK_dwh_DimHedge PRIMARY KEY ("HedgeId")
);

DROP SEQUENCE IF exists dwh.dim_hedge_seq_id cascade;
CREATE sequence dwh.dim_hedge_seq_id
START 1
INCREMENT 1
MINVALUE 1
OWNED BY dwh."DimHedge"."Id";
ALTER TABLE dwh."DimHedge" ALTER COLUMN "Id" SET DEFAULT nextval('dim_hedge_seq_id'::regclass);

/*
 *********** dwh."FactProdPrices" **********
*/
DROP TABLE IF EXISTS warehouse.dwh."FactProdPrices" cascade;
CREATE TABLE dwh."FactProdPrices" (
	"Id" INT4 not NULL,
	"HedgeId" INT4 NOT NULL DEFAULT 0,
	"ProjectId" VARCHAR(50) DEFAULT 'N/A' NOT NULL,
	"DateId" INT4 NOT NULL,
	"ContractPrice" DECIMAL(7, 3) NULL,
	"SettlementPrice"    DECIMAL(7, 3) null,
	"P50Asset" DECIMAL(7, 3) NULL,
	"P90Asset" DECIMAL(7, 3) NULL,
	"P50Hedge" DECIMAL(7, 3) NULL,
	"P90Hedge" DECIMAL(7, 3) NULL,
	CONSTRAINT PK_dwh_FactProdPrices primary KEY ("Id")
);

CREATE UNIQUE INDEX CONCURRENTLY fact_prodprices_id 
ON dwh."FactProdPrices" ("Id");
ALTER TABLE dwh."FactProdPrices" 
ADD CONSTRAINT unique_prodprices_id 
UNIQUE USING INDEX fact_prodprices_id;

CREATE UNIQUE INDEX CONCURRENTLY dim_date_id 
ON dwh."DimDate" ("DateKey");
ALTER TABLE dwh."DimDate" 
ADD CONSTRAINT unique_date_id 
UNIQUE USING INDEX dim_date_id;

DROP SEQUENCE IF exists dwh.fact_prodprices_seq_id cascade;
CREATE sequence dwh.fact_prodprices_seq_id
START 1
INCREMENT 1
MINVALUE 1
OWNED BY dwh."FactProdPrices"."Id";
ALTER TABLE dwh."FactProdPrices" ALTER COLUMN "Id" SET DEFAULT nextval('fact_prodprices_seq_id'::regclass);

ALTER table "FactProdPrices"
ADD CONSTRAINT dim_date_fact_prodprices_fk 
FOREIGN KEY ("DateId") REFERENCES "DimDate" ("DateKey")
ON DELETE NO ACTION
ON UPDATE NO ACTION;
  
ALTER TABLE dwh."DimHedge"  ADD CONSTRAINT dim_hedge_dim_asset_fk
FOREIGN KEY ("AssetId")
REFERENCES dwh."DimAsset" ("AssetId")
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER table "FactProdPrices"
ADD CONSTRAINT dim_hedge_fact_prodprices_fk 
FOREIGN KEY ("HedgeId") REFERENCES "DimHedge" ("HedgeId")
ON DELETE NO ACTION
ON UPDATE NO ACTION;


ALTER table "FactProdPrices"
ADD CONSTRAINT dim_date_fact_prodprices_fk 
FOREIGN KEY ("DateId") REFERENCES "DimDate" ("DateKey")
ON DELETE NO ACTION
ON UPDATE NO ACTION;

/*
 *********** dwh."DimDate" **********
*/
DROP TABLE IF EXISTS dwh."DimDate" cascade;
CREATE TABLE dwh."DimDate" (
   "DateKey" INT PRIMARY KEY,
   "Date" DATE NOT NULL,
   "Day" SMALLINT NOT NULL,
   "DaySuffix" CHAR(2) NOT NULL,
   "Weekday" SMALLINT NOT NULL,
   "WeekDayName" VARCHAR(10) NOT NULL,
   "WeekDayName_Short" CHAR(3) NOT NULL,
   "WeekDayName_FirstLetter" CHAR(1) NOT NULL,
   "DOWInMonth" SMALLINT NOT NULL,
   "DayOfYear" SMALLINT NOT NULL,
   "WeekOfMonth" SMALLINT NOT NULL,
   "WeekOfYear" SMALLINT NOT NULL,
   "Month" SMALLINT NOT NULL,
   "MonthName" VARCHAR(10) NOT NULL,
   "MonthName_Short" CHAR(3) NOT NULL,
   "MonthName_FirstLetter" CHAR(1) NOT NULL,
   "Quarter" SMALLINT NOT NULL,
   "QuarterName" VARCHAR(6) NOT NULL,
   "Year" INT NOT NULL,
   "MMYYYY" CHAR(6) NOT NULL,
   "MonthYear" CHAR(7) NOT NULL,
   "IsWeekend" BOOLEAN NOT NULL,
   "IsHoliday" BOOLEAN NOT NULL,
   "FirstDateofYear" DATE NOT NULL,
   "LastDateofYear" DATE NOT NULL,
   "FirstDateofQuater" DATE NOT NULL,
   "LastDateofQuater" DATE NOT NULL,
   "FirstDateofMonth" DATE NOT NULL,
   "LastDateofMonth" DATE NOT NULL,
   "FirstDateofWeek" DATE NOT NULL,
   "LastDateofWeek" DATE NOT NULL
);

-- Populate the DimDate table
DO $$ 
DECLARE 
   CurrentDate DATE := '2020-01-01';
   EndDate DATE := '2030-12-31';
BEGIN
   TRUNCATE TABLE dwh."DimDate";
   WHILE CurrentDate < EndDate LOOP
      INSERT INTO dwh."DimDate" (
         "DateKey",
         "Date",
         "Day",
         "DaySuffix",
         "Weekday",
         "WeekDayName",
         "WeekDayName_Short",
         "WeekDayName_FirstLetter",
         "DOWInMonth",
         "DayOfYear",
         "WeekOfMonth",
         "WeekOfYear",
         "Month",
         "MonthName",
         "MonthName_Short",
         "MonthName_FirstLetter",
         "Quarter",
         "QuarterName",
         "Year",
         "MMYYYY",
         "MonthYear",
         "IsWeekend",
         "IsHoliday",
         "FirstDateofYear",
         "LastDateofYear",
         "FirstDateofQuater",
         "LastDateofQuater",
         "FirstDateofMonth",
         "LastDateofMonth",
         "FirstDateofWeek",
         "LastDateofWeek"
      )
      SELECT 
         EXTRACT(YEAR FROM CurrentDate) * 10000 + EXTRACT(MONTH FROM CurrentDate) * 100 + EXTRACT(DAY FROM CurrentDate),
         CurrentDate,
         EXTRACT(DAY FROM CurrentDate),
         CASE 
            WHEN EXTRACT(DAY FROM CurrentDate) = 1
               OR EXTRACT(DAY FROM CurrentDate) = 21
               OR EXTRACT(DAY FROM CurrentDate) = 31
               THEN 'st'
            WHEN EXTRACT(DAY FROM CurrentDate) = 2
               OR EXTRACT(DAY FROM CurrentDate) = 22
               THEN 'nd'
            WHEN EXTRACT(DAY FROM CurrentDate) = 3
               OR EXTRACT(DAY FROM CurrentDate) = 23
               THEN 'rd'
            ELSE 'th'
         END,
         EXTRACT(ISODOW FROM CurrentDate),
         TO_CHAR(CurrentDate, 'Day'),
         LEFT(UPPER(TO_CHAR(CurrentDate, 'Day')), 3),
         LEFT(TO_CHAR(CurrentDate, 'Day'), 1),
         EXTRACT(DAY FROM CurrentDate),
         cast(TO_CHAR(CurrentDate, 'DDD') as SMALLINT),
         EXTRACT(WEEK FROM CurrentDate) - EXTRACT(WEEK FROM (DATE_TRUNC('MONTH', CurrentDate) + '1 day')::DATE) + 1,
         EXTRACT(WEEK FROM CurrentDate),
         EXTRACT(MONTH FROM CurrentDate),
         TO_CHAR(CurrentDate, 'Month'),
         LEFT(UPPER(TO_CHAR(CurrentDate, 'Month')), 3),
         LEFT(TO_CHAR(CurrentDate, 'Month'), 1),
         EXTRACT(QUARTER FROM CurrentDate),
         CASE 
            WHEN EXTRACT(QUARTER FROM CurrentDate) = 1
               THEN 'First'
            WHEN EXTRACT(QUARTER FROM CurrentDate) = 2
               THEN 'Second'
            WHEN EXTRACT(QUARTER FROM CurrentDate) = 3
               THEN 'Third'
            WHEN EXTRACT(QUARTER FROM CurrentDate) = 4
               THEN 'Fourth'
         END,
         EXTRACT(YEAR FROM CurrentDate),
         TO_CHAR(CurrentDate, 'MMYYYY'),
         TO_CHAR(CurrentDate, 'YYYY') || UPPER(LEFT(TO_CHAR(CurrentDate, 'Month'), 3)),
         CASE 
            WHEN EXTRACT(ISODOW FROM CurrentDate) = 7
               OR EXTRACT(ISODOW FROM CurrentDate) = 6
               THEN TRUE
            ELSE FALSE
         END,
         FALSE,
         TO_DATE(EXTRACT(YEAR FROM CurrentDate) || '-01-01', 'YYYY-MM-DD'),
         TO_DATE(EXTRACT(YEAR FROM CurrentDate) || '-12-31', 'YYYY-MM-DD'),
         DATE_TRUNC('quarter', CurrentDate),
         DATE_TRUNC('quarter', CurrentDate) + INTERVAL '3 months' - INTERVAL '1 day',
         TO_DATE(EXTRACT(YEAR FROM CurrentDate) || '-' || EXTRACT(MONTH FROM CurrentDate) || '-01', 'YYYY-MM-DD'),
         DATE_TRUNC('month', CurrentDate) + INTERVAL '1 month' - INTERVAL '1 day',
         DATE_TRUNC('week', CurrentDate),
         DATE_TRUNC('week', CurrentDate) + INTERVAL '6 days';
      CurrentDate := CurrentDate + INTERVAL '1 day';
   END LOOP;
END $$;


/*
 *********** dwh."I_ContractPrices" **********
*/

DROP TABLE IF EXISTS warehouse.dwh."I_ContractPrices";
CREATE TABLE dwh."I_ContractPrices" (
	"Id" INT4 not NULL,
	"HedgeId" int4 NOT NULL DEFAULT 0,
	"DateId" INT4, ---to_char("Date", 'YYYYMMDD')::integer
	"ProjectId" VARCHAR(50) DEFAULT 'N/A' NOT NULL,
	"Project" VARCHAR(250) DEFAULT 'N/A',
	"TypeHedge" VARCHAR(250) DEFAULT 'N/A',
	"Date" date NOT NULL DEFAULT '1901-01-01'::date,
	"Year" int4 NOT NULL DEFAULT 0,
	"Quarter" int4 NOT NULL DEFAULT 0,
	"Month" int4 NOT NULL DEFAULT 0,
	"ContractPrice" DECIMAL(7, 3) NULL DEFAULT 0,
	CONSTRAINT pk_i_contract_prices PRIMARY KEY ("Id")
);

DROP SEQUENCE IF exists dwh.i_contract_prices_seq_id cascade;
CREATE sequence dwh.i_contract_prices_seq_id
START 1
INCREMENT 1
MINVALUE 1
OWNED BY dwh."I_ContractPrices"."Id";
ALTER TABLE dwh."I_ContractPrices" ALTER COLUMN "Id" SET DEFAULT nextval('i_contract_prices_seq_id'::regclass);

/*
 *********** dwh."I_MarketPrices" **********
*/

DROP TABLE IF EXISTS warehouse.dwh."I_MarketPrices";
CREATE TABLE warehouse.dwh."I_MarketPrices"( 
	"Id" INT4 not null,
	"HedgeId" int4 NOT NULL DEFAULT 0,
	"DateId" INT4 NOT null default 0, ---to_char("DeliveryPeriod", 'YYYYMMDD')::integer 
	"ProjectId" varchar(50) NOT NULL DEFAULT 'N/A'::character varying,
	"DeliveryPeriod" date NOT NULL DEFAULT '1900-01-01'::date,
	"Year" int4 NULL DEFAULT 0,
	"Quarter" int4 NULL DEFAULT 0,
	"Month" int4 NULL DEFAULT 0,
	"SettlementPrice" numeric(7, 3) NULL DEFAULT 0,
	CONSTRAINT pk_i_market_prices PRIMARY KEY ("Id")
);

DROP SEQUENCE IF exists dwh.i_market_prices_seq_id cascade;
CREATE sequence dwh.i_market_prices_seq_id
START 1
INCREMENT 1
MINVALUE 1
OWNED BY dwh."I_MarketPrices"."Id";
ALTER TABLE dwh."I_MarketPrices" ALTER COLUMN "Id" SET DEFAULT nextval('i_market_prices_seq_id'::regclass);

/*
 *********** dwh."I_Asset" **********
*/
DROP TABLE IF EXISTS warehouse.dwh."I_Asset";
CREATE TABLE warehouse.dwh."I_Asset" (
	"Id" INT4 not null,
    "AssetId" INT4 NOT NULL,
    "DateId" INT4 NOT null default 0, ---to_char("Date", 'YYYYMMDD')::integer  
    "ProjectId" VARCHAR(50) NOT NULL,
    "Project" VARCHAR(250) DEFAULT 'N/A',
    "Date" DATE DEFAULT '1901-01-01',
    "Year" INT4 DEFAULT 0,
    "Quarter" VARCHAR(50) DEFAULT 'N/A',
    "Month" INT4 DEFAULT 0,
    "P50A" DECIMAL(10,5) DEFAULT 0,
    "P90A" DECIMAL(10,5) DEFAULT 0,
    CONSTRAINT pk_i_asset PRIMARY KEY ("Id")
);

DROP SEQUENCE IF exists dwh.i_asset_seq_id cascade;
CREATE sequence dwh.i_asset_seq_id
START 1
INCREMENT 1
MINVALUE 1
OWNED BY dwh."I_Asset"."Id";
ALTER TABLE dwh."I_Asset" ALTER COLUMN "Id" SET DEFAULT nextval('i_asset_seq_id'::regclass);

/*
 *********** dwh."I_Hedge" **********
*/
DROP TABLE IF EXISTS warehouse.dwh."I_Hedge";
CREATE TABLE dwh."I_Hedge" (
	"Id" INT4 NOT NULL,
    "HedgeId" INT4 NOT NULL,
    "DateId" INT4 NOT null default 0, ----to_char("Date", 'YYYYMMDD')::integer 
    "ProjectId" VARCHAR(50) NOT NULL,
    "Project" VARCHAR(250) DEFAULT 'N/A',
    "TypeHedge" VARCHAR(50) DEFAULT 'N/A',
    "Date" DATE DEFAULT '1901-01-01',
    "Year" INT4 DEFAULT 0,
    "Quarter" VARCHAR(50) DEFAULT 'N/A',
    "Month" INT4 DEFAULT 0,
    "P50H" DECIMAL(10,5) DEFAULT 0,
    "P90H" DECIMAL(10,5) DEFAULT 0,
    CONSTRAINT pk_i_hedge PRIMARY KEY ("Id")
);
DROP SEQUENCE IF exists dwh.i_hedge_seq_id cascade;
CREATE sequence dwh.i_hedge_seq_id
START 1
INCREMENT 1
MINVALUE 1
OWNED BY dwh."I_Hedge"."Id";
ALTER TABLE dwh."I_Hedge" ALTER COLUMN "Id" SET DEFAULT nextval('i_hedge_seq_id'::regclass);

/*
 *********** dwh."I_MtM" **********
*/
DROP TABLE IF EXISTS warehouse.dwh."I_MtM" cascade;
CREATE TABLE warehouse.dwh."I_MtM" (
	"Id" INT4 not NULL,
	"DateId" INT4 NOT null default 0,  ---to_char("CotationDate", 'YYYYMMDD')::integer 
	"CotationDate" date NULL,
	"MtM" decimal(10, 5) NULL DEFAULT 0,
	CONSTRAINT pk_i_mtm PRIMARY KEY ("Id")
);
DROP SEQUENCE IF exists dwh.i_mtm_seq_id cascade;
CREATE sequence dwh.i_mtm_seq_id
START 1
INCREMENT 1
MINVALUE 1
OWNED BY dwh."I_MtM"."Id";
ALTER TABLE dwh."I_MtM" ALTER COLUMN "Id" SET DEFAULT nextval('i_mtm_seq_id'::regclass);

/*
 *********** dwh."D_Asset" **********
*/
DROP TABLE IF EXISTS warehouse.dwh."D_Asset" Cascade;
CREATE TABLE warehouse.dwh."D_Asset"(
	"Id"                     INT4                 NOT null,
	"AssetId"                INT4                 NOT null,
	"ProjectId"		       VARCHAR(100)            DEFAULT 'None' NOT NULL, 
    "Project"			       VARCHAR(250)		       DEFAULT 'None' NOT NULL,
	"Technology"		       VARCHAR(200)            DEFAULT 'None' NOT NULL,
	"Cod"				       DATE	                   DEFAULT '1901-01-01' NOT NULL,
	"MW"      		       DECIMAL(5,2)             DEFAULT   0 NOT NULL,
	"SuccessPct"		       DECIMAL(7,5)	            DEFAULT   0 NOT NULL,
	"InstalledPower"	       DECIMAL(7,5)	            DEFAULT   0 NOT NULL,
	"Eoh"				       DECIMAL(10,3)            DEFAULT   0,                            
	"DateMerchant"	       DATE		                DEFAULT '1901-01-01' NOT NULL,
	"DismantleDate"          DATE                     DEFAULT '1901-01-01',
	"Repowering"             VARCHAR(100)             DEFAULT 'None',
	"DateMsi"                DATE                     DEFAULT '1901-01-01',
	"InPlanif"               BOOLEAN,
	"P50"                   DECIMAL(10,3)            DEFAULT   0,
	"P90"                    DECIMAL(10,3)            DEFAULT   0,
	"LastUpdated"            timestamp                default current_timestamp NOT null,
	CONSTRAINT pk_d_asset PRIMARY KEY ("AssetId", "ProjectId")
);

DROP SEQUENCE IF exists d_asset_seq_id cascade;
CREATE sequence dwh.d_asset_seq_id
START 1
INCREMENT 1
MINVALUE 1
OWNED BY dwh."D_Asset"."Id";
ALTER TABLE dwh."D_Asset" ALTER COLUMN "Id" SET DEFAULT nextval('d_asset_seq_id'::regclass);

/*
 *********** dwh."D_Hedge" **********
*/
DROP TABLE IF EXISTS warehouse.dwh."D_Hedge" cascade;
CREATE TABLE warehouse.dwh."D_Hedge"(
	"Id"                     	INT4                 NOT null,
	"HedgeId"				    INT4			     NOT null,
	"AssetId"                   INT4                 NOT null,
	"ProjectId"			    	VARCHAR(50)		     DEFAULT 'N/A',
	"Project"			        VARCHAR(250)         DEFAULT 'N/A',
	"Technology"		       	VARCHAR(200)         DEFAULT 'None' NOT NULL,
	"TypeHedge"		        	VARCHAR(50)          DEFAULT 'N/A',
	"ContractStartDate"       	DATE                 DEFAULT '1900-01-01',
	"ContractEndDate"		    DATE                 DEFAULT '1900-01-01',
	"DismantleDate"				DATE                 DEFAULT '1900-01-01',
	"InstalledPower"	       	DECIMAL(7,5)	     DEFAULT   0 NOT NULL,
	"InPlanif"               	BOOLEAN,
	"Profil"			        VARCHAR(100)		 DEFAULT 'N/A',
	"HedgePct"                	DECIMAL(7, 2)        DEFAULT   0,
	"Counterparty"            	VARCHAR(100)         DEFAULT 'N/A',
	"CountryCounterparty"		VARCHAR(100)         DEFAULT 'N/A',
	"DimensionCheckSum"         INT4       NOT NULL          DEFAULT -1,
	"EffectiveDate"             DATE NOT NULL DEFAULT current_date,
	"EndDate"                   DATE NOT NULL DEFAULT '9999/12/31',
	"CurrentRecord"             BOOLEAN              DEFAULT TRUE,
    "LastUpdated"             	TIMESTAMP  NOT NULL  DEFAULT current_timestamp,
    CONSTRAINT pk_D_Hedge PRIMARY KEY ("HedgeId")
);
DROP SEQUENCE IF exists dwh.d_hedge_seq_id cascade;
CREATE sequence dwh.d_hedge_seq_id
START 1
INCREMENT 1
MINVALUE 1
OWNED BY dwh."D_Hedge"."Id";
ALTER TABLE dwh."D_Hedge" ALTER COLUMN "Id" SET DEFAULT nextval('d_hedge_seq_id'::regclass);

 
ALTER TABLE dwh."D_Hedge"  ADD CONSTRAINT fk_d_hedge_d_asset
FOREIGN KEY ("AssetId", "ProjectId")
REFERENCES dwh."D_Asset" ("AssetId", "ProjectId")
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER table dwh."I_ContractPrices"
ADD CONSTRAINT fk_d_hedge_i_contract_prices 
FOREIGN KEY ("HedgeId") REFERENCES dwh."D_Hedge" ("HedgeId")
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER table dwh."I_ContractPrices"
ADD CONSTRAINT fk_d_date_i_contract_prices 
FOREIGN KEY ("DateId") REFERENCES dwh."D_Date" ("DateKey")
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER table dwh."I_MarketPrices"
ADD CONSTRAINT fk_d_date_i_market_prices 
FOREIGN KEY ("DateId") REFERENCES dwh."D_Date" ("DateKey")
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER table dwh."I_Hedge"
ADD CONSTRAINT fk_d_hedge_i_hedge 
FOREIGN KEY ("HedgeId") REFERENCES "D_Hedge" ("HedgeId")
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER table dwh."I_Hedge"
ADD CONSTRAINT fk_d_date_i_hedge 
FOREIGN KEY ("DateId") REFERENCES "D_Date" ("DateKey")
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER table dwh."I_Asset"
ADD CONSTRAINT fk_d_asset_i_asset 
FOREIGN KEY ("AssetId") REFERENCES "D_Asset" ("AssetId")
ON DELETE NO ACTION
ON UPDATE NO ACTION;

ALTER table dwh."I_Asset"
ADD CONSTRAINT fk_d_date_i_asset 
FOREIGN KEY ("DateId") REFERENCES dwh."D_Date" ("DateKey")
ON DELETE NO ACTION
ON UPDATE NO ACTION;

/*
 *********** dwh."D_Date" **********
*/
DROP TABLE IF EXISTS dwh."D_Date" cascade;
CREATE TABLE dwh."D_Date" (
   "DateKey" INT PRIMARY KEY,
   "Date" DATE NOT NULL,
   "Day" SMALLINT NOT NULL,
   "DaySuffix" CHAR(2) NOT NULL,
   "Weekday" SMALLINT NOT NULL,
   "WeekDayName" VARCHAR(10) NOT NULL,
   "WeekDayName_Short" CHAR(3) NOT NULL,
   "WeekDayName_FirstLetter" CHAR(1) NOT NULL,
   "DOWInMonth" SMALLINT NOT NULL,
   "DayOfYear" SMALLINT NOT NULL,
   "WeekOfMonth" SMALLINT NOT NULL,
   "WeekOfYear" SMALLINT NOT NULL,
   "Month" SMALLINT NOT NULL,
   "MonthName" VARCHAR(10) NOT NULL,
   "MonthName_Short" CHAR(3) NOT NULL,
   "MonthName_FirstLetter" CHAR(1) NOT NULL,
   "Quarter" SMALLINT NOT NULL,
   "QuarterName" VARCHAR(6) NOT NULL,
   "Year" INT NOT NULL,
   "MMYYYY" CHAR(6) NOT NULL,
   "MonthYear" CHAR(7) NOT NULL,
   "IsWeekend" BOOLEAN NOT NULL,
   "IsHoliday" BOOLEAN NOT NULL,
   "FirstDateofYear" DATE NOT NULL,
   "LastDateofYear" DATE NOT NULL,
   "FirstDateofQuater" DATE NOT NULL,
   "LastDateofQuater" DATE NOT NULL,
   "FirstDateofMonth" DATE NOT NULL,
   "LastDateofMonth" DATE NOT NULL,
   "FirstDateofWeek" DATE NOT NULL,
   "LastDateofWeek" DATE NOT NULL
);

-- Populate the DimDate table
DO $$ 
DECLARE 
   CurrentDate DATE := '2020-01-01';
   EndDate DATE := '2030-12-31';
BEGIN
   TRUNCATE TABLE dwh."D_Date";
   WHILE CurrentDate < EndDate LOOP
      INSERT INTO dwh."D_Date" (
         "DateKey",
         "Date",
         "Day",
         "DaySuffix",
         "Weekday",
         "WeekDayName",
         "WeekDayName_Short",
         "WeekDayName_FirstLetter",
         "DOWInMonth",
         "DayOfYear",
         "WeekOfMonth",
         "WeekOfYear",
         "Month",
         "MonthName",
         "MonthName_Short",
         "MonthName_FirstLetter",
         "Quarter",
         "QuarterName",
         "Year",
         "MMYYYY",
         "MonthYear",
         "IsWeekend",
         "IsHoliday",
         "FirstDateofYear",
         "LastDateofYear",
         "FirstDateofQuater",
         "LastDateofQuater",
         "FirstDateofMonth",
         "LastDateofMonth",
         "FirstDateofWeek",
         "LastDateofWeek"
      )
      SELECT 
         EXTRACT(YEAR FROM CurrentDate) * 10000 + EXTRACT(MONTH FROM CurrentDate) * 100 + EXTRACT(DAY FROM CurrentDate),
         CurrentDate,
         EXTRACT(DAY FROM CurrentDate),
         CASE 
            WHEN EXTRACT(DAY FROM CurrentDate) = 1
               OR EXTRACT(DAY FROM CurrentDate) = 21
               OR EXTRACT(DAY FROM CurrentDate) = 31
               THEN 'st'
            WHEN EXTRACT(DAY FROM CurrentDate) = 2
               OR EXTRACT(DAY FROM CurrentDate) = 22
               THEN 'nd'
            WHEN EXTRACT(DAY FROM CurrentDate) = 3
               OR EXTRACT(DAY FROM CurrentDate) = 23
               THEN 'rd'
            ELSE 'th'
         END,
         EXTRACT(ISODOW FROM CurrentDate),
         TO_CHAR(CurrentDate, 'Day'),
         LEFT(UPPER(TO_CHAR(CurrentDate, 'Day')), 3),
         LEFT(TO_CHAR(CurrentDate, 'Day'), 1),
         EXTRACT(DAY FROM CurrentDate),
         cast(TO_CHAR(CurrentDate, 'DDD') as SMALLINT),
         EXTRACT(WEEK FROM CurrentDate) - EXTRACT(WEEK FROM (DATE_TRUNC('MONTH', CurrentDate) + '1 day')::DATE) + 1,
         EXTRACT(WEEK FROM CurrentDate),
         EXTRACT(MONTH FROM CurrentDate),
         TO_CHAR(CurrentDate, 'Month'),
         LEFT(UPPER(TO_CHAR(CurrentDate, 'Month')), 3),
         LEFT(TO_CHAR(CurrentDate, 'Month'), 1),
         EXTRACT(QUARTER FROM CurrentDate),
         CASE 
            WHEN EXTRACT(QUARTER FROM CurrentDate) = 1
               THEN 'First'
            WHEN EXTRACT(QUARTER FROM CurrentDate) = 2
               THEN 'Second'
            WHEN EXTRACT(QUARTER FROM CurrentDate) = 3
               THEN 'Third'
            WHEN EXTRACT(QUARTER FROM CurrentDate) = 4
               THEN 'Fourth'
         END,
         EXTRACT(YEAR FROM CurrentDate),
         TO_CHAR(CurrentDate, 'MMYYYY'),
         TO_CHAR(CurrentDate, 'YYYY') || UPPER(LEFT(TO_CHAR(CurrentDate, 'Month'), 3)),
         CASE 
            WHEN EXTRACT(ISODOW FROM CurrentDate) = 7
               OR EXTRACT(ISODOW FROM CurrentDate) = 6
               THEN TRUE
            ELSE FALSE
         END,
         FALSE,
         TO_DATE(EXTRACT(YEAR FROM CurrentDate) || '-01-01', 'YYYY-MM-DD'),
         TO_DATE(EXTRACT(YEAR FROM CurrentDate) || '-12-31', 'YYYY-MM-DD'),
         DATE_TRUNC('quarter', CurrentDate),
         DATE_TRUNC('quarter', CurrentDate) + INTERVAL '3 months' - INTERVAL '1 day',
         TO_DATE(EXTRACT(YEAR FROM CurrentDate) || '-' || EXTRACT(MONTH FROM CurrentDate) || '-01', 'YYYY-MM-DD'),
         DATE_TRUNC('month', CurrentDate) + INTERVAL '1 month' - INTERVAL '1 day',
         DATE_TRUNC('week', CurrentDate),
         DATE_TRUNC('week', CurrentDate) + INTERVAL '6 days';
      CurrentDate := CurrentDate + INTERVAL '1 day';
   END LOOP;
END $$;

ALTER table "I_MtM"
ADD CONSTRAINT fk_d_date_i_mtm
FOREIGN KEY ("DateId") REFERENCES "D_Date" ("DateKey")
ON DELETE NO ACTION
ON UPDATE NO ACTION;

select current_user;

delete from dwh."I_Asset" a
where exists (select 'fait present dans la table de stagging' from stagging.ProdAsset pa where pa.AssetId=a.AssetId);
commit;