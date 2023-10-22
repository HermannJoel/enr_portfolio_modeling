/*===============================================
======    script création Tables           ======
======    Date de création : 22/04/2022    ======                     
=================================================*/

/*===============================================
====            Table Assets                 ==== 
================================================*/
USE StarDust;

DROP TABLE asset;
CREATE TABLE asset(

	rw_id				   INTEGER        		    NOT NULL,
	asset_id               INTEGER                  NOT NULL,
	projet_id		       VARCHAR(100)				NOT NULL, 
    projet			       VARCHAR(250)		   DEFAULT 'N/A',
	technologie		       VARCHAR(50)		   DEFAULT 'N/A',
	cod				       DATE				   DEFAULT 'N/A',
	mw      		       NUMERIC(10,5)	   DEFAULT 'N/A',
	taux_succès		       NUMERIC(10,5)	   DEFAULT 'N/A',
	puissance_installée	   NUMERIC(10,5)	   DEFAULT 'N/A',
	eoh				       NUMERIC(10,5)       DEFAULT 'N/A',                            
	date_merchant	       DATE				   DEFAULT 'N/A',
	date_dementelement     DATE                DEFAULT 'N/A',
	repowering             VARCHAR(100)        DEFAULT 'N/A',
	date_msi               DATE                DEFAULT 'N/A',
	en_planif              VARCHAR(50)         DEFAULT 'N/A',
	p50                    NUMERIC(10,5)       DEFAULT '0',
	p90                    NUMERIC(10,5)       DEFAULT '0'
	---CONSTRAINT PK_asset PRIMARY KEY(asset_id)
);

BULK INSERT asset
FROM 'C:/Users/hermann.ngayap/Desktop/data/asset.csv'
WITH
(
    FIRSTROW = 2, -- as 1st one is header
    ROWTERMINATOR = '\n'   --Use to shift the control to next row
	---FIELDTERMINATOR = ',',  --CSV field delimiter
);

DROP TABLE hedge;
CREATE TABLE hedge(

	rw_id				   INTEGER        		    NOT NULL,
	hedge_id               INTEGER                  NOT NULL,
	projet_id		       VARCHAR(50)				NOT NULL, 
    projet			       VARCHAR(250)		   DEFAULT 'N/A',
	technologie		       VARCHAR(100)		   DEFAULT 'N/A',
	type_hedge             VARCHAR(50)         DEFAULT 'N/A',
	date_debut			   DATE				   DEFAULT 'N/A',
	date_fin               DATE                DEFAULT 'N/A',
	date_dementelement     DATE                DEFAULT 'N/A',
	puissance_installée    NUMERIC(7,5)	   DEFAULT 'N/A',
	profil		           VARCHAR(100)	       DEFAULT 'N/A',
	pct_couverture	       NUMERIC(5,2)	   DEFAULT 'N/A',
	contrepartie	       VARCHAR(100)        DEFAULT 'N/A',                            
	pays_contrepartie	   VARCHAR(100)		   DEFAULT 'N/A',
	en_planif              VARCHAR(50)         DEFAULT 'N/A'
	---CONSTRAINT PK_hedge PRIMARY KEY(hedge_id)
);
-----------------------------------
--------Table p50_p90 asset-------- 
-----------------------------------
DROP TABLE p50_p90_asset;
CREATE TABLE p50_p90_asset(

	surr_id				   INTEGER        		    NOT NULL,
	asset_id               INTEGER                  NOT NULL,
	projet_id		       VARCHAR(50)				NOT NULL, 
    projet			       VARCHAR(250)		   DEFAULT 'N/A',
	thedates               DATE                DEFAULT 'N/A',
	année		       	   INTEGER             DEFAULT '0',
	trimestre              VARCHAR(50)         DEFAULT 'N/A',
	mois			       INTEGER 			   DEFAULT '0',
	p50     		       NUMERIC(10,5)	   DEFAULT '0',
	p90		               NUMERIC(10,5)	   DEFAULT '0'
	CONSTRAINT PK_p50_p90_asset PRIMARY KEY(surr_id)
);
 
DROP TABLE p50_p90_hedge;
CREATE TABLE p50_p90_hedge(

	rw_id				   INTEGER        		    NOT NULL,
	hedge_id               INTEGER                  NOT NULL,
	projet_id		       VARCHAR(50)				NOT NULL, 
    projet			       VARCHAR(250)		   DEFAULT 'N/A',
	type_hedge             VARCHAR(50)         DEFAULT 'N/A',
	année		       	   INTEGER             DEFAULT '0',
	trimestre              VARCHAR(50)         DEFAULT 'N/A',
	mois			       INTEGER 			   DEFAULT '0',
	p50     		       NUMERIC(10,5)	   DEFAULT 'N/A',
	p90		               NUMERIC(10,5)	   DEFAULT 'N/A'
	---CONSTRAINT PK_p50_p90_hedge PRIMARY KEY(hedge_id)
);

------------------------------------
----       Table Prices         ----
------------------------------------

DROP TABLE prices;
CREATE TABLE prices(
	surr_id			INTEGER,
	hedge_id		INTEGER DEFAULT '0' NOT NULL,
	projet_id		VARCHAR(50)  DEFAULT 'N/A' NOT NULL,
	projet          VARCHAR(250) DEFAULT 'N/A' NOT NULL,
	dates           DATE                DEFAULT '1901-01-01',
	année			INTEGER			    DEFAULT '0',
	trimestre		VARCHAR(10)			DEFAULT '0',
	mois			INTEGER			    DEFAULT '0',
	prix			NUMERIC(7, 3)       DEFAULT '0',
	CONSTRAINT PK_prices PRIMARY KEY(surr_id)
);





DROP TABLE IF EXISTS sous_jacents;
CREATE TABLE sous_jacents(

	projet_id             VARCHAR(50)		    NOT NULL,
	electricité         NUMERIC(10, 5)      DEFAULT '0', 
	garantie_origine    NUMERIC(10, 5)      DEFAULT '0',
	garantie_capacité   NUMERIC(10, 5)      DEFAULT '0',
	CONSTRAINT PK_sous_jacents PRIMARY KEY(projet_id)
);
 
ALTER TABLE asset
	ADD CONSTRAINT FK_asset_RELATION_p50_p90_asset FOREIGN KEY (projet_id)
		REFERENCES p50_p90_asset (projet_id);
 
ALTER TABLE hedge
	ADD CONSTRAINT FK_hedge_RELATION_p50_p90_hedge FOREIGN KEY (projet_id)
		REFERENCES p50_p90_hedge (projet_id);

ALTER TABLE [dbo].[p50_p90_asset]
DROP CONSTRAINT FK_asset_RELATION_p50_sous_jacents;----DROP INDEX;

ALTER TABLE hedge
	ADD CONSTRAINT FK_hedge_RELATION_p50_hedge_2022 FOREIGN KEY (parc_id)
		REFERENCES p50_hedge_2022 (parc_id);

ALTER TABLE hedge
	ADD CONSTRAINT FK_hedge_RELATION_p90_hedge_2022 FOREIGN KEY (parc_id)
		REFERENCES p90_hedge_2022 (parc_id);

CREATE INDEX FK_asset_rw_id ON asset (
	rw_id ASC
);

------------------------
----  Table Hedge   ---- 
------------------------

DROP TABLE IF EXISTS hedge;
CREATE TABLE hedge(

	id					INTEGER                  NOT NULL,
	parc_id				VARCHAR(50)				 NOT NULL,
	projet			    VARCHAR(250)		DEFAULT 'N/A',
	type_hedge			VARCHAR(50)         DEFAULT 'N/A', 
	date_debut		    DATE  DEFAULT        '1900-01-01',
	date_fin		    DATE  DEFAULT        '1900-01-01',
	prix_strike		    NUMERIC(25, 10)     DEFAULT   '0',
	profil			    VARCHAR(250)		DEFAULT 'N/A',
	puissance_installée NUMERIC(25, 10)     DEFAULT   '0',
	pct_couverture      NUMERIC(10, 5)     DEFAULT   '0',
	contrepartie		VARCHAR(250)        DEFAULT 'N/A',
	pays_contrepartie	VARCHAR(200)        DEFAULT 'N/A',
	CONSTRAINT PK_hedge PRIMARY KEY(id, parc_id)
);


BULK INSERT production_asset
FROM 'C:\Users\hermann.ngayap\Desktop\data\production_asset.csv'
WITH
(
    FIRSTROW = 2, -- as 1st one is header
    ROWTERMINATOR = '\n',   --Use to shift the control to next row
	FIELDTERMINATOR = ','  --CSV field delimiter
);

DROP TABLE IF EXISTS production_hedge;
CREATE TABLE production_hedge(
	parc_id VARCHAR(50) NULL, 
	projet  VARCHAR(250) NULL,
	année INTEGER DEFAULT 'N/A',
	mois VARCHAR DEFAULT 'N/A',
    p50 NUMERIC(15, 5),
	p90 NUMERIC(15, 5)
);

DROP TABLE IF EXISTS sj_électricité;
CREATE TABLE sj_électricité(
	parc_id VARCHAR(50) NULL, 
	projet  VARCHAR(250) NULL,
	dates DATE DEFAULT '1900-01-01',
    electricité NUMERIC(15, 5)
);

DROP TABLE IF EXISTS sj_garantie_origine;
CREATE TABLE sj_garantie_origine(
	parc_id VARCHAR(50) NULL, 
	projet  VARCHAR(250) NULL,
	dates DATE DEFAULT '1900-01-01',
    garantie_origine NUMERIC(15, 5)
);

DROP TABLE IF EXISTS sj_garantie_capacité;
CREATE TABLE sj_garantie_capacité(
	parc_id VARCHAR(50)    NULL, 
	projet  VARCHAR(250)   NULL,
	dates DATE DEFAULT '1900-01-01',
	année INTEGER DEFAULT '0',
	mois INTEGER DEFAULT '0',
    garantie_capacité NUMERIC(15, 5)
);


----sp_rename 'dates.dates', 'date_id', 'COLUMN'; #change column name


/*
DECLARE @parc_names TABLE
(
    parc_id VARCHAR(50)
);
INSERT INTO @parc_names (parc_id)
SELECT parc_id FROM parc_names;

DECLARE @Sql nvarchar(max) = 'ALTER TABLE price ADD COLUMN ('
SELECT @Sql = @Sql + parc_id +' numeric,'
FROM @parc_names 

SET @Sql = LEFT(@Sql, LEN(@Sql) - 1) +');'
EXEC(@sql)
*/

SELECT * FROM asset;
SELECT * FROM [dbo].p50_asset_2022;

INSERT INTO [dbo].asset (id, parc_id, projet)
VALUES 
(1, 'AVI1', 'Avignonet', 17000000.0000),
(2, 'AVI2', 'AvignonetII', 10600000.0000),
(3, 'CHEP', 'Chépy', 5955807.0000);

INSERT INTO [dbo].asset (parc_id, )
VALUES 
('AVI1', 17000000.0000),
('AVI2', 10600000.0000),
('CHEP', 5955807.0000);

SELECT * FROM p50_p90_asset;
SELECT * FROM asset;

SELECT a.projet, a.cod, a.mw, a.date_merchant, b.projet, b.année, b.mois, b.p_50, b.p_90  
FROM [dbo].asset AS a 
LEFT JOIN [dbo].p50_p90_asset AS b 
ON a.projet_id = b.projet_id 
WHERE b.année = '2022' AND b.mois = '1';

ALTER TABLE asset 
ADD planif VARCHAR(50);

ALTER TABLE asset
ALTER COLUMN date_msi DATE;


SELECT 
    CASE 
        WHEN 
            ISNUMERIC(p_50 + 'e0') = 1 THEN CAST(p_50 AS decimal(25,5))
        ELSE NULL END AS np_50 
FROM 
	p50_p90_asset;


     
 SELECT a.projet_id, a.projet, a.technologie, a.cod, a.puissance_installée, 
 a.date_merchant, a.date_dementelement, a.repowering, 
 b.projet, b.année, b.mois, b.p_50, b.p_90  
FROM [dbo].asset AS a 
INNER JOIN [dbo].p50_p90_asset AS b 
ON a.projet_id = b.projet_id 
WHERE b.année = '2023'; 

SELECT a.projet_id, a.projet, a.technologie,a.cod, a.puissance_installée, 
 a.date_merchant, a.date_dementelement, a.repowering,b.projet_id, 
 b.projet, b.année, b.mois, b.p_50, b.p_90 
 FROM [dbo].asset AS a 
INNER JOIN [dbo].p50_p90_asset AS b 
ON a.projet_id = b.projet_id 
WHERE a.projet_id = 'CLLC'; 

SELECT SUM(p_50) FROM p50_p90_asset WHERE projet_id = 'MADD' AND année = 2026;
SELECT * FROM asset WHERE repowering = 'Oui';

DELETE FROM asset WHERE projet_id = 'REREC' AND projet = 'Remise Reclainville';

DELETE FROM p50_p90_asset WHERE projet_id = 'EVJO' AND projet = 'Evits et Josaphats';

/*
=================================================
====== To alter and update tables         =======
=================================================
*/
ALTER TABLE asset
ALTER COLUMN date_dementelement DATE;

UPDATE p50_p90_asset
SET p_50 = 1552.67983012143 
WHERE id = 1379 AND projet_id = 'AVI1' AND projet = 'Avignonet I' AND année = 2023 AND mois = 2;

UPDATE p50_p90_asset
SET p_90 = 1443.07890093639 
WHERE id = 1379 AND projet_id = 'AVI1' AND projet = 'Avignonet I' AND année = 2023 AND mois = 2;


ALTER TABLE asset_1 
ADD DEFAULT 'n/a' FOR repowering;

/*
    To delete constraint
*/
EXECUTE [dbo].[sp_helpconstraint] 'asset';---To return all contrains names

ALTER TABLE asset DROP CONSTRAINT PK_asset;---To drop a given constraint

DELETE  
FROM p50_p90_asset
WHERE projet_id = 'STAR' AND projet LIKE 'Stockage de l%'; 
/*
*/

SELECT * FROM asset WHERE projet LIKE 'Christophe%';

SELECT * FROM p50_p90_asset WHERE projet LIKE 'La Grand%';
SELECT SUM(p_50) FROM p50_p90_asset 
WHERE projet_id = 'HDC3' AND année = 2022; 


 SELECT a.asset_id, a.projet_id, a.projet, a.technologie, a.cod, a.puissance_installée, 
 a.date_merchant, a.date_dementelement, a.repowering, a.en_planif, b.projet_id,
 b.projet, b.année, b.mois, b.p50, b.p90  
FROM [dbo].asset AS a 
INNER JOIN [dbo].p50_p90_asset AS b 
ON a.projet_id = b.projet_id;

SELECT DISTINCT projet_id AS nbre_parcs_asset FROM asset ORDER BY nbre_parcs_asset ASC;
SELECT COUNT(DISTINCT projet_id) AS nbre_parcs_asset FROM asset ;

SELECT DISTINCT projet_id AS nbre_parcs_p50_p90 FROM p50_p90_asset ORDER BY nbre_parcs_p50_p90 ASC;
---SELECT COUNT(DISTINCT projet_id) AS nbre_parcs_p50_p90 FROM p50_p90_asset;

 SELECT a.projet_id, a.projet, a.technologie, a.cod, a.puissance_installée, 
 a.date_merchant, a.date_dementelement, a.repowering, a.en_planif, b.projet_id,
 b.projet, b.année, b.mois, b.p_50, b.p_90  
FROM [dbo].asset AS a 
INNER JOIN [dbo].p50_p90_asset AS b 
ON a.projet_id = b.projet_id;


/*
delete peyrs from asset
delete peyrs from p50_p90_asset
delete RMDB from asset and p50_p90_asset,
delete BLEL, BOUG, CDLV, CLBA, CLLC, EVJO, LAGBB, LBOU, RREC
delete bourgainvile BOUR asset planif & p50_p90 
*/

SELECT DISTINCT projet_id 
FROM   p50_p90_asset
WHERE  projet_id NOT IN (
   SELECT DISTINCT projet_id
   FROM asset  
   );


SELECT SUM(ATOT.ASUM) - SUM(HTOT.HSUM) AS NON_HEDGER, a.projet_id, a.année, a.mois, h.projet_id, h.année, h.mois FROM p50_p90_asset AS a
INNER JOIN 
(SELECT SUM(a.p50) AS ASUM, a.projet_id, a.année, a.mois FROM p50_p90_asset AS a GROUP BY a.année, a.projet_id) AS ATOT
ON ATOT.projet_id = a.projet_id

(SELECT SUM(h.p50) AS HSUM, h.projet_id, h.année, h.mois FROM p50_p90_hedge AS h GROUP BY h.année, h.projet_id) AS HTOT
ON HTOT.projet_id = h.projet_id


SELECT a.projet_id, ISNULL(a.p50a, 0) + ISNULL(h.p50h, 0) AS Exposure
FROM (
		SELECT projet_id, ISNULL(SUM(p50), 0) AS p50a
		FROM p50_p90_asset
		WHERE projet_id = 'ALBE'
		AND année= 2022
		GROUP BY projet_id) AS a LEFT JOIN (
										SELECT projet_id, ISNULL(SUM(p50), 0) AS p50h
										FROM p50_p90_hedge
										WHERE projet_id = 'ALBE'
										AND année = 2022
										GROUP BY projet_id
									    ) AS h ON a.projet_id = h.projet_id
										  ORDER BY ASC;

SELECT a.projet_id, a.année, ISNULL(a.p50a, 0) + ISNULL(h.p50h, 0) AS Exposure
FROM (
		SELECT projet_id, année, ISNULL(SUM(p50), 0) AS p50a
		FROM p50_p90_asset
		GROUP BY projet_id, année) AS a INNER JOIN (
										SELECT projet_id, année, ISNULL(SUM(p50), 0) AS p50h
										FROM p50_p90_hedge
										GROUP BY projet_id, année
									    ) AS h ON a.projet_id = h.projet_id
										  ORDER BY projet_id;

SELECT projet_id, année,ISNULL(SUM(p50), 0) AS t_a_annuel
FROM p50_p90_asset
GROUP BY projet_id, année
ORDER BY projet_id;

SELECT projet_id, année, mois,ISNULL(SUM(p50), 0) AS t_a_mensuel
FROM p50_p90_asset
GROUP BY projet_id, année, mois 
ORDER BY projet_id;

SELECT hedge_id, projet_id, année,ISNULL(SUM(p50), 0) AS t_h_annuel
FROM p50_p90_hedge
WHERE hedge_id = 295
GROUP BY hedge_id, projet_id, année
ORDER BY projet_id;

SELECT hedge_id, projet_id, année, mois, ISNULL(SUM(p50), 0) AS t_h_mensuel
FROM p50_p90_hedge
WHERE hedge_id = 294
GROUP BY hedge_id, projet_id, année, mois
ORDER BY projet_id;

/*
============================================
======= To create view p50 annualy   =======
============================================
*/

CREATE VIEW Expostion_p50_année(projet_id, année, Expostion_a) AS
SELECT projet_id, année, ISNULL(SUM(p50), 0) + (SELECT ISNULL(SUM(p50), 0) AS Expostion_a
												FROM p50_p90_hedge AS h
												WHERE a.projet_id = h.projet_id AND a.année=h.année)
FROM p50_p90_asset AS a
GROUP BY projet_id, année
ORDER BY projet_id;


SELECT année, projet_id
	,CAST(ROUND(ISNULL(SUM(p50), 0) / 1000, 3) AS DECIMAL(10, 3)) + ( 
		SELECT CAST(ROUND(ISNULL(SUM(p50), 0) / 1000, 3) AS DECIMAL(10, 3))
		FROM p50_p90_hedge AS h 
		WHERE a.année = h.année 
		) AS Exposition 
		 
FROM p50_p90_asset AS a
WHERE a.année=2022
GROUP BY année, projet_id  
ORDER BY année, projet_id;
/*
==========================================
======= To create view p50 monthly =======
==========================================
*/
CREATE VIEW Expostion_p50_mensuelle(projet_id, année, mois, Expostion_m) AS
SELECT projet_id, année, mois, ISNULL(SUM(p50), 0) + (SELECT ISNULL(SUM(p50), 0) AS Expostion_m
												FROM p50_p90_hedge AS h
												WHERE a.projet_id=h.projet_id AND a.année=h.année AND a.mois=h.mois)
FROM p50_p90_asset AS a
GROUP BY projet_id, année, mois;

SELECT  
    * 
FROM 
    Expostion_p50_mensuelle
ORDER BY 
	projet_id,
	année,
	mois;
/*
==================================================
=============To create view p50 quaterly =========
==================================================
*/

SELECT a.projet_id, a.année, ISNULL(SUM(p50), 0) + (SELECT ISNULL(SUM(p50), 0) AS Exposition_m
												FROM p50_p90_hedge AS h
												WHERE a.projet_id = h.projet_id AND a.année=h.année AND a.projet_id='SE19')
FROM p50_p90_asset AS a
WHERE a.projet_id='SE19'
GROUP BY a.projet_id, a.année;

SELECT projet_id, année, SUM(p50)/1000 AS prod
FROM p50_P90_asset
GROUP BY projet_id, année
ORDER BY projet_id, année;

/*
==================================================
=============To create view p50 quaterly =========
==================================================


/*
==================================================
=============To create view dates tables =========
==================================================
*/

-- prevent set or regional settings from interfering with 
-- interpretation of dates / literals
SET DATEFIRST  7, -- 1 = Monday, 7 = Sunday
    DATEFORMAT mdy, 
    LANGUAGE   US_ENGLISH;
-- assume the above is here in all subsequent code blocks.

DECLARE @StartDate  date = '20220101';

DECLARE @CutoffDate date = DATEADD(MONTH, -1, DATEADD(YEAR, 7, @StartDate));

;WITH seq(n) AS 
(
  SELECT 0 UNION ALL SELECT n + 1 FROM seq
  WHERE n < DATEDIFF(MONTH, @StartDate, @CutoffDate)
),
d(d) AS 
(
  SELECT DATEADD(MONTH, n, @StartDate) FROM seq
)
SELECT d FROM d
ORDER BY d
OPTION (MAXRECURSION 0);

/*
==============================================
====== To create dates dimension table =======
==============================================
*/

DECLARE @StartDate  date = '20220101';

DECLARE @CutoffDate date = DATEADD(MONTH, -1, DATEADD(YEAR, 7, @StartDate));

;WITH seq(n) AS 
(
  SELECT 0 UNION ALL SELECT n + 1 FROM seq
  WHERE n < DATEDIFF(MONTH, @StartDate, @CutoffDate)
),
d(d) AS 
(
  SELECT DATEADD(MONTH, n, @StartDate) FROM seq
),
dim AS
(
  SELECT
    thedates           = CONVERT(date, d),
    ---TheDay          = DATEPART(DAY,       d),
    thedayName         = DATENAME(WEEKDAY,   d),
    ---TheWeek         = DATEPART(WEEK,      d),
    ---TheISOWeek      = DATEPART(ISO_WEEK,  d),
    ---TheDayOfWeek    = DATEPART(WEEKDAY,   d),
    months             = DATEPART(MONTH,     d),
    themonthName       = DATENAME(MONTH,     d),
    quarters           = DATEPART(Quarter,   d),
    years              = DATEPART(YEAR,      d)
    ---TheFirstOfMonth = DATEFROMPARTS(YEAR(d), MONTH(d), 1),
    ---TheLastOfYear   = DATEFROMPARTS(YEAR(d), 12, 31),
    ---TheDayOfYear    = DATEPART(DAYOFYEAR, d)
  FROM d
)
---SELECT * FROM dim
SELECT * INTO dbo.dim_date FROM dim
  ORDER BY thedates
  OPTION (MAXRECURSION 0);

CREATE UNIQUE CLUSTERED INDEX PK_dim_date ON dbo.dim_date(thedates);

DROP TABLE dbo.dim_date;
CREATE TABLE dbo.dim_date
(
  thedates      DATE NOT NULL,
  years         INTEGER DEFAULT '0',
  Quarters      INTEGER DEFAULT '0',
  months        INTEGER DEFAULT '0',
  themonthName  VARCHAR(50) DEFAULT 'N/A',
  thedayName    VARCHAR(50) DEFAULT 'N/A'

  ---CONSTRAINT FK_dim_date FOREIGN KEY(thedates) REFERENCES dbo.p50_p90_asset(thedates)
);



----To index clustered index on prim key
CREATE CLUSTERED INDEX CIX_dim_date ON dbo.dim_date(thedates);
GO

----- To align quarter with fiscal year

  ;WITH d AS (SELECT d FROM 
(
    VALUES('20220101'),
          ('20220401'),
          ('20220701'),
          ('20221001')
    ) AS d(d))
SELECT
  d, 
  StandardQuarter        = DATEPART(QUARTER, d),
  LateFiscalQuarter      = DATEPART(QUARTER, DATEADD(MONTH, -9, d)),
  LateFiscalQuarterYear  = YEAR(DATEADD(MONTH, -9, d)),
  EarlyFiscalQuarter     = DATEPART(QUARTER, DATEADD(MONTH,  3, d)),
  EarlyFiscalQuarterYear = YEAR(DATEADD(MONTH,  3, d))
FROM q;

