import pandas as pd
import sys
import os
import configparser
from datetime import datetime
sys.path.append('/mnt/d/local-repo-github/enr_portfolio_modeling/')
os.chdir('/mnt/d/local-repo-github/enr_portfolio_modeling/')
from src.utils.functions import*
from etl import*

#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'Config/config.ini') 
config=configparser.ConfigParser(allow_no_value=True)
config.read(config_file)


mssqluid = os.path.join(os.path.dirname("__file__"),config['develop']['mssqluid'])
mssqlserver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
msqsldriver = os.path.join(os.path.dirname("__file__"),config['develop']['mssqlserver'])
mssqldwhdb = os.path.join(os.path.dirname("__file__"),config['develop']['mssqldb'])
pgpw=os.path.join(os.path.dirname("__file__"),config['develop']['pgpw'])
pguid=os.path.join(os.path.dirname("__file__"),config['develop']['pguid'])
pgserver=os.path.join(os.path.dirname("__file__"),config['develop']['pgserver'])
pgdwhdb=os.path.join(os.path.dirname("__file__"),config['develop']['pgdwhdb'])
pgport=os.path.join(os.path.dirname("__file__"),config['develop']['pgport'])


#************1.Exposition per year
exposure_y_query = '''SELECT a."Year" 
,CAST(ROUND(COALESCE(SUM(a."P50A"), 0) / 1000, 3) AS DECIMAL(10, 3)) + ( 
        SELECT CAST(ROUND(COALESCE(SUM(h."P50H"), 0) / 1000, 3) AS DECIMAL(10, 3))
        FROM dwh."I_Hedge" AS h 
        WHERE a."Year" = h."Year" 
        ) AS "Exposure" 
FROM dwh."I_Asset" AS a 
GROUP BY a."Year" 
ORDER BY a."Year";'''
exposure_y=query_data_from_postgresql(query=exposure_y_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
#************2.Exposition per quarter per year
exposure_q_query ="""
            SELECT 
            a."Year" 
            ,a."Quarter" 
            ,CASE
                WHEN LEFT(a."Quarter", 2) = 'Q1'
                    THEN 'Q1' 
                WHEN LEFT(a."Quarter", 2) = 'Q2' 
                    THEN 'Q2' 
                WHEN LEFT(a."Quarter", 2) = 'Q3' 
                    THEN 'Q3' 
                WHEN LEFT(a."Quarter", 2) = 'Q4' 
                    THEN 'Q4' 
            END AS "Quarters" 
            ,CAST(ROUND(COALESCE(SUM(a."P50A"), 0) / 1000, 3) AS DECIMAL(10, 3))+ ( 
            SELECT CAST(ROUND(COALESCE(SUM(h."P50H"), 0) / 1000, 3) AS DECIMAL(10,3)) 
            FROM dwh."I_Hedge" AS h 
                WHERE a."Year" = h."Year" AND a."Quarter" = h."Quarter" ) AS "Exposure" 
            FROM dwh."I_Asset" AS a 
            GROUP BY a."Year", a."Quarter" 
            ORDER BY a."Year", a."Quarter"; 
            """
exposure_q=query_data_from_postgresql(query=exposure_q_query, 
                                      pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#************3.Exposition per month per year
exposure_m_query ="""
        SELECT 
            a."Year" 
            ,a."Month"
        ,CASE 
            WHEN a."Month" = 1 
                THEN 'jan'
            WHEN a."Month" = 2
                THEN 'feb' 
            WHEN a."Month" = 3 
                THEN 'mar'
            WHEN a."Month" = 4 
                THEN 'apr' 
            WHEN a."Month" = 5 
                THEN 'may' 
            WHEN a."Month" = 6 
                THEN 'jun' 
            WHEN a."Month" = 7
                THEN 'jul'
            WHEN a."Month" = 8
                THEN 'aug'
            WHEN a."Month" = 9
                THEN 'sep'
            WHEN a."Month" = 10
                THEN 'oct'
            WHEN a."Month" = 11
                THEN 'nov'
            WHEN a."Month" = 12
                THEN 'dec'
        END AS "Months" 
        ,CAST(ROUND(COALESCE(SUM(a."P50A"), 0) / 1000, 3) AS DECIMAL(10, 3))+ ( 
            SELECT CAST(ROUND(COALESCE(SUM(h."P50H"), 0) / 1000, 3) AS DECIMAL(10, 3))
            FROM dwh."I_Hedge" AS h 
            WHERE a."Year" = h."Year" 
            AND a."Month" = h."Month"
            ) AS "Exposure" 
        FROM dwh."I_Asset" AS a 
        GROUP BY a."Year", a."Month" 
        ORDER BY a."Year", a."Month";"""
exposure_m=query_data_from_postgresql(query=exposure_m_query, 
                                      pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#************4. Hedge type per year
hedge_y_query ='''
    SELECT h."Year",
        CASE
            WHEN h."TypeHedge" = 'CR16' 
                THEN 'CR' 
            WHEN h."TypeHedge" = 'CR17'
                THEN 'CR' 
            WHEN h."TypeHedge" = 'CR'
                THEN 'CR' 
            WHEN h."TypeHedge" = 'OA'
                THEN 'OA'
            WHEN h."TypeHedge" = 'PPA'
                THEN 'PPA'
            END AS "TypeContract", 
            CAST(ROUND(COALESCE(SUM(- h."P50H"), 0) / 1000, 3) AS DECIMAL(10, 3)) AS "Hedge"
            FROM dwh."I_Hedge" h
            WHERE h."TypeHedge" IS NOT NULL 
            GROUP BY h."Year",
            CASE 
            WHEN h."TypeHedge" = 'CR16'
                THEN 'CR'
            WHEN h."TypeHedge" = 'CR17'
                THEN 'CR'
            WHEN h."TypeHedge" = 'CR'
                THEN 'CR'
            WHEN h."TypeHedge" = 'OA'
                THEN 'OA'
            WHEN h."TypeHedge" = 'PPA'
                THEN 'PPA'
            END
            ORDER BY h."Year", "TypeContract";'''
hedge_y=query_data_from_postgresql(query=hedge_y_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
#************5. Hedge type per quarter
hedge_q_query ='''SELECT h."Year", h."Quarter",
                 CASE WHEN h."TypeHedge" = 'CR16' THEN 'CR'
                      WHEN h."TypeHedge" = 'CR17' THEN 'CR'
                      WHEN h."TypeHedge" = 'CR' THEN 'CR'
                      WHEN h."TypeHedge" = 'OA' THEN 'OA'
                      WHEN h."TypeHedge" = 'PPA' THEN 'PPA'
                 END AS "TypeContract",
                 CASE WHEN LEFT(h."Quarter", 2)='Q1' THEN 'Q1'
                      WHEN LEFT(h."Quarter", 2)='Q2' THEN 'Q2'
                      WHEN LEFT(h."Quarter", 2)='Q3' THEN 'Q3'
                      WHEN LEFT(h."Quarter", 2)='Q4' THEN 'Q4'
                      END AS "Quarters",
                CAST(ROUND(COALESCE(SUM(-h."P50H"), 0)/1000, 3) AS DECIMAL(10, 3)) AS "Hedge"
          FROM dwh."I_Hedge" h
          WHERE h."TypeHedge" IS NOT NULL
          GROUP BY h."Year", h."Quarter",
              CASE WHEN h."TypeHedge"='CR16' THEN 'CR'
                   WHEN h."TypeHedge"='CR17' THEN 'CR'
                   WHEN h."TypeHedge"='CR' THEN 'CR'
                   WHEN h."TypeHedge"='OA' THEN 'OA'
                   WHEN h."TypeHedge"='PPA' THEN 'PPA'
                   END
          ORDER BY h."Year", "Quarters";'''
hedge_q=query_data_from_postgresql(query=hedge_q_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
#***********6.Hedge per month per year
hedge_m_query ='''SELECT h."Year", h."Month",
                 CASE WHEN h."TypeHedge" = 'CR16' THEN 'CR'
                      WHEN h."TypeHedge" = 'CR17' THEN 'CR'
                      WHEN h."TypeHedge" = 'CR' THEN 'CR'
                      WHEN h."TypeHedge" = 'OA' THEN 'OA'
                      WHEN h."TypeHedge" = 'PPA' THEN 'PPA'
                 END AS "TypeContract",
                 CASE WHEN h."Month"=1 THEN 'jan'
                      WHEN h."Month"=2 THEN 'feb'
                      WHEN h."Month"=3 THEN 'mar'
                      WHEN h."Month"=4 THEN 'apr'
                      WHEN h."Month"=5 THEN 'may'
                      WHEN h."Month"=6 THEN 'jun'
                      WHEN h."Month"=7 THEN 'jul'
                      WHEN h."Month"=8 THEN 'aug'
                      WHEN h."Month"=9 THEN 'sep'
                      WHEN h."Month"=10 THEN 'oct'
                      WHEN h."Month"=11 THEN 'nov'
                      WHEN h."Month"=12 THEN 'dec'
                 END AS "Months",
                CAST(ROUND(COALESCE(SUM(-h."P50H"), 0)/1000, 3) AS DECIMAL(10, 3)) AS "Hedge" 
          FROM dwh."I_Hedge" h
          WHERE h."TypeHedge" IS NOT NULL 
          GROUP BY h."Year", h."Month",
              CASE WHEN h."TypeHedge"='CR16' THEN 'CR'
                   WHEN h."TypeHedge"='CR17' THEN 'CR'
                   WHEN h."TypeHedge"='CR' THEN 'CR'
                   WHEN h."TypeHedge"='OA' THEN 'OA'
                   WHEN h."TypeHedge"='PPA' THEN 'PPA'
                   END
          ORDER BY h."Year", h."Month";'''
hedge_m=query_data_from_postgresql(query=hedge_m_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
#*************7.HCR per year
hcr_y_query = '''SELECT "Year", CAST(CAST(ROUND((SELECT COALESCE(SUM(-h."P50H"), 0)
                        FROM dwh."I_Hedge" AS h
                        WHERE a."Year"=h."Year") / COALESCE(SUM(a."P50A"), 0)*100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) ||'%%' AS "HCR"
            FROM dwh."I_Asset" AS a
            GROUP BY "Year"
            ORDER BY "Year";'''
hcr_y=query_data_from_postgresql(query=hcr_y_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
#************8.HCR per quarter
hcr_q_query = '''SELECT "Year", "Quarter", 
                  CASE WHEN LEFT("Quarter", 2) = 'Q1' THEN 'Q1' 
                       WHEN LEFT("Quarter", 2) = 'Q2' THEN 'Q2' 
                       WHEN LEFT("Quarter", 2) = 'Q3' THEN 'Q3' 
                       WHEN LEFT("Quarter", 2) = 'Q4' THEN 'Q4' 
                       END AS "Quarters",
                          CAST(CAST(ROUND((SELECT COALESCE(SUM(-h."P50H"), 0) 
                            FROM dwh."I_Hedge" AS h 
                            WHERE a."Year"=h."Year" AND  a."Quarter"=h."Quarter") / COALESCE(SUM(a."P50A"), 0)*100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS "HCR" 
            FROM dwh."I_Asset" AS a 
            GROUP BY "Year", "Quarter" 
            ORDER BY "Year", "Quarter";'''
hcr_q=query_data_from_postgresql(query=hcr_q_query, 
                                 pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
#***********9.HCR per month
hcr_m_query = '''SELECT "Year", "Month", 
                  CASE WHEN "Month"=1 THEN 'jan' 
                       WHEN "Month"=2 THEN 'feb' 
                       WHEN "Month"=3 THEN 'mar' 
                       WHEN "Month"=4 THEN 'apr' 
                       WHEN "Month"=5 THEN 'may' 
                       WHEN "Month"=6 THEN 'jun' 
                       WHEN "Month"=7 THEN 'jul' 
                       WHEN "Month"=8 THEN 'aug' 
                       WHEN "Month"=9 THEN 'sep'
                       WHEN "Month"=10 THEN 'oct'
                       WHEN "Month"=11 THEN 'nov'
                       WHEN "Month"=12 THEN 'dec'
                  END AS "Months",
                  CAST(CAST(ROUND((SELECT COALESCE(SUM(-h."P50H"), 0)
                  FROM dwh."I_Hedge" AS h
                  WHERE a."Year"=h."Year" AND  a."Month"=h."Month") / COALESCE(SUM(a."P50A"), 0)*100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS "HCR"
            FROM dwh."I_Asset" AS a
            GROUP BY "Year", "Month"
            ORDER BY "Year", "Month";'''
hcr_m=query_data_from_postgresql(query=hcr_m_query, 
                                 pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
#******************************
#********  PRODUCTION  ********
#******************************

#************  Prod per year
prod_y_query = '''SELECT "Year",
                   CAST(ROUND(COALESCE(SUM(a."P50A"), 0)/1000, 3) AS DECIMAL(10, 3)) AS "Prod"
            FROM dwh."I_Asset" AS a
            GROUP BY "Year"
            ORDER BY "Year";'''
prod_y =query_data_from_postgresql(query=prod_y_query, 
                                 pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#***********  Prod per quarter
prod_q_query = '''SELECT "Year", "Quarter",
                   CASE WHEN LEFT("Quarter", 2)='Q1' THEN 'Q1'
                   WHEN LEFT("Quarter", 2)='Q2' THEN 'Q2'
                   WHEN LEFT("Quarter", 2)='Q3' THEN 'Q3'
                   WHEN LEFT("Quarter", 2)='Q4' THEN 'Q4'
                   END AS "Quarters",
                   CAST(ROUND(COALESCE(SUM(a."P50A"), 0)/1000, 3) AS DECIMAL(10, 3)) AS "Prod"
            FROM dwh."I_Asset" a
            GROUP BY "Year", "Quarter"
            ORDER BY "Year", "Quarter";'''
prod_q =query_data_from_postgresql(query=prod_q_query, 
                                 pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)          
#***********  Prod per month
prod_m_query = '''SELECT "Year", "Month",
                   CASE WHEN "Month"=1 THEN 'jan'
                        WHEN "Month"=2 THEN 'feb'
                        WHEN "Month"=3 THEN 'mar'
                        WHEN "Month"=4 THEN 'apr'
                        WHEN "Month"=5 THEN 'may'
                        WHEN "Month"=6 THEN 'jun'
                        WHEN "Month"=7 THEN 'jul'
                        WHEN "Month"=8 THEN 'aug'
                        WHEN "Month"=9 THEN 'sep'
                        WHEN "Month"=10 THEN 'oct'
                        WHEN "Month"=11 THEN 'nov'
                        WHEN "Month"=12 THEN 'dec'
                    END AS "Months",
CAST(ROUND(COALESCE(SUM(a."P50A"), 0)/1000, 3) AS DECIMAL(10, 2)) AS "Prod"
FROM dwh."I_Asset" a
GROUP BY "Year", "Month"
ORDER BY "Year", "Month";'''
prod_m =query_data_from_postgresql(query=prod_m_query, 
                                 pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb) 

#**********  Hedge PPA/year
h_ppa_y_query = '''SELECT "Year" 
    ,CAST(ROUND(SUM(-h."P50H") / 1000,3) AS DECIMAL(10, 3)) AS "PPA" 
FROM dwh."I_Hedge" h 
WHERE h."TypeHedge" = 'PPA' 
GROUP BY "Year" 
ORDER BY "Year";'''                
h_ppa_y =query_data_from_postgresql(query=h_ppa_y_query, 
                                 pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb) 

#**********  hedge PPA/quarter
h_ppa_q_query = '''SELECT "Year" 
        ,"Quarter" 
        ,CASE 
        WHEN LEFT("Quarter", 2) = 'Q1' 
			THEN 'Q1' 
		WHEN LEFT("Quarter", 2) = 'Q2' 
			THEN 'Q2' 
		WHEN LEFT("Quarter", 2) = 'Q3' 
			THEN 'Q3' 
		WHEN LEFT("Quarter", 2) = 'Q4' 
			THEN 'Q4' 
		END AS "Quarters" 
	,CAST(ROUND(SUM(- h."P50H") / 1000,3) AS DECIMAL(10, 3)) AS "PPA" 
FROM dwh."I_Hedge" h 
WHERE h."TypeHedge" = 'PPA' 
GROUP BY "Year","Quarter" 
ORDER BY "Year", "Quarter";'''
h_ppa_q =query_data_from_postgresql(query=h_ppa_q_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb) 

#************  hedge PPA/month
h_ppa_m_query= '''SELECT "Year" 
	,"Month" 
	,CASE
        WHEN "Month" = 1 \
			THEN 'jan' 
		WHEN "Month" = 2 
			THEN 'feb' 
		WHEN "Month" = 3 
			THEN 'mar' 
		WHEN "Month" = 4 
			THEN 'apr' 
		WHEN "Month" = 5 
			THEN 'may' 
		WHEN "Month" = 6 
			THEN 'jun' 
		WHEN "Month" = 7 
			THEN 'jul' 
		WHEN "Month" = 8 
			THEN 'aug' 
		WHEN "Month" = 9 
			THEN 'sep' 
		WHEN "Month" = 10 
			THEN 'oct' 
		WHEN "Month" = 11 
			THEN 'nov' 
		WHEN "Month" = 12 
			THEN 'dec' 
		END AS "Months" 
	,CAST(ROUND(SUM(- h."P50H") / 1000, 3) AS DECIMAL(10, 3)) AS "PPA"
FROM dwh."I_Hedge" h
WHERE h."TypeHedge" = 'PPA'
GROUP BY "Year","Month"
ORDER BY "Year", "Month";'''
h_ppa_m =query_data_from_postgresql(query=h_ppa_m_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
#************Prod Merchant/year
prod_m_y_query= '''SELECT a."Year"
	,( 
		CAST(ROUND(COALESCE(SUM(a."P50A"), 0)/1000, 3) AS DECIMAL(10, 3)) + ( 
			SELECT CAST(ROUND(COALESCE(SUM(h."P50H"), 0)/1000, 3) AS DECIMAL(10, 3)) 
			FROM dwh."I_Hedge" AS h 
			WHERE h."TypeHedge" IN ( 
					'OA' 
					,'CR' 
					,'CR16' 
					,'CR17' 
					) 
				AND a."Year" = h."Year" 
			) 
		) AS "ProdMerchant" 
FROM dwh."I_Asset" AS a 
GROUP BY a."Year" 
ORDER BY a."Year";'''
prod_m_y =query_data_from_postgresql(query=prod_m_y_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
#************Prod Merchant/quarter
prod_m_q_query='''SELECT a."Year", a."Quarter"
	,CASE 
        WHEN LEFT("Quarter", 2) = 'Q1'
			THEN 'Q1'
		WHEN LEFT("Quarter", 2) = 'Q2'
			THEN 'Q2'
		WHEN LEFT("Quarter", 2) = 'Q3'
			THEN 'Q3'
		WHEN LEFT("Quarter", 2) = 'Q4'
			THEN 'Q4'
		END AS "Quarters"
	,(
    CAST(ROUND(COALESCE(SUM(a."P50A"), 0)/1000, 3) AS DECIMAL(10, 3)) + (
    SELECT CAST(ROUND(COALESCE(SUM(h."P50H"), 0)/1000, 3) AS DECIMAL(10, 3))
    FROM dwh."I_Hedge" AS h 
    WHERE h."TypeHedge" IN (
    'OA', 
    'CR',
    'CR16', 
    'CR17') 
    AND a."Year" = h."Year" 
    AND a."Quarter" = h."Quarter")
    ) AS "ProdMerchant" 
FROM dwh."I_Asset" AS a 
GROUP BY a."Year", a."Quarter"
ORDER BY a."Year", a."Quarter";'''
prod_m_q =query_data_from_postgresql(query=prod_m_q_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
#************Prod Merchant/month
prod_m_m_query='''SELECT "Year", "Month"
	   ,CASE
           WHEN "Month" = 1
				THEN 'jan'
			WHEN "Month" = 2
				THEN 'feb'
			WHEN "Month" = 3
				THEN 'mar'
			WHEN "Month" = 4
				THEN 'apr'
			WHEN "Month" = 5
				THEN 'may'
			WHEN "Month" = 6
				THEN 'jun'
			WHEN "Month" = 7
				THEN 'jul'
			WHEN "Month" = 8
				THEN 'aug'
			WHEN "Month" = 9
				THEN 'sep'
			WHEN "Month" = 10
				THEN 'oct'
			WHEN "Month" = 11
				THEN 'nov'
			WHEN "Month" = 12
				THEN 'dec'
			END AS "Months"
	,( 
		CAST(ROUND(COALESCE(SUM(a."P50A"), 0), 3) AS DECIMAL(10, 3))+ ( 
			SELECT CAST(ROUND(COALESCE(SUM(h."P50H"), 0), 3) AS DECIMAL(10, 3))
			FROM dwh."I_Hedge" AS h 
			WHERE h."TypeHedge" IN ( 
					'OA' 
					,'CR' 
					,'CR16' 
					,'CR17' 
					) 
				AND a."Year" = h."Year" AND a."Month" = h."Month" 
			) 
		) / 1000 AS "ProdMerchant"
FROM dwh."I_Asset" AS a 
GROUP BY "Year", "Month" 
ORDER BY "Year", "Month";'''
prod_m_m =query_data_from_postgresql(query=prod_m_m_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#***********Prod Merchant hedged with PPA Coverage Ratio per year
prod_m_hcr_y_query = '''SELECT ppa."Year" 
	,CAST(CAST(ROUND((ppa."PPAYear" / ProdMerchant."ProdMerchantYear") * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS "HCR"
FROM ( 
	SELECT "Year" 
		,SUM(-"P50H") / 1000 AS "PPAYear" 
	FROM dwh."I_Hedge"
	WHERE "TypeHedge" = 'PPA' 
	GROUP BY "Year" 
	) AS "ppa" 
INNER JOIN ( 
	SELECT "Year" 
		,( 
			COALESCE(SUM(a."P50A"), 0) + ( 
				SELECT COALESCE(SUM(h."P50H"), 0) 
				FROM dwh."I_Hedge" AS h 
				WHERE h."TypeHedge" IN ( 
						'OA' 
						,'CR' 
						,'CR16' 
						,'CR17' 
						)
					AND a."Year" = h."Year"
				) 
			) / 1000 AS "ProdMerchantYear" 
	FROM dwh."I_Asset" AS a
	GROUP BY "Year"
	) AS ProdMerchant ON ppa."Year" = ProdMerchant."Year"
ORDER BY "Year";'''
prod_m_hcr_y =query_data_from_postgresql(query=prod_m_hcr_y_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#***********Prod Merchant hedged with PPA Coverage Ratio per quarter
prod_m_hcr_q_query = '''SELECT ppa."Year"
	,ppa."Quarter"
	,ppa."Quarters"
	,CAST(CAST(ROUND((ppa.PPAQtr / ProdMerchant.ProdMerchantQtr) * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS "HCR" 
FROM (
	SELECT "Year"
		,"Quarter"
		,CASE
            WHEN LEFT("Quarter", 2) = 'Q1' 
				THEN 'Q1' 
			WHEN LEFT("Quarter", 2) = 'Q2' 
				THEN 'Q2' 
			WHEN LEFT("Quarter", 2) = 'Q3' 
				THEN 'Q3' 
			WHEN LEFT("Quarter", 2) = 'Q4' 
				THEN 'Q4' 
			END AS "Quarters" 
		,SUM(-"P50H") / 1000 AS PPAQtr 
	FROM dwh."I_Hedge" 
	WHERE "TypeHedge" = 'PPA' 
	GROUP BY "Year", "Quarter" 
	) AS ppa 
INNER JOIN ( 
	SELECT "Year" 
		,"Quarter" 
		,CASE 
            WHEN LEFT("Quarter", 2) = 'Q1' 
				THEN 'Q1' 
			WHEN LEFT("Quarter", 2) = 'Q2' 
				THEN 'Q2' 
			WHEN LEFT("Quarter", 2) = 'Q3' 
				THEN 'Q3' 
			WHEN LEFT("Quarter", 2) = 'Q4' 
				THEN 'Q4' 
			END AS "Quarters" 
		,( 
			COALESCE(SUM("P50A"), 0) + ( 
				SELECT COALESCE(SUM("P50H"), 0) 
				FROM dwh."I_Hedge" AS h 
				WHERE "TypeHedge" IN ( 
						'OA' 
						,'CR' 
						,'CR16' 
						,'CR17' 
						) 
					AND a."Year" = h."Year" 
					AND a."Quarter" = h."Quarter" 
				) 
			) / 1000 AS ProdMerchantQtr 
	FROM dwh."I_Asset" AS a 
	GROUP BY "Year", "Quarter" 
	) AS ProdMerchant ON ppa."Year" = ProdMerchant."Year" 
	AND ppa."Quarter" = ProdMerchant."Quarter" 
ORDER BY "Year", "Quarter";'''
prod_m_hcr_q =query_data_from_postgresql(query=prod_m_hcr_q_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#***********Prod Merchant hedged with PPA Coverage Ratio per month
prod_m_hcr_m_query = '''SELECT ppa."Year" 
	,ppa."Month" 
	,ppa."Months" 
	,CAST(CAST(ROUND((ppa.PPAMth / ProdMerchant.ProdMerchantMth) * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS "HCR"
FROM ( 
	SELECT "Year", "Month"
		,CASE
            WHEN "Month" = 1 
				THEN 'jan' 
			WHEN "Month" = 2 
				THEN 'feb' 
			WHEN "Month" = 3 
				THEN 'mar' 
			WHEN "Month" = 4 
				THEN 'apr' 
			WHEN "Month" = 5 
				THEN 'may' 
			WHEN "Month" = 6 
				THEN 'jun' 
			WHEN "Month" = 7 
				THEN 'jul' 
			WHEN "Month" = 8 
				THEN 'aug' 
			WHEN "Month" = 9 
				THEN 'sep' 
			WHEN "Month" = 10 
				THEN 'oct' 
			WHEN "Month" = 11 
				THEN 'nov' 
			WHEN "Month" = 12 
				THEN 'dec' 
			END AS "Months" 
		,SUM(-"P50H") / 1000 AS PPAMth 
	FROM dwh."I_Hedge" 
	WHERE "TypeHedge" = 'PPA' 
	GROUP BY "Year", "Month"
	) AS ppa 
INNER JOIN ( 
	SELECT "Year", "Month"
		,CASE 
            WHEN "Month" = 1 
				THEN 'jan' 
			WHEN "Month" = 2 
				THEN 'feb' 
			WHEN "Month" = 3 
				THEN 'mar' 
			WHEN "Month" = 4 
				THEN 'apr' 
			WHEN "Month" = 5 
				THEN 'may' 
			WHEN "Month" = 6 
				THEN 'jun' 
			WHEN "Month" = 7 
				THEN 'jul' 
			WHEN "Month" = 8 
				THEN 'aug' 
			WHEN "Month" = 9 
				THEN 'sep' 
			WHEN "Month" = 10 
				THEN 'oct' 
			WHEN "Month" = 11 
				THEN 'nov' 
			WHEN "Month" = 12 
				THEN 'dec' 
			END AS months 
		,( 
			COALESCE(SUM("P50A"), 0) + ( 
				SELECT COALESCE(SUM("P50H"), 0) 
				FROM dwh."I_Hedge" AS h 
				WHERE "TypeHedge" IN ( 
						'OA' 
						,'CR' 
						,'CR16' 
						,'CR17' 
						) 
					AND a."Year" = h."Year" 
					AND a."Month" = h."Month" 
				) 
			) / 1000 AS ProdMerchantMth 
	FROM dwh."I_Asset" AS a 
	GROUP BY "Year", "Month"
	) AS ProdMerchant ON ppa."Year" = ProdMerchant."Year"
    AND ppa."Month" = ProdMerchant."Month"
    ORDER BY "Year", "Month";'''
prod_m_hcr_m =query_data_from_postgresql(query=prod_m_hcr_m_query, 
                                         pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#/***************************************************
#**************  Prod Solar and Eol  ****************
#****************************************************/

#************  Solar/year
prod_sol_y_query ='''SELECT "Year" 
	,CAST(ROUND((COALESCE(SUM("P50A"), 0) / 1000), 3) AS DECIMAL(10, 3)) AS "ProdSolar" 
FROM dwh."I_Asset" 
WHERE "ProjectId" IN ( 
		SELECT DISTINCT ("ProjectId") 
		FROM dwh."D_Asset" 
		WHERE "Technology" = 'solaire' 
		) 
GROUP BY "Year" 
ORDER BY "Year";'''
prod_sol_y =query_data_from_postgresql(query=prod_sol_y_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
#************ wind power/year
prod_wp_y_query='''SELECT "Year"
	,CAST(ROUND((COALESCE(SUM("P50A"), 0) / 1000), 3) AS DECIMAL(10, 3)) AS "ProdWp"
FROM dwh."I_Asset"
WHERE "ProjectId" IN (
		SELECT DISTINCT ("ProjectId") 
		FROM dwh."D_Asset" 
		WHERE "Technology" = 'éolien' 
		) 
GROUP BY "Year" 
ORDER BY "Year";'''
prod_wp_y =query_data_from_postgresql(query=prod_wp_y_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#************** solar/quarter
prod_sol_q_query='''SELECT "Year", "Quarter"
	,CASE \
        WHEN LEFT("Quarter", 2) = 'Q1' 
			THEN 'Q1' 
		WHEN LEFT("Quarter", 2) = 'Q2' 
			THEN 'Q2' 
		WHEN LEFT("Quarter", 2) = 'Q3' 
			THEN 'Q3' 
        WHEN LEFT("Quarter", 2) = 'Q4' 
            THEN 'Q4' 
		END AS "Quarters" 
	,CAST(ROUND((COALESCE(SUM("P50A"), 0) / 1000), 3) AS DECIMAL(10, 3)) AS "ProdSolar" 
FROM dwh."I_Asset"
WHERE "ProjectId" IN ( 
		SELECT DISTINCT ("ProjectId") 
		FROM dwh."D_Asset" 
		WHERE "Technology" = 'solaire' 
		) 
GROUP BY "Year", "Quarter" 
ORDER BY "Year", "Quarter";'''
prod_sol_q =query_data_from_postgresql(query=prod_sol_q_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#***************  wind power/quarter
prod_wp_q_query='''SELECT "Year", "Quarter"
	,CASE \
        WHEN LEFT("Quarter", 2) = 'Q1' 
			THEN 'Q1' 
		WHEN LEFT("Quarter", 2) = 'Q2' 
			THEN 'Q2' 
		WHEN LEFT("Quarter", 2) = 'Q3' 
			THEN 'Q3' 
		WHEN LEFT("Quarter", 2) = 'Q4' 
			THEN 'Q4' 
		END AS "Quarters" 
	,CAST(ROUND((COALESCE(SUM("P50A"), 0) / 1000), 3) AS DECIMAL(10, 3)) AS "ProdWp" 
FROM dwh."I_Asset" 
WHERE "ProjectId" IN ( 
		SELECT DISTINCT ("ProjectId") 
		FROM dwh."D_Asset"
		WHERE "Technology" = 'éolien' 
		) 
GROUP BY "Year", "Quarter"
ORDER BY "Year", "Quarter";'''
prod_wp_q =query_data_from_postgresql(query=prod_wp_q_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#************* solar/month
prod_sol_m_query='''SELECT "Year", "Month"
	,CASE
        WHEN "Month" = 1 
			THEN 'jan' 
		WHEN "Month" = 2 
			THEN 'feb' 
		WHEN "Month" = 3 
			THEN 'mar' 
		WHEN "Month" = 4 
			THEN 'apr' 
		WHEN "Month" = 5 
			THEN 'may' 
		WHEN "Month" = 6 
			THEN 'jun' 
		WHEN "Month" = 7 
			THEN 'jul' 
		WHEN "Month" = 8 
			THEN 'aug' 
		WHEN "Month" = 9 
			THEN 'sep' 
		WHEN "Month" = 10 
			THEN 'oct' 
		WHEN "Month" = 11 
			THEN 'nov' 
		WHEN "Month" = 12 
			THEN 'dec' 
		END AS "Months"
	,CAST(ROUND((COALESCE(SUM("P50A"), 0) / 1000), 3) AS DECIMAL(10, 3)) AS "ProdSolar"
FROM dwh."I_Asset"
WHERE "ProjectId" IN (
		SELECT DISTINCT ("ProjectId")
		FROM dwh."D_Asset"
		WHERE "Technology" = 'solaire'
		) \
GROUP BY "Year", "Month"
ORDER BY "Year", "Month";'''
prod_sol_m =query_data_from_postgresql(query=prod_sol_m_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#************** wind Power/month
prod_wp_m_query='''SELECT "Year", "Month"
    ,CASE \
        WHEN "Month" = 1 
            THEN 'jan' 
        WHEN "Month" = 2 
            THEN 'feb' 
        WHEN "Month" = 3 
            THEN 'mar' 
        WHEN "Month" = 4 
            THEN 'apr' 
        WHEN "Month" = 5 
            THEN 'may' 
        WHEN "Month" = 6 
            THEN 'jun' 
        WHEN "Month" = 7 
            THEN 'jul' 
        WHEN "Month" = 8 
            THEN 'aug' 
        WHEN "Month" = 9 
            THEN 'sep' 
        WHEN "Month" = 10 
            THEN 'oct' 
        WHEN "Month" = 11 
            THEN 'nov' 
        WHEN "Month" = 12 
            THEN 'dec' 
        END AS "Months" 
    ,CAST(ROUND((COALESCE(SUM("P50A"), 0) / 1000), 3) AS DECIMAL(10, 3)) AS "ProdWp"
FROM dwh."I_Asset"
WHERE "ProjectId" IN (
        SELECT DISTINCT ("ProjectId")
        FROM dwh."D_Asset"
        WHERE "Technology" = 'éolien' 
        ) 
GROUP BY "Year", "Month"
ORDER BY "Year", "Month";'''
prod_wp_m =query_data_from_postgresql(query=prod_wp_m_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)


#/***************************************************
#************  Exposure Solar and Eol  **************
#*/**************************************************

#***************  exposure solar/year
exposure_sol_y_query='''SELECT asset."Year", asset."ProdAsset" + hedge."Hedges" AS "ExposureSolar" 
FROM ( 
	SELECT "Year"
		,CAST(ROUND((COALESCE(SUM("P50A"), 0) / 1000), 2) AS DECIMAL(10, 1)) AS "ProdAsset" 
	FROM dwh."I_Asset" 
	WHERE "ProjectId"IN ( 
			SELECT DISTINCT ("ProjectId") 
			FROM dwh."D_Asset" 
			WHERE "Technology" = 'solaire' 
			) 
	GROUP BY "Year" 
	) AS asset 
INNER JOIN ( 
	SELECT "Year"
		,CAST(ROUND((COALESCE(SUM("P50H"), 0) / 1000), 2) AS DECIMAL(10, 1)) AS "Hedges"
	FROM dwh."I_Hedge"
	WHERE "ProjectId" IN (
			SELECT DISTINCT ("ProjectId")
			FROM dwh."D_Asset"
			WHERE "Technology" = 'solaire'
			) 
	GROUP BY "Year"
	) AS hedge ON asset."Year" = hedge."Year"
ORDER BY "Year";'''
exposure_sol_y =query_data_from_postgresql(query=exposure_sol_y_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#*************  exposure eol/year
exposure_wp_y_query='''SELECT asset."Year", asset."ProdAsset" + hedge."Hedges" AS "ExposureWp" 
FROM (
	SELECT "Year"
		,CAST(ROUND((COALESCE(SUM("P50A"), 0) / 1000), 3) AS DECIMAL(10, 3)) AS "ProdAsset"
	FROM dwh."I_Asset"
	WHERE "ProjectId" IN ( 
			SELECT DISTINCT ("ProjectId") 
			FROM dwh."D_Asset"
			WHERE "Technology" = 'éolien' 
			) 
	GROUP BY "Year"
	) AS asset 
INNER JOIN ( 
	SELECT "Year"
		,CAST(ROUND((COALESCE(SUM("P50H"), 0) / 1000), 3) AS DECIMAL(10, 3)) AS "Hedges"
	FROM dwh."I_Hedge"
	WHERE "ProjectId" IN ( 
			SELECT DISTINCT ("ProjectId") 
			FROM dwh."D_Asset" 
			WHERE "Technology" = 'éolien' 
			) 
	GROUP BY "Year" 
	) AS hedge ON asset."Year" = hedge."Year" 
ORDER BY "Year";'''
exposure_wp_y =query_data_from_postgresql(query=exposure_wp_y_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#-----------------------------------exposure solar/quarter
exposure_sol_q_query='''SELECT asset."Year", asset."Quarter", asset."Quarters", asset."ProdAsset" + hedge."Hedges" AS "ExposureSolar" 
FROM ( 
    SELECT "Year", "Quarter"
        ,CASE 
            WHEN LEFT("Quarter", 2) = 'Q1' 
                THEN 'Q1' 
            WHEN LEFT("Quarter", 2) = 'Q2' 
                THEN 'Q2' 
            WHEN LEFT("Quarter", 2) = 'Q3' 
                THEN 'Q3' 
            WHEN LEFT("Quarter", 2) = 'Q4' 
                THEN 'Q4' \
            END AS "Quarters"
        ,CAST(ROUND((COALESCE(SUM("P50A"), 0) / 1000), 2) AS DECIMAL(10, 1)) AS "ProdAsset"
    FROM dwh."I_Asset"
    WHERE "ProjectId" IN ( 
            SELECT DISTINCT ("ProjectId") 
            FROM dwh."D_Asset"
            WHERE "Technology" = 'solaire' 
            ) 
    GROUP BY "Year", "Quarter"
    ) AS asset 
INNER JOIN (
    SELECT "Year", "Quarter"
        ,CASE
            WHEN LEFT("Quarter", 2) = 'Q1' 
                THEN 'Q1' 
            WHEN LEFT("Quarter", 2) = 'Q2' 
                THEN 'Q2' 
            WHEN LEFT("Quarter", 2) = 'Q3' 
                THEN 'Q3' 
            WHEN LEFT("Quarter", 2) = 'Q4' 
                THEN 'Q4' 
            END AS "Quarters"
        ,CAST(ROUND((COALESCE(SUM("P50H"), 0) / 1000), 2) AS DECIMAL(10, 1)) AS "Hedges"
    FROM dwh."I_Hedge"
    WHERE "ProjectId" IN (
            SELECT DISTINCT ("ProjectId")
            FROM dwh."D_Asset"
            WHERE "Technology" = 'solaire'
            )
    GROUP BY "Year", "Quarter"
    ) AS hedge ON asset."Year" = hedge."Year"
    AND asset."Quarter" = hedge."Quarter"
ORDER BY "Year", "Quarter";'''
exposure_sol_q =query_data_from_postgresql(query=exposure_sol_q_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#-----------------------------------exposure éolien/quarter
exposure_wp_q_query ='''SELECT asset."Year", asset."Quarter", asset."Quarters", asset."ProdAsset" + hedge."Hedges" AS "ExposureWp"
FROM ( 
	SELECT "Year", "Quarter" 
		,CASE 
            WHEN LEFT("Quarter", 2) = 'Q1' 
				THEN 'Q1' 
			WHEN LEFT("Quarter", 2) = 'Q2' 
				THEN 'Q2' 
			WHEN LEFT("Quarter", 2) = 'Q3' 
				THEN 'Q3' 
			WHEN LEFT("Quarter", 2) = 'Q4' 
				THEN 'Q4' 
			END AS "Quarters" 
		,CAST(ROUND((COALESCE(SUM("P50A"), 0) / 1000), 2) AS DECIMAL(10, 1)) AS "ProdAsset"
	FROM dwh."I_Asset"
	WHERE "ProjectId" IN ( 
			SELECT DISTINCT ("ProjectId") 
			FROM dwh."D_Asset" 
			WHERE "Technology" = 'éolien' 
			) 
	GROUP BY "Year", "Quarter"
	) AS asset 
INNER JOIN ( 
	SELECT "Year"
		,"Quarter" 
		,CASE 
            WHEN LEFT("Quarter", 2) = 'Q1' 
				THEN 'Q1' 
			WHEN LEFT("Quarter", 2) = 'Q2' 
				THEN 'Q2' 
			WHEN LEFT("Quarter", 2) = 'Q3' 
				THEN 'Q3' 
			WHEN LEFT("Quarter", 2) = 'Q4' 
				THEN 'Q4' 
			END AS "Quarters" 
		,CAST(ROUND((COALESCE(SUM("P50H"), 0) / 1000), 2) AS DECIMAL(10, 1)) AS "Hedges" 
	FROM dwh."I_Hedge" 
	WHERE "ProjectId" IN ( 
			SELECT DISTINCT ("ProjectId") 
			FROM dwh."D_Asset" 
			WHERE "Technology" = 'éolien' 
			) 
	GROUP BY "Year", "Quarter" 
	) AS hedge ON asset."Year" = hedge."Year" 
	AND asset."Quarter" = hedge."Quarter" 
ORDER BY "Year", "Quarter";'''
exposure_wp_q =query_data_from_postgresql(query=exposure_wp_q_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#*************   exposure solar/month

exposure_sol_m_query ='''SELECT asset."Year", asset."Month", asset."Months", asset."ProdAsset" + hedge."Hedges" AS "ExposureSolar"
FROM( 
	SELECT "Year", "Month",
		CASE
            WHEN "Month" = 1 
				THEN 'jan' 
			WHEN "Month" = 2 
				THEN 'feb' 
			WHEN "Month" = 3 
				THEN 'mar' 
			WHEN "Month" = 4 
				THEN 'apr' 
			WHEN "Month" = 5 
				THEN 'may' 
			WHEN "Month" = 6 
				THEN 'jun' 
			WHEN "Month" = 7 
				THEN 'jul' 
			WHEN "Month" = 8 
				THEN 'aug' 
			WHEN "Month" = 9 
				THEN 'sep' 
			WHEN "Month" = 10 
				THEN 'oct' 
			WHEN "Month" = 11 
				THEN 'nov' 
			WHEN "Month" = 12 
				THEN 'dec' 
			END AS "Months"
	,CAST(ROUND((COALESCE(SUM("P50A"), 0)/1000), 2) AS DECIMAL(10, 1)) AS "ProdAsset"
        FROM dwh."I_Asset"
	WHERE "ProjectId" IN (
		SELECT DISTINCT ("ProjectId" ) 
		FROM dwh."D_Asset" 
		WHERE "Technology" = 'solaire' 
		) 
	GROUP BY "Year", "Month" 
) AS asset 
INNER JOIN 
( 
	SELECT "Year", "Month",
		CASE 
            WHEN "Month" = 1 
				THEN 'jan' 
			WHEN "Month" = 2 
				THEN 'feb' 
			WHEN "Month" = 3 
				THEN 'mar' 
			WHEN "Month" = 4 
				THEN 'apr' 
			WHEN "Month" = 5 
				THEN 'may' 
			WHEN "Month" = 6 
				THEN 'jun' 
			WHEN "Month" = 7 
				THEN 'jul' 
			WHEN "Month" = 8 
				THEN 'aug' 
			WHEN "Month" = 9 
				THEN 'sep' 
			WHEN "Month" = 10 
				THEN 'oct' 
			WHEN "Month" = 11 
				THEN 'nov' 
			WHEN "Month" = 12 
				THEN 'dec' 
			END AS "Months"
	,CAST(ROUND((COALESCE(SUM("P50H"), 0)/1000), 2) AS DECIMAL(10, 1)) AS "Hedges"
    FROM dwh."I_Hedge"
	WHERE "ProjectId" IN ( 
		SELECT DISTINCT ("ProjectId" ) 
		FROM dwh."D_Asset" 
		WHERE "Technology" = 'solaire' 
		) 
	GROUP BY "Year", "Month" 
) AS hedge ON asset."Year" = hedge."Year" AND asset."Month" = hedge."Month"
ORDER BY "Year", "Month";'''
exposure_sol_m =query_data_from_postgresql(query=exposure_sol_m_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#************  exposure éolien/month
exposure_wp_m_query ='''SELECT asset."Year", asset."Month", asset."Months", asset."ProdAsset" + hedge."Hedges" AS "ExposureWp"
FROM( 
	SELECT "Year", "Month",
		CASE
            WHEN "Month" = 1 
				THEN 'jan' 
			WHEN "Month" = 2 
				THEN 'feb' 
			WHEN "Month" = 3 
				THEN 'mar' 
			WHEN "Month" = 4 
				THEN 'apr' 
			WHEN "Month" = 5 
				THEN 'may' 
			WHEN "Month" = 6 
				THEN 'jun' 
			WHEN "Month" = 7 
				THEN 'jul' 
			WHEN "Month" = 8 
				THEN 'aug' 
			WHEN "Month" = 9 
				THEN 'sep' 
			WHEN "Month" = 10 
				THEN 'oct' 
			WHEN "Month" = 11 
				THEN 'nov' 
			WHEN "Month" = 12 
				THEN 'dec' 
			END AS "Months"
	,CAST(ROUND((COALESCE(SUM("P50A"), 0)/1000), 2) AS DECIMAL(10, 1)) AS "ProdAsset"
        FROM dwh."I_Asset"
	WHERE "ProjectId" IN (
		SELECT DISTINCT ("ProjectId" ) 
		FROM dwh."D_Asset" 
		WHERE "Technology" = 'éolien' 
		) 
	GROUP BY "Year", "Month" 
) AS asset 
INNER JOIN 
( 
	SELECT "Year", "Month",
		CASE 
            WHEN "Month" = 1 
				THEN 'jan' 
			WHEN "Month" = 2 
				THEN 'feb' 
			WHEN "Month" = 3 
				THEN 'mar' 
			WHEN "Month" = 4 
				THEN 'apr' 
			WHEN "Month" = 5 
				THEN 'may' 
			WHEN "Month" = 6 
				THEN 'jun' 
			WHEN "Month" = 7 
				THEN 'jul' 
			WHEN "Month" = 8 
				THEN 'aug' 
			WHEN "Month" = 9 
				THEN 'sep' 
			WHEN "Month" = 10 
				THEN 'oct' 
			WHEN "Month" = 11 
				THEN 'nov' 
			WHEN "Month" = 12 
				THEN 'dec' 
			END AS "Months"
	,CAST(ROUND((COALESCE(SUM("P50H"), 0)/1000), 2) AS DECIMAL(10, 1)) AS "Hedges"
    FROM dwh."I_Hedge"
	WHERE "ProjectId" IN ( 
		SELECT DISTINCT ("ProjectId" ) 
		FROM dwh."D_Asset" 
		WHERE "Technology" = 'éolien' 
		) 
	GROUP BY "Year", "Month" 
) AS hedge ON asset."Year" = hedge."Year" AND asset."Month" = hedge."Month"
ORDER BY "Year", "Month";'''
exposure_wp_m =query_data_from_postgresql(query=exposure_wp_m_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)


#========================================================================
#=============Type Hedge Solar Wind Power================================
#========================================================================

#*************  Type Hedge solar/year
typehedge_sol_y_query='''SELECT "Year"
	,CASE 
        WHEN "TypeHedge" = 'CR16' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR17' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'OA' 
			THEN 'OA' 
		WHEN "TypeHedge" = 'PPA' 
			THEN 'PPA' 
		END AS "TypeContract" 
	,COALESCE(SUM(-"P50H"), 0) / 1000 AS "HedgeSolar" 
FROM dwh."I_Hedge" 
WHERE "TypeHedge" IS NOT NULL 
	AND "ProjectId" IN ( 
		SELECT DISTINCT ("ProjectId") 
		FROM dwh."D_Asset"
		WHERE "Technology" = 'solaire' 
		) 
GROUP BY "Year" 
	,CASE 
        WHEN "TypeHedge" = 'CR16' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR17' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'OA' 
			THEN 'OA' 
		WHEN "TypeHedge" = 'PPA' 
			THEN 'PPA' 
		END 
ORDER BY "Year" ,"TypeContract";'''
typehedge_sol_y =query_data_from_postgresql(query=typehedge_sol_y_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#************  Type Hedge éolien/year
typehedge_wp_y_query='''SELECT "Year"
	,CASE 
        WHEN "TypeHedge" = 'CR16' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR17' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'OA' 
			THEN 'OA' 
		WHEN "TypeHedge" = 'PPA' 
			THEN 'PPA' 
		END AS "TypeContract" 
	,COALESCE(SUM(-"P50H"), 0) / 1000 AS "HedgeWp" 
FROM dwh."I_Hedge" 
WHERE "TypeHedge" IS NOT NULL 
	AND "ProjectId" IN ( 
		SELECT DISTINCT ("ProjectId") 
		FROM dwh."D_Asset"
		WHERE "Technology" = 'éolien' 
		) 
GROUP BY "Year" 
	,CASE 
        WHEN "TypeHedge" = 'CR16' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR17' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'OA' 
			THEN 'OA' 
		WHEN "TypeHedge" = 'PPA' 
			THEN 'PPA' 
		END 
ORDER BY "Year" ,"TypeContract";'''
typehedge_wp_y =query_data_from_postgresql(query=typehedge_wp_y_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#---------------------type hedge solar/quarter
typehedge_sol_q_query='''SELECT "Year", "Quarter"
	,CASE
        WHEN LEFT("Quarter", 2) = 'Q1' 
			THEN 'Q1' 
		WHEN LEFT("Quarter", 2) = 'Q2' 
			THEN 'Q2' 
		WHEN LEFT("Quarter", 2) = 'Q3' 
			THEN 'Q3' 
		WHEN LEFT("Quarter", 2) = 'Q4' 
			THEN 'Q4' 
		END AS "Quarters"
	,CASE 
        WHEN "TypeHedge" = 'CR16' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR17' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'OA' 
			THEN 'OA' 
		WHEN "TypeHedge" = 'PPA' 
			THEN 'PPA' 
		END AS "TypeContract"
	,COALESCE(SUM(-"P50H"), 0) / 1000 AS "HedgeSolar"
FROM dwh."I_Hedge"
WHERE "TypeHedge" IS NOT NULL
	AND "ProjectId" IN ( 
		SELECT DISTINCT ("ProjectId") 
		FROM dwh."D_Asset"
		WHERE "Technology" = 'solaire' 
		) 
GROUP BY "Year" ,"Quarter" 
	,CASE 
        WHEN "TypeHedge" = 'CR16'
			THEN 'CR'
		WHEN "TypeHedge" = 'CR17'
			THEN 'CR'
		WHEN "TypeHedge" = 'CR'
			THEN 'CR'
		WHEN "TypeHedge" = 'OA'
			THEN 'OA'
		WHEN "TypeHedge" = 'PPA'
			THEN 'PPA'
		END
ORDER BY "Year", "Quarter", "TypeContract";'''
typehedge_sol_q =query_data_from_postgresql(query=typehedge_sol_q_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#---------------------type hedge wp/quarter
typehedge_wp_q_query='''SELECT "Year", "Quarter"
	,CASE
        WHEN LEFT("Quarter", 2) = 'Q1' 
			THEN 'Q1' 
		WHEN LEFT("Quarter", 2) = 'Q2' 
			THEN 'Q2' 
		WHEN LEFT("Quarter", 2) = 'Q3' 
			THEN 'Q3' 
		WHEN LEFT("Quarter", 2) = 'Q4' 
			THEN 'Q4' 
		END AS "Quarters"
	,CASE 
        WHEN "TypeHedge" = 'CR16' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR17' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'OA' 
			THEN 'OA' 
		WHEN "TypeHedge" = 'PPA' 
			THEN 'PPA' 
		END AS "TypeContract"
	,COALESCE(SUM(-"P50H"), 0) / 1000 AS "HedgeWp"
FROM dwh."I_Hedge"
WHERE "TypeHedge" IS NOT NULL
	AND "ProjectId" IN ( 
		SELECT DISTINCT ("ProjectId") 
		FROM dwh."D_Asset"
		WHERE "Technology" = 'éolien' 
		) 
GROUP BY "Year" ,"Quarter" 
	,CASE 
        WHEN "TypeHedge" = 'CR16'
			THEN 'CR'
		WHEN "TypeHedge" = 'CR17'
			THEN 'CR'
		WHEN "TypeHedge" = 'CR'
			THEN 'CR'
		WHEN "TypeHedge" = 'OA'
			THEN 'OA'
		WHEN "TypeHedge" = 'PPA'
			THEN 'PPA'
		END
ORDER BY "Year", "Quarter", "TypeContract";'''
typehedge_wp_q =query_data_from_postgresql(query=typehedge_wp_q_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#************  type hedge solar/month
typehedge_sol_m_query='''SELECT "Year", "Month"
	,CASE 
        WHEN "Month" = 1 
			THEN 'jan' 
		WHEN "Month" = 2 
			THEN 'feb' 
		WHEN "Month" = 3 
			THEN 'mar' 
		WHEN "Month" = 4 
			THEN 'apr' 
		WHEN "Month" = 5 
			THEN 'may' 
		WHEN "Month" = 6 
			THEN 'jun' 
		WHEN "Month" = 7 
			THEN 'jul' 
		WHEN "Month" = 8 
			THEN 'aug' 
		WHEN "Month" = 9 
			THEN 'sep' 
		WHEN "Month" = 10 
			THEN 'oct' 
		WHEN "Month" = 11 
			THEN 'nov' 
		WHEN "Month" = 12 
			THEN 'dec' 
		END AS "Months"
	,CASE 
        WHEN "TypeHedge" = 'CR16' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR17' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'OA' 
			THEN 'OA' 
		WHEN "TypeHedge" = 'PPA' 
			THEN 'PPA' 
		END AS "TypeContract" 
	,COALESCE(SUM(-"P50H"), 0) / 1000 AS HedgeSolar 
FROM dwh."I_Hedge"
WHERE "TypeHedge" IS NOT NULL 
	AND "ProjectId" IN ( 
		SELECT DISTINCT ("ProjectId") 
		FROM dwh."D_Asset"
		WHERE "Technology" = 'solaire' 
		)
GROUP BY "Year", "Month"
	,CASE
        WHEN "TypeHedge" = 'CR16' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR17' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'OA' 
			THEN 'OA' 
		WHEN "TypeHedge" = 'PPA' 
			THEN 'PPA' 
		END 
ORDER BY "Year", "Month", "TypeContract";'''
typehedge_sol_m =query_data_from_postgresql(query=typehedge_sol_m_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#************  type hedge wp/month
typehedge_wp_m_query='''SELECT "Year", "Month"
	,CASE 
        WHEN "Month" = 1 
			THEN 'jan' 
		WHEN "Month" = 2 
			THEN 'feb' 
		WHEN "Month" = 3 
			THEN 'mar' 
		WHEN "Month" = 4 
			THEN 'apr' 
		WHEN "Month" = 5 
			THEN 'may' 
		WHEN "Month" = 6 
			THEN 'jun' 
		WHEN "Month" = 7 
			THEN 'jul' 
		WHEN "Month" = 8 
			THEN 'aug' 
		WHEN "Month" = 9 
			THEN 'sep' 
		WHEN "Month" = 10 
			THEN 'oct' 
		WHEN "Month" = 11 
			THEN 'nov' 
		WHEN "Month" = 12 
			THEN 'dec' 
		END AS "Months"
	,CASE 
        WHEN "TypeHedge" = 'CR16' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR17' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'OA' 
			THEN 'OA' 
		WHEN "TypeHedge" = 'PPA' 
			THEN 'PPA' 
		END AS "TypeContract" 
	,COALESCE(SUM(-"P50H"), 0) / 1000 AS HedgeWp 
FROM dwh."I_Hedge"
WHERE "TypeHedge" IS NOT NULL 
	AND "ProjectId" IN ( 
		SELECT DISTINCT ("ProjectId") 
		FROM dwh."D_Asset"
		WHERE "Technology" = 'éolien' 
		)
GROUP BY "Year", "Month"
	,CASE
        WHEN "TypeHedge" = 'CR16' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR17' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'CR' 
			THEN 'CR' 
		WHEN "TypeHedge" = 'OA' 
			THEN 'OA' 
		WHEN "TypeHedge" = 'PPA' 
			THEN 'PPA' 
		END 
ORDER BY "Year", "Month", "TypeContract";'''
typehedge_wp_m =query_data_from_postgresql(query=typehedge_wp_m_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#==================================================================
#=================HCR Solar-Wind Power=============================
#==================================================================

#**********  HCR solar/year
hcr_sol_y_query='''SELECT hedges."Year"
	,CAST(CAST(ROUND((Hedges."HedgeSolar" / Prod."ProdSolar") * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS "HCRSolar"
FROM (
	SELECT "Year"
		,COALESCE(SUM(-"P50H"), 0) AS "HedgeSolar" 
	FROM dwh."I_Hedge"
	WHERE "ProjectId" IN (
			SELECT DISTINCT ("ProjectId")
			FROM dwh."D_Asset"
			WHERE "Technology" = 'solaire'
			) 
	GROUP BY "Year" 
	) AS Hedges 
INNER JOIN ( 
	SELECT "Year"
		,COALESCE(SUM("P50A"), 0) AS "ProdSolar" 
	FROM dwh."I_Asset"
	WHERE "ProjectId" IN ( 
			SELECT DISTINCT ("ProjectId") 
                FROM dwh."D_Asset"
			WHERE "Technology" = 'solaire' 
			)
	GROUP BY "Year"
	) AS Prod ON Hedges."Year" = Prod."Year"
ORDER BY "Year";'''
hcr_sol_y =query_data_from_postgresql(query=hcr_sol_y_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#************  HCR wp/year
hcr_wp_y_query='''SELECT hedges."Year"
	,CAST(CAST(ROUND((Hedges."HedgeWp" / Prod."ProdWp") * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS "HCRWp"
FROM (
	SELECT "Year"
		,COALESCE(SUM(-"P50H"), 0) AS "HedgeWp" 
	FROM dwh."I_Hedge"
	WHERE "ProjectId" IN (
			SELECT DISTINCT ("ProjectId")
			FROM dwh."D_Asset"
			WHERE "Technology" = 'éolien'
			) 
	GROUP BY "Year" 
	) AS Hedges 
INNER JOIN ( 
	SELECT "Year"
		,COALESCE(SUM("P50A"), 0) AS "ProdWp" 
	FROM dwh."I_Asset"
	WHERE "ProjectId" IN ( 
			SELECT DISTINCT ("ProjectId") 
                FROM dwh."D_Asset"
			WHERE "Technology" = 'éolien' 
			)
	GROUP BY "Year"
	) AS Prod ON Hedges."Year" = Prod."Year"
ORDER BY "Year";'''
hcr_wp_y =query_data_from_postgresql(query=hcr_wp_y_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#**************  HCR solar/quarter
hcr_sol_q_query='''SELECT hedges."Year"
	,hedges."Quarter"
	,hedges."Quarters"
	,CAST(CAST(ROUND((Hedges."HedgeSolar" / Prod."ProdSolar") * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS "HCRSolar"
FROM ( 
	SELECT "Year"
		,"Quarter"
		,CASE 
			WHEN LEFT("Quarter", 2) = 'Q1' 
				THEN 'Q1' 
			WHEN LEFT("Quarter", 2) = 'Q2' 
				THEN 'Q2' 
			WHEN LEFT("Quarter", 2) = 'Q3' 
				THEN 'Q3' 
			WHEN LEFT("Quarter", 2) = 'Q4' 
				THEN 'Q4' 
			END AS "Quarters"
		,COALESCE(SUM(-"P50H"), 0) AS "HedgeSolar"
	FROM dwh."I_Hedge"
	WHERE "ProjectId" IN ( 
			SELECT DISTINCT ("ProjectId" ) 
			FROM dwh."D_Asset"
			WHERE "Technology" = 'solaire' 
			) 
	GROUP BY "Year", "Quarter" 
            ) AS Hedges 
INNER JOIN ( 
	SELECT "Year", "Quarter" 
		,CASE 
            WHEN LEFT("Quarter", 2) = 'Q1' 
				THEN 'Q1' 
			WHEN LEFT("Quarter", 2) = 'Q2' 
				THEN 'Q2' 
			WHEN LEFT("Quarter", 2) = 'Q3' 
				THEN 'Q3' 
			WHEN LEFT("Quarter", 2) = 'Q4' 
				THEN 'Q4' 
			END AS "Quarters" 
		,COALESCE(SUM("P50A"), 0) AS "ProdSolar" 
	FROM dwh."I_Asset" 
	WHERE "ProjectId" IN ( 
			SELECT DISTINCT ("ProjectId") 
			FROM dwh."D_Asset" 
			WHERE "Technology" = 'solaire' 
			) 
	GROUP BY "Year" 
		,"Quarter" 
	) AS prod ON Hedges."Year" = Prod."Year"
	AND Hedges."Quarter" = Prod."Quarter"
ORDER BY "Year", "Quarter";'''
hcr_sol_q =query_data_from_postgresql(query=hcr_sol_q_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#*************  HCR wp/quarter
hcr_wp_q_query='''SELECT hedges."Year"
	,hedges."Quarter"
	,hedges."Quarters"
	,CAST(CAST(ROUND((Hedges."HedgeWp" / Prod."ProdWp") * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS "HCRWp"
FROM ( 
	SELECT "Year"
		,"Quarter"
		,CASE 
			WHEN LEFT("Quarter", 2) = 'Q1' 
				THEN 'Q1' 
			WHEN LEFT("Quarter", 2) = 'Q2' 
				THEN 'Q2' 
			WHEN LEFT("Quarter", 2) = 'Q3' 
				THEN 'Q3' 
			WHEN LEFT("Quarter", 2) = 'Q4' 
				THEN 'Q4' 
			END AS "Quarters"
		,COALESCE(SUM(-"P50H"), 0) AS "HedgeWp"
	FROM dwh."I_Hedge"
	WHERE "ProjectId" IN ( 
			SELECT DISTINCT ("ProjectId" ) 
			FROM dwh."D_Asset"
			WHERE "Technology" = 'éolien' 
			) 
	GROUP BY "Year", "Quarter" 
            ) AS Hedges 
INNER JOIN ( 
	SELECT "Year", "Quarter" 
		,CASE 
            WHEN LEFT("Quarter", 2) = 'Q1' 
				THEN 'Q1' 
			WHEN LEFT("Quarter", 2) = 'Q2' 
				THEN 'Q2' 
			WHEN LEFT("Quarter", 2) = 'Q3' 
				THEN 'Q3' 
			WHEN LEFT("Quarter", 2) = 'Q4' 
				THEN 'Q4' 
			END AS "Quarters" 
		,COALESCE(SUM("P50A"), 0) AS "ProdWp" 
	FROM dwh."I_Asset" 
	WHERE "ProjectId" IN ( 
			SELECT DISTINCT ("ProjectId") 
			FROM dwh."D_Asset" 
			WHERE "Technology" = 'éolien' 
			) 
	GROUP BY "Year" 
		,"Quarter" 
	) AS prod ON Hedges."Year" = Prod."Year"
	AND Hedges."Quarter" = Prod."Quarter"
ORDER BY "Year", "Quarter";'''
hcr_wp_q =query_data_from_postgresql(query=hcr_wp_q_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
#*************  HCR solar/month
hcr_sol_m_query='''SELECT Hedges."Year"
	,Hedges."Month"
	,Hedges."Months"
	,CAST(CAST(ROUND((Hedges."HedgeSolar" / prod."ProdSolar") * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS "HCRSolar"
FROM ( \
	SELECT "Year"
		,"Month"
		,CASE 
			WHEN "Month" = 1 
				THEN 'jan' 
			WHEN "Month" = 2 
				THEN 'feb' 
			WHEN "Month" = 3 
				THEN 'mar' 
			WHEN "Month" = 4 
				THEN 'apr' 
			WHEN "Month" = 5 
				THEN 'may' 
			WHEN "Month" = 6 
				THEN 'jun' 
			WHEN "Month" = 7 
				THEN 'jul' 
			WHEN "Month" = 8 
				THEN 'aug' 
			WHEN "Month" = 9 
				THEN 'sep' 
			WHEN "Month" = 10 
				THEN 'oct' 
			WHEN "Month" = 11 
				THEN 'nov' 
			WHEN "Month" = 12 
				THEN 'dec' 
			END AS "Months" 
		,COALESCE(SUM(-"P50H"), 0) AS "HedgeSolar" 
	FROM dwh."I_Hedge"
	WHERE "ProjectId" IN ( 
			SELECT DISTINCT ("ProjectId") 
			FROM dwh."D_Asset" 
			WHERE "Technology" = 'solaire' 
			) 
	GROUP BY "Year", "Month" 
	) AS Hedges 
INNER JOIN ( 
	SELECT "Year", "Month"
		,CASE 
			WHEN "Month" = 1 
				THEN 'jan' 
			WHEN "Month" = 2 
				THEN 'feb' 
			WHEN "Month" = 3 
				THEN 'mar' 
			WHEN "Month" = 4 
				THEN 'apr' 
			WHEN "Month" = 5 
				THEN 'may' 
			WHEN "Month" = 6 
				THEN 'jun' 
			WHEN "Month" = 7 
				THEN 'jul' 
			WHEN "Month" = 8 
				THEN 'aug' 
			WHEN "Month" = 9 
				THEN 'sep' 
			WHEN "Month" = 10 
				THEN 'oct' 
			WHEN "Month" = 11 
				THEN 'nov' 
			WHEN "Month" = 12 
				THEN 'dec' 
			END AS "Months" 
		,COALESCE(SUM("P50A"), 0) AS "ProdSolar"
	FROM dwh."I_Asset"
	WHERE "ProjectId" IN ( 
			SELECT DISTINCT ("ProjectId") 
			FROM dwh."D_Asset" 
			WHERE "Technology" = 'solaire' 
			) 
	GROUP BY "Year", "Month" 
	) AS prod ON Hedges."Year" = Prod."Year" 
	AND Hedges."Month" = Prod."Month" 
ORDER BY "Year", "Month";'''
hcr_sol_m =query_data_from_postgresql(query=hcr_sol_m_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#*************  HCR solar/month
hcr_wp_m_query='''SELECT Hedges."Year"
	,Hedges."Month"
	,Hedges."Months"
	,CAST(CAST(ROUND((Hedges."HedgeWp" / prod."ProdWp") * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS "HCRWp"
FROM ( \
	SELECT "Year"
		,"Month"
		,CASE 
			WHEN "Month" = 1 
				THEN 'jan' 
			WHEN "Month" = 2 
				THEN 'feb' 
			WHEN "Month" = 3 
				THEN 'mar' 
			WHEN "Month" = 4 
				THEN 'apr' 
			WHEN "Month" = 5 
				THEN 'may' 
			WHEN "Month" = 6 
				THEN 'jun' 
			WHEN "Month" = 7 
				THEN 'jul' 
			WHEN "Month" = 8 
				THEN 'aug' 
			WHEN "Month" = 9 
				THEN 'sep' 
			WHEN "Month" = 10 
				THEN 'oct' 
			WHEN "Month" = 11 
				THEN 'nov' 
			WHEN "Month" = 12 
				THEN 'dec' 
			END AS "Months" 
		,COALESCE(SUM(-"P50H"), 0) AS "HedgeWp" 
	FROM dwh."I_Hedge"
	WHERE "ProjectId" IN ( 
			SELECT DISTINCT ("ProjectId") 
			FROM dwh."D_Asset" 
			WHERE "Technology" = 'éolien' 
			) 
	GROUP BY "Year", "Month" 
	) AS Hedges 
INNER JOIN ( 
	SELECT "Year", "Month"
		,CASE 
			WHEN "Month" = 1 
				THEN 'jan' 
			WHEN "Month" = 2 
				THEN 'feb' 
			WHEN "Month" = 3 
				THEN 'mar' 
			WHEN "Month" = 4 
				THEN 'apr' 
			WHEN "Month" = 5 
				THEN 'may' 
			WHEN "Month" = 6 
				THEN 'jun' 
			WHEN "Month" = 7 
				THEN 'jul' 
			WHEN "Month" = 8 
				THEN 'aug' 
			WHEN "Month" = 9 
				THEN 'sep' 
			WHEN "Month" = 10 
				THEN 'oct' 
			WHEN "Month" = 11 
				THEN 'nov' 
			WHEN "Month" = 12 
				THEN 'dec' 
			END AS "Months" 
		,COALESCE(SUM("P50A"), 0) AS "ProdWp"
	FROM dwh."I_Asset"
	WHERE "ProjectId" IN ( 
			SELECT DISTINCT ("ProjectId") 
			FROM dwh."D_Asset" 
			WHERE "Technology" = 'éolien' 
			) 
	GROUP BY "Year", "Month" 
	) AS prod ON Hedges."Year" = Prod."Year" 
	AND Hedges."Month" = Prod."Month" 
ORDER BY "Year", "Month";'''
hcr_wp_m =query_data_from_postgresql(query=hcr_wp_m_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#======================================
#==========     MtM     ===============
#======================================

#*************  MtM year  *************
mtm_query='''SELECT h."Year",
    CAST(ROUND(SUM(-h."P50H" * (cp."ContractPrice" - mp."SettlementPrice"))/1000000, 2) AS DECIMAL(20, 3)) AS "MtM"
FROM dwh."I_Hedge" AS h
INNER JOIN dwh."I_ContractPrices" AS cp ON h."HedgeId" = cp."HedgeId" 
    AND h."ProjectId" = cp."ProjectId" 
    AND h."Year" = cp."Year" 
    AND CAST(SUBSTRING(h."Quarter", 2, 1) AS INTEGER) = cp."Quarter" 
    AND h."Month" = cp."Month" 
INNER JOIN dwh."I_MarketPrices" AS mp ON h."HedgeId" = mp."HedgeId"
    AND h."ProjectId" = mp."ProjectId"
    AND h."Year" = mp."Year"
    AND CAST(SUBSTRING(h."Quarter", 2, 1) AS INTEGER) = mp."Quarter"
    AND h."Month" = mp."Month"
GROUP BY h."Year"
ORDER BY h."Year";'''
mtm =query_data_from_postgresql(query=mtm_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)
#***********  MtM Portfolio Merchant ***************
mtm_merch_query='''SELECT h."Year"
    ,CAST(ROUND(SUM(- h."P50H" * (cp."ContractPrice" - mp."SettlementPrice")) / 1000000, 2) AS DECIMAL(20, 3)) AS "MtM"
FROM dwh."I_Hedge" AS h
INNER JOIN dwh."I_ContractPrices" AS cp ON h."HedgeId" = cp."HedgeId" 
    AND h."ProjectId" = cp."ProjectId"
    AND h."Year" = cp."Year"
    AND CAST(SUBSTRING(h."Quarter", 2, 1) AS INTEGER) = cp."Quarter"
    AND h."Month"  = cp."Month" 
INNER JOIN dwh."I_MarketPrices" AS mp ON h."HedgeId" = mp."HedgeId"
    AND h."ProjectId" = mp."ProjectId"
    AND h."Year" = mp."Year"
    AND CAST(SUBSTRING(h."Quarter", 2, 1) AS INTEGER) = mp."Quarter"
    AND h."Month" = mp."Month" 
WHERE h."TypeHedge" = 'PPA' 
    OR h."TypeHedge" IS NULL 
GROUP BY h."Year"
ORDER BY h."Year";'''
mtm_merch =query_data_from_postgresql(query=mtm_merch_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#*************  MtM Portfolio Reguled **************
mtm_reg_query='''SELECT h."Year"
	,CAST(ROUND(SUM(- h."P50H" * (cp."ContractPrice" - mp."SettlementPrice")) / 1000000, 2) AS DECIMAL(20, 3)) AS "MtM"
FROM dwh."I_Hedge" AS h
INNER JOIN dwh."I_ContractPrices" AS cp ON h."HedgeId" = cp."HedgeId" 
    AND h."ProjectId" = cp."ProjectId"
    AND h."Year" = cp."Year"
    AND CAST(SUBSTRING(h."Quarter", 2, 1) AS INTEGER) = cp."Quarter"
    AND h."Month" = cp."Month"
INNER JOIN dwh."I_MarketPrices" AS mp ON h."HedgeId" = mp."HedgeId"
    AND h."ProjectId" = cp."ProjectId"
    AND h."Year" = mp."Year" 
    AND CAST(SUBSTRING(h."Quarter", 2, 1) AS INTEGER) = mp."Quarter"
    AND h."Month" = mp."Month"
WHERE h."TypeHedge" != 'PPA' 
    OR h."TypeHedge" IS NULL 
GROUP BY h."Year"
ORDER BY h."Year";'''
mtm_reg =query_data_from_postgresql(query=mtm_reg_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

#*************  MtM Portfolio history ***************
mtm_hist_query='''SELECT "CotationDate", "MtM"
                    FROM dwh."I_MtM"
                ;'''
mtm_hist =query_data_from_postgresql(query=mtm_hist_query, 
                                    pguid=pguid, pgpw=pgpw, pgserver=pgserver, pgport=pgport, pgdb=pgdwhdb)

