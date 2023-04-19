dimasset_query = '''
    SELECT
         "AssetId"
        ,"ProjectId"
        ,"Project"
        ,"Technology"
        ,"Cod"
        ,"Mw"
        ,"SuccesPct"
        ,"InstalledPower"
        ,"Eoh"
        ,"DateMerchant"
        ,"DismentleDate"
        ,"Repowering"
        ,"DateMsi" 
        ,"InPlanif"
        ,"P50"
        ,"P90"
        FROM dwh."DimAsset"
        '''

dimhedge_query = '''
    SELECT
         "HedgeId"
        ,"AssetKey"
        ,"ProjectId"
        ,"Project"
        ,"Technology"
        ,"TypeHedge"
        ,"ContractStartDate"
        ,"ContractEndDate"
        ,"DismentleDate"
        ,"InstalledPower"
        ,"Profil"
        ,"HedgePct"
        ,"Counterparty"
        ,"CountryCounterparty"
        ,"InPlanif"
      FROM dwh."DimHedge" 
        '''

dimdate_query = '''
    SELECT
        "DateKey"
        ,"Date"
        ,"CalenderYear"
        ,"QuarterNumberOfYear"
        ,"MonthNumberOfYear"
        ,"MonthNameOfYear"
        ,"WeekNumberOfYear"
        ,"DayNumberOfWeek"
        ,"DayNumberOfYear"
        ,"DayNumberOfMonth"
        ,"DayNameOfWeek"
      FROM dwh."DimDate" 
        '''

factprodprices_query = '''
    SELECT 
        "Hedgekey"
        ,"DateKey"
        ,"ProjetId"
        ,"P50Asset"
        ,"P90Asset"
        ,"P50Hedge"
        ,"P90Hedge"
        ,"ContractPrice"
        ,"SettlementPrice"
      FROM dwh."FactProdPrices"
    '''
viewhedgeasset_query= '''
     SELECT * 
         FROM dwh."HedgeAsset"
    '''


#======1.Exposition per year
exposure_y = '''
    SELECT year 
    ,CAST(ROUND(COALESCE(SUM(p50), 0) / 1000, 3) AS DECIMAL(10, 3)) + ( 
        SELECT CAST(ROUND(COALESCE(SUM(p50), 0) / 1000, 3) AS DECIMAL(10, 3))
        FROM dash.p50p90hedge AS h 
        WHERE a.year = h.year 
        ) AS Exposure 
FROM dash.p50p90asset AS a 
GROUP BY year 
ORDER BY year
'''


#======2.Exposition per quarter per year
query_2 ="SELECT year \
	,quarter \
	,CASE \
        WHEN LEFT(quarter, 2) = 'Q1' \
			THEN 'Q1' \
		WHEN LEFT(quarter, 2) = 'Q2' \
			THEN 'Q2' \
		WHEN LEFT(quarter, 2) = 'Q3' \
			THEN 'Q3' \
		WHEN LEFT(quarter, 2) = 'Q4' \
			THEN 'Q4' \
		END AS quarters \
	,CAST(ROUND(COALESCE(SUM(p50), 0) / 1000, 3) AS DECIMAL(10, 3))+ ( \
		SELECT CAST(ROUND(COALESCE(SUM(p50), 0) / 1000, 3) AS DECIMAL(10,3)) \
		FROM dash.p50p90hedge AS h \
		WHERE a.year = h.year \
			AND a.quarter = h.quarter \
		) AS Exposure \
FROM dash.p50p90asset AS a \
GROUP BY year \
	,quarter \
ORDER BY year \
	,quarter;"
query_results_2 = pd.read_sql(query_2, postgressql_engine())

#======3.Exposition per month per year

query_3="SELECT year \
	,month \
	,CASE \
        WHEN month = 1 \
			THEN 'jan' \
		WHEN month = 2 \
			THEN 'feb' \
		WHEN month = 3 \
			THEN 'mar' \
		WHEN month = 4 \
			THEN 'apr' \
		WHEN month = 5 \
			THEN 'may' \
		WHEN month = 6 \
			THEN 'jun' \
		WHEN month = 7 \
			THEN 'jul' \
		WHEN month = 8 \
			THEN 'aug' \
		WHEN month = 9 \
			THEN 'sep' \
		WHEN month = 10 \
			THEN 'oct' \
		WHEN month = 11 \
			THEN 'nov' \
		WHEN month = 12 \
			THEN 'dec' \
		END AS months \
	,   CAST(ROUND(COALESCE(SUM(p50), 0) / 1000, 3) AS DECIMAL(10, 3))+ ( \
		SELECT CAST(ROUND(COALESCE(SUM(p50), 0) / 1000, 3) AS DECIMAL(10, 3))\
		FROM dash.p50p90hedge AS h \
		WHERE a.year = h.year \
			AND a.month = h.month \
		) AS Exposure \
FROM dash.p50p90asset AS a \
GROUP BY year \
	,month \
ORDER BY year \
	,month;"
query_results_3 = pd.read_sql(query_3, postgressql_engine())

#=====4. Hedge type per year

query_4 ="SELECT year \
	,   CASE \
        WHEN hedgetype = 'CR16' \
			THEN 'CR' \
		WHEN hedgetype = 'CR17' \
			THEN 'CR' \
		WHEN hedgetype = 'CR' \
			THEN 'CR' \
		WHEN hedgetype = 'OA' \
			THEN 'OA' \
		WHEN hedgetype = 'PPA' \
			THEN 'PPA' \
		END AS TypeContract \
	,   CAST(ROUND(COALESCE(SUM(- p50), 0) / 1000, 3) AS DECIMAL(10, 3)) AS Hedge \
FROM dash.p50p90hedge \
WHERE hedgetype IS NOT NULL \
GROUP BY year \
	,   CASE \
		WHEN hedgetype = 'CR16' \
			THEN 'CR' \
		WHEN hedgetype = 'CR17' \
			THEN 'CR' \
		WHEN hedgetype = 'CR' \
			THEN 'CR' \
		WHEN hedgetype = 'OA' \
			THEN 'OA' \
		WHEN hedgetype = 'PPA' \
			THEN 'PPA' \
		END \
ORDER BY year \
	,TypeContract;"
query_results_4 = pd.read_sql(query_4, postgressql_engine())  
  
#=====5. Hedge per quarter per year

query_5 ="SELECT year, quarter, \
                 CASE WHEN hedgetype = 'CR16' THEN 'CR' \
                      WHEN hedgetype = 'CR17' THEN 'CR' \
                      WHEN hedgetype = 'CR' THEN 'CR' \
                      WHEN hedgetype = 'OA' THEN 'OA' \
                      WHEN hedgetype = 'PPA' THEN 'PPA' \
                 END AS TypeContract, \
                 CASE WHEN LEFT(quarter, 2)='Q1' THEN 'Q1' \
                      WHEN LEFT(quarter, 2)='Q2' THEN 'Q2' \
                      WHEN LEFT(quarter, 2)='Q3' THEN 'Q3' \
                      WHEN LEFT(quarter, 2)='Q4' THEN 'Q4' \
                      END AS quarters, \
                CAST(ROUND(COALESCE(SUM(-p50), 0)/1000, 3) AS DECIMAL(10, 3)) AS Hedge \
          FROM dash.p50p90hedge \
          WHERE hedgetype IS NOT NULL\
          GROUP BY year, quarter, \
              CASE WHEN hedgetype='CR16' THEN 'CR' \
                   WHEN hedgetype='CR17' THEN 'CR' \
                   WHEN hedgetype='CR' THEN 'CR' \
                   WHEN hedgetype='OA' THEN 'OA' \
                   WHEN hedgetype='PPA' THEN 'PPA' \
                   END \
          ORDER BY year, quarters;"
query_results_5 = pd.read_sql(query_5, postgressql_engine())  

#=====6. Hedge per month per year
query_6 ="SELECT year, month,\
                 CASE WHEN hedgetype = 'CR16' THEN 'CR' \
                      WHEN hedgetype = 'CR17' THEN 'CR' \
                      WHEN hedgetype = 'CR' THEN 'CR' \
                      WHEN hedgetype = 'OA' THEN 'OA' \
                      WHEN hedgetype = 'PPA' THEN 'PPA' \
                 END AS TypeContract, \
                 CASE WHEN month=1 THEN 'jan'\
                      WHEN month=2 THEN 'feb' \
                      WHEN month=3 THEN 'mar' \
                      WHEN month=4 THEN 'apr' \
                      WHEN month=5 THEN 'may' \
                      WHEN month=6 THEN 'jun' \
                      WHEN month=7 THEN 'jul' \
                      WHEN month=8 THEN 'aug' \
                      WHEN month=9 THEN 'sep' \
                      WHEN month=10 THEN 'oct' \
                      WHEN month=11 THEN 'nov' \
                      WHEN month=12 THEN 'dec' \
                 END AS months,\
                CAST(ROUND(COALESCE(SUM(-p50), 0)/1000, 3) AS DECIMAL(10, 3))AS Hedge \
          FROM dash.p50p90hedge \
          WHERE hedgetype IS NOT NULL \
          GROUP BY year, month,\
              CASE WHEN hedgetype='CR16' THEN 'CR' \
                   WHEN hedgetype='CR17' THEN 'CR' \
                   WHEN hedgetype='CR' THEN 'CR' \
                   WHEN hedgetype='OA' THEN 'OA' \
                   WHEN hedgetype='PPA' THEN 'PPA' \
                   END \
          ORDER BY year, month;"
query_results_6 = pd.read_sql(query_6, postgressql_engine()) 

#=====7.HCR per year
query_7 = "SELECT year, CAST(CAST(ROUND((SELECT COALESCE(SUM(-p50), 0) \
                        FROM dash.p50p90hedge AS h \
                        WHERE a.year=h.year) / COALESCE(SUM(p50), 0)*100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) ||'%%' AS HCR \
            FROM dash.p50p90asset AS a \
            GROUP BY year \
            ORDER BY year;"
query_results_7 = pd.read_sql(query_7, postgressql_engine())
#=====8.HCR per quarter
query_8 = "SELECT year, quarter, \
                  CASE WHEN LEFT(quarter, 2) = 'Q1' THEN 'Q1' \
                       WHEN LEFT(quarter, 2) = 'Q2' THEN 'Q2' \
                       WHEN LEFT(quarter, 2) = 'Q3' THEN 'Q3' \
                       WHEN LEFT(quarter, 2) = 'Q4' THEN 'Q4' \
                       END AS quarters,\
                          CAST(CAST(ROUND((SELECT COALESCE(SUM(-p50), 0) \
                            FROM dash.p50p90hedge AS h \
                            WHERE a.year=h.year AND  a.quarter=h.quarter) / COALESCE(SUM(p50), 0)*100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS HCR \
            FROM dash.p50p90asset AS a \
            GROUP BY year, quarter \
            ORDER BY year, quarter;"
query_results_8= pd.read_sql(query_8, postgressql_engine())
#=====9.HCR per month
query_9 = "SELECT year, month, \
                  CASE WHEN month=1 THEN 'jan' \
                       WHEN month=2 THEN 'feb' \
                       WHEN month=3 THEN 'mar' \
                       WHEN month=4 THEN 'apr' \
		               WHEN month=5 THEN 'may' \
		               WHEN month=6 THEN 'jun' \
		               WHEN month=7 THEN 'jul' \
		               WHEN month=8 THEN 'aug' \
		               WHEN month=9 THEN 'sep' \
		               WHEN month=10 THEN 'oct' \
		               WHEN month=11 THEN 'nov' \
		               WHEN month=12 THEN 'dec' \
		          END AS months, \
                  CAST(CAST(ROUND((SELECT COALESCE(SUM(-p50), 0) \
                  FROM dash.p50p90hedge AS h \
                  WHERE a.year=h.year AND  a.month=h.month) / COALESCE(SUM(p50), 0)*100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS HCR \
            FROM dash.p50p90asset AS a \
            GROUP BY year, month \
            ORDER BY year, month;"
query_results_9= pd.read_sql(query_9, postgressql_engine())

#=============================
#====    PRODUCTION  =========
#=============================

#=====Prod per year
query_10 = "SELECT year, \
                   CAST(ROUND(COALESCE(SUM(p50), 0)/1000, 3) AS DECIMAL(10, 3)) AS Prod \
            FROM dash.p50p90asset AS a \
            GROUP BY year \
            ORDER BY year;"
query_results_10 = pd.read_sql(query_10, postgressql_engine())

#====Prod per quarter

query_11 = "SELECT year, quarter, \
                   CASE WHEN LEFT(quarter, 2)='Q1' THEN 'Q1' \
                   WHEN LEFT(quarter, 2)='Q2' THEN 'Q2' \
                   WHEN LEFT(quarter, 2)='Q3' THEN 'Q3' \
                   WHEN LEFT(quarter, 2)='Q4' THEN 'Q4' \
                   END AS quarters, \
                   CAST(ROUND(COALESCE(SUM(p50), 0)/1000, 3) AS DECIMAL(10, 3))AS Prod \
            FROM dash.p50p90asset \
            GROUP BY year, quarter \
            ORDER BY year, quarter;"
query_results_11 = pd.read_sql(query_11, postgressql_engine())            

#=====Prod per month
query_12 = "SELECT year, month, \
                   CASE WHEN month=1 THEN 'jan' \
                        WHEN month=2 THEN 'feb' \
                        WHEN month=3 THEN 'mar' \
	                    WHEN month=4 THEN 'apr' \
			            WHEN month=5 THEN 'may' \
			            WHEN month=6 THEN 'jun' \
			            WHEN month=7 THEN 'jul' \
			            WHEN month=8 THEN 'aug' \
			            WHEN month=9 THEN 'sep' \
			            WHEN month=10 THEN 'oct' \
			            WHEN month=11 THEN 'nov' \
			            WHEN month=12 THEN 'dec' \
			       END AS months, \
CAST(ROUND(COALESCE(SUM(p50), 0)/1000, 3) AS DECIMAL(10, 2)) AS Prod \
FROM dash.p50p90asset \
GROUP BY year, month \
ORDER BY year, month;"
query_results_12 = pd.read_sql(query_12, postgressql_engine())   

#=====Fixed & merchant per year
query_13 = "SELECT year, \
            SUM(CASE WHEN hedgetype='CR16' OR hedgetype='CR17' OR hedgetype='CR' OR hedgetype='OA' THEN -p50/1000 END) AS FixedPrice, \
			SUM(CASE WHEN hedgetype='PPA' THEN  -p50/1000 END) AS Merchant \
FROM dash.p50p90hedge \
GROUP BY year \
ORDER BY year;"
query_results_13 = pd.read_sql(query_13, postgressql_engine())

#=====Fixed & merchant per quarter
query_14 = "SELECT year, quarter, \
			       CASE WHEN LEFT(quarter, 2)='Q1' THEN 'Q1' \
			            WHEN LEFT(quarter, 2)='Q2' THEN 'Q2' \
			            WHEN LEFT(quarter, 2)='Q3' THEN 'Q3' \
			            WHEN LEFT(quarter, 2)='Q4' THEN 'Q4' \
			END AS quarters, \
			SUM(CASE WHEN hedgetype='CR16' OR hedgetype='CR17' OR hedgetype='CR' OR hedgetype='OA' THEN -p50/1000 END) AS FixedPrice, \
			SUM(CASE WHEN hedgetype='PPA' THEN  -p50/1000 END) AS Merchant \
FROM dash.p50p90hedge \
GROUP BY year, quarter \
ORDER BY year, quarter;"
query_results_14 = pd.read_sql(query_14, postgressql_engine())

#=====Fixed & merchant per months
query_15 = "SELECT year, month, \
		           CASE WHEN month=1 THEN 'jan' \
			            WHEN month=2 THEN 'feb' \
			            WHEN month=3 THEN 'mar' \
	                    WHEN month=4 THEN 'apr' \
			            WHEN month=5 THEN 'may' \
			            WHEN month=6 THEN 'jun' \
			            WHEN month=7 THEN 'jul' \
			            WHEN month=8 THEN 'aug' \
			            WHEN month=9 THEN 'sep' \
			            WHEN month=10 THEN 'oct' \
			            WHEN month=11 THEN 'nov' \
			            WHEN month=12 THEN 'dec' \
		            END AS months, \
			        SUM(CASE WHEN hedgetype='CR16' OR hedgetype='CR17' OR hedgetype='CR' OR hedgetype='OA' THEN -p50/1000 END) AS FixedPrice, \
			        SUM(CASE WHEN hedgetype='PPA' THEN  -p50/1000 END) AS Merchant \
FROM dash.p50p90hedge \
GROUP BY year, month \
ORDER BY year, month;"
query_results_15 = pd.read_sql(query_15, postgressql_engine())

#=====Hedge PPA/year
query_16 = "SELECT year \
	,CAST(ROUND(SUM(- p50) / 1000,3) AS DECIMAL(10, 3))AS PPA \
FROM dash.p50p90hedge \
WHERE hedgetype = 'PPA' \
GROUP BY year \
ORDER BY year;"                
query_results_16 = pd.read_sql(query_16, postgressql_engine())

#=====hedge PPA/quarter
query_17 = "SELECT year \
	,quarter \
	,CASE \
        WHEN LEFT(quarter, 2) = 'Q1' \
			THEN 'Q1' \
		WHEN LEFT(quarter, 2) = 'Q2' \
			THEN 'Q2' \
		WHEN LEFT(quarter, 2) = 'Q3' \
			THEN 'Q3' \
		WHEN LEFT(quarter, 2) = 'Q4' \
			THEN 'Q4' \
		END AS quarters \
	,CAST(ROUND(SUM(- p50) / 1000,3) AS DECIMAL(10, 3)) AS PPA \
FROM dash.p50p90hedge \
WHERE hedgetype = 'PPA' \
GROUP BY year \
	,quarter \
ORDER BY year \
	,quarter;"
query_results_17 = pd.read_sql(query_17, postgressql_engine())

#====hedge PPA/month
query_18= "SELECT year \
	,month \
	,CASE \
        WHEN month = 1 \
			THEN 'jan' \
		WHEN month = 2 \
			THEN 'feb' \
		WHEN month = 3 \
			THEN 'mar' \
		WHEN month = 4 \
			THEN 'apr' \
		WHEN month = 5 \
			THEN 'may' \
		WHEN month = 6 \
			THEN 'jun' \
		WHEN month = 7 \
			THEN 'jul' \
		WHEN month = 8 \
			THEN 'aug' \
		WHEN month = 9 \
			THEN 'sep' \
		WHEN month = 10 \
			THEN 'oct' \
		WHEN month = 11 \
			THEN 'nov' \
		WHEN month = 12 \
			THEN 'dec' \
		END AS months \
	,CAST(ROUND(SUM(- p50) / 1000, 3) AS DECIMAL(10, 3)) AS PPA \
FROM dash.p50p90hedge \
WHERE hedgetype = 'PPA' \
GROUP BY year \
	,month \
ORDER BY year \
	,month;"
query_results_18 = pd.read_sql(query_18, postgressql_engine())


#=====Prod Merchant/year
query_19= "SELECT year\
	,( \
		CAST(ROUND(COALESCE(SUM(p50), 0)/1000, 3) AS DECIMAL(10, 3)) + ( \
			SELECT CAST(ROUND(COALESCE(SUM(p50), 0)/1000, 3) AS DECIMAL(10, 3)) \
			FROM dash.p50p90hedge AS h \
			WHERE hedgetype IN ( \
					'OA' \
					,'CR' \
					,'CR16' \
					,'CR17' \
					) \
				AND a.year = h.year \
			) \
		) AS ProdMerchant \
FROM dash.p50p90asset AS a \
GROUP BY year \
ORDER BY year;"
query_results_19 = pd.read_sql(query_19, postgressql_engine())

#=====Prod Merchant/quarter
query_20="SELECT year \
	,quarter \
	,CASE \
        WHEN LEFT(quarter, 2) = 'Q1' \
			THEN 'Q1' \
		WHEN LEFT(quarter, 2) = 'Q2' \
			THEN 'Q2' \
		WHEN LEFT(quarter, 2) = 'Q3' \
			THEN 'Q3' \
		WHEN LEFT(quarter, 2) = 'Q4' \
			THEN 'Q4' \
		END AS quarters \
	,( \
		    CAST(ROUND(COALESCE(SUM(p50), 0)/1000, 3) AS DECIMAL(10, 3)) + ( \
			SELECT CAST(ROUND(COALESCE(SUM(p50), 0)/1000, 3) AS DECIMAL(10, 3))\
			FROM dash.p50p90hedge AS h \
			WHERE hedgetype IN ( \
					'OA' \
					,'CR' \
					,'CR16' \
					,'CR17' \
					) \
				AND a.year = h.year \
				AND a.quarter = h.quarter \
			) \
		) AS ProdMerchant \
FROM dash.p50p90asset AS a \
GROUP BY year \
	,quarter \
ORDER BY year \
	,quarter;"
query_results_20 = pd.read_sql(query_20, postgressql_engine()) 
#=====Prod Merchant/month
query_21="SELECT year, month \
	   ,CASE \
           WHEN month = 1 \
				THEN 'jan' \
			WHEN month = 2 \
				THEN 'feb' \
			WHEN month = 3 \
				THEN 'mar' \
			WHEN month = 4 \
				THEN 'apr' \
			WHEN month = 5 \
				THEN 'may' \
			WHEN month = 6 \
				THEN 'jun' \
			WHEN month = 7 \
				THEN 'jul' \
			WHEN month = 8 \
				THEN 'aug' \
			WHEN month = 9 \
				THEN 'sep' \
			WHEN month = 10 \
				THEN 'oct' \
			WHEN month = 11 \
				THEN 'nov' \
			WHEN month = 12 \
				THEN 'dec' \
			END AS months \
	,( \
		CAST(ROUND(COALESCE(SUM(p50), 0), 3) AS DECIMAL(10, 3))+ ( \
			SELECT CAST(ROUND(COALESCE(SUM(p50), 0), 3) AS DECIMAL(10, 3))\
			FROM dash.p50p90hedge AS h \
			WHERE hedgetype IN ( \
					'OA' \
					,'CR' \
					,'CR16' \
					,'CR17' \
					) \
				AND a.year = h.year AND a.month = h.month \
			) \
		) / 1000 AS ProdMerchant \
FROM dash.p50p90asset AS a \
GROUP BY year, month \
ORDER BY year, month;"
query_results_21 = pd.read_sql(query_21, postgressql_engine())

#=====Prod Merchant hedged with PPA Coverage Ratio per year
query_22 = "SELECT ppa.year \
	,CAST(CAST(ROUND((ppa.PPAYear / ProdMerchant.ProdMerchantYear) * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS HCR \
FROM ( \
	SELECT year \
		,SUM(- p50) / 1000 AS PPAYear \
	FROM dash.p50p90hedge \
	WHERE hedgetype = 'PPA' \
	GROUP BY year \
	) AS ppa \
INNER JOIN ( \
	SELECT year \
		,( \
			COALESCE(SUM(p50), 0) + ( \
				SELECT COALESCE(SUM(p50), 0) \
				FROM dash.p50p90hedge AS h \
				WHERE hedgetype IN ( \
						'OA' \
						,'CR' \
						,'CR16' \
						,'CR17' \
						) \
					AND a.year = h.year \
				) \
			) / 1000 AS ProdMerchantYear \
	FROM dash.p50p90asset AS a \
	GROUP BY year \
	) AS ProdMerchant ON ppa.year = ProdMerchant.year \
ORDER BY year;"
query_results_22 = pd.read_sql(query_22, postgressql_engine())
#=====Prod Merchant hedged with PPA Coverage Ratio per quarter
query_23 = "SELECT ppa.year \
	,ppa.quarter \
	,ppa.quarters \
	,CAST(CAST(ROUND((ppa.PPAQtr / ProdMerchant.ProdMerchantQtr) * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS HCR \
FROM ( \
	SELECT year \
		,quarter \
		,CASE \
            WHEN LEFT(quarter, 2) = 'Q1' \
				THEN 'Q1' \
			WHEN LEFT(quarter, 2) = 'Q2' \
				THEN 'Q2' \
			WHEN LEFT(quarter, 2) = 'Q3' \
				THEN 'Q3' \
			WHEN LEFT(quarter, 2) = 'Q4' \
				THEN 'Q4' \
			END AS quarters \
		,SUM(- p50) / 1000 AS PPAQtr \
	FROM dash.p50p90hedge \
	WHERE hedgetype = 'PPA' \
	GROUP BY year \
		,quarter \
	) AS ppa \
INNER JOIN ( \
	SELECT year \
		,quarter \
		,CASE \
            WHEN LEFT(quarter, 2) = 'Q1' \
				THEN 'Q1' \
			WHEN LEFT(quarter, 2) = 'Q2' \
				THEN 'Q2' \
			WHEN LEFT(quarter, 2) = 'Q3' \
				THEN 'Q3' \
			WHEN LEFT(quarter, 2) = 'Q4' \
				THEN 'Q4' \
			END AS quarters \
		,( \
			COALESCE(SUM(p50), 0) + ( \
				SELECT COALESCE(SUM(p50), 0) \
				FROM dash.p50p90hedge AS h \
				WHERE hedgetype IN ( \
						'OA' \
						,'CR' \
						,'CR16' \
						,'CR17' \
						) \
					AND a.year = h.year \
					AND a.quarter = h.quarter \
				) \
			) / 1000 AS ProdMerchantQtr \
	FROM dash.p50p90asset AS a \
	GROUP BY year \
		,quarter \
	) AS ProdMerchant ON ppa.year = ProdMerchant.year \
	AND ppa.quarter = ProdMerchant.quarter \
ORDER BY year \
	,quarter;"
query_results_23 = pd.read_sql(query_23, postgressql_engine())

#=====Prod Merchant hedged with PPA Coverage Ratio per month
query_24 = "SELECT ppa.year \
	,ppa.month \
	,ppa.months \
	,CAST(CAST(ROUND((ppa.PPAMth / ProdMerchant.ProdMerchantMth) * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS HCR \
FROM ( \
	SELECT year \
		,month \
		,CASE \
            WHEN month = 1 \
				THEN 'jan' \
			WHEN month = 2 \
				THEN 'feb' \
			WHEN month = 3 \
				THEN 'mar' \
			WHEN month = 4 \
				THEN 'apr' \
			WHEN month = 5 \
				THEN 'may' \
			WHEN month = 6 \
				THEN 'jun' \
			WHEN month = 7 \
				THEN 'jul' \
			WHEN month = 8 \
				THEN 'aug' \
			WHEN month = 9 \
				THEN 'sep' \
			WHEN month = 10 \
				THEN 'oct' \
			WHEN month = 11 \
				THEN 'nov' \
			WHEN month = 12 \
				THEN 'dec' \
			END AS months \
		,SUM(- p50) / 1000 AS PPAMth \
	FROM dash.p50p90hedge \
	WHERE hedgetype = 'PPA' \
	GROUP BY year \
		,month \
	) AS ppa \
INNER JOIN ( \
	SELECT year \
		,month \
		,CASE \
            WHEN month = 1 \
				THEN 'jan' \
			WHEN month = 2 \
				THEN 'feb' \
			WHEN month = 3 \
				THEN 'mar' \
			WHEN month = 4 \
				THEN 'apr' \
			WHEN month = 5 \
				THEN 'may' \
			WHEN month = 6 \
				THEN 'jun' \
			WHEN month = 7 \
				THEN 'jul' \
			WHEN month = 8 \
				THEN 'aug' \
			WHEN month = 9 \
				THEN 'sep' \
			WHEN month = 10 \
				THEN 'oct' \
			WHEN month = 11 \
				THEN 'nov' \
			WHEN month = 12 \
				THEN 'dec' \
			END AS months \
		,( \
			COALESCE(SUM(p50), 0) + ( \
				SELECT COALESCE(SUM(p50), 0) \
				FROM dash.p50p90hedge AS h \
				WHERE hedgetype IN ( \
						'OA' \
						,'CR' \
						,'CR16' \
						,'CR17' \
						) \
					AND a.year = h.year \
					AND a.month = h.month \
				) \
			) / 1000 AS ProdMerchantMth \
	FROM dash.p50p90asset AS a \
	GROUP BY year \
		,month \
	) AS ProdMerchant ON ppa.year = ProdMerchant.year \
	AND ppa.month = ProdMerchant.month \
ORDER BY year \
	,month;"
query_results_24 = pd.read_sql(query_24, postgressql_engine())           

#/*
#=====================================================Prod Solar and Eol
#*/
#-----------------------------------------------------Solar/year
query_25 ="SELECT year \
	,CAST(ROUND((COALESCE(SUM(p50), 0) / 1000), 3) AS DECIMAL(10, 3)) AS ProdSolar \
FROM dash.p50p90asset \
WHERE projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'solaire' \
		) \
GROUP BY year \
ORDER BY year;"
query_results_25 = pd.read_sql(query_25, postgressql_engine()) 
#------------------------------------------------------wind power/year
query_26="SELECT year \
	,CAST(ROUND((COALESCE(SUM(p50), 0) / 1000), 3) AS DECIMAL(10, 3)) AS ProdWP \
FROM dash.p50p90asset \
WHERE projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'éolien' \
		) \
GROUP BY year \
ORDER BY year;"
query_results_26 = pd.read_sql(query_26, postgressql_engine()) 

#-----------------------------------------------------solar/quarter
query_27="SELECT year \
	,quarter \
	,CASE \
        WHEN LEFT(quarter, 2) = 'Q1' \
			THEN 'Q1' \
		WHEN LEFT(quarter, 2) = 'Q2' \
			THEN 'Q2' \
		WHEN LEFT(quarter, 2) = 'Q3' \
			THEN 'Q3' \
        WHEN LEFT(quarter, 2) = 'Q4' \
            THEN 'Q4' \
		END AS quarters \
	,CAST(ROUND((COALESCE(SUM(p50), 0) / 1000), 3) AS DECIMAL(10, 3)) AS ProdSolar \
FROM dash.p50p90asset \
WHERE projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'solaire' \
		) \
GROUP BY year \
	,quarter \
ORDER BY year \
	,quarter;"
query_results_27 = pd.read_sql(query_27, postgressql_engine()) 

#-----------------------------------------------------wind power/quarter
query_28="SELECT year \
	,quarter \
	,CASE \
        WHEN LEFT(quarter, 2) = 'Q1' \
			THEN 'Q1' \
		WHEN LEFT(quarter, 2) = 'Q2' \
			THEN 'Q2' \
		WHEN LEFT(quarter, 2) = 'Q3' \
			THEN 'Q3' \
		WHEN LEFT(quarter, 2) = 'Q4' \
			THEN 'Q4' \
		END AS quarters \
	,CAST(ROUND((COALESCE(SUM(p50), 0) / 1000), 3) AS DECIMAL(10, 3)) AS ProdWP \
FROM dash.p50p90asset \
WHERE projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'éolien' \
		) \
GROUP BY year \
	,quarter \
ORDER BY year \
	,quarter;"
query_results_28 = pd.read_sql(query_28, postgressql_engine()) 

#------------------------------------------------------solar/month
query_29="SELECT year \
    ,month \
	,CASE \
        WHEN month = 1 \
			THEN 'jan' \
		WHEN month = 2 \
			THEN 'feb' \
		WHEN month = 3 \
			THEN 'mar' \
		WHEN month = 4 \
			THEN 'apr' \
		WHEN month = 5 \
			THEN 'may' \
		WHEN month = 6 \
			THEN 'jun' \
		WHEN month = 7 \
			THEN 'jul' \
		WHEN month = 8 \
			THEN 'aug' \
		WHEN month = 9 \
			THEN 'sep' \
		WHEN month = 10 \
			THEN 'oct' \
		WHEN month = 11 \
			THEN 'nov' \
		WHEN month = 12 \
			THEN 'dec' \
		END AS months \
	,CAST(ROUND((COALESCE(SUM(p50), 0) / 1000), 3) AS DECIMAL(10, 3)) AS ProdSolar \
FROM dash.p50p90asset \
WHERE projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'solaire' \
		) \
GROUP BY year, month \
ORDER BY year, month;"
query_results_29 = pd.read_sql(query_29, postgressql_engine())

#------------------------------------------------------wind Power/month
query_30="SELECT year \
	,month \
	,CASE \
        WHEN month = 1 \
            THEN 'jan' \
		WHEN month = 2 \
			THEN 'feb' \
		WHEN month = 3 \
			THEN 'mar' \
		WHEN month = 4 \
			THEN 'apr' \
		WHEN month = 5 \
			THEN 'may' \
		WHEN month = 6 \
			THEN 'jun' \
		WHEN month = 7 \
			THEN 'jul' \
		WHEN month = 8 \
			THEN 'aug' \
		WHEN month = 9 \
			THEN 'sep' \
		WHEN month = 10 \
			THEN 'oct' \
		WHEN month = 11 \
			THEN 'nov' \
		WHEN month = 12 \
			THEN 'dec' \
		END AS months \
	,CAST(ROUND((COALESCE(SUM(p50), 0) / 1000), 3) AS DECIMAL(10, 3)) AS ProdWP \
FROM dash.p50p90asset \
WHERE projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'éolien' \
		) \
GROUP BY year \
	,month \
ORDER BY year \
	,month;"
query_results_30 = pd.read_sql(query_30, postgressql_engine())

#/*
#=================================================================================================
#=============================================Exposure Solar and Eol==============================
#=================================================================================================
#*/
#---------------------------------------------exposure solar/year
query_31="SELECT asset.year \
	,asset.ProdAsset + hedge.Hedges AS ExposureSolar \
FROM ( \
	SELECT year \
		,CAST(ROUND((COALESCE(SUM(p50), 0) / 1000), 2) AS DECIMAL(10, 1)) AS ProdAsset \
	FROM dash.p50p90asset \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'solaire' \
			) \
	GROUP BY year \
	) AS asset \
INNER JOIN ( \
	SELECT year \
		,CAST(ROUND((COALESCE(SUM(p50), 0) / 1000), 2) AS DECIMAL(10, 1)) AS Hedges \
	FROM dash.p50p90hedge \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'solaire' \
			) \
	GROUP BY year \
	) AS hedge ON asset.year = hedge.year \
ORDER BY year;"
query_results_31 = pd.read_sql(query_31, postgressql_engine())

#----------------------------------------------exposure eol/year
query_32="SELECT asset.year \
	,asset.ProdAsset + hedge.Hedges AS ExposureWp \
FROM ( \
	SELECT year \
		,CAST(ROUND((COALESCE(SUM(p50), 0) / 1000), 3) AS DECIMAL(10, 3)) AS ProdAsset \
	FROM dash.p50p90asset \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'éolien' \
			) \
	GROUP BY year \
	) AS asset \
INNER JOIN ( \
	SELECT year \
		,CAST(ROUND((COALESCE(SUM(p50), 0) / 1000), 3) AS DECIMAL(10, 3)) AS Hedges \
	FROM dash.p50p90hedge \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'éolien' \
			) \
	GROUP BY year \
	) AS hedge ON asset.year = hedge.year \
ORDER BY year;"
query_results_32 = pd.read_sql(query_32, postgressql_engine())

#-----------------------------------exposure solar/quarter
query_33="SELECT asset.year \
	,asset.quarter \
	,asset.quarters \
	,asset.ProdAsset + hedge.Hedges AS ExposureSolar \
FROM ( \
	SELECT year \
		,quarter \
		,CASE \
            WHEN LEFT(quarter, 2) = 'Q1' \
				THEN 'Q1' \
			WHEN LEFT(quarter, 2) = 'Q2' \
				THEN 'Q2' \
			WHEN LEFT(quarter, 2) = 'Q3' \
				THEN 'Q3' \
			WHEN LEFT(quarter, 2) = 'Q4' \
				THEN 'Q4' \
			END AS quarters \
		,CAST(ROUND((COALESCE(SUM(p50), 0) / 1000), 2) AS DECIMAL(10, 1)) AS ProdAsset \
	FROM dash.p50p90asset \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'solaire' \
			) \
	GROUP BY year \
		,quarter \
	) AS asset \
INNER JOIN ( \
	SELECT year \
		,quarter \
		,CASE \
            WHEN LEFT(quarter, 2) = 'Q1' \
				THEN 'Q1' \
			WHEN LEFT(quarter, 2) = 'Q2' \
				THEN 'Q2' \
			WHEN LEFT(quarter, 2) = 'Q3' \
				THEN 'Q3' \
			WHEN LEFT(quarter, 2) = 'Q4' \
				THEN 'Q4' \
			END AS quarters \
		,CAST(ROUND((COALESCE(SUM(p50), 0) / 1000), 2) AS DECIMAL(10, 1)) AS Hedges \
	FROM dash.p50p90hedge \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'solaire' \
			) \
	GROUP BY year \
		,quarter \
	) AS hedge ON asset.year = hedge.year \
	AND asset.quarter = hedge.quarter \
ORDER BY year \
	,quarter;"
query_results_33 = pd.read_sql(query_33, postgressql_engine())

#-----------------------------------exposure éolien/quarter
query_34 ="SELECT asset.year \
	,asset.quarter \
	,asset.quarters \
	,asset.ProdAsset + hedge.Hedges AS ExposureWp \
FROM ( \
	SELECT year \
		,quarter \
		,CASE \
            WHEN LEFT(quarter, 2) = 'Q1' \
				THEN 'Q1' \
			WHEN LEFT(quarter, 2) = 'Q2' \
				THEN 'Q2' \
			WHEN LEFT(quarter, 2) = 'Q3' \
				THEN 'Q3' \
			WHEN LEFT(quarter, 2) = 'Q4' \
				THEN 'Q4' \
			END AS quarters \
		,CAST(ROUND((COALESCE(SUM(p50), 0) / 1000), 2) AS DECIMAL(10, 1)) AS ProdAsset \
	FROM dash.p50p90asset \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'éolien' \
			) \
	GROUP BY year \
		,quarter \
	) AS asset \
INNER JOIN ( \
	SELECT year \
		,quarter \
		,CASE \
            WHEN LEFT(quarter, 2) = 'Q1' \
				THEN 'Q1' \
			WHEN LEFT(quarter, 2) = 'Q2' \
				THEN 'Q2' \
			WHEN LEFT(quarter, 2) = 'Q3' \
				THEN 'Q3' \
			WHEN LEFT(quarter, 2) = 'Q4' \
				THEN 'Q4' \
			END AS quarters \
		,CAST(ROUND((COALESCE(SUM(p50), 0) / 1000), 2) AS DECIMAL(10, 1)) AS Hedges \
	FROM dash.p50p90hedge \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'éolien' \
			) \
	GROUP BY year \
		,quarter \
	) AS hedge ON asset.year = hedge.year \
	AND asset.quarter = hedge.quarter \
ORDER BY year \
	,quarter;"
query_results_34 = pd.read_sql(query_34, postgressql_engine())

#-----------------------------------exposure solar/month

query_35 ="SELECT asset.year, asset.month, asset.months, asset.ProdAsset + hedge.Hedges AS ExposureSolar \
FROM( \
	SELECT year, month, \
		CASE \
            WHEN month = 1 \
				THEN 'jan' \
			WHEN month = 2 \
				THEN 'feb' \
			WHEN month = 3 \
				THEN 'mar' \
			WHEN month = 4 \
				THEN 'apr' \
			WHEN month = 5 \
				THEN 'may' \
			WHEN month = 6 \
				THEN 'jun' \
			WHEN month = 7 \
				THEN 'jul' \
			WHEN month = 8 \
				THEN 'aug' \
			WHEN month = 9 \
				THEN 'sep' \
			WHEN month = 10 \
				THEN 'oct' \
			WHEN month = 11 \
				THEN 'nov' \
			WHEN month = 12 \
				THEN 'dec' \
			END AS months \
	,CAST(ROUND((COALESCE(SUM(p50), 0)/1000), 2) AS DECIMAL(10, 1)) AS ProdAsset \
        FROM dash.p50p90asset \
	WHERE projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'solaire' \
		) \
	GROUP BY year, month \
) AS asset \
INNER JOIN \
( \
	SELECT year, month, \
		CASE \
            WHEN month = 1 \
				THEN 'jan' \
			WHEN month = 2 \
				THEN 'feb' \
			WHEN month = 3 \
				THEN 'mar' \
			WHEN month = 4 \
				THEN 'apr' \
			WHEN month = 5 \
				THEN 'may' \
			WHEN month = 6 \
				THEN 'jun' \
			WHEN month = 7 \
				THEN 'jul' \
			WHEN month = 8 \
				THEN 'aug' \
			WHEN month = 9 \
				THEN 'sep' \
			WHEN month = 10 \
				THEN 'oct' \
			WHEN month = 11 \
				THEN 'nov' \
			WHEN month = 12 \
				THEN 'dec' \
			END AS months \
	,CAST(ROUND((COALESCE(SUM(p50), 0)/1000), 2) AS DECIMAL(10, 1)) AS Hedges \
    FROM dash.p50p90hedge \
	WHERE projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'solaire' \
		) \
	GROUP BY year, month \
) AS hedge ON asset.year = hedge.year AND asset.month = hedge.month \
ORDER BY year, month;"
query_results_35 = pd.read_sql(query_35, postgressql_engine())

#-----------------------------------exposure éolien/month
query_36 ="SELECT asset.year, asset.month, asset.months, asset.ProdAsset + hedge.Hedges AS ExposureWp \
FROM( \
	SELECT year, month, \
		CASE \
            WHEN month = 1 \
				THEN 'jan' \
			WHEN month = 2 \
				THEN 'feb' \
			WHEN month = 3 \
				THEN 'mar' \
			WHEN month = 4 \
				THEN 'apr' \
			WHEN month = 5 \
				THEN 'may' \
			WHEN month = 6 \
				THEN 'jun' \
			WHEN month = 7 \
				THEN 'jul' \
			WHEN month = 8 \
				THEN 'aug' \
			WHEN month = 9 \
				THEN 'sep' \
			WHEN month = 10 \
				THEN 'oct' \
			WHEN month = 11 \
				THEN 'nov' \
			WHEN month = 12 \
				THEN 'dec' \
			END AS months \
	,CAST(ROUND((COALESCE(SUM(p50), 0)/1000), 2) AS DECIMAL(10, 1)) AS ProdAsset \
    FROM dash.p50p90asset \
	WHERE projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'éolien' \
		) \
	GROUP BY year, month \
) AS asset \
INNER JOIN \
( \
	SELECT year, month, \
		CASE \
            WHEN month = 1 \
				THEN 'jan' \
			WHEN month = 2 \
				THEN 'feb' \
			WHEN month = 3 \
				THEN 'mar' \
			WHEN month = 4 \
				THEN 'apr' \
			WHEN month = 5 \
				THEN 'may' \
			WHEN month = 6 \
				THEN 'jun' \
			WHEN month = 7 \
				THEN 'jul' \
			WHEN month = 8 \
				THEN 'aug' \
			WHEN month = 9 \
				THEN 'sep' \
			WHEN month = 10 \
				THEN 'oct' \
			WHEN month = 11 \
				THEN 'nov' \
			WHEN month = 12 \
				THEN 'dec' \
			END AS months \
	,CAST(ROUND((COALESCE(SUM(p50), 0)/1000), 2) AS DECIMAL(10, 1)) AS Hedges \
    FROM dash.p50p90hedge \
	WHERE projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'éolien' \
		) \
	GROUP BY year, month \
) AS hedge ON asset.year = hedge.year AND asset.month = hedge.month \
ORDER BY year, month;"
query_results_36 = pd.read_sql(query_36, postgressql_engine())

#========================================================================
#=============Type Hedge Solar Wind Power================================
#========================================================================

#-------------Type Hedge solar/year
query_37="SELECT year \
	,CASE \
        WHEN hedgetype = 'CR16' \
			THEN 'CR' \
		WHEN hedgetype = 'CR17' \
			THEN 'CR' \
		WHEN hedgetype = 'CR' \
			THEN 'CR' \
		WHEN hedgetype = 'OA' \
			THEN 'OA' \
		WHEN hedgetype = 'PPA' \
			THEN 'PPA' \
		END AS TypeContract \
	,COALESCE(SUM(- p50), 0) / 1000 AS HedgeSolar \
FROM dash.p50p90hedge \
WHERE hedgetype IS NOT NULL \
	AND projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'solaire' \
		) \
GROUP BY year \
	,CASE \
        WHEN hedgetype = 'CR16' \
			THEN 'CR' \
		WHEN hedgetype = 'CR17' \
			THEN 'CR' \
		WHEN hedgetype = 'CR' \
			THEN 'CR' \
		WHEN hedgetype = 'OA' \
			THEN 'OA' \
		WHEN hedgetype = 'PPA' \
			THEN 'PPA' \
		END \
ORDER BY year \
	,TypeContract;"
query_results_37 = pd.read_sql(query_37, postgressql_engine())

#-------------Type Hedge éolien/year
query_38="SELECT year \
	,CASE \
        WHEN hedgetype = 'CR16' \
			THEN 'CR' \
		WHEN hedgetype = 'CR17' \
			THEN 'CR' \
		WHEN hedgetype = 'CR' \
			THEN 'CR' \
		WHEN hedgetype = 'OA' \
			THEN 'OA' \
		WHEN hedgetype = 'PPA' \
			THEN 'PPA' \
		END AS TypeContract \
	,COALESCE(SUM(- p50), 0) / 1000 AS HedgeWp \
FROM dash.p50p90hedge \
WHERE hedgetype IS NOT NULL \
	AND projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'éolien' \
		) \
GROUP BY year \
	,CASE \
        WHEN hedgetype = 'CR16' \
			THEN 'CR' \
		WHEN hedgetype = 'CR17' \
			THEN 'CR' \
		WHEN hedgetype = 'CR' \
			THEN 'CR' \
		WHEN hedgetype = 'OA' \
			THEN 'OA' \
		WHEN hedgetype = 'PPA' \
			THEN 'PPA' \
		END \
ORDER BY year \
	,TypeContract;"
query_results_38 = pd.read_sql(query_38, postgressql_engine())

#---------------------type hedge solar/quarter
query_39="SELECT year \
	,quarter \
	,CASE \
        WHEN LEFT(quarter, 2) = 'Q1' \
			THEN 'Q1' \
		WHEN LEFT(quarter, 2) = 'Q2' \
			THEN 'Q2' \
		WHEN LEFT(quarter, 2) = 'Q3' \
			THEN 'Q3' \
		WHEN LEFT(quarter, 2) = 'Q4' \
			THEN 'Q4' \
		END AS quarters \
	,CASE \
        WHEN hedgetype = 'CR16' \
			THEN 'CR' \
		WHEN hedgetype = 'CR17' \
			THEN 'CR' \
		WHEN hedgetype = 'CR' \
			THEN 'CR' \
		WHEN hedgetype = 'OA' \
			THEN 'OA' \
		WHEN hedgetype = 'PPA' \
			THEN 'PPA' \
		END AS TypeContract \
	,COALESCE(SUM(- p50), 0) / 1000 AS HedgeSolar \
FROM dash.p50p90hedge \
WHERE hedgetype IS NOT NULL \
	AND projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'solaire' \
		) \
GROUP BY year \
	,quarter \
	,CASE \
        WHEN hedgetype = 'CR16' \
			THEN 'CR' \
		WHEN hedgetype = 'CR17' \
			THEN 'CR' \
		WHEN hedgetype = 'CR' \
			THEN 'CR' \
		WHEN hedgetype = 'OA' \
			THEN 'OA' \
		WHEN hedgetype = 'PPA' \
			THEN 'PPA' \
		END \
ORDER BY year \
	,quarter \
	,TypeContract;"
query_results_39 = pd.read_sql(query_39, postgressql_engine())

#---------------------type hedge wp/quarter
query_40="SELECT year \
	,quarter \
	,CASE \
        WHEN LEFT(quarter, 2) = 'Q1' \
			THEN 'Q1' \
		WHEN LEFT(quarter, 2) = 'Q2' \
			THEN 'Q2' \
		WHEN LEFT(quarter, 2) = 'Q3' \
			THEN 'Q3' \
		WHEN LEFT(quarter, 2) = 'Q4' \
			THEN 'Q4' \
		END AS quarters \
	,CASE \
        WHEN hedgetype = 'CR16' \
			THEN 'CR' \
		WHEN hedgetype = 'CR17' \
			THEN 'CR' \
		WHEN hedgetype = 'CR' \
			THEN 'CR' \
		WHEN hedgetype = 'OA' \
			THEN 'OA' \
		WHEN hedgetype = 'PPA' \
			THEN 'PPA' \
		END AS TypeContract \
	,COALESCE(SUM(- p50), 0) / 1000 AS HedgeWp \
FROM dash.p50p90hedge \
WHERE hedgetype IS NOT NULL \
	AND projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'éolien' \
		) \
GROUP BY year \
	,quarter \
	,CASE  \
		WHEN hedgetype = 'CR16' \
			THEN 'CR' \
		WHEN hedgetype = 'CR17' \
			THEN 'CR' \
		WHEN hedgetype = 'CR' \
			THEN 'CR' \
		WHEN hedgetype = 'OA' \
			THEN 'OA' \
		WHEN hedgetype = 'PPA' \
			THEN 'PPA' \
		END \
ORDER BY year \
	,quarter \
	,TypeContract;"
query_results_40 = pd.read_sql(query_40, postgressql_engine())

#---------------------type hedge solar/month
query_41="SELECT year \
	,month \
	,CASE \
        WHEN month = 1 \
			THEN 'jan' \
		WHEN month = 2 \
			THEN 'feb' \
		WHEN month = 3 \
			THEN 'mar' \
		WHEN month = 4 \
			THEN 'apr' \
		WHEN month = 5 \
			THEN 'may' \
		WHEN month = 6 \
			THEN 'jun' \
		WHEN month = 7 \
			THEN 'jul' \
		WHEN month = 8 \
			THEN 'aug' \
		WHEN month = 9 \
			THEN 'sep' \
		WHEN month = 10 \
			THEN 'oct' \
		WHEN month = 11 \
			THEN 'nov' \
		WHEN month = 12 \
			THEN 'dec' \
		END AS months \
	,CASE \
        WHEN hedgetype = 'CR16' \
			THEN 'CR' \
		WHEN hedgetype = 'CR17' \
			THEN 'CR' \
		WHEN hedgetype = 'CR' \
			THEN 'CR' \
		WHEN hedgetype = 'OA' \
			THEN 'OA' \
		WHEN hedgetype = 'PPA' \
			THEN 'PPA' \
		END AS TypeContract \
	,COALESCE(SUM(- p50), 0) / 1000 AS HedgeSolar \
FROM dash.p50p90hedge \
WHERE hedgetype IS NOT NULL \
	AND projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'solaire' \
		) \
GROUP BY year\
	,month \
	,CASE \
        WHEN hedgetype = 'CR16' \
			THEN 'CR' \
		WHEN hedgetype = 'CR17' \
			THEN 'CR' \
		WHEN hedgetype = 'CR' \
			THEN 'CR' \
		WHEN hedgetype = 'OA' \
			THEN 'OA' \
		WHEN hedgetype = 'PPA' \
			THEN 'PPA' \
		END \
ORDER BY year \
	,month \
	,TypeContract;"
query_results_41 = pd.read_sql(query_41, postgressql_engine())

#---------------------type hedge wp/month
query_42="SELECT year \
	,month \
	,CASE \
        WHEN month = 1 \
			THEN 'jan' \
		WHEN month = 2 \
			THEN 'feb' \
		WHEN month = 3 \
			THEN 'mar' \
		WHEN month = 4 \
			THEN 'apr' \
		WHEN month = 5 \
			THEN 'may' \
		WHEN month = 6 \
			THEN 'jun' \
		WHEN month = 7 \
			THEN 'jul' \
		WHEN month = 8 \
			THEN 'aug' \
		WHEN month = 9 \
			THEN 'sep' \
		WHEN month = 10 \
			THEN 'oct' \
		WHEN month = 11 \
			THEN 'nov' \
		WHEN month = 12 \
			THEN 'dec' \
		END AS months \
	,CASE \
        WHEN hedgetype = 'CR16' \
			THEN 'CR' \
		WHEN hedgetype = 'CR17' \
			THEN 'CR' \
		WHEN hedgetype = 'CR' \
			THEN 'CR' \
		WHEN hedgetype = 'OA' \
			THEN 'OA' \
		WHEN hedgetype = 'PPA' \
			THEN 'PPA' \
		END AS TypeContract \
	,COALESCE(SUM(- p50), 0) / 1000 AS HedgeWp \
FROM dash.p50p90hedge \
WHERE hedgetype IS NOT NULL \
	AND projetid IN ( \
		SELECT DISTINCT (projetid) \
		FROM dash.asset \
		WHERE technology = 'éolien' \
		) \
GROUP BY year \
	,month \
	,CASE  \
		WHEN hedgetype = 'CR16' \
			THEN 'CR' \
		WHEN hedgetype = 'CR17' \
			THEN 'CR' \
		WHEN hedgetype = 'CR' \
			THEN 'CR' \
		WHEN hedgetype = 'OA' \
			THEN 'OA' \
		WHEN hedgetype = 'PPA' \
			THEN 'PPA' \
		END \
ORDER BY year \
	,month \
	,TypeContract;"
query_results_42 = pd.read_sql(query_42, postgressql_engine())

#==================================================================
#=================HCR Solar-Wind Power=============================
#==================================================================

#-----------------HCR solar/year
query_43="SELECT hedges.year \
	,CAST(CAST(ROUND((Hedges.HedgeSolar / Prod.ProdSolar) * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS HCRSolar \
FROM ( \
	SELECT year \
		,COALESCE(SUM(- p50), 0) AS HedgeSolar \
	FROM dash.p50p90hedge \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'solaire' \
			) \
	GROUP BY year \
	) AS Hedges \
INNER JOIN ( \
	SELECT year \
		,COALESCE(SUM(p50), 0) AS ProdSolar \
	FROM dash.p50p90asset \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
                FROM dash.asset \
			WHERE technology = 'solaire' \
			) \
	GROUP BY year \
	) AS Prod ON Hedges.year = Prod.year \
ORDER BY year;"
query_results_43 = pd.read_sql(query_43, postgressql_engine())

#-----------------HCR wp/year
query_44="SELECT hedges.year \
	,CAST(CAST(ROUND((Hedges.HedgeWp / Prod.ProdWp) * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS HCRWp \
FROM ( \
	SELECT year \
		,COALESCE(SUM(- p50), 0) AS HedgeWp \
	FROM dash.p50p90hedge \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'éolien' \
			) \
	GROUP BY year \
	) AS hedges \
INNER JOIN ( \
	SELECT year \
		,COALESCE(SUM(p50), 0) AS ProdWp \
	FROM dash.p50p90asset \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'éolien' \
			) \
	GROUP BY year \
	) AS Prod ON Hedges.year = Prod.year \
ORDER BY year;"
query_results_44 = pd.read_sql(query_44, postgressql_engine())

#-------------------------------HCR solar/quarter
query_45="SELECT hedges.year \
	,hedges.quarter \
	,hedges.quarters \
	,CAST(CAST(ROUND((Hedges.HedgeSolar / Prod.ProdSolar) * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS HCRSolar \
FROM ( \
	SELECT year \
		,quarter \
		,CASE \
			WHEN LEFT(quarter, 2) = 'Q1' \
				THEN 'Q1' \
			WHEN LEFT(quarter, 2) = 'Q2' \
				THEN 'Q2' \
			WHEN LEFT(quarter, 2) = 'Q3' \
				THEN 'Q3' \
			WHEN LEFT(quarter, 2) = 'Q4' \
				THEN 'Q4' \
			END AS quarters \
		,COALESCE(SUM(- p50), 0) AS HedgeSolar \
	FROM dash.p50p90hedge \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'solaire' \
			) \
	GROUP BY year \
        ,quarter \
            ) AS Hedges \
INNER JOIN ( \
	SELECT year \
		,quarter \
		,CASE \
            WHEN LEFT(quarter, 2) = 'Q1' \
				THEN 'Q1' \
			WHEN LEFT(quarter, 2) = 'Q2' \
				THEN 'Q2' \
			WHEN LEFT(quarter, 2) = 'Q3' \
				THEN 'Q3' \
			WHEN LEFT(quarter, 2) = 'Q4' \
				THEN 'Q4' \
			END AS quarters \
		,COALESCE(SUM(p50), 0) AS ProdSolar \
	FROM dash.p50p90asset \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'solaire' \
			) \
	GROUP BY year \
		,quarter \
	) AS prod ON Hedges.year = Prod.year \
	AND Hedges.quarter = Prod.quarter \
ORDER BY year \
	,quarter;"
query_results_45 = pd.read_sql(query_45, postgressql_engine())

#-------------------------------HCR wp/quarter
query_46="SELECT Hedges.year \
	,Hedges.quarter \
	,Hedges.quarters \
	,CAST(CAST(ROUND((Hedges.HedgeWp / Prod.ProdWp) * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS HCRWp \
FROM ( \
	SELECT year \
		,quarter \
		,CASE \
            WHEN LEFT(quarter, 2) = 'Q1' \
				THEN 'Q1' \
			WHEN LEFT(quarter, 2) = 'Q2' \
				THEN 'Q2' \
			WHEN LEFT(quarter, 2) = 'Q3' \
				THEN 'Q3' \
			WHEN LEFT(quarter, 2) = 'Q4' \
				THEN 'Q4' \
			END AS quarters \
		,COALESCE(SUM(- p50), 0) AS HedgeWp \
	FROM dash.p50p90hedge \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'éolien' \
			) \
	GROUP BY year \
		,quarter \
	) AS Hedges \
INNER JOIN ( \
	SELECT year \
		,quarter \
		,CASE \
			WHEN LEFT(quarter, 2) = 'Q1' \
                THEN 'Q1' \
			WHEN LEFT(quarter, 2) = 'Q2' \
				THEN 'Q2' \
			WHEN LEFT(quarter, 2) = 'Q3' \
				THEN 'Q3' \
			WHEN LEFT(quarter, 2) = 'Q4' \
				THEN 'Q4' \
			END AS quarters \
		,COALESCE(SUM(p50), 0) AS ProdWp \
	FROM dash.p50p90asset \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'éolien' \
			) \
	GROUP BY year \
		,quarter \
	) AS prod ON Hedges.year = Prod.year \
	AND Hedges.quarter = Prod.quarter \
ORDER BY year \
	,quarter;"
query_results_46 = pd.read_sql(query_46, postgressql_engine())
#---------------------------HCR solar/month
query_47="SELECT Hedges.year \
	,Hedges.month \
	,Hedges.months \
	,CAST(CAST(ROUND((Hedges.HedgeSolar / prod.ProdSolar) * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS HCRSolar \
FROM ( \
	SELECT year \
		,month \
		,CASE \
			WHEN month = 1 \
				THEN 'jan' \
			WHEN month = 2 \
				THEN 'feb' \
			WHEN month = 3 \
				THEN 'mar' \
			WHEN month = 4 \
				THEN 'apr' \
			WHEN month = 5 \
				THEN 'may' \
			WHEN month = 6 \
				THEN 'jun' \
			WHEN month = 7 \
				THEN 'jul' \
			WHEN month = 8 \
				THEN 'aug' \
			WHEN month = 9 \
				THEN 'sep' \
			WHEN month = 10 \
				THEN 'oct' \
			WHEN month = 11 \
				THEN 'nov' \
			WHEN month = 12 \
				THEN 'dec' \
			END AS months \
		,COALESCE(SUM(- p50), 0) AS HedgeSolar \
	FROM dash.p50p90hedge \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'solaire' \
			) \
	GROUP BY year \
		,month \
	) AS Hedges \
INNER JOIN ( \
	SELECT year \
		,month \
		,CASE \
			WHEN month = 1 \
				THEN 'jan' \
			WHEN month = 2 \
				THEN 'feb' \
			WHEN month = 3 \
				THEN 'mar' \
			WHEN month = 4 \
				THEN 'apr' \
			WHEN month = 5 \
				THEN 'may' \
			WHEN month = 6 \
				THEN 'jun' \
			WHEN month = 7 \
				THEN 'jul' \
			WHEN month = 8 \
				THEN 'aug' \
			WHEN month = 9 \
				THEN 'sep' \
			WHEN month = 10 \
				THEN 'oct' \
			WHEN month = 11 \
				THEN 'nov' \
			WHEN month = 12 \
				THEN 'dec' \
			END AS months \
		,COALESCE(SUM(p50), 0) AS ProdSolar \
	FROM dash.p50p90asset \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'solaire' \
			) \
	GROUP BY year \
		,month \
	) AS prod ON Hedges.year = Prod.year \
	AND Hedges.month = Prod.month \
ORDER BY year \
	,month;"
query_results_47 = pd.read_sql(query_47, postgressql_engine())

#---------------------------HCR solar/month
query_48="SELECT Hedges.year \
	,Hedges.month \
	,Hedges.months \
	,CAST(CAST(ROUND((Hedges.HedgeWp / Prod.ProdWp) * 100, 2) AS DECIMAL(5, 2)) AS VARCHAR(10)) || '%%' AS HCRWp \
FROM ( \
	SELECT year \
		,month \
		,CASE \
            WHEN month = 1 \
				THEN 'jan' \
			WHEN month = 2 \
				THEN 'feb' \
			WHEN month = 3 \
				THEN 'mar' \
			WHEN month = 4 \
				THEN 'apr' \
			WHEN month = 5 \
				THEN 'may' \
			WHEN month = 6 \
				THEN 'jun' \
			WHEN month = 7 \
				THEN 'jul' \
			WHEN month = 8 \
				THEN 'aug' \
			WHEN month = 9 \
				THEN 'sep' \
			WHEN month = 10 \
				THEN 'oct' \
			WHEN month = 11 \
				THEN 'nov' \
			WHEN month = 12 \
				THEN 'dec' \
			END AS months \
		,COALESCE(SUM(- p50), 0) AS HedgeWp \
	FROM dash.p50p90hedge \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset \
			WHERE technology = 'éolien' \
			) \
	GROUP BY year \
		,month \
	) AS Hedges \
INNER JOIN ( \
	SELECT year \
		,month \
		,CASE \
			WHEN month = 1 \
				THEN 'jan' \
			WHEN month = 2 \
				THEN 'feb' \
			WHEN month = 3 \
				THEN 'mar' \
			WHEN month = 4 \
				THEN 'apr' \
			WHEN month = 5 \
				THEN 'may' \
			WHEN month = 6 \
				THEN 'jun' \
			WHEN month = 7 \
				THEN 'jul' \
			WHEN month = 8 \
				THEN 'aug' \
			WHEN month = 9 \
				THEN 'sep' \
			WHEN month = 10 \
				THEN 'oct' \
			WHEN month = 11 \
				THEN 'nov' \
			WHEN month = 12 \
				THEN 'dec' \
			END AS months \
		,COALESCE(SUM(p50), 0) AS ProdWp \
	FROM dash.p50p90asset \
	WHERE projetid IN ( \
			SELECT DISTINCT (projetid) \
			FROM dash.asset  \
			WHERE technology = 'éolien'  \
			)  \
	GROUP BY year  \
		,month  \
	) AS prod ON Hedges.year = Prod.year  \
	AND Hedges.month = Prod.month  \
ORDER BY year  \
	,month;"
query_results_48 = pd.read_sql(query_48, postgressql_engine())

#======================================
#==========     MtM     ===============
#======================================

#===========MtM year===============
query_49="SELECT h.year,\
	CAST(ROUND(SUM(- h.p50 * (cp.contractprice - mp.settlementprice))/1000000, 2) AS DECIMAL(20, 3)) AS MtM \
FROM dash.p50p90hedge AS h \
INNER JOIN dash.contractprice AS cp ON h.projetid = cp.projetid \
	AND h.hedgeid = cp.hedgeid \
	AND h.year = cp.year \
	AND CAST(SUBSTRING(h.quarter, 2, 1) AS INTEGER) = cp.quarter \
	AND h.month = cp.month \
INNER JOIN dash.marketprice AS mp ON h.projetid = mp.projetid \
	AND h.hedgeid = mp.hedgeid \
	AND h.year = mp.year \
	AND CAST(SUBSTRING(h.quarter, 2, 1) AS INTEGER) = mp.quarter \
	AND h.month = mp.month \
GROUP BY h.year \
ORDER BY h.year;"
query_results_49 = pd.read_sql(query_49, postgressql_engine())

#===========MtM Portfolio Merchant ============
query_50="SELECT h.year \
	,CAST(ROUND(SUM(- h.p50 * (cp.contractprice - mp.settlementprice)) / 1000000, 2) AS DECIMAL(20, 3)) AS MtM \
FROM dash.p50p90hedge AS h \
INNER JOIN dash.contractprice AS cp ON h.projetid = cp.projetid \
	AND h.hedgeid = cp.hedgeid \
    AND h.year = cp.year \
	AND CAST(SUBSTRING(h.quarter, 2, 1) AS INTEGER) = cp.quarter \
	AND h.month = cp.month \
INNER JOIN dash.marketprice AS mp ON h.projetid = mp.projetid \
	AND h.hedgeid = mp.hedgeid \
	AND h.year = mp.year \
	AND CAST(SUBSTRING(h.quarter, 2, 1) AS INTEGER) = mp.quarter \
	AND h.month = mp.month \
WHERE h.hedgetype = 'PPA' \
	OR h.hedgetype IS NULL \
GROUP BY h.year \
ORDER BY h.year;"
query_results_50 = pd.read_sql(query_50, postgressql_engine())

#===========MtM Portfolio Reguled ============
query_51="SELECT h.year \
	,CAST(ROUND(SUM(- h.p50 * (cp.contractprice - mp.settlementprice)) / 1000000, 2) AS DECIMAL(20, 3)) AS MtM \
FROM dash.p50p90hedge AS h \
INNER JOIN dash.contractprice AS cp ON h.projetid = cp.projetid \
	AND h.hedgeid = cp.hedgeid \
	AND h.year = cp.year \
	AND CAST(SUBSTRING(h.quarter, 2, 1) AS INTEGER) = cp.quarter \
	AND h.month = cp.month \
INNER JOIN dash.marketprice AS mp ON h.projetid = mp.projetid \
	AND h.hedgeid = mp.hedgeid \
	AND h.year = mp.year \
	AND CAST(SUBSTRING(h.quarter, 2, 1) AS INTEGER) = mp.quarter \
	AND h.month = mp.month \
WHERE h.hedgetype != 'PPA' \
	OR h.hedgetype IS NULL \
GROUP BY h.year \
ORDER BY h.year;"
query_results_51 = pd.read_sql(query_51, postgressql_engine())

#===========MtM Portfolio history ============
query_52="SELECT cotationdate, \
       mtm \
FROM dash.mtm \
ORDER BY cotationdate ASC;"
query_results_52 = pd.read_sql(query_52, postgressql_engine())