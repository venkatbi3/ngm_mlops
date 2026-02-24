    WITH
    as_of_dates AS (
      SELECT dataset_type,
      WHEN dataset_type = 'train' THEN GENERATE_DATE_ARRAY('{start_date_train}', '{end_date_train', INTERVAL 1 MONTH) -- 3m train 
      WHEN dataset_type = 'test' THEN GENERATE_DATE_ARRAY('{start_date_test}', '{end_date_test}', INTERVAL 1 MONTH) -- 3m test
    END as as_of_dates)
    ,monthly_repayments AS (
      SELECT ACC_NO
        ,NORM_PYMT
      FROM src_dwh_source.host_LOAN -- {source_catalog}.{source_schema}
      WHERE (RowEndDate>DATE_SUB(d.as_of_date, INTERVAL 1 DAY ) OR RowEndDate IS NULL)
        AND DATE_SUB(d.as_of_date, INTERVAL 1 DAY ) between RowStartDate AND IFNULL(RowEndDate,CURRENT_DATE())
        AND NORM_PYMT IS NOT NULL
    )
    ,base_loan_accounts AS (
      SELECT mai.Brand
        ,mai.AccountNumber
        ,mai.CreationDateTimeAEDT
        ,mai.ClosedDateTimeAEDT
        ,mai.MaturityDate
        ,mai.RepaymentTypeDescription
        ,mai.ApprovedAmount
        ,mai.FinancedAmount
        ,CONCAT(FLOOR(mai.FinancedAmount / 50000) * 50000, '-', (FLOOR(mai.FinancedAmount / 50000) * 50000) + 49999) AS FinancedAmountBucket
        ,mai.BalanceAmount
        ,mai.ActualLoanValueRatio
        ,CASE 
          WHEN LoanProductTypeDescription IN ('Home Deposit', 'Buy home - established (1st buyer)', 'Buy Home - new (1st buyer)', 'Home Construction (1st buyer)') THEN 'first home purchase'
          WHEN LoanProductTypeDescription IN ('Buy Home - established (not 1st buyer)', 'Buy Home - New (not first buyer)', 'Home Construction (not 1st buyer)', 'Purchase Land', 'Purchase and Construct', 'Residential Freehold Torrens Title', 'Purchase Dwelling') THEN 'home Purchase'
          WHEN LoanProductTypeDescription IN ('Refinance Home Mortgage', 'Home Improvements') THEN 'refinancing - house'
          WHEN LoanProductTypeDescription IN ('Holiday Expenses', 'Medical Expenses', 'Dental Expenses', 'Education Expenses', 'Pay Tax') THEN 'refinancing - living expenses'
          WHEN LoanProductTypeDescription IN ('Debt Consolidation', 'Debt Minimization') THEN 'refinancing - debt management'
          WHEN LoanProductTypeDescription IN ('Purchase Vehicle - Used', 'Purchase motor cycle', 'Purchase Trailer', 'Purchase Vehicle - NEW', 'Motor Vehicle Expenses') THEN 'refinancing - vehicle'
          WHEN LoanProductTypeDescription IN ('Substitution of Security Extra Funds', 'Line of Credit', 'Property Settlement', 'Property Transfer', 'Legal Fees', 'Wedding Expenses', 'Travel Requirements', 'Household Items', 'Miscellaneous', 'Recreation Expenses', 'Purchase boat', 'Purchase Caravan') THEN 'refinancing - personal'
          WHEN LoanProductTypeDescription IN ('Buy Commerical R/E', 'Business Capital', 'Purchase Shares', 'Wealth Creation', 'Buy Home Investment') THEN 'business'
          ELSE LoanProductTypeDescription
        END AS LoanCategory
        ,mai.InAdvanceAmount
        ,mai.ArrearsAmount
        ,mai.AvailableForRedrawAmount
        ,mai.InactiveMonthsCount
        ,mai.InterestRate
        ,mr.NORM_PYMT AS CurrentMonthlyInstallment 
      FROM Integration.Account mai --{source_catalog}.{source_schema}
      LEFT JOIN monthly_repayments mr
        ON CAST(mai.AccountNumber AS INT64)= mr.ACC_NO
      WHERE mai.EndOfDayDate = DATE_SUB(d.as_of_date, INTERVAL 1 DAY)
        AND Brand = 'NP'
        AND source = 'HOST_CBS'
        AND GeneralAccountTypeDescription = 'Home Loan'
        AND InterestRateTypeDescription = 'Variable'
        AND AccountStatusType = 'Open'
        AND BalanceAmount >= 100000
        AND DATE_DIFF(DATE_SUB(d.as_of_date, INTERVAL 1 DAY ), CreationDateTimeAEDT, DAY) >= 200 -- considerin loans that are atleast 6 month old   
    ) 
    ,client_account_mapping AS (
      SELECT AccountNumber
        ,ClientNumber
        ,SimpleWeighting
        ,AccountStatusType
        ,GeneralAccountTypeDescription
      FROM Integration.ClientAccountRelationship car
      WHERE EndOfDayDate = DATE_SUB(d.as_of_date, INTERVAL 1 DAY)
        AND Brand = 'NP'
        AND Source = 'HOST_CBS'
    )
    ,client_details as (
      SELECT car.AccountNumber
      ,COUNT(DISTINCT car.ClientNumber) AS NumClients
        ,MAX(DATE_DIFF(DATE_SUB(d.as_of_date, INTERVAL 1 DAY ), c.BirthDate, YEAR) 
        - IF(
            EXTRACT(DAYOFYEAR FROM DATE_SUB(d.as_of_date, INTERVAL 1 DAY )) < EXTRACT(DAYOFYEAR FROM c.BirthDate)
            , 1, 0 )) AS MaxAge
        ,IF(COUNT(DISTINCT GenderCodeDescription)>1, ARRAY['Couple'], ARRAY_AGG(GenderCodeDescription)) AS Gendercomposition
      FROM client_account_mapping car
      LEFT JOIN Integration.Client c
        ON car.ClientNumber = c.ClientNumber
      WHERE c.EndOfDayDate = DATE_SUB(d.as_of_date, INTERVAL 1 DAY)
        AND c.Brand = 'NP'
        AND c.source = 'HOST_CBS'
      GROUP BY 1 
    )
    ,securities AS(
      SELECT sar.AccountNumber
        ,SUM(ValueAmount) AS TotalSecurityValue
        ,MAX(ValueAmount) AS MaxSecurityValue
        -- ,SUM(PledgedAmount) AS PledgedSecurityValue
        ,ARRAY_AGG(PostCode ORDER BY sar.PledgedAmount DESC)[OFFSET(0)] AS PostCode
        --,ARRAY_AGG(STRUCT(s.SecurityNumber, CategoryType, PostCode, ValueAmount)) SecurityDetails
      FROM Integration.SecurityAccountRelationship sar
      LEFT JOIN Integration.Security s
        ON sar.SecurityNumber = s.SecurityNumber
          AND sar.Brand = 'NP'
          AND sar.Source = 'HOST_CBS'
          AND sar.Brand = s.Brand
          AND sar.Source = s.source
      WHERE s.EndOfDayDate = DATE_SUB(d.as_of_date, INTERVAL 1 DAY)
        AND sar.EndOfDayDate = DATE_SUB(d.as_of_date, INTERVAL 1 DAY)
        AND sar.Brand = 'NP'
        AND sar.Source = 'HOST_CBS'
        AND s.Brand = 'NP'
        AND s.Source = 'HOST_CBS'
      GROUP BY 1
    )
    ,account_adress_changes AS ( 
      SELECT -- Final query : converts to account level 
        car.AccountNumber  
        ,IF(SUM(cac.AddressChangedIn6M)>0,1,0) AS AddressChangedIn6M
      FROM (SELECT -- Sub-query 2: groups those at client level
              ClientNumber 
              ,SUM(AddressChanged) AS AddressChangedIn6M
            FROM (SELECT -- Sub-query 1: queries 6 month address for each client & checks for changes
                    ClientNumber 
                    ,EndOfDayDate
                    ,StreetAddressName1
                    ,CAST((StreetAddressName1 != LAG(StreetAddressName1) OVER(PARTITION BY ClientNumber, AddressTypeDescription ORDER BY EndOfDayDate)) AS INT64) AS AddressChanged
                  FROM Integration.ClientAddressRelationship
                  WHERE DATE_ADD(EndOfDayDate, INTERVAL 1 day) in UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(d.as_of_date , INTERVAL 5 MONTH), d.as_of_date, INTERVAL 1 MONTH))
                    AND Brand = 'NP'
                    AND Source = 'HOST_CBS')
            GROUP BY 1) cac
      LEFT JOIN client_account_mapping car
        ON cac.ClientNumber = car.ClientNumber
          AND car.GeneralAccountTypeDescription = 'Home Loan'
      GROUP BY 1
    ) 
    ,offset_balances AS (
      SELECT oa.LoanAccountNumber
        ,oa.EndOfDayDate
        ,SUM(ab.BalanceAmount) AS OffsetBalance
      FROM (SELECT AccountNumber AS LoanAccountNumber
            ,EndOfDayDate
            ,OffsetAccountNumber
          FROM Integration.LoanAccountOffsetAccountRelationship 
          WHERE DATE_ADD(EndOfDayDate, INTERVAL 1 day) in UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(d.as_of_date , INTERVAL 5 MONTH), d.as_of_date, INTERVAL 1 MONTH))
            AND GeneralAccountTypeDescription = 'Home Loan'
            AND Brand = 'NP'
            AND Source = 'HOST_CBS') oa
      LEFT JOIN Integration.Account ab
        ON oa.OffsetAccountNumber = ab.AccountNumber
          AND oa.EndOfDayDate = ab.EndOfDayDate
          AND ab.Brand = 'NP'
          AND ab.Source = 'HOST_CBS'
      WHERE DATE_ADD(ab.EndOfDayDate, INTERVAL 1 day) in UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(d.as_of_date , INTERVAL 5 MONTH), d.as_of_date, INTERVAL 1 MONTH))
        AND ab.Brand = 'NP'
        AND ab.Source = 'HOST_CBS'
      GROUP BY 1, 2
      ORDER BY 1, 2
    )
    ,other_accounts AS (
      SELECT bla.AccountNumber
        ,ARRAY_AGG(DISTINCT car2.AccountNUmber) DistinctAccounts
      FROM base_loan_accounts bla
      LEFT JOIN client_account_mapping car1
        ON bla.AccountNumber = car1.AccountNumber
      LEFT JOIN client_account_mapping car2
        ON car1.ClientNumber = car2.ClientNumber
          AND car2.GeneralAccountTypeDescription IN ('Transaction', 'Savings', 'Term Share', 'Home Loan', 'Credit Card', 'Personal Loan')
      GROUP BY 1
    )
    ,other_account_details As (
      SELECT oa.AccountNumber
        ,COUNT(DISTINCT (IF(a.AccountStatusType = 'Open', related_account, NULL))) OtherOpenAccounts
        ,COUNT(DISTINCT (IF(a.AccountStatusType = 'Closed', related_account, NULL))) AccountsClosedInLast3M
        ,SUM(IF(a.GeneralAccountTypeDescription IN ('Savings','Transaction', 'Term Share') AND a.AccountStatusType = 'Open', a.BalanceAmount, NULL)) CurrentHoldings
        ,SUM(IF(a.GeneralAccountTypeDescription IN ('Personal Loan', 'Home Loan') AND a.AccountStatusType = 'Open', a.BalanceAmount, NULL)) CurrentlyOwingLoans
        -- ,SUM(IF(a.GeneralAccountTypeDescription = 'Credit Card' AND a.AccountStatusType = 'Open', a.BalanceAmount, NULL)) CurrentlyOwingCC
        ,COUNT(DISTINCT (IF(a.AccountStatusType = 'Open' AND a.GeneralAccountTypeDescription ='Transaction', related_account, NULL))) NumTransactionAccounts
        ,COUNT(DISTINCT (IF(a.AccountStatusType = 'Open' AND a.GeneralAccountTypeDescription IN ('Savings', 'Term Share'), related_account, NULL))) NumSavingsAccounts
        ,COUNT(DISTINCT (IF(a.AccountStatusType = 'Open' AND a.GeneralAccountTypeDescription IN ('Credit Card','Personal Loan', 'Home Loan'), related_account, NULL))) NumLendingAccounts
      FROM other_accounts oa, UNNEST(DistinctAccounts) AS related_account
      LEFT JOIN Integration.Account a
        ON related_account = a.AccountNumber
          AND a.Brand = 'NP'
          AND a.Source = 'HOST_CBS'
      WHERE a.EndOfDayDate = DATE_SUB(d.as_of_date, INTERVAL 1 DAY)
        AND  (a.AccountStatusType = 'Open' OR DATE_DIFF(DATE_SUB(d.as_of_date, INTERVAL 1 DAY),a.ClosedDateTimeAEDT,MONTH) BETWEEN 0 AND 6)
      GROUP BY 1
    ) 
    ,logins_rel_change AS (
      SELECT AccountNumber -- main query3: calculating relative change
        ,SAFE_DIVIDE((MIN(IF(CalcWindow = 'recent 3 months', WebLogins_Avg,NULL)) - MIN(IF(CalcWindow = '6 months before recent 3 months', WebLogins_Avg,NULL)))
            ,MIN(IF(CalcWindow = '6 months before recent 3 months', WebLogins_Avg,NULL))) AS WebLoginsRelChange
        ,SAFE_DIVIDE((MIN(IF(CalcWindow = 'recent 3 months', Applogins_Avg,NULL)) - MIN(IF(CalcWindow = '6 months before recent 3 months', Applogins_Avg,NULL)))
            ,MIN(IF(CalcWindow = '6 months before recent 3 months', Applogins_Avg,NULL))) ApploginsRelChange
      FROM (SELECT -- sub-query2: aggregating to last 3 onth and 6 months before last 3 months to enable rel change calcs
              AccountNumber
              ,CASE
                WHEN DATE_DIFF(DATE_SUB(d.as_of_date, INTERVAL 1 DAY ), LoginMonth, MONTH) BETWEEN 0 AND 2 THEN 'recent 3 months'
                WHEN DATE_DIFF(DATE_SUB(d.as_of_date, INTERVAL 1 DAY ), LoginMonth, MONTH) BETWEEN 3 AND 8 THEN '6 months before recent 3 months'
              END AS CalcWindow
              ,AVG(WebLogins) WebLogins_Avg
              ,AVG(Applogins) Applogins_Avg
            FROM (SELECT -- sub-query1: month wise logins for each account
                    car.AccountNumber
                    ,DATE_TRUNC(cc.EndOfDayDate, MONTH) AS LoginMonth
                    ,SUM(cc.InternetBankingLogonCountMTD) AS WebLogins
                    ,SUM(cc.MobileBankingLogonCountMTD) AS Applogins
                  FROM client_account_mapping car
                  LEFT JOIN Access.ClientCalculation cc
                    ON car.ClientNumber = cc.ClientNumber
                  WHERE DATE_ADD(cc.EndOfDayDate, INTERVAL 1 day) in UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(d.as_of_date , INTERVAL 9 MONTH), d.as_of_date, INTERVAL 1 MONTH)) 
                    AND cc.Brand = 'NP'
                    AND cc.Source = 'HOST_CBS'
                  GROUP BY 1,2)
            GROUP BY 1, 2)
      GROUP BY 1
    )
    ,income_data AS (
      SELECT
        ba.AccountNumber ,  lan.ApplicationNumber
        ,lad.repaymentAmount
        ,IF(lad.totalIncomeValue<100, NULL, lad.totalIncomeValue) AS MonthlyIncome
        ,lad.totalExpenseValue AS MonthlyExpenses
        -- ,ROW_NUMBER() OVER(PARTITION BY ba.AccountNumber ORDER BY ABS(DATE_DIFF(DATE_SUB(d.as_of_date, INTERVAL 1 DAY ),lad.RowEndDate, DAY)) ) AS ServicabilityInstance
      FROM base_loan_accounts ba
      LEFT JOIN (SELECT ACC_NO
                  ,CASE 
                    WHEN length(ALPS_APPL_NO) = 8 THEN SUBSTR(ALPS_APPL_NO,3)
                    WHEN length(ALPS_APPL_NO) = 9 THEN SUBSTR(ALPS_APPL_NO,4) 
                    ELSE ALPS_APPL_NO
                  END AS ApplicationNumber
                FROM src_dwh_source.host_LOANHIST
                WHERE PROC_DATE <= CAST(FORMAT_DATE('%Y%m%d',DATE_SUB(d.as_of_date, INTERVAL 1 DAY))AS INT64)
                  -- AND PARSE_DATE('%Y%m%d',CAST(PROC_DATE AS STRING)) <= DATE_SUB(d.as_of_date, INTERVAL 1 DAY )
                  AND LENGTH(CAST(PROC_DATE AS STRING))=8
                  AND ALPS_APPL_NO IS NOT NULL
                QUALIFY ROW_NUMBER() OVER(PARTITION BY ACC_NO ORDER BY PARSE_DATE('%Y%m%d',CAST(PROC_DATE AS STRING)) DESC) =1
                ) lan
        ON SUBSTR(ba.AccountNumber,2) = CAST(lan.ACC_NO AS STRING)
      LEFT JOIN (SELECT CAST(sr.applicationId AS STRING) AS ApplicationNumber
                  ,sr.RowEndDate
                  ,sr.repaymentAmount
                  ,sr.totalIncomeValue
                  ,sr.totalExpenseValue
                FROM src_dwh_source.Lendfast_ServiceabilityResult sr
                WHERE (RowEndDate>DATE_SUB(d.as_of_date, INTERVAL 1 DAY ) OR RowEndDate IS NULL)
                  AND DATE_SUB(d.as_of_date, INTERVAL 1 DAY ) BETWEEN RowStartDate AND IFNULL(RowEndDate,CURRENT_DATE())
                  AND sr.type = 'APPLICANT'
                ) lad
      ON lan.ApplicationNumber = lad.ApplicationNumber
    )
    ,client_repayments_after_current_loan AS ( -- Calculating repayments for loans taken after current home loan
      SELECT bla.AccountNumber 
        ,SUM(cam2.SimpleWeighting * mr.NORM_PYMT) AS AdditionalRepayments
      FROM base_loan_accounts bla -- home loans that are in this run
      LEFT JOIN client_account_mapping cam 
        ON bla.AccountNumber = cam.AccountNumber --- obtaining all the clients on current home loans.
      LEFT JOIN client_account_mapping cam2
        ON cam.ClientNumber = cam2.ClientNumber -- obtaining all the additional loans related to the clients
      LEFT JOIN Integration.Account a -- obtaining details of these additional loans
        ON cam2.AccountNumber = a.AccountNumber
      LEFT JOIN monthly_repayments mr -- obtaining monthly repayment on all those additional loans
        ON CAST(a.AccountNumber AS INT64) = mr.ACC_NO
      WHERE a.EndOfDayDate = DATE_SUB(d.as_of_date, INTERVAL 1 DAY ) -- because we want details of this additional loan on as_of_date
        AND cam2.GeneralAccountTypeDescription IN ('Home Loan', 'Commercial Loan', 'Personal Loan')
        AND a.AccountStatusType = 'Open'
        AND a.CreationDateTimeAEDT > bla.CreationDateTimeAEDT AND a.CreationDateTimeAEDT <= DATE_SUB(d.as_of_date, INTERVAL 1 DAY ) -- only choosing additional loans after current loan and before as_of_date
      GROUP BY 1
    )
    ,account_repayments AS (
      SELECT bla.AccountNumber
        ,SUM(Repayment) AS TotalMonthlyRepayments
      FROM base_loan_accounts bla
      LEFT JOIN client_account_mapping car
        ON bla.AccountNumber = car.AccountNumber
      LEFT JOIN (SELECT
                  cam.ClientNumber
                  ,SUM(mr.NORM_PYMT* cam.SimpleWeighting) AS Repayment
                FROM client_account_mapping cam
                LEFT JOIN `Integration.Account` a
                  ON cam.AccountNumber = a.AccountNumber
                LEFT JOIN monthly_repayments mr -- obtaining monthly repayment on all those additional loans
                  ON CAST(a.AccountNumber AS INT64) = mr.ACC_NO
                WHERE cam.AccountStatusType = 'Open'
                  AND cam.GeneralAccountTypeDescription IN ('Home Loan', 'Commercial Loan', 'Personal Loan')
                  AND a.EndOfDayDate = DATE_SUB(d.as_of_date, INTERVAL 1 DAY ) 
                GROUP BY 1) icr
        ON car.ClientNumber = icr.ClientNumber
      GROUP BY 1
    )
    ,occupation_codes AS (
      SELECT la.AccountNumber
        ,cam.ClientNumber
        ,hc.OCC_CDE AS OccupationCode
      FROM base_loan_accounts la
      LEFT JOIN client_account_mapping cam 
        ON la.AccountNumber = cam.AccountNumber
      LEFT JOIN (SELECT --Sub query1: getting client occupation codes
                  CUS_NO, OCC_CDE FROM src_dwh_source.host_CUSTOMER
                  WHERE (RowEndDate>=DATE_SUB(d.as_of_date, INTERVAL 1 DAY ) OR RowEndDate IS NULL)
                    AND DATE_SUB(d.as_of_date, INTERVAL 1 DAY ) between RowStartDate AND IFNULL(RowEndDate,CURRENT_DATE())
                )hc
        ON CAST(cam.ClientNumber AS INT64) = hc.CUS_NO
    ) 
    ,occupation_avg_income AS (
      SELECT oc.OccupationCode
        ,AVG(id.MonthlyIncome) AS AvgMonthlyIncome
        ,AVG(MonthlyExpenses) AS AvgMonthlyExpenses
      FROM occupation_codes oc 
      LEFT JOIN client_details cd
        ON oc.AccountNumber = cd.AccountNumber
      LEFT JOIN income_data id
        ON oc.AccountNumber = id.AccountNumber
      WHERE cd.NumClients =1
      GROUP BY 1
    ) 
    ,income_data_imputation AS (
      SELECT id.AccountNumber
        ,COALESCE(id.MonthlyIncome,idest.MonthlyIncomeEstimation) as IncomeMonthly
        ,COALESCE(id.MonthlyExpenses, idest.MonthlyExpenseEstimation) as ExpensesMonthly
      FROM income_data id 
      LEFT JOIN (SELECT oc.AccountNumber
                  ,SUM(oai.AvgMonthlyIncome) AS MonthlyIncomeEstimation
                  ,SUM(oai.AvgMonthlyExpenses) AS MonthlyExpenseEstimation
                FROM occupation_codes oc 
                LEFT JOIN occupation_avg_income oai
                  ON oc.OccupationCode = oai.OccupationCode
                GROUP BY 1) idest
        ON id.AccountNumber = idest.AccountNumber
    )
    SELECT DATE_SUB(d.as_of_date, INTERVAL 1 DAY) AS as_of_date
     -- Generic details
      ,la.AccountNumber
      ,la.LoanCategory
      ,la.RepaymentTypeDescription
      ,car.Gendercomposition[OFFSET(0)] AS Gender
      ,la.BalanceAmount AS LoanLeftToPay
      ,hlr.MtdInterestCharge
      ,hlr.MtdLoanRunoff
     -- Maturing loans
      ,FinancedAmountBucket
      ,DATE_DIFF(DATE_SUB(d.as_of_date, INTERVAL 1 DAY ), la.CreationDateTimeAEDT, MONTH)/ DATE_DIFF(la.MaturityDate, la.CreationDateTimeAEDT, MONTH) AS PercTenureCompleted
      ,GREATEST(((la.FinancedAmount - (la.BalanceAmount+la.InAdvanceAmount))/ la.FinancedAmount),0) AS PercMortgagePaid
      ,car.MaxAge
     -- Selling
      ,(scs.TotalSecurityValue- la.BalanceAmount) AS TotalEquity
      ,mtsp.percentage_stock_on_market --supply
      ,mtsp.Median_sales_price_last_12_months --sales trend
      ,mtsp.Median_days_on_market_for_properties_sold_in_last_12_months -- market liquidity
      ,mtsp.Average_hold_period --resale activity
      ,adc.AddressChangedIn6M
     -- Refinancing
      -- Financial situation
        ,scs.MaxSecurityValue - Median_sales_price_last_6_months AS SecurityCompetitiveness
        ,oad.CurrentHoldings
        ,oad.CurrentlyOwingLoans
        ,(id.IncomeMonthly - (id.ExpensesMonthly + la.CurrentMonthlyInstallment + IFNULL(cracl.AdditionalRepayments,0))) AS IncomeLeft
        ,SAFE_DIVIDE(ar.TotalMonthlyRepayments , id.IncomeMonthly) AS DTI
        ,SAFE_DIVIDE(id.IncomeMonthly - (id.ExpensesMonthly + la.CurrentMonthlyInstallment + IFNULL(cracl.AdditionalRepayments,0)), ar.TotalMonthlyRepayments ) AS NSR
      -- Transaction Indicators
        ,la.InAdvanceAmount AS CurrentAdvanceAmount
        ,la.ArrearsAmount AS CurrentArrearsAmount
        ,nvaf.BalanceSlope
        ,nvaf.AdvanceSlope
        ,nvaf.ArrearsSlope
        ,nvaf.RatesSlope
        ,nvaf.LVRSlope
        ,nvaf.OffsetBalanceSlope
        ,nvaf.BalanceRelChange
        ,nvaf.AdvanceRelChange
        ,nvaf.RateRelChange
        ,nvaf.LvrRelChange
        ,nvaf.OffsetBalRelChange
        ,NTILE(5) OVER(ORDER BY la.InterestRate) AS InterestRateDecile
        ,(la.InterestRate *100 - rba.FLRHOOVA) AS DiffFromMarketrate
     -- Debt restructuring 
      ,la.ActualLoanValueRatio
      ,nvtf.Savings_Credit_Slope
      ,nvtf.Savings_Debit_Slope
      ,nvtf.Lending_Credit_Slope
      ,nvtf.Lending_Debit_Slope
      ,nvtf.SavingsDebitRelChange
      ,nvtf.LendingDebitRelChange
       -- Loyalty
        ,oad.OtherOpenAccounts
        ,oad.NumTransactionAccounts
        ,oad.NumSavingsAccounts
        ,oad.NumLendingAccounts
        ,IFNULL(nvtf.HasHomeLoanPayments, false) AS HasHomeLoanPayments
        -- ,IFNULL(nvtf.HasOtherLoanPayments, false) AS HasOtherLoanPayments
        -- ,IFNULL(nvtf.HasLargePayments, false) AS HasLargePayments
        ,IFNULL(nvtf.LoanPaymentsFromExternalBanks, false) AS LoanPaymentsFromExternalBanks
     -- Other Indicators of churn
      ,oad.AccountsClosedInLast3M
      ,la.InactiveMonthsCount
      ,lrc.WebLoginsRelChange
      ,lrc.ApploginsRelChange
     -- Target
      ,IF( DATE_DIFF(AccCl.ClosedDateTimeAEDT , DATE_SUB(d.as_of_date, INTERVAL 1 DAY ) , DAY) BETWEEN 0 AND 90, 1, 0 ) AS ChurnedIn90D
    FROM base_loan_accounts la
    LEFT JOIN client_details car
      ON la.AccountNumber = car.AccountNumber
    LEFT JOIN securities scs
      ON la.AccountNumber = scs.AccountNumber
    LEFT JOIN src_dwh_source.CoreLogic_MarketTrends_Standard_Postcode mtsp
      ON scs.PostCode = CAST(mtsp.Postcode AS STRING)
      AND LEAST(DATE_SUB(d.as_of_date, INTERVAL 1 DAY), DATE('2024-11-30')) = DATE(mtsp.Month_end)
      AND mtsp.Property_Type = 'H'
    LEFT JOIN account_adress_changes adc
      ON la.AccountNumber = adc.AccountNumber
    LEFT JOIN other_account_details oad
      ON la.AccountNumber = oad.AccountNumber
    LEFT JOIN AnalyticsSandbox.RBALendingStats2025 rba
      ON  LEAST(DATE_SUB(d.as_of_date, INTERVAL 1 DAY), DATE('2024-11-30')) = rba.Month
    LEFT JOIN (SELECT AccountNumber, ClosedDateTimeAEDT 
              FROM Integration.Account 
              WHERE EndOfDayDate = DATE(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
                  AND Brand = 'NP'
                  AND Source = 'HOST_CBS'
                  AND GeneralAccountTypeDescription = 'Home Loan'
              ) AS AccCl
      ON la.AccountNumber = AccCl.AccountNumber
    LEFT JOIN logins_rel_change lrc
      ON la.AccountNumber = lrc.AccountNumber
    LEFT JOIN income_data_imputation id
      ON la.AccountNumber = id.AccountNumber
    LEFT JOIN account_repayments ar
      ON la.AccountNumber = ar.AccountNumber
    LEFT JOIN client_repayments_after_current_loan cracl
      ON la.AccountNumber = cracl.AccountNumber
    LEFT JOIN AnalyticsSandbox.NP_VRMC_Transaction_Features nvtf
      ON la.AccountNumber = nvtf.HomeLoanAccountNumber
        AND DATE_SUB(d.as_of_date, INTERVAL 1 DAY) = nvtf.TransactionMonth
    LEFT JOIN AnalyticsSandbox.NP_VRMC_Account_Features nvaf
      ON la.AccountNumber = nvaf.AccountNumber
        AND DATE_SUB(d.as_of_date, INTERVAL 1 DAY) = nvaf.EndOfDayDate
    LEFT JOIN Reporting.HomeLoanRepayment hlr
      ON la.AccountNumber = hlr.Accountnumber
        AND DATE_SUB(d.as_of_date, INTERVAL 1 DAY) = hlr.EndOfMonth
        AND hlr.Brand = 'NP'
        AND hlr.source = 'HOST_CBS'
    WHERE hlr.LoanChannel != 'Broker';