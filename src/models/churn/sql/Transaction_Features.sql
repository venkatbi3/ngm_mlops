-- Transaction Features: Read from BigQuery, Write to Databricks Delta
-- This SQL uses Databricks SQL syntax (not BigQuery)
-- But reads from BigQuery tables via foreign catalog


  WITH clients_and_accounts AS (
    SELECT a.AccountNumber AS HomeLoanAccountNumber
      ,car_cl.ClientNumber AS AssociatedClientNumber
      ,car_acc.AccountNumber AS RelatedTransactionAccount
      ,car_acc.SimpleWeighting
    FROM Integration.Account a
    LEFT JOIN `Integration.ClientAccountRelationship` car_cl -- Join all clients associated with these home loans.
      ON a.AccountNumber = car_cl.AccountNumber
        AND a.Brand = car_cl.Brand
        AND a.Source = car_cl.Source
    LEFT JOIN `Integration.ClientAccountRelationship` car_acc -- Join all other accounts linked to these clients who are associated with the home loans.
      ON car_cl.ClientNumber = car_acc.ClientNumber
        AND car_acc.Brand = car_cl.Brand
        AND car_acc.Source = car_cl.Source
        AND car_acc.GeneralAccountTypeDescription IN ('Transaction', 'Savings', 'Term Share', 'Home Loan', 'Credit Card', 'Personal Loan')
    WHERE a.EndOfDayDate = DATE_SUB(end_date, INTERVAL 1 DAY)
      AND car_cl.EndOfDayDate = DATE_SUB(end_date, INTERVAL 1 DAY)
      AND car_acc.EndOfDayDate = DATE_SUB(end_date, INTERVAL 1 DAY)
      AND a.Brand = 'NP'
      AND a.Source = 'HOST_CBS'
      AND a.GeneralAccountTypeDescription = 'Home Loan'
      AND a.InterestRateTypeDescription = 'Variable'
      AND (a.AccountStatusType = 'Open' OR a.ClosedDateTimeAEDT BETWEEN start_date AND DATE_SUB(end_date, INTERVAL 1 DAY)) 
  ),
  transactions AS (
    SELECT 
      caa.HomeLoanAccountNumber
      ,LAST_DAY(TransactionDate, MONTH) AS TransactionMonth
      ,ats.GeneralAccountTypeDescription AS AccountType
      ,CASE
        WHEN SIGN(SourceTransactionAmount) = 1 AND ats.GeneralAccountTypeDescription IN ('Transaction', 'Savings', 'Term Share') THEN 'Credit'
        WHEN SIGN(SourceTransactionAmount) = -1 AND ats.GeneralAccountTypeDescription IN ('Transaction', 'Savings', 'Term Share') THEN 'Debit'
        WHEN SIGN(SourceTransactionAmount) = 1 AND ats.GeneralAccountTypeDescription IN ('Home Loan', 'Credit Card', 'Personal Loan') THEN 'Debit'
        WHEN SIGN(SourceTransactionAmount) = -1 AND ats.GeneralAccountTypeDescription IN ('Home Loan', 'Credit Card', 'Personal Loan') THEN 'Credit'
      END AS TransactionType
      ,CASE 
        WHEN ats.GeneralAccountTypeDescription = 'Transaction' AND SIGN(SourceTransactionAmount) = -1 AND CounterPartyBSB IS NOT NULL
                AND CounterPartyAccountNumber IS NOT NULL AND TransactionCode != '472' THEN
                  CASE 
                    WHEN LOWER(TransactionDescription) LIKE '%home%' OR LOWER(TransactionDescription) LIKE '%mortgage%' THEN 'home loan payment'
                    WHEN LOWER(TransactionDescription) LIKE '%loan%' THEN 'other loans'
                    WHEN TransactionAmount >= 2500 THEN 'large transaction (2.5K)'
                  END
      END AS ExternalPaymenttype
      ,CASE 
        WHEN ats.GeneralAccountTypeDescription IN ('Home Loan', 'Credit Card', 'Personal Loan') AND SIGN(SourceTransactionAmount) = 1 AND CounterPartyBSB IS NOT NULL
                AND CounterPartyAccountNumber IS NOT NULL AND TransactionCode != '472' THEN 'PaymentFromExternalBank'
      END AS IncomingPaymentType
      ,SUM(ABS(SourceTransactionAmount)*caa.SimpleWeighting) AS Amount -- for joint accounts we are only attributing their share. but we later combine them at account level
    FROM Integration.AccountTransaction ats
    LEFT JOIN clients_and_accounts caa
      ON ats.AccountNumber = caa.RelatedTransactionAccount
    WHERE TransactionDate BETWEEN start_date AND DATE_SUB(end_date, INTERVAL 1 DAY)
      AND ats.Brand = 'NP'
      AND ats.Source = 'HOST_CBS'
      AND SourceTransactionAmount != 0
    GROUP BY 1, 2, 3, 4 ,5, 6
  ),
  dates AS ( -- to make sure we are not missing any months
    SELECT DISTINCT DateValue
    FROM `Integration.Date`
    WHERE DateValue BETWEEN start_date AND DATE_SUB(end_date, INTERVAL 1 DAY)
      AND DateValue = LAST_DAY(DateValue,MONTH)
      AND Brand = 'NP'
      AND SOURCE = 'HOST_CBS'
  )
  ,transaction_aggregations AS (
    SELECT HomeLoanAccountNumber
      ,COALESCE(TransactionMonth, d.DateValue) AS TransactionMonth
      ,SUM(IF(AccountType IN ('Transaction', 'Savings', 'Term Share') AND TransactionType = 'Credit', Amount, NULL)) AS Savings_Credit
      ,SUM(IF(AccountType IN ('Transaction', 'Savings', 'Term Share') AND TransactionType = 'Credit', 1, NULL)) AS NumTrans_Savings_Credit
      ,SUM(IF(AccountType IN ('Transaction', 'Savings', 'Term Share') AND TransactionType = 'Debit', Amount, NULL)) AS Savings_Debit
      ,SUM(IF(AccountType IN ('Transaction', 'Savings', 'Term Share') AND TransactionType = 'Debit', 1, NULL)) AS NumTrans_Savings_Debit
      ,SUM(IF(AccountType IN ('Home Loan', 'Personal Loan', 'Credit Card') AND TransactionType = 'Credit', Amount, NULL)) AS Lending_Credit
      ,SUM(IF(AccountType IN ('Home Loan', 'Personal Loan', 'Credit Card') AND TransactionType = 'Debit', Amount, NULL)) AS Lending_Debit
      ,SUM(IF(AccountType IN ('Home Loan', 'Personal Loan', 'Credit Card') AND TransactionType = 'Debit', 1, NULL)) AS NumTrans_Lending_Debit
      ,SUM(IF(ExternalPaymenttype ='home loan payment', Amount, NULL)) AS ExternalHomeLoanPayments
      ,SUM(IF(ExternalPaymenttype ='other loans', Amount, NULL)) AS ExternalOtherLoanPayments
      ,SUM(IF(ExternalPaymenttype ='large transaction (2.5K)', Amount, NULL)) AS ExternalLargePayments
      ,SUM(IF(IncomingPaymentType ='PaymentFromExternalBank', 1, NULL)) AS NumPaymentsFromExternalBanks
    FROM dates d
    LEFT JOIN transactions t
      ON d.DateValue = t.TransactionMonth
    GROUP BY 1,2 
  )
  SELECT HomeLoanAccountNumber
    ,TransactionMonth
    ,SAFE_DIVIDE(
      COVAR_SAMP(UNIX_DATE(TransactionMonth),Savings_Credit) OVER(w5m)
      ,VARIANCE(UNIX_DATE(TransactionMonth)) OVER(w5m) 
    ) AS Savings_Credit_Slope
    ,SAFE_DIVIDE(
      AVG(Savings_Credit) OVER(w2m) 
      - AVG(Savings_Credit) OVER(w8m) 
      ,AVG(Savings_Credit) OVER(w8m) 
    ) AS SavingsCreditRelChange
    ,SAFE_DIVIDE(
      COVAR_SAMP(UNIX_DATE(TransactionMonth),NumTrans_Savings_Credit) OVER(w5m)
      ,VARIANCE(UNIX_DATE(TransactionMonth)) OVER(w5m) 
    ) AS NumTrans_Savings_Credit_Slope
    ,SAFE_DIVIDE(
      AVG(NumTrans_Savings_Credit) OVER(w2m) 
      - AVG(NumTrans_Savings_Credit) OVER(w8m) 
      ,AVG(NumTrans_Savings_Credit) OVER(w8m) 
    ) AS NumTransSavingsCreditRelChange
    --
    ,SAFE_DIVIDE(
      COVAR_SAMP(UNIX_DATE(TransactionMonth),Savings_Debit) OVER(w5m)
      ,VARIANCE(UNIX_DATE(TransactionMonth)) OVER(w5m) 
    ) AS Savings_Debit_Slope
    ,SAFE_DIVIDE(
      AVG(Savings_Debit) OVER(w2m) 
      - AVG(Savings_Debit) OVER(w8m) 
      ,AVG(Savings_Debit) OVER(w8m) 
    ) AS SavingsDebitRelChange
    ,SAFE_DIVIDE(
      COVAR_SAMP(UNIX_DATE(TransactionMonth),NumTrans_Savings_Debit) OVER(w5m)
      ,VARIANCE(UNIX_DATE(TransactionMonth)) OVER(w5m) 
    ) AS NumTrans_Savings_Debit_Slope
    ,SAFE_DIVIDE(
      AVG(NumTrans_Savings_Debit) OVER(w2m) 
      - AVG(NumTrans_Savings_Debit) OVER(w8m) 
      ,AVG(NumTrans_Savings_Debit) OVER(w8m) 
    ) AS NumTrans_Savings_DebitRelChange
    --
    ,SAFE_DIVIDE(
      COVAR_SAMP(UNIX_DATE(TransactionMonth),Lending_Credit) OVER(w5m)
      ,VARIANCE(UNIX_DATE(TransactionMonth)) OVER(w5m) 
    ) AS Lending_Credit_Slope
    ,SAFE_DIVIDE(
      AVG(Lending_Credit) OVER(w2m) 
      - AVG(Lending_Credit) OVER(w8m) 
      ,AVG(Lending_Credit) OVER(w8m) 
    ) AS LendingCreditRelChange
    --
    ,SAFE_DIVIDE(
      COVAR_SAMP(UNIX_DATE(TransactionMonth),Lending_Debit) OVER(w5m)
      ,VARIANCE(UNIX_DATE(TransactionMonth)) OVER(w5m) 
    ) AS Lending_Debit_Slope
    ,SAFE_DIVIDE(
      AVG(Lending_Debit) OVER(w2m) 
      - AVG(Lending_Debit) OVER(w8m) 
      ,AVG(Lending_Debit) OVER(w8m) 
    ) AS LendingDebitRelChange
    --
    ,SAFE_DIVIDE(
      COVAR_SAMP(UNIX_DATE(TransactionMonth),NumTrans_Lending_Debit) OVER(w5m)
      ,VARIANCE(UNIX_DATE(TransactionMonth)) OVER(w5m) 
    ) AS NumTrans_Lending_Debit_Slope
    ,SAFE_DIVIDE(
      AVG(NumTrans_Lending_Debit) OVER(w2m) 
      - AVG(NumTrans_Lending_Debit) OVER(w8m) 
      ,AVG(NumTrans_Lending_Debit) OVER(w8m) 
    ) AS NumTrans_Lending_DebitRelChange
    ,IF(COUNT(IF(ExternalHomeLoanPayments>0, TransactionMonth, NULL)) OVER(w5m)>=1
      ,true
      ,false
    ) AS HasHomeLoanPayments
    ,IF(COUNT(IF(ExternalOtherLoanPayments>0, TransactionMonth, NULL)) OVER(w5m)>=1
      ,true
      ,false
    ) AS HasOtherLoanPayments
    ,IF(COUNT(IF(ExternalLargePayments>0, TransactionMonth, NULL)) OVER(w5m)>=5
      ,true
      ,false
    ) AS HasLargePayments
    ,IF(SUM(IF(NumPaymentsFromExternalBanks>1, 1, 0)) OVER(w5m)>=5
      ,true
      ,false
    ) AS LoanPaymentsFromExternalBanks
  FROM transaction_aggregations
  ORDER BY TransactionMonth

WINDOW 
  w5m AS (
    PARTITION BY HomeLoanAccountNumber 
    ORDER BY TransactionMonth 
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
  ),
  w2m AS (
    PARTITION BY HomeLoanAccountNumber 
    ORDER BY TransactionMonth 
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ),
  w8m AS (
    PARTITION BY HomeLoanAccountNumber 
    ORDER BY TransactionMonth 
    ROWS BETWEEN 8 PRECEDING AND 3 PRECEDING
  );

