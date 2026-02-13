-- Account Features: Read from BigQuery, Write to Databricks Delta
-- This SQL uses Databricks SQL syntax (not BigQuery)
-- But reads from BigQuery tables via foreign catalog

CREATE OR REPLACE TABLE {output_table}
USING DELTA
AS

WITH required_accounts AS (
  SELECT a.AccountNumber 
  FROM {bq_catalog}.{bq_dataset}.Account a
  WHERE a.EndOfDayDate = '{end_date}'
    AND a.Brand = 'NP'
    AND a.Source = 'HOST_CBS'
    AND a.GeneralAccountTypeDescription = 'Home Loan'
    AND a.InterestRateTypeDescription = 'Variable'
    AND (
      a.AccountStatusType = 'Open' 
      OR a.ClosedDateTimeAEDT BETWEEN '{start_date}' AND DATE_SUB('{end_date}', 1)
    )
),

offset_balances AS (
  SELECT 
    oa.LoanAccountNumber,
    oa.EndOfDayDate,
    SUM(ab.BalanceAmount) AS OffsetBalance
  FROM (
    SELECT 
      AccountNumber AS LoanAccountNumber,
      EndOfDayDate,
      OffsetAccountNumber
    FROM {bq_catalog}.{bq_dataset}.LoanAccountOffsetAccountRelationship 
    WHERE EndOfDayDate BETWEEN '{start_date}' AND '{end_date}'
      AND GeneralAccountTypeDescription = 'Home Loan'
      AND Brand = 'NP'
      AND Source = 'HOST_CBS'
  ) oa
  LEFT JOIN {bq_catalog}.{bq_dataset}.Account ab
    ON oa.OffsetAccountNumber = ab.AccountNumber
    AND oa.EndOfDayDate = ab.EndOfDayDate
    AND ab.Brand = 'NP'
    AND ab.Source = 'HOST_CBS'
  GROUP BY 1, 2
),

monthly_account_info AS (
  SELECT 
    a.EndOfDayDate,
    a.AccountNumber,
    a.BalanceAmount,
    a.InAdvanceAmount,
    a.ArrearsAmount,
    a.InterestRate,
    a.ActualLoanValueRatio,
    a.ClosedDateTimeAEDT,
    COALESCE(ob.OffsetBalance, 0) AS OffsetBalance
  FROM required_accounts ra 
  LEFT JOIN {bq_catalog}.{bq_dataset}.Account a
    ON a.AccountNumber = ra.AccountNumber
  LEFT JOIN offset_balances ob
    ON a.AccountNumber = ob.LoanAccountNumber
    AND a.EndOfDayDate = ob.EndOfDayDate
  WHERE a.EndOfDayDate BETWEEN '{start_date}' AND '{end_date}'
    AND a.Brand = 'NP'
    AND a.Source = 'HOST_CBS'
    AND a.GeneralAccountTypeDescription = 'Home Loan'
    AND a.InterestRateTypeDescription = 'Variable'
)

SELECT 
  AccountNumber,
  EndOfDayDate,
  OffsetBalance,
  
  -- Balance slope and relative change
  (COVAR_SAMP(UNIX_TIMESTAMP(EndOfDayDate), BalanceAmount) OVER w6m) / 
    NULLIF(VAR_SAMP(UNIX_TIMESTAMP(EndOfDayDate)) OVER w6m, 0) AS BalanceSlope,
  
  (AVG(BalanceAmount) OVER w3m_recent - AVG(BalanceAmount) OVER w6m_older) / 
    NULLIF(AVG(BalanceAmount) OVER w6m_older, 0) AS BalanceRelChange,
  
  -- Advance slope and relative change
  (COVAR_SAMP(UNIX_TIMESTAMP(EndOfDayDate), InAdvanceAmount) OVER w6m) / 
    NULLIF(VAR_SAMP(UNIX_TIMESTAMP(EndOfDayDate)) OVER w6m, 0) AS AdvanceSlope,
  
  (AVG(InAdvanceAmount) OVER w3m_recent - AVG(InAdvanceAmount) OVER w6m_older) / 
    NULLIF(AVG(InAdvanceAmount) OVER w6m_older, 0) AS AdvanceRelChange,
  
  -- Arrears slope and relative change
  (COVAR_SAMP(UNIX_TIMESTAMP(EndOfDayDate), ArrearsAmount) OVER w6m) / 
    NULLIF(VAR_SAMP(UNIX_TIMESTAMP(EndOfDayDate)) OVER w6m, 0) AS ArrearsSlope,
  
  (AVG(ArrearsAmount) OVER w3m_recent - AVG(ArrearsAmount) OVER w6m_older) / 
    NULLIF(AVG(ArrearsAmount) OVER w6m_older, 0) AS ArrearsRelChange,
  
  -- Interest rate slope and relative change
  (COVAR_SAMP(UNIX_TIMESTAMP(EndOfDayDate), InterestRate) OVER w6m) / 
    NULLIF(VAR_SAMP(UNIX_TIMESTAMP(EndOfDayDate)) OVER w6m, 0) AS RatesSlope,
  
  (AVG(InterestRate) OVER w3m_recent - AVG(InterestRate) OVER w6m_older) / 
    NULLIF(AVG(InterestRate) OVER w6m_older, 0) AS RateRelChange,
  
  -- LVR slope and relative change
  (COVAR_SAMP(UNIX_TIMESTAMP(EndOfDayDate), ActualLoanValueRatio) OVER w6m) / 
    NULLIF(VAR_SAMP(UNIX_TIMESTAMP(EndOfDayDate)) OVER w6m, 0) AS LVRSlope,
  
  (AVG(ActualLoanValueRatio) OVER w3m_recent - AVG(ActualLoanValueRatio) OVER w6m_older) / 
    NULLIF(AVG(ActualLoanValueRatio) OVER w6m_older, 0) AS LvrRelChange,
  
  -- Offset balance slope and relative change
  (COVAR_SAMP(UNIX_TIMESTAMP(EndOfDayDate), OffsetBalance) OVER w6m) / 
    NULLIF(VAR_SAMP(UNIX_TIMESTAMP(EndOfDayDate)) OVER w6m, 0) AS OffsetBalanceSlope,
  
  (AVG(OffsetBalance) OVER w3m_recent - AVG(OffsetBalance) OVER w6m_older) / 
    NULLIF(AVG(OffsetBalance) OVER w6m_older, 0) AS OffsetBalRelChange

FROM monthly_account_info

WINDOW 
  w6m AS (
    PARTITION BY AccountNumber 
    ORDER BY EndOfDayDate 
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
  ),
  w3m_recent AS (
    PARTITION BY AccountNumber 
    ORDER BY EndOfDayDate 
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ),
  w6m_older AS (
    PARTITION BY AccountNumber 
    ORDER BY EndOfDayDate 
    ROWS BETWEEN 8 PRECEDING AND 3 PRECEDING
  );
