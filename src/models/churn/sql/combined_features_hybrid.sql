-- Combined Features: Join Account and Transaction Features
-- Both source tables are now in Databricks Delta (fast local joins)

CREATE OR REPLACE TABLE {output_table}
USING DELTA
AS

SELECT 
  a.*,
  -- Add all transaction features except the join keys
  t.Savings_Credit_Slope,
  t.SavingsCreditRelChange,
  t.NumTrans_Savings_Credit_Slope,
  t.NumTransSavingsCreditRelChange,
  t.Savings_Debit_Slope,
  t.SavingsDebitRelChange,
  t.NumTrans_Savings_Debit_Slope,
  t.NumTrans_Savings_DebitRelChange,
  t.Lending_Credit_Slope,
  t.LendingCreditRelChange,
  t.Lending_Debit_Slope,
  t.LendingDebitRelChange,
  t.NumTrans_Lending_Debit_Slope,
  t.NumTrans_Lending_DebitRelChange,
  t.HasHomeLoanPayments,
  t.HasOtherLoanPayments,
  t.HasLargePayments,
  t.LoanPaymentsFromExternalBanks
  
FROM {db_catalog}.{db_schema}.NP_VRMC_Account_Features a

LEFT JOIN {db_catalog}.{db_schema}.NP_VRMC_Transaction_Features t
  ON a.AccountNumber = t.HomeLoanAccountNumber
  AND LAST_DAY(a.EndOfDayDate) = t.TransactionMonth;
