import pandas as pd
import random
import numpy as np

data_path = r'C:\week3_assignment\Synthetic_Banking_Customer_Dataset.csv'
df = pd.read_csv(data_path)


df['LoanStatus'] = ''
df['Reason'] = ''
df['empSal'] = df['Asset Value'] * 0.024  # 2.4% of Asset Value
df['EMI Amount'] = (((df['Loan Amount Requested'] * ((df['Rate of Interest (%)'] / 100)*df['Actual Loan Tenure (Years)']))+df['Loan Amount Requested'])/(df['Actual Loan Tenure (Years)']*12))
# df['EMI Amount'] = ((df['Loan Amount Requested'] * df['Rate of Interest (%)'] / 100))/df["Actual Loan Tenure (Years)"] * 12


df['Employed Company'] = [random.randint(1, 4) for _ in range(len(df))]



dti_condition = (
    (df['Loan Amount Requested'] < df['Asset Value'] * 0.5) &
    (df['EMI Amount'] < (df['empSal'])* 0.25)
)
df.loc[dti_condition, 'LoanStatus'] = 'Denied'
df.loc[dti_condition, 'Reason'] = [random.choice([
    'Insufficient Income', 'High Debt-to-Income Ratio (DTI)']) for _ in range(dti_condition.sum())]


cancel_indices = random.sample(range(len(df)), int(len(df) * 0.1)) 
df.loc[cancel_indices, 'LoanStatus'] = 'Cancelled'
df.loc[cancel_indices, [
    'Loan Amount Sanctioned', 'Disbursed Amount', 'Rate of Interest (%)',
    'Loan Insurance Taken', 'Approval Date', 'Actual Loan Tenure (Years)',
    'Paid Tenure (Years)', 'Remaining Tenure (Years)', 'Late Repayments'
]] = np.nan
df.loc[cancel_indices, 'Reason'] = [random.choice([
    'Customer not continuing', 'Cancellation against Bank policy', 'Technical Issue cancellation'])
    for _ in range(len(cancel_indices))]


missing_doc_indices = random.sample(range(len(df)), int(len(df) * 0.1)) 
df.loc[missing_doc_indices, ['Proof of Identity']] = np.nan
df.loc[missing_doc_indices, [
    'Loan Amount Sanctioned', 'Disbursed Amount', 'Rate of Interest (%)',
    'Loan Insurance Taken', 'Approval Date', 'KYC Document', 'Income Proof'
]] = np.nan
df.loc[missing_doc_indices, 'LoanStatus'] = 'Denied'
df.loc[missing_doc_indices, 'Reason'] = 'Documents Missing'


poor_credit_condition = (df['Credit Score'] >= 300) & (df['Credit Score'] <= 650)
df.loc[poor_credit_condition, 'LoanStatus'] = 'Denied'
df.loc[poor_credit_condition, 'Reason'] = 'Poor credit score'


employment_instability_condition = (
    df['Employment History'].str.extract(r'(\d+)').astype(float)[0] / df['Employed Company'] < 2
)
df.loc[employment_instability_condition, 'LoanStatus'] = 'Denied'
df.loc[employment_instability_condition, 'Reason'] = 'Employment instability'


remaining_condition = df['LoanStatus'] == ''
df.loc[remaining_condition, 'LoanStatus'] = 'Approved'
df.loc[remaining_condition, 'Reason'] = np.nan


updated_file_path = r'C:\week3_assignment\Synthetic_Banking_Customer_Dataset_1.csv'
df.to_csv(updated_file_path, index=False)

print(f"Updated dataset saved to {updated_file_path}")
