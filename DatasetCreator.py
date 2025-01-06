import pandas as pd
import random
from datetime import datetime, timedelta

# Define parameters for customer-centric data
customer_names = [
    "Aarav", "Vihaan", "Aditya", "Arjun", "Rohan", "Siddharth",
    "Karan", "Rahul", "Aryan", "Sameer", "Dev", "Ishaan", "Anirudh",
    "Abhinav", "Kartik", "Nikhil", "Yash", "Manish", "Vivek", "Pranav",
    "Akash", "Tanmay", "Harsh", "Shivansh", "Parth", "Aanya", "Priya",
    "Ananya", "Riya", "Simran", "Sneha", "Pooja",
    "Shruti", "Shreya", "Isha", "Neha", "Aishwarya", "Kritika", "Meera",
    "Divya", "Sakshi", "Vidya", "Swati", "Khushboo", "Charulata", "Nidhi",
    "Manisha", "Tanvi", "Sonal", "Sanya"
]

additional_branches = [
    "Mumbai", "Delhi", "Bengaluru", "Chennai", "Hyderabad", "Kolkata", "Ahmedabad", "Pune", "Jaipur",
    "Lucknow", "Coimbatore", "Nagpur", "Kanpur", "Madurai", "Patna"
]
regions = ["01", "02", "Main", "West", "North", "South", "East"]
loan_types = ["Housing Loan", "Vehicle Loan", "Education Loan", "Personal Loan", "Gold Loan", "Loan Against Property", "Business Loan"]
asset_types = {
    "Housing Loan": "Home Documents",
    "Vehicle Loan": "Car/Bike Documents",
    "Education Loan": "Certificate",
    "Personal Loan": "No Asset",
    "Gold Loan": "Gold",
    "Loan Against Property": "Property Documents",
    "Business Loan": "Business Proof"
}
kyc_documents = ["Aadhar card", "PAN card"]
proof_of_identity = ["Passport", "Voter ID", "Driver's License", "Aadhar card"]
income_proofs = ["Salary slips", "Income tax returns", "Bank statements", "Form 16", "Profit/Loss statement for business loans"]

# Define status distribution
status_distribution = {
    "Work in Progress": 0.65,
    "Sanctioned": 0.20,
    "Disbursed": 0.15
}
manager_names = [
    "Aarohi", "Nandini", "Darsh", "Veda", "Kavya", "Lakshay", "Rishabh",
    "Madhuri", "Tanushree", "Veer", "Rupal", "Esha", "Raghav", "Ira",
    "Yogesh", "Jhanvi", "Bhavya", "Devanshi", "Sahil", "Mehul"
]

# Create a mapping of branches to specific branch managers
branch_manager_mapping = {
    "Karur": "Aarohi", "Mumbai": "Nandini", "Delhi": "Darsh", "Bengaluru": "Veda",
    "Chennai": "Kavya", "Hyderabad": "Lakshay", "Kolkata": "Rishabh", "Ahmedabad": "Madhuri",
    "Pune": "Tanushree", "Jaipur": "Veer", "Lucknow": "Rupal", "Indore": "Esha",
    "Coimbatore": "Raghav", "Nagpur": "Ira", "Kanpur": "Yogesh", "Vadodara": "Jhanvi",
    "Ludhiana": "Bhavya", "Agra": "Devanshi", "Surat": "Sahil", "Mysore": "Mehul",
    "Madurai": "Aarohi", "Patna": "Nandini"
}

# Function to generate credit scores with specified distribution
def generate_credit_score():
    low_score = random.choices(range(300, 650), k=int(0.05 * 3000))
    high_score = random.choices(range(651, 850), k=int(0.95 * 3000))
    return random.choice(low_score + high_score)

# Generate requested and approval dates within 2022 to 2024
def generate_requested_and_approval_dates():
    requested_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 1094))  # 3 years
    approval_date = requested_date + timedelta(days=random.randint(1, 30))
    return requested_date, approval_date

# Initialize synthetic data
synthetic_data = []

for _ in range(3000):  # Generate 3000 customer records
    customer_id = f"CUST-{random.randint(1000, 9999)}"
    customer_name = random.choice(customer_names)
    credit_score = generate_credit_score()
    employment_history = f"{random.randint(1, 20)} years"
    branch = random.choice(additional_branches)
    region = random.choice(regions)
    manager_name = branch_manager_mapping[branch]  # Get the manager for the branch
    loan_type = random.choice(loan_types)
    asset_type = asset_types[loan_type]
    asset_value = random.randint(500_000, 5_000_000) if asset_type != "No Asset" else "N/A"
    loan_requested = (
        int(asset_value * 0.5) if asset_type != "No Asset" else random.randint(50_000, 500_000)
    )
    loan_sanctioned = int(loan_requested * random.uniform(0.8, 0.95))
    loan_disbursed = int(loan_sanctioned * random.uniform(0.9, 0.98))
    actual_tenure = random.randint(10, 20)
    rate_of_interest = round(random.uniform(7, 14), 2)
    late_repayments = random.randint(0, 20)
    loan_insurance = random.choice(["Yes", "No"])
    kyc_doc = random.choice(kyc_documents)
    proof_id = ", ".join(random.sample(proof_of_identity, 2))
    income_proof = ", ".join(random.sample(income_proofs, 3))

    # Determine process status
    process_status = random.choices(list(status_distribution.keys()), weights=list(status_distribution.values()), k=1)[0]
    requested_date, approval_date = generate_requested_and_approval_dates()
    paid_tenure = None

    if process_status == "Disbursed":
        time_diff = datetime(2023, 12, 20) - approval_date
        paid_tenure = round(time_diff.days / 365, 1)
    elif process_status == "Work in Progress":
        paid_tenure = "N/A"

    # Calculate remaining tenure
    if paid_tenure is not None and paid_tenure != "N/A":
        remaining_tenure = actual_tenure - paid_tenure
    else:
        remaining_tenure = "N/A"

    # Adjust values for "Work in Progress"
    if process_status == "Work in Progress":
        loan_sanctioned = loan_requested
        loan_disbursed = "N/A"
    
    # Generate query type for "Work in Progress"
    query_type = None
    if process_status == "Work in Progress":
        query_type = random.choice(["Operational Cases", "Credit Cases", "Sales Queries"])

    synthetic_data.append({
        "Customer ID": customer_id,
        "Customer Name": customer_name,
        "Credit Score": credit_score,
        "Employment History": employment_history,
        "Branch": branch + "-" + region,  # Add the region to form the full branch name
        "Manager Name": manager_name,  # Use the consistent branch manager
        "Loan Type": loan_type,
        "Submitted Asset Type": asset_type,
        "Asset Value": asset_value,
        "Loan Amount Requested": loan_requested,
        "Loan Amount Sanctioned": loan_sanctioned,
        "Disbursed Amount": loan_disbursed if process_status == "Disbursed" else "N/A",
        "Rate of Interest (%)": rate_of_interest,
        "Loan Insurance Taken": loan_insurance,
        "Process Status": process_status,
        "Query Type (Only WIP)": query_type,
        "Requested Date": requested_date.strftime('%Y-%m-%d'),
        "Approval Date": approval_date.strftime('%Y-%m-%d'),
        "Actual Loan Tenure (Years)": actual_tenure,
        "Paid Tenure (Years)": paid_tenure,
        "Remaining Tenure (Years)": remaining_tenure,
        "Late Repayments": late_repayments,
        "KYC Document": kyc_doc,
        "Proof of Identity": proof_id,
        "Income Proof": income_proof
    })

# Create a DataFrame
df_synthetic = pd.DataFrame(synthetic_data)

# Rearrange columns for better readability
column_order = [
    "Customer ID", "Customer Name", "Credit Score", "Employment History",
    "Branch", "Manager Name", "Loan Type", "Submitted Asset Type", "Asset Value", "Loan Amount Requested",
    "Loan Amount Sanctioned", "Disbursed Amount", "Rate of Interest (%)", "Loan Insurance Taken",
    "Process Status", "Query Type (Only WIP)", "Requested Date", "Approval Date",
    "Actual Loan Tenure (Years)", "Paid Tenure (Years)", "Remaining Tenure (Years)", "Late Repayments",
    "KYC Document", "Proof of Identity", "Income Proof"
]
df_synthetic = df_synthetic[column_order]

# Save to CSV
file_path_adjusted = "Synthetic_Banking_Customer_Dataset.csv"
df_synthetic.to_csv(file_path_adjusted, index=False)

print(f"Dataset saved to {file_path_adjusted}")











# import pandas as pd
# import random
# from datetime import datetime, timedelta

# # Define parameters for customer-centric data
# customer_names = [
#     "Aarav", "Vihaan", "Aditya", "Arjun", "Rohan", "Siddharth",
#     "Karan", "Rahul", "Aryan", "Sameer", "Dev", "Ishaan", "Anirudh",
#     "Abhinav", "Kartik", "Nikhil", "Yash", "Manish", "Vivek", "Pranav",
#     "Akash", "Tanmay", "Harsh", "Shivansh", "Parth", "Aanya", "Priya",
#     "Ananya", "Riya", "Simran", "Sneha", "Pooja",
#     "Shruti", "Shreya", "Isha", "Neha", "Aishwarya", "Kritika", "Meera",
#     "Divya", "Sakshi", "Vidya", "Swati", "Khushboo", "Charulata", "Nidhi",
#     "Manisha", "Tanvi", "Sonal", "Sanya"
# ]

# additional_branches = [
#     "Mumbai", "Delhi", "Bengaluru", "Chennai", "Hyderabad", "Kolkata", "Ahmedabad", "Pune", "Jaipur",
#     "Lucknow", "Coimbatore", "Nagpur", "Kanpur", "Madurai", "Patna"
# ]
# regions = ["01", "02", "Main", "West", "North", "South", "East"]
# loan_types = ["Housing Loan", "Vehicle Loan", "Education Loan", "Personal Loan", "Gold Loan", "Loan Against Property", "Business Loan"]
# asset_types = {
#     "Housing Loan": "Home Documents",
#     "Vehicle Loan": "Car/Bike Documents",
#     "Education Loan": "Certificate",
#     "Personal Loan": "No Asset",
#     "Gold Loan": "Gold",
#     "Loan Against Property": "Property Documents",
#     "Business Loan": "Business Proof"
# }
# kyc_documents = ["Aadhar card", "PAN card"]
# proof_of_identity = ["Passport", "Voter ID", "Driver's License", "Aadhar card"]
# income_proofs = ["Salary slips", "Income tax returns", "Bank statements", "Form 16", "Profit/Loss statement for business loans"]

# # Define status distribution
# status_distribution = {
#     "Work in Progress": 0.65,
#     "Sanctioned": 0.20,
#     "Disbursed": 0.15
# }
# manager_names = [
#     "Aarohi", "Nandini", "Darsh", "Veda", "Kavya", "Lakshay", "Rishabh",
#     "Madhuri", "Tanushree", "Veer", "Rupal", "Esha", "Raghav", "Ira",
#     "Yogesh", "Jhanvi", "Bhavya", "Devanshi", "Sahil", "Mehul"
# ]

# # Create a mapping of branches to specific branch managers
# branch_manager_mapping = {
#     "Karur": "Aarohi", "Mumbai": "Nandini", "Delhi": "Darsh", "Bengaluru": "Veda",
#     "Chennai": "Kavya", "Hyderabad": "Lakshay", "Kolkata": "Rishabh", "Ahmedabad": "Madhuri",
#     "Pune": "Tanushree", "Jaipur": "Veer", "Lucknow": "Rupal", "Indore": "Esha",
#     "Coimbatore": "Raghav", "Nagpur": "Ira", "Kanpur": "Yogesh", "Vadodara": "Jhanvi",
#     "Ludhiana": "Bhavya", "Agra": "Devanshi", "Surat": "Sahil", "Mysore": "Mehul",
#     "Madurai": "Aarohi", "Patna": "Nandini"
# }

# # Generate requested and approval dates within 2022 to 2024
# def generate_requested_and_approval_dates():
#     requested_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 1094))  # 3 years
#     approval_date = requested_date + timedelta(days=random.randint(1, 30))
#     return requested_date, approval_date

# # Initialize synthetic data
# synthetic_data = []

# for _ in range(3000):  # Generate 2000 customer records
#     customer_id = f"CUST-{random.randint(1000, 9999)}"
#     customer_name = random.choice(customer_names)
#     credit_score = random.randint(300, 850)
#     employment_history = f"{random.randint(1, 20)} years"
#     branch = random.choice(additional_branches)
#     region = random.choice(regions)
#     manager_name = branch_manager_mapping[branch]  # Get the manager for the branch
#     loan_type = random.choice(loan_types)
#     asset_type = asset_types[loan_type]
#     asset_value = random.randint(500_000, 5_000_000) if asset_type != "No Asset" else "N/A"
#     loan_requested = (
#         int(asset_value * 0.5) if asset_type != "No Asset" else random.randint(50_000, 500_000)
#     )
#     loan_sanctioned = int(loan_requested * random.uniform(0.8, 0.95))
#     loan_disbursed = int(loan_sanctioned * random.uniform(0.9, 0.98))
#     actual_tenure = random.randint(10, 20)
#     rate_of_interest = round(random.uniform(7, 14), 2)
#     late_repayments = random.randint(0, 20)
#     loan_insurance = random.choice(["Yes", "No"])
#     kyc_doc = random.choice(kyc_documents)
#     proof_id = ", ".join(random.sample(proof_of_identity, 2))
#     income_proof = ", ".join(random.sample(income_proofs, 3))

#     # Determine process status
#     process_status = random.choices(list(status_distribution.keys()), weights=list(status_distribution.values()), k=1)[0]
#     requested_date, approval_date = generate_requested_and_approval_dates()
#     paid_tenure = None

#     if process_status == "Disbursed":
#         time_diff = datetime(2023, 12, 20) - approval_date
#         paid_tenure = round(time_diff.days / 365, 1)
#     elif process_status == "Work in Progress":
#         paid_tenure = "N/A"

#     # Calculate remaining tenure
#     if paid_tenure is not None and paid_tenure != "N/A":
#         remaining_tenure = actual_tenure - paid_tenure
#     else:
#         remaining_tenure = "N/A"

#     # Adjust values for "Work in Progress"
#     if process_status == "Work in Progress":
#         loan_sanctioned = loan_requested
#         loan_disbursed = "N/A"
    
#     # Generate query type for "Work in Progress"
#     query_type = None
#     if process_status == "Work in Progress":
#         query_type = random.choice(["Operational Cases", "Credit Cases", "Sales Queries"])

#     synthetic_data.append({
#         "Customer ID": customer_id,
#         "Customer Name": customer_name,
#         "Credit Score": credit_score,
#         "Employment History": employment_history,
#         "Branch": branch + "-" + region,  # Add the region to form the full branch name
#         "Manager Name": manager_name,  # Use the consistent branch manager
#         "Loan Type": loan_type,
#         "Submitted Asset Type": asset_type,
#         "Asset Value": asset_value,
#         "Loan Amount Requested": loan_requested,
#         "Loan Amount Sanctioned": loan_sanctioned,
#         "Disbursed Amount": loan_disbursed if process_status == "Disbursed" else "N/A",
#         "Rate of Interest (%)": rate_of_interest,
#         "Loan Insurance Taken": loan_insurance,
#         "Process Status": process_status,
#         "Query Type (Only WIP)": query_type,
#         "Requested Date": requested_date.strftime('%Y-%m-%d'),
#         "Approval Date": approval_date.strftime('%Y-%m-%d'),
#         "Actual Loan Tenure (Years)": actual_tenure,
#         "Paid Tenure (Years)": paid_tenure,
#         "Remaining Tenure (Years)": remaining_tenure,
#         "Late Repayments": late_repayments,
#         "KYC Document": kyc_doc,
#         "Proof of Identity": proof_id,
#         "Income Proof": income_proof
#     })

# # Create a DataFrame
# df_synthetic = pd.DataFrame(synthetic_data)

# # Rearrange columns for better readability
# column_order = [
#     "Customer ID", "Customer Name", "Credit Score", "Employment History",
#     "Branch", "Manager Name", "Loan Type", "Submitted Asset Type", "Asset Value", "Loan Amount Requested",
#     "Loan Amount Sanctioned", "Disbursed Amount", "Rate of Interest (%)", "Loan Insurance Taken",
#     "Process Status", "Query Type (Only WIP)", "Requested Date", "Approval Date",
#     "Actual Loan Tenure (Years)", "Paid Tenure (Years)", "Remaining Tenure (Years)", "Late Repayments",
#     "KYC Document", "Proof of Identity", "Income Proof"
# ]
# df_synthetic = df_synthetic[column_order]

# # Save to CSV
# file_path_adjusted = "Synthetic_Banking_Customer_Dataset.csv"
# df_synthetic.to_csv(file_path_adjusted, index=False)

# print(f"Dataset saved to {file_path_adjusted}")
