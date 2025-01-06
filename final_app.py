import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from openai import AzureOpenAI
import json
import warnings
from datetime import datetime
from calendar import monthrange
warnings.filterwarnings("ignore")

app = Flask(__name__)
CORS(app)


llm = AzureOpenAI(
    api_key='32726a649d7b4d6a8e0b2d770c72155c',
    api_version='2024-08-01-preview',
    azure_endpoint='https://aina-coc.openai.azure.com/',
    azure_deployment='gpt-4o-2'
)

def preprocess_data(dataset, group_by_columns, aggregation_rules, column_renames=None, conversion_columns=None, conversion_rate=1):

    grouped_data = dataset.groupby(group_by_columns).agg(aggregation_rules).reset_index()

    if column_renames:
        grouped_data.rename(columns=column_renames, inplace=True)

    if conversion_columns:
        for col in conversion_columns:
            if col in grouped_data.columns:
                grouped_data[f"{col}_usd"] = grouped_data[col] * conversion_rate

    return grouped_data



def prompt(safe_data_string, question, formatted_response):
    return f"""
    You are given the following processed loan data and a question. Use the data to answer the question in the exact JSON format provided below.

    Processed Loan Data:
    {safe_data_string}

    Question:
    {question}

    Format the response strictly in this JSON format:
    {formatted_response}

    Answer:
    """

def subtract_months(date, months):
    
    month = date.month - months
    year = date.year + month // 12
    month = month % 12
    if month <= 0:
        month += 12
        year -= 1
    return datetime(year, month, 1)

def ask_question(processed_data, question, formatt):
    data_string = processed_data.to_json(orient="records")
    safe_data_string = json.dumps(json.loads(data_string))
    try:
        completion = llm.chat.completions.create(             
            model='gpt-4o-2',            
            messages=[                 
                {"role": "system", 
                 "content": "You are an assistant that analyzes loan data."
                 },                 
                {
                    "role": "user",
                    "content": prompt(safe_data_string, question, formatt)
                }            
            ],             
            temperature=0.5,             
            max_tokens=4000         
        )
        response = completion.choices[0].message.content
        if response.startswith("```json"):
            response = response[7:]
        if response.endswith("```"):
            response = response[:-3]

        response = response.strip()

        try:
            response_json = json.loads(response)
            return response_json
        except json.JSONDecodeError as e:
            return {"error": "Invalid JSON format in response", "details": str(e)}

    except Exception as e:
        return {"error": str(e)}
    

def format_date_range(start_date_str, end_date_str):

    try:

        formatted_start_date = start_date_str.replace(day=1).strftime("%d-%m-%Y")
                    
        last_day = monthrange(end_date_str.year, end_date_str.month)[1]
        formatted_end_date = end_date_str.replace(day=last_day).strftime("%d-%m-%Y")
                    
        return (formatted_start_date, formatted_end_date)
                
    except ValueError as e:
        return {"error": str(e)}

    
##############################################################################################

def case_status(dataset):
    group_by = ['Process Status']
    aggregations = {
        'Customer ID': 'count',
        'Loan Amount Sanctioned': 'sum',
        'Disbursed Amount': 'sum'
    }
    rename_map = {
        'Customer ID': 'cases',
        'Loan Amount Sanctioned': 'sanctioned_amount',
        'Disbursed Amount': 'disbursed_amount'
    }
    conversion_cols = ['sanctioned_amount', 'disbursed_amount']
    conversion_rate = 0.012

    processed_data = preprocess_data(
        dataset=dataset, 
        group_by_columns=group_by, 
        aggregation_rules=aggregations, 
        column_renames=rename_map, 
        conversion_columns=conversion_cols, 
        conversion_rate=conversion_rate
    )
    try:
        question = """
            Summarize the loan cases by process status, including the count of cases and the total amounts Work in Progress, sanctioned and disbursed. 
            Provide the result in the specified JSON format.
        """
        formatt = """
        {
        "casesStatus": [
            {
            "status": "Work_in_Progress",
            "cases": 763,
            "amount": 5000
            },
            {
            "status": "Sanctioned",
            "cases": 10000,
            "amount": 20000
            },
            {
            "status": Disbursed,
            "cases": 2000,
            "amount": 7000
            }
        ]
        }
    """
        
        analysis_result = ask_question(processed_data,question, formatt)

        if isinstance(analysis_result, dict) and 'error' in analysis_result:
            return jsonify(analysis_result)
        
        return jsonify(analysis_result)

    except Exception as e:
        return jsonify({"error": str(e)})
    

##############################################################################################


def progress_status(dataset):
    try:
        data = dataset[dataset['Process Status'] == 'Work in Progress']
        data['Query Type (Only WIP)'] = data['Query Type (Only WIP)'].str.lower()

         # Calculate the total branches per city
        data['City'] = data['Branch'].str.split('-').str[0].str.strip()
        branch_counts = data['City'].value_counts().to_dict()

        grouped_summary = data.groupby('Branch').apply(lambda x: pd.Series({
            'branchManager': x['Manager Name'].iloc[0],
            'credit': (x['Query Type (Only WIP)'] == 'credit cases').sum(),
            'operational': (x['Query Type (Only WIP)'] == 'operational cases').sum(),
            'salesQueries': (x['Query Type (Only WIP)'] == 'sales queries').sum(),
            'total': x['Query Type (Only WIP)'].count()
        })).reset_index()

        # Update branch names to include total branch count
        grouped_summary['Branch'] = grouped_summary['Branch'].apply(
            lambda x: f"{x.split('-')[0].strip()} - {branch_counts.get(x.split('-')[0].strip(), 1)}"
        )

        grouped_summary = grouped_summary.sort_values(by='total', ascending=False).reset_index(drop=True)

        chunk_size = 65
        chunks = [grouped_summary[i:i + chunk_size] for i in range(0, len(grouped_summary), chunk_size)]

        all_results = []

        question = """
        Summarize the loan data by branch name and branch manager. The response must be in strict JSON format without any errors. Include the total operational cases, credit cases, sales queries, and the total for each branch.
        The JSON should include an array of objects, each representing a branch with the following properties:

        1. "branch" (string): The city name and number of branches in that city.
        2. "branchManager" (string): The name of the branch manager.
        3. "operational" (integer): The total number of operational cases.
        4. "credit" (integer): The total number of credit cases.
        5. "salesQueries" (integer): The total number of sales queries.
        6. "total" (integer): The sum of operational, credit, and sales queries for the branch.

        Example format:
            {
                "progressStatus": [
                    {
                        "branch": "CityName - Count",
                        "branchManager": "ManagerName",
                        "operational": <integer>,
                        "credit": <integer>,
                        "salesQueries": <integer>,
                        "total": <integer>
                    }
                ]
            }

        Ensure that the response is always valid JSON and strictly adheres to the above format.
        
        """
        formatt = {
            "progressStatus": [
                {
                    "branch": "Mumbai - 10",
                    "branchManager": "Jane Doe",
                    "operational": 56000,
                    "credit": 31000,
                    "salesQueries": 100,
                    "total": 87100
                },
                {
                    "branch": "Chennai - 01",
                    "branchManager": "Jane Doe",
                    "operational": 0,
                    "credit": 60000,
                    "salesQueries": 98,
                    "total": 60098
                },
                {
                    "branch": "Coimbatore - 02",
                    "branchManager": "Jane Doe",
                    "operational": 80000,
                    "credit": 25000,
                    "salesQueries": 32,
                    "total": 105032
                }
            ]
        }

        for chunk in chunks:
            analysis_result = ask_question(chunk, question, formatt)
            all_results.append(analysis_result)

        combined_result = json.dumps(all_results)
       
        try:
            final_result = json.loads(combined_result)
            return jsonify(final_result[0])
        except json.JSONDecodeError as e:
            return jsonify({"error": f"JSON Decode Error: {str(e)} - Combined result: {combined_result}"})

    except Exception as e:
        return jsonify({"error": str(e)})
    
##############################################################################################

def loan_processing(dataset_1, dataset_2):
    # print(dataset_1['LoanStatus'].unique())

    group_by = ['LoanStatus']
    aggregations = {
        'Customer ID': 'count',
        'Loan Amount Requested': 'sum'
    }
    rename_map = {
        'Customer ID': 'Total_cases',
        'Loan Amount Requested': 'Total_amount'
    }

    # Preprocess the datasets
    processed_data_1 = preprocess_data(
        dataset=dataset_1, 
        group_by_columns=group_by, 
        aggregation_rules=aggregations, 
        column_renames=rename_map
    )
    processed_data_2 = preprocess_data(
        dataset=dataset_2, 
        group_by_columns=group_by, 
        aggregation_rules=aggregations, 
        column_renames=rename_map
    )

    try:
        question = """
            Analyze the two datasets provided (dataset_1 and dataset_2) and calculate the metrics "Approval Rate," "Denial Rate," and "Submission Canceled". Return only the values. No need of returning calculations.

            For each metric:
            1. Calculate the percentage for both datasets.
            2. Determine the percentage change between the two datasets. Use the formula: 
            Percentage Change = ((New - Old) / Old) * 100.
            3. For "Approval Rate," if the percentage increases, mark the status as "Positive"; if it decreases, mark it as "Negative."
            4. For "Denial Rate" and "Submission Canceled," if the percentage decreases, mark the status as "Positive"; if it increases, mark it as "Negative."
        """
        formatt = """
        {
            "loanProcessingMetrics": [
                {
                    "name": "approval_rate",
                    "totalPercentage": 85.46,
                    "status": "Positive", 
                    "statusRate": 1.5
                },
                {
                    "name": "denial_rate",
                    "totalPercentage": 9.73,
                    "status": "Positive",
                    "statusRate": 1.5
                },
                {
                    "name": "submission_cancelled",
                    "totalPercentage": 20,
                    "status": "Negative",
                    "statusRate": 1.5
                }
            ]
        }
        """
        prompt = f"""
            You are given the following processed loan datasets and a question. Use the datasets to answer the question in the exact JSON format provided below.

            Processed Loan Data:
            {processed_data_1, processed_data_2}

            Question:
            {question}

            Format the response strictly in this JSON format:
            {formatt}

            Answer:
        """
        
        completion = llm.chat.completions.create(             
            model='gpt-4o-2',            
            messages=[                 
                {"role": "system", 
                 "content": "You are an assistant that analyzes loan data."
                 },                 
                {
                    "role": "user",
                    "content": prompt
                }            
            ],             
            temperature=0.5,             
            max_tokens=4000         
        )
        response = completion.choices[0].message.content
        
        # Clean up the response
        if response.startswith("```json"):
            response = response[7:]
        if response.endswith("```"):
            response = response[:-3]

        response = response.strip()
        # print(response)
        try:
            response_json = json.loads(response)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format in LLM response: {e}")

        # print(response_json)
        return jsonify(response_json)
        
    except Exception as e:
        return jsonify({"error": str(e)})





##############################################################################################



def categories(dataset,time_period,loan_type):
    try:
        
        group_by = ['Loan Type']
        aggregations = {
            'Customer ID': 'count'
        }
        rename_map = {
            'Customer ID': 'Total_cases'
        }

        grouped_data = preprocess_data(
            dataset=dataset, 
            group_by_columns=group_by, 
            aggregation_rules=aggregations, 
            column_renames=rename_map
        )
        
        total_count = grouped_data['Total_cases'].sum()
        grouped_data['percentage'] = (grouped_data['Total_cases'] / total_count) * 100
        grouped_data['percentage'] = grouped_data['percentage'].round(2)

        subCategory = ""
        percentages = {
            "allCategories": 0,
            "housingLoan": 0,
            "vehicleLoan": 0,
            "educationalLoan": 0,
            "personalLoan": 0,
            "goldLoan": 0,
            "loanAgainstProperty": 0
        }

        if loan_type.lower() == "retail loan":
            subCategory = "allCategories"
            percentages = grouped_data.set_index('Loan Type')['percentage'].to_dict()
        else:
            subCategory = loan_type.lower()
            for loan in percentages.keys():
                percentages[loan] = grouped_data.loc[grouped_data['Loan Type'].str.lower() == subCategory, 'percentage'].sum() if loan == subCategory else 0

        question = f"""
        Format the loan data into a JSON structure where each loan type's name is a key, where the values is calculated by finding the percentage to the total count,
        and the value is its corresponding percentage. Use camelCase formatting for keys.
        Add \"timePeriod\": \"{time_period}\" and \"subCategories\": \"{subCategory}\" at the top level.
        """

        formatt = f"""
                "categories": {{
                "timePeriod": "{time_period}",
                "subCategories": "{subCategory}",
                "allCategories": {percentages.get('allCategories', 0):.2f},
                "housingLoan": {percentages.get('housingLoan', 0):.2f},
                "vehicleLoan": {percentages.get('vehicleLoan', 0):.2f},
                "educationalLoan": {percentages.get('educationalLoan', 0):.2f},
                "personalLoan": {percentages.get('personalLoan', 0):.2f},
                "goldLoan": {percentages.get('goldLoan', 0):.2f},
                "loanAgainstProperty": {percentages.get('loanAgainstProperty', 0):.2f}
            }},
            """
        
        analysis_result = ask_question(grouped_data, question, formatt)


        if isinstance(analysis_result, dict) and 'error' in analysis_result:
            return jsonify(analysis_result)
        
        return jsonify(analysis_result)

    except Exception as e:
        return jsonify({"error": str(e)})
    
##############################################################################################

 
def loan_summary(dataset):
    try:
        # Define the question and expected format
        question = f"""
        Summarize the loan data grouped by Loan Type, Year, and Month . Strictly Include the total no of logged in cases and total amount sanctioned, and format the results for a bar chart.
        """
        format_template = """
        {
        'barChart': {
                'chartHeader': [
                    {
                        "name": "Total Logged In Cases",
                        "totalcases": 300
                    },
                    {
                        "name": "Total Loan Amount",
                        "amt": 500000
                    }
                ],
                "barChartValues": [
                    
                        {
                            "businessLoan": 25,
                            "businessLoanamt": 34511,
                            "educationLoan": 25,
                            "educationLoanamt": 320143,
                            "goldLoan": 17,
                            "goldLoanamt": 23658,
                            "housingLoan": 24,
                            "housingLoanamt": 302780,
                            "loanAgainstProperty": 28,
                            "loanAgainstPropertyamt": 420171,
                            "personalLoan": 19,
                            "personalLoanamt": 42943,
                            "vehicleLoan": 17,
                            "vehicleLoanamt": 20773,
                            "month": "JAN"
                        },
                        {
                            "businessLoan": 35,
                            "businessLoanamt": 345100,
                            "educationLoan": 15,
                            "educationLoanamt": 320137,
                            "goldLoan": 20,
                            "goldLoanamt": 23650,
                            "housingLoan": 31,
                            "housingLoanamt": 302080,
                            "loanAgainstProperty": 27,
                            "loanAgainstPropertyamt": 120171,
                            "personalLoan": 14,
                            "personalLoanamt": 43943,
                            "vehicleLoan": 23,
                            "vehicleLoanamt": 50773,
                            "month": "FEB"
                        }
                    ...
                ]
            }
        }
        """
        # print(dataset.shape)

        dataset['Year'] = pd.to_datetime(dataset['Requested Date']).dt.year
        dataset['Month'] = pd.to_datetime(dataset['Requested Date']).dt.strftime('%b').str.upper()
 
        group_by_columns = ['Loan Type', 'Month']
        aggregations = {
            'Customer ID': 'count',
            'Loan Amount Sanctioned': 'sum'
        }
        rename_map = {
            'Customer ID': 'total_cases',
            'Loan Amount Sanctioned': 'total_amount'
        }
 
        processed_data = preprocess_data(
            dataset=dataset,
            group_by_columns=group_by_columns,
            aggregation_rules=aggregations,
            column_renames=rename_map
        )
        # print(processed_data)
        analysis_result = ask_question(processed_data, question, format_template)
 
        return jsonify(analysis_result)
 
    except Exception as e:
        return jsonify({"error": str(e)})

##############################################################################################


def api_ask_question(query_text, category=None, timeperiod=None ):

    # Get today's date for default start and end dates
    today = datetime.now()
    default_date = today.strftime("%b %Y")

    formatt = """
        {
            "startDate": "Oct 2024",
            "endDate": "Dec 2024",
            "region": "Pan India",
            "loanType": "Retail Loan",
            "queryType": "logged-in cases",
            "status": "allcategories"
        }
    """
    prompt = f"""
            You are given the text entered by the user. Use the text and extract the useful information as per the attached format.

            Rules:
            1. If the user doesn't mention a date range:
            - Default to the today's date {default_date}  in the format of month and year.
            - If the query mentions complete month name "december,september or similar", return only that month.

            2. For loanType:
            - If the query mentions "home loan, house loan" return "Housing Loan."
            - If it mentions "education loan," return "Education Loan."
            - If it mentions "car loan, bike loan or similiar" return "vehicle Loan."
            - Similarly, map other loanType explicitly if mentioned in the query.

            3. For status:
            - If the {category} is not None, then keep the status as {category} 
            - If the query specifies a particular loan type (e.g., "home loan"), set status to match the corresponding loan type (e.g., "housingLoan").
            - If no specific category is mentioned in query, keep status as "allcategories"

            4. For timeperiod
            - If the {timeperiod} is not None, then keep the type as {timeperiod}

            5. Ensure the extracted details include:
            - A valid startDate and endDate.
            - Region (default to "Pan India" if not specified).
            - A loanType that matches the context.
            - A queryType relevant to the query, default to "logged-in cases" if no specific query type is mentioned.

            Query:
            {query_text}

            Format the response strictly in this JSON format:
            {formatt}

            Answer:
        """
    try:
        completion = llm.chat.completions.create(
            model='gpt-4o-2',
            messages=[
                {"role": "system", "content": "You are an assistant that analyzes loan data."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5,
            max_tokens=4000
        )
        response = completion.choices[0].message.content
        if response.startswith("```json"):
            response = response[7:]
        if response.endswith("```"):
            response = response[:-3]

        response = response.strip()
        return response
    except Exception as e:
            return {"api_error is ": str(e)}


def extracter(response_json):
    try:
        start_date_str = response_json.get("startDate", "default_date")
        end_date_str = response_json.get("endDate", "default_date")
        loan_type = response_json.get("loanType", "")
        region = response_json.get("region", "")
        status = response_json.get("status","")
        print()

        start_date = datetime.strptime(start_date_str, "%b %Y")
        end_date = datetime.strptime(end_date_str, "%b %Y")

            # Determine the time period
        delta_months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1

        if delta_months < 3:
            time_period = "Monthly"
        elif delta_months < 12:
            time_period = "Quarterly"
        else:
            time_period = "Annually"

        dataset = pd.read_csv(r"C:\Users\chandinip\Downloads\Synthetic_Banking_Customer_Dataset_1 1.csv")
        # dataset = pd.read_csv(r"C:\week3_assignment\Synthetic_Banking_Customer_Dataset_1.csv")


        try:
            dataset['Approval Date'] = pd.to_datetime(dataset['Approval Date'], format="%d-%m-%Y", errors='coerce')
        except Exception as e:
            return jsonify({"error": f"Error parsing 'Approval Date': {e}"})

        # dataset = dataset.dropna(subset=['Approval Date'])
        dataset['Requested Date'] = pd.to_datetime(dataset['Requested Date'], errors='coerce')
        filtered_dataset = dataset[
            (dataset['Requested Date'] >= start_date) & (dataset['Requested Date'] <= end_date)
        ]

        start_date_1 = subtract_months(start_date, delta_months)
        end_date_1 = subtract_months(end_date, delta_months)

        dataset_1 = dataset[
            (dataset['Requested Date'] >= start_date_1) & (dataset['Requested Date'] <= end_date_1)
        ]
            # print(loan_type)

        if loan_type.lower() != "retail loan":
            filtered_dataset = filtered_dataset[filtered_dataset['Loan Type'].str.contains(loan_type, case=False, na=False)]
            dataset_1 = dataset_1[dataset_1['Loan Type'].str.contains(loan_type, case=False, na=False)]

        if region.lower() != "pan india":
            filtered_dataset = filtered_dataset[filtered_dataset['Branch'].str.contains(region, case=False, na=False)]
            dataset_1 = dataset_1[dataset_1['Branch'].str.contains(loan_type, case=False, na=False)]

        if filtered_dataset.empty:
            return {
                "queryResult": response_json,
                "message": "No records found for the given criteria."
            }
        # print(filtered_dataset.shape)
            
        loan_summary_response = loan_summary(filtered_dataset).get_json()
        print("loan_summary_response is done..")
        case_status_response = case_status(filtered_dataset).get_json()
        print("case_status_response is done..")
        progress_status_response = progress_status(filtered_dataset).get_json()
        print("progress_status_response is done..")
        categories_response = categories(filtered_dataset,time_period,loan_type).get_json()
        print("categories_response is done..")
        loan_processing_response = loan_processing(filtered_dataset, dataset_1).get_json()
        print("loan_processing_response is done..")

        # print(start_date, end_date)
        formatted_dates = format_date_range(start_date, end_date)

        aggregated_response = {
                "type": loan_type,
                "subCategory": status,
                "fromDate": formatted_dates[0],
                "toDate": formatted_dates[1],
                "timePeriod": time_period,
                "totalNoofCases":loan_summary_response['barChart']['chartHeader'][0]['totalcases'],
                "amount":loan_summary_response['barChart']['chartHeader'][1]['amt'],
                "currency":"USD",
                "cases": loan_summary_response['barChart']["barChartValues"],
                "loanProcessingMetrics":loan_processing_response["loanProcessingMetrics"],
                "caseStatus": case_status_response["casesStatus"],
                "progressStatus": progress_status_response["progressStatus"],
                "categories": categories_response['categories']
            }

        return aggregated_response
    except json.JSONDecodeError as e:
        return jsonify({"error": "Invalid JSON format in response", "details": str(e)})
    except Exception as e:
        return {"extracter_error is ": str(e)}

##############################################################################################

@app.route('/query-search', methods=['GET'])
def search():
    data = request.json
    query_text = data.get("query", "")
    try:
        response_json = json.loads(api_ask_question(query_text))
        return jsonify(extracter(response_json))
    except json.JSONDecodeError as e:
        return jsonify({"error": "Invalid JSON format in response", "details": str(e)})
    except Exception as e:
        return {"search_error is ": str(e)}
    
##############################################################################################


@app.route('/category-selected', methods=['GET'])
def category_selected():
    data = request.json
    
    query_text = data.get("query", "")
    catType = data.get("categoryType","")

    try:
        response_cat_json = json.loads(api_ask_question(query_text, catType))
        print(response_cat_json)
        return jsonify(extracter(response_cat_json))
    except json.JSONDecodeError as e:
        return jsonify({"error": "Invalid JSON format in response", "details": str(e)})
    except Exception as e:
        return jsonify({"category_error is ": str(e)})




##############################################################################################

@app.route('/time-period-selected', methods=['GET'])
def time_period_selected():

    data = request.json
    query_text = data.get("query", "")
    time_periodType = data.get("timePeriod", "").lower()

    if not query_text or not time_periodType:
        return jsonify({"error": "Missing required fields 'query' or 'timePeriod'"})

    try:
        response_timeperiod_json = json.loads(api_ask_question(query_text, time_periodType))
        result = extracter(response_timeperiod_json)

       
        return jsonify(result)
    except json.JSONDecodeError as e:
        return jsonify({"error": "Invalid JSON format in response", "details": str(e)})
    except Exception as e:
        return {"timePeriod_error is ": str(e)}


if __name__ == "__main__":     
    app.run(debug=True)