import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from openai import AzureOpenAI
import json
import warnings
from datetime import datetime
from calendar import monthrange
import re
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
    """
    Preprocess the dataset by grouping and aggregating data.

    Args:
        dataset (pd.DataFrame): The input dataset.
        group_by_columns (list): Columns to group by.
        aggregation_rules (dict): Aggregation rules.
        column_renames (dict, optional): Columns to rename.
        conversion_columns (list, optional): Columns to convert.
        conversion_rate (float, optional): Conversion rate.

    Returns:
        pd.DataFrame: The processed dataset.
    """
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

    Ensure that the response is always valid JSON and strictly adheres to the provided format. Do not include any additional information or explanations.

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
        return {"error": "Failed to get completion from Azure OpenAI", "details": str(e)}
    

def format_date_range(start_date_str, end_date_str):
    """
    Format the start and end dates to the first and last day of the month respectively.

    Args:
        start_date_str (datetime): The start date.
        end_date_str (datetime): The end date.

    Returns:
        Union[Tuple[str, str], dict]: A tuple with formatted start and end dates, or an error dictionary.
    """

    try:
        # Convert start date to 1st day of the month
        formatted_start_date = start_date_str.replace(day=1).strftime("%d-%m-%Y")
                    
        # Convert end date to the last day of the month
        last_day = monthrange(end_date_str.year, end_date_str.month)[1]
        formatted_end_date = end_date_str.replace(day=last_day).strftime("%d-%m-%Y")
                    
        return (formatted_start_date, formatted_end_date)
                
    except ValueError as e:
        return {"error": f"Invalid date format: {e}"}

    
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
            Summarize the loan cases by process status. For each status, provide the following details:
            - status: The process status (e.g., "WORK_IN_PROGRESS", "SANCTIONED", "DISBURSED").
            - amount: The total amount for the status.
            - caseCount: The count of cases for the status.
            - fillColor_percentage: The percentage of the total amount represented by this status. It should always same as in the formatt.
            - color: A color code associated with the status. It should always same as in the formatt.

            Provide the result in the specified JSON format.
        """
        formatt = """
        {
            "caseStatusForBarchart": {
                "total": [
                    {
                        "status": "WORK_IN_PROGRESS",
                        "amount": 1489348.00,
                        "caseCount": 1,
                        "fillColor_percentage": 21.23,
                        "color": "#7E3BF0"
                    },
                    {
                        "status": "SANCTIONED",
                        "amount": 1626610.00,
                        "caseCount": 2,
                        "fillColor_percentage": 23.19,
                        "color":"#B0AFFF"
                    },
                    {
                        "status": "DISBURSED",
                        "amount": 3610340.00,
                        "caseCount": 3,
                        "fillColor_percentage": 55.58,
                        "color":"#3B39F0"
                    }
                ]
            }
        }
    """
        
        analysis_result = ask_question(processed_data, question, formatt)

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

        # Check if there are multiple branches in the dataset
        total_branches = len(data['Branch'].unique())

        # Update branch names to include total branch count only if there are multiple branches in the dataset
        grouped_summary['Branch'] = grouped_summary['Branch'].apply(
            lambda x: f"{x.split('-')[0].strip()} - {branch_counts.get(x.split('-')[0].strip(), 1)}" 
            if total_branches > 1 else x.split('-')[0].strip()
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

        Ensure that the response is always valid JSON and strictly adheres to the above format.
        """
        formatt = """
        {
            "progressStatus": {
                "commonTitle": "Progress Status",
                "items" : [
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
        }
        """

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
            "metrics" : {
                "commonTitle": "Loan Processing",
                "items": [
                    {
                    "subTitle": "Approval Rate",
                    "value": "82%",
                    "diffValue": "1.5%",
                    "color": "red",
                    "direction":"down"
                    },
                    {
                    "subTitle": "Denial Rate",
                    "value": "22.5%",
                    "diffValue": "2.1%",
                    "color": "green",
                    "direction":"up"
                    },
                    {
                    "subTitle": "Submission Cancelled",
                    "value": "32%",
                    "diffValue": "1.5%",
                    "color": "red",
                    "direction":"down"
                    }
                ]
            }
        }
        """
        prompt = f"""
            You are given the following processed loan datasets and a question. Use the datasets to answer the question in the exact JSON format provided below.

            Processed Loan Data:
            {processed_data_1.to_dict(orient='records')}, {processed_data_2.to_dict(orient='records')}

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
        
        try:
            response_json = json.loads(response)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format in LLM response: {e}")

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

        # subCategory = ""
        percentages = {
            "allCategories": 0,
            "housingLoan": 0,
            "vehicleLoan": 0,
            "educationalLoan": 0,
            "personalLoan": 0,
            "goldLoan": 0,
            "loanAgainstProperty": 0
        }

        # if loan_type.lower() == "retail loan":
        #     subCategory = "allCategories"
        #     percentages = grouped_data.set_index('Loan Type')['percentage'].to_dict()
        # else:
        #     subCategory = loan_type.lower()
        #     for loan in percentages.keys():
        #         percentages[loan] = grouped_data.loc[grouped_data['Loan Type'].str.lower() == subCategory, 'percentage'].sum() if loan == subCategory else 0

        question = f"""
            Format the loan data into a JSON structure where each loan type's name is a key, and the values are calculated by finding the percentage of the total count.
            Strictly follow the format of dataKey. Add "timePeriod": "{time_period}" and "subCategories": "{loan_type}" at the top level.
            Ensure that the response includes a "selected" field for each category, which is true for the subCategory and false for others.

            Rules:
            1. Each loan type's name should be a key in the JSON structure.
            2. The value for each key should be the percentage of the total count.
            3. Add "timePeriod": "{time_period}" at the top level.
            4. Add "subCategories": "{loan_type}" at the top level.
            5. Include a "selected" field for each category:
            - Set "selected" to true for the subCategory.
            - Set "selected" to false for all other categories.
            6. Ensure the response is always valid JSON and strictly adheres to the format.
        """

        formatt = f"""
        {{
            "categoryKeyArr": [
            {{"dataKey": "ALL_CATEGORIES", "fill": "#4A3AFF", "label": "All Categories", "selected": true}},
            {{"dataKey": "HOUSING_LOAN", "fill": "#962DFF", "label": "Housing Loan", "percentageValue": "{percentages.get('housingLoan', 0)}%", "selected": false}},
            {{"dataKey": "VEHICLE_LOAN", "fill": "#4A3AFF", "label": "Vehicle Loan", "percentageValue": "{percentages.get('vehicleLoan', 0)}%", "selected": false}},
            {{"dataKey": "EDUCATIONAL_LOAN", "fill": "#E0C6FD", "label": "Educational Loan", "percentageValue": "{percentages.get('educationalLoan', 0)}%", "selected": false}},
            {{"dataKey": "PERSONAL_LOAN", "fill": "#D2DCFE", "label": "Personal Loan", "percentageValue": "{percentages.get('personalLoan', 0)}%", "selected": false}},
            {{"dataKey": "GOLD_LOAN", "fill": "#7A47B4", "label": "Gold Loan", "percentageValue": "{percentages.get('goldLoan', 0)}%", "selected": false}},
            {{"dataKey": "LOAN_AGAINST_PROPERTY", "fill": "#4B66C5", "label": "Loan Against Property", "percentageValue": "{percentages.get('loanAgainstProperty', 0)}%", "selected": false}}
            ]
        }}
        """
        
        analysis_result = ask_question(grouped_data, question, formatt)

        # print("  ")
        # print("############################################################################")
        # print("  ")
        # print(analysis_result)

        if isinstance(analysis_result, dict) and 'error' in analysis_result:
            return jsonify(analysis_result)
        
        return jsonify(analysis_result)

    except Exception as e:
        return jsonify({"error": str(e)})
    
##############################################################################################

 
def loan_summary(dataset):
    try:
        # Define the question and expected format
        question = """
        Summarize the loan data grouped by Loan Type, Year, and Month. Strictly include the total number of logged-in cases and total amount sanctioned, and format the results for a bar chart.
        The JSON should include:
        - "barChartLeftLbl": Label for the left side of the bar chart.
        - "barChartRightLbl": Label for the right side of the bar chart.
        - "barChartRightValue": Total loan amount.
        - "barChartLeftValue": Total number of logged-in cases. It should be correct values.
        - "chartDetails": An array of objects, each representing a month with the following properties:
            - "name": The month and year (e.g., "JAN-2024").
            - For each loan type (e.g., "HOUSING_LOAN", "GOLD_LOAN"), include:
            - "cases": The total number of cases. If number of cases is 0, then don't add the loanType in response (ex: if number of cases is 0 for education loan , then don't show "EDUCATIONAL_LOAN" in response)
            - "amount": The total loan amount.

        Ensure that the response is always valid JSON and strictly adheres to the above format.
        """
        format_template = """
        {
            "barChart": {
                "barChartLeftLbl": "Total Logged In Cases",
                "barChartRightLbl": "Total Loan Amount",
                "barChartRightValue": 12563000,
                "barChartLeftValue": 32000,
                "chartDetails": [
                    {
                        "name": "JAN-2024",
                        "HOUSING_LOAN": { "cases": 111, "amount": 111 },
                        "GOLD_LOAN": { "cases": 111, "amount": 111 },
                        "LOAN_AGAINST_PROPERTY": { "cases": 111, "amount": 111 },
                        "VEHICLE_LOAN": { "cases": 111, "amount": 111 },
                        "EDUCATIONAL_LOAN": { "cases": 111, "amount": 111 }, 
                        "PERSONAL_LOAN": { "cases": 111, "amount": 111 }
                    },
                    { "name": "FEB-2024", 
                        "HOUSING_LOAN": { "cases": 111, "amount": 111 },
                        "GOLD_LOAN": { "cases": 111, "amount": 111 },
                        "LOAN_AGAINST_PROPERTY": { "cases": 111, "amount": 111 },
                        "VEHICLE_LOAN": { "cases": 111, "amount": 111 },
                        "EDUCATIONAL_LOAN": { "cases": 111, "amount": 111 }, 
                        "PERSONAL_LOAN": { "cases": 111, "amount": 111 }
                    },
                    { 
                        "name": "MAR-2024", 
                        "HOUSING_LOAN": { "cases": 111, "amount": 111 },
                        "GOLD_LOAN": { "cases": 111, "amount": 111 },
                        "LOAN_AGAINST_PROPERTY": { "cases": 111, "amount": 111 },
                        "VEHICLE_LOAN": { "cases": 111, "amount": 111 },
                        "EDUCATIONAL_LOAN": { "cases": 111, "amount": 111 }, 
                        "PERSONAL_LOAN": { "cases": 111, "amount": 111 }
                    }
                ]
            }
        }
        """
        
        dataset['Year'] = pd.to_datetime(dataset['Requested Date']).dt.year
        dataset['Month'] = pd.to_datetime(dataset['Requested Date']).dt.strftime('%b-%Y').str.upper()

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

        analysis_result = ask_question(processed_data, question, format_template)

        return jsonify(analysis_result)

    except Exception as e:
        return jsonify({"error": str(e)})
    

##############################################################################################


def api_ask_question(query_text, category=None, timePeriod=None):
    formatt = """
        {
            "startDate": "Oct 2024",
            "endDate": "Dec 2024",
            "region": "Pan India",
            "loanType": "retailLoan",
            "queryType": "logged-in cases",
            "status": "allcategories",
            "timePeriod": "quarterly"
        }
    """
    today = datetime.now()
    default_date = today.strftime("%Y-%m-%d")
    print(timePeriod)
    prompt = f"""
    You are a system that extracts structured information from user queries. Based on the query, determine details for loan analytics.

    ### Inputs:
    - **Default Date**: {default_date} (use this as the base date for calculations when no explicit date is mentioned).
    - **Timeperiod**: {timePeriod} (can be "monthly", "quarterly", "annually", or None).

    ### Rules:

    1. **Date Extraction**:
        - First, analyze the query for explicit mentions of dates, months, or years:
            - If a specific month and year (e.g., "Sep 2024") is mentioned, extract it.
            - If only a year is mentioned (e.g., "2024"), assume the entire year.
            - If it mentions a range (e.g., "from Jan 2024 to Mar 2024"), extract the start and end dates.
            - If it specifies quarterly data and given a year, extract the first quarter of the year, Or if the quarter is mentioned, extract the corresponding dates, or if the month and year are mentioned, extract the corresponding quarter.
            - If a relative time period is mentioned (e.g., "previous year", "this month"), infer the corresponding dates based on `default_date`.
    
    2. **Default Date Behavior**:
        - If no explicit dates are mentioned in the query:
            - Default to the current month and year from `default_date`.
            - Set `startDate` to the first day of the current month and `endDate` to the last day of the current month.

    3. **Timeperiod Adjustment**:
        - After extracting dates from the query:
            - **If `timePeriod` is not None**:
                - Adjust the dates based on the specified `timePeriod`:
                    - **If it is "monthly"**:
                        - If the query already specifies quarterly or yearly data:
                            - Adjust `startDate` to the first day of the first month in the specified period.
                            - Adjust `endDate` to the last day of the same month.
                        - Otherwise, adjust `startDate` and `endDate` to the extracted month's boundaries.
                    - **If it is "quarterly"**:
                        - If the query already specifies yearly data:
                            - Identify the quarter based on the extracted year and adjust:
                                - Q1: Jan 1 to Mar 31
                                - Q2: Apr 1 to Jun 30
                                - Q3: Jul 1 to Sep 30
                                - Q4: Oct 1 to Dec 31
                        - If the query specifies monthly data, expand the range to include the full quarter of the extracted month.
                    - **If it is "annually"**:
                        - Adjust `startDate` to Jan 1 of the extracted year and `endDate` to Dec 31 of the same year.
            - **If `timePeriod` is None**:
                - Leave the extracted dates unchanged.

    4. For `loanType`:
        - If `category` is "all categories", set `loanType` to "retailLoan".
            - If a `category` is provided and is not "all categories", always set `loanType` to the value of `category`, regardless of what the query mentions.
            - If no `category` is provided, determine the loan type from the query:
                - If the query mentions "home loan," set `loanType` to "Housing Loan."
                - If the query mentions "car loan", "bike loan", or any similar query with "vehicle", set `loanType` to "Vehicle Loan."
                - If the query mentions "loan against property or loanagainstproperty", set `loanType` to "Loan Against Property"
                - Map other loan types explicitly if mentioned in the query.
                - If no specific loan type is mentioned, default to "retailLoan".

    5. For `status`:
         - If `category` is "all categories", set `status` to "allcategories".
            - If a `category` is provided and is not "all categories", always set `status` to the value of `category`, regardless of what the query mentions.
            - If no `category` is provided, determine the status from the query:
                - If the query specifies a loan type (e.g., "home loan", "gold loan"), set `status` to match the corresponding loan type (e.g., "housingLoan", "goldLoan").
                - If no specific loan type is mentioned, default to "allcategories".

    6. Ensure the extracted details include:
        - A valid `startDate` and `endDate` in the format `YYYY-MM-DD`.
        - `Region` (default to "Pan India").
        - A `loanType` matching the `category` or query context.
        - A `queryType` (default to "logged-in cases").
    

    7. **For `timePeriod` in the output JSON**:
        - Determine the value dynamically based on the calculated `startDate` and `endDate`:
            - **"monthly"**: If the difference between `startDate` and `endDate` is within one month.
            - **"quarterly"**: If the difference between `startDate` and `endDate` spans three months.
            - **"annually"**: If the difference between `startDate` and `endDate` spans twelve months.
        - Ensure that the `timePeriod` aligns with the final determined date range.
    Query:
    {query_text}

    Additional Information:
    - Category provided: {category}
    - TimePeriod provided: {timePeriod}

    Format the response strictly in this JSON format:
    {formatt}

    Answer:
    """


    try:
        completion = llm.chat.completions.create(
            model="gpt-4o-2",
            messages=[{"role": "system", "content": "You are an assistant that extracts and formats data for loan analytics."},
                      {"role": "user", "content": prompt}],
            temperature=0.5,
            max_tokens=4000
        )
        response = completion.choices[0].message.content

        # Extract JSON from the response
        if response.startswith("```json"):
            response = response[7:]
        if response.endswith("```"):
            response = response[:-3]

        response_data = json.loads(response)

        # Validate and fix dates
        start_date = response_data.get("startDate", "").strip()
        end_date = response_data.get("endDate", "").strip()

        if not start_date or not end_date:
            raise ValueError("Missing or invalid startDate/endDate in response.")

        return response_data

    except ValueError as date_error:
        # Provide fallback for invalid dates
        current_month = today.strftime("%b")
        current_year = today.strftime("%Y")
        return {
            "startDate": f"{current_month} 01, {current_year}",
            "endDate": f"{current_month} 31, {current_year}",
            "error": f"Date parsing failed: {str(date_error)}"
        }
    except Exception as e:
        return {"error from the LLM": str(e)}



def extracter(response_json):
    try:
        start_date_str = response_json.get("startDate", "")
        end_date_str = response_json.get("endDate", "")
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str,"%Y-%m-%d")
        loan_type = response_json.get("loanType", "")
        region = response_json.get("region", "")
        status = response_json.get("status","")
        

        # # Determine the time period
        delta_months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1

        # if delta_months < 3:
        #     time_period = "monthly"
        # elif delta_months < 12:
        #     time_period = "quarterly"
        # else:
        #     time_period = "Annually"
        time_period = response_json.get("timePeriod", "")
        dataset = pd.read_csv(r"C:\week3_assignment\Synthetic_Banking_Customer_Dataset_1.csv")
        # dataset = pd.read_csv(r"C:\Users\chandinip\Downloads\Synthetic_Banking_Customer_Dataset_1 1.csv")

        try:
            dataset['Approval Date'] = pd.to_datetime(dataset['Approval Date'], format="%d-%m-%Y", errors='coerce')
        except Exception as e:
            return {"error": f"Error parsing 'Approval Date': {e}"}

        dataset['Requested Date'] = pd.to_datetime(dataset['Requested Date'], errors='coerce')
        filtered_dataset = dataset[
            (dataset['Requested Date'] >= start_date) & (dataset['Requested Date'] <= end_date)
        ]

        start_date_1 = subtract_months(start_date, delta_months)
        end_date_1 = subtract_months(end_date, delta_months)

        dataset_1 = dataset[
            (dataset['Requested Date'] >= start_date_1) & (dataset['Requested Date'] <= end_date_1)
        ]

        if loan_type.lower() != "retailloan":
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
        
        # loan_summary_response = loan_summary(filtered_dataset).get_json()
        # print("loan_summary_response")
        # print(" ")
        # print(loan_summary_response)
        # case_status_response = case_status(filtered_dataset).get_json()
        # print("case_status_response")
        # print(" ")
        # print(case_status_response)
        # progress_status_response = progress_status(filtered_dataset).get_json()
        # print("progress_status_response")
        # print(" ")
        # print(progress_status_response)
        # categories_response = categories(filtered_dataset,time_period,loan_type).get_json()
        # print("categories_response")
        # print(" ")
        # print(categories_response)
        # loan_processing_response = loan_processing(filtered_dataset, dataset_1).get_json()
        # print("loan_processing_response")
        # print(" ")
        # print(loan_processing_response)
        print("Started to load....")
        loan_summary_response = loan_summary(filtered_dataset).get_json()
        print("loan_summary_response....")
        case_status_response = case_status(filtered_dataset).get_json()
        print("case_status_response....")
        progress_status_response = progress_status(filtered_dataset).get_json()
        print("progress_status_response....")
        categories_response = categories(filtered_dataset,time_period,loan_type).get_json()
        print("categories_response....")
        loan_processing_response = loan_processing(filtered_dataset, dataset_1).get_json()
        print("loan_processing_response....")

        formatted_dates = format_date_range(start_date, end_date)

        aggregated_response = {
            "type": loan_type,
            "subCategory": status,
            "fromDate": formatted_dates[0],
            "toDate": formatted_dates[1],
            "timePeriod": time_period,
            "currency": "USD",
            "barChart": loan_summary_response['barChart'],
            "metrics": loan_processing_response["metrics"],
            "caseStatusForBarchart": case_status_response["caseStatusForBarchart"],
            "progressStatus": progress_status_response["progressStatus"],
            "categoryKeyArr": categories_response['categoryKeyArr']
        }

        return aggregated_response
    except json.JSONDecodeError as e:
        return {"error": "Invalid JSON format in response", "details": str(e)}
    except Exception as e:
        return {"error while aggregating the response ": str(e)}
    

##############################################################################################

@app.route('/search', methods=['GET'])
def search():
    data = request.json
    query_text = data.get("query", "")
    try:
        response_json = api_ask_question(query_text)
        print(response_json)
        return jsonify(extracter(response_json))
    except json.JSONDecodeError as e:
        return jsonify({"error": "Invalid JSON format in response", "details": str(e)})
    except Exception as e:
        return jsonify({"error is ": str(e)})
    
##############################################################################################

@app.route('/categorySelected', methods=['GET'])
def category_selected():
    data = request.json
    query_text = data.get("query", "")
    catType = data.get("categoryType","")

    try:
        response_json = api_ask_question(query_text, category=catType, timePeriod=None)
        return jsonify(extracter(response_json))
    except json.JSONDecodeError as e:
        return jsonify({"error": "Invalid JSON format in response", "details": str(e)})
    except Exception as e:
        return jsonify({"error is ": str(e)})




##############################################################################################

@app.route('/time-period-selected', methods=['GET'])
def time_period_selected():

    data = request.json
    query_text = data.get("query", "")
    time_periodType = data.get("timePeriod", "").lower()

    try:
        response_timeperiod_json = api_ask_question(query_text, category=None, timePeriod=time_periodType)
        print(response_timeperiod_json)
        result = extracter(response_timeperiod_json)

       
        return jsonify(result)
    except json.JSONDecodeError as e:
        return jsonify({"error": "Invalid JSON format in response", "details": str(e)})
    except Exception as e:
        return {"timePeriod_error is ": str(e)}

if __name__ == "__main__":     
    app.run(debug=True)