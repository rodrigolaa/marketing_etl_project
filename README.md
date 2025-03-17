# AWS CHALLENGE - Airflow DAG for Attribution Analysis

This project is an Airflow DAG implementation designed to process and analyze attribution data. The DAG extracts data from a PostgreSQL database, processes it, and performs API calls for customer journey analysis. It then generates a channel report and saves the results back to the database.

## Table of Contents
1. [Overview](#overview)
2. [Dependencies](#dependencies)
3. [Environment Variables](#environment-variables)
4. [DAG Workflow](#dag-workflow)
5. [Setup Instructions](#setup-instructions)
6. [Task Descriptions](#task-descriptions)

## Overview

This DAG is used to extract data, process it, and call an external API to compute IHC (Incremental Human Contribution) values for customer journeys. The results are saved back to a PostgreSQL database, and a final channel report is created and stored. The pipeline involves multiple tasks such as data extraction, batch processing, API calls, and report generation.

## Dependencies

- **Apache Airflow**: The task scheduler and orchestrator.
- **PostgreSQL**: Database for storing and retrieving attribution data.
- **Pandas**: Data manipulation and analysis.
- **Requests**: HTTP library for calling external APIs.
- **Airflow PostgresHook**: To interact with PostgreSQL from Airflow.

## Environment Variables

The following environment variables are required to run the DAG:

- `API_KEY`: The API key used for authenticating requests to the IHC API.
- `CONV_TYPE_ID`: The type of conversion for the customer journey analysis.
- `MAX_CUSTOMER_JOURNEYS`: Maximum number of customer journeys per batch.
- `MAX_SESSIONS`: Maximum number of sessions per batch.
- `START_DATE`: The start date for extracting the data.
- `END_DATE`: The end date for extracting the data.

These variables are stored as **Airflow Variables** (JSON format) and can be configured in the Airflow UI.

### Example JSON for Airflow Variables:
```json
{
    "API_KEY": "809fdccd-7467-4bb5-8669-cb37a96eb25e",
    "CONV_TYPE_ID": "challenge_test_02",
    "MAX_CUSTOMER_JOURNEYS": 100,
    "MAX_SESSIONS": 200,
    "START_DATE": "2023-09-01",
    "END_DATE": "2023-09-07"
}

DAG Workflow
Data Extraction (extract_data):

Fetches conversion, session, and session cost data from the PostgreSQL database for the specified date range.
Data Processing and Batch Splitting (process_data_to_api):

Merges conversion data with session data.
Filters and prepares the data for API submission.
Splits the data into batches based on conversion ID.
API Call (call_api):

Sends data batches to the IHC API for human contribution computation.
Saves the results back to the PostgreSQL database.
Channel Report Creation (create_channel_report):

Creates a report from the processed session data.
Calculates key metrics (Cost per Order, Return on Ad Spend) and saves the report to the database.
Setup Instructions
Install Airflow: Follow the instructions on the Airflow official website to install and set up Apache Airflow in your environment.

Set Up PostgreSQL: Ensure that a PostgreSQL database is available and the connection is configured in Airflow. You can configure the connection in the Airflow UI or through environment variables.

Configure Airflow Variables:

Go to the Airflow UI > Admin > Variables.
Add the JSON environment variables (mentioned above) in the Airflow Variables section.
Upload the DAG:

Copy the aws_challenge DAG to your Airflow DAGs folder.
Ensure all dependencies and connections are set up correctly in Airflow.
Run the DAG: Trigger the DAG manually from the Airflow UI or wait for the scheduled time (0 9,15 * * * for 9:00 AM and 3:00 PM daily).

Task Descriptions
extract_data(start_date, end_date):

Extracts conversion, session, and session cost data from the PostgreSQL database.
Returns the data as a dictionary of JSON strings.
split_into_batches(df):

Splits the DataFrame into batches based on conversion ID and customer journey limits.
process_data_to_api(data_dict):

Processes the extracted data, merges it, and filters it for API submission.
Splits the data into manageable batches.
call_api(batch):

Calls the IHC API with the provided batch of data and saves the results to the database.
create_channel_report(data_dict):

Generates a report from the session sources, session costs, and conversion data.
Calculates metrics like CPO (Cost per Order) and ROAS (Return on Ad Spend), and saves the report to the database.


### How to Use This `README.md`:

- Copy and paste the above content into a `README.md` file in the root of your repository.
- Modify any specific details (like database configurations or external dependencies) if needed.
- Ensure the instructions align with your setup.

This `README.md` should help others understand your DAG, how to set it up, and how to run it. Let me know if you need any changes!
