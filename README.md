# Haensel AMS - Airflow DAG for DE Challenge

This project is an Airflow DAG implementation designed to process and analyze marketing customer journey data. The DAG extracts data from a PostgreSQL database, processes it, and performs API calls for customer journey analysis. It then generates a channel report and saves the results back to the database.

## Table of Contents
1. [Overview](#overview)
2. [Dependencies](#dependencies)
3. [Environment Variables](#environment-variables)
4. [Postgres Variables](#postgres-variables)
5. [DAG Workflow](#dag-workflow)

## Overview

This DAG is used to extract data, process it, and call an external API to compute IHC (Initializer Holder Closer) values for customer journeys. The results are saved back to a PostgreSQL database, and a final channel report is created and stored. The pipeline involves multiple tasks such as data extraction, batch processing, API calls, and report generation.

## Dependencies

- **Apache Airflow**: The task scheduler and orchestrator.
- **PostgreSQL**: Database for storing and retrieving attribution data.
- **Pandas**: Data manipulation and analysis.
- **Requests**: HTTP library for calling external APIs.
- **Airflow PostgresHook**: To interact with PostgreSQL from Airflow.

## Environment Variables

The following environment variables are required to run the DAG:

- `API_KEY`: The API key used for authenticating requests to the [IHC API](https://ihc-attribution.com/marketing-attribution-api/).
- `CONV_TYPE_ID`: The conversion type used after train the model in the IHC platform.
- `MAX_CUSTOMER_JOURNEYS`: Maximum number of sessions per customer journeys (conversion_id) the API can consume [Limits And Quotas](https://ihc-attribution.com/marketing-attribution-api/#api-anchor-5).
- `MAX_SESSIONS`: Maximum number of sessions per batch [Limits And Quotas](https://ihc-attribution.com/marketing-attribution-api/#api-anchor-5).
- `START_DATE`: The start date for extracting the data of the conversions table.
- `END_DATE`: The end date for extracting the data of the conversions table.

These variables are stored as **Airflow Variables** (JSON format) and can be configured in the Airflow UI.

### Example JSON for Airflow Variables:
```json
{
    "API_KEY": "XXXXX-XXXXX-XXXXXX",
    "CONV_TYPE_ID": "challenge_test_02",
    "MAX_CUSTOMER_JOURNEYS": 100,
    "MAX_SESSIONS": 200,
    "START_DATE": "2023-09-01",
    "END_DATE": "2023-09-07"
}
```
## Postgres Variables

The following hard coded variables are required to run the DAG:

- `table_name_customer_journey`: Name of the Table created in Postgres to retain the API response results.
- `table_name_channel_reporting`: Name of Table created in Postgres to retain the Channel Report data.
- `schema_name`: Name of the schema created for this project.
- `conflict_columns`: Columns used as primary key (PK) in the **table_name_customer_journey** table.
- `conflict_columns_channel_reporting`: Columns used as primary key (PK) in the **table_name_channel_reporting** table.

![postgres_schema_table_names](assets\images\postgres_schema_table_names.png)

![postgres_db_structure](assets\images\postgres_db_structure.png)

### Postgress Airflow Connection
- Configure under Airflow UI > Admin > Connections
- Select a Postgres connection type
- Configure the Host (as seen in the image)
- Database (In this case I created a new DB called challenge, but the normal would be the Public)
- Login and Password (as defined by user in the moment to configure the Postgres DB)

![postgres_airflow_conn](assets\images\postgres_airflow_conn.png)

# DAG Workflow

![airflow_tasks_dependencies](assets\images\airflow_tasks_dependencies.png)

## Data Extraction (extract_data):

Fetches conversion, session, and session cost data from the PostgreSQL database for the specified date range.
- First retrieves all data from conversions table with a specific date range.
- Separates a list of user_ids to be used to query the session_sources table
- Retrieve all sessions in sessions_sources table where the user_id exists
- Separates a list of sessions_ids to be used to query the session_costs table
- retrieve also all sessions from sessions_costs where the session

This logic is used to be not only eficient when consulting data from our DB, but also implements a data range option in case we need to filter our data based on a date range. 

## Data Processing (process_data_to_api)

Data Processing (process_data_to_api)
Processes the extracted data and prepares it for API consumption by structuring and batching the data efficiently.

- Reads the extracted data from conversions, session sources, and session costs.
- Merges session sources with conversion data based on user_id, ensuring all session interactions before a conversion are considered.
- Filters out sessions that occurred after the corresponding conversion and assigns the conversion attribute (0 or 1) to mark the last session of each conversion_id.
- Formats timestamps and structures the data to match the required API input format.
- Splits the processed data into manageable batches, ensuring each batch adheres to API limitations while maintaining efficiency.
- Optimization for API Constraints

This step optimizes the data structure for API calls, considering both the limitations of a free API account and the standard API constraints. Efficient batch creation is crucial to retain accuracy and performance.

First, the function identifies any conversion_id with more than 100 associated sessions, since this exceeds the API limit. Fortunately, in our dataset, the highest number of sessions for a single conversion was 37.
![example_count_session_per_conv_id](assets\images\example_count_session_per_conv_id.png)

Next, it creates batches of up to 200 sessions, ensuring that all sessions related to a specific conversion_id remain in the same batch. This preserves the data's integrity and reliability.

## API Call (call_api)

API Call (call_api)
Sends processed customer journey data to the IHC API for attribution calculations and stores the results in the database.

- Parallel Processing with `expand` : Each batch is processed in parallel, improving execution time by leveraging Airflow’s dynamic task mapping.
- Efficient API Calls: Sends each batch as a structured JSON payload to the IHC API, ensuring minimal API load per request.
- Direct Database Storage: Saves the API response directly to PostgreSQL, avoiding the need to store results as an XCom variable (which has a 1GB limitation).
- Incremental Storage: Each batch result is stored immediately after processing, eliminating the need for aggregation before database insertion.
- Error Handling: Logs API failures and skips problematic batches when necessary to prevent DAG failures.

This approach ensures high performance by enabling parallel execution, reducing memory overhead, and optimizing data storage in the database. 🚀

## Channel Report Creation (create_channel_report)
Generates aggregated channel performance reports using extracted data and API results.
- Merges session sources, session costs, and attribution data from the database.
- Integrates the attribution data to associate costs and revenue with different channels.
- Computes key performance metrics, including IHC-based revenue, cost-per-order (CPO), and return on ad spend (ROAS).
- Aggregates the results by channel and date to create a structured performance report.
- Saves the final report in the channel_reporting table, ensuring up-to-date marketing insights.

This step provides a data-driven view of marketing channel efficiency, enabling better decision-making.
