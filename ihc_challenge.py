from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import time

# Get the dictionary from the Airflow Variables
variables = Variable.get('IHC', deserialize_json=True)
CONV_TYPE_ID = variables['CONV_TYPE_ID']
API_KEY = variables['API_KEY']
MAX_CUSTOMER_JOURNEYS = variables['MAX_CUSTOMER_JOURNEYS']
MAX_SESSIONS = variables['MAX_SESSIONS']
START_DATE = variables['START_DATE']
END_DATE = variables['END_DATE']

# Postgres database varaibles
table_name_customer_journey = 'attribution_customer_journey'
table_name_channel_reporting = 'channel_reporting'
schema_name = 'data_aws'
conflict_columns = ['conversion_id', 'session_id']
conflict_columns_channel_reporting = ['channel_name', 'date']


@dag(
    dag_id='AWS_CHALLENGE',
    schedule='0 9,15 * * *',
    start_date=datetime(2022, 10, 14),
    catchup=False,
    tags=['JOB_INTERVIEW']
)
def aws_challenge():
    
    @task()
    def extract_data(start_date, end_date):

        hook = PostgresHook(postgres_conn_id="postgres_conn")

        conversions = hook.get_records(f"SELECT * FROM {schema_name}.conversions  WHERE conv_date BETWEEN '{start_date}' and '{end_date}';")
        user_ids = [f"'{row[1]}'" for row in conversions]
        user_ids_str = ", ".join(user_ids)

        session_sources = hook.get_records(f"SELECT * FROM {schema_name}.session_sources  WHERE user_id in ({user_ids_str});")
        session_ids = [f"'{row[0]}'" for row in session_sources]
        session_ids_str = ", ".join(session_ids)

        session_costs = hook.get_records(f"SELECT * FROM {schema_name}.session_costs  WHERE session_id in ({session_ids_str});")
        
        conversions_df = pd.DataFrame(conversions, columns=["conv_id", "user_id", "conv_date","conv_time","revenue"]) 
        session_sources_df = pd.DataFrame(session_sources, columns=["session_id", "user_id", "event_date","event_time", "channel_name", "holder_engagement", "closer_engagement", "impression_interaction"])
        session_costs_df = pd.DataFrame(session_costs, columns=["session_id", "cost"])

        print(conversions_df.head())
        print(session_sources_df.head())
        return {
            "conversions": conversions_df.to_json(),
            "session_sources": session_sources_df.to_json(),
            "session_costs": session_costs_df.to_json()

        }


    def split_into_batches(df):

        """Splits the DataFrame into manageable batches"""
        conv_counts = df["conversion_id"].value_counts()
        large_conv_ids = conv_counts[conv_counts >= MAX_CUSTOMER_JOURNEYS].index
        large_batches = [df[df["conversion_id"] == conv_id] for conv_id in large_conv_ids]
        df_filtered = df[~df["conversion_id"].isin(large_conv_ids)]
        batches = []
        current_batch = []
        current_count = 0
        
        for conv_id, group in df_filtered.groupby("conversion_id"):
            group_size = len(group)
            if current_count + group_size > MAX_SESSIONS:
                batches.append(pd.concat(current_batch))
                current_batch = []
                current_count = 0
            current_batch.append(group)
            current_count += group_size
        if current_batch:
            batches.append(pd.concat(current_batch))
        return large_batches + batches


    @task()
    def process_data_to_api(data_dict):

        """Processes extracted data and splits it into batches"""
        df_conversion = pd.read_json(data_dict["conversions"])
        df_session_sources = pd.read_json(data_dict["session_sources"])
        
        df_conversion['conv_timestamp'] = pd.to_datetime(df_conversion['conv_date'].astype(str) + ' ' + df_conversion['conv_time'].astype(str))
        df_session_sources['event_timestamp'] = pd.to_datetime(df_session_sources['event_date'].astype(str) + ' ' + df_session_sources['event_time'].astype(str))
        
        df_merge = df_conversion.merge(df_session_sources, on='user_id', how='left')
        
        df_filtered = df_merge[
            (df_merge['event_timestamp'] <= df_merge['conv_timestamp']) &
            (df_merge.groupby('conv_id')['conv_id'].transform('count') > 1) |
            (df_merge.groupby('conv_id')['conv_id'].transform('count') == 1)
        ]
        
        df_filtered['conversion'] = (df_filtered['event_timestamp'] == df_filtered.groupby('conv_id')['event_timestamp'].transform('max')).astype(int)
        df_filtered['timestamp'] = pd.to_datetime(df_filtered['event_timestamp'], format="%d/%m/%Y %H:%M").dt.strftime("%Y-%m-%d %H:%M:%S")

        train_journeys_df = df_filtered[['conv_id','session_id','timestamp','channel_name','holder_engagement','closer_engagement','conversion','impression_interaction']]
        train_journeys_df.rename(columns={"conv_id": "conversion_id", "channel_name": "channel_label"}, inplace=True)

        batches =  split_into_batches(train_journeys_df)
        print('Lenght batches: ', len(batches))
        print('-'*30)

        return  batches # Limit batches for testing


    def save_to_db(schema_name, table_name, data, conflict_columns):

        columns = ", ".join(f'"{col}"' for col in data.columns)
        placeholders = ", ".join(["%s" for _ in data.columns])
        conflict_clause = ", ".join(f'"{col}"' for col in conflict_columns)
        updates = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in data.columns if col not in conflict_columns])
        query = f"""
            INSERT INTO {schema_name}.{table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT({conflict_clause}) DO UPDATE SET {updates}
        """

        values = [tuple(row) for _, row in data.iterrows()]

        try:
            hook = PostgresHook(postgres_conn_id="postgres_conn")
            conn = hook.get_conn()
            cursor = conn.cursor()
            cursor.executemany(query, values)
            conn.commit()
            print(f"✅ Successfully saved {len(data)} records to {table_name}")

        except Exception as e:
            print(f"⚠️ Error saving to DB: {e}")


    @task()
    def call_api(batch):
        print('batch: ', batch.head())
        payload = batch.to_dict(orient="records")
        """Calls the IHC API and returns the response"""
        api_url = f"https://api.ihc-attribution.com/v1/compute_ihc?conv_type_id={CONV_TYPE_ID}"
        response = requests.post(
            api_url,
            json={'customer_journeys': payload},
            headers={
                'Content-Type': 'application/json',    
                'x-api-key': API_KEY
            }
        )
        results = response.json()
        if response.status_code == 200:
            print(f"Status Code: {results['statusCode']}")
            print("-"*30)
            print(results['value'])
            df_ihc =  pd.DataFrame(results['value'])
            save_to_db(schema_name, table_name_customer_journey, df_ihc, conflict_columns)
        else:
            print(f"Partial Failure Errors: {results['partialFailureErrors']}")


    @task()
    def create_channel_report(data_dict):
        hook = PostgresHook(postgres_conn_id="postgres_conn")

        df_session_sources = pd.read_json(data_dict["session_sources"])
        df_session_costs = pd.read_json(data_dict["session_costs"])
        df_conversion = pd.read_json(data_dict["conversions"])

        conv_ids = [f"'{conv_id}'" for conv_id in df_conversion['conv_id'].unique()]
        conv_ids_str = ", ".join(conv_ids)

        attribution_customer_journey = hook.get_records(f"SELECT * FROM {schema_name}.attribution_customer_journey  WHERE conversion_id in ({conv_ids_str});")
        df_attribution_customer_journey = pd.DataFrame(attribution_customer_journey, columns=["conversion_id", "session_id","initializer", "holder", "closer", "ihc"])
        
        df_sources_costs = df_session_sources.merge(df_session_costs, on='session_id', how='left')
        df_sources_costs_ifc = df_sources_costs.merge(df_attribution_customer_journey, on='session_id', how='left')
        df_sources_costs_ifc_conv = df_sources_costs_ifc.merge(df_conversion, left_on='conversion_id', right_on='conv_id', how='left')
        
        df_sources_costs_ifc_conv['cost'] = df_sources_costs_ifc_conv['cost'].fillna(0)
        df_sources_costs_ifc_conv['revenue'] = df_sources_costs_ifc_conv['revenue'].fillna(0)
        df_sources_costs_ifc_conv['ihc'] = df_sources_costs_ifc_conv['ihc'].fillna(0)
        df_sources_costs_ifc_conv.rename(columns={"event_date": "date"}, inplace=True)

        df_sources_costs_ifc_conv = df_sources_costs_ifc_conv[['session_id','channel_name','cost','date','revenue','ihc']]
        
        # Calculate ihc_revenue as ihc * revenue
        df_sources_costs_ifc_conv['ihc_revenue'] = df_sources_costs_ifc_conv['ihc'] * df_sources_costs_ifc_conv['revenue']

        # Group by channel_name and date, then aggregate the required columns
        channel_reporting = df_sources_costs_ifc_conv.groupby(['channel_name', 'date']).agg(
            cost=('cost', 'sum'),
            ihc=('ihc', 'sum'),
            ihc_revenue=('ihc_revenue', 'sum')
        ).reset_index()   

        # Add CPO (Cost per Order) and ROAS (Return on Ad Spend) to the channel_reporting DataFrame
        channel_reporting['CPO'] = channel_reporting['cost'] / channel_reporting['ihc'].replace(0, float('nan'))
        channel_reporting['ROAS'] = channel_reporting['ihc_revenue'] / channel_reporting['cost'].replace(0, float('nan'))
        save_to_db(schema_name, table_name_channel_reporting, channel_reporting, conflict_columns_channel_reporting)
    
        
    extracted_data = extract_data(START_DATE, END_DATE)
    batches = process_data_to_api(extracted_data)
    api_results = call_api.expand(batch=batches)
    api_results >> create_channel_report(extracted_data)
    
aws_challenge()