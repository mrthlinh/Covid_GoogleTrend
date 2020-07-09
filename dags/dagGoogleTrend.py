import datetime
import logging
import json

from airflow import DAG
from airflow.operators.myplugin import googletrend_operator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator

# --------------------------------------------------------------------------------
# Set variables
# --------------------------------------------------------------------------------

# 'table_list_file_path': This variable will contain the location of the master
# file.

# with open('/Users/linhtruong/airflow/tmp/googletrend_schema.json') as f:
#     googletrend_schema = json.load(f)

state_code = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
                'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
                'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
                'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
                'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']


startdate= '2019-06-30'
enddate= '2020-06-30'
keywords_path = "Covid19_keyword_test.csv"
gcp_conn_id = "gcp_credentials"
source_objects = ['US-' +state+ '/' +startdate+ '_' +enddate+ '.csv' for state in state_code]

destination_project_dataset_table   = "Covid.GoogleTrend_test"

# --------------------------------------------------------------------------------
# Main DAG
# --------------------------------------------------------------------------------
dag = DAG(
    'DAG_GoogleTrend',
    start_date=datetime.datetime.now())

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Loading data to Google Cloud Storage
googletrend = googletrend_operator(
    task_id         = 'Stage_to_GCS',
    provide_context = False,
    dag             = dag,
    gcp_conn_id     = gcp_conn_id,
    gcs_bucket      = "testcovidlinh",
    keywords_path   = keywords_path, 
    startdate       = startdate,
    enddate         = enddate
)

# Create table
# CreateTable = BigQueryCreateEmptyTableOperator(
#     task_id='BigQueryCreateEmptyTableOperator_task',
#     dataset_id='ODS',
#     table_id='Employees',
#     project_id='internal-gcp-project',
#     gcs_schema_object='gs://schema-bucket/employee_schema.json',
#     bigquery_conn_id='airflow-service-account',
#     google_cloud_storage_conn_id='airflow-service-account'
# )

CreateTable = BigQueryCreateEmptyTableOperator(
    task_id='BigQueryCreateEmptyTableOperator_task',
    dataset_id='Covid',
    table_id='GoogleTrend_test',
    project_id='covidproject-278521',
    gcs_schema_object = 'gs://testcovidlinh/googletrend_schema.json',
    # schema_fields=googletrend_schema,
    bigquery_conn_id=gcp_conn_id,
    google_cloud_storage_conn_id=gcp_conn_id
)


# Loading data from GCS to BigQuery
gcs_to_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id         = 'GCS_to_BigQuery',
    dag             = dag,
    bucket          = 'testcovidlinh',
    source_objects  = source_objects,
    # schema_object   = "/tmp/covidStatSchema.json",
    schema_object = 'googletrend_schema.json',
    # schema_fields   = googletrend_schema,
    source_format   = 'CSV',
    destination_project_dataset_table   = destination_project_dataset_table,
    write_disposition                   = 'WRITE_TRUNCATE',
    # autodetect = False,
    bigquery_conn_id                    = gcp_conn_id,
    google_cloud_storage_conn_id        = gcp_conn_id
)

# Check Count
check_count = BigQueryCheckOperator(
    task_id             = "check_count",
    bigquery_conn_id    = gcp_conn_id,
    sql                 = "SELECT COUNT(*) FROM "+destination_project_dataset_table,
    use_legacy_sql      = False
    # location=location
)

start_operator >> googletrend >> CreateTable >> gcs_to_bigquery >> check_count