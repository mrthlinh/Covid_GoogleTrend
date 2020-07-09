import datetime
import logging
import json
from airflow import DAG
from airflow.operators.myplugin import stage_gcs_operator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.models import Variable

# --------------------------------------------------------------------------------
# Set variables
# --------------------------------------------------------------------------------

with open('/Users/linhtruong/airflow/tmp/covidStatSchema.json') as f:
    covidstat_schema = json.load(f)

state_code = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
                'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
                'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
                'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
                'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']


source_objects = ['US-'+state+'/covidstat.json' for state in state_code]

gcp_conn_id="gcp_credentials"
destination_project_dataset_table   = "Covid.CovidStat_new"
# --------------------------------------------------------------------------------
# Main DAG
# --------------------------------------------------------------------------------

dag = DAG(
    'DAG_CovidStat',
    start_date=datetime.datetime.now())

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_to_gcp = stage_gcs_operator(
    task_id         = 'Stage_to_GCS',
    provide_context = False,
    dag             = dag,
    gcp_conn_id     = gcp_conn_id,
    gcs_bucket      = "testcovidlinh",
    xcom_task_id_key = "covidstat_filelist"
)

gcs_to_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id         = 'GCS_to_BigQuery',
    dag             = dag,
    bucket          = 'testcovidlinh',
    source_objects  = source_objects,
    # schema_object   = "/tmp/covidStatSchema.json",
    schema_fields   = covidstat_schema,
    source_format   = 'NEWLINE_DELIMITED_JSON',
    destination_project_dataset_table   = destination_project_dataset_table,
    write_disposition   = 'WRITE_TRUNCATE',
    # autodetect = False,
    bigquery_conn_id    = gcp_conn_id,
    google_cloud_storage_conn_id    = gcp_conn_id
)

check_count = BigQueryCheckOperator(
    task_id             = "check_count",
    bigquery_conn_id    = gcp_conn_id,
    sql                 = "SELECT COUNT(*) FROM "+destination_project_dataset_table,
    use_legacy_sql      = False
    # location=location
)

start_operator >> stage_to_gcp >> gcs_to_bigquery >> check_count