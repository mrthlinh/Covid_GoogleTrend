from airflow.models import BaseOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.utils.decorators import apply_defaults
import requests
import json
import os
from os import path

# https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
class loadBigQuery_operator(BaseOperator):

    state_code = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
                'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
                'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
                'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
                'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']

    @apply_defaults
    def __init__(self,
                gcp_conn_id = "",
                gcs_bucket="",
                schema_filepath="",
                source_format = "",
                destination_project_dataset_table="",
                *args, **kwargs):
        
        # super(GoogleCloudStorageToBigQueryOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.schema_filepath = schema_filepath
        self.source_format = source_format
        self.destination_project_dataset_table = destination_project_dataset_table

    def execute(self, context):
        gcshook = GoogleCloudStorageHook(self.gcp_conn_id)
        self.log.info(gcshook.list("testcovidlinh"))   


        # gcs_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        #     task_id = 'GCS_to_BigQuery',
        #     dag     = dag,
        #     bucket          = 'testcovidlinh',
        #     source_objects  = ['US-AK/test.json'],
        #     schema_object   = "/tmp/covidStatSchema.json",
        #     # schema_fields= schema,
        #     source_format   = 'NEWLINE_DELIMITED_JSON',
        #     destination_project_dataset_table= "Covid.CovidStat",
        #     write_disposition='WRITE_TRUNCATE',
        #     # autodetect = False,
        #     bigquery_conn_id='gcp_credentials',
        #     google_cloud_storage_conn_id='gcp_credentials'
        # )
