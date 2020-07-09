# from airflow.models import BaseOperator

# from airflow.contrib.hooks.bigquery_hook import BigQueryHook

# from airflow.contrib.hooks.bigquery_hook import BigQueryBaseCursor
# # from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


# from airflow.utils.decorators import apply_defaults
# import requests
# import json
# import os
# from os import path


# class transformBigQuery_operator(BaseOperator):

#     @apply_defaults
#     def __init__(self,
#                 gcp_conn_id = "",
#                 gcs_bucket="",
#                 schema_filepath="",
#                 source_format = "",
#                 destination_project_dataset_table="",
#                 *args, **kwargs):

#         pass
