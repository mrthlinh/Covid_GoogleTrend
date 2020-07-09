# https://airflow.readthedocs.io/en/latest/howto/operator/gcp/gcs_to_gcs.html
# https://medium.com/bakdata/data-warehousing-made-easy-with-google-bigquery-and-apache-airflow-bf62e6c727ed
# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/composer/workflows/bq_copy_across_locations.py

from airflow.models import BaseOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.utils.decorators import apply_defaults
import requests
import json
import os
from os import path

class stage_gcs_operator(BaseOperator):
    
    state_code = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
                'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
                'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
                'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
                'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']

    @apply_defaults
    def __init__(self,
                gcp_conn_id = "",
                gcs_bucket="",
                xcom_task_id_key="",
                *args, **kwargs):

        super(stage_gcs_operator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.xcom_task_id_key = xcom_task_id_key

    def execute(self, context):
        gcshook = GoogleCloudStorageHook(self.gcp_conn_id)
        self.log.info(gcshook.list("testcovidlinh"))     

        # Create a temporary folder
        # print(os.path.)
        if not path.exists("tmp"):
            os.mkdir("tmp")
        
        # Track failure 
        failure_count = 0

        # Passing filename to next job
        file_list = []

        # Consume API
        for state in self.state_code: 
            URL = "https://covidtracking.com/api/v1/states/" + state.lower() + "/daily.json"
            # self.log.info(URL)
            response = requests.get(URL).json()
            
            try:
                # If we have any message error
                self.log.info(response["message"])
                failure_count += 1
                continue
            except:
                # The response is successfully
                filename = "tmp/"+state+".json"
                # self.log.info(filename)
                with open(filename,'w', encoding='utf-8') as f:
                    dict2str = [json.dumps(i,sort_keys=True) for i in response]
                    json_output = "\n".join(dict2str)
                    f.write(json_output)
                    # json.dump(response, f, ensure_ascii=False)
                
                object_name = 'US-' + state + "/" + "covidstat.json"
                file_list.append(object_name)
                gcshook.upload(bucket=self.gcs_bucket, object=object_name, filename=filename)
        
        self.log.info("Number of failure cases: "+str(failure_count))

        task_instance = context['task_instance']
        task_instance.xcom_push(self.xcom_task_id_key, file_list)
