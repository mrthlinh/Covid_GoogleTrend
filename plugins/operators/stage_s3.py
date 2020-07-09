from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.decorators import apply_defaults
import requests
import json

class StateToS3Operator(BaseOperator):

    state_code = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
                'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
                'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
                'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
                'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']

    @apply_defaults
    def __init__(self,
                aws_conn_id="",
                s3_bucket="",
                 *args, **kwargs):

        super(StateToS3Operator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket



    def execute(self, context):
        # aws = AwsHook(self.aws_conn_id)
        # Create S3 connection
        s3 = S3Hook(self.aws_conn_id)

        # self.log.info(s3.list_keys(bucket_name=self.s3_bucket))


        for state in self.state_code: 
            URL = "https://covidtracking.com/api/v1/states/" + state + "/daily.json"
            self.log.info(URL)

            # Get the return request
            response = requests.get(URL)
            dict2str = [json.dumps(i,sort_keys=True) for i in response.json()]
            json_output = "\n".join(dict2str)

            key = "Test"+ "/" + state + "/" + "daily.json"

            s3.load_string(json_output,key,bucket_name=self.s3_bucket)





    # def upload_stat_state(self):
    #     """
    #         Get daily stat for each state 
    #     """
    #     s3 = self.S3Connection.s3

    #     for state in self.state_code: 
    #         URL = "https://covidtracking.com/api/v1/states/" + state + "/daily.json"
            
    #         # URL = "https://covidtracking.com/api/v1/states/" + state + "/current.json"
    #         print(URL)
    #         response = requests.get(URL)
    #         file_name = self.sub_dir + "/" + state + "/" + "daily.json"
    #         file_object = s3.Object(self.s3bucket_name, file_name)
    #         # Convert dict to string
    #         # dict2str = [str(i) for i in response.json()]
    #         dict2str = [json.dumps(i,sort_keys=True) for i in response.json()]
    #         json_output = "\n".join(dict2str)

    #         # print(json_output)
    #         file_object.put(Body=json_output)
    #         # file_object.put(Body = json.dumps(response.json()[0], indent=2))
    #         # file_object.put(Body=bytes(response.content))
        


