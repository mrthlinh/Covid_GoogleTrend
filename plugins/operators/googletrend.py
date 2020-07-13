from airflow.models import BaseOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.utils.decorators import apply_defaults
# from pytrends.request import TrendReq
import time
import pandas as pd
import random
import os
from os import path
import csv
import datetime
import sys
import time
import json
from apiclient.discovery import build


class googletrend_operator(BaseOperator):
    state_code = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
                'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
                'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
                'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
                'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']
    
    # ------ Insert your API key in the string below. -------
    # API_KEY = ''

    SERVER = 'https://www.googleapis.com'
    API_VERSION = 'v1beta'

    DISCOVERY_URL_SUFFIX = '/discovery/v1/apis/trends/' + API_VERSION + '/rest'
    DISCOVERY_URL = SERVER + DISCOVERY_URL_SUFFIX

    MAX_QUERIES = 3

    AIRFLOW_HOME = "/Users/linhtruong/airflow"

    @apply_defaults
    def __init__(self,
                gcp_conn_id = "",
                gcs_bucket="",
                keywords_path="",
                startdate = "",
                enddate="",
                *args, **kwargs):

        super(googletrend_operator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        if not path.exists("tmp"):
            os.mkdir("tmp")
        print(os.getcwd())
        # self.pytrends = TrendReq(hl='en-US', tz=360, timeout=(10,25), proxies=proxy_list, retries=2, backoff_factor=0.1)
        self.keywords = self.loadKeyword(keywords_path)
        self.startdate = startdate
        self.enddate = enddate

        # Generate schema 
        self.generateSchema(self.keywords, stagetable_flag=True)
        self.generateSchema(self.keywords, stagetable_flag=False)

        # Generate SQL to create table
        self.generateSQLquery(self.keywords, stagetable_flag=True)
        self.generateSQLquery(self.keywords, stagetable_flag=False)

        # self.timeframe = timeframe
    
    def execute(self, context):
        # pass
        gcshook = GoogleCloudStorageHook(self.gcp_conn_id)

        self.log.info(gcshook.list("testcovidlinh"))  


        for state in self.state_code:
            state_geo =  'US-' + state 
            print(state_geo) 

            query_results = self.GetQueryVolumes(queries = self.keywords,
                                start_date=self.startdate,
                                end_date=self.enddate,
                                geo=state_geo,
                                geo_level='region',
                                frequency='day')

            # Example of writing one of these files out as a CSV file to STDOUT.
            filename = "tmp/"+state_geo+".csv"
            with open(filename,"w") as csvfile:
                outwriter = csv.writer(csvfile)
    
                for row in query_results:
                    outwriter.writerow(row)

            # Upload file to GCS
            object_name = state_geo + "/" + self.startdate + "_" +self.enddate+ ".csv"

            gcshook.upload(bucket=self.gcs_bucket, object=object_name, filename=filename)
                

        
    def loadKeyword(self, gcs_filepath):
        """
            Read a file from S3 and get list of keywords
        """
        gcshook = GoogleCloudStorageHook(self.gcp_conn_id)
        file_path = "tmp/" + gcs_filepath
        gcshook.download(self.gcs_bucket, object = gcs_filepath, filename = file_path)
        keyword = []
        with open(file_path,"r") as file:
            for line in file:
                word = line.split(",")
                keyword.append(word[-1].strip())
        return keyword[1:]

    def generateSQLquery(self,keyword, stagetable_flag=True):
        """
            Generate SQL file for creating table to redshift
        """

        columns = ""
        file_path = self.AIRFLOW_HOME+"/tmp/create_googletrend.sql" 
        data_type = 'STRING'
        if stagetable_flag:
            data_type = 'STRING'
            file_path = self.AIRFLOW_HOME+"/tmp/create_googletrend_stage.sql" 

        for word in keyword:
            columns += "\t`" + word.replace(" ","_") + "`" + data_type + ",\n"

        query = "CREATE TABLE GoogleTrend( \n" \
                "\t`date` STRING,\n" \
                "\t`state` STRING,\n" \
                "{}" \
                ")".format(columns)
        
        if stagetable_flag:
            query = "CREATE TABLE GoogleTrend( \n" \
                    "\t`date` STRING,\n" \
                    "\t`state` STRING,\n" \
                    "{}" \
                    ")".format(columns)  

        # print(query)
        
        with open(file_path,"w") as f:
            f.write(query)



    def generateSchema(self, keyword, stagetable_flag=True):
        """
            Generate schema for bigquery
        """

        schema_json = [{"name":"date","type":"STRING"},{"name":"state","type":"STRING"}]
        data_type = 'STRING'
        file_path = self.AIRFLOW_HOME+"/tmp/googletrend_schema.json" 
        if stagetable_flag:
            schema_json = [{"name":"date","type":"STRING"},{"name":"state","type":"STRING"}]
            data_type = 'STRING'
            file_path = self.AIRFLOW_HOME+"/tmp/googletrend_schema_stage.json" 

        d = {}
        print(keyword)
        for word in keyword:
            d["name"] = word.replace(" ","_") 
            d["type"] = data_type
            schema_json.append(d)
            d = {}
        
        with open(file_path,"w") as f:
            json.dump(schema_json, f,indent=4)
        
        # Upload schema to GCS
        object_name = "googletrend_schema.json"
        gcshook = GoogleCloudStorageHook(self.gcp_conn_id)
        gcshook.upload(bucket=self.gcs_bucket, object=object_name, filename=file_path)
    
    def DateToISOString(self,datestring):
        """Convert date from (eg) 'Jul 04 2004' to '2004-07-11'.
        Args:
            datestring: A date in the format 'Jul 11 2004', 'Jul 2004', or '2004'
        Returns:
            The same date in the format '2004-11-04'
        Raises:
            ValueError: when date doesn't match one of the three expected formats.
        """

        try:
            new_date = datetime.datetime.strptime(datestring, '%b %d %Y')
        except ValueError:
            try:
                new_date = datetime.datetime.strptime(datestring, '%b %Y')
            except ValueError:
                try:
                    new_date = datetime.datetime.strptime(datestring, '%Y')
                except:
                    raise ValueError("Date doesn't match any of '%b %d %Y', '%b %Y', '%Y'.")

        return new_date.strftime('%Y-%m-%d')

    def GetQueryVolumes(self,queries, start_date, end_date,
                        geo='US', geo_level='country', frequency='week'):

        """Extract query volumes from Flu Trends API.
        Args:
            queries: A list of all queries to use.
            start_date: Start date for timelines, in form YYYY-MM-DD.
            end_date: End date for timelines, in form YYYY-MM-DD.
            geo: The code for the geography of interest which can be either country
                (eg "US"), region (eg "US-NY") or DMA (eg "501").
            geo_level: The granularity for the geo limitation. Can be "country",
                    "region", or "dma"
            frequency: The time resolution at which to pull queries. One of "day",
                    "week", "month", "year".

        Returns:
            A list of lists (one row per date) that can be output by csv.writer.

        Raises:
            ValueError: when geo_level is not one of "country", "region" or "dma".
        """

        if not self.API_KEY:
            raise ValueError('API_KEY not set.')

        service = build('trends', self.API_VERSION,
                        developerKey= self.API_KEY,
                        discoveryServiceUrl= self.DISCOVERY_URL)

        dat = {}

        # Note that the API only allows querying 30 queries in one request. In
        # the event that we want to use more queries than that, we need to break
        # our request up into batches of 30.
        batch_intervals = range(0, len(queries), self.MAX_QUERIES)

        for batch_start in batch_intervals:
            batch_end = min(batch_start + self.MAX_QUERIES, len(queries))
            query_batch = queries[batch_start:batch_end]
            # Make API query
            if geo_level == 'country':
            # Country format is ISO-3166-2 (2-letters), e.g. 'US'
                req = service.getTimelinesForHealth(terms=query_batch,
                                                    time_startDate=start_date,
                                                    time_endDate=end_date,
                                                    timelineResolution=frequency,
                                                    geoRestriction_country=geo)


            elif geo_level == 'dma':
                # See https://support.google.com/richmedia/answer/2745487
                req = service.getTimelinesForHealth(terms=query_batch,
                                                    time_startDate=start_date,
                                                    time_endDate=end_date,
                                                    timelineResolution=frequency,
                                                    geoRestriction_dma=geo)

            elif geo_level == 'region':
                # Region format is ISO-3166-2 (4-letters), e.g. 'US-NY' (see more examples
                # here: en.wikipedia.org/wiki/ISO_3166-2:US)
                req = service.getTimelinesForHealth(terms=query_batch,
                                                    time_startDate=start_date,
                                                    time_endDate=end_date,
                                                    timelineResolution=frequency,
                                                    geoRestriction_region=geo)
            else:
                raise ValueError("geo_type must be one of 'country', 'region' or 'dma'")

            res = req.execute()

            # Sleep for 1 second so as to avoid hittting rate limiting.
            time.sleep(1)

            # Convert the data from the API into a dictionary of the form
            # {(query, date): count, ...}
            res_dict = {(line[u'term'], self.DateToISOString(point[u'date'])):
                        point[u'value']
                        for line in res[u'lines']
                        for point in line[u'points']}

            # Update the global results dictionary with this batch's results.
            dat.update(res_dict)

        state = geo.split("-")[-1]
        # Make the list of lists that will be the output of the function
        res = [['date'] + ['state'] + queries]
        for date in sorted(list(set([x[1] for x in dat]))):
            vals = [dat.get((term, date), 0) for term in queries]
            res.append([date] + [state] + vals)

        return res

