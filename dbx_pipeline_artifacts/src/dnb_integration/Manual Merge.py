# Databricks notebook source
dbutils.widgets.text("entity_name", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("id_value", "", "primary key value")
dbutils.widgets.text("dnb_duns_value", "", "duns value")

# COMMAND ----------

entity_name=dbutils.widgets.get("entity_name")
id_value=dbutils.widgets.get("id_value")
catalog_name=dbutils.widgets.get("catalog_name")
dnb_duns_value=dbutils.widgets.get("dnb_duns_value")
master_table=f"{catalog_name}.gold.{entity_name}_master_prod"
master_dnb_table=f"{catalog_name}.gold.{entity_name}_master_prod_dnb_duns"

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

logger.info(entity_name)
logger.info(id_value)
logger.info(dnb_duns_value)
logger.info(master_dnb_table)

# COMMAND ----------

spark.sql(f"""
    UPDATE {master_dnb_table}
    SET merge_status = CASE 
        WHEN dnb_duns = '{dnb_duns_value}' THEN 'MANUAL_MERGE' 
        ELSE 'NOT_APPLICABLE' 
    END 
    WHERE lakefusion_id = '{id_value}'
""")

# COMMAND ----------

input_df=spark.sql(f"""select * from {master_dnb_table} where dnb_duns='{dnb_duns_value}'""")

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

import requests
import json
import base64
import time
import urllib
from datetime import date
from pyspark.sql.types import *

# COMMAND ----------

API_KEY=''
API_SECRET=''
MIN_CONFIDENCE = 7
THRESHOLD=9
CANDIDATEMAXIMUMQUANTITY = 3

# COMMAND ----------

class Db_api:
    DEFAULT_MIN_CONFIDENCE = 8
    threshold=9

    def __init__(self, key, secret):
        self.key = key
        self.secret = secret
        self.api_auth_url = "https://plus.dnb.com/v2/token"
        self.api_url = "https://plus.dnb.com/v1"
        self.auth_token = self.__define_auth_token()
        self.matched_records = None
        try:
            self.session_token = self.__auth_proc()['access_token']
            logger.info('Succesfully authenticated to the D&B API.')
        except Exception as error:
            logger.error('Authentication Error:\nWe could not authenticate to the D&B API. Please check your credentials.')

    schema =StructType(
    [StructField('update_date', DateType(), True),
     StructField('accepted', BooleanType(), True),
     StructField('input_source_id', StringType(), True), 
     StructField('input_name', StringType(), True), 
     StructField('input_streetaddress', StringType(), True),
     StructField('input_city', StringType(), True), 
     StructField('input_country', StringType(), True), 
     StructField('input_state', StringType(), True), 
     StructField('input_postal_code', StringType(), True), 
     StructField('dnb_duns', StringType(), True), 
     StructField('dnb_name', StringType(), True), 
     StructField('dnb_nameMatchScore', DoubleType(), True), 
     StructField('dnb_status', StringType(), True), 
     StructField('dnb_streetaddress', StringType(), True),
     StructField('dnb_city', StringType(), True), 
     StructField('dnb_country', StringType(), True), 
     StructField('dnb_state', StringType(), True), 
     StructField('dnb_postal_code', StringType(), True), 
     StructField('dnb_confidenceCode', LongType(), True), 
     StructField('dnb_matchDataProfileComponents', ArrayType(MapType(StringType(), StringType(), True), True), True), 
     StructField('dnb_matchgradeComponents', ArrayType(MapType(StringType(), StringType(), True), True), True)
     ])
    
    
    def __define_auth_token(self):
        """
        This function will generate the authentication token for the API This is simple creating an base64 encoded string from the API key and secret
        """
        key_sec = f"{self.key}:{self.secret}"
        encoded_creds = base64.b64encode(str(key_sec).encode("ascii")).decode(
            "ascii"
        )  # Base 64 activation
        return encoded_creds

    def __auth_proc(self):
        """
        This function will create the session token for all api calls calls
        """
        payload = {"grant_type": "client_credentials"}
        headers = {
            "Authorization": f"Basic {self.auth_token}",
        }
        response = requests.request(
            "POST", self.api_auth_url, headers=headers, json=payload
        )
        # print(response.json())
        return response.json()

    def match_d_and_b(self, record):
        """
        This is the primary API call for each company in the input dataframe. This will package returns a JSON object with the match results
        For complete Match API details, please refer to our documentation website, https://directplus.documentation.dnb.com/openAPI.html?apiID=IDRCleanseMatch
        """
        endpoint = f"{self.api_url}/match/cleanseMatch?"
        params = {
            "candidateMaximumQuantity": candidatemaximumquantity,
            "name": record["name"],
            "streetAddressLine1": record["streetaddress"],
            "street&addressLocality": record["city"],
            "addressRegion": record["state"],
            "postalCode": record["postal_code"],
            "countryISOAlpha2Code": record["country"],
        }
        

        payload = {}
        headers = {
            "Authorization": f"Bearer {self.session_token}",
        }
        params = urllib.parse.urlencode(params)
        url2 = endpoint + params

        response = requests.request("GET", url2, headers=headers, data=payload)
        json_resp = response.json()
        return json_resp

    def match_and_cleanse(self,input_df,minimum_confidence=DEFAULT_MIN_CONFIDENCE):
        """
        This method updates two object properties with dataframes. The first is a list of all the records that have a match confidence score of defined by the user (default is 7) or higher. The second is a list of all the records that have a match confidence score lower than the user defined score.
        """
        # check input minimum confidence value
        if minimum_confidence > 10:
            raise Exception(
                "The minimum confidence needs to be a number between 1 and 10"
            )

        matched_records_list = []
        data_collect = input_df.collect()
        for row in data_collect:
            results = self.match_d_and_b(row)
            input_data = results["inquiryDetail"]
            for match in results["matchCandidates"]:
                t = match["organization"]
                # set the accepted value
                if (
                    match["matchQualityInformation"]["confidenceCode"]
                    >= minimum_confidence
                ):
                    accepted = True
                else:
                    accepted = False
                
                # create the dictionary of results
                matched_cos = {
                    "update_date": date.today(),
                    "input_source_id": row["source_id"],
                    "input_name": row["name"],
                    "input_streetaddress": row["streetaddress"],
                    "input_city": row["city"],
                    "input_country": row["country"],
                    "input_postal_code": row["postal_code"],
                    "input_state":row["state"],
                    "dnb_duns": t["duns"],
                    "dnb_name": t["primaryName"],
                    "dnb_streetaddress": t["primaryAddress"]["streetAddress"].get(
                        "line1", None
                    ),
                    "dnb_city": t["primaryAddress"]["addressLocality"].get(
                        "name", None
                    ),
                    "dnb_country": t["primaryAddress"]["addressCountry"].get(
                        "isoAlpha2Code", None
                    ),
                     "dnb_state": t["primaryAddress"]["addressRegion"].get(
                        "abbreviatedName", None
                    ),
                    "dnb_postal_code": t["primaryAddress"]["postalCode"],
                    "dnb_status": t["dunsControlStatus"]["operatingStatus"]["description"],
                    "dnb_confidenceCode": match["matchQualityInformation"][
                        "confidenceCode"
                    ],
                    "accepted": accepted,
                    "dnb_nameMatchScore": match["matchQualityInformation"][
                        "nameMatchScore"
                    ],
                    "dnb_matchgradeComponents": match["matchQualityInformation"][
                        "matchGradeComponents"
                    ],
                    "dnb_matchDataProfileComponents": match["matchQualityInformation"][
                        "matchDataProfileComponents"
                    ],
                }
                matched_records_list.append(matched_cos)
        self.matched_records = spark.createDataFrame(matched_records_list, schema=self.schema)
        #self.matched_records = spark.createDataFrame(matched_records_list)

        logger.info(
            f"All records have been processed.\nThe result set is {len(matched_records_list)} records which can be accessed using the matched_records object property"
        )
        return


    def export_monitoring_registrations(self,registration_id):
        """
        This method will add the input dataframe to the monitoring endpoint. The registration_id is the unique identifier for the monitoring endpoint
        """
        endpoint = f"{self.api_url}/monitoring/registrations/{registration_id}/subjects"
        payload = {}
        headers = {
                "Authorization": f"Bearer {self.session_token}",
            }
        response = requests.request("GET", endpoint, headers=headers)
        json_resp = response.json()
      
        return 
    

    def add_to_monitoring(self, input_df,registration_id):
        """
        This method will add the input dataframe to the monitoring endpoint. The registration_id is the unique identifier for the monitoring endpoint
        """
        already_registered_count = 0
        successfully_added_count = 0
        for row in input_df.collect():
            duns = row["dnb_duns"]
            endpoint = f"{self.api_url}/monitoring/registrations/{registration_id}/duns/{duns}"
            payload = {}
            headers = {
                "Authorization": f"Bearer {self.session_token}",
            }
            url2 = endpoint

            response = requests.request("POST", url2, headers=headers)
            json_resp = response.json()
            # Check for 'error' key and the specific error message
            if 'error' in json_resp and json_resp['error'].get('errorCode') == '21012':
                already_registered_count += 1
            elif 'information' in json_resp and json_resp['information'].get('code') == '21113':
                successfully_added_count += 1
        message = (f"Processed {already_registered_count + successfully_added_count} records.\n"
            f"{already_registered_count} were already registered.\n"
            f"{successfully_added_count} were successfully added to monitoring.")
    
        logger.info(message)
        return 
    
    
    def delete_from_monitoring(self, input_df,registration_id):
        """
        This method will delete the input dataframe to the monitoring endpoint. The registration_id is the unique identifier for the monitoring endpoint
        """
        for row in input_df.collect():
            duns = row["dnb_duns"]
            endpoint = f"{self.api_url}/monitoring/registrations/{registration_id}/duns/{duns}"
            payload = {}
            headers = {
                "Authorization": f"Bearer {self.session_token}",
            }
            url2 = endpoint

            response = requests.request("DELETE", url2, headers=headers)
            json_resp = response.json()
            
        return 
    
    

# COMMAND ----------

z = Db_api(API_KEY, API_SECRET)

# COMMAND ----------

z.add_to_monitoring(input_df,registration_id)
