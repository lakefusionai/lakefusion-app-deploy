# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://www.dnb.com/content/experience-fragments/dnb-marketing-website/us/en/site/header/master/_jcr_content/root/headercontainer/_cq_logo.coreimg.svg/1732744437450/logo-dnb-wordmark.svg"
# MAGIC         alt="Picture"
# MAGIC         width="800"
# MAGIC         height="600"
# MAGIC         style="display: block; margin: 15 auto" />
# MAGIC
# MAGIC
# MAGIC #### Company Match and Monitoring
# MAGIC In this notebook, we will walk you through integrating the Dun & Bradstreet (D&B) Company Match API with Databricks using Python. The API allows users to match company data by passing in company-level information. The API can then return enriched company data in the form of a DataFrame, which can be stored and used within Databricks for further analysis, reporting, or as a company identifier master for other data processes. For more information about the D&B Matching API, please refer to the [Matching API Guide](https://www.dnb.com/content/dam/english/dnb-data-insight/DNB_The_Basics_On_Data_Matching.pdf).
# MAGIC
# MAGIC After the user has all of their companies enriched with the DUNS number, we will then pass these records to the D&B monitoring service which when set up to the Databricks Delta Sharing process will allow the user to stay informed about changes to status of their business partners, and maintain up-to-date, accurate company information.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Install and import required libraries
# MAGIC In the next two commands, we will install the [requests](https://requests.readthedocs.io/en/latest/) library which is required to run the D&B Match API command and import all of the necessary libraries to to execute the notebook

# COMMAND ----------

# DBTITLE 1,Install requests
# MAGIC %pip install requests

# COMMAND ----------

# DBTITLE 1,remove for production
# %pip install pycountry

# COMMAND ----------

# DBTITLE 1,Imports
import requests
import json
import base64
import time
import urllib
from datetime import date
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC In the command below, input your API Key and Secret. To enhance security, it's advisable to utilize a tool like [Databricks Secret Manager](https://www.databricks.com/blog/2018/06/04/securely-managing-credentials-in-databricks.html) to securely store and manage these sensitive credentials.
# MAGIC
# MAGIC **Please make sure to never share this notebook with your credentials intact.**

# COMMAND ----------

dbutils.widgets.text("entity_dnb_settings", "", "Attributes to include in dnb")
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text("catalog_name", "", "Catalog Name")

# COMMAND ----------

entity_dnb_settings=dbutils.widgets.get("entity_dnb_settings")
entity=dbutils.widgets.get("entity")
catalog_name=dbutils.widgets.get("catalog_name")

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

logger.info(entity)

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
entity_dnb_settings = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON","entity_dnb_settings", debugValue=entity_dnb_settings)
logger.info(entity)

# COMMAND ----------

entity_dnb_settings=json.loads(entity_dnb_settings)

# COMMAND ----------

attributesmapping=entity_dnb_settings.get("attributesmapping")
threshold = entity_dnb_settings.get("threshold")
minimumconfidence=entity_dnb_settings.get("minimumconfidence")
candidatemaximumquantity=entity_dnb_settings.get("candidatemaximumquantity")
master_table=f"{catalog_name}.gold.{entity}_master_prod"
dnb_duns_table=f"{master_table}_dnb_duns"
source_id_column=attributesmapping.get("source_id") or "lakefusion_id"
id_key="lakefusion_id"

# COMMAND ----------

# DBTITLE 1,Load master records and filter to unprocessed only
from pyspark.sql import functions as F
from pyspark.sql.types import StringType as T

df_master_full=spark.read.table(master_table)

# Check if _dnb_duns table already exists
dnb_table_exists = spark.catalog.tableExists(dnb_duns_table)

if dnb_table_exists:
    df_existing = spark.read.table(dnb_duns_table)
    # Skip lakefusion_ids that have at least one active DUNS candidate (ACCEPTED, AUTO_MERGED, MANUAL_MERGE, or IN_REVIEW)
    active_ids = df_existing.filter(
        F.col("merge_status").isin("ACCEPTED", "AUTO_MERGED", "MANUAL_MERGE", "IN_REVIEW")
    ).select(id_key).distinct()
    df_master_new = df_master_full.join(
        active_ids,
        on=id_key,
        how="left_anti"
    )
    #new_count = df_master_new.count()
    #total_count = df_master_full.count()
    #print(f"Found {new_count} new records out of {total_count} total master records to process")
else:
    df_master_new = df_master_full
    logger.info(f"Table {dnb_duns_table} does not exist — processing all {df_master_new.count()} master records")

# COMMAND ----------

# DBTITLE 1,Map entity attributes to D&B field names
updated_data = {k: (None if v == "__none__" else v) for k, v in attributesmapping.items()}

# Build column expressions
columns_expr = [
    F.col(v).alias(k) if v is not None else F.lit("US").alias(k)
    for k, v in updated_data.items()
]

# Ensure source_id column is present for D&B result tracking
if "source_id" not in updated_data:
    columns_expr.append(F.col(source_id_column).cast("string").alias("source_id"))

# Always include lakefusion_id for incremental tracking
columns_expr.append(F.col(id_key).cast("string").alias(id_key))

# Apply to DataFrame — use only new/unprocessed records
df_master = df_master_new.select(*columns_expr)

# COMMAND ----------

# DBTITLE 1,remove for production
# # Convert country names/alpha3 codes to ISO Alpha-2 for D&B API
# import pycountry
# from pyspark.sql.functions import create_map, lit, lower, trim, coalesce

# # Auto-generate mappings for ALL countries
# country_map = {}

# for c in pycountry.countries:
#     # Full name → Alpha-2
#     country_map[c.name.lower()] = c.alpha_2
#     # Alpha-3 → Alpha-2
#     country_map[c.alpha_3.lower()] = c.alpha_2
#     # Common name if available (e.g., "Bolivia" instead of "Bolivia, Plurinational State of")
#     if hasattr(c, 'common_name'):
#         country_map[c.common_name.lower()] = c.alpha_2

# # Custom mappings for non-standard names in your data
# custom_mappings = {
#     "korea, south": "KR",
#     "russia": "RU",
#     "hong kong sar, prc": "HK",
#     "macau sar, prc": "MO",
#     "taiwan, china": "TW",
#     "cote d'ivoire": "CI",
#     "vatican city state (holy see)": "VA",
#     "palestine, occupied territories as palestine, state of": "PS",
#     "virgin islands, u.s.": "VI",
#     "virgin islands, british": "VG",
#     "north macedonia": "MK",
#     "cape verde": "CV",
#     "brunei": "BN",
#     "myanmar": "MM",
#     "kosovo": "XK",
#     "isle of man": "IM",
#     "reunion": "RE",
#     "czech republic": "CZ",
#     "turkey": "TR"
# }
# country_map.update(custom_mappings)

# mapping_expr = create_map([lit(x) for pair in country_map.items() for x in pair])
# df_master = df_master.withColumn(
#     "country",
#     coalesce(mapping_expr[lower(trim(F.col("country")))], F.col("country"))
# )

# COMMAND ----------

# DBTITLE 1,Load D&B API credentials from Databricks secrets
API_KEY = dbutils.secrets.get(scope="lakefusion", key="dnb_consumer_key")
API_SECRET = dbutils.secrets.get(scope="lakefusion", key="dnb_consumer_secret")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the D&B Class object
# MAGIC In the following commands, we will:
# MAGIC
# MAGIC - Create a D&B Class object to manage the connection to the D&B API service. This object will also allow us to process the dataframe we pass to it. The main function we’ll use in this example is `match_and_cleanse`. This function will take all records from the input dataframe, submit them to the matching API, and return the matches along with their corresponding DUNS number, standardized addresses, and match scores.
# MAGIC
# MAGIC To initialize the D&B API class, you will need to provide the following:
# MAGIC
# MAGIC - **API Key** (Provided by D&B)
# MAGIC - **API Secret** (Provided by D&B)
# MAGIC - **A DataFrame** containing the necessary input data
# MAGIC
# MAGIC
# MAGIC ###Required Inputs
# MAGIC The input dataframe that will be used by the D&B Matching service nust contain the columns below. If any of these fields are missing from your input table, include them in your query with a default value of `null`. The more details you provide for a record, the higher the likelihood that the match engine will identify the correct match. Missing fields can reduce confidence, which may result in the correct record being found but failing to meet the D&B quality threshold—causing the match to be rejected.
# MAGIC
# MAGIC for more details about Matching, please refer to our documentation website or contact your D&B Representative, https://directplus.documentation.dnb.com/html/guides/Identify/IdentityResolution.html
# MAGIC
# MAGIC
# MAGIC | Field | Notes |
# MAGIC | -------- | ------- |
# MAGIC | source_id | Your primary key |
# MAGIC | name | Company Name |
# MAGIC | streetaddress | The street address of the company |
# MAGIC | city | City of the property |
# MAGIC | state | State or province |
# MAGIC | postal_code | Postal code|
# MAGIC | country | [Two character ISO country code](https://www.iban.com/country-codes) |
# MAGIC
# MAGIC **The dataframe must include these field names to comply with the required API input format. You can alter the inputs to your specific needs, however, you will then need to make the appropriate changes to the 'params' below in the 'match_d_and_b object below'**

# COMMAND ----------

# DBTITLE 1,Class Definition
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
    [StructField('lakefusion_id', StringType(), True),
     StructField('update_date', DateType(), True),
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
        logger.info(url2)
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
            logger.info(results)
            input_data = results["inquiryDetail"]
            match_candidates = results.get("matchCandidates")
            if match_candidates:
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
            else:
                logger.warning(f"No matchCandidates found for: {row}")
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

# MAGIC %md
# MAGIC #### Fetch the Required Data from the Source Table
# MAGIC In the next command, we will query the source table containing the company-level information. The query results will be returned in the `_sqldf` DataFrame, which holds the data fetched from the SQL query. This DataFrame will then be used in the API call to generate the desired results.
# MAGIC
# MAGIC _Note that you can use any DataFrame for the API call, as long as the input variables match the expected format._

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process your company level information
# MAGIC In the command below, we will process the dataframe passed to the D&B API object. The Match API will be used to find relevant matches, and the resulting dataframe will be stored in the `matched_records` property. You can optionally specify a minimum confidence level by passing the `minimal_confidence` argument to the function. **The default confidence level is commented out and must be adjusted to meet your use case. By doing so, user accepts all risk associated.** 
# MAGIC
# MAGIC In the results, any record with a confidence level equal to or above the specified threshold will have a value of `true` in the **accepted** column, while records below the threshold will be marked as `false`.

# COMMAND ----------

# DBTITLE 1,Limit records for demo (remove for production)
# df_master = df_master.limit(5)

# COMMAND ----------

# DBTITLE 1,Skip if no new records to process
if df_master.count() == 0:
    logger.info("No new records to process — skipping D&B matching")
    dbutils.notebook.exit("No new records to process")

# COMMAND ----------

# DBTITLE 1,Preview new records
df_master.show(50, truncate=False)

# COMMAND ----------

# DBTITLE 1,Authenticate to D&B API
z = Db_api(API_KEY, API_SECRET)

# Capture config as plain Python values for closure (serverless-compatible)
_session_token = z.session_token
_candidate_max_qty = candidatemaximumquantity
_min_confidence = minimumconfidence

# COMMAND ----------

# DBTITLE 1,Run throttled D&B CleanseMatch via mapInPandas (5 TPS limit)
import pandas as pd

def match_partition(pdf_iterator):
    """Process each partition against D&B CleanseMatch API.
    Throttled to ~2 TPS per partition (2 partitions = ~4 TPS total, under 5 TPS D&B limit).

    REFACTORED: Quota exhaustion (error code 00050) and Auth failure (HTTP 401) are now
    captured as special error rows instead of raising exceptions. This ensures all successful
    API calls are persisted before the notebook exits gracefully.
    """
    import requests as _requests
    import urllib as _urllib
    import time as _time
    from datetime import date as _date

    _api_url = "https://plus.dnb.com/v1"

    for pdf in pdf_iterator:
        rows = []
        stop_processing = False  # Flag to stop processing after quota/auth failure

        for _, record in pdf.iterrows():
            # If we hit a fatal error (quota/auth), skip remaining records in this partition
            if stop_processing:
                break

            endpoint = f"{_api_url}/match/cleanseMatch?"
            params = {
                "confidenceLowerLevelThresholdValue": _min_confidence,
                "candidateMaximumQuantity": _candidate_max_qty,
                "name": str(record.get("name", "")),
                "streetAddressLine1": str(record.get("streetaddress", "")),
                "addressLocality": str(record.get("city", "")),
                "addressRegion": str(record.get("state", "")),
                "postalCode": str(record.get("postal_code", "")),
                "countryISOAlpha2Code": str(record.get("country", "")),
            }
            headers = {"Authorization": f"Bearer {_session_token}"}
            url = endpoint + _urllib.parse.urlencode(params)

            try:
                resp = _requests.get(url, headers=headers, timeout=30)

                # --- Check 1: Non-200 HTTP status ---
                if resp.status_code != 200:

                    # --- HTTP 401: Authentication failed - stop processing ---
                    if resp.status_code == 401:
                        rows.append({
                            "lakefusion_id": str(record.get("lakefusion_id", "")),
                            "update_date": _date.today(),
                            "accepted": False,
                            "input_source_id": str(record.get("source_id", "")),
                            "input_name": str(record.get("name", "")),
                            "input_streetaddress": str(record.get("streetaddress", "")),
                            "input_city": str(record.get("city", "")),
                            "input_country": str(record.get("country", "")),
                            "input_state": str(record.get("state", "")),
                            "input_postal_code": str(record.get("postal_code", "")),
                            "dnb_duns": None,
                            "dnb_name": None,
                            "dnb_nameMatchScore": None,
                            "dnb_status": "AUTH_FAILED:401|Token invalid or expired",
                            "dnb_streetaddress": None,
                            "dnb_city": None,
                            "dnb_country": None,
                            "dnb_state": None,
                            "dnb_postal_code": None,
                            "dnb_confidenceCode": None,
                            "dnb_matchDataProfileComponents": None,
                            "dnb_matchgradeComponents": None,
                        })
                        stop_processing = True
                        continue

                    # --- HTTP 429: Check if quota error (00050) ---
                    if resp.status_code == 429:
                        try:
                            body = resp.json()
                            if body.get("error", {}).get("errorCode") == "00050":
                                # Capture as special quota exhausted row, then stop processing
                                rows.append({
                                    "lakefusion_id": str(record.get("lakefusion_id", "")),
                                    "update_date": _date.today(),
                                    "accepted": False,
                                    "input_source_id": str(record.get("source_id", "")),
                                    "input_name": str(record.get("name", "")),
                                    "input_streetaddress": str(record.get("streetaddress", "")),
                                    "input_city": str(record.get("city", "")),
                                    "input_country": str(record.get("country", "")),
                                    "input_state": str(record.get("state", "")),
                                    "input_postal_code": str(record.get("postal_code", "")),
                                    "dnb_duns": None,
                                    "dnb_name": None,
                                    "dnb_nameMatchScore": None,
                                    "dnb_status": f"QUOTA_EXHAUSTED:00050|{body['error'].get('errorMessage', '')}",
                                    "dnb_streetaddress": None,
                                    "dnb_city": None,
                                    "dnb_country": None,
                                    "dnb_state": None,
                                    "dnb_postal_code": None,
                                    "dnb_confidenceCode": None,
                                    "dnb_matchDataProfileComponents": None,
                                    "dnb_matchgradeComponents": None,
                                })
                                stop_processing = True
                                continue
                        except ValueError:
                            pass

                    # Other non-200 errors — capture as error row but continue
                    rows.append({
                        "lakefusion_id": str(record.get("lakefusion_id", "")),
                        "update_date": _date.today(),
                        "accepted": False,
                        "input_source_id": str(record.get("source_id", "")),
                        "input_name": str(record.get("name", "")),
                        "input_streetaddress": str(record.get("streetaddress", "")),
                        "input_city": str(record.get("city", "")),
                        "input_country": str(record.get("country", "")),
                        "input_state": str(record.get("state", "")),
                        "input_postal_code": str(record.get("postal_code", "")),
                        "dnb_duns": None,
                        "dnb_name": None,
                        "dnb_nameMatchScore": None,
                        "dnb_status": f"ERROR:HTTP_{resp.status_code}|{resp.text[:500]}",
                        "dnb_streetaddress": None,
                        "dnb_city": None,
                        "dnb_country": None,
                        "dnb_state": None,
                        "dnb_postal_code": None,
                        "dnb_confidenceCode": None,
                        "dnb_matchDataProfileComponents": None,
                        "dnb_matchgradeComponents": None,
                    })
                    continue

                json_resp = resp.json()

                # --- Check 2: HTTP 200 but error in body ---
                if "error" in json_resp:
                    err = json_resp["error"]
                    error_code = str(err.get("errorCode", ""))
                    if error_code == "00050":
                        # Capture as special quota exhausted row, then stop processing
                        rows.append({
                            "lakefusion_id": str(record.get("lakefusion_id", "")),
                            "update_date": _date.today(),
                            "accepted": False,
                            "input_source_id": str(record.get("source_id", "")),
                            "input_name": str(record.get("name", "")),
                            "input_streetaddress": str(record.get("streetaddress", "")),
                            "input_city": str(record.get("city", "")),
                            "input_country": str(record.get("country", "")),
                            "input_state": str(record.get("state", "")),
                            "input_postal_code": str(record.get("postal_code", "")),
                            "dnb_duns": None,
                            "dnb_name": None,
                            "dnb_nameMatchScore": None,
                            "dnb_status": f"QUOTA_EXHAUSTED:00050|{err.get('errorMessage', '')}",
                            "dnb_streetaddress": None,
                            "dnb_city": None,
                            "dnb_country": None,
                            "dnb_state": None,
                            "dnb_postal_code": None,
                            "dnb_confidenceCode": None,
                            "dnb_matchDataProfileComponents": None,
                            "dnb_matchgradeComponents": None,
                        })
                        stop_processing = True
                        continue

                    # Other API errors — capture as error row
                    rows.append({
                        "lakefusion_id": str(record.get("lakefusion_id", "")),
                        "update_date": _date.today(),
                        "accepted": False,
                        "input_source_id": str(record.get("source_id", "")),
                        "input_name": str(record.get("name", "")),
                        "input_streetaddress": str(record.get("streetaddress", "")),
                        "input_city": str(record.get("city", "")),
                        "input_country": str(record.get("country", "")),
                        "input_state": str(record.get("state", "")),
                        "input_postal_code": str(record.get("postal_code", "")),
                        "dnb_duns": None,
                        "dnb_name": None,
                        "dnb_nameMatchScore": None,
                        "dnb_status": f"ERROR:{error_code}|{err.get('errorMessage', '')}",
                        "dnb_streetaddress": None,
                        "dnb_city": None,
                        "dnb_country": None,
                        "dnb_state": None,
                        "dnb_postal_code": None,
                        "dnb_confidenceCode": None,
                        "dnb_matchDataProfileComponents": None,
                        "dnb_matchgradeComponents": None,
                    })
                    continue

                # --- Check 3: HTTP 200, no error = successful call, process candidates ---
                match_candidates = json_resp.get("matchCandidates", [])
                if match_candidates:
                    for match in match_candidates:
                        org = match["organization"]
                        confidence = match["matchQualityInformation"]["confidenceCode"]
                        rows.append({
                            "lakefusion_id": str(record.get("lakefusion_id", "")),
                            "update_date": _date.today(),
                            "accepted": confidence >= _min_confidence,
                            "input_source_id": str(record.get("source_id", "")),
                            "input_name": str(record.get("name", "")),
                            "input_streetaddress": str(record.get("streetaddress", "")),
                            "input_city": str(record.get("city", "")),
                            "input_country": str(record.get("country", "")),
                            "input_state": str(record.get("state", "")),
                            "input_postal_code": str(record.get("postal_code", "")),
                            "dnb_duns": org["duns"],
                            "dnb_name": org["primaryName"],
                            "dnb_nameMatchScore": float(match["matchQualityInformation"].get("nameMatchScore", 0.0)),
                            "dnb_status": org.get("dunsControlStatus", {}).get("operatingStatus", {}).get("description"),
                            "dnb_streetaddress": org.get("primaryAddress", {}).get("streetAddress", {}).get("line1"),
                            "dnb_city": org.get("primaryAddress", {}).get("addressLocality", {}).get("name"),
                            "dnb_country": org.get("primaryAddress", {}).get("addressCountry", {}).get("isoAlpha2Code"),
                            "dnb_state": org.get("primaryAddress", {}).get("addressRegion", {}).get("abbreviatedName"),
                            "dnb_postal_code": org.get("primaryAddress", {}).get("postalCode"),
                            "dnb_confidenceCode": int(confidence),
                            "dnb_matchDataProfileComponents": match["matchQualityInformation"].get("matchDataProfileComponents", []),
                            "dnb_matchgradeComponents": match["matchQualityInformation"].get("matchGradeComponents", []),
                        })
                # No candidates = valid response, not an error

            except Exception as e:
                # Capture exception as error row (no re-raising)
                rows.append({
                    "lakefusion_id": str(record.get("lakefusion_id", "")),
                    "update_date": _date.today(),
                    "accepted": False,
                    "input_source_id": str(record.get("source_id", "")),
                    "input_name": str(record.get("name", "")),
                    "input_streetaddress": str(record.get("streetaddress", "")),
                    "input_city": str(record.get("city", "")),
                    "input_country": str(record.get("country", "")),
                    "input_state": str(record.get("state", "")),
                    "input_postal_code": str(record.get("postal_code", "")),
                    "dnb_duns": None,
                    "dnb_name": None,
                    "dnb_nameMatchScore": None,
                    "dnb_status": f"EXCEPTION:{str(e)}",
                    "dnb_streetaddress": None,
                    "dnb_city": None,
                    "dnb_country": None,
                    "dnb_state": None,
                    "dnb_postal_code": None,
                    "dnb_confidenceCode": None,
                    "dnb_matchDataProfileComponents": None,
                    "dnb_matchgradeComponents": None,
                })

            # Throttle: 2 partitions × ~2 TPS each = ~4 TPS total (under 5 TPS limit)
            _time.sleep(0.5)

        if rows:
            yield pd.DataFrame(rows)
        else:
            yield pd.DataFrame(columns=[
                "lakefusion_id", "update_date", "accepted", "input_source_id", "input_name",
                "input_streetaddress", "input_city", "input_country", "input_state",
                "input_postal_code", "dnb_duns", "dnb_name", "dnb_nameMatchScore",
                "dnb_status", "dnb_streetaddress", "dnb_city", "dnb_country",
                "dnb_state", "dnb_postal_code", "dnb_confidenceCode",
                "dnb_matchDataProfileComponents", "dnb_matchgradeComponents",
            ])

# 2 partitions: each throttled to ~2 TPS = ~4 TPS total, safely under D&B 5 TPS limit
# Write ALL results (matches + errors) to silver layer — API calls execute ONCE here
dnb_silver_table = f"{catalog_name}.silver.{entity}_master_prod_dnb_results"

df_master.repartition(2).mapInPandas(match_partition, schema=Db_api.schema) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dnb_silver_table)
logger.info(f"D&B CleanseMatch complete — all results written to {dnb_silver_table}")

# --- Check for fatal errors (quota/auth) AFTER successful write ---
df_silver_check = spark.read.table(dnb_silver_table)
match_quota_exhausted = df_silver_check.filter(F.col("dnb_status").startswith("QUOTA_EXHAUSTED:")).count() > 0
match_auth_failed = df_silver_check.filter(F.col("dnb_status").startswith("AUTH_FAILED:")).count() > 0
match_api_blocked = match_quota_exhausted or match_auth_failed

if match_api_blocked:
    total_processed = df_silver_check.count()
    successful_matches = df_silver_check.filter(
        ~(F.col("dnb_status").startswith("ERROR:") |
          F.col("dnb_status").startswith("EXCEPTION:") |
          F.col("dnb_status").startswith("QUOTA_EXHAUSTED:") |
          F.col("dnb_status").startswith("AUTH_FAILED:")) |
        F.col("dnb_status").isNull()
    ).count()
    logger.warning("=" * 70)
    if match_auth_failed:
        logger.error("D&B API AUTHENTICATION FAILED (HTTP 401)")
        logger.error("The API token is invalid or has expired.")
    else:
        logger.error("D&B API QUOTA EXHAUSTED")
        logger.error("The API token has reached its transaction limit.")
    logger.warning("=" * 70)
    logger.info(f"Successfully processed and SAVED: {successful_matches} match results")
    logger.info(f"Total records written to silver: {total_processed}")
    logger.warning("All successful results have been persisted. Request a new API token and rerun.")
    logger.warning("Hierarchy enrichment will be SKIPPED.")
    logger.warning("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persist Data to the D&B DUNS Table
# MAGIC ###
# MAGIC Read from silver, split successful matches to gold._dnb_duns, errors to gold._dnb_duns_error.

# COMMAND ----------

# DBTITLE 1,Remove it after demo
# test_row = df_master.first()
# result = z.match_d_and_b(test_row)
# print(json.dumps(result, indent=2))

# COMMAND ----------

# DBTITLE 1,Split silver results: errors to _duns_error, matches through scoring to _dnb_duns
dnb_error_table = f"{catalog_name}.gold.{entity}_master_prod_dnb_duns_error"
df_silver = spark.read.table(dnb_silver_table)

# --- Write error rows to gold._duns_error ---
df_errors = df_silver.filter(
    F.col("dnb_status").startswith("ERROR:") |
    F.col("dnb_status").startswith("EXCEPTION:") |
    F.col("dnb_status").startswith("QUOTA_EXHAUSTED:") |
    F.col("dnb_status").startswith("AUTH_FAILED:")
)
error_count = df_errors.count()

if error_count > 0:
    error_table_exists = spark.catalog.tableExists(dnb_error_table)
    if error_table_exists:
        temp_view = f"_tmp_dnb_errors_{entity}"
        df_errors.createOrReplaceTempView(temp_view)
        spark.sql(f"""
            MERGE INTO {dnb_error_table} AS target
            USING {temp_view} AS source
            ON target.lakefusion_id = source.lakefusion_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info(f"Merged {error_count} error records into {dnb_error_table}")
    else:
        df_errors.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dnb_error_table)
        logger.info(f"Created {dnb_error_table} with {error_count} error records")
else:
    logger.info("No D&B API failures recorded")

# --- Score and write successful matches ---
df_matched = df_silver.filter(
    ~(F.col("dnb_status").startswith("ERROR:") |
      F.col("dnb_status").startswith("EXCEPTION:") |
      F.col("dnb_status").startswith("QUOTA_EXHAUSTED:") |
      F.col("dnb_status").startswith("AUTH_FAILED:")) |
    F.col("dnb_status").isNull()
)
logger.info(f"Match rows: {df_matched.count()} | Error rows: {error_count}")

# COMMAND ----------

# DBTITLE 1,Score matches and write to gold._dnb_duns table
df_matched.createOrReplaceTempView("dnb_new_matches")

df_merge_score=spark.sql(f"""
WITH max_scores AS (
    SELECT
        input_source_id,
        MAX(dnb_confidenceCode) AS max_score
    FROM dnb_new_matches
    WHERE dnb_confidenceCode >= {threshold}
    GROUP BY input_source_id
),

score_counts AS (
    SELECT
        m.input_source_id,
        m.dnb_confidenceCode,
        COUNT(*) AS score_level_count
    FROM dnb_new_matches m
    LEFT JOIN max_scores ms
      ON m.input_source_id = ms.input_source_id
     AND m.dnb_confidenceCode = ms.max_score
    GROUP BY m.input_source_id, m.dnb_confidenceCode
),
final_decision AS (
    SELECT
        m.*,
        CASE
            WHEN sc.score_level_count = 1 AND m.dnb_confidenceCode = ms.max_score THEN 'AUTO_MERGED'
            WHEN sc.score_level_count > 1 AND m.dnb_confidenceCode = ms.max_score THEN 'IN_REVIEW'
            WHEN ms.max_score IS NULL AND m.dnb_confidenceCode >= {minimumconfidence} THEN 'IN_REVIEW'
            ELSE 'NOT_A_MATCH'
        END AS merge_status
    FROM dnb_new_matches m
    LEFT JOIN max_scores ms
      ON m.input_source_id = ms.input_source_id
    LEFT JOIN score_counts sc
      ON m.input_source_id = sc.input_source_id AND m.dnb_confidenceCode = sc.dnb_confidenceCode
)

SELECT *
FROM final_decision
""")

# Write to _dnb_duns table (dedup on lakefusion_id + dnb_duns)
match_count = df_merge_score.count()
if match_count > 0:
    if dnb_table_exists and not spark.table(dnb_duns_table).isEmpty():
        temp_view = f"_tmp_dnb_duns_{entity}"
        df_merge_score.createOrReplaceTempView(temp_view)
        spark.sql(f"""
            MERGE INTO {dnb_duns_table} AS target
            USING {temp_view} AS source
            ON target.lakefusion_id = source.lakefusion_id AND target.dnb_duns = source.dnb_duns
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info(f"Merged {match_count} D&B match records into {dnb_duns_table}")
    else:
        df_merge_score.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dnb_duns_table)
        logger.info(f"Created {dnb_duns_table} with {match_count} D&B match records")
else:
    logger.info("No successful D&B matches to write to DUNS table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hierarchy Enrichment
# MAGIC Fetch corporate hierarchy from D&B for confirmed matches (AUTO_MERGED / ACCEPTED).
# MAGIC Writes to a separate enrichment table — one row per DUNS per enrichment type.
# MAGIC Only enriches records not yet enriched (incremental).

# COMMAND ----------

# DBTITLE 1,Load DUNS needing hierarchy enrichment
enrichment_policies = entity_dnb_settings.get("enrichment_policies", {})
hierarchy_enabled = enrichment_policies.get("hierarchy", {}).get("enabled", False)
dnb_enrichment_table = f"{catalog_name}.gold.{entity}_master_prod_dnb_enrichment"

# Skip hierarchy enrichment if API was blocked during matching
if hierarchy_enabled and match_api_blocked:
    logger.warning("=" * 70)
    logger.warning("SKIPPING HIERARCHY ENRICHMENT")
    logger.warning("=" * 70)
    if match_auth_failed:
        logger.error("Authentication failed during the matching phase (HTTP 401).")
    else:
        logger.error("Quota was exhausted during the matching phase.")
    logger.warning("Hierarchy enrichment will be skipped to avoid wasting API calls.")
    logger.warning("Once you have a new API token, rerun the notebook.")
    logger.warning("=" * 70)
    hierarchy_enabled = False

if hierarchy_enabled:
    enrichment_table_exists = spark.catalog.tableExists(dnb_enrichment_table)
    df_confirmed = spark.sql(f"""
        SELECT DISTINCT lakefusion_id, dnb_duns
        FROM {dnb_duns_table}
        WHERE merge_status IN ('AUTO_MERGED', 'ACCEPTED', 'MANUAL_MERGE')
    """)
    if enrichment_table_exists:
        df_already_enriched = spark.sql(f"""
            SELECT dnb_duns FROM {dnb_enrichment_table}
            WHERE enrichment_type = 'hierarchy'
        """)
        df_to_enrich = df_confirmed.join(df_already_enriched, on="dnb_duns", how="left_anti")
    else:
        df_to_enrich = df_confirmed

# COMMAND ----------

# DBTITLE 1,Hierarchy enrichment UDF (mapInPandas)
from pyspark.sql.types import TimestampType

enrichment_schema = StructType([
    StructField("lakefusion_id", StringType(), True),
    StructField("dnb_duns", StringType(), True),
    StructField("enrichment_type", StringType(), True),
    StructField("enrichment_data", StringType(), True),
    StructField("enriched_at", TimestampType(), True),
])

def enrich_hierarchy_partition(pdf_iterator):
    """Fetch hierarchy data from D&B for each DUNS.
    HTTP 401 and 429+00050 stop processing and save what we have.
    """
    import requests as _requests, json as _json
    import time as _time
    from datetime import datetime as _datetime
    _api_url = "https://plus.dnb.com/v1"

    for pdf in pdf_iterator:
        rows = []
        stop_processing = False

        for _, record in pdf.iterrows():
            if stop_processing:
                break

            duns = str(record["dnb_duns"])
            lf_id = str(record["lakefusion_id"])
            url = f"{_api_url}/data/duns/{duns}?blockIDs=hierarchyconnections_L1_v1"
            headers = {"Authorization": f"Bearer {_session_token}"}

            try:
                resp = _requests.get(url, headers=headers, timeout=30)

                if resp.status_code != 200:
                    # HTTP 401 - Auth failed, stop
                    if resp.status_code == 401:
                        rows.append({
                            "lakefusion_id": lf_id,
                            "dnb_duns": duns,
                            "enrichment_type": "hierarchy",
                            "enrichment_data": "AUTH_FAILED:401|Token invalid or expired",
                            "enriched_at": _datetime.utcnow(),
                        })
                        stop_processing = True
                        continue

                    # HTTP 429 - Check for quota
                    if resp.status_code == 429:
                        try:
                            body = resp.json()
                            if body.get("error", {}).get("errorCode") == "00050":
                                rows.append({
                                    "lakefusion_id": lf_id,
                                    "dnb_duns": duns,
                                    "enrichment_type": "hierarchy",
                                    "enrichment_data": f"QUOTA_EXHAUSTED:00050|{body['error'].get('errorMessage', '')}",
                                    "enriched_at": _datetime.utcnow(),
                                })
                                stop_processing = True
                                continue
                        except ValueError:
                            pass

                    # Other errors - capture and continue
                    rows.append({
                        "lakefusion_id": lf_id,
                        "dnb_duns": duns,
                        "enrichment_type": "hierarchy",
                        "enrichment_data": f"ERROR:HTTP_{resp.status_code}|{resp.text[:500]}",
                        "enriched_at": _datetime.utcnow(),
                    })
                    _time.sleep(0.5)
                    continue

                json_resp = resp.json()

                if "error" in json_resp:
                    err = json_resp["error"]
                    error_code = str(err.get("errorCode", ""))
                    if error_code == "00050":
                        rows.append({
                            "lakefusion_id": lf_id,
                            "dnb_duns": duns,
                            "enrichment_type": "hierarchy",
                            "enrichment_data": f"QUOTA_EXHAUSTED:00050|{err.get('errorMessage', '')}",
                            "enriched_at": _datetime.utcnow(),
                        })
                        stop_processing = True
                        continue

                    rows.append({
                        "lakefusion_id": lf_id,
                        "dnb_duns": duns,
                        "enrichment_type": "hierarchy",
                        "enrichment_data": f"ERROR:{error_code}|{err.get('errorMessage', '')}",
                        "enriched_at": _datetime.utcnow(),
                    })
                    _time.sleep(0.5)
                    continue

                # Success - extract hierarchy
                org = json_resp.get("organization", {})
                linkage = org.get("corporateLinkage", {})
                hierarchy = {
                    "hierarchyLevel": linkage.get("hierarchyLevel"),
                    "familytreeRolesPlayed": linkage.get("familytreeRolesPlayed", []),
                    "globalUltimateFamilyTreeMembersCount": linkage.get("globalUltimateFamilyTreeMembersCount"),
                    "branchesCount": linkage.get("branchesCount"),
                    "globalUltimate": linkage.get("globalUltimate", {}),
                    "domesticUltimate": linkage.get("domesticUltimate", {}),
                    "parent": linkage.get("parent", {}),
                    "headQuarter": linkage.get("headQuarter", {}),
                    "branches": linkage.get("branches", []),
                }
                rows.append({
                    "lakefusion_id": lf_id,
                    "dnb_duns": duns,
                    "enrichment_type": "hierarchy",
                    "enrichment_data": _json.dumps(hierarchy),
                    "enriched_at": _datetime.utcnow(),
                })

            except Exception as e:
                rows.append({
                    "lakefusion_id": lf_id,
                    "dnb_duns": duns,
                    "enrichment_type": "hierarchy",
                    "enrichment_data": f"EXCEPTION:{str(e)}",
                    "enriched_at": _datetime.utcnow(),
                })

            _time.sleep(0.5)

        if rows:
            yield pd.DataFrame(rows)
        else:
            yield pd.DataFrame(columns=["lakefusion_id", "dnb_duns", "enrichment_type", "enrichment_data", "enriched_at"])

# COMMAND ----------

# DBTITLE 1,Run hierarchy enrichment
if hierarchy_enabled:
    enrichment_silver_table = f"{catalog_name}.silver.{entity}_master_prod_dnb_enrichment_results"
    dnb_enrichment_error_table = f"{catalog_name}.gold.{entity}_master_prod_dnb_enrichment_error"

    df_to_enrich.repartition(2).mapInPandas(
        enrich_hierarchy_partition, schema=enrichment_schema
    ).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(enrichment_silver_table)
    logger.info(f"Enrichment results written to {enrichment_silver_table}")

    # Check for fatal errors
    df_enrichment_silver = spark.read.table(enrichment_silver_table)
    enrichment_api_blocked = df_enrichment_silver.filter(
        F.col("enrichment_data").startswith("QUOTA_EXHAUSTED:") |
        F.col("enrichment_data").startswith("AUTH_FAILED:")
    ).count() > 0

    if enrichment_api_blocked:
        successful_enrichments = df_enrichment_silver.filter(
            ~(F.col("enrichment_data").startswith("ERROR:") |
              F.col("enrichment_data").startswith("EXCEPTION:") |
              F.col("enrichment_data").startswith("QUOTA_EXHAUSTED:") |
              F.col("enrichment_data").startswith("AUTH_FAILED:"))
        ).count()
        logger.warning("=" * 70)
        logger.error("D&B API BLOCKED (during enrichment)")
        logger.info(f"Successfully enriched and SAVED: {successful_enrichments} records")
        logger.warning("All successful results have been persisted. Request a new API token and rerun for remaining records.")
        logger.warning("=" * 70)

    # Split errors and successes
    df_enrichment_errors = df_enrichment_silver.filter(
        F.col("enrichment_data").startswith("ERROR:") |
        F.col("enrichment_data").startswith("EXCEPTION:") |
        F.col("enrichment_data").startswith("QUOTA_EXHAUSTED:") |
        F.col("enrichment_data").startswith("AUTH_FAILED:")
    )
    enrichment_error_count = df_enrichment_errors.count()

    if enrichment_error_count > 0:
        enrichment_error_table_exists = spark.catalog.tableExists(dnb_enrichment_error_table)
        if enrichment_error_table_exists:
            temp_view = f"_tmp_dnb_enrichment_errors_{entity}"
            df_enrichment_errors.createOrReplaceTempView(temp_view)
            spark.sql(f"""
                MERGE INTO {dnb_enrichment_error_table} AS target
                USING {temp_view} AS source
                ON target.lakefusion_id = source.lakefusion_id AND target.dnb_duns = source.dnb_duns
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            logger.info(f"Merged {enrichment_error_count} enrichment error records into {dnb_enrichment_error_table}")
        else:
            df_enrichment_errors.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dnb_enrichment_error_table)
            logger.info(f"Created {dnb_enrichment_error_table} with {enrichment_error_count} enrichment error records")
    else:
        logger.info("No enrichment API failures recorded")

    # Write successful enrichments
    df_enrichment_success = df_enrichment_silver.filter(
        ~(F.col("enrichment_data").startswith("ERROR:") |
          F.col("enrichment_data").startswith("EXCEPTION:") |
          F.col("enrichment_data").startswith("QUOTA_EXHAUSTED:") |
          F.col("enrichment_data").startswith("AUTH_FAILED:"))
    )
    success_count = df_enrichment_success.count()
    logger.info(f"Enrichment — Success: {success_count} | Errors: {enrichment_error_count}")

    if success_count > 0:
        if enrichment_table_exists:
            temp_view = f"_tmp_dnb_enrichment_{entity}"
            df_enrichment_success.createOrReplaceTempView(temp_view)
            spark.sql(f"""
                MERGE INTO {dnb_enrichment_table} AS target
                USING {temp_view} AS source
                ON target.lakefusion_id = source.lakefusion_id AND target.dnb_duns = source.dnb_duns AND target.enrichment_type = source.enrichment_type
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            logger.info(f"Merged enrichment records into {dnb_enrichment_table}")
        else:
            df_enrichment_success.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dnb_enrichment_table)
            logger.info(f"Created {dnb_enrichment_table} with enrichment records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exit gracefully if API was blocked
# MAGIC All successful results have been persisted above. Now exit with a clear message.

# COMMAND ----------

# DBTITLE 1,Check for API issues and exit gracefully
enrichment_api_blocked_final = False
try:
    if spark.catalog.tableExists(enrichment_silver_table):
        df_enrichment_final = spark.read.table(enrichment_silver_table)
        enrichment_api_blocked_final = df_enrichment_final.filter(
            F.col("enrichment_data").startswith("QUOTA_EXHAUSTED:") |
            F.col("enrichment_data").startswith("AUTH_FAILED:")
        ).count() > 0
except:
    pass  # enrichment_silver_table may not be defined if hierarchy was disabled

if match_api_blocked or enrichment_api_blocked_final:
    issues = []
    if match_quota_exhausted:
        issues.append("quota exhausted during matching")
    if match_auth_failed:
        issues.append("auth failed during matching")
    if enrichment_api_blocked_final:
        issues.append("API blocked during enrichment")
    issue_summary = ", ".join(issues)
    dbutils.notebook.exit(f"API_BLOCKED: {issue_summary}. All successful results saved. Request a new API token and rerun.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register your population for monitoring.
# MAGIC ###
# MAGIC Using our saved data, let's enable monitoring for our company:
# MAGIC
# MAGIC 1. **Query the DUNS Numbers**: We will first retrieve the DUNS numbers we stored from our Company Match process. In this query we are only retrieving those D-U-N-S which were 'accepted' and marked TRUE. (i.e., satidfying our Confidence Code minimum)
# MAGIC 2. **Execute the Monitoring API Call**: Next, we'll use the `add_to_monitoring` function. This API call will register the retrieved DUNS records for monitoring.
# MAGIC
# MAGIC
# MAGIC By following these steps, you'll activate monitoring for the relevant companies in your system via Delta Share and you should receive a SEED file within 24-36 hours for any new D-U-N-S added.

# COMMAND ----------

from pyspark.sql.functions import *
df_merge_score=df_merge_score.filter(col("merge_status")=='AUTO_MERGED')

# COMMAND ----------

# DBTITLE 1,Add to Monitoring Registration
### This is an example of how to add to D-U-N-S to an existing monitoring registration
### The following code adds all accepted MATCHED D-U-N-S to your monitoring registration. By running this command you are accepting all risks
### associated with your Records Under Management (RUM) within your current D&B Contract.

#z.add_to_monitoring(input_df=df_merge_score,registration_id='####YOUR MONITORING REGISTRATION ID####')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export your monitoring population.
# MAGIC ###
# MAGIC If at any time you wish to know exactly which D-U-N-S are currently in your monitoring registration, you must query the Export Monitoring Registration API endpoint.
# MAGIC
# MAGIC This will initiate a command to the system that will trigger the Monitoring system to populate the DUNS table within your Delta Share.
# MAGIC
# MAGIC **Please Note: This table is ONLY populated when the API is called. It is NOT a LIVE table.**

# COMMAND ----------

### This is an example of how to trigger the Export Monitoring Registration API and populate your DUNS table
#z.export_monitoring_registrations(input_df=df_merge_score,registration_id='####YOUR MONITORING REGISTRATION ID####')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete a D-U-N-S from your monitoring population.
# MAGIC ###
# MAGIC If at any time you wish to delete a D-U-N-S Number from a current monitoring registration, you must query the Delete From Monitoring API endpoint.
# MAGIC
# MAGIC This will initiate a command to the system that will remove the D-U-N-S Number in question from the Monitoring system.

# COMMAND ----------

### This is an example of how to remove a D-U-N-S Number from the current Monitoring Registration
### The DataFrame (_sqldf) should contain the D-U-N-S Numbers you wish to remove from your Monitoring Registration

#z.delete_from_monitoring(input_df=df_merge_score,registration_id='####YOUR MONITORING REGISTRATION ID####')
