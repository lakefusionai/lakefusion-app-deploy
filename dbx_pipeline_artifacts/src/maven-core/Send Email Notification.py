# Databricks notebook source
# MAGIC %pip install sendgrid

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../utils/execute_utils

# COMMAND ----------

from dbruntime.databricks_repl_context import get_context

# COMMAND ----------

dbutils.widgets.text('entity_id', '', 'Entity ID')
dbutils.widgets.text('id_key', '', 'Primary Key')
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text('to_emails', '', 'Receipient Emails (Comma Separated)')
dbutils.widgets.text('job_id', '', 'Job ID')
dbutils.widgets.text('run_id', '', 'Run ID')
dbutils.widgets.dropdown("smtp_type", choices=["SendGrid"], defaultValue="SendGrid", label="SMTP Type")
dbutils.widgets.text("experiment_id", "", "Match Maven Experiment")
dbutils.widgets.text("catalog_name", "", "Catalog Name")

# COMMAND ----------

entity_id = dbutils.widgets.get('entity_id')
id_key = dbutils.widgets.get('id_key')
entity = dbutils.widgets.get("entity")
to_emails = dbutils.widgets.get('to_emails')
smtp_type = dbutils.widgets.get('smtp_type')
job_id = dbutils.widgets.get('job_id')
run_id = dbutils.widgets.get('run_id')
experiment_id = dbutils.widgets.get("experiment_id")
catalog_name = dbutils.widgets.get("catalog_name")
to_emails_arr = to_emails.split(',')
catalog_name=dbutils.widgets.get("catalog_name")

# COMMAND ----------

logger.info(type(to_emails_arr))

# COMMAND ----------

if(to_emails_arr==['']):
    dbutils.notebook.exit("No receipient emails provided")

# COMMAND ----------

entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
id_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "id_key", debugValue=id_key)

# COMMAND ----------

unified_table = f"{catalog_name}.silver.{entity}_unified"
master_table = f"{catalog_name}.gold.{entity}_master"
processed_unified_table = f"{catalog_name}.silver.{entity}_processed_unified"
if experiment_id:
  unified_table += f"_{experiment_id}"
  master_table += f"_{experiment_id}"
  processed_unified_table += f"_{experiment_id}"

merge_activities_table = f"{master_table}_merge_activities"
master_attribute_version_sources_table = f"{master_table}_attribute_version_sources"

master_table_list = master_table.split(".")
master_catalog, master_schema, master_table_name = master_table_list[0], master_table_list[1], master_table_list[2]

# COMMAND ----------

query = f"""
    WITH no_match AS (
    SELECT
        *
    FROM
        {merge_activities_table}
    WHERE
        action_type IN ('MANUAL_NOT_A_MATCH', 'JOB_NOT_A_MATCH')
    ),
    merges AS (
    SELECT
        *
    FROM
        {merge_activities_table}
    WHERE
        action_type IN ('MANUAL_MERGE', 'JOB_MERGE')
    ),
    pot_match AS (
    select
        ma.*,
        pu.potential_matches
    from
        (
        SELECT
            u.master_{id_key},
            collect_set(
            struct(u.*)
            ) AS potential_matches
        FROM
            (
                select pu.*, uo.* except ({id_key}) from 
                {processed_unified_table} pu
                inner join {unified_table} uo
                on pu.{id_key} = uo.{id_key}
            ) u
            JOIN {master_table} m ON m.{id_key} = u.master_{id_key}
            LEFT ANTI JOIN no_match on (
            u.master_{id_key} = no_match.master_{id_key}
            AND u.{id_key} = no_match.{id_key}
            AND u.dataset.path = no_match.`source`
            )
            LEFT ANTI JOIN merges on (
            u.master_{id_key} = merges.master_{id_key}
            AND u.{id_key} = merges.{id_key}
            AND u.dataset.path = merges.`source`
            )
        WHERE
            u.exploded_result.match = 'MATCH'
        GROUP BY
            u.master_{id_key}
        ) pu,
        {master_table} ma
    where
        pu.master_{id_key} = ma.{id_key}
    )
    SELECT
    *
    FROM
    pot_match;
"""
df_potential_matches = spark.sql(query)
total_reviewable_records = df_potential_matches.count()

# COMMAND ----------

# Access notebook context
notebook_context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

# Retrieve job ID and run ID
try:
    job_id = notebook_context.jobId().getOrElse(None)  # Job ID
    run_id = notebook_context.tags().get("runId").getOrElse(None)  # Run ID
except:
    pass

# COMMAND ----------

WORKSPACE_URL = get_context().workspaceUrl

# COMMAND ----------

import sendgrid
from sendgrid.helpers.mail import Mail, From, To, Content
from sendgrid import SendGridAPIClient

class SendGridEmail:
    sg: SendGridAPIClient
    sendgrid_from_email: str

    def __init__(self, api_key):
        self.sg = SendGridAPIClient(api_key)
        self.sendgrid_from_email = dbutils.secrets.get(scope="lakefusion", key="sendgrid_from_email")

    def send_email(self, to_emails, subject, content, content_type):
        # Create a list of To objects for multiple recipients
        to_emails_list = []
        for email in to_emails:
            to_emails_list.append(To(email))

        message = Mail(from_email=From(self.sendgrid_from_email),
                       to_emails=to_emails_list,
                       subject=subject,
                       html_content=Content(content_type, content))
        try:
            response = self.sg.send(message)
            logger.info("Email sent successfully")
        except Exception as e:
            logger.error(f"Error sending email: {e}")

# COMMAND ----------

html_body = f"""
<!DOCTYPE html>
<html>
<head>
  <title>Attention Needed: Review Critical Entities</title>
  <style>
    .button {{
      background-color: #750f11;
      color: white !important;
      padding: 10px 10px;
      text-align: center;
      text-decoration: none;
      display: inline-block;
      font-size: 12px;
      margin: 10px 0;
      cursor: pointer;
      border-radius: 8px;
    }}
    .container {{
      font-family: Arial, sans-serif;
      line-height: 1.6;
      max-width: 600px;
      margin: 0 auto;
      padding: 10px;
    }}
    h1 {{
      color: #333;
    }}
    p {{
      color: #555;
    }}
  </style>
</head>
<body>
  <div class="container">
    <h1>Attention Needed: Review Critical Entities</h1>
    <p>Dear User,</p>
    <p>LakeFusion has detected <strong>{total_reviewable_records} entities</strong> that require your review. These entities have been flagged as potential matches and need your attention to confirm or resolve their statuses.</p>
    <p>These flagged entities may indicate:</p>
    <ul>
      <li>Potential duplicate records.</li>
      <li>Unresolved entity relationships.</li>
      <li>Inconsistent data points requiring alignment.</li>
    </ul>
    <p><strong>Next Steps:<BR />Review Entities: </strong>Click below to access the list of flagged entities and take appropriate actions.</p>
    <a href="http://localhost:3000/entity-search" class="button">Review Critical Entities</a>
    <p>Addressing these flagged entities promptly ensures data accuracy and optimal system performance.</p>
    <p>Best Regards,<BR />The LakeFusion Team</p>
    <p></p>
  </div>
</body>
</html>
"""


# COMMAND ----------

email_sender = None
if smtp_type == 'SendGrid':
    api_key = dbutils.secrets.get(scope="lakefusion", key="sendgrid_api_key")
    email_sender = SendGridEmail(api_key=api_key)

    subject = "Attention Needed: Resolve Critical Entities"

    email_sender.send_email(to_emails_arr, subject, html_body, content_type='text/html')
