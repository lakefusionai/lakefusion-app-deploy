# Databricks notebook source
# MAGIC %pip install sendgrid
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text('entity_id', '', 'Entity ID')
dbutils.widgets.text('id_key', '', 'Primary Key')
dbutils.widgets.text("entity", "", "Entity Name")
dbutils.widgets.text('to_emails', '', 'Recipient Emails (Comma Separated)')
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

# COMMAND ----------

# Get from task values if available
entity = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "entity", debugValue=entity)
id_key = dbutils.jobs.taskValues.get("Parse_Entity_Model_JSON", "id_key", debugValue=id_key)


# COMMAND ----------

# MAGIC %run ../utils/execute_utils


# COMMAND ----------

# DBTITLE 1,Configuration
ENABLE_INTERACTIVE = False  # Set to True for step-by-step debugging
if ENABLE_INTERACTIVE:
    logger = _PrintLogger()
    logger.info("Interactive logging enabled — logger.info will print to console")


# COMMAND ----------

# Import SDK components
from lakefusion_core_engine.executors import TaskContext
from lakefusion_core_engine.executors.tasks.maven_core import SendEmailNotificationExecutor

# COMMAND ----------

# Build context
context = TaskContext(
    task_name="Send_Email_Notification",
    entity=entity,
    experiment_id=experiment_id,
    catalog_name=catalog_name,
    dataset_objects=[],
    spark=spark,
    dbutils=dbutils,
    params={
        'entity': entity,
        'catalog_name': catalog_name,
        'experiment_id': experiment_id,
        'id_key': id_key,
        'to_emails': to_emails,
        'smtp_type': smtp_type,
        'job_id': job_id,
        'run_id': run_id,
        "logger": logger
    }
)

# COMMAND ----------

# DBTITLE 1,Execute task
executor = SendEmailNotificationExecutor(context)

try:
    if not ENABLE_INTERACTIVE:
        result = executor.run()
    else:
        executor.interactive()
except Exception as e:
    logger.error(f"Error running executor: {e}")
    logger_instance.shutdown()
    raise

# COMMAND ----------

# DBTITLE 1,Display result
if not ENABLE_INTERACTIVE:
    logger.info(f"Status: {result.status.value}")
    logger_instance.shutdown()
    output_json = {
        "email_sent": result.task_values.get('email_sent', False),
        "total_reviewable_records": result.task_values.get('total_reviewable_records', 0)
    }
    dbutils.notebook.exit(output_json)

logger_instance.shutdown()