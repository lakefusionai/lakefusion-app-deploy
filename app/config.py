"""
Shared config for the unified LakeFusion app.
Reads environment variables common to all services.
"""
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv('.env')

# PG* vars (same convention as LakeGraph)
PGHOST = os.environ.get("PGHOST", "")
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ.get("PGDATABASE", "")
PGUSER = os.environ.get("PGUSER", "")
PGPASSWORD = os.environ.get("PGPASSWORD", "")

# SQL_* vars (for mysql/mssql/postgresql legacy)
sql_username = os.environ.get('SQL_USERNAME', 'root')
sql_password = os.environ.get('SQL_PASSWORD', '')
sql_server = os.environ.get('SQL_SERVER', 'localhost:3306')
sql_db_name = os.environ.get('SQL_DBNAME', 'lakefusion_transactional_db')
deployment_env = os.environ.get('DEPLOYMENT_ENV', 'dev')
db_type = os.environ.get("DB_TYPE", "lakebase").lower()

if deployment_env:
    sql_db_name = f'{sql_db_name}_{deployment_env}'

lakefusion_databricks_dapi = os.environ.get("LAKEFUSION_DATABRICKS_DAPI", "")
run_dbx_pipeline_artifacts_import = os.environ.get("RUN_DBX_PIPELINE_ARTIFACTS_IMPORT", "True").lower() == "true"

class AppConfig(object):
    if PGHOST and PGDATABASE:
        # PG* vars present → local PostgreSQL (takes priority)
        encoded_password = quote_plus(PGPASSWORD) if PGPASSWORD else ""
        DATABASE_URL = (
            f"postgresql+psycopg2://{PGUSER}:{encoded_password}@{PGHOST}:{PGPORT}/{PGDATABASE}"
        )
    elif db_type == "mysql":
        encoded_password = quote_plus(sql_password)
        DATABASE_URL = (
            f"mysql+pymysql://{sql_username}:{encoded_password}@{sql_server}/{sql_db_name}"
        )
    elif db_type == "mssql":
        encoded_password = quote_plus(sql_password)
        DATABASE_URL = (
            f"mssql+pyodbc://{sql_username}:{encoded_password}@{sql_server}/{sql_db_name}"
            "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )
    elif db_type == "postgresql":
        encoded_password = quote_plus(sql_password)
        DATABASE_URL = (
            f"postgresql+psycopg2://{sql_username}:{encoded_password}@{sql_server}/{sql_db_name}"
        )
    elif db_type == "lakebase":
        DATABASE_URL = ""  # Generated dynamically in database.py via Databricks SDK
    else:
        raise ValueError(f"Unsupported DB_TYPE: {db_type}")
