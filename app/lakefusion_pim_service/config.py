import os
from urllib.parse import quote_plus

# --- Primary DB Configuration (MySQL — shared transactional: db_config_properties, audit_logs) ---
# This is the same DB used by all other LakeFusion services (middlelayer, cron, etc.)
# get_db() in app_db.py connects here via lakefusion-utility's database module.
sql_username = os.environ.get('SQL_USERNAME', 'root')
sql_password = os.environ.get('SQL_PASSWORD', '')
sql_server = os.environ.get('SQL_SERVER', 'localhost:3306')
sql_db_name = os.environ.get('SQL_DBNAME', 'lakefusion_transactional_db')
deployment_env = os.environ.get('DEPLOYMENT_ENV', 'prod')
databricks_host = os.environ.get('DATABRICKS_HOST', '')

db_type = os.environ.get("DB_TYPE", "mysql").lower()

if deployment_env:
    sql_db_name = f'{sql_db_name}_{deployment_env}'

class AppConfig(object):
    DATABRICKS_HOST = databricks_host

    encoded_password = quote_plus(sql_password)
    if db_type == "mysql":
        DATABASE_URL = (
            f"mysql+pymysql://{sql_username}:{encoded_password}@{sql_server}/{sql_db_name}"
        )
    elif db_type == "postgresql":
        DATABASE_URL = (
            f"postgresql+psycopg2://{sql_username}:{encoded_password}@{sql_server}/{sql_db_name}"
        )
    elif db_type == "mssql":
        DATABASE_URL = (
            f"mssql+pyodbc://{sql_username}:{encoded_password}@{sql_server}/{sql_db_name}"
            "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )
    elif db_type == "lakebase":
        DATABASE_URL = ""  # Generated dynamically in the database module
    else:
        raise ValueError(f"Unsupported DB_TYPE: {db_type}. Supported types are 'mysql', 'postgresql', 'mssql' and 'lakebase'.")


# --- Data DB Configuration (Postgres/Lakebase — data: products, taxonomy, attributes, pricing) ---
# get_data_db() in app_db.py connects here.
data_db_type = os.environ.get("DATA_DB_TYPE", "postgresql").lower()
data_sql_username = os.environ.get('DATA_SQL_USERNAME', 'postgres')
data_sql_password = os.environ.get('DATA_SQL_PASSWORD', '')
data_sql_server = os.environ.get('DATA_SQL_SERVER', 'localhost:5432')
data_sql_db_name = os.environ.get('DATA_SQL_DBNAME', '')  # Empty = lazy init via entity bridge
data_db_search_path = os.environ.get('DATA_DB_SEARCH_PATH', 'public')

if data_sql_db_name and deployment_env:
    data_sql_db_name = f'{data_sql_db_name}_{deployment_env}'

data_encoded_password = quote_plus(data_sql_password)

if data_db_type == "postgresql" and data_sql_db_name:
    DATA_DATABASE_URL = (
        f"postgresql+psycopg2://{data_sql_username}:{data_encoded_password}@{data_sql_server}/{data_sql_db_name}"
        f"?options=-csearch_path%3D{data_db_search_path}"
    )
elif data_db_type == "lakebase":
    DATA_DATABASE_URL = ""  # Generated dynamically
else:
    DATA_DATABASE_URL = ""  # No DB name configured — lazy init via entity bridge
