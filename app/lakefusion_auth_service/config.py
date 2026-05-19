import os
from urllib.parse import quote_plus

#  Database Configuration
sql_username = os.environ.get('SQL_USERNAME', 'root')
sql_password = os.environ.get('SQL_PASSWORD', '')
sql_server = os.environ.get('SQL_SERVER', 'localhost:3306')

sql_db_name = os.environ.get('SQL_DBNAME', 'lakefusion_transactional_db')
deployment_env = os.environ.get('DEPLOYMENT_ENV', 'prod')

db_type = os.environ.get("DB_TYPE", "mysql").lower()

if deployment_env:
    sql_db_name = f'{sql_db_name}_{deployment_env}'

class AppConfig(object):
    # Configure your MySQL database connection
    encoded_password = quote_plus(sql_password)
    if db_type == "mysql":
        DATABASE_URL = (
            f"mysql+pymysql://{sql_username}:{encoded_password}@{sql_server}/{sql_db_name}"
        )
    elif db_type == "mssql":
        DATABASE_URL = (
            f"mssql+pyodbc://{sql_username}:{encoded_password}@{sql_server}/{sql_db_name}"
            "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )
    elif db_type == "postgresql":
        DATABASE_URL = (
            f"postgresql+psycopg2://{sql_username}:{encoded_password}@{sql_server}/{sql_db_name}"
        )
    elif db_type == "lakebase":
        DATABASE_URL = "" # Generate this URL dynamically in the database module
    else:
        raise ValueError(f"Unsupported DB_TYPE: {db_type}. Supported types are 'mysql', 'mssql', 'postgresql' and 'lakebase'.")
