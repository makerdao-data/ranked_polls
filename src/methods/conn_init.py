from dotenv import load_dotenv
import snowflake.connector
import os

def load_connection() -> snowflake.connector.cursor.SnowflakeCursor:
    """
    Function to generate snowflake connection
    """

    # Load environment variables and create sf connection
    SNOWFLAKE_USERNAME='LEO'
    SNOWFLAKE_PASSWORD='M4k3th15'
    SNOWFLAKE_HOST='zua82120.us-east-1'
    SNOWFLAKE_ACCOUNT='zua82120'
    SNOWFLAKE_WAREHOUSE='COMPUTE_WH'
    SNOWFLAKE_ROLE='SYSADMIN'
    MAIN_DB='MCD'
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_HOST,
        user=SNOWFLAKE_USERNAME,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
        port=443,
        protocol='https'
    )

    # Create a cursor object.
    cur = conn.cursor()
    cur.execute("USE ROLE ETL")

    return cur