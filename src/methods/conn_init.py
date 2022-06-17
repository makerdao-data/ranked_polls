from dotenv import load_dotenv
import snowflake.connector
import os

def load_connection() -> snowflake.connector.cursor.SnowflakeCursor:
    """
    Function to generate snowflake connection
    """

    # Load environment variables and create sf connection
    load_dotenv()
    conn = snowflake.connector.connect(
                    user=os.getenv("SNOWFLAKE_USERNAME"),
                    password=os.getenv("SNOWFLAKE_PASSWORD"),
                    account=os.getenv("SNOWFLAKE_HOST"),
                    role=os.getenv("SNOWFLAKE_ROLE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                    database='TOKEN_FLOW',
                    schema='DICU',
                    protocol='https',
                    port=443
                    )


    # Create a cursor object.
    cur = conn.cursor()
    cur.execute("USE ROLE ETL")

    return cur