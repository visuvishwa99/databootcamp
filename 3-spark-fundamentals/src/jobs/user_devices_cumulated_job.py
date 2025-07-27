from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

def read_from_postgres(db_host, db_port, db_name, db_user, db_password, table_name, jdbc_driver_version="42.7.3"):
    """
    Reads data from a PostgreSQL table into a Spark DataFrame.

    Args:
        db_host (str): The hostname or IP address of the PostgreSQL container.
        db_port (str): The port number of the PostgreSQL database.
        db_name (str): The name of the database to connect to.
        db_user (str): The username for PostgreSQL authentication.
        db_password (str): The password for PostgreSQL authentication.
        table_name (str): The name of the table to read from.
        jdbc_driver_version (str): The version of the PostgreSQL JDBC driver.
    """
    spark = None
    try:
        # Initialize SparkSession with PostgreSQL JDBC driver package
        spark = SparkSession.builder \
            .appName(f"ReadFromPostgreSQL_{table_name}") \
            .config("spark.jars.packages", f"org.postgresql:postgresql:{jdbc_driver_version}") \
            .getOrCreate()

        print(f"SparkSession created successfully. Reading from table: {table_name}")

        # Construct JDBC URL
        jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

        # Database connection properties
        properties = {
            "user": db_user,
            "password": db_password,
            "driver": "org.postgresql.Driver"
        }

        # Read data from PostgreSQL table
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

        print(f"Successfully read data from {table_name}. Showing first 5 rows:")
        df.show(5)
        print(f"Schema of {table_name}:")
        df.printSchema()

        return df

    except AnalysisException as e:
        print(f"Error reading from PostgreSQL: {e}")
        print("Please ensure the table name, database name, and credentials are correct.")
        print("Also, verify that the PostgreSQL container is running and accessible from the Spark container.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if spark:
            spark.stop()
            print("SparkSession stopped.")



query = """

with yesterday as (

select 
    user_id,
    device_activity_datelist,
    current_user_date
from user_devices_cumulated where current_user_date = DATE('2023-01-30' )

)

,today as (

select 
user_id::TEXT,
DATE(cast(event_time as TIMESTAMP)) as date_active
from events where DATE(cast(event_time as TIMESTAMP)) = DATE('2023-01-31') 
and user_id is not null --filter input data 
GROUP BY user_id, DATE(cast(event_time as TIMESTAMP))

)

select 
COALESCE(y.user_id, t.user_id) as user_id,
case 
    when y.device_activity_datelist  is null then ARRAY[t.date_active]
    when t.date_active is null then y.device_activity_datelist
    else  ARRAY[t.date_active] || y.device_activity_datelist END as device_activity_datelist,
COALESCE(t.date_active,y.current_user_date + INTERVAL  '1 day') as date_active
from today T FULL OUTER JOIN yesterday y
ON t.user_id=y.user_id

"""


def do_history_scd_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("userdevcies")
    return spark.sql(query)


# --- PostgreSQL Connection Configuration ---
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
TABLE_TO_READ = "postgres.public.user_devices_cumulated" # Based on the SQL query "FROM postgres.public.actors"
JDBC_DRIVER_VERSION = "42.7.3"

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("historical_scd") \
      .config("spark.jars.packages", f"org.postgresql:postgresql:{JDBC_DRIVER_VERSION}") \
      .getOrCreate()

    # Read initial data from PostgreSQL
    initial_df = read_from_postgres(
        db_host=POSTGRES_HOST,
        db_port=POSTGRES_PORT,
        db_name=POSTGRES_DB,
        db_user=POSTGRES_USER,
        db_password=POSTGRES_PASSWORD,
        table_name=TABLE_TO_READ,
        jdbc_driver_version=JDBC_DRIVER_VERSION
    )

    output_df = do_history_scd_transformation(spark, initial_df)
    output_df.write.mode("overwrite").insertInto("historical_scd")

