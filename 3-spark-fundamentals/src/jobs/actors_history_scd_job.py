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
WITH previous AS (
  SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    current_year,
    LAG(quality_class) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_quality_class,
    LAG(is_active)      OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_is_active
  FROM actors
  WHERE current_year <= 2021
),
change_indicator AS (
  SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    current_year,
    -- fire on first row OR any real change
    CASE
      WHEN previous_quality_class IS NULL
        OR quality_class    <> previous_quality_class
        OR previous_is_active IS NULL
        OR is_active         <> previous_is_active
      THEN 1
      ELSE 0
    END AS indicator
  FROM previous
),
streaks AS (
  SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    current_year,
    SUM(indicator) OVER (PARTITION BY actor_id,actor ORDER BY current_year) AS streak_number
  FROM change_indicator
),
grouped_ranges AS (
  SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    MIN(current_year) AS start_year,
    MAX(current_year) AS end_year,
    2021 AS current_year,
    streak_number
  FROM streaks
  GROUP BY
    actor_id,
    actor,
    quality_class,
    is_active,
    streak_number
),
ranges AS (
  SELECT
    actor_id,
    actor,
    quality_class,
    is_active,
    -- start on Jan 1 of the first year | use DATE_TRUNC if it is string
    MAKE_DATE(start_year, 1, 1)                AS start_date,
    -- end on Dec 31 of the last year
    MAKE_DATE(end_year, 12, 31)                 AS end_date,
    current_year
  FROM grouped_ranges
)
SELECT
  actor_id,
  actor,
 EXTRACT(YEAR FROM start_date) as start_date,
 EXTRACT(YEAR FROM end_date) as end_date,
  quality_class,
  is_active,
  current_year
FROM ranges
ORDER BY actor_id,actor,start_date,end_date
"""


def do_actor_history_scd_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)


# --- PostgreSQL Connection Configuration ---
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
TABLE_TO_READ = "postgres.public.actors" # Based on the SQL query "FROM postgres.public.actors"
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

    output_df = do_actor_history_scd_transformation(spark, initial_df)
    output_df.write.mode("overwrite").insertInto("actors_scd")

