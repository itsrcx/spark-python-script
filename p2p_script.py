# jdbc file should be in same dir as script current jdbc:  "postgresql-42.6.0.jar" <change in script as yours>

# bash cmd: python your_script.py -f /path/to/your/parquet/file -t your_table_name -H your_host -p your_port -d your_database -U your_username 
 
### SCRIPT TO LOAD PARAQUET DATA TO POSTGRES ###
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import argparse
import getpass
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.realpath(__file__))

# command line arguments
parser = argparse.ArgumentParser(description="Load Parquet file into PostgreSQL table.")
parser.add_argument("-H", "--host", default="localhost", help="PostgreSQL server host (default: localhost)")
parser.add_argument("-p", "--port", type=int, default=5432, help="PostgreSQL server port (default: 5432)")
parser.add_argument("-d", "--database", default="postgres", help="PostgreSQL database name (default: postgres)")
parser.add_argument("-f", "--file", required=True, help="Path to Parquet file (required)")
parser.add_argument("-t", "--table", required=True, help="Table name in PostgreSQL (required)")
parser.add_argument("-U", "--user", default="postgres", help="PostgreSQL user (default: postgres)")

args, unknown_args = parser.parse_known_args()
if not args.file or not args.table or not args.user:
    print("Required arguments are missing. Use -h or --help for usage information.")
else:
    # Prompt for password securely
    password = getpass.getpass(prompt="Enter PostgreSQL password: ")  # password prompt

args = parser.parse_args()

spark = SparkSession.builder \
    .appName("PostgresIntegration") \
    .config("spark.jars", os.path.join(script_dir, "postgresql-42.6.0.jar")) \
    .getOrCreate()

# reading parquet file to df
parquet_df = spark.read.parquet(args.file)

# lowering column name (optional) >>postgres turns automatically it to lowercase<< 
lowercase_columns = [col(column).alias(column.lower()) for column in parquet_df.columns]
lower_parquet = parquet_df.select(*lowercase_columns)

# Set up PostgreSQL connection properties
postgres_url = f"jdbc:postgresql://{args.host}:{args.port}/{args.database}"
properties = {
    "user": args.user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

lower_parquet.write \
    .mode("overwrite") \
    .jdbc(url=postgres_url, table=args.table, properties=properties)

# Stop the Spark session
spark.stop()

print("Script completed successfully! Data loaded")



      

