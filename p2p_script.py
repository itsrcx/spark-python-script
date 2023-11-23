# jdbc file should be in same dir as script current jdbc:  "postgresql-42.6.0.jar" <change in script as yours>

# bash cmd: python your_script.py -f /path/to/your/parquet/file -t your_table_name -H your_host -p your_port -d your_database -U your_username 
 
### SCRIPT TO LOAD PARAQUET DATA TO POSTGRES ###
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import argparse
import getpass
import os
import time

# Get the directory of the current script
script_dir = os.path.dirname(os.path.realpath(__file__))

# command line arguments
parser = argparse.ArgumentParser(description="Load Parquet file into PostgreSQL table.")
parser.add_argument("-H", "--host", default="localhost", help="PostgreSQL server host (default: localhost)")
parser.add_argument("-p", "--port", type=int, default=5432, help="PostgreSQL server port (default: 5432)")
parser.add_argument("-d", "--database", default="postgres", help="PostgreSQL database name (default: postgres)")
parser.add_argument("-f", "--file", required=True, help="Path to Parquet file (required)")
parser.add_argument("-t", "--table", required=True, help="Table name to create table in PostgreSQL (required)")
parser.add_argument("-U", "--user", default="postgres", help="PostgreSQL user (default: postgres)")

args, unknown_args = parser.parse_known_args()
if not args.file or not args.table or not args.user:
    print("Required arguments are missing. Use -h or --help for usage information.")
else:
    # Prompt for password securely
    password = getpass.getpass(prompt="Enter PostgreSQL password: ")  # password prompt

args = parser.parse_args()

spark_start_time = time.time()

spark = SparkSession.builder \
    .appName("PostgresIntegration") \
    .config("spark.jars", os.path.join(script_dir, "postgresql-42.6.0.jar")) \
    .getOrCreate()
    

# Check if the specified table already exists
existing_tables = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{args.host}:{args.port}/{args.database}") \
    .option("dbtable", "information_schema.tables") \
    .option("user", args.user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

table_exists = existing_tables.filter(col("table_name") == args.table).count() > 0

# If the table exists, print existing tables in the specified database
if table_exists:
    print(f"\nThe table '{args.table}' already exists in the '{args.database}' database.")
    print("Existing tables in the database:")
    existing_tables_list = existing_tables.filter(col("table_schema") == "public").select("table_name").collect()
    for table_row in existing_tables_list:
        print(table_row["table_name"])

    # Give the option to enter another table name
    new_table_name = input("\nEnter a new table name or press Enter to exit: ").strip()
    if new_table_name:
        args.table = new_table_name
    else:
        print("Exiting the script.")
        exit()

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

loading_start_time = time.time()

lower_parquet.write \
    .mode("overwrite") \
    .jdbc(url=postgres_url, table=args.table, properties=properties)
    
loading_end_time = time.time()

# Stop the Spark session
spark.stop()

spark_end_time = time.time()

spark_time = spark_end_time - spark_start_time
loading_time = loading_end_time - loading_start_time

print(f"Script completed successfully! Data loaded.\n\n[SparkTime: {round(spark_time,2)}s], [PgLoadingTime: {round(loading_time,2)}s]")
