import boto3
import sys
import base64
import json
import logging
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# Get job arguments
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "REGION", "PG_SECRECT_NAME", "GLUE_DATABASE_NAME", "ENV"]
)

# Initialize Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
region_name = args["REGION"]
secret_name = args["PG_SECRECT_NAME"]
glue_db_name = args["GLUE_DATABASE_NAME"]

# Set dynamic partition overwrite (used later when table exists)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

def get_secret(secret_name):
    """Retrieves the secret from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        if "SecretString" in get_secret_value_response:
            return json.loads(get_secret_value_response["SecretString"])  # Return secret as dictionary
        else:
            return base64.b64decode(get_secret_value_response["SecretBinary"])

    except Exception as e:
        log.error(f"Error retrieving secret {secret_name}: {str(e)}", exc_info=True)
        raise

# PostgreSQL Connection Details
try:
    secret = get_secret(secret_name)
    PG_HOST = secret["host"]
    PG_PORT = secret["port"]
    PG_DATABASE = secret["dbname"]
    PG_USER = secret["username"]
    PG_PASSWORD = secret["password"]
    JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
except Exception as e:
    log.error("Error retrieving secret or setting up PostgreSQL connection details.", exc_info=True)
    sys.exit(1)  # Exit script if connection details can't be retrieved

# Read Table from PostgreSQL
try:
    sales = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "public.sales")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
except Exception as e:
    log.error("ERROR: Failed to read table from PostgreSQL.", exc_info=True)
    sys.exit(1)  # Exit script if sales data cannot be loaded

# Read Other Tables
try:
    events = spark.table(f"{glue_db_name}.events")
    resellers = spark.table(f"{glue_db_name}.resellers")
    sales_channels = spark.table(f"{glue_db_name}.sales_channels")
    partnerships = spark.table(f"{glue_db_name}.partnerships")
    customers = spark.table(f"{glue_db_name}.customers")
    sales_csv = spark.table(f"{glue_db_name}.sales_csv")
    
except Exception as e:
    log.error("ERROR: Failed to load Glue tables.", exc_info=True)
    sys.exit(1)  # Exit script if tables cannot be loaded

# Determine the latest partition value.
# If the target table exists, we query its partitions; otherwise, we use a default value.
table_name = f"{glue_db_name}.sales"

if spark.catalog.tableExists(table_name):
    try:
        partitions_df = spark.sql(f"SHOW PARTITIONS {table_name}")
        if partitions_df.count() > 0:
            latest_partition = partitions_df.orderBy("partition", ascending=False).first()[0]
            latest_partition_value = latest_partition.split("=")[-1]
            log.info(f"Latest partition found: {latest_partition_value}")
        else:
            latest_partition_value = '0001-01-01'
            log.info("No partitions found in table; using default value.")
    except Exception as e:
        log.error("ERROR: Failed to retrieve partitions.", exc_info=True)
        latest_partition_value = '0001-01-01'
else:
    latest_partition_value = '0001-01-01'
    log.info("Table does not exist; using default partition value.")

# Data Transformations
try:
    # Use '>=' so that if new data for the latest partition arrive, they are included.
    sales = (
        sales
        .filter(F.col("sale_date") > latest_partition_value)
        .withColumn(
            "seller_type",
            F.when(F.col("reseller_id").isNull(), F.lit("organizer")).otherwise(F.lit("reseller"))
        )
        .join(
            events.select('event_id', 'event_type'),
            on="event_id",
            how='left'
        )
        .join(
            resellers.withColumnRenamed("location", "reseller_location"),
            on="reseller_id",
            how='left'
        )
        .join(
            sales_channels,
            on="sales_channel_id",
            how='left'
        )
        .join(
            partnerships,
            on=["organizer_id", "reseller_id"],
            how='left'
        )        
        .withColumn("sale_date", F.to_date(F.col("sale_date")))
        .join(
            customers,
            on=["customer_id"],
            how='left'
        )
    )

    sales = (
        sales
        .withColumn("data_source", F.lit("B2B platform"))
        .unionByName(
            sales_csv.withColumn("data_source", F.lit("3rd party")),
            allowMissingColumns=True
        )
    )
    
except Exception as e:
    log.error("ERROR: Failed to transform sales data.", exc_info=True)
    sys.exit(1)  # Exit script if transformations fail

# Write Data to Parquet in S3
try:
    if not spark.catalog.tableExists(table_name):
        # Table does not exist; create it using saveAsTable.
        sales.write.mode("overwrite") \
            .partitionBy("sale_date") \
            .format("parquet") \
            .saveAsTable(table_name)
        log.info(f"Table {table_name} created successfully using saveAsTable.")
    else:
        # Table exists; update only the affected partitions using dynamic partition overwrite.
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        sales.write.mode("overwrite").insertInto(table_name)
        log.info(f"Table {table_name} partitions updated successfully using insertInto.")
except Exception as e:
    log.error("ERROR: Failed to write Parquet data to S3.", exc_info=True)
    sys.exit(1)  # Exit script if writing fails

# Commit Glue Job
job.commit()
log.info("Glue job committed successfully.")
