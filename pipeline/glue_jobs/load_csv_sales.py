import boto3
import sys
import base64
import json
import logging
import datetime as dt
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
    sys.argv, ["JOB_NAME", "REGION", "PG_SECRECT_NAME", "GLUE_DATABASE_NAME", "ENV", "S3_BUCKET_NAME_DATA"]
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
s3_bucket_name_data = args["S3_BUCKET_NAME_DATA"]

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

# Read Other Tables
try:
    events = spark.table(f"{glue_db_name}.events")
    resellers = spark.table(f"{glue_db_name}.resellers")    
    partnerships = spark.table(f"{glue_db_name}.partnerships")
    customers = spark.table(f"{glue_db_name}.customers")
    organizers = spark.table(f"{glue_db_name}.organizers")    
except Exception as e:
    log.error("ERROR: Failed to load Glue tables.", exc_info=True)
    sys.exit(1)  # Exit script if tables cannot be loaded

table_name = f"{glue_db_name}.sales_csv"

# Retrieve partition information
try:
    partitions_df = spark.sql(f"SHOW PARTITIONS {table_name}")
    if partitions_df.count() > 0:
        latest_partition = partitions_df.orderBy("partition", ascending=False).first()[0]
        latest_partition_value = latest_partition.split("=")[-1]
        target_date = (dt.datetime.strptime(latest_partition_value, '%Y-%m-%d') + dt.timedelta(days=1)).strftime('%m%d%Y')
        log.info(f"Latest partition found: {latest_partition_value}")
    else:
        target_date = '*'
        log.info("No partitions found in table; using default value.")
except:
    target_date = '*'

# Data Transformations
try:
    s3_bucket_path = f"s3://{s3_bucket_name_data}/sales/"
    file_pattern = f"DailySales_{target_date}_*.csv"
    full_path = s3_bucket_path + file_pattern

    # Load all CSV files matching the pattern into one DataFrame
    sales_csv = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(full_path)
        
    from pyspark.sql.functions import input_file_name, regexp_extract

    # Add a column with the file name
    sales_csv = sales_csv.withColumn("source_file", input_file_name())

    # Extract the reseller_id using regexp_extract
    # The regex explanation:
    #   - "DailySales_" matches the literal prefix.
    #   - "\d{8}" matches the 8 digits for MMDDYYYY.
    #   - "_" matches the underscore.
    #   - "([^\.]+)" captures one or more characters that are not a dot (i.e., the reseller id).
    #   - "\.CSV" matches the literal ".CSV" extension.
    sales_csv = sales_csv.withColumn(
        "reseller_id",
        regexp_extract("source_file", r"DailySales_\d{8}_([^\.]+)\.csv", 1)
    )

    sales_csv = (
        sales_csv
        .withColumnRenamed("Transaction ID", "sale_id")
        .withColumnRenamed("Total Amount", "total_amount")
        .withColumnRenamed("Event Name", "event_name")
        .withColumnRenamed("Number of Purchased Tickets", "ticket_quantity")
        .withColumnRenamed("Created Date", "sale_date")    
        .withColumn("customer_name", F.concat(F.col("Customer First Name"), F.lit(" "), F.col("Customer Last Name")))
        .withColumn(
            "channel_name",
            F.when(
                F.col("Sales Channel")=="office", F.lit("on-the-site")
            ).otherwise(
                F.col("Sales Channel")
            )
        )
        .join(
            partnerships,
            on=["reseller_id"],
            how='left'
        )
        .join(
            resellers.withColumnRenamed("location", "reseller_location"),
            on="reseller_id",
            how='left'
        )
        .join(
            organizers,
            on=["organizer_id"],
            how='left'
        )    
        .join(
            events,
            on=["event_name"],
            how='left'
        )
        .drop(
            "Sales Channel",
            "Customer First Name",
            "Customer Last Name",
            "Office Location",
            "event_name",
            "organizer_name"
        )
        .withColumn("seller_type",F.lit("reseller"))
    )

except Exception as e:
    log.error("ERROR: Failed to transform sales data.", exc_info=True)
    sys.exit(1)  # Exit script if transformations fail

# Write Data to Parquet in S3
try:
    if not spark.catalog.tableExists(table_name):
        # Table does not exist; create it using saveAsTable.
        sales_csv.write.mode("overwrite") \
            .partitionBy("sale_date") \
            .format("parquet") \
            .saveAsTable(table_name)
        log.info(f"Table {table_name} created successfully using saveAsTable.")
    else:
        # Table exists; update only the affected partitions using dynamic partition overwrite.
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        sales_csv.write.mode("overwrite").insertInto(table_name)
        log.info(f"Table {table_name} partitions updated successfully using insertInto.")
except Exception as e:
    log.error("ERROR: Failed to write Parquet data to S3.", exc_info=True)
    sys.exit(1)  # Exit script if writing fails

# Commit Glue Job
job.commit()
log.info("Glue job committed successfully.")