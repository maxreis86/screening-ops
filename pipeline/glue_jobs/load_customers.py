import boto3
import sys
import base64
import json
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

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


def get_secret(secret_name):
    """Retrieves the secret from Secrets Manager."""

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        if e.response["Error"]["Code"] == "DecryptionFailureException":
            # The secret can't be decrypted using the provided KMS key.
            raise e
        elif e.response["Error"]["Code"] == "InternalServiceErrorException":
            # An error occurred inside Secrets Manager.
            raise e
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            # You provided an invalid value for a parameter.
            raise e
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            # You provided a bad request.
            raise e
        elif e.response["Error"]["Code"] == "ResourceNotFoundException":
            # The requested secret was not found.
            raise e
    else:
        # Decrypted secrets are available directly.
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
            return json.loads(secret)  # Return secret as a dictionary
        else:
            decoded_binary_secret = base64.b64decode(
                get_secret_value_response["SecretBinary"]
            )
            return decoded_binary_secret


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
    print(f"Error retrieving secret or connecting to database: {e}")


try:
    # Read Table from PostgreSQL
    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "public.customers")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

except Exception as e:
    print(f"ERROR: Failed to read table from PostgreSQL. Exception: {str(e)}")

# Write Data to Parquet in S3
try:

    df.write.mode("overwrite").format("parquet").saveAsTable(
        f"{glue_db_name}.customers"
    )

except Exception as e:
    print(f" ERROR: Failed to write Parquet data to S3. Exception: {str(e)}")

# Commit Glue Job
job.commit()
