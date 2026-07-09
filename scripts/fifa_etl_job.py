import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# =====================================================================
# GLOBAL CONFIGURATION (Parameterized for public repository security)
# =====================================================================
# Replace these strings with your actual S3 bucket names when running in AWS Glue
RAW_INPUT_BUCKET = "YOUR-RAW-INPUT-BUCKET-NAME"       # e.g., "herezlovic-bucket-01"
OPTIMIZED_OUTPUT_BUCKET = "YOUR-OUTPUT-BUCKET-NAME"   # e.g., "herezlovic-bucket-01"

RAW_S3_PATH = f"s3://{RAW_INPUT_BUCKET}/archive/"
OUTPUT_S3_PATH = f"s3://{OPTIMIZED_OUTPUT_BUCKET}/parquet_optimized/"

# =====================================================================
# INITIALIZE GLUE & SPARK CONTEXT
# =====================================================================
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =====================================================================
# ETL PIPELINE EXECUTION
# =====================================================================

# 1. High-speed parallel extraction of raw CSV records
datasource = spark.read.option("header", "true") \
                       .option("inferSchema", "true") \
                       .csv(RAW_S3_PATH)

# 2. Convert to compressed columnar format and partition out horizontally by team
datasource.write.mode("overwrite") \
                .partitionBy("team") \
                .parquet(OUTPUT_S3_PATH)

# Commit job status to Glue
job.commit()
