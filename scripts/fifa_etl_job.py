import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Parameters for security
RAW_INPUT_BUCKET = "YOUR-RAW-INPUT-BUCKET-NAME"
OPTIMIZED_OUTPUT_BUCKET = "YOUR-OUTPUT-BUCKET-NAME"

RAW_S3_PATH = f"s3://{RAW_INPUT_BUCKET}/archive/"
OUTPUT_S3_PATH = f"s3://{OPTIMIZED_OUTPUT_BUCKET}/parquet_optimized/"

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Stateful Extraction: Glue tracks S3 metadata behind the scenes
datasource = spark.read.option("header", "true") \
                       .option("inferSchema", "true") \
                       .csv(RAW_S3_PATH)

# 2. Stateful Load: Use "append" so new data is added without erasing old records
datasource.write.mode("append") \
                .partitionBy("team") \
                .parquet(OUTPUT_S3_PATH)

job.commit()
