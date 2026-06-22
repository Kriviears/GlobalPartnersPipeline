"""
AWS Glue Job: Wistia Silver — events
======================================
Reads today's bronze partition for events, applies transformations,
and appends to today's silver partition.

Triggered by: Glue Workflow, after the bronze job completes.

Transformations:
  - Cast: received_at, pipeline_ingested_at_utc -> TIMESTAMP
"""

import sys
import logging
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Job parameters
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "s3_bucket",
        "s3_prefix",
    ],
)

S3_BUCKET = args["s3_bucket"]
S3_PREFIX = args["s3_prefix"].rstrip("/")

def _get_optional_arg(name: str, default: str) -> str:
    return dict(zip(sys.argv, sys.argv[1:])).get(f"--{name}", default)

TODAY = _get_optional_arg(
    "ingested_date",
    datetime.now(timezone.utc).strftime("%Y-%m-%d")
)

logger.info("Processing silver for ingested_date: %s", TODAY)

# ---------------------------------------------------------------------------
# Spark / Glue setup
# ---------------------------------------------------------------------------
sc       = SparkContext()
glue_ctx = GlueContext(sc)
spark    = glue_ctx.spark_session
job      = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BRONZE_PATH = f"s3://{S3_BUCKET}/{S3_PREFIX}/bronze/events/ingested_date={TODAY}/"
SILVER_PATH = f"s3://{S3_BUCKET}/{S3_PREFIX}/silver/events/ingested_date={TODAY}/"

# ---------------------------------------------------------------------------
# Read bronze
# ---------------------------------------------------------------------------
logger.info("Reading bronze from: %s", BRONZE_PATH)
df = spark.read.parquet(BRONZE_PATH)
logger.info("Bronze record count: %d", df.count())

# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------
df = df.withColumn("received_at",              F.to_timestamp("received_at"))
df = df.withColumn("pipeline_ingested_at_utc", F.to_timestamp("pipeline_ingested_at_utc"))

logger.info("Silver schema:")
df.printSchema()

# ---------------------------------------------------------------------------
# Write silver — overwrite today's partition only
# ---------------------------------------------------------------------------
logger.info("Writing silver to: %s", SILVER_PATH)
(
    df.write
      .mode("overwrite")
      .parquet(SILVER_PATH)
)
logger.info("Silver write complete. Records written: %d", df.count())

job.commit()
logger.info("Silver events job completed successfully.")