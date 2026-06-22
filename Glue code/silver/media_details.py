"""
AWS Glue Job: Wistia Silver — media_details
=============================================
Reads today's bronze partition for media_details, applies transformations,
and overwrites today's silver partition.

Triggered by: Glue Workflow, after the bronze job completes.

Transformations:
  - Drop: thumbnail, subfolder, project, share_link, assets
  - Cast: created, updated, pipeline_ingested_at_utc -> TIMESTAMP
  - Partition: ingested_date (passed as job parameter or defaults to today)
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

# Default to today if not passed — should match the bronze job's run date
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
BRONZE_PATH = f"s3://{S3_BUCKET}/{S3_PREFIX}/bronze/media_details/ingested_date={TODAY}/"
SILVER_PATH = f"s3://{S3_BUCKET}/{S3_PREFIX}/silver/media_details/ingested_date={TODAY}/"

# ---------------------------------------------------------------------------
# Read bronze
# ---------------------------------------------------------------------------
logger.info("Reading bronze from: %s", BRONZE_PATH)
df = spark.read.parquet(BRONZE_PATH)
logger.info("Bronze record count: %d", df.count())

# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------

# 1. Drop nested/unwanted columns
COLS_TO_DROP = ["thumbnail", "subfolder", "project", "share_link", "assets"]
cols_to_drop_existing = [c for c in COLS_TO_DROP if c in df.columns]
df = df.drop(*cols_to_drop_existing)
logger.info("Dropped columns: %s", cols_to_drop_existing)

# 2. Cast timestamp fields
TIMESTAMP_COLS = ["created", "updated", "pipeline_ingested_at_utc"]
for col in TIMESTAMP_COLS:
    if col in df.columns:
        df = df.withColumn(col, F.to_timestamp(col))

# 3. Ensure boolean type for archived
if "archived" in df.columns:
    df = df.withColumn("archived", df["archived"].cast("boolean"))

# 4. Ensure correct numeric types
if "duration" in df.columns:
    df = df.withColumn("duration", df["duration"].cast("double"))
if "progress" in df.columns:
    df = df.withColumn("progress", df["progress"].cast("double"))

# 5. Derive platform indicator columns from name
if "name" in df.columns:
    df = df.withColumn("facebook", F.when(F.lower(F.col("name")).contains("facebook"), 1).otherwise(0))
    df = df.withColumn("youtube",  F.when(F.lower(F.col("name")).contains("youtube"),  1).otherwise(0))

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
logger.info("Silver media_details job completed successfully.")