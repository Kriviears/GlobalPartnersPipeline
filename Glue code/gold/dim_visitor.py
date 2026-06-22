"""
AWS Glue Job: Wistia Gold — dim_visitor
=========================================
Builds a visitor dimension table directly from silver events.
No join required — all columns come from silver events.

Triggered by: Glue Workflow, after all silver jobs complete.

Output columns:
  - visitor_id : silver events.visitor_key (deduplicated)
  - ip         : silver events.ip
  - country    : silver events.country
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

logger.info("Processing gold dim_visitor for ingested_date: %s", TODAY)

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
SILVER_EVENTS_PATH = f"s3://{S3_BUCKET}/{S3_PREFIX}/silver/events/"
GOLD_PATH          = f"s3://{S3_BUCKET}/{S3_PREFIX}/gold/dim_visitor/"

# ---------------------------------------------------------------------------
# Read silver events
# ---------------------------------------------------------------------------
logger.info("Reading silver events from: %s", SILVER_EVENTS_PATH)
events_df = spark.read.parquet(SILVER_EVENTS_PATH)
logger.info("Silver events record count: %d", events_df.count())

# ---------------------------------------------------------------------------
# Build dim_visitor — one row per visitor_id
# ---------------------------------------------------------------------------
dim_visitor_df = (
    events_df
    .filter(F.col("visitor_key").isNotNull())
    .groupBy("visitor_key")
    .agg(
        F.first("ip",      ignorenulls=True).alias("ip"),
        F.first("country", ignorenulls=True).alias("country"),
    )
    .withColumnRenamed("visitor_key", "visitor_id")
)

logger.info("Gold dim_visitor schema:")
dim_visitor_df.printSchema()
logger.info("Gold dim_visitor record count: %d", dim_visitor_df.count())

# ---------------------------------------------------------------------------
# Write gold — overwrite today's partition only
# ---------------------------------------------------------------------------
logger.info("Writing gold dim_visitor to: %s", GOLD_PATH)
(
    dim_visitor_df.write
                  .mode("overwrite")
                  .parquet(GOLD_PATH)
)
logger.info("Gold dim_visitor write complete.")

job.commit()
logger.info("Gold dim_visitor job completed successfully.")