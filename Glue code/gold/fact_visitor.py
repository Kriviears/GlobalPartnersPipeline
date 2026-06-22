"""
AWS Glue Job: Wistia Gold — fact_visitor
==========================================
Builds a visitor fact table by joining silver events and silver visitors
on visitor_key.

Triggered by: Glue Workflow, after all silver jobs complete.

Output columns:
  - visitor_id    : silver events.visitor_key
  - event_id      : silver events.event_key
  - percent_viewed: silver events.percent_viewed
  - load_count    : silver visitors.load_count
  - play_count    : silver visitors.play_count
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

logger.info("Processing gold fact_visitor for ingested_date: %s", TODAY)

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
SILVER_EVENTS_PATH   = f"s3://{S3_BUCKET}/{S3_PREFIX}/silver/events/ingested_date={TODAY}/"
SILVER_VISITORS_PATH = f"s3://{S3_BUCKET}/{S3_PREFIX}/silver/visitors/ingested_date={TODAY}/"
GOLD_PATH            = f"s3://{S3_BUCKET}/{S3_PREFIX}/gold/fact_visitor/ingested_date={TODAY}/"

# ---------------------------------------------------------------------------
# Read silver
# ---------------------------------------------------------------------------
logger.info("Reading silver events from: %s", SILVER_EVENTS_PATH)
events_df = spark.read.parquet(SILVER_EVENTS_PATH)
logger.info("Silver events record count: %d", events_df.count())

logger.info("Reading silver visitors from: %s", SILVER_VISITORS_PATH)
visitors_df = spark.read.parquet(SILVER_VISITORS_PATH)
logger.info("Silver visitors record count: %d", visitors_df.count())

# ---------------------------------------------------------------------------
# Select only what we need from each table
# ---------------------------------------------------------------------------
events_slim = events_df.select(
    F.col("visitor_key"),
    F.col("event_key"),
    F.col("percent_viewed"),
    F.col("pipeline_ingested_at_utc")
)

visitors_slim = visitors_df.select(
    F.col("visitor_key"),
    F.col("load_count"),
    F.col("play_count"),
)

# ---------------------------------------------------------------------------
# Join events -> visitors on visitor_key
# ---------------------------------------------------------------------------
fact_visitor_df = (
    events_slim
    .join(visitors_slim, on="visitor_key", how="left")
    .select(
        F.col("visitor_key").alias("visitor_id"),
        F.col("event_key").alias("event_id"),
        F.col("percent_viewed"),
        F.col("load_count"),
        F.col("play_count"),
        F.col("pipeline_ingested_at_utc"),
    )
)

logger.info("Gold fact_visitor schema:")
fact_visitor_df.printSchema()
logger.info("Gold fact_visitor record count: %d", fact_visitor_df.count())

# ---------------------------------------------------------------------------
# Write gold — overwrite today's partition only
# ---------------------------------------------------------------------------
logger.info("Writing gold fact_visitor to: %s", GOLD_PATH)
(
    fact_visitor_df.write
                   .mode("overwrite")
                   .parquet(GOLD_PATH)
)
logger.info("Gold fact_visitor write complete.")

job.commit()
logger.info("Gold fact_visitor job completed successfully.")