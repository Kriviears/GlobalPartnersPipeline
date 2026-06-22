"""
AWS Glue Job: Wistia Gold — fact_media
========================================
Reads today's silver partition for media_stats and writes the fact_media
gold table. No joins required — all columns come from silver_media_stats.

Triggered by: Glue Workflow, after all silver jobs complete.

Output columns:
  - media_id     : silver_media_stats.media_id
  - load_count   : silver_media_stats.load_count
  - play_count   : silver_media_stats.play_count
  - play_rate    : silver_media_stats.play_rate
  - hours_watched: silver_media_stats.hours_watched
  - engagement   : silver_media_stats.engagement
  - visitors     : silver_media_stats.visitors
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

logger.info("Processing gold fact_media for ingested_date: %s", TODAY)

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
SILVER_MEDIA_STATS_PATH = f"s3://{S3_BUCKET}/{S3_PREFIX}/silver/media_stats/ingested_date={TODAY}/"
GOLD_PATH               = f"s3://{S3_BUCKET}/{S3_PREFIX}/gold/fact_media/ingested_date={TODAY}/"

# ---------------------------------------------------------------------------
# Read silver
# ---------------------------------------------------------------------------
logger.info("Reading silver media_stats from: %s", SILVER_MEDIA_STATS_PATH)
df = spark.read.parquet(SILVER_MEDIA_STATS_PATH)
logger.info("Silver media_stats record count: %d", df.count())

# ---------------------------------------------------------------------------
# Select fact columns
# ---------------------------------------------------------------------------
fact_media_df = df.select(
    F.col("media_id"),
    F.col("load_count"),
    F.col("play_count"),
    F.col("play_rate"),
    F.col("hours_watched"),
    F.col("engagement"),
    F.col("visitors"),
    F.col("pipeline_ingested_at_utc"),
)

logger.info("Gold fact_media schema:")
fact_media_df.printSchema()
logger.info("Gold fact_media record count: %d", fact_media_df.count())

# ---------------------------------------------------------------------------
# Write gold — overwrite today's partition only
# ---------------------------------------------------------------------------
logger.info("Writing gold fact_media to: %s", GOLD_PATH)
(
    fact_media_df.write
                 .mode("overwrite")
                 .parquet(GOLD_PATH)
)
logger.info("Gold fact_media write complete.")

job.commit()
logger.info("Gold fact_media job completed successfully.")