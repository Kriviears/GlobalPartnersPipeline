"""
AWS Glue Job: Wistia Gold — media_details
==========================================
Reads today's silver partition for media_details and events, joins them
to enrich media_details with the media URL, and overwrites today's gold partition.

Triggered by: Glue Workflow, after all silver jobs complete.

Transformations:
  - Rename: hashed_id -> media_id
  - Join:   silver events (today) to pull a single distinct media_url per media_id
"""

import sys
import logging
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

logger.info("Processing gold for ingested_date: %s", TODAY)

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
SILVER_MEDIA_PATH  = f"s3://{S3_BUCKET}/{S3_PREFIX}/silver/media_details/ingested_date={TODAY}/"
SILVER_EVENTS_PATH = f"s3://{S3_BUCKET}/{S3_PREFIX}/silver/events/ingested_date={TODAY}/"
GOLD_PATH          = f"s3://{S3_BUCKET}/{S3_PREFIX}/gold/media_details/ingested_date={TODAY}/"

# ---------------------------------------------------------------------------
# Read silver
# ---------------------------------------------------------------------------
logger.info("Reading silver media_details from: %s", SILVER_MEDIA_PATH)
media_df = spark.read.parquet(SILVER_MEDIA_PATH)
logger.info("Silver media_details record count: %d", media_df.count())

logger.info("Reading silver events from: %s", SILVER_EVENTS_PATH)
events_df = spark.read.parquet(SILVER_EVENTS_PATH)
logger.info("Silver events record count: %d", events_df.count())

# ---------------------------------------------------------------------------
# Deduplicate events to one media_url per media_id
# ---------------------------------------------------------------------------
# Take the first non-null media_url per media_id
url_df = (
    events_df
    .filter(F.col("media_url").isNotNull())
    .groupBy("media_id")
    .agg(F.first("media_url", ignorenulls=True).alias("url"))
)

# ---------------------------------------------------------------------------
# Transformations on media_details
# ---------------------------------------------------------------------------

# 1. Rename hashed_id -> media_id
media_df = media_df.withColumnRenamed("hashed_id", "media_id")

# 2. Join media_url from events
media_df = media_df.join(url_df, on="media_id", how="left")

# 3. Default url to wistia media page if null or empty
media_df = media_df.withColumn(
    "url",
    F.when(
        F.col("url").isNull() | (F.trim(F.col("url")) == ""),
        F.concat(F.lit("https://chrisgarzon19.wistia.com/medias/"), F.col("media_id"))
    ).otherwise(F.col("url"))
)

logger.info("Gold schema:")
media_df.printSchema()
logger.info("Gold record count: %d", media_df.count())

# ---------------------------------------------------------------------------
# Write gold — overwrite today's partition only
# ---------------------------------------------------------------------------
logger.info("Writing gold to: %s", GOLD_PATH)
(
    media_df.write
            .mode("overwrite")
            .parquet(GOLD_PATH)
)
logger.info("Gold write complete.")

job.commit()
logger.info("Gold media_details job completed successfully.")