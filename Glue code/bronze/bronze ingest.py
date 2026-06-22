import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Config ---
GLUE_CONNECTION = "Aurora connection"
S3_OUTPUT_BASE  = "s3://global-partner/bronze/"
CONNECTION_TYPE = "sqlserver"
SCHEMA_NAME     = "GlobalPartners" 


# --- Step 1: Discover tables dynamically from Aurora ---
logger.info(f"Querying information_schema for tables in schema: {SCHEMA_NAME}")


discovered_tables = ["date_dim", "order_items", "order_item_options"]

logger.info(f"Tables to extract:")
for i, table_name in enumerate(discovered_tables, 1):
    logger.info(f"  {i}. dbo.{table_name}")
logger.info(f"Total: {len(discovered_tables)} tables\n")



# --- Step 2: Extract each table ---
def extract_table(table_name, partition_col=None):
    full_table_name = f"dbo.{table_name}"
    logger.info(f"Extracting: {full_table_name}")

    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": full_table_name,
        "connectionName": GLUE_CONNECTION,
    }

    if partition_col:
        connection_options["hashfield"] = partition_col
        connection_options["hashpartitions"] = "10"

    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type=CONNECTION_TYPE,
        connection_options=connection_options,
        transformation_ctx=f"src_{table_name}"
    )

    record_count = dynamic_frame.count()
    logger.info(f"  → {record_count} records pulled from {full_table_name}")

    return dynamic_frame


def write_to_s3(dynamic_frame, table_name):
    output_path = f"{S3_OUTPUT_BASE}{table_name}/"
    logger.info(f"  → Writing to {output_path}")

    dynamic_frame.toDF().write \
        .mode("overwrite") \
        .parquet(output_path)


# --- Step 3: Loop through all discovered tables ---
failed_tables = []
successful_tables = []

for table_name in discovered_tables:
    try:
        dynamic_frame = extract_table(table_name)
        write_to_s3(dynamic_frame, table_name)
        successful_tables.append(table_name)
        logger.info(f"  ✓ {table_name} complete\n")
    except Exception as e:
        logger.error(f"  ✗ Failed to extract {table_name}: {str(e)}\n")
        failed_tables.append(table_name)


# --- Step 4: Summary ---
logger.info("=" * 40)
logger.info("Extraction Summary")
logger.info("=" * 40)
logger.info(f"Successful: {len(successful_tables)}/{len(discovered_tables)} tables")
for t in successful_tables:
    logger.info(f"  ✓ {t}")

if failed_tables:
    logger.warning(f"Failed: {len(failed_tables)}/{len(discovered_tables)} tables")
    for t in failed_tables:
        logger.warning(f"  ✗ {t}")
    raise Exception(f"Some tables failed to extract: {failed_tables}")

job.commit()