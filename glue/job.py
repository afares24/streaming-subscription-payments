import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Module imports
from schemas import get_schema
from framework import KinesisToIcebergJob

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# GET JOB PARAMETERS
# ============================================================================

# Required parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'stream_name',          # e.g., 'subscription-payments'
    'table_name',           # e.g., 'netflix_raw.subscription_payments'
    'schema_name',          # e.g., 'subscription_payment'
    'checkpoint_location',  # e.g., 's3://bucket/checkpoints/payments/'
    'region',               # e.g., 'us-west-2'
    'starting_position'     # e.g., 'LATEST'
])

# ============================================================================
# INITIALIZE GLUE CONTEXT
# ============================================================================

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================================
# RUN THE JOB
# ============================================================================

logger.info(f"Retrieving schema for: {args['schema_name']}")
schema = get_schema(args['schema_name'])

logger.info(f"Starting streaming job: {args['JOB_NAME']}")
streaming_job = KinesisToIcebergJob(
    spark_session=spark,
    glue_context=glueContext,
    job_name=args['JOB_NAME'],
    stream_name=args['stream_name'],
    table_name=args['table_name'],
    schema=schema,
    checkpoint_location=args['checkpoint_location'],
    region=args['region'],
    starting_position=args['starting_position']
)

streaming_job.run()
job.commit()