import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, current_timestamp, current_date, to_timestamp
from pyspark.sql.types import StructType


# ============================================================================
# KINESIS READER
# ============================================================================

class KinesisReader:
    """
    Reads from Kinesis stream and parses JSON
    """
    
    def __init__(self, spark_session, stream_name: str, job_name: str, region: str = "us-west-2"):
        self.spark = spark_session
        self.stream_name = stream_name
        self.region = region
        self.logger = logging.getLogger(f"{__name__}.{job_name}.KinesisReader")
    
    def read_stream(self, schema: StructType, starting_position: str = "LATEST") -> DataFrame:
        """
        Read from Kinesis stream and parse JSON
        
        Args:
            schema: PySpark schema for JSON parsing
            starting_position: LATEST or TRIM_HORIZON
        
        Returns:
            Parsed DataFrame with columns from schema + metadata columns
        """
        self.logger.info(f"Configuring stream reader for: {self.stream_name}")
        
        # Read from Kinesis stream
        kinesis_df = self.spark.readStream \
            .format("kinesis") \
            .option("streamName", self.stream_name) \
            .option("region", self.region) \
            .option("endpointUrl", f"https://kinesis.{self.region}.amazonaws.com") \
            .option("startingPosition", starting_position) \
            .load()
        
        # Parse JSON and add metadata
        parsed_df = kinesis_df.select(
            from_json(col("data").cast("string"), schema).alias("event")
        ).select("event.*") \
         .withColumn("ingestion_timestamp", current_timestamp()) \
         .withColumn("ingestion_date", current_date())
        
        # Convert event_timestamp to timestamp type
        if "event_timestamp" in parsed_df.columns:
            parsed_df = parsed_df.withColumn(
                "event_timestamp", 
                to_timestamp(col("event_timestamp"))
            )
        
        self.logger.info(f"Stream '{self.stream_name}' configured with {len(schema.fields)} fields")
        return parsed_df


# ============================================================================
# ICEBERG WRITER
# ============================================================================

class IcebergWriter:
    """
    Writes streaming data to Iceberg tables
    """
    
    def __init__(self, spark_session, table_name: str, checkpoint_location: str, job_name: str):
        self.spark = spark_session
        self.table_name = table_name
        self.checkpoint_location = checkpoint_location
        self.logger = logging.getLogger(f"{__name__}.{job_name}.IcebergWriter")
        self.table_created = False
    
    def write_batch(self, batch_df: DataFrame, batch_id: int):
        """
        Write a single batch to Iceberg table
        
        Args:
            batch_df: DataFrame to write
            batch_id: Batch identifier
        """
        try:
            if batch_df.isEmpty():
                self.logger.info(f"Batch {batch_id}: Empty batch, skipping")
                return
            
            self.logger.info(f"Batch {batch_id}: Writing to {self.table_name}")
            deduped = batch_df.dropDuplicates(["event_id"])
            
            # On first batch, create table if it doesn't exist
            if not self.table_created:
                try:
                    self.spark.sql(f"DESCRIBE TABLE glue_catalog.{self.table_name}")
                    self.logger.info(f"Table {self.table_name} already exists")
                    self.table_created = True
                except Exception:
                    self.logger.info(f"Creating table {self.table_name}")
                    deduped.writeTo(f"glue_catalog.{self.table_name}") \
                        .tableProperty("write.parquet.compression-codec", "zstd") \
                        .create()
                    self.table_created = True
                    return
            
            # Append to existing table
            deduped.writeTo(f"glue_catalog.{self.table_name}").append()
        
        except Exception as e:
            self.logger.error(f"Batch {batch_id} failed: {str(e)}", exc_info=True)
            raise
    
    def start_streaming_write(self, stream_df: DataFrame):
        """
        Start the streaming write query
        
        Args:
            stream_df: Source streaming DataFrame
        
        Returns:
            StreamingQuery object
        """
        self.logger.info(f"Starting streaming write to: {self.table_name}")
        self.logger.info(f"Checkpoint location: {self.checkpoint_location}")
        
        query = stream_df.writeStream \
            .foreachBatch(self.write_batch) \
            .option("checkpointLocation", self.checkpoint_location) \
            .start()
        
        self.logger.info(f"Streaming query started with ID: {query.id}")
        return query


# ============================================================================
# COMPLETE JOB ORCHESTRATOR
# ============================================================================

class KinesisToIcebergJob:
    """
    Orchestrates Kinesis stream to Iceberg table ingestion
    """
    
    def __init__(
        self,
        spark_session,
        glue_context,
        job_name: str,
        stream_name: str,
        table_name: str,
        schema: StructType,
        checkpoint_location: str,
        region: str = "us-west-2",
        starting_position: str = "LATEST"
    ):
        self.spark = spark_session
        self.glue_context = glue_context
        self.job_name = job_name
        self.stream_name = stream_name
        self.table_name = table_name
        self.schema = schema
        self.checkpoint_location = checkpoint_location
        self.region = region
        self.starting_position = starting_position
        self.logger = logging.getLogger(f"{__name__}.{job_name}")
        
        # Initialize components
        self.reader = KinesisReader(spark_session, stream_name, job_name, region)
        self.writer = IcebergWriter(spark_session, table_name, checkpoint_location, job_name)
    
    def run(self):
        """Execute the streaming job"""
        self.logger.info("=" * 70)
        self.logger.info("Starting Kinesis to Iceberg Streaming Job")
        self.logger.info(f"Job: {self.job_name}")
        self.logger.info(f"Stream: {self.stream_name}")
        self.logger.info(f"Table: {self.table_name}")
        self.logger.info(f"Region: {self.region}")
        self.logger.info("=" * 70)
        
        try:
            # Read from Kinesis
            stream_df = self.reader.read_stream(self.schema, self.starting_position)
            
            # Start streaming write to Iceberg
            query = self.writer.start_streaming_write(stream_df)
            
            # Wait for termination (runs until stopped)
            self.logger.info("Streaming job running")
            query.awaitTermination()
            
        except Exception as e:
            self.logger.error(f"Job failed: {str(e)}", exc_info=True)
            raise
        finally:
            self.logger.info("Streaming job stopped")
