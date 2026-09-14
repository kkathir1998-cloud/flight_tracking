from pyspark.sql.functions import current_timestamp,col
source_path="/Volumes/workspace/flight_tracking/incoming"
schema_path="/Volumes/workspace/flight_tracking/schema/bronze"
checkpoint_path="/Volumes/workspace/flight_tracking/checkpoints/bronze"

bronze_df=spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option("cloudFiles.schemaLocation", schema_path) \
  .option("cloudFiles.schemaEvolutionMode", "rescue") \
  .option("recuedDataColumn", "rescued_data") \
  .load(source_path)\
  .withColumn("ingestion_time", current_timestamp())\
  .withColumn("path",col("_metadata.file_path"))
bronze_query=bronze_df.writeStream \
  .format("delta") \
  .option("checkpointLocation", checkpoint_path) \
  .trigger(availableNow=True) \
  .toTable("workspace.flight_tracking.bronze")

  
