from pyspark.sql.functions import (
    col,
    from_json,
    explode
)
from pyspark.sql.types import ArrayType, StringType

# =====================================================
# Configuration
# =====================================================

checkpoint = "/Volumes/workspace/flight_tracking/checkpoints/silver"
checkpoint2 = "/Volumes/workspace/flight_tracking/checkpoints/silver_quar"

target_table = "workspace.flight_tracking.silver"
target_table2 = "workspace.flight_tracking.silver_quar"

# =====================================================
# OpenSky Schema
# =====================================================

parsed_schema = ArrayType(ArrayType(StringType()))

# =====================================================
# Read Bronze Stream
# =====================================================

df = spark.readStream.table("workspace.flight_tracking.bronze")

# =====================================================
# Parse JSON
# =====================================================

parsed_df = df.withColumn(
    "parsed_states",
    from_json(col("states"), parsed_schema)
)

# =====================================================
# Explode Aircraft Records
# =====================================================

explode_df = parsed_df.withColumn(
    "aircraft",
    explode(col("parsed_states"))
)

# =====================================================
# Flatten Aircraft Data
# =====================================================

flight_df = explode_df.select(
    col("aircraft")[0].alias("icao24"),
    col("aircraft")[1].alias("callsign"),
    col("aircraft")[2].alias("origin_country"),
    col("aircraft")[3].cast("long").alias("time_position"),
    col("aircraft")[4].cast("long").alias("last_contact"),
    col("aircraft")[5].cast("double").alias("longitude"),
    col("aircraft")[6].cast("double").alias("latitude"),
    col("aircraft")[7].cast("double").alias("baro_altitude"),
    col("aircraft")[8].cast("boolean").alias("on_ground"),
    col("aircraft")[9].cast("double").alias("velocity"),
    col("aircraft")[10].cast("double").alias("true_track"),
    col("aircraft")[11].cast("double").alias("vertical_rate"),
    col("aircraft")[12].alias("sensors"),
    col("aircraft")[13].cast("double").alias("geo_altitude"),
    col("aircraft")[14].alias("squawk"),
    col("aircraft")[15].cast("boolean").alias("spi"),
    col("aircraft")[16].cast("int").alias("position_source"),
    col("path"),
    col("ingestion_time")
)

# =====================================================
# Valid Records (Silver)
# =====================================================

silver_df = flight_df.filter(
    col("icao24").isNotNull()
    & col("origin_country").isNotNull()
    & col("longitude").isNotNull()
    & col("latitude").isNotNull()
)
silver_df=silver_df..withWatermark("ingestion_time", "1 hour").dropDuplicates(["icao24","ingestion_time"])

# =====================================================
# Quarantine Records
# =====================================================

silver_df_quar = flight_df.filter(
    col("icao24").isNull()
    | col("origin_country").isNull()
    | col("longitude").isNull()
    | col("latitude").isNull()
)

# =====================================================
# Write Silver Table
# =====================================================

silver_query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(target_table)
)

# =====================================================
# Write Silver Quarantine Table
# =====================================================

silver_quar_query = (
    silver_df_quar.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint2)
    .trigger(availableNow=True)
    .toTable(target_table2)
)

# =====================================================
# Wait Until Both Streams Finish
# =====================================================

silver_query.awaitTermination()
silver_quar_query.awaitTermination()

print("Silver and Silver Quarantine loads completed successfully.")
