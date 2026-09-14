from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    from_json,
    explode,
    from_unixtime,
    when
)
from pyspark.sql.types import ArrayType, StringType

# Configuration
checkpoint = "/Volumes/workspace/flight_tracking/checkpoints/silver"
target_table = "workspace.flight_tracking.silver"

# OpenSky schema
parsed_schema = ArrayType(ArrayType(StringType()))

# Read Bronze stream
df = spark.readStream.table("workspace.flight_tracking.bronze")

# Parse JSON
parsed_df = df.withColumn(
    "parsed_states",
    from_json(col("states"), parsed_schema)
)

# Explode aircraft records
explode_df = parsed_df.withColumn(
    "aircraft",
    explode(col("parsed_states"))
)

# Flatten aircraft data
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

# Silver transformations
silver_df = (
    flight_df
    .withColumn(
        "time_position",
        from_unixtime(col("time_position"), "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "last_contact",
        from_unixtime(col("last_contact"), "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "position_valid",
        when(
            col("latitude").isNotNull()
            & col("longitude").isNotNull()
            & (col("longitude") >= -180)
            & (col("longitude") <= 180)
            & (col("latitude") >= -90)
            & (col("latitude") <= 90),
            True
        ).otherwise(False)
    )
    .dropDuplicates(["icao24"])
)

# MERGE logic
def process_batch(batch_df, batch_id):

    target = DeltaTable.forName(spark, target_table)

    (
        target.alias("t")
        .merge(
            batch_df.alias("s"),
            "t.icao24 = s.icao24"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# Start stream
silver_query = (
    silver_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start()
)

silver_query.awaitTermination()
