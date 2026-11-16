import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


def get_event_schema() -> StructType:
    return StructType(
        [
            StructField("sensor_id", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("source_period", StringType(), True),
            StructField("ingestion_time", StringType(), True),
        ]
    )


def map_physical_quantity(unit_col):
    # Mapeo basado estrictamente en los metadatos oficiales I-BiDaaS / CRF SCADA
    return (
        F.when(unit_col == "mg", F.lit("Acceleration"))
        .when(unit_col == "mm/s", F.lit("Velocity"))
        .when(unit_col == "°C", F.lit("Temperature"))
        .when(unit_col == "bar", F.lit("Pressure"))
        .when(unit_col.isin("l/min", "Nl/min"), F.lit("Flow"))
        .when(unit_col == "mm", F.lit("Displacement"))
        .when(unit_col.isin("l", "m³", "m3", "m³ std.", "m³/h", "m3/h", "m³/h std."), F.lit("Energy vector"))
        .otherwise(F.lit("Unknown"))
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Procesamiento en flujo con Spark Structured Streaming desde Kafka hacia HDFS."
    )
    parser.add_argument(
        "--kafka-bootstrap",
        type=str,
        default="kafka:29092",
        help="Broker Kafka interno en la red Docker.",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="sensores_scada",
        help="Tópico de Kafka a consumir.",
    )
    parser.add_argument(
        "--starting-offsets",
        type=str,
        default="earliest",
        choices=["earliest", "latest"],
        help="Offset inicial de lectura en Kafka.",
    )
    parser.add_argument(
        "--hdfs-base",
        type=str,
        default="hdfs://namenode:9000/scada",
        help="Ruta base en HDFS.",
    )
    parser.add_argument(
        "--window-duration",
        type=str,
        default="1 minute",
        help="Duración de ventana temporal para agregaciones.",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("SCADA-Predictive-Maintenance-Streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = get_event_schema()

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_stream.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.from_json(F.col("value").cast("string"), schema).alias("data"),
        )
        .select("data.*")
        .withColumn("sensor_id", F.trim(F.col("sensor_id")))
        .withColumn("unit", F.trim(F.col("unit")))
        .withColumn("source_period", F.trim(F.col("source_period")))
        .withColumn(
            "event_ts",
            F.coalesce(
                F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss.SSS"),
                F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"),
            ),
        )
        .withColumn("ingestion_ts", F.to_timestamp("ingestion_time"))
    )

    # Validación de calidad y limpieza
    clean_df = (
        parsed_df.filter(
            F.col("sensor_id").isNotNull()
            & (F.col("sensor_id") != "")
            & F.col("value").isNotNull()
            & (~F.isnan("value"))
            & F.col("unit").isNotNull()
            & (F.col("unit") != "")
            & F.col("event_ts").isNotNull()
        )
        .withColumn("physical_quantity", map_physical_quantity(F.col("unit")))
        .withWatermark("event_ts", "10 minutes")
        .dropDuplicates(["sensor_id", "event_ts", "value"])
    )

    # 1. Almacenamiento de eventos curados en HDFS (Parquet)
    processed_query = (
        clean_df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", f"{args.hdfs_base}/processed")
        .option("checkpointLocation", f"{args.hdfs_base}/checkpoints/processed")
        .partitionBy("source_period", "unit")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # 2. Agregaciones por ventana temporal y sensor (respetando unidades)
    windowed_metrics_df = (
        clean_df.groupBy(
            F.window(F.col("event_ts"), args.window_duration),
            F.col("sensor_id"),
            F.col("unit"),
            F.col("physical_quantity"),
            F.col("source_period"),
        )
        .agg(
            F.count("value").alias("sample_count"),
            F.avg("value").alias("mean_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.stddev("value").alias("stddev_value"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "sensor_id",
            "unit",
            "physical_quantity",
            "source_period",
            "sample_count",
            "mean_value",
            "min_value",
            "max_value",
            "stddev_value",
        )
    )

    metrics_query = (
        windowed_metrics_df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", f"{args.hdfs_base}/metrics")
        .option("checkpointLocation", f"{args.hdfs_base}/checkpoints/metrics")
        .partitionBy("source_period", "unit")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("[INFO] Spark Structured Streaming iniciado. Escribiendo Parquet en HDFS...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
