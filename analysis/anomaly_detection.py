import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Detección estadística de anomalías sobre datos SCADA en HDFS."
    )
    parser.add_argument(
        "--hdfs-base",
        type=str,
        default="hdfs://namenode:9000/scada",
        help="Ruta base en HDFS.",
    )
    parser.add_argument(
        "--z-threshold",
        type=float,
        default=3.0,
        help="Umbral absoluto de Z-score para marcar anomalías.",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=30,
        help="Mínimo de lecturas por sensor para evaluar Z-score.",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("/opt/spark/work-dir/results"),
        help="Directorio local para exportar tablas, muestras y gráficas.",
    )
    args = parser.parse_args()
    args.results_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName("SCADA-Anomaly-Detection-Analysis")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    processed_df = spark.read.parquet(f"{args.hdfs_base}/processed").cache()
    metrics_df = spark.read.parquet(f"{args.hdfs_base}/metrics").cache()

    total_processed = processed_df.count()
    total_windows = metrics_df.count()
    distinct_sensors = processed_df.select("sensor_id").distinct().count()

    print(f"[INFO] Registros curados en HDFS: {total_processed}")
    print(f"[INFO] Ventanas temporales (1 min) en HDFS: {total_windows}")
    print(f"[INFO] Sensores activos detectados: {distinct_sensors}")

    # Calcular estadísticos base por sensor y unidad (sin mezclar unidades)
    sensor_window = Window.partitionBy("sensor_id", "unit")

    scored_df = (
        processed_df.withColumn("sensor_count", F.count("value").over(sensor_window))
        .withColumn("sensor_mean", F.avg("value").over(sensor_window))
        .withColumn("sensor_stddev", F.stddev("value").over(sensor_window))
        .withColumn(
            "z_score",
            F.when(
                (F.col("sensor_count") >= args.min_samples)
                & F.col("sensor_stddev").isNotNull()
                & (F.col("sensor_stddev") > 1e-6),
                (F.col("value") - F.col("sensor_mean")) / F.col("sensor_stddev"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("is_anomaly", F.abs(F.col("z_score")) > args.z_threshold)
    )

    anomalies_df = scored_df.filter(F.col("is_anomaly")).cache()
    total_anomalies = anomalies_df.count()
    anomaly_rate_pct = (total_anomalies / total_processed * 100.0) if total_processed > 0 else 0.0

    print(f"[INFO] Anomalías detectadas (|Z| > {args.z_threshold}): {total_anomalies} ({anomaly_rate_pct:.2f}%)")

    # Guardar anomalías en HDFS en formato Parquet
    (
        anomalies_df.select(
            "sensor_id",
            "unit",
            "physical_quantity",
            "source_period",
            "event_ts",
            "value",
            "sensor_mean",
            "sensor_stddev",
            "z_score",
        )
        .write.mode("overwrite")
        .partitionBy("source_period", "unit")
        .parquet(f"{args.hdfs_base}/anomalies")
    )

    # Resumen por magnitud física y unidad
    summary_unit_pd = (
        scored_df.groupBy("physical_quantity", "unit")
        .agg(
            F.count("*").alias("total_records"),
            F.countDistinct("sensor_id").alias("active_sensors"),
            F.round(F.avg("value"), 4).alias("global_mean"),
            F.round(F.min("value"), 4).alias("global_min"),
            F.round(F.max("value"), 4).alias("global_max"),
            F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
        )
        .orderBy(F.desc("total_records"))
        .toPandas()
    )
    summary_unit_pd.to_csv(args.results_dir / "summary_by_unit.csv", index=False)

    # Top sensores con más anomalías
    top_sensors_pd = (
        scored_df.groupBy("sensor_id", "unit", "physical_quantity")
        .agg(
            F.count("*").alias("total_records"),
            F.round(F.first("sensor_mean"), 4).alias("mean_value"),
            F.round(F.first("sensor_stddev"), 4).alias("stddev_value"),
            F.round(F.max(F.abs(F.col("z_score"))), 2).alias("max_abs_z"),
            F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
        )
        .withColumn("anomaly_pct", F.round(F.col("anomaly_count") / F.col("total_records") * 100.0, 2))
        .filter(F.col("anomaly_count") > 0)
        .orderBy(F.desc("anomaly_count"), F.desc("max_abs_z"))
        .toPandas()
    )
    top_sensors_pd.to_csv(args.results_dir / "top_anomalous_sensors.csv", index=False)

    # Exportar muestras (50 registros) de cada etapa para el repositorio GitHub
    (
        processed_df.select("sensor_id", "value", "unit", "physical_quantity", "event_time", "source_period", "ingestion_time")
        .limit(50)
        .toPandas()
        .to_csv(args.results_dir / "sample_processed.csv", index=False)
    )
    (
        metrics_df.orderBy("window_start", "sensor_id")
        .limit(50)
        .toPandas()
        .to_csv(args.results_dir / "sample_window_metrics.csv", index=False)
    )
    (
        anomalies_df.select("sensor_id", "unit", "physical_quantity", "event_ts", "value", "sensor_mean", "sensor_stddev", "z_score")
        .orderBy(F.desc(F.abs(F.col("z_score"))))
        .limit(50)
        .toPandas()
        .to_csv(args.results_dir / "sample_anomalies.csv", index=False)
    )

    # Guardar resumen JSON general
    overview = {
        "total_processed_records": int(total_processed),
        "total_1min_windows": int(total_windows),
        "distinct_active_sensors": int(distinct_sensors),
        "z_score_threshold": args.z_threshold,
        "total_anomalies": int(total_anomalies),
        "anomaly_rate_percent": round(anomaly_rate_pct, 3),
    }
    with (args.results_dir / "metrics_overview.json").open("w", encoding="utf-8") as f:
        json.dump(overview, f, indent=2)

    # --- VISUALIZACIONES ---
    plt.style.use("seaborn-v0_8-whitegrid")

    # Gráfica 1: Volumen de mediciones por unidad de medida
    if not summary_unit_pd.empty:
        fig, ax = plt.subplots(figsize=(9, 4.8))
        labels = summary_unit_pd["unit"] + " (" + summary_unit_pd["physical_quantity"] + ")"
        bars = ax.barh(labels[::-1], summary_unit_pd["total_records"][::-1], color="#1f77b4")
        ax.set_xlabel("Cantidad de mediciones procesadas")
        ax.set_title("Distribución de Lecturas SCADA Procesadas por Unidad y Magnitud Física")
        for bar in bars:
            w = bar.get_width()
            ax.text(w + max(summary_unit_pd["total_records"]) * 0.01, bar.get_y() + bar.get_height() / 2, f"{int(w):,}", va="center", fontsize=9)
        plt.tight_layout()
        plt.savefig(args.results_dir / "fig1_records_by_quantity.png", dpi=150)
        plt.close(fig)

    # Gráfica 2: Top 10 sensores con mayor cantidad de anomalías
    if not top_sensors_pd.empty:
        top10 = top_sensors_pd.head(10)
        fig, ax = plt.subplots(figsize=(9, 4.8))
        sensor_labels = "Sensor " + top10["sensor_id"] + " [" + top10["unit"] + "]"
        bars = ax.barh(sensor_labels[::-1], top10["anomaly_count"][::-1], color="#d62728")
        ax.set_xlabel("Número de eventos anómalos (|Z| > 3.0)")
        ax.set_title("Top Sensores con Mayor Frecuencia de Anomalías Estadísticas")
        for bar in bars:
            w = bar.get_width()
            ax.text(w + max(top10["anomaly_count"]) * 0.01, bar.get_y() + bar.get_height() / 2, f"{int(w)}", va="center", fontsize=9)
        plt.tight_layout()
        plt.savefig(args.results_dir / "fig2_anomalies_by_sensor.png", dpi=150)
        plt.close(fig)

        # Gráfica 3: Serie temporal del sensor con mayor Z-score máximo
        target_row = top_sensors_pd.iloc[0]
        target_sensor = target_row["sensor_id"]
        target_unit = target_row["unit"]

        ts_pd = (
            scored_df.filter((F.col("sensor_id") == target_sensor) & (F.col("unit") == target_unit))
            .select("event_ts", "value", "sensor_mean", "sensor_stddev", "is_anomaly")
            .orderBy("event_ts")
            .toPandas()
        )
        if not ts_pd.empty:
            fig, ax = plt.subplots(figsize=(10, 4.8))
            ax.plot(ts_pd["event_ts"], ts_pd["value"], color="#2b5c8f", linewidth=1.1, label="Lectura normal")
            anom_pts = ts_pd[ts_pd["is_anomaly"]]
            ax.scatter(anom_pts["event_ts"], anom_pts["value"], color="#d62728", s=28, zorder=5, label=f"Anomalía (|Z| > {args.z_threshold})")
            mean_val = ts_pd["sensor_mean"].iloc[0]
            std_val = ts_pd["sensor_stddev"].iloc[0]
            ax.axhline(mean_val, color="#2ca02c", linestyle="--", linewidth=1.2, label=f"Media ({mean_val:.2f} {target_unit})")
            ax.axhline(mean_val + args.z_threshold * std_val, color="#ff7f0e", linestyle=":", linewidth=1.2, label=f"Umbral +{args.z_threshold}σ")
            ax.axhline(mean_val - args.z_threshold * std_val, color="#ff7f0e", linestyle=":", linewidth=1.2, label=f"Umbral -{args.z_threshold}σ")
            ax.set_title(f"Serie Temporal y Detección de Anomalías — Sensor {target_sensor} ({target_unit})")
            ax.set_xlabel("Tiempo del evento (event_ts)")
            ax.set_ylabel(f"Valor ({target_unit})")
            ax.legend(loc="best", fontsize=8.5)
            plt.xticks(rotation=25)
            plt.tight_layout()
            plt.savefig(args.results_dir / "fig3_sensor_timeseries_anomalies.png", dpi=150)
            plt.close(fig)

    print("[DONE] Análisis completado. Resultados exportados en:", args.results_dir)
    spark.stop()


if __name__ == "__main__":
    main()
