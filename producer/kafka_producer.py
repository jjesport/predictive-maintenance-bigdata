import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from kafka import KafkaProducer


def discover_csv_files(raw_dir: Path, month_filter: str | None = None) -> list[Path]:
    pattern = f"{month_filter}/*.csv" if month_filter else "*/*.csv"
    files = sorted(raw_dir.glob(pattern))
    # Filtrar archivos vacíos o que solo contienen la cabecera (<= 100 bytes)
    return [f for f in files if f.is_file() and f.stat().st_size > 100]


def parse_and_sort_csv(csv_path: Path) -> list[dict]:
    records = []
    source_period = csv_path.parent.name

    raw_bytes = csv_path.read_bytes()
    try:
        text_data = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        text_data = raw_bytes.decode("cp1252", errors="replace")
    import io
    with io.StringIO(text_data, newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            if not row:
                continue
            sensor_id = (row.get("id") or "").strip()
            raw_val = (row.get("value") or "").strip().replace(",", ".")
            unit = (row.get("unit") or "").strip()
            event_time = (row.get("timestamp") or "").strip()

            if not sensor_id or not raw_val or not event_time:
                continue

            try:
                value = float(raw_val)
            except ValueError:
                continue

            records.append(
                {
                    "sensor_id": sensor_id,
                    "value": value,
                    "unit": unit,
                    "event_time": event_time,
                    "source_period": source_period,
                }
            )

    # Ordenar cronológicamente por timestamp original dentro del bloque diario
    records.sort(key=lambda r: r["event_time"])
    return records


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simulador de flujo SCADA en tiempo real hacia Apache Kafka."
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directorio raíz con carpetas mensuales de CSVs.",
    )
    parser.add_argument(
        "--month",
        type=str,
        default=None,
        help="Filtrar por carpeta mensual específica (ej. 2020_01).",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Dirección del broker Kafka.",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="sensores_scada",
        help="Tópico destino en Kafka.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=200.0,
        help="Velocidad de envío en eventos por segundo (0 = sin pausa).",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Límite máximo de eventos a publicar (0 = sin límite).",
    )
    args = parser.parse_args()

    csv_files = discover_csv_files(args.raw_dir, args.month)
    if not csv_files:
        raise FileNotFoundError(f"No se encontraron archivos CSV con datos en {args.raw_dir}")

    print(f"[INFO] Archivos CSV válidos encontrados: {len(csv_files)}")
    print(f"[INFO] Conectando a Kafka en {args.bootstrap_servers} -> tópico '{args.topic}'...")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=10,
    )

    sleep_interval = (1.0 / args.rate) if args.rate > 0 else 0.0
    sent_count = 0
    start_time = time.time()

    try:
        for csv_file in csv_files:
            print(f"[FILE] Procesando {csv_file.parent.name}/{csv_file.name} ...")
            events = parse_and_sort_csv(csv_file)

            for event in events:
                event["ingestion_time"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
                producer.send(args.topic, key=event["sensor_id"], value=event)
                sent_count += 1

                if sent_count % 500 == 0:
                    producer.flush()
                    elapsed = time.time() - start_time
                    print(f"[PROGRESS] Eventos publicados: {sent_count} ({sent_count / max(elapsed, 0.001):.1f} msg/s)")

                if args.max_events > 0 and sent_count >= args.max_events:
                    break

                if sleep_interval > 0:
                    time.sleep(sleep_interval)

            if args.max_events > 0 and sent_count >= args.max_events:
                break

    except KeyboardInterrupt:
        print("\n[INFO] Simulación detenida manualmente por el usuario.")
    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        print(f"[DONE] Total eventos publicados: {sent_count} en {elapsed:.2f}s")


if __name__ == "__main__":
    main()
