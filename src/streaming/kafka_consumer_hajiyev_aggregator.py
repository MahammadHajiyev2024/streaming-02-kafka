"""src/streaming/kafka_consumer_hajiyev_aggregator.py.

Kafka consumer: Sales aggregator by region and product.

Reads sales messages from a Kafka topic.
Aggregates transactions and computes summaries:
- Count, total revenue, average price per region
- Count, total revenue, average price per product

Start with main() at the bottom.
Work up to see how it all fits together.

Author: Modified from kafka_consumer_case.py
Date: 2026-05

Terminal command to run this file from the root project folder:

    uv run python -m streaming.kafka_consumer_hajiyev_aggregator

"""

# === DECLARE IMPORTS ===

import os
from pathlib import Path
from typing import Any, Final

from confluent_kafka.cimpl import OFFSET_BEGINNING, TopicPartition
from datafun_streaming.io.io_utils import append_csv_row
from datafun_streaming.kafka.kafka_admin_utils import (
    create_admin_client,
    get_topic_message_count,
    topic_exists,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_consumer_utils import (
    consume_kafka_message,
    create_consumer,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_streaming.stats.stats_utils import RunningStats
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars

# === CONFIGURE LOGGER ===

LOG = get_logger("H03", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

COURSE_NAME: Final[str] = "Streaming Data"
TIMEOUT_SECONDS: Final[float] = float(os.getenv("CONSUMER_TIMEOUT_SECONDS", "10.0"))
MAX_MESSAGES: Final[int] = int(os.getenv("CONSUMER_MAX_MESSAGES", "1000"))

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

OUTPUT_CSV: Final[Path] = OUTPUT_DIR / "consumed_sales_hajiyev_summary.csv"


# ==========================================================
# DEFINE SECTION A. ACQUIRE RESOURCES AND GET READY HELPERS
# ==========================================================


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "H03")
    LOG.info("========================")
    LOG.info("START consumer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)


def load_settings() -> KafkaSettings:
    """Load settings from .env and log them.

    Returns:
        A KafkaSettings instance populated from environment variables.
    """
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS  = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC              = {settings.topic}")
    LOG.info(f"KAFKA_GROUP_ID           = {settings.group_id}")
    LOG.info(f"CONSUMER_TIMEOUT_SECONDS = {TIMEOUT_SECONDS}")
    LOG.info(f"CONSUMER_MAX_MESSAGES    = {MAX_MESSAGES}")
    return settings


def verify_connection(settings: KafkaSettings) -> None:
    """Verify Kafka is reachable before doing anything else.

    Raises:
        SystemExit: If Kafka is not reachable.
    """
    LOG.info("Verifying Kafka connection...")
    try:
        verify_kafka_connection(settings)
        LOG.info("Kafka port is reachable.")
    except ConnectionError as error:
        LOG.error(str(error))
        raise SystemExit(1) from error


def verify_topic(settings: KafkaSettings) -> None:
    """Verify the topic exists and has messages.

    Raises:
        SystemExit: If the topic does not exist or is empty.
    """
    LOG.info("Verifying Kafka topic...")
    admin = create_admin_client(settings)

    if not topic_exists(admin, settings.topic):
        LOG.error(f"Topic {settings.topic!r} does not exist.")
        LOG.error("Run the producer first.")
        raise SystemExit(1)

    message_count = get_topic_message_count(admin, settings.topic, settings)
    LOG.info(f"Topic {settings.topic!r} exists.")
    LOG.info(f"Found {message_count} message(s) available.")

    if message_count == 0:
        LOG.error("Topic is empty. Run the producer first.")
        raise SystemExit(1)


def get_kafka_consumer(settings: KafkaSettings) -> Any:
    """Create a Kafka consumer subscribed to the topic.

    Resets offsets to the beginning so this example reads all available messages.

    Returns:
        A confluent_kafka.Consumer instance subscribed to the topic.
    """
    LOG.info("Creating Kafka consumer...")
    consumer = create_consumer(settings)
    consumer.subscribe(
        [settings.topic],
        on_assign=lambda c, partitions: c.assign(
            [
                TopicPartition(
                    partition.topic,
                    partition.partition,
                    OFFSET_BEGINNING,
                )
                for partition in partitions
            ]
        ),
    )
    LOG.info(f"Subscribed to topic: {settings.topic!r} (reading from beginning)")
    return consumer


# ===========================================================================
# DEFINE SECTION C. CONSUME AND PROCESS MESSAGES HELPERS
# ===========================================================================


def initialize_output() -> RunningStats:
    """Initialize output directory, CSV, database, chart, and stats.

    Returns:
        A RunningStats instance.
    """
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()
    LOG.info(f"Output CSV cleared: {OUTPUT_CSV.name}")

    return RunningStats()


def process_message(
    row: dict[str, Any],
    region_stats: dict[str, dict[str, Any]],
    product_stats: dict[str, dict[str, Any]],
) -> None:
    """Process one consumed message and update aggregation stats.

    Arguments:
        row: A raw consumed Kafka message row.
        region_stats: Dictionary to accumulate region-level stats.
        product_stats: Dictionary to accumulate product-level stats.
    """
    LOG.info("Processing message for aggregation.")

    region = row.get("region_id", "UNKNOWN")
    product = row.get("product_id", "UNKNOWN")
    price = float(row.get("unit_price", 0))
    quantity = int(row.get("quantity", 1))
    revenue = price * quantity

    # Update region stats
    if region not in region_stats:
        region_stats[region] = {"count": 0, "total_revenue": 0.0, "total_quantity": 0}
    region_stats[region]["count"] += 1
    region_stats[region]["total_revenue"] += revenue
    region_stats[region]["total_quantity"] += quantity

    # Update product stats
    if product not in product_stats:
        product_stats[product] = {"count": 0, "total_revenue": 0.0, "total_quantity": 0}
    product_stats[product]["count"] += 1
    product_stats[product]["total_revenue"] += revenue
    product_stats[product]["total_quantity"] += quantity

    LOG.info(f"Updated region={region}, product={product}, revenue={revenue}")


def consume_messages(consumer: Any) -> tuple[int, dict, dict]:
    """Consume raw messages from the Kafka topic and aggregate.

    Runs until MAX_MESSAGES is reached or TIMEOUT_SECONDS elapses
    with no new message.

    Arguments:
        consumer: An open Kafka consumer subscribed to the topic.

    Returns:
        Tuple of (consumed_count, region_stats, product_stats).
    """
    LOG.info("Consuming and aggregating messages...")
    LOG.info(f"Waiting for up to {MAX_MESSAGES} message(s).")
    LOG.info("Press CTRL+C to stop early.\n")

    consumed_count = 0
    total_seen = 0
    region_stats: dict[str, dict[str, Any]] = {}
    product_stats: dict[str, dict[str, Any]] = {}

    while total_seen < MAX_MESSAGES:
        row = consume_kafka_message(
            consumer=consumer,
            timeout_seconds=TIMEOUT_SECONDS,
        )

        if row is None:
            LOG.info(f"No message received within {TIMEOUT_SECONDS}s timeout.")
            LOG.info("Producer finished or paused. Stopping consumer.")
            break

        LOG.info(row)
        total_seen += 1

        process_message(row, region_stats, product_stats)

        consumed_count += 1
        LOG.info("MESSAGE PROCESSED")
        LOG.info(f"consumed={consumed_count}")

    return consumed_count, region_stats, product_stats


def write_summary_csv(
    region_stats: dict[str, dict[str, Any]],
    product_stats: dict[str, dict[str, Any]],
) -> None:
    """Write aggregated statistics to summary CSV.

    Arguments:
        region_stats: Dictionary with region-level aggregations.
        product_stats: Dictionary with product-level aggregations.
    """
    LOG.info("Writing summary statistics to CSV...")

    # Write region summaries
    LOG.info("Region Summary:")
    for region, stats in sorted(region_stats.items()):
        avg_revenue = (
            stats["total_revenue"] / stats["count"] if stats["count"] > 0 else 0
        )
        avg_quantity = (
            stats["total_quantity"] / stats["count"] if stats["count"] > 0 else 0
        )

        row = {
            "type": "region",
            "category": region,
            "count": stats["count"],
            "total_revenue": round(stats["total_revenue"], 2),
            "avg_revenue_per_order": round(avg_revenue, 2),
            "total_quantity": stats["total_quantity"],
            "avg_quantity_per_order": round(avg_quantity, 2),
        }

        append_csv_row(
            path=OUTPUT_CSV,
            row=row,
            fieldnames=list(row.keys()),
        )

        LOG.info(
            f"  {region}: {stats['count']} orders, ${stats['total_revenue']:.2f} total"
        )

    # Write product summaries
    LOG.info("Product Summary:")
    for product, stats in sorted(product_stats.items()):
        avg_revenue = (
            stats["total_revenue"] / stats["count"] if stats["count"] > 0 else 0
        )
        avg_quantity = (
            stats["total_quantity"] / stats["count"] if stats["count"] > 0 else 0
        )

        row = {
            "type": "product",
            "category": product,
            "count": stats["count"],
            "total_revenue": round(stats["total_revenue"], 2),
            "avg_revenue_per_order": round(avg_revenue, 2),
            "total_quantity": stats["total_quantity"],
            "avg_quantity_per_order": round(avg_quantity, 2),
        }

        append_csv_row(
            path=OUTPUT_CSV,
            row=row,
            fieldnames=list(row.keys()),
        )

        LOG.info(
            f"  {product}: {stats['count']} orders, ${stats['total_revenue']:.2f} total"
        )


def save_artifacts(stats: RunningStats) -> None:
    """Save output artifacts."""
    LOG.info("Saving artifacts...")
    log_path(LOG, "WROTE OUTPUT_CSV", OUTPUT_CSV)


# ===========================================================================
# DEFINE SECTION E. EXIT AND CLEANUP HELPERS
# ===========================================================================


def log_summary(consumed_count: int, settings: KafkaSettings) -> None:
    """Log final summary statistics."""
    LOG.info("Summary:")
    LOG.info(
        f"Consumed and aggregated {consumed_count} message(s) from topic {settings.topic!r}."
    )
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)
    LOG.info("========================")
    LOG.info("Consumer executed successfully!")
    LOG.info("========================")


# ===========================================================================
# MAIN FUNCTION
# ===========================================================================


def main() -> None:
    """Main entry point for the Kafka consumer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    verify_topic(settings)
    consumer = get_kafka_consumer(settings)

    LOG.info("========================")
    LOG.info("SECTION C. Consume and Aggregate Messages")
    LOG.info("========================")

    initialize_output()

    consumed_count = 0
    region_stats = {}
    product_stats = {}

    try:
        consumed_count, region_stats, product_stats = consume_messages(consumer)
    finally:
        consumer.close()
        LOG.info("Kafka consumer closed.")

    LOG.info("========================")
    LOG.info("SECTION D. Write Summary")
    LOG.info("========================")

    write_summary_csv(region_stats, product_stats)

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    log_summary(consumed_count, settings)


# === CONDITIONAL EXECUTION GUARD ===

# WHY: If running this file as a script, then call main().
# This is standard Python "boilerplate".

if __name__ == "__main__":
    main()
