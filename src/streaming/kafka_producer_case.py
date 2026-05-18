"""src/streaming/kafke_producer_case.py - Kafka producer example.

Reads sales from data/sales.csv
and sends records to a Kafka topic one message at a time.

Start with main() at the bottom.
Work up to see how it all fits together.

Many functions are standard helpers
and should not need project-specific modifications.

Author: Denise Case
Date: 2026-05

Terminal command to run this file from the root project folder:

    uv run python -m streaming.kafka_producer_case

OBS:
  Don't edit this file - it should remain a working example.
  Copy it, rename it producer_yourname.py, and modify your copy.
"""

# === DECLARE IMPORTS ===

from collections.abc import Generator
import os
from pathlib import Path
import time
from typing import Any, Final

from datafun_streaming.io.errors import missing_csv_field_message
from datafun_streaming.io.io_utils import format_message_for_log, read_csv_rows
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_producer_utils import (
    create_producer,
    prepare_producer_topic,
    produce_kafka_message,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars

# === CONFIGURE LOGGER ===

LOG = get_logger("P02", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

msg_count = os.getenv("PRODUCER_MESSAGE_COUNT", "6")
msg_interval_seconds = os.getenv("PRODUCER_MESSAGE_INTERVAL_SECONDS", "2.0")

MESSAGE_COUNT: Final[int] = int(msg_count)
MESSAGE_INTERVAL_SECONDS: Final[float] = float(msg_interval_seconds)

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
SALES_CSV: Final[Path] = DATA_DIR / "sales.csv"


# ==========================================================
# DEFINE SECTION A. ACQUIRE RESOURCES AND GET READY HELPERS
# ==========================================================


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "P02")
    LOG.info("========================")
    LOG.info("START producer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "SALES_CSV", SALES_CSV)


def load_settings() -> KafkaSettings:
    """Load settings from .env and log them."""
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS           = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC                       = {settings.topic}")
    LOG.info(f"PRODUCER_MESSAGE_COUNT            = {MESSAGE_COUNT}")
    LOG.info(f"PRODUCER_MESSAGE_INTERVAL_SECONDS = {MESSAGE_INTERVAL_SECONDS}")
    LOG.info(f"KAFKA_CLEAR_TOPIC_ON_START        = {settings.clear_topic_on_start}")
    return settings


def verify_connection(settings: KafkaSettings) -> None:
    """Verify Kafka is reachable before doing anything else."""
    LOG.info("Verifying Kafka connection...")
    try:
        verify_kafka_connection(settings)
        LOG.info("Kafka port is reachable.")
    except ConnectionError as error:
        LOG.error(str(error))
        raise SystemExit(1) from error


# ===========================================================================
# DEFINE SECTION P. PRODUCE MESSAGES HELPERS
# ===========================================================================


def get_message_key(message: dict[str, Any]) -> str:
    """Return the Kafka message key for a sale record."""
    try:
        return str(message["region_id"])
    except KeyError as error:
        msg = missing_csv_field_message(
            field="region_id",
            available_fields=list(message.keys()),
        )
        raise KeyError(msg) from error


def generate_messages(count: int) -> Generator[dict[str, str]]:
    """Generate a stream of sales from the input CSV file."""
    sales_rows = read_csv_rows(SALES_CSV)
    yield from sales_rows[:count]


def send_messages(producer: Any, settings: KafkaSettings) -> int:
    """Generate and send messages to the Kafka topic."""
    LOG.info("Sending messages...")
    LOG.info(f"Sending up to {MESSAGE_COUNT} message(s) to topic {settings.topic!r}.")
    LOG.info("Watch each sale arrive. Press CTRL+C to stop early.\n")

    sent_count = 0

    try:
        for message in generate_messages(MESSAGE_COUNT):
            LOG.info(format_message_for_log(message))

            key = get_message_key(message)
            LOG.info(f"  Sending message with key={key}")

            produce_kafka_message(
                producer=producer,
                topic=settings.topic,
                key=key,
                message=message,
            )

            sent_count += 1
            LOG.info(f"  MESSAGE SENT  sent={sent_count}")
            time.sleep(MESSAGE_INTERVAL_SECONDS)

    except (FileNotFoundError, KeyError, RuntimeError, ValueError) as error:
        LOG.error(str(error))
        LOG.error("Producer stopped before completing all messages.")
        raise SystemExit(1) from error

    return sent_count


# ===========================================================================
# DEFINE SECTION E. EXIT AND CLEANUP HELPERS
# ===========================================================================


def log_summary(sent_count: int, settings: KafkaSettings) -> None:
    """Log final summary statistics.

    Arguments:
        sent_count: The number of messages successfully sent to Kafka.
        settings: The KafkaSettings object containing configuration details.
    """
    LOG.info("Summary:")
    LOG.info(f"Sent {sent_count} message(s) to topic {settings.topic!r}.")
    LOG.info("========================")
    LOG.info("Producer executed successfully!")
    LOG.info("========================")


# ===========================================================================
# MAIN FUNCTION
# ===========================================================================


def main() -> None:
    """Main entry point for the Kafka producer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    prepare_producer_topic(settings)
    producer = create_producer(settings)

    LOG.info("========================")
    LOG.info("SECTION P. Produce Messages")
    LOG.info("========================")

    sent_count = send_messages(producer, settings)

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    producer.flush()
    log_summary(sent_count, settings)


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
