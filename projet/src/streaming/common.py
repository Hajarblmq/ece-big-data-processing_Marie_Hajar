"""Shared settings and helpers for the streaming pipeline."""
from pathlib import Path

from pyspark.sql import Column
from pyspark.sql import functions as F

PROJECT_DIR = Path(__file__).resolve().parents[2]
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
LOOKUP_DIR = PROCESSED_DIR / "streaming_lookup"
OUTPUT_DIR = PROJECT_DIR / "output" / "streaming"
CHECKPOINT_DIR = PROJECT_DIR / "data" / "checkpoints" / "streaming"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "wikipedia-changes"


def normalize_title(column: Column) -> Column:
    """Build the key used to match Wikipedia page titles with IMDb names.

    The same function is applied on both sides of the join so that
    "Titanic (1997 film)" on Wikipedia and "Titanic" on IMDb give the
    same key "titanic".
    """
    no_suffix = F.regexp_replace(F.trim(column), r"\s*\([^)]*\)$", "")
    no_underscore = F.regexp_replace(no_suffix, "_", " ")
    return F.lower(F.trim(no_underscore))


def disambiguation(column: Column) -> Column:
    """Return the text inside a trailing "(...)" or null if there is none."""
    suffix = F.regexp_extract(column, r"\(([^)]*)\)$", 1)
    return F.when(suffix == "", F.lit(None)).otherwise(F.lower(suffix))
