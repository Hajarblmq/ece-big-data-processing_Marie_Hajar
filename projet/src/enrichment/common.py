"""Shared paths and settings for the enrichment pipeline."""
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
TARGETS_DIR = PROCESSED_DIR / "enrichment_targets"
RAW_ENRICHMENT_DIR = PROJECT_DIR / "data" / "raw" / "enrichment"
OUTPUT_DIR = PROJECT_DIR / "output" / "enrichment"

# Files exchanged between the Spark jobs and the (non-Spark) fetcher.
IDS_DIR = TARGETS_DIR / "ids_csv"
LINKS_FILE = RAW_ENRICHMENT_DIR / "wikidata_links.jsonl"
PAGES_FILE = RAW_ENRICHMENT_DIR / "wikipedia_pages.jsonl"

SOURCE_NAME = "English Wikipedia (CC BY-SA 4.0)"
