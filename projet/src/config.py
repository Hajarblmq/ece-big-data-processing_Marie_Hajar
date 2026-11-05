"""Central configuration: paths, IMDb schemas, constants."""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "projet" / "data" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "output" / "preparation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

NULL_TOKEN = "\\N"

IMDB_SCHEMAS = {
    "name.basics": [
        ("nconst", "string"),
        ("primaryName", "string"),
        ("birthYear", "int"),
        ("deathYear", "int"),
        ("primaryProfession", "string"),
        ("knownForTitles", "string"),
    ],
    "title.basics": [
        ("tconst", "string"),
        ("titleType", "string"),
        ("primaryTitle", "string"),
        ("originalTitle", "string"),
        ("isAdult", "int"),
        ("startYear", "int"),
        ("endYear", "int"),
        ("runtimeMinutes", "int"),
        ("genres", "string"),
    ],
    "title.ratings": [
        ("tconst", "string"),
        ("averageRating", "double"),
        ("numVotes", "int"),
    ],
    "title.crew": [
        ("tconst", "string"),
        ("directors", "string"),
        ("writers", "string"),
    ],
    "title.principals": [
        ("tconst", "string"),
        ("ordering", "int"),
        ("nconst", "string"),
        ("category", "string"),
        ("job", "string"),
        ("characters", "string"),
    ],
    "title.episode": [
        ("tconst", "string"),
        ("parentTconst", "string"),
        ("seasonNumber", "int"),
        ("episodeNumber", "int"),
    ],
    "title.akas": [
        ("titleId", "string"),
        ("ordering", "int"),
        ("title", "string"),
        ("region", "string"),
        ("language", "string"),
        ("types", "string"),
        ("attributes", "string"),
        ("isOriginalTitle", "int"),
    ],
}
