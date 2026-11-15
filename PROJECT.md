# Big Data Processing Project

## Contributors
- Hajar Benlemqadem (hajar.benlemqadem@edu.ece.fr)
- Marie Leona NOMO NGAH (marie.nomongah@edu.ece.fr)

## AI / LLM Usage Declaration
AI tools were used for code only. The explanations, interpretations and
methodology texts of REPORT.md were written by us.

- **ChatGPT (Hajar)**: debugging PySpark errors, structuring the code of
  parts 1 and 2, Markdown formatting of the report.
- **Claude (Marie)**: writing the code of the streaming pipeline
  (`src/streaming/`), the enrichment pipeline (`src/enrichment/`) and the
  open question (`src/open_question/`); help with the Windows setup, Git
  (branches, merge conflicts), reviewing pull requests and the run
  instructions of this file.

## Environment Setup
- Python 3.9+
- Java 17 or 21 (required for PySpark 4.0; Java 22+ is not supported)
- Apache Spark (PySpark)
- Apache Kafka & Docker (for streaming)

### macOS / Linux
```bash
# Activate Java 17
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

python3 -m venv .venv
source .venv/bin/activate
```

### Windows
- Java 17 or 21, with `JAVA_HOME` pointing to it
- winutils: download `winutils.exe` and `hadoop.dll` from
  https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin into
  `C:\hadoop\bin`, set `HADOOP_HOME=C:\hadoop` and add `C:\hadoop\bin` to
  `Path` (needed by Spark to write files)
- Virtual environment: `python -m venv .venv` then
  `.\.venv\Scripts\Activate.ps1`

### Data
Download the 7 IMDb files from https://datasets.imdbws.com/
(`name.basics`, `title.basics`, `title.ratings`, `title.crew`,
`title.principals`, `title.episode`, `title.akas`) into `projet/data/raw/`,
without unzipping them.

## Execution Instructions

The code uses package-relative imports: every script is run as a module
(`python -m ...`) from the `projet` folder, with the virtual environment
activated.

### 1. Preparation & Exploration (Hajar)
```bash
# Install dependencies (from the repository root)
pip install -r projet/requirements.txt

cd projet

# Run data preparation
python -m src.preparation.null_analysis
python -m src.preparation.data_quality
python -m src.preparation.cleaning

# Run data exploration
python -m src.exploration.genre_analysis            # Q1, Q2, Q3
python -m src.exploration.director_analysis         # Q4
python -m src.exploration.multilingual_analysis     # Q6
python -m src.exploration.career_analysis           # Q7
python -m src.exploration.multitallented_analysis   # Q8

# Q5 is run from the repository root
cd ..
python -m projet.src.exploration.series_analysis
cd projet
```

### 2. Enrichment, Streaming and Open Question (Marie)
Run from the `projet` folder, after the data preparation.

```bash
# Part 3: enrichment (~1 h, needs internet)
python -m src.enrichment.select_targets
python -m src.enrichment.wikipedia_fetch        # ~50 min, resumes if interrupted
python -m src.enrichment.enrichment_pipeline

# Part 4: streaming (needs Docker Desktop running)
docker compose up -d
python -m src.streaming.build_lookup
python -m src.streaming.kafka_consumer --minutes 25 --fresh
# in a second terminal, once "Streaming for 25.0 min" is printed:
python -m src.streaming.wikipedia_stream --minutes 20
docker compose down

# Part 5: open question
python -m src.open_question.analysis
```

Results of each part are written to `projet/output/<part>/`.
