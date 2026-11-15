# Big Data Processing Project

## Contributors
- Hajar Benlemqadem (hajar.benlemqadem@edu.ece.fr)
- Marie Leona NOMO NGAH (marie.nomongah@edu.ece.fr)

## AI / LLM Usage Declaration
We used AI tools (ChatGPT) to help us debug PySpark errors, structure our code, and format our Markdown report. All the logic, data analysis, and final writing were done by us.

## Environment Setup
- Python 3.9+
- Java 17 (required for PySpark)
- Apache Spark (PySpark)
- Apache Kafka & Docker (for streaming)

## Execution Instructions

### 1. Preparation & Exploration (Hajar)
```bash
# Activate Java 17
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

# Install dependencies
pip install -r requirements.txt

# Run data preparation
PYTHONPATH=. python3 projet/src/preparation/cleaning.py

# Run data exploration (Q1 to Q8)
PYTHONPATH=. python3 projet/src/exploration/genre_analysis.py
PYTHONPATH=. python3 projet/src/exploration/director_analysis.py
PYTHONPATH=. python3 projet/src/exploration/series_analysis.py
PYTHONPATH=. python3 projet/src/exploration/multilingual_analysis.py
PYTHONPATH=. python3 projet/src/exploration/career_analysis.py
PYTHONPATH=. python3 projet/src/exploration/multitallented_analysis.py
