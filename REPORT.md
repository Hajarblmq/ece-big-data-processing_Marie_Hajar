# ECE Big Data Processing — Project Report

**Author:** Hajar Benlemqadem (hajar.benlemqadem@edu.ece.fr) and Marie NOMO NGAH (marie.nomongah@edu.ece.fr)
**Repository:** https://github.com/Hajarblmq/ece-big-data-processing_Marie_Hajar
**Due date:** Sunday, November 15, 2026

---

## 1. Data Preparation

### 1.1 Null / missing / empty ratio per column

**Results:**

| table | column | null_pct (%) |
| --- | --- | --- |
| name.basics | birthYear | 95.66 |
| name.basics | deathYear | 98.32 |
| name.basics | knownForTitles | 12.12 |
| name.basics | nconst | 0.00 |
| name.basics | primaryName | 0.0006 |
| name.basics | primaryProfession | 20.34 |
| title.akas | attributes | 99.47 |
| title.akas | isOriginalTitle | 0.00006 |
| title.akas | language | 32.58 |
| title.akas | ordering | 0.00 |
| title.akas | region | 21.56 |
| title.akas | title | 0.00 |
| title.akas | titleId | 0.00 |
| title.akas | types | 66.90 |
| title.basics | endYear | 98.73 |
| title.basics | genres | 4.23 |
| title.basics | isAdult | 0.00 |
| title.basics | originalTitle | 0.00 |
| title.basics | primaryTitle | 0.00 |
| title.basics | runtimeMinutes | 62.26 |
| title.basics | startYear | 11.50 |
| title.basics | tconst | 0.00 |
| title.basics | titleType | 0.00 |
| title.crew | directors | 44.60 |
| title.crew | tconst | 0.00 |
| title.crew | writers | 49.56 |
| title.episode | episodeNumber | 21.08 |
| title.episode | parentTconst | 0.00 |
| title.episode | seasonNumber | 21.08 |
| title.episode | tconst | 0.00 |
| title.principals | category | 0.00 |
| title.principals | characters | 51.26 |
| title.principals | job | 80.67 |
| title.principals | nconst | 0.00 |
| title.principals | ordering | 0.00 |
| title.principals | tconst | 0.00 |
| title.ratings | averageRating | 0.00 |
| title.ratings | numVotes | 0.00 |
| title.ratings | tconst | 0.00 |

**Interpretation:**

Columns with a 0% null ratio correspond logically to primary keys (`tconst`, `nconst`) and mandatory IMDb schema fields. Highly sparse columns are driven by semantic design rather than data errors: `endYear` applies only to multi-episode series, `deathYear` and `birthYear` are frequently unrecorded (for living individuals or uncataloged historical figures), and optional metadata fields are only filled for specialized releases. A critical takeaway for subsequent analytics is that `runtimeMinutes` is 62% null; therefore, missing runtimes must be filtered out rather than imputed during duration analyses.

---

### 1.2 Top 5 columns with highest null ratio

**Results:**

| rank | table | column | null_pct (%) |
| --- | --- | --- | --- |
| 1 | title.akas | attributes | 99.47 |
| 2 | title.basics | endYear | 98.73 |
| 3 | name.basics | deathYear | 98.32 |
| 4 | name.basics | birthYear | 95.66 |
| 5 | title.principals | job | 80.67 |

**Interpretation:**

These top 5 columns do not indicate data quality issues, but rather reflect inherent conditional fields in the IMDb schema. For instance, `endYear` is only populated for series, `deathYear` applies strictly to deceased persons (a minority in the database), and `job` in `principals` is restricted to specific crew roles rather than actors. Consequently, these null values are semantically meaningful and must be preserved without imputation.

---

### 1.3 Data quality issues beyond nulls

**Results:**

| issue | count |
| --- | --- |
| principals.nconst missing in name.basics (FK violation) | 3 824 |
| deathYear < birthYear (impossible dates) | 28 |
| title.basics.startYear out of [1874, 2030] | 6 |
| episode.parentTconst missing in title.basics (FK violation) | 1 |
| ratings.tconst missing in title.basics | 0 |
| principals.tconst missing in title.basics | 0 |
| averageRating not in [0, 10] or numVotes < 0 | 0 |
| runtimeMinutes < 0 | 0 |

**Interpretation:**

Overall, the dataset is remarkably healthy: four checks returned zero errors, confirming that primary foreign keys on `tconst` are strictly maintained. The 3,824 orphaned records in `principals` are not code bugs, but an intentional platform behavior where IMDb removes individuals from `name.basics` for privacy reasons while preserving their historical credits. Other anomalies (impossible dates, aberrant years) account for less than 0.01% of the data. Our cleaning strategy handles these by appending a `_quality_flags` array column rather than blindly dropping valuable rows.

---

### 1.4 Methodological Explanation (Preparation)

**Methodology chosen:**

To process the data efficiently in PySpark, we defined an explicit schema (`StructType`) to avoid automatic inference overhead and ensure correct data types (e.g., parsing runtimes as integers). IMDb's default missing value token (`\N`) is systematically cast to `NULL`. To identify orphaned foreign keys, we used a `left_anti` join—a distributed and scalable equivalent to `NOT IN`. For data cleaning, rather than dropping records with minor quirks, we track them via a `_quality_flags` column to maintain observability. Execution runs locally on Spark with optimized partitioning to comfortably handle millions of rows.

**Alternatives considered and trade-offs:**

* **Pandas:** Too memory-intensive for large tables like `principals` (~753 MB raw).
* **Schema auto-inference:** Faster to write initially, but degrades performance and risks corrupting data types.
* **Hard deletion of anomalies:** Too destructive; discards otherwise valid records (e.g., a title missing its runtime can still have valid ratings and genres).
* **Monolithic queries:** Results in unmaintainable, hard-to-test code that cannot be easily modularized.
