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
## 2. Data Exploration

All queries read the cleaned Parquet tables produced in the Preparation step
(`projet/data/processed/`). Rows flagged in `_quality_flags` are excluded from
`title_basics`, `title_ratings` and `name_basics`. Flagged rows of
`title_principals` are kept: its orphan `nconst` values disappear anyway when
joining with `name_basics`. Full results are written as CSV files under
`projet/output/exploration/`.

### 2.1 Feature-length movies with an atypical runtime per genre (Q1)

**Results:**

Number of atypical movies per genre (10 genres with the most; the complete
list has 22 genres, down to Documentary with 58 and Mystery with 52):

| genre | atypical movies |
| --- | --- |
| Thriller | 2 093 |
| Romance | 2 043 |
| Drama | 1 966 |
| Comedy | 1 614 |
| Crime | 1 179 |
| Action | 838 |
| Horror | 707 |
| Adventure | 632 |
| Biography | 455 |
| Family | 282 |

Examples (extract of `q1_runtime_outliers`, which has one row per atypical
movie and genre):

| title | genre | year | runtime (min) | genre average (min) | z-score |
| --- | --- | --- | --- | --- | --- |
| Barrabas | Crime | 1919 | 489 | 95.5 | 15.4 |
| Les Vampires | Crime | 1915 | 421 | 95.5 | 12.7 |
| The Crimson Stain Mystery | Horror | 1916 | 355 | 88.6 | 11.9 |
| Dick Tracy | Crime | 1937 | 290 | 95.5 | 7.6 |
| Are They Born or Made? | Crime | 1915 | 40 | 95.5 | -2.2 |

**Interpretation:**

In the extract inspected, the atypical movies are almost all **much longer**
than their genre average (up to 15 standard deviations above it), and only
rarely shorter. Many of them are film serials of the 1910s and 1930s, such as
*Les Vampires* (421 minutes) or *Judex*, which IMDb lists as a single feature
although they were released in episodes. The average runtime of a genre is
about 95 minutes for Crime and 89 minutes for Horror, with a standard
deviation of 22 to 26 minutes. A few extreme values inflate this standard
deviation, so the real number of atypical movies is probably underestimated.
Thriller, Romance, Drama and Comedy have the most atypical movies (between
1 600 and 2 100 each), while Documentary and Mystery have the fewest (under
60). These counts are not normalized by genre size, so large genres naturally
come first; a ratio of atypical movies to total movies would be needed to
compare genres fairly.

**Methodological Explanation (Q1):**

*Methodology chosen:* a feature-length movie is a title with
`titleType = 'movie'` and `runtimeMinutes >= 40`, the minimum length used by the
AFI and the Academy for a feature film. Movies with a null runtime are
filtered out rather than imputed, since 62% of runtimes are missing. Each movie
is exploded by genre (the `genres` column is a comma-separated string), so a
multi-genre movie is evaluated against the average of each of its genres. A
movie is *notably different* when the absolute z-score of its runtime within
the genre is above 2, i.e. more than two standard deviations from the genre
mean. The z-score is easy to explain and is computed in a single distributed
aggregation followed by a join.

*Alternatives considered and trade-offs:*

* **IQR rule (Q1 - 1.5 IQR, Q3 + 1.5 IQR):** more robust to extreme values,
  because the mean and standard deviation are themselves pulled by outliers.
  Rejected because it is harder to read and needs approximate quantiles on
  large groups, but it would be a good cross-check.
* **Fixed threshold in minutes (e.g. +/- 30 min):** simple, but it ignores the
  fact that the spread differs by genre (documentaries vary far more than
  animation).
* **Counting a movie only in its first genre:** avoids duplicates but loses
  information; we preferred to keep all genres.

---

### 2.2 Top three most popular genres per year, 1980-2025 (Q2)

**Results:**

Extract (the complete table, 46 years, is in `q2_top3_genres_per_year`):

| year | rank | genre | movies | total votes | avg rating |
| --- | --- | --- | --- | --- | --- |
| 1980 | 1 | Drama | 1 169 | 3 165 796 | 6.13 |
| 1980 | 2 | Adventure | 145 | 2 481 649 | 5.93 |
| 1980 | 3 | Horror | 151 | 2 072 119 | 5.28 |
| 1982 | 1 | Drama | 1 216 | 3 075 114 | 6.15 |
| 1982 | 2 | Sci-Fi | 53 | 2 456 980 | 5.49 |
| 1982 | 3 | Action | 326 | 1 873 784 | 5.75 |
| 1985 | 1 | Comedy | 680 | 4 566 410 | 5.92 |
| 1985 | 2 | Adventure | 155 | 3 410 206 | 5.85 |
| 1985 | 3 | Drama | 1 200 | 3 079 797 | 6.19 |

Later years:

| year | rank | genre | movies | total votes | avg rating |
| --- | --- | --- | --- | --- | --- |
| 2000 | 1 | Drama | 1 591 | 13 026 448 | 6.14 |
| 2000 | 2 | Comedy | 896 | 8 338 954 | 5.83 |
| 2000 | 3 | Adventure | 155 | 6 990 327 | 5.59 |
| 2010 | 1 | Drama | 3 251 | 17 563 329 | 6.22 |
| 2010 | 2 | Action | 760 | 13 335 494 | 5.70 |
| 2010 | 3 | Adventure | 367 | 13 176 510 | 6.10 |
| 2020 | 1 | Drama | 3 379 | 10 478 523 | 6.03 |
| 2020 | 2 | Comedy | 1 673 | 6 514 384 | 5.72 |
| 2020 | 3 | Action | 686 | 6 128 379 | 5.61 |
| 2025 | 1 | Drama | 3 898 | 8 596 360 | 6.44 |
| 2025 | 2 | Action | 1 051 | 7 678 335 | 5.97 |
| 2025 | 3 | Comedy | 2 096 | 5 895 450 | 6.09 |

Number of years (out of 46) in which each genre is in the top 3:

| genre | years in the top 3 |
| --- | --- |
| Drama | 46 |
| Comedy | 34 |
| Action | 29 |
| Adventure | 16 |
| Crime | 9 |
| Horror | 2 |
| Thriller | 1 |
| Sci-Fi | 1 |

**Interpretation:**

Drama is in the top 3 in all 46 years, followed by Comedy (34 years) and
Action (29 years); Adventure (16) and Crime (9) appear more occasionally, and
Horror, Thriller and Sci-Fi only once or twice. Popularity is not driven by
production volume alone: in 1982, Sci-Fi ranks second with only 53
movies, but they gathered about 2.5 million votes, close to Drama's 3.1
million for 1 216 movies. Horror reaches the top 3 in 1980 and 1981 with the
lowest average rating of the group (5.3 and 5.5), which shows that a genre can
be popular without being well rated.

In the later years, Drama keeps the first place (2000, 2010, 2020 and 2025),
while the number of Drama movies released per year more than triples between
1980 (1 169) and 2025 (3 898). Comedy and Action alternate for the next
places, and Adventure still reaches the top 3 in 2000 and 2010. The total
votes of the top genre peak around 2010 (17.6 million for Drama) and then
decrease (10.5 million in 2020 and 8.6 million in 2025), probably because the
most recent movies have had less time to accumulate votes.

**Methodological Explanation (Q2):**

*Methodology chosen:* popularity is defined as the **total number of votes**
received by the movies of a genre released in a given year. Votes measure the
size of the audience, which is what "popular" means here. For each
`(startYear, genre)` pair we compute the number of movies, the total votes and
the mean rating, then rank the genres within each year with a window function
(`row_number` over `startYear`, ordered by total votes descending) and keep the
first three. Only titles with a rating (join with `title_ratings`) and a
`startYear` between 1980 and 2025 are used.

*Alternatives considered and trade-offs:*

* **Number of movies released:** measures production volume, not popularity;
  a genre can be produced a lot and watched very little.
* **Average rating:** favors niche genres with few, enthusiastic voters.
* **Votes-weighted score (Bayesian average, as on IMDb Top 250):** combines
  quality and audience size, but depends on arbitrary parameters and is harder
  to explain.
* **`row_number` versus `rank`:** `row_number` always returns exactly three
  genres; a tie on total votes is extremely unlikely with sums of this size.

---

### 2.3 Top three genres compared with the other genres (Q3)

**Results:**

Extract of the comparison (complete series in `q3_top3_vs_rest`):

| year | group | avg rating | avg movies per genre | avg votes per genre |
| --- | --- | --- | --- | --- |
| 1980 | top3 | 5.78 | 488 | 2 573 188 |
| 1980 | rest | 6.16 | 119 | 462 943 |
| 1990 | top3 | 5.89 | 794 | 4 644 062 |
| 1990 | rest | 6.01 | 126 | 875 608 |
| 1994 | top3 | 5.93 | 748 | 9 306 830 |
| 1994 | rest | 6.06 | 115 | 1 053 597 |
| 2000 | top3 | 5.85 | 881 | 9 451 910 |
| 2000 | rest | 6.06 | 163 | 1 826 014 |
| 2010 | top3 | 6.01 | 1 459 | 14 691 778 |
| 2010 | rest | 6.11 | 419 | 3 151 276 |
| 2016 | top3 | 5.99 | 1 931 | 16 817 891 |
| 2016 | rest | 6.27 | 585 | 3 047 497 |
| 2020 | top3 | 5.79 | 1 913 | 7 707 095 |
| 2020 | rest | 5.98 | 408 | 1 294 324 |
| 2025 | top3 | 6.17 | 2 348 | 7 390 048 |
| 2025 | rest | 6.41 | 453 | 1 557 270 |

**Interpretation:**

In every year inspected (1980 to 1995, then 2000, 2005, 2010, 2015 and 2016 to
2025), the three most popular genres have a slightly **lower** average rating
(about 5.8 to 6.2) than the remaining genres (about 6.0 to 6.4), but they contain
roughly three to seven times more movies per genre and collect roughly five to
nine times more votes per genre. Popular genres are therefore mass-production
genres with a large audience; their popularity does not come from better
ratings. The smaller genres are rated slightly higher on average, which is
consistent with niche audiences rating more generously. The volume of movies
per genre is much higher in recent years (about 450 to 585 for the rest and
1 750 to 2 750 for the top 3, against 115 to 140 and 480 to 840 in the 1980s),
reflecting the growth of film production over time.



**Methodological Explanation (Q3):**

*Methodology chosen:* reusing the ranking of Q2, each `(year, genre)` row is
labelled `top3` or `rest`. For each year and group, we average over genres the
mean rating, the number of movies and the number of votes. Averaging at the
genre level gives each genre the same weight, so that the comparison is not
dominated by the biggest genres.

*Alternatives considered and trade-offs:*

* **Averaging over movies instead of genres:** the top 3 would weigh much more
  than the rest and the comparison would be biased by construction.
* **Statistical test (t-test) per year:** more rigorous, but with about 20
  genres per year the "rest" group is small, and the descriptive comparison
  already shows a stable gap.
* **Comparing the top 3 with the median genre only:** simpler, but hides the
  spread within the rest group.

---

### 2.4 Directors with exactly one movie rated >= 8.0 and several others averaging <= 6.0 (Q4)

**Results:**

44 directors satisfy the three conditions. Extract (sorted by best rating):

| director | best rating | other movies | average of other movies |
| --- | --- | --- | --- |
| Irvin Kershner | 8.7 | 9 | 5.94 |
| Sundar C. | 8.6 | 10 | 5.52 |
| M. Night Shyamalan | 8.2 | 15 | 5.87 |
| Sinan Cetin | 8.0 | 7 | 4.84 |
| Gunasekhar | 8.0 | 6 | 5.38 |

The complete list is in `q4_one_hit_directors`.

**Interpretation:**

These directors have a single outstanding film and an otherwise average to
weak filmography. The result is most meaningful for directors with many other
movies (Shyamalan, 15 movies; Sundar C., 10 movies), because their average is
based on a large sample. Directors with only two other movies can appear by
chance. A large share of the names comes from Indian cinema, which has many
prolific directors and a high volume of votes.

**Limits:** the same person can appear under two IMDb identifiers (for example
"Sateesh Paramveda" and "Satish Paramvedha" show identical figures), which
inflates the count slightly; and the result depends on the vote threshold.

**Methodological Explanation (Q4):**

*Methodology chosen:* we select credits with `category = 'director'` in
`title_principals`, keep distinct `(director, movie)` pairs, and join with the
movies (`titleType = 'movie'`) having a rating and **at least 1 000 votes**, so
that ratings based on a handful of votes are ignored. For each director, a
single aggregation computes the number of movies, the number of movies rated
>= 8.0 and the sum and maximum of ratings. A director is kept if exactly one
movie is rated >= 8.0, if he or she has at least two other movies, and if the
mean rating of those other movies is <= 6.0. Because there is exactly one movie
>= 8.0, it is also the maximum, so the average of the other movies is
`(sum - max) / (count - 1)`, which avoids a second pass over the data. "Other
movies" means every movie of the director except the one rated >= 8.0.

*Alternatives considered and trade-offs:*

* **No vote threshold:** returns many more directors, but includes ratings
  such as 9.8 coming from a few votes. A higher threshold (e.g. 5 000) would
  be more robust but shrinks the result.
* **Interpreting "other movies" as only those rated <= 6.0:** the average
  condition would then be trivially true; we use all remaining movies instead,
  which is the stricter and more natural reading of the question.
* **Self-join of high-rated and low-rated movies:** gives the same result but
  needs a shuffle-heavy join; conditional aggregation scans the data once.
* **Merging duplicate identifiers of the same person:** not possible reliably
  with IMDb data alone; documented as a limit.
