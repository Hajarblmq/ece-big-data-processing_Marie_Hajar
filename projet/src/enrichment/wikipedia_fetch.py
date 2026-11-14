"""Step 2 (plain Python): fetch Wikipedia texts for the selected IDs.

Data gathering is done outside Spark, as the subject allows. The script:

1. maps each IMDb ID to its English Wikipedia article through Wikidata
   (property P345 "IMDb ID"), with batched SPARQL queries. This is far
   more reliable than searching Wikipedia by title (remakes, homonyms);
2. downloads, from the MediaWiki API, the full plain-text article of
   each film (the synopsis is extracted later in Spark) and the
   introduction of each person's article (their biography).

Politeness: requests are sequential, spaced by a minimum delay (longer
for the Wikidata Query Service, whose limits are stricter), carry a
descriptive User-Agent (Wikimedia policy), use `maxlag` on the MediaWiki
API, and back off on HTTP 429/5xx using the `Retry-After` header. Every
response is appended to a JSONL file, so an interrupted run resumes
where it stopped without requesting the same page twice.

Run from the `projet` folder:
    python -m src.enrichment.wikipedia_fetch
"""
import argparse
import csv
import glob
import json
import os
import time
import urllib.parse
from datetime import datetime, timezone

import requests

from .common import IDS_DIR, LINKS_FILE, PAGES_FILE

SPARQL_URL = "https://query.wikidata.org/sparql"
API_URL = "https://en.wikipedia.org/w/api.php"
REPO_URL = "https://github.com/Hajarblmq/ece-big-data-processing_Marie_Hajar"
CONTACT = os.environ.get("WIKI_CONTACT", REPO_URL)
USER_AGENT = f"ECE-BigData-Student-Project/1.0 ({CONTACT})"

SPARQL_BATCH = 200
PEOPLE_BATCH = 20  # MediaWiki limit for intro extracts of several pages


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class PoliteClient:
    """Sequential HTTP client with a minimum delay and retries."""

    def __init__(self, min_interval: float, max_retries: int = 6):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.min_interval = min_interval
        self.max_retries = max_retries
        self.last_call = 0.0
        self.calls = 0

    def get_json(self, url: str, params: dict) -> dict:
        backoff = 5.0
        for _ in range(self.max_retries):
            wait = self.min_interval - (time.time() - self.last_call)
            if wait > 0:
                time.sleep(wait)
            self.last_call = time.time()
            self.calls += 1
            try:
                response = self.session.get(url, params=params, timeout=60)
            except requests.RequestException as error:
                print(f"  network error ({error}), retry in {backoff:.0f}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)
                continue
            if response.status_code in (429, 500, 502, 503, 504):
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else backoff
                print(f"  HTTP {response.status_code}, retry in {delay:.0f}s")
                time.sleep(delay)
                backoff = min(backoff * 2, 300)
                continue
            response.raise_for_status()
            data = response.json()
            if data.get("error", {}).get("code") == "maxlag":
                time.sleep(backoff)
                continue
            return data
        raise RuntimeError(f"giving up after {self.max_retries} attempts")


def read_ids() -> list:
    """(imdb_id, kind) pairs written by select_targets.py."""
    rows = []
    for path in glob.glob(str(IDS_DIR / "part-*.csv")):
        with open(path, encoding="utf-8") as f:
            rows.extend((r["imdb_id"], r["kind"]) for r in csv.DictReader(f))
    if not rows:
        raise SystemExit(f"No IDs in {IDS_DIR}: run select_targets first")
    return rows


def read_jsonl(path) -> list:
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def append_jsonl(path, records: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def sparql_query(imdb_ids: list) -> str:
    values = " ".join(f'"{i}"' for i in imdb_ids)
    return (
        "SELECT ?imdb ?item ?article WHERE { "
        f"VALUES ?imdb {{ {values} }} "
        "?item wdt:P345 ?imdb . "
        "OPTIONAL { ?article schema:about ?item ; "
        "schema:isPartOf <https://en.wikipedia.org/> . } }"
    )


def title_from_url(url: str) -> str:
    return urllib.parse.unquote(url.rsplit("/wiki/", 1)[1]).replace("_", " ")


def parse_sparql(data: dict, requested: list, kinds: dict) -> list:
    """One link record per requested ID, found or not."""
    found = {}
    for b in data["results"]["bindings"]:
        imdb_id = b["imdb"]["value"]
        record = found.setdefault(imdb_id, {
            "wikidata_item": b["item"]["value"], "wiki_title": None,
        })
        if "article" in b and record["wiki_title"] is None:
            record["wiki_title"] = title_from_url(b["article"]["value"])
    retrieved = now()
    return [
        {
            "imdb_id": i,
            "kind": kinds[i],
            "wikidata_item": found.get(i, {}).get("wikidata_item"),
            "wiki_title": found.get(i, {}).get("wiki_title"),
            "link_source": SPARQL_URL,
            "retrieved_at": retrieved,
        }
        for i in requested
    ]


def resolve_links(client: PoliteClient, ids: list) -> None:
    kinds = dict(ids)
    done = {r["imdb_id"] for r in read_jsonl(LINKS_FILE)}
    todo = [i for i, _ in ids if i not in done]
    print(f"Wikidata links: {len(done)} cached, {len(todo)} to resolve")
    for start in range(0, len(todo), SPARQL_BATCH):
        batch = todo[start:start + SPARQL_BATCH]
        data = client.get_json(SPARQL_URL, {
            "query": sparql_query(batch), "format": "json",
        })
        append_jsonl(LINKS_FILE, parse_sparql(data, batch, kinds))
        print(f"  {min(start + SPARQL_BATCH, len(todo))}/{len(todo)}")


def api_params(titles: list, intro_only: bool) -> dict:
    params = {
        "action": "query", "format": "json", "formatversion": "2",
        "prop": "extracts|info", "explaintext": "1",
        "exsectionformat": "wiki", "redirects": "1", "maxlag": "5",
        "titles": "|".join(titles),
    }
    if intro_only:
        params.update({"exintro": "1", "exlimit": "max"})
    return params


def parse_pages(data: dict, requested: list) -> list:
    """Page records keyed back to the requested titles."""
    query = data.get("query", {})
    renamed = {}
    for step in query.get("normalized", []) + query.get("redirects", []):
        renamed[step["from"]] = step["to"]
    pages = {p["title"]: p for p in query.get("pages", [])}
    retrieved = now()
    records = []
    for title in requested:
        final = title
        for _ in range(3):  # normalization, then redirect
            final = renamed.get(final, final)
        page = pages.get(final, {})
        revid = page.get("lastrevid")
        url = None
        if revid:
            url = ("https://en.wikipedia.org/w/index.php?title="
                   f"{urllib.parse.quote(final.replace(' ', '_'))}"
                   f"&oldid={revid}")
        records.append({
            "requested_title": title,
            "page_title": final if page and not page.get("missing") else None,
            "pageid": page.get("pageid"),
            "revid": revid,
            "source_url": url,
            "extract": page.get("extract"),
            "retrieved_at": retrieved,
        })
    return records


def fetch_pages(client: PoliteClient, max_pages: int | None) -> None:
    links = [r for r in read_jsonl(LINKS_FILE) if r["wiki_title"]]
    done = {r["requested_title"] for r in read_jsonl(PAGES_FILE)}
    films = sorted({r["wiki_title"] for r in links if r["kind"] == "film"}
                   - done)
    people = sorted({r["wiki_title"] for r in links
                     if r["kind"] == "person"} - done - set(films))
    if max_pages is not None:
        films, people = films[:max_pages], people[:max_pages]
    print(f"Wikipedia pages: {len(done)} cached, {len(films)} films and "
          f"{len(people)} people to fetch")

    # Full article for films (one page per request: MediaWiki limit).
    for n, title in enumerate(films, 1):
        data = client.get_json(API_URL, api_params([title], False))
        append_jsonl(PAGES_FILE, parse_pages(data, [title]))
        if n % 100 == 0:
            print(f"  films {n}/{len(films)}")

    # Introduction only for people, 20 pages per request.
    for start in range(0, len(people), PEOPLE_BATCH):
        batch = people[start:start + PEOPLE_BATCH]
        data = client.get_json(API_URL, api_params(batch, True))
        append_jsonl(PAGES_FILE, parse_pages(data, batch))
        if (start // PEOPLE_BATCH) % 20 == 0:
            print(f"  people {min(start + PEOPLE_BATCH, len(people))}"
                  f"/{len(people)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--delay", type=float, default=0.25,
                        help="minimum seconds between two Wikipedia calls")
    parser.add_argument("--sparql-delay", type=float, default=5.0,
                        help="minimum seconds between two Wikidata queries")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="fetch at most N films and N people (tests)")
    args = parser.parse_args()

    wikidata = PoliteClient(args.sparql_delay)
    wikipedia = PoliteClient(args.delay)
    start = time.time()
    resolve_links(wikidata, read_ids())
    fetch_pages(wikipedia, args.max_pages)
    print(f"Done: {wikidata.calls} Wikidata and {wikipedia.calls} Wikipedia "
          f"requests in {(time.time() - start) / 60:.1f} min")


if __name__ == "__main__":
    main()
