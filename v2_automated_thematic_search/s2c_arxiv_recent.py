# s2c_arxiv_recent.py
import time
import requests
import feedparser
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import random

BASE_URL = "https://export.arxiv.org/api/query"

# arXiv recommande ~3s entre requêtes consécutives
_ARXIV_MIN_DELAY_S = 3.0
_last_arxiv_request_ts = 0.0

CATEGORY_MAP = {
    "physics": "Physics",
    "math": "Mathematics",
    "cs": "Computer Science",
    "stat": "Statistics",
    "eess": "Electrical Engineering and Systems Science",
}


def get_arxiv_category_prefix(entry) -> Optional[str]:
    """Préfixe de la catégorie principale ('cs', 'physics', 'math', 'stat', 'eess')."""
    if not hasattr(entry, "tags") or not entry.tags:
        return None
    term = entry.tags[0].term  # ex: "cs.AI"
    prefix = term.split(".")[0]
    return prefix

def _request_with_retry(
    params,
    max_retries: int = 8,
    base_sleep: float = 5.0,
) -> requests.Response:
    """Requête arXiv avec gestion du HTTP 429 + timeouts, et throttling 3s global."""
    global _last_arxiv_request_ts

    session = requests.Session()
    headers = {
        "User-Agent": "BibliographieScript/0.1 (mailto:sebastien.menigot@eseo.fr)"
    }

    # timeout séparé (connect, read)
    timeout = (10, 60)

    for attempt in range(max_retries):
        # Throttling: garantit >= 3s entre deux hits arXiv, même en cas de retries
        now = time.monotonic()
        elapsed = now - _last_arxiv_request_ts
        if elapsed < _ARXIV_MIN_DELAY_S:
            time.sleep(_ARXIV_MIN_DELAY_S - elapsed)

        try:
            _last_arxiv_request_ts = time.monotonic()
            resp = session.get(BASE_URL, params=params, headers=headers, timeout=timeout)

            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                if ra is not None:
                    try:
                        wait = float(ra)
                    except ValueError:
                        wait = base_sleep * (attempt + 1)
                else:
                    wait = base_sleep * (attempt + 1)

                # jitter léger
                wait *= (1.0 + 0.2 * random.random())

                print(f"[arXiv] 429 Too Many Requests. Retry dans {wait:.1f} s...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as e:
            wait = base_sleep * (attempt + 1) * (1.0 + 0.2 * random.random())
            print(f"[arXiv] Timeout ({type(e).__name__}). Retry dans {wait:.1f} s...")
            time.sleep(wait)
            continue

        except requests.exceptions.RequestException as e:
            wait = base_sleep * (attempt + 1) * (1.0 + 0.2 * random.random())
            print(f"[arXiv] Erreur réseau ({type(e).__name__}). Retry dans {wait:.1f} s...")
            time.sleep(wait)
            continue

    raise RuntimeError(f"Échec après {max_retries} tentatives (arXiv).")

def fetch_recent_arxiv_articles(
    days_back: int,
    page_size: int = 100,
    max_total_results: int = 1000,
) -> pd.DataFrame:
    """
    Articles arXiv soumis dans les `days_back` derniers jours
    pour les catégories : physics*, math*, cs*, stat*, eess*.

    Retourne un DataFrame :
    title, authors, journal, published_date, doi, url, issn, abstract, source
    """
    if days_back <= 0:
        raise ValueError("`days_back` doit être > 0")

    now = datetime.utcnow()
    cutoff = now - timedelta(days=days_back)

    category_query = "(cat:physics* OR cat:math* OR cat:cs* OR cat:stat* OR cat:eess*)"

    all_rows: List[Dict[str, Any]] = []
    start = 0
    older_reached = False

    while not older_reached and start < max_total_results:
        params = {
            "search_query": category_query,
            "start": start,
            "max_results": page_size,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }

        print(f"[arXiv] Requête start={start}, page_size={page_size}")
        resp = _request_with_retry(params=params)

        # petite pause pour rester sympa avec arXiv
        time.sleep(3.0)

        feed = feedparser.parse(resp.text)
        entries = feed.entries
        if not entries:
            break

        for entry in entries:
            published_raw = getattr(entry, "published", None)
            if not published_raw:
                continue
            try:
                published_dt = datetime.strptime(
                    published_raw, "%Y-%m-%dT%H:%M:%SZ"
                )
            except ValueError:
                continue

            if published_dt < cutoff:
                older_reached = True
                break

            published_date = published_dt.date().isoformat()

            title = (getattr(entry, "title", "") or "").replace("\n", " ").strip()
            abstract = (getattr(entry, "summary", "") or "").replace("\n", " ").strip()

            authors_list = []
            if hasattr(entry, "authors"):
                authors_list = [
                    a.name for a in entry.authors if hasattr(a, "name")
                ]
            authors = ", ".join(authors_list)

            cat_prefix = get_arxiv_category_prefix(entry)
            if cat_prefix in CATEGORY_MAP:
                domain_name = CATEGORY_MAP[cat_prefix]
                journal = f"arXiv {domain_name}"
            else:
                journal = "arXiv"

            doi = getattr(entry, "arxiv_doi", "") or ""
            url = getattr(entry, "id", "") or ""
            issn = ""

            row = {
                "title": title,
                "authors": authors,
                "journal": journal,
                "published_date": published_date,
                "doi": doi,
                "url": url,
                "issn": issn,
                "abstract": abstract,
                "source": "arxiv",
            }
            all_rows.append(row)

        start += page_size

    df = pd.DataFrame(
        all_rows,
        columns=[
            "title",
            "authors",
            "journal",
            "published_date",
            "doi",
            "url",
            "issn",
            "abstract",
            "source",
        ],
    )
    return df
