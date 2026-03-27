import random
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import feedparser
import pandas as pd
import requests

BASE_URL = "https://export.arxiv.org/api/query"

_ARXIV_MIN_DELAY_S = 3.0
_last_arxiv_request_ts = 0.0

CATEGORY_MAP = {
    "physics": "Physics",
    "math": "Mathematics",
    "cs": "Computer Science",
    "stat": "Statistics",
    "eess": "Electrical Engineering and Systems Science",
}

DEFAULT_ARXIV_QUERY = "(cat:physics* OR cat:math* OR cat:cs* OR cat:stat* OR cat:eess*)"


def get_arxiv_category_prefix(entry) -> Optional[str]:
    if not hasattr(entry, "tags") or not entry.tags:
        return None
    term = entry.tags[0].term
    return term.split(".")[0]


def _parse_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except ValueError:
        return None


def _resolve_date_window(
    days_back: Optional[int],
    from_date: Optional[str],
    until_date: Optional[str],
    use_date_filter: Optional[bool],
) -> tuple[bool, Optional[date], Optional[date]]:
    parsed_from = _parse_iso_date(from_date)
    parsed_until = _parse_iso_date(until_date)

    if days_back is not None:
        if days_back <= 0:
            raise ValueError("`days_back` doit etre > 0 quand utilise.")
        parsed_from = (datetime.utcnow() - timedelta(days=int(days_back))).date()

    effective_filter = bool(use_date_filter) if use_date_filter is not None else (
        parsed_from is not None or parsed_until is not None
    )

    if not effective_filter:
        return False, None, None

    if parsed_from and parsed_until and parsed_from > parsed_until:
        raise ValueError("`from_date` doit etre <= `until_date`.")

    return True, parsed_from, parsed_until


def _date_in_window(pub_date: date, from_date: Optional[date], until_date: Optional[date]) -> bool:
    if from_date is not None and pub_date < from_date:
        return False
    if until_date is not None and pub_date > until_date:
        return False
    return True


def _request_with_retry(
    params,
    max_retries: int = 8,
    base_sleep: float = 5.0,
) -> requests.Response:
    global _last_arxiv_request_ts

    session = requests.Session()
    headers = {
        "User-Agent": "BibliographieScript/0.1 (mailto:sebastien.menigot@eseo.fr)"
    }
    timeout = (10, 60)

    for attempt in range(max_retries):
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
            print(f"[arXiv] Erreur reseau ({type(e).__name__}). Retry dans {wait:.1f} s...")
            time.sleep(wait)
            continue

    raise RuntimeError(f"Echec apres {max_retries} tentatives (arXiv).")


def fetch_arxiv_articles(
    query: str = DEFAULT_ARXIV_QUERY,
    days_back: Optional[int] = None,
    from_date: Optional[str] = None,
    until_date: Optional[str] = None,
    use_date_filter: Optional[bool] = None,
    page_size: int = 100,
    max_total_results: int = 1000,
) -> pd.DataFrame:
    """
    Recupere des articles arXiv avec pagination.

    Mode historique:
      - use_date_filter=False
      - days_back=None
    """
    if page_size <= 0:
        raise ValueError("`page_size` doit etre > 0.")
    if max_total_results <= 0:
        return pd.DataFrame(
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
            ]
        )

    date_filter_on, parsed_from, parsed_until = _resolve_date_window(
        days_back=days_back,
        from_date=from_date,
        until_date=until_date,
        use_date_filter=use_date_filter,
    )

    all_rows: List[Dict[str, Any]] = []
    start = 0
    older_reached = False

    while start < max_total_results and not older_reached:
        page_len = min(page_size, max_total_results - start)
        params = {
            "search_query": query or DEFAULT_ARXIV_QUERY,
            "start": start,
            "max_results": page_len,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }

        print(f"[arXiv] Requete start={start}, page_size={page_len}")
        resp = _request_with_retry(params=params)
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
                published_dt = datetime.strptime(published_raw, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                continue

            published_day = published_dt.date()
            if date_filter_on:
                if parsed_from is not None and published_day < parsed_from:
                    older_reached = True
                    break
                if not _date_in_window(published_day, parsed_from, parsed_until):
                    continue

            title = (getattr(entry, "title", "") or "").replace("\n", " ").strip()
            abstract = (getattr(entry, "summary", "") or "").replace("\n", " ").strip()

            authors_list = []
            if hasattr(entry, "authors"):
                authors_list = [a.name for a in entry.authors if hasattr(a, "name")]
            authors = ", ".join(authors_list)

            cat_prefix = get_arxiv_category_prefix(entry)
            if cat_prefix in CATEGORY_MAP:
                journal = f"arXiv {CATEGORY_MAP[cat_prefix]}"
            else:
                journal = "arXiv"

            doi = getattr(entry, "arxiv_doi", "") or ""
            url = getattr(entry, "id", "") or ""

            all_rows.append(
                {
                    "title": title,
                    "authors": authors,
                    "journal": journal,
                    "published_date": published_day.isoformat(),
                    "doi": doi,
                    "url": url,
                    "issn": "",
                    "abstract": abstract,
                    "source": "arxiv",
                }
            )

        start += len(entries)
        if len(entries) < page_len:
            break

    return pd.DataFrame(
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


def fetch_recent_arxiv_articles(
    days_back: int,
    page_size: int = 100,
    max_total_results: int = 1000,
) -> pd.DataFrame:
    """
    Wrapper de compatibilite vers l'ancien nom.
    """
    return fetch_arxiv_articles(
        query=DEFAULT_ARXIV_QUERY,
        days_back=days_back,
        use_date_filter=True,
        page_size=page_size,
        max_total_results=max_total_results,
    )
