# -*- coding: utf-8 -*-

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

BASE_URL_HAL = "https://api.archives-ouvertes.fr/search/"


def _first(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _choose_field(doc: Dict, keys: List[str]) -> Optional[str]:
    for key in keys:
        if key in doc and doc[key]:
            return _first(doc[key])
    return None


def _parse_iso_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _build_hal_date_filter(
    days_back: Optional[int],
    from_date: Optional[str],
    until_date: Optional[str],
    use_date_filter: Optional[bool],
) -> Optional[str]:
    if days_back is not None:
        if days_back <= 0:
            raise ValueError("`days_back` doit etre > 0.")
        return f"submittedDate_tdate:[NOW-{int(days_back)}DAYS/DAY TO NOW/HOUR]"

    from_dt = _parse_iso_date(from_date)
    until_dt = _parse_iso_date(until_date)

    effective_filter = bool(use_date_filter) if use_date_filter is not None else (
        from_dt is not None or until_dt is not None
    )
    if not effective_filter:
        return None

    from_bound = "*" if from_dt is None else from_dt.strftime("%Y-%m-%dT00:00:00Z")
    until_bound = "NOW/HOUR" if until_dt is None else until_dt.strftime("%Y-%m-%dT23:59:59Z")
    return f"submittedDate_tdate:[{from_bound} TO {until_bound}]"


def fetch_hal_theses(
    query: str = "*:*",
    days_back: Optional[int] = None,
    rows_per_page: int = 200,
    max_total_results: int = 5000,
    from_date: Optional[str] = None,
    until_date: Optional[str] = None,
    use_date_filter: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Recupere des theses HAL avec pagination.

    Mode historique:
      - use_date_filter=False
      - days_back=None
    """
    if rows_per_page <= 0:
        raise ValueError("`rows_per_page` doit etre > 0.")
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

    date_filter = _build_hal_date_filter(
        days_back=days_back,
        from_date=from_date,
        until_date=until_date,
        use_date_filter=use_date_filter,
    )

    all_rows: List[Dict[str, Any]] = []
    start = 0

    while start < max_total_results:
        rows_this_page = min(rows_per_page, max_total_results - start)
        params = [
            ("q", query or "*:*"),
            ("wt", "json"),
            ("rows", rows_this_page),
            ("start", start),
            ("sort", "submittedDate_tdate desc"),
            (
                "fl",
                "halId_s,docType_s,"
                "submittedDate_tdate,defenseDate_s,"
                "title_s,title_en_s,title_fr_s,label_s,"
                "abstract_s,abstract_en_s,abstract_fr_s,"
                "authFullName_s,"
                "authorityInstitution_s",
            ),
            ("fq", "docType_s:THESE"),
        ]
        if date_filter:
            params.append(("fq", date_filter))

        resp = requests.get(BASE_URL_HAL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        response = data.get("response", {})
        docs = response.get("docs", [])
        num_found = int(response.get("numFound", 0))

        if not docs:
            break

        for d in docs:
            title = _choose_field(d, ["title_en_s", "title_s", "label_s", "title_fr_s"])
            abstract = _choose_field(d, ["abstract_en_s", "abstract_s", "abstract_fr_s"])
            author = _choose_field(d, ["authFullName_s"])

            inst = _choose_field(d, ["authorityInstitution_s"])
            journal = f"PhD Thesis of {inst}" if inst else "PhD Thesis"

            published_date = d.get("defenseDate_s")
            hal_id = d.get("halId_s")
            doi = hal_id or ""
            url = f"https://hal.science/{hal_id}" if hal_id else ""

            all_rows.append(
                {
                    "title": title or "",
                    "authors": author or "",
                    "journal": journal,
                    "published_date": published_date or "",
                    "doi": doi,
                    "url": url,
                    "issn": "",
                    "abstract": abstract or "",
                    "source": "hal",
                }
            )

        start += len(docs)
        if start >= num_found:
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


def fetch_recent_hal_theses(days_back: int, rows_per_page: int = 200) -> pd.DataFrame:
    """
    Wrapper de compatibilite vers l'ancien nom.
    """
    return fetch_hal_theses(
        query="*:*",
        days_back=days_back,
        rows_per_page=rows_per_page,
        max_total_results=5000,
        use_date_filter=True,
    )


if __name__ == "__main__":
    df_hal = fetch_recent_hal_theses(days_back=31)
    print(df_hal.head())
    print(len(df_hal))
