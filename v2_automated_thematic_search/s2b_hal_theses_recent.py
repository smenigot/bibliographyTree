# s2b_hal_theses_recent.py
# -*- coding: utf-8 -*-

import requests
import pandas as pd
from typing import List, Dict, Any, Optional

BASE_URL_HAL = "https://api.archives-ouvertes.fr/search/"

def _first(value: Any) -> Any:
    """Si HAL renvoie une liste, on prend le premier élément, sinon la valeur brute."""
    if isinstance(value, list):
        return value[0] if value else None
    return value

def _choose_field(doc: Dict, keys: List[str]) -> Optional[str]:
    """
    Retourne la première valeur non vide parmi une liste de clés HAL,
    en gérant le fait que les champs peuvent être mono ou multi-valués.
    """
    for key in keys:
        if key in doc and doc[key]:
            return _first(doc[key])
    return None

def fetch_recent_hal_theses(days_back: int, rows_per_page: int = 200) -> pd.DataFrame:
    """
    Récupère les thèses déposées sur HAL dans les `days_back` derniers jours
    et renvoie un DataFrame avec colonnes :
    title, authors, journal, published_date, doi, url, issn, abstract
    """
    if days_back <= 0:
        raise ValueError("`days_back` doit être > 0")

    all_rows: List[Dict[str, Any]] = []
    start = 0

    # Filtre sur la date de dépôt (dépôts des `days_back` derniers jours)
    date_filter = f"submittedDate_tdate:[NOW-{days_back}DAYS/DAY TO NOW/HOUR]"

    while True:
        params = [
            ("q", "*:*"),
            ("wt", "json"),
            ("rows", rows_per_page),
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
            ("fq", date_filter),
            # éventuellement :
            # ("fq", "submitType_s:(-notice)"),
        ]

        resp = requests.get(BASE_URL_HAL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        response = data.get("response", {})
        docs = response.get("docs", [])
        num_found = response.get("numFound", 0)

        if not docs:
            break

        for d in docs:
            # Titre : EN -> sinon FR/générique
            title = _choose_field(
                d, ["title_en_s", "title_s", "label_s", "title_fr_s"]
            )
            # Abstract : EN -> sinon FR/générique
            abstract = _choose_field(
                d, ["abstract_en_s", "abstract_s", "abstract_fr_s"]
            )
            # Auteur principal
            author = _choose_field(d, ["authFullName_s"])

            # Université / organisme de délivrance
            inst = _choose_field(d, ["authorityInstitution_s"])
            if inst:
                journal = f"PhD Thesis of {inst}"
            else:
                journal = "PhD Thesis"

            # Date de soutenance
            published_date = d.get("defenseDate_s")

            # Identifiant HAL (on le met dans 'doi' pour homogénéiser)
            hal_id = d.get("halId_s")
            doi = hal_id or ""

            # URL HAL
            url = f"https://hal.science/{hal_id}" if hal_id else ""

            row = {
                "title": title or "",
                "authors": author or "",
                "journal": journal,
                "published_date": published_date,
                "doi": doi,
                "url": url,
                "issn": "",
                "abstract": abstract or "",
            }
            all_rows.append(row)

        start += len(docs)
        if start >= num_found:
            break

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
        ],
    )
    return df

if __name__ == "__main__":
    # test rapide
    df_hal = fetch_recent_hal_theses(days_back=31)
    print(df_hal.head())
    print(len(df_hal))
