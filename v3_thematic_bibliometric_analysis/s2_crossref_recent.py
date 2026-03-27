# s2_crossref_recent.py
import asyncio
import csv
import os
import re
from datetime import date, datetime, timedelta
from html import unescape
from typing import Optional

import aiohttp
import pandas as pd

BASE_URL = "https://api.crossref.org/works"
MAX_CROSSREF_ROWS = 1000

def clean_xml_text(text: str) -> str:
    """
    Supprime toutes les balises XML/HTML et leur contenu lorsque présent.
    Nettoie également les entités HTML & les espaces multiples.
    """
    if not text:
        return ""

    # 1) Enlever toutes les balises <tag>...</tag> (et leur contenu si nécessaire)
    # Exemple : <italic>foo</italic> -> ""
    text = re.sub(r"<[^>]*>", " ", text)

    # 2) décoder entités HTML (&amp; -> &)
    text = unescape(text)

    # 3) nettoyer espaces multiples
    text = re.sub(r"\s+", " ", text).strip()

    return text


def extract_published_date(item: dict) -> Optional[date]:
    for key in ["published-print", "published-online", "published"]:
        if key in item and "date-parts" in item[key]:
            parts = item[key]["date-parts"][0]
            year = parts[0]
            month = parts[1] if len(parts) > 1 else 1
            day = parts[2] if len(parts) > 2 else 1
            return datetime(year, month, day).date()
    return None


def extract_authors(item: dict):
    authors = item.get("author", [])
    names = []
    for a in authors:
        given = a.get("given", "")
        family = a.get("family", "")
        full = " ".join([given, family]).strip()
        if full:
            names.append(full)
    return "; ".join(names)


def extract_abstract(item: dict) -> str:
    raw = item.get("abstract")
    if not raw:
        return ""
    txt = re.sub(r"<[^>]+>", " ", raw)
    txt = unescape(txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


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

    if parsed_from is None and parsed_until is None:
        # Date filter active mais aucune borne fournie -> bornes implicites raisonnables
        parsed_until = datetime.utcnow().date()

    if parsed_from and parsed_until and parsed_from > parsed_until:
        raise ValueError("`from_date` doit etre <= `until_date`.")

    return True, parsed_from, parsed_until


def build_params(
    issn,
    mailto,
    rows,
    cursor=None,
    from_date: Optional[date] = None,
    until_date: Optional[date] = None,
    bibliographic_query: Optional[str] = None,
):
    filter_parts = [f"issn:{issn}"]
    if from_date is not None:
        filter_parts.append(f"from-pub-date:{from_date.isoformat()}")
    if until_date is not None:
        filter_parts.append(f"until-pub-date:{until_date.isoformat()}")

    params = {
        "filter": ",".join(filter_parts),
        "mailto": mailto,
        "sort": "published",
        "order": "desc",
        "rows": rows,
    }
    if isinstance(bibliographic_query, str) and bibliographic_query.strip():
        params["query.bibliographic"] = bibliographic_query.strip()
    if cursor is not None:
        params["cursor"] = cursor
    return params


def _compile_patterns(keywords, use_regex: bool):
    cleaned = [k for k in (keywords or []) if isinstance(k, str) and k.strip()]
    if not cleaned:
        return None
    if use_regex:
        pattern = "|".join(cleaned)
    else:
        pattern = "|".join(re.escape(k) for k in cleaned)
    return re.compile(pattern, flags=re.IGNORECASE)


def _record_matches(title: str, abstract: str, inc_re, exc_re, search_in_title: bool, search_in_abstract: bool) -> bool:
    if not (search_in_title or search_in_abstract):
        return True

    # Inclusion
    if inc_re is not None:
        inc_match = False
        if search_in_title and title and inc_re.search(title):
            inc_match = True
        if search_in_abstract and abstract and inc_re.search(abstract):
            inc_match = True
        if not inc_match:
            return False

    # Exclusion
    if exc_re is not None:
        if search_in_title and title and exc_re.search(title):
            return False
        if search_in_abstract and abstract and exc_re.search(abstract):
            return False

    return True


def _date_in_window(pub_date: date, from_date: Optional[date], until_date: Optional[date]) -> bool:
    if from_date is not None and pub_date < from_date:
        return False
    if until_date is not None and pub_date > until_date:
        return False
    return True


async def fetch_one_issn(
    session,
    issn,
    mailto,
    max_rows_per_issn,
    sem,
    output_csv: Optional[str] = None,
    csv_columns=None,
    csv_lock: Optional[asyncio.Lock] = None,
    from_date: Optional[date] = None,
    until_date: Optional[date] = None,
    use_date_filter: bool = False,
    include_re=None,
    exclude_re=None,
    search_in_title: bool = True,
    search_in_abstract: bool = True,
    deduplicate_doi: bool = True,
    seen_dois=None,
    max_retries: int = 5,
    backoff_base: float = 2.0,
    bibliographic_query: Optional[str] = None,
):
    """
    Récupère les articles pour un ISSN donné, avec gestion des 429 et 5xx.
    """
    async def request_page(cursor, rows):
        for attempt in range(max_retries):
            async with sem:
                try:
                    params = build_params(
                        issn=issn,
                        mailto=mailto,
                        rows=rows,
                        cursor=cursor,
                        from_date=from_date if use_date_filter else None,
                        until_date=until_date if use_date_filter else None,
                        bibliographic_query=bibliographic_query,
                    )
                    async with session.get(BASE_URL, params=params, timeout=20) as resp:
                        status = resp.status

                        # ----- Gestion rate limiting -----
                        if status == 429:
                            retry_after = resp.headers.get("Retry-After")
                            if retry_after:
                                try:
                                    delay = float(retry_after)
                                except ValueError:
                                    delay = backoff_base ** attempt
                            else:
                                delay = backoff_base ** attempt
                            print(f"[{issn}] HTTP 429 (rate limit). Tentative {attempt+1}/{max_retries}. Attente {delay:.1f}s puis retry...")
                            await asyncio.sleep(delay)
                            continue  # on retente

                        # ----- Gestion erreurs serveur -----
                        if 500 <= status < 600:
                            delay = backoff_base ** attempt
                            print(f"[{issn}] HTTP {status}. Tentative {attempt+1}/{max_retries}. Attente {delay:.1f}s puis retry...")
                            await asyncio.sleep(delay)
                            continue

                        # ----- Autres erreurs HTTP -----
                        if status != 200:
                            print(f"[{issn}] HTTP {status}. Abandon pour cet ISSN.")
                            return None

                        # Si on est là : status == 200
                        data = await resp.json()

                except Exception as e:
                    delay = backoff_base ** attempt
                    print(f"[{issn}] ERREUR réseau ({e}). Tentative {attempt+1}/{max_retries}. Attente {delay:.1f}s puis retry...")
                    await asyncio.sleep(delay)
                    continue

            # Si on sort du with sans exception ni continue, on a "data"
            return data

        # boucle épuisée
        print(f"[{issn}] Échec après {max_retries} tentatives.")
        return None

    if max_rows_per_issn is None:
        remaining = None
    else:
        max_rows_per_issn = int(max_rows_per_issn)
        if max_rows_per_issn <= 0:
            print(f"[{issn}] max_rows_per_issn<=0 -> aucun article demande.")
            return 0 if output_csv else []
        remaining = max_rows_per_issn

    cursor = "*"
    results = []
    written = 0

    while True:
        if remaining is not None and remaining <= 0:
            break

        rows = MAX_CROSSREF_ROWS if remaining is None else min(MAX_CROSSREF_ROWS, remaining)
        data = await request_page(cursor, rows)
        if data is None:
            return []

        items = data.get("message", {}).get("items", [])
        if not items:
            break

        fetched_count = len(items)
        page_records = []

        for it in items:
            pub_date = extract_published_date(it)
            if use_date_filter:
                if pub_date is None:
                    continue
                if not _date_in_window(pub_date, from_date, until_date):
                    continue

            title_list = it.get("title", [])
            raw_title = title_list[0] if title_list else ""
            title = clean_xml_text(raw_title)

            if not title:
                continue

            container_title_list = it.get("container-title", [])
            journal = container_title_list[0] if container_title_list else ""

            authors = extract_authors(it)
            doi = it.get("DOI", "")
            url = it.get("URL", "")
            abstract = extract_abstract(it)

            if include_re is not None or exclude_re is not None:
                if not _record_matches(
                    title=title,
                    abstract=abstract,
                    inc_re=include_re,
                    exc_re=exclude_re,
                    search_in_title=search_in_title,
                    search_in_abstract=search_in_abstract,
                ):
                    continue

            if deduplicate_doi and seen_dois is not None:
                doi_key = doi.strip().lower() if isinstance(doi, str) else ""
                if doi_key:
                    if doi_key in seen_dois:
                        continue
                    seen_dois.add(doi_key)

            page_records.append(
                {
                    "title": title,
                    "authors": authors,
                    "journal": journal,
                    "published_date": pub_date.isoformat() if pub_date else "",
                    "doi": doi,
                    "url": url,
                    "issn": issn,
                    "abstract": abstract,
                    "source": "crossref",
                }
            )

        if output_csv:
            if page_records:
                if csv_lock is None:
                    written += _append_records_to_csv(output_csv, page_records, csv_columns)
                else:
                    async with csv_lock:
                        written += _append_records_to_csv(output_csv, page_records, csv_columns)
        else:
            results.extend(page_records)

        if remaining is not None:
            remaining -= fetched_count

        if len(items) < rows:
            break

        next_cursor = data.get("message", {}).get("next-cursor")
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor

    final_count = written if output_csv else len(results)
    print(f"[{issn}] {final_count} articles")
    return written if output_csv else results


def _append_records_to_csv(path: str, records, columns) -> int:
    if not records:
        return 0
    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        if not file_exists:
            writer.writeheader()
        writer.writerows(records)
    return len(records)


def _initialize_csv(path: str, columns) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()


async def fetch_recent_articles_for_issn_df(
    df_issn: pd.DataFrame,
    days_back: Optional[int] = 31,
    mailto: str = "sebastien.menigot@eseo.fr",
    max_rows_per_issn: Optional[int] = 200,
    max_concurrent_requests: int = 3,
    output_csv=None,
    return_dataframe: bool = True,
    include_keywords=None,
    exclude_keywords=None,
    search_in_title: bool = True,
    search_in_abstract: bool = True,
    use_regex: bool = True,
    deduplicate_doi: bool = True,
    from_date: Optional[str] = None,
    until_date: Optional[str] = None,
    use_date_filter: Optional[bool] = None,
    append_output_csv: bool = False,
    crossref_bibliographic_queries=None,
) -> pd.DataFrame:
    """
    Version ASYNCHRONE.
    Entrée : df_issn avec colonne 'issn' ou 'ISSN'
    Sortie : DataFrame des articles filtrés.

    Compatibilite:
      - mode "recent": renseigner days_back (ou from_date/until_date)
      - mode "historique": use_date_filter=False et days_back=None

    Memoire:
      - si output_csv est renseigne, traitement sequentiel par ISSN
        avec ecriture progressive sur disque (RAM minimale).

    append_output_csv:
      - True: conserve le CSV existant et ajoute les nouvelles lignes.
      - False: reinitialise le CSV de sortie.

    crossref_bibliographic_queries:
      - liste optionnelle de requetes `query.bibliographic`.
      - chaque requete est executee pour chaque ISSN.
    """
    if "issn" in df_issn.columns:
        col = "issn"
    elif "ISSN" in df_issn.columns:
        col = "ISSN"
    else:
        raise ValueError("df_issn doit contenir une colonne 'issn' ou 'ISSN'.")

    issn_list = (
        df_issn[col]
        .astype(str)
        .str.strip()
        .replace({"": None})
        .dropna()
        .unique()
    )

    print(f"Nombre d'ISSN uniques : {len(issn_list)}")

    effective_date_filter, parsed_from_date, parsed_until_date = _resolve_date_window(
        days_back=days_back,
        from_date=from_date,
        until_date=until_date,
        use_date_filter=use_date_filter,
    )

    if effective_date_filter:
        bounds = []
        if parsed_from_date is not None:
            bounds.append(f"from={parsed_from_date.isoformat()}")
        if parsed_until_date is not None:
            bounds.append(f"until={parsed_until_date.isoformat()}")
        print(f"Fenetre temporelle active ({', '.join(bounds)})")
    else:
        print("Fenetre temporelle desactivee (collecte historique large).")

    sem = asyncio.Semaphore(max_concurrent_requests)

    # User-Agent recommandé par CrossRef
    headers = {
        "User-Agent": f"biblio-bot/1.0 (mailto:{mailto})"
    }

    include_re = _compile_patterns(include_keywords, use_regex=use_regex)
    exclude_re = _compile_patterns(exclude_keywords, use_regex=use_regex)
    seen_dois = set() if deduplicate_doi else None
    query_list = []
    for q in crossref_bibliographic_queries or []:
        if not isinstance(q, str):
            continue
        cleaned = q.strip()
        if cleaned:
            query_list.append(cleaned)
    if not query_list:
        query_list = [None]
    else:
        print(f"Mode query.bibliographic actif: {len(query_list)} requete(s) par ISSN.")

    columns = [
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

    records = []
    total_written = 0

    async with aiohttp.ClientSession(headers=headers) as session:
        if output_csv:
            if not append_output_csv or not os.path.exists(output_csv):
                _initialize_csv(output_csv, columns)
            print("Mode streaming disque: ecriture progressive par revue (ISSN).")

            # Traitement sequentiel volontaire pour minimiser la RAM.
            for idx, issn in enumerate(issn_list, start=1):
                print(f"[CrossRef] Revue {idx}/{len(issn_list)} - ISSN {issn}")
                for q_idx, query_text in enumerate(query_list, start=1):
                    if query_text:
                        print(f"[CrossRef]   Query {q_idx}/{len(query_list)}: {query_text}")
                    written_issn = await fetch_one_issn(
                        session=session,
                        issn=issn,
                        mailto=mailto,
                        max_rows_per_issn=max_rows_per_issn,
                        sem=sem,
                        output_csv=output_csv,
                        csv_columns=columns,
                        csv_lock=None,
                        from_date=parsed_from_date,
                        until_date=parsed_until_date,
                        use_date_filter=effective_date_filter,
                        include_re=include_re,
                        exclude_re=exclude_re,
                        search_in_title=search_in_title,
                        search_in_abstract=search_in_abstract,
                        deduplicate_doi=deduplicate_doi,
                        seen_dois=seen_dois,
                        bibliographic_query=query_text,
                    )
                    total_written += int(written_issn or 0)
                    print(f"[CrossRef] Cumul ecrit: {total_written} articles")
        else:
            tasks = [
                fetch_one_issn(
                    session=session,
                    issn=issn,
                    mailto=mailto,
                    max_rows_per_issn=max_rows_per_issn,
                    sem=sem,
                    from_date=parsed_from_date,
                    until_date=parsed_until_date,
                    use_date_filter=effective_date_filter,
                    include_re=include_re,
                    exclude_re=exclude_re,
                    search_in_title=search_in_title,
                    search_in_abstract=search_in_abstract,
                    deduplicate_doi=deduplicate_doi,
                    seen_dois=seen_dois,
                    bibliographic_query=query_text,
                )
                for issn in issn_list
                for query_text in query_list
            ]

            # Traiter au fil de l'eau pour limiter la RAM
            for fut in asyncio.as_completed(tasks):
                res = await fut
                if not res:
                    continue
                records.extend(res)

    if output_csv and not return_dataframe:
        if not os.path.exists(output_csv):
            _initialize_csv(output_csv, columns)
        if total_written == 0:
            print("Aucun article trouve.")
            return pd.DataFrame(columns=columns)
        print("✅ Fini. Articles enregistrés sur disque.")
        return pd.DataFrame(columns=columns)
    elif output_csv:
        if not os.path.exists(output_csv):
            _initialize_csv(output_csv, columns)
            print("Aucun article trouve.")
            return pd.DataFrame(columns=columns)
        df_out = pd.read_csv(output_csv, encoding="utf-8")
    elif not records:
        print("Aucun article trouvé.")
        return pd.DataFrame(columns=columns)
    else:
        df_out = pd.DataFrame(records)

    if effective_date_filter:
        parsed_dates = pd.to_datetime(df_out["published_date"], errors="coerce").dt.date
        if parsed_from_date is not None:
            df_out = df_out[parsed_dates >= parsed_from_date]
        if parsed_until_date is not None:
            df_out = df_out[parsed_dates <= parsed_until_date]

    print(f"✅ Fini. {len(df_out)} articles retenus après filtrage local.")
    return df_out
