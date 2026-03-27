# -*- coding: utf-8 -*-
import asyncio
import csv
import gc
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from config import load_config
from s1_journals_issn import get_issns_for_categories
from s2_crossref_recent import fetch_recent_articles_for_issn_df
from s3_filter_keywords import filter_by_keywords
from s4_llm_relevance import score_articles_with_llm
from s5_pubmed_abstracts import enrich_with_pubmed_abstracts

DEFAULT_ARXIV_QUERY = "(cat:physics* OR cat:math* OR cat:cs* OR cat:stat* OR cat:eess*)"


def _count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with open(path, "r", encoding="utf-8", newline="") as f:
        count = sum(1 for _ in f)
    return max(count - 1, 0)


def _append_df_to_csv(path: Path, df: pd.DataFrame, columns, sep: str = ",") -> None:
    if df is None or df.empty:
        return
    df = df.copy()
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    df = df[columns]
    write_header = not path.exists()
    df.to_csv(path, mode="a", header=write_header, index=False, encoding="utf-8", sep=sep)


def _stream_dedupe_csv(input_path: Path, output_path: Path, doi_col: str = "doi", sep: str = ",") -> None:
    if not input_path.exists():
        return
    if output_path.exists():
        output_path.unlink()

    sqlite_path = output_path.with_suffix(output_path.suffix + ".dedupe.sqlite")
    if sqlite_path.exists():
        sqlite_path.unlink()

    conn = sqlite3.connect(str(sqlite_path))
    try:
        conn.execute("CREATE TABLE seen_doi (doi TEXT PRIMARY KEY)")
        conn.commit()

        with open(input_path, "r", encoding="utf-8", newline="") as fin:
            reader = csv.DictReader(fin, delimiter=sep)
            if not reader.fieldnames:
                return
            with open(output_path, "w", encoding="utf-8", newline="") as fout:
                writer = csv.DictWriter(fout, fieldnames=reader.fieldnames, delimiter=sep)
                writer.writeheader()

                for row in reader:
                    doi = (row.get(doi_col) or "").strip().lower()
                    if doi:
                        try:
                            conn.execute("INSERT INTO seen_doi(doi) VALUES (?)", (doi,))
                        except sqlite3.IntegrityError:
                            continue
                    writer.writerow(row)
        conn.commit()
    finally:
        conn.close()
        if sqlite_path.exists():
            sqlite_path.unlink()


def _stream_filter_csv(
    input_path: Path,
    output_path: Path,
    include_keywords,
    exclude_keywords,
    search_in_title: bool = True,
    search_in_abstract: bool = True,
    use_regex: bool = True,
    chunksize: int = 50000,
) -> None:
    if output_path.exists():
        output_path.unlink()
    if not input_path.exists():
        return

    first = True
    output_columns = None

    for chunk in pd.read_csv(input_path, encoding="utf-8", chunksize=chunksize):
        if output_columns is None:
            output_columns = list(chunk.columns)

        filtered = filter_by_keywords(
            chunk,
            include_keywords=include_keywords,
            exclude_keywords=exclude_keywords,
            search_in_title=search_in_title,
            search_in_abstract=search_in_abstract,
            use_regex=use_regex,
        )
        if filtered.empty:
            continue

        filtered.to_csv(
            output_path,
            mode="a",
            header=first,
            index=False,
            encoding="utf-8",
            sep=";",
        )
        first = False

    if first:
        if output_columns is None:
            try:
                output_columns = list(pd.read_csv(input_path, nrows=0, encoding="utf-8").columns)
            except Exception:
                output_columns = []
        pd.DataFrame(columns=output_columns or []).to_csv(
            output_path,
            index=False,
            encoding="utf-8",
            sep=";",
        )


def _resolve_date_filter_config(cfg: dict) -> dict:
    """
    Priorite:
      1) Nouvelle config: collection.date_filter
      2) Fallback legacy: journals.max_article_age_days
    """
    journals_cfg = cfg.get("journals", {})
    collection_cfg = cfg.get("collection", {})
    date_cfg = collection_cfg.get("date_filter")

    if isinstance(date_cfg, dict):
        enabled = bool(date_cfg.get("enabled", False))
        days_back = date_cfg.get("days_back")
        from_date = date_cfg.get("from")
        until_date = date_cfg.get("to")
    else:
        enabled = False
        days_back = None
        from_date = None
        until_date = None

    if date_cfg is None:
        legacy_days_back = journals_cfg.get("max_article_age_days")
        if legacy_days_back is not None:
            enabled = True
            days_back = int(legacy_days_back)

    if days_back is not None:
        days_back = int(days_back)

    if not enabled:
        days_back = None
        from_date = None
        until_date = None

    return {
        "enabled": enabled,
        "days_back": days_back,
        "from_date": from_date,
        "until_date": until_date,
    }


def _resolve_keyword_filter_config(cfg: dict) -> dict:
    filters_cfg = cfg.get("filters", {})
    kw_cfg = filters_cfg.get("title_keywords", {})
    return {
        "include": kw_cfg.get("include", []) or [],
        "exclude": kw_cfg.get("exclude", []) or [],
        "search_in_title": bool(kw_cfg.get("search_in_title", True)),
        "search_in_abstract": bool(kw_cfg.get("search_in_abstract", True)),
        "use_regex": bool(kw_cfg.get("use_regex", True)),
    }


def _resolve_source_config(cfg: dict, use_hal: bool, use_arxiv: bool) -> dict:
    journals_cfg = cfg.get("journals", {})
    collection_cfg = cfg.get("collection", {})

    crossref_cfg = collection_cfg.get("crossref", {})
    year_windows_cfg = crossref_cfg.get("year_windows", {})
    if not isinstance(year_windows_cfg, dict):
        year_windows_cfg = {}
    global_query_cfg = crossref_cfg.get("global_query", {})
    if not isinstance(global_query_cfg, dict):
        global_query_cfg = {}

    raw_bibliographic_queries = global_query_cfg.get("bibliographic_queries", [])
    if isinstance(raw_bibliographic_queries, str):
        raw_bibliographic_queries = [raw_bibliographic_queries]
    elif not isinstance(raw_bibliographic_queries, (list, tuple)):
        raw_bibliographic_queries = []

    bibliographic_queries = [
        str(q).strip()
        for q in raw_bibliographic_queries
        if isinstance(q, str) and str(q).strip()
    ]
    hal_cfg = collection_cfg.get("hal", {})
    arxiv_cfg = collection_cfg.get("arxiv", {})

    return {
        "categories": journals_cfg.get("categories", []),
        "crossref": {
            "enabled": bool(crossref_cfg.get("enabled", True)),
            "max_rows_per_issn": crossref_cfg.get(
                "max_rows_per_issn",
                journals_cfg.get("max_rows_per_issn", 1000),
            ),
            "max_concurrent_requests": int(crossref_cfg.get("max_concurrent_requests", 10)),
            "max_issn_to_process": (
                int(crossref_cfg["max_issn_to_process"])
                if crossref_cfg.get("max_issn_to_process") is not None
                else None
            ),
            "year_windows": {
                "enabled": bool(year_windows_cfg.get("enabled", False)),
                "start_year": (
                    int(year_windows_cfg["start_year"])
                    if year_windows_cfg.get("start_year") is not None
                    else None
                ),
                "end_year": (
                    int(year_windows_cfg["end_year"])
                    if year_windows_cfg.get("end_year") is not None
                    else None
                ),
                "step_years": int(year_windows_cfg.get("step_years", 1)),
            },
            "global_query": {
                "enabled": bool(global_query_cfg.get("enabled", False)),
                "bibliographic_queries": bibliographic_queries,
            },
        },
        "hal": {
            "enabled": bool(use_hal) and bool(hal_cfg.get("enabled", True)),
            "query": hal_cfg.get("query", "*:*"),
            "rows_per_page": int(hal_cfg.get("rows_per_page", 200)),
            "max_total_results": int(hal_cfg.get("max_total_results", 5000)),
        },
        "arxiv": {
            "enabled": bool(use_arxiv) and bool(arxiv_cfg.get("enabled", True)),
            "query": arxiv_cfg.get("query", DEFAULT_ARXIV_QUERY),
            "page_size": int(arxiv_cfg.get("page_size", 100)),
            "max_total_results": int(arxiv_cfg.get("max_total_results", 5000)),
        },
    }


def _build_year_windows(start_year: int, end_year: int, step_years: int = 1) -> list[tuple[str, str]]:
    if step_years <= 0:
        raise ValueError("collection.crossref.year_windows.step_years doit etre > 0.")
    if start_year > end_year:
        raise ValueError("collection.crossref.year_windows.start_year doit etre <= end_year.")

    windows = []
    y = start_year
    while y <= end_year:
        y_end = min(y + step_years - 1, end_year)
        windows.append((f"{y:04d}-01-01", f"{y_end:04d}-12-31"))
        y = y_end + 1
    return windows


async def run_pipeline_for_config(
    config_path: str,
    recompute_issn: bool = True,
    recompute_crossref: bool = True,
    recompute_keywords: bool = True,
    recompute_llm: bool = True,
    recompute_pubmed: bool = True,
    use_hal: bool = False,
    use_arxiv: bool = False,
):
    """
    Pipeline complet:
      1) ISSN Scimago
      2) Collecte large (CrossRef + HAL + arXiv)
      3) Filtrage par mots-cles
      4) Scoring LLM (embeddings)
      5) Enrichissement abstracts via PubMed
    """
    cfg = load_config(config_path)
    cfg_name = Path(config_path).stem

    print("\n==============================")
    print(f"   PIPELINE pour config: {cfg_name}")
    print("==============================\n")

    prefix = cfg_name
    columns_all = ["title", "authors", "journal", "published_date", "doi", "url", "issn", "abstract", "source"]

    kw_cfg = _resolve_keyword_filter_config(cfg)
    source_cfg = _resolve_source_config(cfg, use_hal=use_hal, use_arxiv=use_arxiv)
    date_cfg = _resolve_date_filter_config(cfg)

    path_issn = Path(f"{prefix}_issns_and_titles_snapshot.csv")
    path_full = Path(f"{prefix}_articles_collected_full.csv")
    path_kw = Path(f"{prefix}_articles_after_keywords.csv")
    path_scored = Path(f"{prefix}_articles_scored.csv")
    path_relevant = Path(f"{prefix}_articles_relevant_only.csv")

    date_tag = datetime.utcnow().strftime("%Y%m%d")
    path_pubmed = Path(f"{prefix}_articles_with_pubmed_abstracts_{date_tag}.csv")

    # =====================
    # 1) Journaux / ISSN
    # =====================
    df_issn = pd.DataFrame(columns=["issn", "journal"])
    if source_cfg["crossref"]["enabled"]:
        if recompute_issn or not path_issn.exists():
            print(">> Etape 1 : calcul ISSN (Scimago)")
            categories = source_cfg["categories"]
            if not categories:
                raise ValueError("Aucune categorie configuree dans `journals.categories`.")
            df_issn = get_issns_for_categories(categories_to_keep=categories)
            print(f"ISSN recuperes : {len(df_issn)}")
            df_issn.to_csv(path_issn, index=False, encoding="utf-8")
        else:
            print(">> Etape 1 : chargement ISSN depuis CSV")
            df_issn = pd.read_csv(path_issn, encoding="utf-8")
            print(f"ISSN charges : {len(df_issn)}")

        max_issn_to_process = source_cfg["crossref"]["max_issn_to_process"]
        if max_issn_to_process is not None and len(df_issn) > max_issn_to_process:
            df_issn = df_issn.head(max_issn_to_process).copy()
            print(f"ISSN limites pour ce run: {len(df_issn)}")
    else:
        print(">> Etape 1 : CrossRef desactive, ISSN ignores.")

    # =====================
    # 2) Collecte large
    # =====================
    if recompute_crossref or not path_full.exists():
        active_sources = []
        if source_cfg["crossref"]["enabled"]:
            active_sources.append("CrossRef")
        if source_cfg["hal"]["enabled"]:
            active_sources.append("HAL")
        if source_cfg["arxiv"]["enabled"]:
            active_sources.append("arXiv")
        print(f">> Etape 2 : collecte {' + '.join(active_sources) if active_sources else 'aucune source'}")
        if date_cfg["enabled"]:
            bounds = []
            if date_cfg["days_back"] is not None:
                bounds.append(f"days_back={date_cfg['days_back']}")
            if date_cfg["from_date"]:
                bounds.append(f"from={date_cfg['from_date']}")
            if date_cfg["until_date"]:
                bounds.append(f"to={date_cfg['until_date']}")
            print(f"[Date filter] actif ({', '.join(bounds)})")
        else:
            print("[Date filter] desactive (mode historique large)")

        # ---- 2a) CrossRef (stream vers disque) ----
        crossref_tmp = Path(f"{prefix}_articles_crossref_tmp.csv")
        if crossref_tmp.exists():
            crossref_tmp.unlink()

        if source_cfg["crossref"]["enabled"]:
            crossref_global_query_cfg = source_cfg["crossref"]["global_query"]
            if crossref_global_query_cfg["enabled"] and crossref_global_query_cfg["bibliographic_queries"]:
                crossref_bibliographic_queries = crossref_global_query_cfg["bibliographic_queries"]
                print(
                    "[CrossRef] Filtrage global actif via query.bibliographic "
                    f"({len(crossref_bibliographic_queries)} requete(s))."
                )
            else:
                crossref_bibliographic_queries = None
                print("[CrossRef] Filtrage global query.bibliographic desactive.")

            year_windows_cfg = source_cfg["crossref"]["year_windows"]
            if year_windows_cfg["enabled"]:
                start_year = year_windows_cfg["start_year"]
                end_year = year_windows_cfg["end_year"]
                if start_year is None or end_year is None:
                    raise ValueError(
                        "year_windows est active mais start_year/end_year sont absents dans collection.crossref."
                    )
                windows = _build_year_windows(
                    start_year=start_year,
                    end_year=end_year,
                    step_years=year_windows_cfg["step_years"],
                )
                print(f"[CrossRef] Decoupage annuel actif : {len(windows)} fenetre(s).")
                for idx, (window_from, window_to) in enumerate(windows, start=1):
                    print(f"[CrossRef] Fenetre {idx}/{len(windows)} : {window_from} -> {window_to}")
                    await fetch_recent_articles_for_issn_df(
                        df_issn,
                        days_back=None,
                        from_date=window_from,
                        until_date=window_to,
                        use_date_filter=True,
                        mailto=cfg["pubmed"]["email"],
                        max_rows_per_issn=source_cfg["crossref"]["max_rows_per_issn"],
                        max_concurrent_requests=source_cfg["crossref"]["max_concurrent_requests"],
                        output_csv=str(crossref_tmp),
                        return_dataframe=False,
                        include_keywords=None,
                        exclude_keywords=None,
                        search_in_title=True,
                        search_in_abstract=True,
                        use_regex=True,
                        deduplicate_doi=True,
                        append_output_csv=(idx > 1),
                        crossref_bibliographic_queries=crossref_bibliographic_queries,
                    )
            else:
                await fetch_recent_articles_for_issn_df(
                    df_issn,
                    days_back=date_cfg["days_back"],
                    from_date=date_cfg["from_date"],
                    until_date=date_cfg["until_date"],
                    use_date_filter=date_cfg["enabled"],
                    mailto=cfg["pubmed"]["email"],
                    max_rows_per_issn=source_cfg["crossref"]["max_rows_per_issn"],
                    max_concurrent_requests=source_cfg["crossref"]["max_concurrent_requests"],
                    output_csv=str(crossref_tmp),
                    return_dataframe=False,
                    include_keywords=None,
                    exclude_keywords=None,
                    search_in_title=True,
                    search_in_abstract=True,
                    use_regex=True,
                    deduplicate_doi=True,
                    append_output_csv=False,
                    crossref_bibliographic_queries=crossref_bibliographic_queries,
                )
            print(f"[CrossRef] Articles trouves (avant dedoublonnage) : {_count_csv_rows(crossref_tmp)}")
        else:
            print("[CrossRef] Desactive")

        # ---- 2b) HAL ----
        if source_cfg["hal"]["enabled"]:
            from s2b_hal_theses_recent import fetch_hal_theses

            print("[HAL] Recuperation des theses...")
            df_hal = fetch_hal_theses(
                query=source_cfg["hal"]["query"],
                days_back=date_cfg["days_back"],
                from_date=date_cfg["from_date"],
                until_date=date_cfg["until_date"],
                use_date_filter=date_cfg["enabled"],
                rows_per_page=source_cfg["hal"]["rows_per_page"],
                max_total_results=source_cfg["hal"]["max_total_results"],
            )
            print(f"[HAL] Theses trouvees : {len(df_hal)}")
        else:
            df_hal = pd.DataFrame(columns=columns_all)
            print("[HAL] Desactive")

        # ---- 2c) arXiv ----
        if source_cfg["arxiv"]["enabled"]:
            from s2c_arxiv_recent import fetch_arxiv_articles

            print("[arXiv] Recuperation des preprints...")
            df_arxiv = fetch_arxiv_articles(
                query=source_cfg["arxiv"]["query"],
                days_back=date_cfg["days_back"],
                from_date=date_cfg["from_date"],
                until_date=date_cfg["until_date"],
                use_date_filter=date_cfg["enabled"],
                page_size=source_cfg["arxiv"]["page_size"],
                max_total_results=source_cfg["arxiv"]["max_total_results"],
            )
            print(f"[arXiv] Articles trouves : {len(df_arxiv)}")
        else:
            df_arxiv = pd.DataFrame(columns=columns_all)
            print("[arXiv] Desactive")

        # ---- 2d) Fusion ----
        if path_full.exists():
            path_full.unlink()
        if crossref_tmp.exists():
            os.replace(crossref_tmp, path_full)
        else:
            pd.DataFrame(columns=columns_all).to_csv(path_full, index=False, encoding="utf-8")

        if "source" not in df_hal.columns:
            df_hal["source"] = "hal"
        if "source" not in df_arxiv.columns:
            df_arxiv["source"] = "arxiv"

        _append_df_to_csv(path_full, df_hal, columns_all, sep=",")
        _append_df_to_csv(path_full, df_arxiv, columns_all, sep=",")

        print(f"[ALL] Total avant dedoublonnage DOI : {_count_csv_rows(path_full)}")
        path_full_dedup = Path(f"{prefix}_articles_collected_full_dedup.csv")
        _stream_dedupe_csv(path_full, path_full_dedup, doi_col="doi", sep=",")
        if path_full_dedup.exists():
            if path_full.exists():
                path_full.unlink()
            os.replace(path_full_dedup, path_full)
        print(f"[ALL] Total apres dedoublonnage DOI : {_count_csv_rows(path_full)}")
    else:
        print(">> Etape 2 : chargement collecte complete depuis CSV")
        print(f"Articles charges (full) : {_count_csv_rows(path_full)}")

    # =====================
    # 3) Filtre mots-cles
    # =====================
    if recompute_keywords or not path_kw.exists():
        print(">> Etape 3 : filtrage par mots-cles (titre/abstract)")
        _stream_filter_csv(
            path_full,
            path_kw,
            include_keywords=kw_cfg["include"],
            exclude_keywords=kw_cfg["exclude"],
            search_in_title=kw_cfg["search_in_title"],
            search_in_abstract=kw_cfg["search_in_abstract"],
            use_regex=kw_cfg["use_regex"],
        )

    print(">> Etape 3 : chargement articles filtres depuis CSV")
    if path_kw.exists():
        df_filtered = pd.read_csv(path_kw, encoding="utf-8", sep=";")
    else:
        df_filtered = pd.DataFrame(columns=columns_all)
    print(f"Articles charges (apres keywords) : {len(df_filtered)}")

    gc.collect()

    # =====================
    # 4) LLM / embeddings
    # =====================
    if recompute_llm or (not path_scored.exists() or not path_relevant.exists()):
        print(">> Etape 4 : scoring LLM (embeddings)")
        llm_cfg = cfg["llm"]
        df_scored, df_relevant = score_articles_with_llm(
            df_filtered,
            domain_description=llm_cfg["domain_description"],
            model_name=llm_cfg["model_name"],
            relevance_threshold=llm_cfg["relevance_threshold"],
        )
        print(f"Articles scores (total) : {len(df_scored)}")
        print(f"Articles pertinents (>= {llm_cfg['relevance_threshold']}) : {len(df_relevant)}")
        df_scored.to_csv(path_scored, sep=";", index=False, encoding="utf-8")
        df_relevant.to_csv(path_relevant, sep=";", index=False, encoding="utf-8")
    else:
        print(">> Etape 4 : chargement scoring LLM depuis CSV")
        df_scored = pd.read_csv(path_scored, encoding="utf-8", sep=";")
        df_relevant = pd.read_csv(path_relevant, encoding="utf-8", sep=";")
        print(f"Articles scores charges : {len(df_scored)}")
        print(f"Articles pertinents charges : {len(df_relevant)}")

    if "df_filtered" in locals():
        del df_filtered
    if "df_scored" in locals():
        del df_scored
    gc.collect()

    # =====================
    # 5) Enrichissement PubMed
    # =====================
    if recompute_pubmed or not path_pubmed.exists():
        print(">> Etape 5 : enrichissement des abstracts via PubMed")
        pubmed_cfg = cfg["pubmed"]
        df_pubmed, stats = enrich_with_pubmed_abstracts(
            df_relevant,
            email=pubmed_cfg["email"],
            api_key=pubmed_cfg.get("api_key", ""),
            max_articles=None,
            batch_size=100,
            max_workers=6,
        )
        print(f"[PUBMED] Stats : {stats}")

        df_pubmed.to_csv(path_pubmed, sep=";", index=False, encoding="utf-8")

        excel_path = path_pubmed.with_suffix(".xlsx")
        if "doi" in df_pubmed.columns:
            df_pubmed["doi"] = df_pubmed["doi"].astype(str)
        df_pubmed.to_excel(excel_path, index=False)
        print(f"[PUBMED] CSV sauvegarde : {path_pubmed}")
        print(f"[PUBMED] Excel sauvegarde : {excel_path}")
    else:
        print(">> Etape 5 : chargement des abstracts enrichis depuis CSV")
        df_pubmed = pd.read_csv(path_pubmed, encoding="utf-8", sep=";")
        print(f"Articles avec abstracts PubMed charges : {len(df_pubmed)}")

        excel_path = path_pubmed.with_suffix(".xlsx")
        if "doi" in df_pubmed.columns:
            df_pubmed["doi"] = df_pubmed["doi"].astype(str)
        df_pubmed.to_excel(excel_path, index=False)
        print(f"[PUBMED] Excel regenere : {excel_path}")

    if "df_relevant" in locals():
        del df_relevant
    if "df_pubmed" in locals():
        del df_pubmed
    gc.collect()
