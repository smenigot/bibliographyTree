# main.py
# -*- coding: utf-8 -*-
import asyncio
from pathlib import Path
from datetime import datetime
import pandas as pd

from config import load_config
from s1_journals_issn import get_issns_for_categories
from s2_crossref_recent import fetch_recent_articles_for_issn_df
from s2b_hal_theses_recent import fetch_recent_hal_theses
from s2c_arxiv_recent import fetch_recent_arxiv_articles
from s3_filter_keywords import filter_by_keywords
from s4_llm_relevance import score_articles_with_llm
from s5_pubmed_abstracts import enrich_with_pubmed_abstracts


def deduplicate_by_doi(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    lower_cols = {c.lower(): c for c in df.columns}
    if "doi" not in lower_cols:
        return df
    doi_col = lower_cols["doi"]
    return df.drop_duplicates(subset=doi_col, keep="first")


async def run_pipeline_for_config(
    config_path: str,
    recompute_issn: bool = True,
    recompute_crossref: bool = True,
    recompute_keywords: bool = True,
    recompute_llm: bool = True,
    recompute_pubmed: bool = True,
    use_hal: bool = True,
    use_arxiv: bool = True,
):
    """
    Lance tout le pipeline pour un fichier de config donné.
    Tous les fichiers intermédiaires sont préfixés par le nom de la config.
    Le fichier final PubMed est suffixé par la date du jour.
    """

    cfg = load_config(config_path)
    cfg_name = Path(config_path).stem   # ex: 'config_ultrasound'

    print(f"\n==============================")
    print(f"   PIPELINE pour config: {cfg_name}")
    print(f"==============================\n")

    # préfixe pour les fichiers intermédiaires
    prefix = cfg_name

    # chemins des snapshots (spécifiques à cette config)
    path_issn     = Path(f"{prefix}_issns_and_titles_snapshot.csv")
    path_full     = Path(f"{prefix}_articles_recent_full.csv")
    path_kw       = Path(f"{prefix}_articles_after_keywords.csv")
    path_scored   = Path(f"{prefix}_articles_scored.csv")
    path_relevant = Path(f"{prefix}_articles_relevant_only.csv")

    # date pour les fichiers finaux
    date_tag = datetime.utcnow().strftime("%Y%m%d")
    path_pubmed = Path(f"{prefix}_articles_with_pubmed_abstracts_{date_tag}.csv")

    # =====================
    # 1) Journaux / ISSN
    # =====================
    if recompute_issn or not path_issn.exists():
        print("▶ Étape 1 : calcul ISSN (Scimago)")
        categories = cfg["journals"]["categories"]
        df_issn = get_issns_for_categories(categories_to_keep=categories)
        print(f"ISSN récupérés : {len(df_issn)}")
        df_issn.to_csv(path_issn, index=False, encoding="utf-8")
    else:
        print("▶ Étape 1 : chargement ISSN depuis CSV")
        df_issn = pd.read_csv(path_issn, encoding="utf-8")
        print(f"ISSN chargés : {len(df_issn)}")

    # =====================
    # 2) Articles (CrossRef + HAL + arXiv)
    # =====================
    if recompute_crossref or not path_full.exists():
        print("▶ Étape 2 : requêtes CrossRef + HAL + arXiv")
        days_back = cfg["journals"]["max_article_age_days"]

        # ---- 2a) CrossRef (comme avant) ----
        df_articles = await fetch_recent_articles_for_issn_df(
            df_issn,
            days_back=days_back,
            mailto=cfg["pubmed"]["email"],
            max_rows_per_issn=1000,
            max_concurrent_requests=10,
        )
        print(f"[CrossRef] Articles trouvés (avant dédoublonnage) : {len(df_articles)}")

        # Normaliser un minimum : s'assurer que les colonnes existent
        for col in ["title", "authors", "journal", "published_date", "doi", "url", "issn", "abstract"]:
            if col not in df_articles.columns:
                df_articles[col] = ""

        df_articles["source"] = "crossref"

        # ---- 2b) HAL (thèses) ----
        if use_hal:
            print(f"[HAL] Récupération des thèses sur les {days_back} derniers jours...")
            df_hal = fetch_recent_hal_theses(days_back=days_back)
            print(f"[HAL] Thèses trouvées : {len(df_hal)}")
        else:
            df_hal = pd.DataFrame(columns=df_articles.columns)

        # ---- 2c) arXiv ----
        if use_arxiv:
            print(f"[arXiv] Récupération des preprints sur les {days_back} derniers jours...")
            df_arxiv = fetch_recent_arxiv_articles(
                days_back=days_back,
                page_size=100,
                max_total_results=5000,
            )
            print(f"[arXiv] Articles trouvés : {len(df_arxiv)}")
        else:
            df_arxiv = pd.DataFrame(columns=df_articles.columns)

        # ---- 2d) Fusion des trois sources ----
        df_all = pd.concat(
            [df_articles, df_hal, df_arxiv],
            ignore_index=True,
            sort=False,
        )
        print(f"[ALL] Total avant dédoublonnage DOI : {len(df_all)}")

        df_all = deduplicate_by_doi(df_all)
        print(f"[ALL] Total après dédoublonnage DOI : {len(df_all)}")

        # On garde ce nom pour ne pas casser la suite
        df_articles = df_all

        df_articles.to_csv(path_full, index=False, encoding="utf-8")
    else:
        print("▶ Étape 2 : chargement articles complets depuis CSV")
        df_articles = pd.read_csv(path_full, encoding="utf-8")
        print(f"Articles chargés (full) : {len(df_articles)}")


    # =====================
    # 3) Filtre mots-clés
    # =====================
    if recompute_keywords or not path_kw.exists():
        print("▶ Étape 3 : filtrage par mots-clés (titre/abstract)")
    
        # Récupération propre dans le YAML
        kw_cfg = cfg["filters"].get("title_keywords", {})
        include = kw_cfg.get("include", []) or []
        exclude = kw_cfg.get("exclude", []) or []
    
        df_filtered = filter_by_keywords(
            df_articles,
            include_keywords=include,
            exclude_keywords=exclude,   # vide => pas d’exclusion
            search_in_title=True,
            search_in_abstract=True,
            use_regex=True,             # important : on utilise nos regex
        )
    
        print(f"Articles après filtre keywords : {len(df_filtered)}")
        df_filtered.to_csv(path_kw, index=False, encoding="utf-8", sep=";")
    
    else:
        print("▶ Étape 3 : chargement articles filtrés depuis CSV")
        df_filtered = pd.read_csv(path_kw, encoding="utf-8", sep=";")
        print(f"Articles chargés (après keywords) : {len(df_filtered)}")

    # =====================
    # 4) LLM / embeddings
    # =====================
    if recompute_llm or (not path_scored.exists() or not path_relevant.exists()):
        print("▶ Étape 4 : scoring LLM (embeddings)")
        llm_cfg = cfg["llm"]
        df_scored, df_relevant = score_articles_with_llm(
            df_filtered,
            domain_description=llm_cfg["domain_description"],
            model_name=llm_cfg["model_name"],
            relevance_threshold=llm_cfg["relevance_threshold"],
        )
        print(f"Articles scorés (total) : {len(df_scored)}")
        print(f"Articles pertinents (>= {llm_cfg['relevance_threshold']}) : {len(df_relevant)}")
        df_scored.to_csv(path_scored, sep=";", index=False, encoding="utf-8")
        df_relevant.to_csv(path_relevant, sep=";", index=False, encoding="utf-8")
    else:
        print("▶ Étape 4 : chargement scoring LLM depuis CSV")
        df_scored = pd.read_csv(path_scored, encoding="utf-8", sep=";")
        df_relevant = pd.read_csv(path_relevant, encoding="utf-8", sep=";")
        print(f"Articles scorés chargés : {len(df_scored)}")
        print(f"Articles pertinents chargés : {len(df_relevant)}")

    # =====================
    # 5) Compléter / améliorer les abstracts via PubMed
    # =====================
    if recompute_pubmed or not path_pubmed.exists():
        print("▶ Étape 5 : enrichissement des abstracts via PubMed")
        pubmed_cfg = cfg["pubmed"]
        df_pubmed, stats = enrich_with_pubmed_abstracts(
            df_relevant,
            email=pubmed_cfg["email"],
            api_key=pubmed_cfg.get("api_key", ""),
            max_articles=None,      # ou un entier pour tester (ex: 50)
            batch_size=100,
            max_workers=6,
        )

        print(f"[PUBMED] Stats : {stats}")

        # Sauvegarde CSV (spécifique config + date)
        df_pubmed.to_csv(
            path_pubmed,
            sep=";",
            index=False,
            encoding="utf-8",
        )

        # Sauvegarde Excel (édition manuelle / ChatGPT)
        excel_path = path_pubmed.with_suffix(".xlsx")
        if "doi" in df_pubmed.columns:
            df_pubmed["doi"] = df_pubmed["doi"].astype(str)
        df_pubmed.to_excel(excel_path, index=False)
        print(f"[PUBMED] CSV sauvegardé : {path_pubmed}")
        print(f"[PUBMED] Excel sauvegardé : {excel_path}")
    else:
        print("▶ Étape 5 : chargement des abstracts enrichis depuis CSV")
        df_pubmed = pd.read_csv(path_pubmed, encoding="utf-8", sep=";")
        print(f"Articles avec abstracts PubMed chargés : {len(df_pubmed)}")

        excel_path = path_pubmed.with_suffix(".xlsx")
        if "doi" in df_pubmed.columns:
            df_pubmed["doi"] = df_pubmed["doi"].astype(str)
        df_pubmed.to_excel(excel_path, index=False)
        print(f"[PUBMED] Excel régénéré : {excel_path}")
