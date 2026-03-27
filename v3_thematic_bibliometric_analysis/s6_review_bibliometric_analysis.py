#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

"""
Independent bibliometric review analysis script.

This script builds a full analysis package from a CSV/XLSX bibliography file:
- data quality diagnostics
- publication timeline analysis
- journal and author statistics
- thematic mining (terms + trends)
- thematic clustering (TF-IDF + MiniBatchKMeans)
- co-authorship network
- paper similarity network (text-based links)
- automatic figure/table export + markdown report

Example:
    python s6_review_bibliometric_analysis.py \
        --input bibliography.xlsx \
        --output-dir review_analysis_output/my_theme
"""

import argparse
import difflib
import html
import importlib.util
import itertools
import json
import math
import re
import ssl
import time
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


REQUIRED_MODULES: Dict[str, str] = {
    "numpy": "numpy",
    "pandas": "pandas",
    "matplotlib": "matplotlib",
    "networkx": "networkx",
    "sklearn": "scikit-learn",
}

EXTRA_STOPWORDS = {
    "study",
    "results",
    "result",
    "methods",
    "method",
    "using",
    "used",
    "based",
    "analysis",
    "review",
    "paper",
    "data",
    "system",
    "model",
    "models",
    "clinical",
    "patients",
    "patient",
    "group",
    "groups",
    "significant",
    "different",
    "new",
    "high",
    "low",
    "abstract",
    "et",
    "al",
    "papers",
    "systems",
    "background",
    "conclusion",
    "conclusions",
    "objective",
    "objectives",
    "aim",
    "aims",
    "purpose",
    "purposes",
}

AFFILIATION_KEYWORDS = (
    "laboratory",
    "laboratoire",
    "lab",
    "centre",
    "center",
    "institute",
    "institut",
    "university",
    "universite",
    "college",
    "school",
    "faculty",
    "hospital",
    "clinic",
    "department",
    "division",
    "unit",
    "team",
    "programme",
    "program",
)

INSTITUTION_KEYWORDS = (
    "university",
    "universite",
    "college",
    "school",
    "faculty",
    "hospital",
    "clinic",
    "institute",
    "institut",
    "centre",
    "center",
)

TEAM_LAB_KEYWORDS = (
    "department",
    "division",
    "unit",
    "team",
    "laboratory",
    "laboratoire",
    "lab",
    "centre",
    "center",
    "service",
)

AFFILIATION_LOCATION_STOPWORDS = {
    "france",
    "germany",
    "italy",
    "spain",
    "belgium",
    "switzerland",
    "canada",
    "usa",
    "u.s.a.",
    "united states",
    "united kingdom",
    "england",
    "scotland",
    "wales",
    "ireland",
    "uk",
    "u.k.",
    "netherlands",
    "australia",
    "japan",
    "china",
    "egypt",
    "india",
    "brazil",
    "taiwan",
    "turkey",
    "sweden",
    "norway",
    "denmark",
    "finland",
    "the netherlands",
    "south korea",
    "north korea",
    "united arab emirates",
    "new zealand",
}

NON_CITY_LOCATION_STOPWORDS = AFFILIATION_LOCATION_STOPWORDS.union(
    {
        "washington",
        "leicestershire",
        "california",
        "texas",
        "ontario",
        "alberta",
        "quebec",
        "victoria",
        "new south wales",
        "massachusetts",
        "missouri",
        "new york",
        "florida",
        "england",
        "scotland",
        "wales",
        "europe",
    }
)
NON_CITY_LOCATION_STOPWORDS_KEYS = {re.sub(r"[^a-z]+", " ", value.lower()).strip() for value in NON_CITY_LOCATION_STOPWORDS}

NON_CITY_ENTITY_KEYWORDS = {
    "applied",
    "avenue",
    "bat",
    "boulevard",
    "building",
    "campus",
    "center",
    "centre",
    "clinic",
    "college",
    "department",
    "division",
    "drive",
    "faculty",
    "foundation",
    "harvard",
    "hospital",
    "imagerie",
    "infirmary",
    "institute",
    "lab",
    "laboratory",
    "medical",
    "medicine",
    "nhs",
    "park",
    "parc",
    "physiology",
    "research",
    "road",
    "rue",
    "school",
    "sciences",
    "service",
    "signal",
    "team",
    "trust",
    "university",
    "ultrasonore",
    "ultrasons",
    "women",
    "women's",
    "womens",
}

INSTITUTION_CITY_PATTERNS = [
    r"\bUniversity Of\s+([A-Za-z' -]+)$",
    r"\bUniversity Hospitals Of\s+([A-Za-z' -]+)$",
    r"^([A-Za-z' -]+)\s+University$",
]

CLASS_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "methode_rejection_artefact": {
        "display_name": "methode de rejection d'artefact",
        "tie_break_rank": 0,
        "title_patterns": [
            (r"\barte?fact\w*\b", 8),
            (r"\bfalse positives?\b", 8),
            (r"\bfalse alarms?\b", 8),
            (r"\breject(?:ion|ing)?\b", 7),
            (r"\b(?:differentiat(?:e|ion)|distinguish(?:ing)?|discriminat(?:e|ion))\b.{0,40}\barte?fact\w*\b", 10),
            (r"\barte?fact\w*\b.{0,40}\b(?:differentiat(?:e|ion)|distinguish(?:ing)?|discriminat(?:e|ion))\b", 10),
            (r"\bnon[- ]embolic\b", 7),
        ],
        "abstract_patterns": [
            (r"\barte?fact\w*\b", 5),
            (r"\bfalse positives?\b", 5),
            (r"\bfalse alarms?\b", 5),
            (r"\breject(?:ion|ing)?\b", 4),
            (r"\bclutter\b", 3),
            (r"\bnoise\b", 2),
            (r"\bmotion\b", 3),
            (r"\bprobe movement\b", 4),
            (r"\bpatient movement\b", 3),
            (r"\bsurgical manipulation\b", 3),
            (r"\bnon[- ]embolic\b", 4),
            (r"\breduc(?:e|ing) false positives?\b", 6),
            (r"\b(?:differentiat(?:e|ion)|distinguish(?:ing)?|discriminat(?:e|ion))\b.{0,60}\b(?:genuine|true)\b.{0,20}\bemboli", 6),
        ],
        "journal_patterns": [],
        "strong_title_patterns": [
            r"\barte?fact\w*\b",
            r"\bfalse positives?\b",
            r"\bfalse alarms?\b",
        ],
    },
    "instrumentation_doppler": {
        "display_name": "instrumentation doppler",
        "tie_break_rank": 1,
        "title_patterns": [
            (r"\binstrumentation\b", 9),
            (r"\bhardware\b", 8),
            (r"\bprototype\b", 7),
            (r"\btransducer\b", 9),
            (r"\bprobe\b", 7),
            (r"\bmultigate\b", 7),
            (r"\bmulti[- ]gate\b", 7),
            (r"\bdual[- ]gate\b", 7),
            (r"\bmulti[- ]depth\b", 7),
            (r"\bdevice\b", 6),
            (r"\bmonitor(?:ing)? system\b", 6),
            (r"\bdoppler system\b", 6),
            (r"\bultrasound system\b", 6),
            (r"\bpower m[- ]mode\b", 7),
            (r"\bPMD\b", 6),
            (r"\bhigh[- ]frequency\b", 5),
            (r"\b[12](?:\.\d+)?\s*mhz\b", 5),
            (r"\btechnical considerations?\b", 7),
            (r"\bphysical principles?\b", 8),
            (r"\btest performance\b", 7),
        ],
        "abstract_patterns": [
            (r"\bhardware\b", 4),
            (r"\btransducer\b", 5),
            (r"\bprobe\b", 4),
            (r"\bdynamic range\b", 5),
            (r"\bbeam refraction\b", 4),
            (r"\bbeam\b", 2),
            (r"\belectronic\w*\b", 3),
            (r"\barchitecture\b", 3),
            (r"\bmodified\b.{0,30}\binstrument", 5),
            (r"\bmonitor(?:ing)? system\b", 4),
            (r"\bdoppler system\b", 4),
            (r"\bdevice\b", 3),
            (r"\bgate\b", 3),
            (r"\bpower m[- ]mode\b", 4),
            (r"\bhardware and software\b", 5),
        ],
        "journal_patterns": [
            (r"\bengineering\b", 2),
            (r"\btechnology\b", 2),
            (r"\bIEEE\b", 2),
        ],
        "strong_title_patterns": [
            r"\btransducer\b",
            r"\binstrumentation\b",
            r"\bhardware\b",
            r"\bphysical principles?\b",
        ],
    },
    "methode_detection_microemboles": {
        "display_name": "methode de detection de microemboles",
        "tie_break_rank": 2,
        "title_patterns": [
            (r"\bdetect(?:ion|ing|or)?\b", 4),
            (r"\balgorithm\w*\b", 8),
            (r"\bautomatic(?:ally)?\b", 7),
            (r"\bautomated\b", 7),
            (r"\bclassifier\b", 7),
            (r"\bclassification\b", 6),
            (r"\bsignal processing\b", 7),
            (r"\bwavelet\w*\b", 6),
            (r"\bSTFT\b", 6),
            (r"\bCWT\b", 6),
            (r"\bfourier\b", 6),
            (r"\bwigner(?:-ville)?\b", 6),
            (r"\bautoregressive\b", 6),
            (r"\bdecision\b", 4),
            (r"\broc\b", 4),
            (r"\bfeature extraction\b", 6),
            (r"\bidentif(?:y|ication)\b", 5),
            (r"\brecogn(?:ition|izer)\b", 5),
            (r"\bmachine learning\b", 7),
            (r"\bdeep learning\b", 7),
            (r"\bneural network\b", 7),
            (r"\bsvm\b", 6),
            (r"\bneurofuzzy\b", 6),
            (r"\bknowledge[- ]based\b", 5),
            (r"\bdetection system\b", 7),
            (r"\bdetector\b", 6),
            (r"\bcriteria\b", 4),
            (r"\bvalidation of\b.{0,40}\bsystem\b", 6),
        ],
        "abstract_patterns": [
            (r"\balgorithm\w*\b", 4),
            (r"\bautomatic(?:ally)?\b", 4),
            (r"\bautomated\b", 4),
            (r"\bclassifier\b", 4),
            (r"\bclassification\b", 4),
            (r"\bclassifies events\b", 5),
            (r"\bsignal processing\b", 4),
            (r"\bwavelet\w*\b", 4),
            (r"\bSTFT\b", 4),
            (r"\bCWT\b", 4),
            (r"\bfourier\b", 4),
            (r"\bwigner(?:-ville)?\b", 4),
            (r"\bautoregressive\b", 4),
            (r"\bdecision(?:-making)? component\b", 5),
            (r"\broc\b", 3),
            (r"\bfeature extraction\b", 4),
            (r"\bmachine learning\b", 5),
            (r"\bdeep learning\b", 5),
            (r"\bneural network\b", 5),
            (r"\bvalidated against human experts\b", 5),
            (r"\bderive\b.{0,30}\bcriteria\b", 4),
        ],
        "journal_patterns": [
            (r"\bengineering\b", 2),
            (r"\btechnology\b", 2),
            (r"\bpattern recognition\b", 3),
            (r"\bIEEE\b", 2),
            (r"\bsignal\b", 2),
        ],
        "strong_title_patterns": [
            r"\balgorithm\w*\b",
            r"\bautomatic(?:ally)?\b",
            r"\bautomated\b",
            r"\bsignal processing\b",
            r"\bwavelet\w*\b",
            r"\bneural network\b",
        ],
    },
    "etude_clinique": {
        "display_name": "etude clinique",
        "tie_break_rank": 3,
        "title_patterns": [
            (r"\bclinical\b", 5),
            (r"\bprevalence\b", 8),
            (r"\bincidence\b", 8),
            (r"\brisk(?: factors?)?\b", 7),
            (r"\bassociat(?:e|ed|ion)\b", 7),
            (r"\boutcome(?:s)?\b", 7),
            (r"\befficac(?:y|ies)\b", 7),
            (r"\bprospective\b", 8),
            (r"\bretrospective\b", 8),
            (r"\bcohort\b", 8),
            (r"\brandomi[sz]ed\b", 8),
            (r"\btrial\b", 8),
            (r"\bclinical relevance\b", 7),
            (r"\bclinical applications?\b", 6),
            (r"\bprognos(?:is|tic)\b", 7),
            (r"\bpatients?\b", 1),
            (r"\bstroke\b", 1),
            (r"\bendarterectomy\b", 2),
            (r"\bsurgery\b", 1),
        ],
        "abstract_patterns": [
            (r"\bprevalence\b", 6),
            (r"\bincidence\b", 6),
            (r"\brisk(?: factors?)?\b", 5),
            (r"\bassociat(?:e|ed|ion)\b", 5),
            (r"\boutcome(?:s)?\b", 5),
            (r"\befficac(?:y|ies)\b", 5),
            (r"\bprospective\b", 5),
            (r"\bretrospective\b", 5),
            (r"\bcohort\b", 5),
            (r"\brandomi[sz]ed\b", 5),
            (r"\btrial\b", 5),
            (r"\btreatment\b", 4),
            (r"\btherapy\b", 4),
            (r"\bprognos(?:is|tic)\b", 5),
            (r"\bpathophysiolog\w*\b", 4),
            (r"\bpatients?\b", 1),
            (r"\bstroke\b", 1),
            (r"\bcarotid\b", 1),
            (r"\bmonitor(?:ed|ing)\b", 1),
            (r"\bin vivo\b", 1),
        ],
        "journal_patterns": [
            (r"\bstroke\b", 2),
            (r"\bneurolog\w*\b", 2),
            (r"\bvascular\b", 2),
            (r"\bcardiol\w*\b", 2),
            (r"\bsurgery\b", 2),
        ],
        "strong_title_patterns": [
            r"\bprevalence\b",
            r"\bincidence\b",
            r"\brandomi[sz]ed\b",
            r"\btrial\b",
            r"\boutcome(?:s)?\b",
        ],
    },
}

UNCLASSIFIED_LABEL = "autre / non classe"
TARGET_INPUT_FILENAME = "bibliography_input.xlsx"
CLUSTER_LABEL_STOPWORDS = {
    "analysis",
    "data",
    "method",
    "methods",
    "new",
    "patient",
    "patients",
    "review",
    "study",
    "studies",
    "system",
    "systems",
    "time",
    "using",
}

# Direct script configuration for PyCharm execution.
# Set any boolean below to False to disable a specific analysis block.
DEFAULT_OUTPUT_DIR = "review_analysis_output"
DEFAULT_RANDOM_STATE = 42
DEFAULT_ROLLING_WINDOW = 3
DEFAULT_TOP_N = 20
DEFAULT_MIN_AUTHOR_OCCURRENCES = 4
DEFAULT_MAX_AUTHORS_PER_PAPER = 15
DEFAULT_MAX_PAPER_NETWORK_DOCS = 2500
DEFAULT_PAPER_SIM_THRESHOLD = 0.33
DEFAULT_NEIGHBORS = 8
DEFAULT_K_MIN = 4
DEFAULT_K_MAX = 12
DEFAULT_METADATA_TIMEOUT = 1.0
DEFAULT_METADATA_REQUEST_PAUSE = 0.0
DEFAULT_METADATA_MAX_RUNTIME_SECONDS = 300.0
EXPORT_SUPPLEMENTARY_FIGURES = False

RUN_TIMELINE_ANALYSIS = True
RUN_JOURNAL_ANALYSIS = True
RUN_AUTHOR_ANALYSIS = True
RUN_KEYWORD_ANALYSIS = True
RUN_CLASSIFICATION_ANALYSIS = True
RUN_CLUSTERING_ANALYSIS = True
RUN_EXTERNAL_METADATA_ENRICHMENT = True
RUN_TEAM_LAB_ANALYSIS = True
RUN_COAUTHOR_NETWORK = True
RUN_PAPER_SIMILARITY_NETWORK = True
RUN_INTERNAL_REFERENCE_ANALYSIS = True

SUPPRESSED_FIGURE_FILENAMES = [
    "02_cumulative_publications.png",
    "03_yearly_growth_rate.png",
    "07_journal_concentration_curve.png",
    "08_journal_diversity_over_time.png",
    "12_collaboration_over_time.png",
    "14_cluster_projection_svd.png",
    "17_coauthor_network.png",
    "18_paper_similarity_network.png",
    "25_internal_reference_time_lag.png",
    "13_cluster_sizes.png",
    "15_cluster_evolution_area.png",
    "16_cluster_heatmap_over_time.png",
    "27_reference_cluster_transition_heatmap.png",
]


def log(message: str) -> None:
    print(f"[review-analysis] {message}")


def ensure_dependencies(input_path: Path) -> None:
    missing = []
    for module_name, package_name in REQUIRED_MODULES.items():
        if importlib.util.find_spec(module_name) is None:
            missing.append(package_name)

    if input_path.suffix.lower() in {".xlsx", ".xls"} and importlib.util.find_spec("openpyxl") is None:
        missing.append("openpyxl")

    if missing:
        missing_unique = sorted(set(missing))
        pkg_list = " ".join(missing_unique)
        message = [
            "Missing Python dependencies detected.",
            f"Install them with: python -m pip install {pkg_list}",
        ]
        raise SystemExit("\n".join(message))


def import_runtime_dependencies() -> Dict[str, object]:
    import numpy as np
    import pandas as pd
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import networkx as nx
    from sklearn.cluster import MiniBatchKMeans
    from sklearn.decomposition import TruncatedSVD
    from sklearn.feature_extraction.text import CountVectorizer, ENGLISH_STOP_WORDS, TfidfVectorizer
    from sklearn.metrics import silhouette_score
    from sklearn.neighbors import NearestNeighbors

    return {
        "np": np,
        "pd": pd,
        "plt": plt,
        "nx": nx,
        "MiniBatchKMeans": MiniBatchKMeans,
        "TruncatedSVD": TruncatedSVD,
        "CountVectorizer": CountVectorizer,
        "TfidfVectorizer": TfidfVectorizer,
        "ENGLISH_STOP_WORDS": ENGLISH_STOP_WORDS,
        "silhouette_score": silhouette_score,
        "NearestNeighbors": NearestNeighbors,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Advanced bibliometric review analysis")
    parser.add_argument(
        "--input",
        type=str,
        default="",
        help="Input bibliography file (.csv, .xlsx, or .xls).",
    )
    parser.add_argument("--output-dir", type=str, default="review_analysis_output", help="Output directory")
    parser.add_argument("--random-state", type=int, default=42, help="Random seed")
    parser.add_argument("--min-author-occurrences", type=int, default=4, help="Minimum papers per author in coauthor network")
    parser.add_argument("--max-authors-per-paper", type=int, default=15, help="Upper cap for coauthor edge creation")
    parser.add_argument("--max-paper-network-docs", type=int, default=2500, help="Max documents for similarity network")
    parser.add_argument("--paper-sim-threshold", type=float, default=0.33, help="Similarity threshold for paper links")
    parser.add_argument("--neighbors", type=int, default=8, help="Nearest neighbors used for similarity network")
    parser.add_argument("--k-min", type=int, default=4, help="Minimum number of clusters to test")
    parser.add_argument("--k-max", type=int, default=12, help="Maximum number of clusters to test")
    return parser.parse_args()


def find_default_input(base_dir: Path) -> Path:
    patterns = [
        "*_articles_with_pubmed_abstracts_*.xlsx",
        "*_articles_with_pubmed_abstracts_*.csv",
        "*.xlsx",
        "*.csv",
    ]

    seen = set()
    candidates = []
    for pattern in patterns:
        for path in sorted(base_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True):
            if not path.is_file() or path.name in seen:
                continue
            seen.add(path.name)
            candidates.append(path)

    if candidates:
        return candidates[0]

    raise FileNotFoundError(
        "No input bibliography file found. Provide --input or place a CSV/XLSX file in the working directory."
    )


def detect_csv_separator(path: Path) -> str:
    with path.open("r", encoding="utf-8", errors="ignore") as fh:
        first_line = fh.readline()
    return ";" if first_line.count(";") >= first_line.count(",") else ","


def clean_text(value: object) -> str:
    text = "" if value is None else str(value)
    if text.lower() == "nan":
        return ""
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_author_name(name: str) -> str:
    text = clean_text(name).strip(" ;,")
    if not text:
        return ""
    text = re.sub(r"\s*\.\s*", ".", text)
    text = re.sub(r"\s+", " ", text)
    tokens = []
    for token in text.split(" "):
        if re.fullmatch(r"[A-Za-z]\.", token):
            tokens.append(token.upper())
        elif token.isupper() and len(token) <= 4:
            tokens.append(token)
        else:
            tokens.append(token.capitalize())
    return " ".join(tokens).strip()


def split_authors(authors_raw: str) -> List[str]:
    text = clean_text(authors_raw)
    if not text:
        return []

    if ";" in text:
        parts = text.split(";")
    elif "|" in text:
        parts = text.split("|")
    elif re.search(r"\band\b", text, flags=re.IGNORECASE):
        parts = re.split(r"\band\b", text, flags=re.IGNORECASE)
    else:
        comma_count = text.count(",")
        if comma_count >= 3:
            parts = text.split(",")
        else:
            parts = [text]

    seen = set()
    authors = []
    for part in parts:
        author = normalize_author_name(part)
        if author and author not in seen:
            seen.add(author)
            authors.append(author)
    return authors


def ensure_output_dirs(output_dir: Path) -> Dict[str, Path]:
    figures = output_dir / "figures"
    tables = output_dir / "tables"
    networks = output_dir / "networks"
    for p in (output_dir, figures, tables, networks):
        p.mkdir(parents=True, exist_ok=True)
    return {"base": output_dir, "figures": figures, "tables": tables, "networks": networks}


def save_figure(plt, output_path: Path, use_tight_layout: bool = True) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if use_tight_layout:
        plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()


def remove_stale_figure_outputs(figures_dir: Path) -> None:
    for filename in SUPPRESSED_FIGURE_FILENAMES:
        path = figures_dir / filename
        if path.exists():
            path.unlink()


def load_dataset(pd, input_path: Path):
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        sep = detect_csv_separator(input_path)
        return pd.read_csv(input_path, sep=sep, dtype=str, encoding="utf-8", low_memory=False)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(input_path, dtype=str)
    raise ValueError(f"Unsupported input format: {input_path.suffix}")


def prepare_dataframe(pd, df_raw):
    df = df_raw.copy()
    df.columns = [clean_text(c).lower() for c in df.columns]

    required = [
        "title",
        "authors",
        "journal",
        "published_date",
        "doi",
        "url",
        "issn",
        "abstract",
        "source",
        "relevance",
        "label",
    ]
    for col in required:
        if col not in df.columns:
            df[col] = ""

    text_columns = ["title", "authors", "journal", "doi", "url", "issn", "abstract", "source", "label", "published_date"]
    for col in text_columns:
        df[col] = df[col].map(clean_text)

    df = df[df["title"].str.len() > 0].copy()
    df["doi_norm"] = df["doi"].str.lower().str.strip()

    before_dedupe = len(df)
    with_doi = df[df["doi_norm"] != ""].drop_duplicates(subset="doi_norm", keep="first")
    without_doi = df[df["doi_norm"] == ""]
    df = pd.concat([with_doi, without_doi], ignore_index=True)
    deduped = before_dedupe - len(df)

    df["published_dt"] = pd.to_datetime(df["published_date"], errors="coerce")
    df["year"] = df["published_dt"].dt.year.astype("Int64")
    month_period = df["published_dt"].dt.to_period("M").astype(str)
    df["month"] = month_period.where(df["published_dt"].notna(), "")

    df["author_list"] = df["authors"].map(split_authors)
    df["n_authors"] = df["author_list"].map(len).astype(int)
    df["has_abstract"] = (df["abstract"].str.len() > 0).astype(int)
    df["relevance"] = pd.to_numeric(df["relevance"], errors="coerce")

    df["text_for_nlp"] = (df["title"] + " " + df["abstract"]).map(clean_text)
    empty_text_mask = df["text_for_nlp"].str.len() == 0
    df.loc[empty_text_mask, "text_for_nlp"] = df.loc[empty_text_mask, "title"]

    quality = {
        "rows_initial": int(len(df_raw)),
        "rows_after_title_filter": int(before_dedupe),
        "rows_after_deduplication": int(len(df)),
        "duplicates_removed_by_doi": int(deduped),
    }
    return df.reset_index(drop=True), quality


def write_data_quality_tables(pd, df, tables_dir: Path, quality_info: Dict[str, int]) -> None:
    rows = []
    for col in ["title", "authors", "journal", "published_date", "doi", "url", "issn", "abstract", "source", "label", "relevance"]:
        missing = int((df[col].astype(str).str.strip() == "").sum())
        rows.append(
            {
                "column": col,
                "missing_count": missing,
                "missing_pct": round(100.0 * missing / max(len(df), 1), 2),
            }
        )
    quality_df = pd.DataFrame(rows).sort_values("missing_count", ascending=False)
    quality_df.to_csv(tables_dir / "data_quality.csv", index=False)

    quality_meta_df = pd.DataFrame(
        [{"metric": key, "value": value} for key, value in quality_info.items()]
    )
    quality_meta_df.to_csv(tables_dir / "dataset_summary_metrics.csv", index=False)


def compute_author_counts(df, pd):
    exploded = df[["year", "author_list"]].explode("author_list").rename(columns={"author_list": "author"})
    exploded["author"] = exploded["author"].fillna("").astype(str).str.strip()
    exploded = exploded[exploded["author"] != ""]
    author_counts = exploded["author"].value_counts()
    return exploded, author_counts


def plot_publication_timeline(df, pd, plt, figures_dir: Path, tables_dir: Path) -> None:
    dated = df[df["year"].notna()].copy()
    if dated.empty:
        log("No valid publication dates found. Timeline plots skipped.")
        return

    yearly = dated.groupby("year").size().sort_index().astype(int)
    yearly_df = yearly.rename("paper_count").reset_index()
    yearly_df.to_csv(tables_dir / "yearly_publication_counts.csv", index=False)

    rolling = yearly.rolling(window=3, min_periods=1).mean()
    plt.figure(figsize=(12, 6))
    plt.bar(yearly.index.astype(int), yearly.values, color="#4E79A7", alpha=0.75, label="Papers / year")
    plt.plot(yearly.index.astype(int), rolling.values, color="#F28E2B", linewidth=2.5, label="3y moving average")
    plt.xlabel("Year")
    plt.ylabel("Number of papers")
    plt.title("Publication dynamics over time")
    plt.legend()
    save_figure(plt, figures_dir / "01_publications_per_year.png")

    cumulative = yearly.cumsum()
    plt.figure(figsize=(12, 6))
    plt.plot(cumulative.index.astype(int), cumulative.values, color="#59A14F", linewidth=2.5)
    plt.xlabel("Year")
    plt.ylabel("Cumulative papers")
    plt.title("Cumulative publication growth")
    save_figure(plt, figures_dir / "02_cumulative_publications.png")

    growth = yearly.pct_change() * 100.0
    growth_df = growth.rename("growth_pct_vs_prev_year").reset_index()
    growth_df.to_csv(tables_dir / "yearly_growth_rate.csv", index=False)

    plt.figure(figsize=(12, 6))
    plt.axhline(0, color="#666666", linewidth=1)
    plt.plot(growth.index.astype(int), growth.values, color="#E15759", marker="o", linewidth=1.6)
    plt.xlabel("Year")
    plt.ylabel("Growth vs previous year (%)")
    plt.title("Year-over-year publication growth")
    save_figure(plt, figures_dir / "03_yearly_growth_rate.png")

    abstract_rate = dated.groupby("year")["has_abstract"].mean() * 100.0
    abstract_rate_df = abstract_rate.rename("abstract_coverage_pct").reset_index()
    abstract_rate_df.to_csv(tables_dir / "abstract_coverage_by_year.csv", index=False)

    plt.figure(figsize=(12, 6))
    plt.plot(abstract_rate.index.astype(int), abstract_rate.values, color="#76B7B2", marker="o", linewidth=2)
    plt.ylim(0, 100)
    plt.xlabel("Year")
    plt.ylabel("Papers with abstract (%)")
    plt.title("Abstract coverage by year")
    save_figure(plt, figures_dir / "04_abstract_coverage_by_year.png")


def plot_journal_statistics(df, pd, np, plt, figures_dir: Path, tables_dir: Path) -> None:
    journal_series = df["journal"].copy()
    journal_series = journal_series.where(journal_series.str.len() > 0, "Unknown journal")
    journal_counts = journal_series.value_counts()

    top_journals = journal_counts.rename_axis("journal").reset_index(name="paper_count")
    top_journals.to_csv(tables_dir / "top_journals.csv", index=False)

    top20 = top_journals.head(20).sort_values("paper_count", ascending=True)
    plt.figure(figsize=(12, 8))
    plt.barh(top20["journal"], top20["paper_count"], color="#4E79A7")
    plt.xlabel("Number of papers")
    plt.title("Top 20 journals by paper count")
    save_figure(plt, figures_dir / "05_top_journals.png")

    rank = np.arange(1, len(journal_counts) + 1)
    share = journal_counts.values / max(journal_counts.sum(), 1)
    cumulative_share = share.cumsum() * 100.0
    concentration_df = pd.DataFrame(
        {
            "journal_rank": rank,
            "paper_share_pct": share * 100.0,
            "cumulative_share_pct": cumulative_share,
        }
    )
    concentration_df.to_csv(tables_dir / "journal_concentration_curve.csv", index=False)

    plt.figure(figsize=(10, 6))
    plt.plot(rank, cumulative_share, color="#F28E2B", linewidth=2.2)
    plt.axhline(50, color="#999999", linewidth=1, linestyle="--")
    plt.axhline(80, color="#999999", linewidth=1, linestyle="--")
    plt.xlabel("Journal rank (most frequent to least frequent)")
    plt.ylabel("Cumulative paper share (%)")
    plt.title("Journal concentration (rank-frequency cumulative curve)")
    save_figure(plt, figures_dir / "06_journal_concentration_curve.png")

    dated = df[df["year"].notna()].copy()
    if dated.empty:
        return

    def shannon_index(values) -> float:
        counts = values.value_counts().astype(float)
        probs = counts / max(counts.sum(), 1.0)
        return float(-(probs * np.log2(probs + 1e-12)).sum())

    diversity = dated.groupby("year")["journal"].apply(shannon_index)
    diversity_df = diversity.rename("shannon_diversity").reset_index()
    diversity_df.to_csv(tables_dir / "journal_diversity_by_year.csv", index=False)

    plt.figure(figsize=(10, 6))
    plt.plot(diversity.index.astype(int), diversity.values, color="#59A14F", marker="o", linewidth=2)
    plt.xlabel("Year")
    plt.ylabel("Shannon diversity index")
    plt.title("Journal diversity over time")
    save_figure(plt, figures_dir / "07_journal_diversity_over_time.png")


def plot_author_statistics(df, pd, plt, figures_dir: Path, tables_dir: Path):
    exploded, author_counts = compute_author_counts(df, pd)

    top_authors_df = author_counts.rename_axis("author").reset_index(name="paper_count")
    top_authors_df.to_csv(tables_dir / "top_authors.csv", index=False)

    top25 = top_authors_df.head(25).sort_values("paper_count", ascending=True)
    plt.figure(figsize=(12, 10))
    plt.barh(top25["author"], top25["paper_count"], color="#E15759")
    plt.xlabel("Number of papers")
    plt.title("Top 25 authors by paper count")
    save_figure(plt, figures_dir / "08_top_authors.png")

    plt.figure(figsize=(10, 6))
    bins_max = int(min(max(df["n_authors"].max(), 6), 30))
    plt.hist(df["n_authors"], bins=bins_max, color="#76B7B2", edgecolor="white")
    plt.xlabel("Number of authors per paper")
    plt.ylabel("Paper count")
    plt.title("Authorship size distribution")
    save_figure(plt, figures_dir / "09_authors_per_paper_histogram.png")

    dated = df[df["year"].notna()].copy()
    if not dated.empty:
        collab = dated.groupby("year")["n_authors"].agg(["mean", "median"]).reset_index()
        collab.to_csv(tables_dir / "collaboration_index_by_year.csv", index=False)

        plt.figure(figsize=(11, 6))
        plt.plot(collab["year"].astype(int), collab["mean"], label="Mean", color="#4E79A7", linewidth=2)
        plt.plot(collab["year"].astype(int), collab["median"], label="Median", color="#F28E2B", linewidth=2)
        plt.xlabel("Year")
        plt.ylabel("Authors per paper")
        plt.title("Collaboration intensity over time")
        plt.legend()
        save_figure(plt, figures_dir / "10_collaboration_over_time.png")

    return exploded, author_counts


def compute_term_statistics(df, pd, np, CountVectorizer, ENGLISH_STOP_WORDS, plt, figures_dir: Path, tables_dir: Path):
    stop_words = sorted(set(ENGLISH_STOP_WORDS).union(EXTRA_STOPWORDS))
    texts = df["text_for_nlp"].fillna("").astype(str).tolist()
    non_empty_count = sum(1 for t in texts if t.strip())
    if non_empty_count == 0:
        log("No textual content found for term analysis.")
        return None, None, None

    vectorizer = CountVectorizer(
        stop_words=stop_words,
        min_df=10,
        max_df=0.80,
        max_features=8000,
        ngram_range=(1, 2),
    )
    matrix = vectorizer.fit_transform(texts)
    terms = np.array(vectorizer.get_feature_names_out())
    frequencies = np.asarray(matrix.sum(axis=0)).ravel()

    term_df = pd.DataFrame({"term": terms, "frequency": frequencies}).sort_values("frequency", ascending=False)
    term_df.to_csv(tables_dir / "top_terms.csv", index=False)

    top30 = term_df.head(30).iloc[::-1]
    plt.figure(figsize=(12, 10))
    plt.barh(top30["term"], top30["frequency"], color="#59A14F")
    plt.xlabel("Frequency")
    plt.title("Top terms in titles + abstracts")
    save_figure(plt, figures_dir / "11_top_terms.png")

    unigram_vectorizer = CountVectorizer(
        stop_words=stop_words,
        min_df=10,
        max_df=0.85,
        max_features=5000,
        ngram_range=(1, 1),
    )
    unigram_matrix = unigram_vectorizer.fit_transform(texts)
    unigram_terms = np.array(unigram_vectorizer.get_feature_names_out())
    unigram_freq = np.asarray(unigram_matrix.sum(axis=0)).ravel()
    unigram_df = pd.DataFrame({"term": unigram_terms, "frequency": unigram_freq}).sort_values(
        "frequency", ascending=False
    )
    unigram_df.to_csv(tables_dir / "top_unigrams.csv", index=False)

    dated = df[df["year"].notna()].copy()
    if not dated.empty:
        top_terms_for_trends = unigram_df.head(15)["term"].tolist()
        term_to_idx = {t: i for i, t in enumerate(unigram_terms)}
        chosen_idx = [term_to_idx[t] for t in top_terms_for_trends if t in term_to_idx]

        years = sorted(dated["year"].dropna().astype(int).unique())
        heat = np.zeros((len(chosen_idx), len(years)), dtype=float)
        for j, year in enumerate(years):
            mask = df["year"] == year
            if mask.sum() == 0:
                continue
            mask_np = mask.to_numpy(dtype=bool, na_value=False)
            yearly_counts = np.asarray(unigram_matrix[mask_np][:, chosen_idx].sum(axis=0)).ravel()
            heat[:, j] = yearly_counts / max(mask.sum(), 1) * 100.0

        trend_df_rows = []
        for term_pos, term_idx in enumerate(chosen_idx):
            term_name = unigram_terms[term_idx]
            for year_pos, year in enumerate(years):
                trend_df_rows.append(
                    {"term": term_name, "year": int(year), "count_per_100_papers": float(heat[term_pos, year_pos])}
                )
        pd.DataFrame(trend_df_rows).to_csv(tables_dir / "term_trends_by_year.csv", index=False)

        plt.figure(figsize=(14, 8))
        im = plt.imshow(heat, aspect="auto", cmap="YlGnBu", interpolation="nearest")
        plt.colorbar(im, label="Occurrences per 100 papers")
        plt.yticks(range(len(chosen_idx)), [unigram_terms[idx] for idx in chosen_idx])
        x_positions = list(range(len(years)))
        x_labels = [str(y) for y in years]
        if len(x_labels) > 20:
            step = max(1, len(x_labels) // 20)
            x_positions = x_positions[::step]
            x_labels = x_labels[::step]
        plt.xticks(x_positions, x_labels, rotation=45, ha="right")
        plt.xlabel("Year")
        plt.ylabel("Term")
        plt.title("Top term trends over time (normalized)")
        save_figure(plt, figures_dir / "12_term_trend_heatmap.png")

    return vectorizer, matrix, term_df


def compute_domain_theme_signals(df, pd, plt, figures_dir: Path, tables_dir: Path):
    text = df["text_for_nlp"].fillna("").astype(str).str.lower()

    theme_patterns = {
        "microemboli_core": r"\b(?:micro[- ]?embol\w*|embol\w*|embolus|emboli|high[- ]intensity transient signal|hits?)\b",
        "tcd_cerebral": r"\b(?:transcranial doppler|tcd\b|middle cerebral artery|mca\b|cerebral)\b",
        "ultrasound_echo": r"\b(?:ultrasound|doppler|sonograph\w*|echocardiograph\w*)\b",
        "ml_ai": r"\b(?:machine learning|deep learning|neural network|cnn|svm|classification)\b",
        "intervention_stimulation": r"\b(?:transcranial magnetic stimulation|tms\b|tdcs\b|stimulation)\b",
        "radar_lidar": r"\b(?:radar|lidar)\b",
        "laser_vibrometry": r"\b(?:laser doppler|vibromet\w*|flowmetry)\b",
    }

    for theme, pattern in theme_patterns.items():
        df[f"theme_{theme}"] = text.str.contains(pattern, regex=True, na=False)

    df["theme_core_microemboli"] = df["theme_microemboli_core"] & (df["theme_tcd_cerebral"] | df["theme_ultrasound_echo"])
    df["theme_probable_offtopic"] = (~df["theme_core_microemboli"]) & (
        df["theme_radar_lidar"] | df["theme_intervention_stimulation"] | df["theme_laser_vibrometry"]
    )

    theme_cols = [f"theme_{name}" for name in theme_patterns.keys()] + [
        "theme_core_microemboli",
        "theme_probable_offtopic",
    ]

    prevalence_rows = []
    for col in theme_cols:
        count = int(df[col].sum())
        prevalence_rows.append(
            {
                "theme": col.replace("theme_", ""),
                "paper_count": count,
                "paper_pct": round(100.0 * count / max(len(df), 1), 2),
            }
        )
    prevalence_df = pd.DataFrame(prevalence_rows).sort_values("paper_pct", ascending=False)
    prevalence_df.to_csv(tables_dir / "domain_theme_prevalence.csv", index=False)

    top = prevalence_df.sort_values("paper_pct", ascending=True)
    plt.figure(figsize=(12, 7))
    plt.barh(top["theme"], top["paper_pct"], color="#4E79A7")
    plt.xlabel("Paper share (%)")
    plt.title("Domain signal prevalence in corpus")
    save_figure(plt, figures_dir / "19_domain_theme_prevalence.png")

    dated = df[df["year"].notna()].copy()
    if not dated.empty:
        by_year = dated.groupby("year")[theme_cols].mean() * 100.0
        by_year.to_csv(tables_dir / "domain_theme_by_year.csv")

        selected_cols = [
            "theme_core_microemboli",
            "theme_probable_offtopic",
            "theme_ultrasound_echo",
            "theme_tcd_cerebral",
            "theme_ml_ai",
            "theme_microemboli_core",
        ]
        selected_cols = [c for c in selected_cols if c in by_year.columns]

        plt.figure(figsize=(13, 7))
        for col in selected_cols:
            plt.plot(
                by_year.index.astype(int),
                by_year[col],
                linewidth=2,
                label=col.replace("theme_", ""),
            )
        plt.xlabel("Year")
        plt.ylabel("Paper share (%)")
        plt.title("Domain signal dynamics over time")
        plt.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0))
        save_figure(plt, figures_dir / "20_domain_theme_evolution.png")

        stacked_cols = ["theme_core_microemboli", "theme_probable_offtopic"]
        base = by_year[stacked_cols].copy()
        base["other"] = (100.0 - base.sum(axis=1)).clip(lower=0.0)

        plt.figure(figsize=(12, 7))
        years = base.index.astype(int).to_numpy()
        plt.stackplot(
            years,
            [base["theme_core_microemboli"].to_numpy(), base["theme_probable_offtopic"].to_numpy(), base["other"].to_numpy()],
            labels=["core_microemboli", "probable_offtopic", "other"],
            alpha=0.9,
        )
        plt.xlabel("Year")
        plt.ylabel("Share of papers (%)")
        plt.title("Corpus composition: core vs probable off-topic vs other")
        plt.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0))
        save_figure(plt, figures_dir / "21_core_vs_offtopic_over_time.png")

    return df, prevalence_df


def choose_cluster_count(np, MiniBatchKMeans, silhouette_score, tfidf_matrix, random_state: int, k_min: int, k_max: int):
    n_samples = tfidf_matrix.shape[0]
    if n_samples < 50:
        return max(2, min(k_max, n_samples // 5 or 2)), []

    rng = np.random.RandomState(random_state)
    sample_size = min(1800, n_samples)
    if n_samples > sample_size:
        idx = rng.choice(np.arange(n_samples), size=sample_size, replace=False)
        sample_matrix = tfidf_matrix[idx]
    else:
        sample_matrix = tfidf_matrix

    tested_scores = []
    upper = max(k_min, min(k_max, int(math.sqrt(n_samples)) + 4))
    for k in range(k_min, upper + 1):
        model = MiniBatchKMeans(n_clusters=k, random_state=random_state, batch_size=1024, n_init=20)
        labels = model.fit_predict(sample_matrix)
        unique_labels = len(set(labels))
        if unique_labels < 2:
            continue
        try:
            score = float(silhouette_score(sample_matrix, labels, metric="cosine"))
            tested_scores.append((k, score))
        except Exception:
            continue

    if not tested_scores:
        return min(max(4, k_min), upper), tested_scores

    best_k = max(tested_scores, key=lambda x: x[1])[0]
    return best_k, tested_scores


def cluster_documents(
    df,
    pd,
    np,
    TfidfVectorizer,
    MiniBatchKMeans,
    TruncatedSVD,
    silhouette_score,
    plt,
    figures_dir: Path,
    tables_dir: Path,
    random_state: int,
    k_min: int,
    k_max: int,
):
    texts = df["text_for_nlp"].fillna("").astype(str).tolist()
    vectorizer = TfidfVectorizer(
        stop_words="english",
        min_df=5,
        max_df=0.80,
        max_features=15000,
        ngram_range=(1, 2),
        sublinear_tf=True,
    )
    tfidf = vectorizer.fit_transform(texts)

    k_best, scores = choose_cluster_count(np, MiniBatchKMeans, silhouette_score, tfidf, random_state, k_min, k_max)
    score_df = pd.DataFrame(scores, columns=["k", "silhouette_cosine"])
    score_df.to_csv(tables_dir / "cluster_model_selection.csv", index=False)

    model = MiniBatchKMeans(n_clusters=k_best, random_state=random_state, batch_size=1024, n_init=20)
    labels = model.fit_predict(tfidf)
    df["cluster"] = labels.astype(int)

    terms = np.array(vectorizer.get_feature_names_out())
    rows = []
    for c in range(k_best):
        center = model.cluster_centers_[c]
        top_idx = np.argsort(center)[::-1][:12]
        top_terms = [terms[i] for i in top_idx]
        rows.append(
            {
                "cluster": int(c),
                "paper_count": int((labels == c).sum()),
                "top_terms": ", ".join(top_terms),
            }
        )
    cluster_terms_df = pd.DataFrame(rows).sort_values("paper_count", ascending=False)
    cluster_terms_df.to_csv(tables_dir / "cluster_top_terms.csv", index=False)

    cluster_size = df["cluster"].value_counts().sort_index()
    cluster_size_df = cluster_size.rename_axis("cluster").reset_index(name="paper_count")
    cluster_size_df.to_csv(tables_dir / "cluster_sizes.csv", index=False)

    if EXPORT_SUPPLEMENTARY_FIGURES and tfidf.shape[0] > 1:
        rng = np.random.RandomState(random_state)
        sample_size = min(6000, tfidf.shape[0])
        sample_idx = rng.choice(np.arange(tfidf.shape[0]), size=sample_size, replace=False)
        svd = TruncatedSVD(n_components=2, random_state=random_state)
        coords = svd.fit_transform(tfidf[sample_idx])

        plt.figure(figsize=(11, 8))
        scatter = plt.scatter(
            coords[:, 0],
            coords[:, 1],
            c=df.iloc[sample_idx]["cluster"].astype(int),
            cmap="tab20",
            s=10,
            alpha=0.70,
            linewidths=0,
        )
        plt.colorbar(scatter, label="Cluster")
        plt.xlabel("SVD component 1")
        plt.ylabel("SVD component 2")
        plt.title("Document embedding projection colored by cluster")
        save_figure(plt, figures_dir / "14_cluster_projection_svd.png")

    dated = df[df["year"].notna()].copy()
    if not dated.empty:
        by_year = dated.groupby(["year", "cluster"]).size().unstack(fill_value=0).sort_index()
        by_year.to_csv(tables_dir / "cluster_by_year_counts.csv")

    return df, tfidf, vectorizer, model, score_df, cluster_terms_df


def build_coauthor_network(
    df,
    pd,
    nx,
    min_author_occurrences: int,
    max_authors_per_paper: int,
):
    author_counter = Counter()
    for authors in df["author_list"]:
        for author in authors:
            if author:
                author_counter[author] += 1

    eligible_authors = {a for a, c in author_counter.items() if c >= min_author_occurrences}
    graph = nx.Graph()
    for author in eligible_authors:
        graph.add_node(author, papers=int(author_counter[author]))

    for authors in df["author_list"]:
        clean = sorted({a for a in authors if a in eligible_authors})
        if len(clean) < 2:
            continue
        if len(clean) > max_authors_per_paper:
            continue
        for a, b in itertools.combinations(clean, 2):
            if graph.has_edge(a, b):
                graph[a][b]["weight"] += 1
            else:
                graph.add_edge(a, b, weight=1)

    degrees = dict(graph.degree())
    weighted_degrees = dict(graph.degree(weight="weight"))
    node_rows = []
    for node, data in graph.nodes(data=True):
        node_rows.append(
            {
                "author": node,
                "papers": int(data.get("papers", 0)),
                "degree": int(degrees.get(node, 0)),
                "weighted_degree": float(weighted_degrees.get(node, 0.0)),
            }
        )
    nodes_df = pd.DataFrame(node_rows).sort_values(["papers", "degree"], ascending=False)

    edge_rows = []
    for u, v, data in graph.edges(data=True):
        edge_rows.append({"author_a": u, "author_b": v, "weight": float(data.get("weight", 1))})
    edges_df = pd.DataFrame(edge_rows).sort_values("weight", ascending=False)

    return graph, nodes_df, edges_df


def compute_coauthor_centrality(nx, pd, graph):
    if graph.number_of_nodes() == 0:
        return pd.DataFrame(columns=["author", "degree_centrality", "betweenness", "eigenvector", "papers"])

    degree_cent = nx.degree_centrality(graph)

    max_for_betweenness = 900
    if graph.number_of_nodes() <= max_for_betweenness:
        betweenness = nx.betweenness_centrality(graph, weight="weight", normalized=True)
    else:
        top_nodes = [n for n, _ in sorted(graph.degree(), key=lambda x: x[1], reverse=True)[:max_for_betweenness]]
        sub = graph.subgraph(top_nodes).copy()
        sub_bet = nx.betweenness_centrality(sub, weight="weight", normalized=True)
        betweenness = {node: float(sub_bet.get(node, 0.0)) for node in graph.nodes()}

    try:
        eigen = nx.eigenvector_centrality(graph, max_iter=1500, weight="weight")
    except Exception:
        eigen = {node: 0.0 for node in graph.nodes()}

    rows = []
    for node, data in graph.nodes(data=True):
        rows.append(
            {
                "author": node,
                "papers": int(data.get("papers", 0)),
                "degree_centrality": float(degree_cent.get(node, 0.0)),
                "betweenness": float(betweenness.get(node, 0.0)),
                "eigenvector": float(eigen.get(node, 0.0)),
            }
        )
    centrality_df = pd.DataFrame(rows).sort_values(
        ["degree_centrality", "eigenvector", "papers"], ascending=False
    )
    return centrality_df


def plot_coauthor_network(nx, np, plt, graph, figures_dir: Path, max_nodes: int = 140) -> None:
    if graph.number_of_nodes() == 0:
        log("Coauthor network is empty. Plot skipped.")
        return

    top_nodes = [n for n, _ in sorted(graph.degree(weight="weight"), key=lambda x: x[1], reverse=True)[:max_nodes]]
    sub = graph.subgraph(top_nodes).copy()
    if sub.number_of_nodes() == 0:
        return

    plt.figure(figsize=(14, 11))
    pos = nx.spring_layout(sub, seed=42, k=1.0 / math.sqrt(max(sub.number_of_nodes(), 2)))

    node_sizes = [80 + 35 * sub.nodes[n].get("papers", 1) for n in sub.nodes()]
    edge_weights = [sub[u][v].get("weight", 1.0) for u, v in sub.edges()]
    max_w = max(edge_weights) if edge_weights else 1.0
    edge_widths = [0.3 + 2.8 * (w / max_w) for w in edge_weights]

    nx.draw_networkx_edges(sub, pos, width=edge_widths, alpha=0.25, edge_color="#6b6b6b")
    nx.draw_networkx_nodes(sub, pos, node_size=node_sizes, node_color="#4E79A7", alpha=0.85, linewidths=0.3)

    label_nodes = sorted(sub.nodes(), key=lambda n: sub.nodes[n].get("papers", 0), reverse=True)[:35]
    labels = {n: n for n in label_nodes}
    nx.draw_networkx_labels(sub, pos, labels=labels, font_size=7)

    plt.title("Co-authorship network (top connected authors)")
    plt.axis("off")
    save_figure(plt, figures_dir / "17_coauthor_network.png")


def sample_indices_for_paper_network(np, df, max_docs: int, random_state: int) -> List[int]:
    n = len(df)
    if n <= max_docs:
        return list(range(n))

    rng = np.random.RandomState(random_state)
    if "cluster" not in df.columns:
        return sorted(rng.choice(np.arange(n), size=max_docs, replace=False).tolist())

    sampled = []
    for _, group in df.groupby("cluster"):
        share = len(group) / max(n, 1)
        k = max(1, int(round(share * max_docs)))
        idx = group.index.to_numpy()
        selected = rng.choice(idx, size=min(k, len(idx)), replace=False)
        sampled.extend(int(x) for x in selected)

    sampled = sorted(set(sampled))
    if len(sampled) > max_docs:
        sampled = sampled[:max_docs]
    elif len(sampled) < max_docs:
        remaining = sorted(set(range(n)).difference(sampled))
        extra_needed = max_docs - len(sampled)
        if extra_needed > 0 and remaining:
            extra = rng.choice(np.array(remaining), size=min(extra_needed, len(remaining)), replace=False)
            sampled.extend(int(x) for x in extra.tolist())
            sampled = sorted(set(sampled))
    return sampled[:max_docs]


def build_paper_similarity_network(
    df,
    pd,
    np,
    nx,
    NearestNeighbors,
    tfidf_matrix,
    max_docs: int,
    neighbors: int,
    threshold: float,
    random_state: int,
):
    sample_idx = sample_indices_for_paper_network(np, df, max_docs=max_docs, random_state=random_state)
    if not sample_idx:
        return nx.Graph(), pd.DataFrame(), pd.DataFrame(), sample_idx

    sub_matrix = tfidf_matrix[sample_idx]
    k_neighbors = min(neighbors + 1, sub_matrix.shape[0])
    if k_neighbors <= 1:
        return nx.Graph(), pd.DataFrame(), pd.DataFrame(), sample_idx

    nn = NearestNeighbors(n_neighbors=k_neighbors, metric="cosine", algorithm="brute")
    nn.fit(sub_matrix)
    distances, indices = nn.kneighbors(sub_matrix, return_distance=True)

    candidate_thresholds = [threshold, max(0.25, threshold - 0.05), max(0.20, threshold - 0.10)]
    edge_map: Dict[Tuple[int, int], float] = {}
    chosen_threshold = candidate_thresholds[0]

    for thr in candidate_thresholds:
        edge_map.clear()
        for i in range(sub_matrix.shape[0]):
            src = int(sample_idx[i])
            for j, dist in zip(indices[i, 1:], distances[i, 1:]):
                tgt = int(sample_idx[int(j)])
                if src == tgt:
                    continue
                sim = 1.0 - float(dist)
                if sim < thr:
                    continue
                a, b = (src, tgt) if src < tgt else (tgt, src)
                existing = edge_map.get((a, b))
                if existing is None or sim > existing:
                    edge_map[(a, b)] = sim
        chosen_threshold = thr
        if len(edge_map) >= max(1000, int(1.5 * len(sample_idx))) or thr <= 0.20:
            break

    graph = nx.Graph()
    for idx in sample_idx:
        row = df.iloc[idx]
        graph.add_node(
            int(idx),
            title=row.get("title", ""),
            year=int(row["year"]) if str(row.get("year", "")) not in {"", "<NA>", "nan"} else -1,
            cluster=int(row["cluster"]) if "cluster" in df.columns else -1,
            journal=row.get("journal", ""),
            doi=row.get("doi", ""),
        )

    for (a, b), w in edge_map.items():
        graph.add_edge(int(a), int(b), weight=float(w))

    node_rows = []
    for node, data in graph.nodes(data=True):
        node_rows.append(
            {
                "paper_id": int(node),
                "cluster": int(data.get("cluster", -1)),
                "year": int(data.get("year", -1)),
                "journal": data.get("journal", ""),
                "doi": data.get("doi", ""),
                "title": data.get("title", ""),
            }
        )
    nodes_df = pd.DataFrame(node_rows)

    edge_rows = []
    for u, v, data in graph.edges(data=True):
        edge_rows.append({"paper_id_a": int(u), "paper_id_b": int(v), "similarity": float(data.get("weight", 0.0))})
    edges_df = pd.DataFrame(edge_rows).sort_values("similarity", ascending=False)
    edges_df["threshold_used"] = float(chosen_threshold)

    return graph, nodes_df, edges_df, sample_idx


def plot_paper_similarity_network(nx, plt, np, graph, figures_dir: Path, max_nodes: int = 260) -> None:
    if graph.number_of_nodes() == 0 or graph.number_of_edges() == 0:
        log("Paper similarity network too sparse. Plot skipped.")
        return

    components = sorted(nx.connected_components(graph), key=len, reverse=True)
    largest = graph.subgraph(components[0]).copy() if components else graph.copy()

    if largest.number_of_nodes() > max_nodes:
        top_nodes = [n for n, _ in sorted(largest.degree(weight="weight"), key=lambda x: x[1], reverse=True)[:max_nodes]]
        largest = largest.subgraph(top_nodes).copy()

    plt.figure(figsize=(14, 11))
    pos = nx.spring_layout(largest, seed=42, k=1.0 / math.sqrt(max(largest.number_of_nodes(), 2)))

    edge_weights = [largest[u][v].get("weight", 0.0) for u, v in largest.edges()]
    max_w = max(edge_weights) if edge_weights else 1.0
    edge_widths = [0.2 + 2.3 * (w / max_w) for w in edge_weights]

    clusters = np.array([largest.nodes[n].get("cluster", -1) for n in largest.nodes()])
    node_sizes = [25 + 12 * largest.degree(n) for n in largest.nodes()]

    nx.draw_networkx_edges(largest, pos, width=edge_widths, alpha=0.20, edge_color="#777777")
    nx.draw_networkx_nodes(
        largest,
        pos,
        node_size=node_sizes,
        node_color=clusters,
        cmap="tab20",
        alpha=0.85,
        linewidths=0.0,
    )

    plt.title("Paper similarity network (largest connected component)")
    plt.axis("off")
    save_figure(plt, figures_dir / "18_paper_similarity_network.png")


def save_enriched_dataset(df, output_path: Path) -> None:
    export_df = df.copy()
    export_df["author_list"] = export_df["author_list"].map(lambda x: "; ".join(x) if isinstance(x, list) else "")
    export_df.to_csv(output_path, sep=";", index=False, encoding="utf-8")


def generate_markdown_report(
    df,
    quality_info: Dict[str, int],
    author_counts,
    theme_prevalence_df,
    cluster_terms_df,
    cluster_scores_df,
    top_terms_df,
    coauthor_nodes_df,
    coauthor_edges_df,
    paper_network_nodes_df,
    paper_network_edges_df,
    output_report_path: Path,
    input_path: Path,
) -> None:
    total = len(df)
    dated = df[df["year"].notna()]
    min_year = int(dated["year"].min()) if not dated.empty else None
    max_year = int(dated["year"].max()) if not dated.empty else None
    abstract_rate = float(df["has_abstract"].mean() * 100.0) if total else 0.0
    unique_journals = int(df["journal"].nunique())
    unique_authors = int(len(author_counts))
    median_authors = float(df["n_authors"].median()) if total else 0.0
    mean_authors = float(df["n_authors"].mean()) if total else 0.0

    top_journals = (
        df["journal"].where(df["journal"].str.len() > 0, "Unknown journal").value_counts().head(10).reset_index()
    )
    top_journals.columns = ["journal", "paper_count"]
    top_authors = author_counts.head(10).reset_index()
    top_authors.columns = ["author", "paper_count"]
    top_years = (
        df[df["year"].notna()]
        .groupby("year")
        .size()
        .sort_values(ascending=False)
        .head(10)
        .reset_index(name="paper_count")
    )

    lines: List[str] = []
    lines.append("# Bibliometric Review Report")
    lines.append("")
    lines.append("## Scope")
    lines.append(f"- Input file: `{input_path.name}`")
    lines.append(f"- Papers analyzed: **{total}**")
    if min_year is not None and max_year is not None:
        lines.append(f"- Date coverage: **{min_year} -> {max_year}**")
    lines.append(f"- Unique journals: **{unique_journals}**")
    lines.append(f"- Unique authors (parsed): **{unique_authors}**")
    lines.append(f"- Abstract coverage: **{abstract_rate:.1f}%**")
    lines.append(f"- Authors per paper: mean **{mean_authors:.2f}**, median **{median_authors:.1f}**")
    lines.append("")

    lines.append("## Data Quality")
    for key, value in quality_info.items():
        lines.append(f"- {key}: **{value}**")
    lines.append("")

    lines.append("## Publication Peaks")
    for _, row in top_years.iterrows():
        lines.append(f"- {int(row['year'])}: {int(row['paper_count'])} papers")
    lines.append("")

    lines.append("## Top Journals")
    for _, row in top_journals.iterrows():
        lines.append(f"- {row['journal']}: {int(row['paper_count'])} papers")
    lines.append("")

    lines.append("## Top Authors")
    for _, row in top_authors.iterrows():
        lines.append(f"- {row['author']}: {int(row['paper_count'])} papers")
    lines.append("")

    lines.append("## Thematic Clusters")
    if cluster_terms_df is not None and not cluster_terms_df.empty:
        for _, row in cluster_terms_df.sort_values("paper_count", ascending=False).iterrows():
            lines.append(
                f"- Cluster {int(row['cluster'])} ({int(row['paper_count'])} papers): {row['top_terms']}"
            )
    else:
        lines.append("- No clusters available.")
    lines.append("")

    lines.append("## Cluster Model Selection")
    if cluster_scores_df is not None and not cluster_scores_df.empty:
        best = cluster_scores_df.sort_values("silhouette_cosine", ascending=False).head(1)
        if not best.empty:
            b = best.iloc[0]
            lines.append(
                f"- Best silhouette on sample: k={int(b['k'])}, score={float(b['silhouette_cosine']):.4f}"
            )
    else:
        lines.append("- No silhouette scores computed.")
    lines.append("")

    lines.append("## Dominant Terms")
    if top_terms_df is not None and not top_terms_df.empty:
        for _, row in top_terms_df.head(15).iterrows():
            lines.append(f"- {row['term']}: {int(row['frequency'])}")
    else:
        lines.append("- No term statistics available.")
    lines.append("")

    lines.append("## Domain Signals")
    if theme_prevalence_df is not None and not theme_prevalence_df.empty:
        for _, row in theme_prevalence_df.head(8).iterrows():
            lines.append(f"- {row['theme']}: {float(row['paper_pct']):.2f}% ({int(row['paper_count'])} papers)")
    else:
        lines.append("- No domain signal table available.")
    lines.append("")

    lines.append("## Network Summary")
    lines.append(
        f"- Coauthor network: {len(coauthor_nodes_df)} nodes, {len(coauthor_edges_df)} edges"
    )
    lines.append(
        f"- Paper similarity network: {len(paper_network_nodes_df)} nodes, {len(paper_network_edges_df)} edges"
    )
    lines.append("")

    lines.append("## Interpretation Notes")
    lines.append("- The paper similarity graph is semantic (title+abstract text), not citation-based.")
    lines.append("- If references/citations are absent in input data, bibliographic coupling and co-citation cannot be computed directly.")
    lines.append("- Clusters are unsupervised themes and should be interpreted using top terms plus manual reading of representative papers.")
    lines.append("")

    output_report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    base_dir = Path.cwd()
    if args.input:
        input_path = Path(args.input).expanduser().resolve()
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
    else:
        input_path = find_default_input(base_dir)
    output_dir = Path(args.output_dir).resolve()

    log(f"Input file: {input_path}")
    log(f"Output directory: {output_dir}")

    ensure_dependencies(input_path)
    deps = import_runtime_dependencies()
    np = deps["np"]
    pd = deps["pd"]
    plt = deps["plt"]
    nx = deps["nx"]
    MiniBatchKMeans = deps["MiniBatchKMeans"]
    TruncatedSVD = deps["TruncatedSVD"]
    CountVectorizer = deps["CountVectorizer"]
    TfidfVectorizer = deps["TfidfVectorizer"]
    ENGLISH_STOP_WORDS = deps["ENGLISH_STOP_WORDS"]
    silhouette_score = deps["silhouette_score"]
    NearestNeighbors = deps["NearestNeighbors"]

    plt.style.use("ggplot")
    out_dirs = ensure_output_dirs(output_dir)

    log("Loading dataset...")
    raw_df = load_dataset(pd, input_path)
    log(f"Raw rows: {len(raw_df)}")

    log("Preparing and cleaning metadata...")
    df, quality_info = prepare_dataframe(pd, raw_df)
    log(f"Rows after cleaning: {len(df)}")

    write_data_quality_tables(pd, df, out_dirs["tables"], quality_info)

    log("Building timeline plots...")
    plot_publication_timeline(df, pd, plt, out_dirs["figures"], out_dirs["tables"])

    log("Building journal plots...")
    plot_journal_statistics(df, pd, np, plt, out_dirs["figures"], out_dirs["tables"])

    log("Building author plots...")
    _, author_counts = plot_author_statistics(df, pd, plt, out_dirs["figures"], out_dirs["tables"])

    log("Computing term statistics...")
    _, _, top_terms_df = compute_term_statistics(
        df, pd, np, CountVectorizer, ENGLISH_STOP_WORDS, plt, out_dirs["figures"], out_dirs["tables"]
    )

    log("Computing domain-specific signals...")
    df, theme_prevalence_df = compute_domain_theme_signals(
        df, pd, plt, out_dirs["figures"], out_dirs["tables"]
    )

    log("Running thematic clustering...")
    df, tfidf_matrix, _, _, cluster_scores_df, cluster_terms_df = cluster_documents(
        df=df,
        pd=pd,
        np=np,
        TfidfVectorizer=TfidfVectorizer,
        MiniBatchKMeans=MiniBatchKMeans,
        TruncatedSVD=TruncatedSVD,
        silhouette_score=silhouette_score,
        plt=plt,
        figures_dir=out_dirs["figures"],
        tables_dir=out_dirs["tables"],
        random_state=args.random_state,
        k_min=args.k_min,
        k_max=args.k_max,
    )

    log("Building coauthorship network...")
    coauthor_graph, coauthor_nodes_df, coauthor_edges_df = build_coauthor_network(
        df=df,
        pd=pd,
        nx=nx,
        min_author_occurrences=args.min_author_occurrences,
        max_authors_per_paper=args.max_authors_per_paper,
    )
    coauthor_nodes_df.to_csv(out_dirs["networks"] / "coauthor_nodes.csv", index=False)
    coauthor_edges_df.to_csv(out_dirs["networks"] / "coauthor_edges.csv", index=False)
    if coauthor_graph.number_of_nodes() > 0:
        nx.write_gexf(coauthor_graph, out_dirs["networks"] / "coauthor_network.gexf")
    coauthor_centrality_df = compute_coauthor_centrality(nx, pd, coauthor_graph)
    coauthor_centrality_df.to_csv(out_dirs["tables"] / "coauthor_centrality.csv", index=False)
    plot_coauthor_network(nx, np, plt, coauthor_graph, out_dirs["figures"])

    log("Building paper similarity network...")
    paper_graph, paper_nodes_df, paper_edges_df, sample_idx = build_paper_similarity_network(
        df=df,
        pd=pd,
        np=np,
        nx=nx,
        NearestNeighbors=NearestNeighbors,
        tfidf_matrix=tfidf_matrix,
        max_docs=args.max_paper_network_docs,
        neighbors=args.neighbors,
        threshold=args.paper_sim_threshold,
        random_state=args.random_state,
    )
    paper_nodes_df.to_csv(out_dirs["networks"] / "paper_similarity_nodes.csv", index=False)
    paper_edges_df.to_csv(out_dirs["networks"] / "paper_similarity_edges.csv", index=False)
    if paper_graph.number_of_nodes() > 0:
        nx.write_gexf(paper_graph, out_dirs["networks"] / "paper_similarity_network.gexf")
    plot_paper_similarity_network(nx, plt, np, paper_graph, out_dirs["figures"])

    log("Saving enriched dataset...")
    save_enriched_dataset(df, out_dirs["tables"] / "cleaned_articles_with_clusters.csv")

    log("Writing markdown report...")
    generate_markdown_report(
        df=df,
        quality_info=quality_info,
        author_counts=author_counts,
        theme_prevalence_df=theme_prevalence_df,
        cluster_terms_df=cluster_terms_df,
        cluster_scores_df=cluster_scores_df,
        top_terms_df=top_terms_df,
        coauthor_nodes_df=coauthor_nodes_df,
        coauthor_edges_df=coauthor_edges_df,
        paper_network_nodes_df=paper_nodes_df,
        paper_network_edges_df=paper_edges_df,
        output_report_path=out_dirs["base"] / "review_report.md",
        input_path=input_path,
    )

    log("Done.")
    log(f"Figures: {out_dirs['figures']}")
    log(f"Tables: {out_dirs['tables']}")
    log(f"Networks: {out_dirs['networks']}")
    log(f"Paper network sample size: {len(sample_idx)}")


# Updated analysis functions below override earlier implementations.


def normalize_doi(value: object) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", text)
    text = re.sub(r"^doi:\s*", "", text)
    return text.strip(" /")


def normalize_title_key(value: object) -> str:
    text = clean_text(value).lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_entity_name(value: object) -> str:
    text = clean_text(value).strip(" ;,")
    if not text:
        return ""
    parts = re.split(r"(\s+|-|/)", text)
    normalized = []
    for part in parts:
        if not part or re.fullmatch(r"\s+|-|/", part):
            normalized.append(part)
            continue
        if part.isupper() and len(part) <= 8:
            normalized.append(part)
        elif re.fullmatch(r"[A-Za-z]\.", part):
            normalized.append(part.upper())
        else:
            normalized.append(part[:1].upper() + part[1:])
    return "".join(normalized).strip()


def normalize_text_for_nlp(value: object) -> str:
    text = clean_text(value)
    text = text.replace("/", " ")
    text = text.replace("-", " ")
    return re.sub(r"\s+", " ", text).strip()


def unique_preserve_order(items: Iterable[object]) -> List[str]:
    seen = set()
    output = []
    for item in items:
        value = clean_text(item)
        if not value or value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def to_semicolon_string(value: object) -> str:
    if isinstance(value, list):
        return "; ".join(clean_text(x) for x in value if clean_text(x))
    return clean_text(value)


def extract_year_from_text(value: object) -> Optional[int]:
    text = clean_text(value)
    match = re.search(r"\b(19|20)\d{2}\b", text)
    if not match:
        return None
    year = int(match.group(0))
    if 1900 <= year <= 2100:
        return year
    return None


def title_similarity(title_a_key: str, title_b_key: str) -> float:
    if not title_a_key or not title_b_key:
        return 0.0
    seq_ratio = difflib.SequenceMatcher(None, title_a_key, title_b_key).ratio()
    tokens_a = set(title_a_key.split())
    tokens_b = set(title_b_key.split())
    if not tokens_a or not tokens_b:
        return seq_ratio
    jaccard = len(tokens_a & tokens_b) / max(len(tokens_a | tokens_b), 1)
    containment = len(tokens_a & tokens_b) / max(min(len(tokens_a), len(tokens_b)), 1)
    return max(seq_ratio, jaccard, containment * 0.98)


def first_non_empty(*values: object) -> str:
    for value in values:
        text = clean_text(value)
        if text:
            return text
    return ""


def write_empty_csv(pd, path: Path, columns: Sequence[str]) -> None:
    pd.DataFrame(columns=list(columns)).to_csv(path, index=False)


def build_scalar_entity_tables(df, pd, source_col: str, entity_col: str, unknown_label: str = ""):
    work = df[["paper_id", "year", source_col]].copy()
    work[entity_col] = work[source_col].map(clean_text)
    if unknown_label:
        work.loc[work[entity_col] == "", entity_col] = unknown_label
    work = work[work[entity_col] != ""].copy()

    if work.empty:
        empty_total = pd.DataFrame(columns=[entity_col, "paper_count"])
        empty_by_year = pd.DataFrame(columns=["year", entity_col, "paper_count"])
        empty_pivot = pd.DataFrame()
        empty_active = pd.DataFrame(columns=[entity_col, "paper_count", "first_year", "last_year", "active_years"])
        return work, empty_total, empty_by_year, empty_pivot, empty_active

    total_df = work.groupby(entity_col).size().reset_index(name="paper_count").sort_values(
        ["paper_count", entity_col], ascending=[False, True]
    )
    dated = work[work["year"].notna()].copy()
    if dated.empty:
        by_year_df = pd.DataFrame(columns=["year", entity_col, "paper_count"])
        pivot_df = pd.DataFrame()
        active_df = total_df.copy()
        active_df["first_year"] = None
        active_df["last_year"] = None
        active_df["active_years"] = 0
        return work, total_df, by_year_df, pivot_df, active_df

    by_year_df = (
        dated.groupby(["year", entity_col]).size().reset_index(name="paper_count").sort_values(
            ["year", "paper_count", entity_col], ascending=[True, False, True]
        )
    )
    pivot_df = by_year_df.pivot(index="year", columns=entity_col, values="paper_count").fillna(0).astype(int).sort_index()
    active_df = (
        dated.groupby(entity_col)["year"]
        .agg(first_year="min", last_year="max", active_years="nunique")
        .reset_index()
        .merge(total_df, on=entity_col, how="left")
        .sort_values(["paper_count", "active_years", entity_col], ascending=[False, False, True])
    )
    active_df = active_df[[entity_col, "paper_count", "first_year", "last_year", "active_years"]]
    return work, total_df, by_year_df, pivot_df, active_df


def build_list_entity_tables(df, pd, list_col: str, entity_col: str):
    work = df[["paper_id", "year", list_col]].explode(list_col).rename(columns={list_col: entity_col})
    work[entity_col] = work[entity_col].fillna("").map(clean_text)
    work = work[work[entity_col] != ""].drop_duplicates(subset=["paper_id", entity_col]).copy()

    if work.empty:
        empty_total = pd.DataFrame(columns=[entity_col, "paper_count"])
        empty_by_year = pd.DataFrame(columns=["year", entity_col, "paper_count"])
        empty_pivot = pd.DataFrame()
        empty_active = pd.DataFrame(columns=[entity_col, "paper_count", "first_year", "last_year", "active_years"])
        return work, empty_total, empty_by_year, empty_pivot, empty_active

    total_df = work.groupby(entity_col).size().reset_index(name="paper_count").sort_values(
        ["paper_count", entity_col], ascending=[False, True]
    )
    dated = work[work["year"].notna()].copy()
    if dated.empty:
        by_year_df = pd.DataFrame(columns=["year", entity_col, "paper_count"])
        pivot_df = pd.DataFrame()
        active_df = total_df.copy()
        active_df["first_year"] = None
        active_df["last_year"] = None
        active_df["active_years"] = 0
        return work, total_df, by_year_df, pivot_df, active_df

    by_year_df = (
        dated.groupby(["year", entity_col]).size().reset_index(name="paper_count").sort_values(
            ["year", "paper_count", entity_col], ascending=[True, False, True]
        )
    )
    pivot_df = by_year_df.pivot(index="year", columns=entity_col, values="paper_count").fillna(0).astype(int).sort_index()
    active_df = (
        dated.groupby(entity_col)["year"]
        .agg(first_year="min", last_year="max", active_years="nunique")
        .reset_index()
        .merge(total_df, on=entity_col, how="left")
        .sort_values(["paper_count", "active_years", entity_col], ascending=[False, False, True])
    )
    active_df = active_df[[entity_col, "paper_count", "first_year", "last_year", "active_years"]]
    return work, total_df, by_year_df, pivot_df, active_df


def plot_top_entity_bar(plt, total_df, entity_col: str, output_path: Path, title: str, top_n: int, color: str) -> None:
    if total_df is None or total_df.empty:
        return
    top = total_df.head(top_n).sort_values("paper_count", ascending=True)
    if top.empty:
        return
    plt.figure(figsize=(12, max(6, 0.4 * len(top) + 2)))
    plt.barh(top[entity_col], top["paper_count"], color=color)
    plt.xlabel("Number of papers")
    plt.title(title)
    save_figure(plt, output_path)


def plot_entity_year_heatmap(plt, pivot_df, output_path: Path, title: str, top_n: int) -> None:
    if pivot_df is None or pivot_df.empty:
        return
    column_sums = pivot_df.sum(axis=0).sort_values(ascending=False)
    top_entities = column_sums.head(top_n).index.tolist()
    if not top_entities:
        return
    heat_df = pivot_df[top_entities].T
    plt.figure(figsize=(max(10, 0.35 * heat_df.shape[1] + 4), max(6, 0.45 * heat_df.shape[0] + 2)))
    im = plt.imshow(heat_df.to_numpy(), aspect="auto", cmap="YlGnBu", interpolation="nearest")
    plt.colorbar(im, label="Paper count")
    plt.yticks(range(len(heat_df.index)), heat_df.index.tolist())
    x_labels = [str(int(x)) for x in heat_df.columns]
    x_positions = list(range(len(x_labels)))
    if len(x_labels) > 20:
        step = max(1, len(x_labels) // 20)
        x_positions = x_positions[::step]
        x_labels = x_labels[::step]
    plt.xticks(x_positions, x_labels, rotation=45, ha="right")
    plt.xlabel("Year")
    plt.ylabel("Entity")
    plt.title(title)
    save_figure(plt, output_path)


def stringify_top_entities(count_df, entity_col: str, top_n: int) -> str:
    if count_df is None or count_df.empty:
        return ""
    items = []
    for _, row in count_df.head(top_n).iterrows():
        items.append(f"{row[entity_col]} ({int(row['paper_count'])})")
    return ", ".join(items)


def normalize_author_name(name: str) -> str:
    text = clean_text(name).strip(" ;,")
    if not text:
        return ""
    text = re.sub(r"\s*\.\s*", ".", text)
    text = re.sub(r"\.([A-Z][a-z])", r". \1", text)
    text = re.sub(r"\s+", " ", text)
    tokens = []
    for token in text.split(" "):
        if re.fullmatch(r"[A-Za-z]\.", token):
            tokens.append(token.upper())
        elif token.isupper() and len(token) <= 6:
            tokens.append(token)
        else:
            tokens.append(token.capitalize())
    return " ".join(tokens).strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Advanced bibliometric review analysis")
    parser.add_argument("--input", type=str, default="", help="Input bibliography file (.csv, .xlsx, or .xls).")
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR, help="Output directory")
    parser.add_argument("--random-state", type=int, default=DEFAULT_RANDOM_STATE, help="Random seed")
    parser.add_argument("--rolling-window", type=int, default=DEFAULT_ROLLING_WINDOW, help="Moving average window for yearly counts")
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N, help="Default top-n used in summary plots")
    parser.add_argument(
        "--min-author-occurrences",
        type=int,
        default=DEFAULT_MIN_AUTHOR_OCCURRENCES,
        help="Minimum papers per author in coauthor network",
    )
    parser.add_argument(
        "--max-authors-per-paper",
        type=int,
        default=DEFAULT_MAX_AUTHORS_PER_PAPER,
        help="Upper cap for coauthor edge creation",
    )
    parser.add_argument(
        "--max-paper-network-docs",
        type=int,
        default=DEFAULT_MAX_PAPER_NETWORK_DOCS,
        help="Max documents for similarity network",
    )
    parser.add_argument("--paper-sim-threshold", type=float, default=DEFAULT_PAPER_SIM_THRESHOLD, help="Similarity threshold for paper links")
    parser.add_argument("--neighbors", type=int, default=DEFAULT_NEIGHBORS, help="Nearest neighbors used for similarity network")
    parser.add_argument("--k-min", type=int, default=DEFAULT_K_MIN, help="Minimum number of clusters to test")
    parser.add_argument("--k-max", type=int, default=DEFAULT_K_MAX, help="Maximum number of clusters to test")
    parser.add_argument("--metadata-timeout", type=float, default=DEFAULT_METADATA_TIMEOUT, help="Timeout per external metadata request in seconds")
    parser.add_argument(
        "--metadata-request-pause",
        type=float,
        default=DEFAULT_METADATA_REQUEST_PAUSE,
        help="Delay between successful external metadata requests",
    )
    parser.add_argument("--metadata-cache", type=str, default="", help="Optional JSON cache file for external metadata")
    return parser.parse_args()


def prepare_dataframe(pd, df_raw):
    df = df_raw.copy()
    df.columns = [clean_text(c).lower() for c in df.columns]

    required = [
        "title",
        "authors",
        "journal",
        "published_date",
        "doi",
        "url",
        "issn",
        "abstract",
        "source",
        "relevance",
        "label",
    ]
    for col in required:
        if col not in df.columns:
            df[col] = ""

    text_columns = ["title", "authors", "journal", "doi", "url", "issn", "abstract", "source", "label", "published_date"]
    for col in text_columns:
        df[col] = df[col].map(clean_text)

    rows_initial = int(len(df))
    df = df[df["title"].str.len() > 0].copy()
    rows_after_title_filter = int(len(df))

    df["doi_norm"] = df["doi"].map(normalize_doi)
    df["title_key"] = df["title"].map(normalize_title_key)
    df["journal_clean"] = df["journal"].map(clean_text)
    df.loc[df["journal_clean"] == "", "journal_clean"] = "Unknown journal"

    df["published_dt"] = pd.to_datetime(df["published_date"], errors="coerce")
    fallback_year = df["published_date"].map(extract_year_from_text)
    df["year"] = df["published_dt"].dt.year.astype("Int64")
    df.loc[df["year"].isna(), "year"] = fallback_year[df["year"].isna()].astype("Int64")
    month_period = df["published_dt"].dt.to_period("M").astype(str)
    df["month"] = month_period.where(df["published_dt"].notna(), "")

    before_dedupe = len(df)
    with_doi = df[df["doi_norm"] != ""].drop_duplicates(subset="doi_norm", keep="first")
    without_doi = df[df["doi_norm"] == ""].drop_duplicates(subset=["title_key", "year"], keep="first")
    df = pd.concat([with_doi, without_doi], ignore_index=True)
    duplicates_removed = int(before_dedupe - len(df))

    df["author_list"] = df["authors"].map(split_authors)
    df["n_authors"] = df["author_list"].map(len).astype(int)
    df["has_abstract"] = (df["abstract"].str.len() > 0).astype(int)
    df["relevance"] = pd.to_numeric(df["relevance"], errors="coerce")

    df["text_for_nlp"] = (df["title"] + " " + df["abstract"]).map(normalize_text_for_nlp)
    empty_text_mask = df["text_for_nlp"].str.len() == 0
    df.loc[empty_text_mask, "text_for_nlp"] = df.loc[empty_text_mask, "title"].map(normalize_text_for_nlp)
    df["text_for_rules"] = (df["title"] + " " + df["abstract"]).str.lower().map(clean_text)

    df = df.reset_index(drop=True)
    df["paper_id"] = df.index.astype(int)
    df["paper_uid"] = df["paper_id"].map(lambda x: f"P{x:04d}")

    df["institution_list"] = [[] for _ in range(len(df))]
    df["team_lab_list"] = [[] for _ in range(len(df))]
    df["team_city_list"] = [[] for _ in range(len(df))]
    df["reference_details"] = [[] for _ in range(len(df))]
    df["referenced_openalex_ids"] = [[] for _ in range(len(df))]
    df["openalex_id"] = ""
    df["metadata_status"] = "not_requested"
    df["metadata_sources"] = ""
    df["reference_count"] = 0
    df["crossref_reference_count"] = 0
    df["openalex_reference_count"] = 0
    df["references_in_corpus_count"] = 0
    df["references_in_corpus_match_count"] = 0
    df["references_in_corpus_paper_ids"] = ""
    df["references_in_corpus_titles"] = ""
    df["references_in_corpus_match_sources"] = ""
    df["cited_by_corpus_count"] = 0
    df["cited_by_corpus_paper_ids"] = ""
    df["cited_by_corpus_titles"] = ""

    quality = {
        "rows_initial": rows_initial,
        "rows_after_title_filter": rows_after_title_filter,
        "rows_after_deduplication": int(len(df)),
        "duplicates_removed_by_doi_or_title_year": duplicates_removed,
    }
    return df, quality


def plot_publication_timeline(df, pd, plt, figures_dir: Path, tables_dir: Path, rolling_window: int) -> None:
    dated = df[df["year"].notna()].copy()
    if dated.empty:
        log("No valid publication dates found. Timeline plots skipped.")
        write_empty_csv(pd, tables_dir / "yearly_publication_counts.csv", ["year", "paper_count", "moving_average"])
        return

    years = dated["year"].dropna().astype(int)
    full_index = pd.Index(range(int(years.min()), int(years.max()) + 1), name="year")
    yearly = dated.groupby("year").size().reindex(full_index, fill_value=0).astype(int)
    rolling = yearly.rolling(window=max(1, rolling_window), min_periods=1).mean()

    yearly_df = pd.DataFrame(
        {
            "year": yearly.index.astype(int),
            "paper_count": yearly.values.astype(int),
            "moving_average": rolling.values.astype(float),
            "cumulative_paper_count": yearly.cumsum().values.astype(int),
        }
    )
    yearly_df.to_csv(tables_dir / "yearly_publication_counts.csv", index=False)

    plt.figure(figsize=(12, 6))
    plt.bar(yearly.index.astype(int), yearly.values, color="#4E79A7", alpha=0.75, label="Papers / year")
    plt.plot(
        yearly.index.astype(int),
        rolling.values,
        color="#F28E2B",
        linewidth=2.5,
        label=f"{rolling_window}y moving average",
    )
    plt.xlabel("Year")
    plt.ylabel("Number of papers")
    plt.title("Publication dynamics over time")
    plt.legend()
    save_figure(plt, figures_dir / "01_publications_per_year.png")

    growth = yearly.pct_change() * 100.0
    pd.DataFrame({"year": growth.index.astype(int), "growth_pct_vs_prev_year": growth.values}).to_csv(
        tables_dir / "yearly_growth_rate.csv", index=False
    )
    if EXPORT_SUPPLEMENTARY_FIGURES:
        plt.figure(figsize=(12, 6))
        plt.plot(yearly.index.astype(int), yearly.cumsum().values, color="#59A14F", linewidth=2.5)
        plt.xlabel("Year")
        plt.ylabel("Cumulative papers")
        plt.title("Cumulative publication growth")
        save_figure(plt, figures_dir / "02_cumulative_publications.png")

        plt.figure(figsize=(12, 6))
        plt.axhline(0, color="#666666", linewidth=1)
        plt.plot(growth.index.astype(int), growth.values, color="#E15759", marker="o", linewidth=1.6)
        plt.xlabel("Year")
        plt.ylabel("Growth vs previous year (%)")
        plt.title("Year-over-year publication growth")
        save_figure(plt, figures_dir / "03_yearly_growth_rate.png")

    abstract_rate = dated.groupby("year")["has_abstract"].mean().reindex(full_index, fill_value=0.0) * 100.0
    pd.DataFrame({"year": abstract_rate.index.astype(int), "abstract_coverage_pct": abstract_rate.values}).to_csv(
        tables_dir / "abstract_coverage_by_year.csv", index=False
    )

    plt.figure(figsize=(12, 6))
    plt.plot(abstract_rate.index.astype(int), abstract_rate.values, color="#76B7B2", marker="o", linewidth=2)
    plt.ylim(0, 100)
    plt.xlabel("Year")
    plt.ylabel("Papers with abstract (%)")
    plt.title("Abstract coverage by year")
    save_figure(plt, figures_dir / "04_abstract_coverage_by_year.png")


def plot_journal_statistics(df, pd, np, plt, figures_dir: Path, tables_dir: Path, top_n: int):
    _, total_df, by_year_df, pivot_df, active_df = build_scalar_entity_tables(
        df=df,
        pd=pd,
        source_col="journal_clean",
        entity_col="journal",
        unknown_label="Unknown journal",
    )
    total_df.to_csv(tables_dir / "journal_total_counts.csv", index=False)
    total_df.to_csv(tables_dir / "top_journals.csv", index=False)
    by_year_df.to_csv(tables_dir / "journal_by_year_counts.csv", index=False)
    pivot_df.to_csv(tables_dir / "journal_by_year_pivot.csv")
    active_df.to_csv(tables_dir / "journal_active_years.csv", index=False)

    plot_top_entity_bar(
        plt,
        total_df,
        entity_col="journal",
        output_path=figures_dir / "05_top_journals.png",
        title=f"Top {top_n} journals by paper count",
        top_n=top_n,
        color="#4E79A7",
    )
    plot_entity_year_heatmap(
        plt,
        pivot_df,
        output_path=figures_dir / "06_journal_heatmap_by_year.png",
        title=f"Top {min(top_n, 15)} journals over time",
        top_n=min(top_n, 15),
    )

    if not total_df.empty:
        rank = np.arange(1, len(total_df) + 1)
        share = total_df["paper_count"].to_numpy(dtype=float) / max(float(total_df["paper_count"].sum()), 1.0)
        cumulative_share = share.cumsum() * 100.0
        pd.DataFrame(
            {
                "journal_rank": rank,
                "paper_share_pct": share * 100.0,
                "cumulative_share_pct": cumulative_share,
            }
        ).to_csv(tables_dir / "journal_concentration_curve.csv", index=False)

        if EXPORT_SUPPLEMENTARY_FIGURES:
            plt.figure(figsize=(10, 6))
            plt.plot(rank, cumulative_share, color="#F28E2B", linewidth=2.2)
            plt.axhline(50, color="#999999", linewidth=1, linestyle="--")
            plt.axhline(80, color="#999999", linewidth=1, linestyle="--")
            plt.xlabel("Journal rank (most frequent to least frequent)")
            plt.ylabel("Cumulative paper share (%)")
            plt.title("Journal concentration (rank-frequency cumulative curve)")
            save_figure(plt, figures_dir / "07_journal_concentration_curve.png")

    dated = df[df["year"].notna()].copy()
    if dated.empty:
        write_empty_csv(pd, tables_dir / "journal_diversity_by_year.csv", ["year", "shannon_diversity"])
        return total_df, by_year_df, pivot_df

    def shannon_index(values) -> float:
        counts = values.value_counts().astype(float)
        probs = counts / max(counts.sum(), 1.0)
        return float(-(probs * np.log2(probs + 1e-12)).sum())

    diversity = dated.groupby("year")["journal_clean"].apply(shannon_index)
    diversity_df = diversity.rename("shannon_diversity").reset_index()
    diversity_df.to_csv(tables_dir / "journal_diversity_by_year.csv", index=False)

    if EXPORT_SUPPLEMENTARY_FIGURES:
        plt.figure(figsize=(10, 6))
        plt.plot(diversity.index.astype(int), diversity.values, color="#59A14F", marker="o", linewidth=2)
        plt.xlabel("Year")
        plt.ylabel("Shannon diversity index")
        plt.title("Journal diversity over time")
        save_figure(plt, figures_dir / "08_journal_diversity_over_time.png")
    return total_df, by_year_df, pivot_df


def plot_author_statistics(df, pd, plt, figures_dir: Path, tables_dir: Path, top_n: int):
    exploded, author_counts = compute_author_counts(df, pd)
    total_df = author_counts.rename_axis("author").reset_index(name="paper_count")
    total_df.to_csv(tables_dir / "author_total_counts.csv", index=False)
    total_df.to_csv(tables_dir / "top_authors.csv", index=False)

    plot_top_entity_bar(
        plt,
        total_df,
        entity_col="author",
        output_path=figures_dir / "09_top_authors.png",
        title=f"Top {top_n} authors by paper count",
        top_n=top_n,
        color="#E15759",
    )

    if exploded.empty:
        write_empty_csv(pd, tables_dir / "author_by_year_counts.csv", ["year", "author", "paper_count"])
        write_empty_csv(pd, tables_dir / "author_active_years.csv", ["author", "paper_count", "first_year", "last_year", "active_years"])
    else:
        by_year_df = (
            exploded[exploded["year"].notna()]
            .groupby(["year", "author"])
            .size()
            .reset_index(name="paper_count")
            .sort_values(["year", "paper_count", "author"], ascending=[True, False, True])
        )
        by_year_df.to_csv(tables_dir / "author_by_year_counts.csv", index=False)
        author_active_df = (
            exploded[exploded["year"].notna()]
            .groupby("author")["year"]
            .agg(first_year="min", last_year="max", active_years="nunique")
            .reset_index()
            .merge(total_df, on="author", how="left")
            .sort_values(["paper_count", "active_years", "author"], ascending=[False, False, True])
        )
        author_active_df = author_active_df[["author", "paper_count", "first_year", "last_year", "active_years"]]
        author_active_df.to_csv(tables_dir / "author_active_years.csv", index=False)
        pivot_df = by_year_df.pivot(index="year", columns="author", values="paper_count").fillna(0).astype(int).sort_index()
        pivot_df.to_csv(tables_dir / "author_by_year_pivot.csv")
        plot_entity_year_heatmap(
            plt,
            pivot_df,
            output_path=figures_dir / "10_author_heatmap_by_year.png",
            title=f"Top {min(top_n, 15)} authors over time",
            top_n=min(top_n, 15),
        )

    plt.figure(figsize=(10, 6))
    bins_max = int(min(max(int(df["n_authors"].max()), 6), 30)) if len(df) else 6
    plt.hist(df["n_authors"], bins=bins_max, color="#76B7B2", edgecolor="white")
    plt.xlabel("Number of authors per paper")
    plt.ylabel("Paper count")
    plt.title("Authorship size distribution")
    save_figure(plt, figures_dir / "11_authors_per_paper_histogram.png")

    dated = df[df["year"].notna()].copy()
    if dated.empty:
        write_empty_csv(pd, tables_dir / "collaboration_index_by_year.csv", ["year", "mean", "median"])
    else:
        collab = dated.groupby("year")["n_authors"].agg(["mean", "median"]).reset_index()
        collab.to_csv(tables_dir / "collaboration_index_by_year.csv", index=False)

        if EXPORT_SUPPLEMENTARY_FIGURES:
            plt.figure(figsize=(11, 6))
            plt.plot(collab["year"].astype(int), collab["mean"], label="Mean", color="#4E79A7", linewidth=2)
            plt.plot(collab["year"].astype(int), collab["median"], label="Median", color="#F28E2B", linewidth=2)
            plt.xlabel("Year")
            plt.ylabel("Authors per paper")
            plt.title("Collaboration intensity over time")
            plt.legend()
            save_figure(plt, figures_dir / "12_collaboration_over_time.png")
    return exploded, author_counts


def compute_keyword_statistics(df, pd, np, CountVectorizer, ENGLISH_STOP_WORDS, plt, figures_dir: Path, tables_dir: Path):
    texts = df["text_for_nlp"].fillna("").astype(str).tolist()
    if sum(1 for text in texts if text.strip()) == 0:
        log("No textual content found for keyword analysis.")
        write_empty_csv(pd, tables_dir / "keyword_total_counts.csv", ["term", "frequency", "document_frequency"])
        write_empty_csv(pd, tables_dir / "keyword_by_year_counts.csv", ["term", "year", "term_count"])
        write_empty_csv(pd, tables_dir / "keyword_by_year_normalized.csv", ["term", "year", "count_per_100_papers"])
        write_empty_csv(pd, tables_dir / "top_keywords_by_year.csv", ["year", "term", "term_count", "rank_in_year"])
        return None, None, None, None, None

    stop_words = sorted(set(ENGLISH_STOP_WORDS).union(EXTRA_STOPWORDS))
    min_df = 2 if len(df) >= 40 else 1
    vectorizer = CountVectorizer(
        stop_words=stop_words,
        min_df=min_df,
        max_df=0.85,
        max_features=12000,
        ngram_range=(1, 2),
        token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z\-]{2,}\b",
    )
    try:
        matrix = vectorizer.fit_transform(texts)
    except ValueError:
        log("Keyword vectorizer produced an empty vocabulary.")
        write_empty_csv(pd, tables_dir / "keyword_total_counts.csv", ["term", "frequency", "document_frequency"])
        write_empty_csv(pd, tables_dir / "keyword_by_year_counts.csv", ["term", "year", "term_count"])
        write_empty_csv(pd, tables_dir / "keyword_by_year_normalized.csv", ["term", "year", "count_per_100_papers"])
        write_empty_csv(pd, tables_dir / "top_keywords_by_year.csv", ["year", "term", "term_count", "rank_in_year"])
        return None, None, None, None, None

    terms = np.array(vectorizer.get_feature_names_out())
    frequencies = np.asarray(matrix.sum(axis=0)).ravel()
    document_frequencies = np.asarray((matrix > 0).sum(axis=0)).ravel()

    keyword_total_df = (
        pd.DataFrame({"term": terms, "frequency": frequencies, "document_frequency": document_frequencies})
        .sort_values(["frequency", "document_frequency", "term"], ascending=[False, False, True])
        .reset_index(drop=True)
    )
    keyword_total_df.to_csv(tables_dir / "keyword_total_counts.csv", index=False)
    keyword_total_df.to_csv(tables_dir / "top_terms.csv", index=False)

    top30 = keyword_total_df.head(30).iloc[::-1]
    plt.figure(figsize=(12, 10))
    plt.barh(top30["term"], top30["frequency"], color="#59A14F")
    plt.xlabel("Frequency")
    plt.title("Top keywords in titles + abstracts")
    save_figure(plt, figures_dir / "13_top_keywords.png")

    dated = df[df["year"].notna()].copy()
    if dated.empty:
        write_empty_csv(pd, tables_dir / "keyword_by_year_counts.csv", ["term", "year", "term_count"])
        write_empty_csv(pd, tables_dir / "keyword_by_year_normalized.csv", ["term", "year", "count_per_100_papers"])
        write_empty_csv(pd, tables_dir / "top_keywords_by_year.csv", ["year", "term", "term_count", "rank_in_year"])
        return vectorizer, matrix, terms, keyword_total_df, None

    years = sorted(dated["year"].dropna().astype(int).unique())
    yearly_counts_records = []
    yearly_norm_records = []
    top_terms_for_trends = keyword_total_df.head(20)["term"].tolist()
    term_to_index = {term: idx for idx, term in enumerate(terms)}
    chosen_indices = [term_to_index[term] for term in top_terms_for_trends if term in term_to_index]
    heat = np.zeros((len(chosen_indices), len(years)), dtype=float)

    for year_pos, year in enumerate(years):
        year_mask = df["year"] == year
        year_count = int(year_mask.sum())
        if year_count == 0:
            continue
        year_matrix = matrix[year_mask.to_numpy(dtype=bool, na_value=False)]
        year_counts = np.asarray(year_matrix.sum(axis=0)).ravel()
        non_zero_indices = np.where(year_counts > 0)[0]
        for term_idx in non_zero_indices:
            yearly_counts_records.append({"term": terms[term_idx], "year": int(year), "term_count": int(year_counts[term_idx])})
            yearly_norm_records.append(
                {
                    "term": terms[term_idx],
                    "year": int(year),
                    "count_per_100_papers": float(year_counts[term_idx] / max(year_count, 1) * 100.0),
                }
            )
        for term_pos, term_idx in enumerate(chosen_indices):
            heat[term_pos, year_pos] = float(year_counts[term_idx] / max(year_count, 1) * 100.0)

    keyword_by_year_df = pd.DataFrame(yearly_counts_records).sort_values(["year", "term_count", "term"], ascending=[True, False, True])
    keyword_by_year_df.to_csv(tables_dir / "keyword_by_year_counts.csv", index=False)
    keyword_by_year_df.pivot(index="year", columns="term", values="term_count").fillna(0).astype(int).to_csv(
        tables_dir / "keyword_by_year_pivot.csv"
    )

    keyword_by_year_norm_df = pd.DataFrame(yearly_norm_records).sort_values(
        ["year", "count_per_100_papers", "term"], ascending=[True, False, True]
    )
    keyword_by_year_norm_df.to_csv(tables_dir / "keyword_by_year_normalized.csv", index=False)

    top_keywords_by_year = keyword_by_year_df.groupby("year", group_keys=False).head(15).copy().reset_index(drop=True)
    top_keywords_by_year["rank_in_year"] = top_keywords_by_year.groupby("year").cumcount() + 1
    top_keywords_by_year.to_csv(tables_dir / "top_keywords_by_year.csv", index=False)

    if len(chosen_indices) > 0:
        plt.figure(figsize=(14, 8))
        im = plt.imshow(heat, aspect="auto", cmap="YlGnBu", interpolation="nearest")
        plt.colorbar(im, label="Occurrences per 100 papers")
        plt.yticks(range(len(chosen_indices)), [terms[idx] for idx in chosen_indices])
        x_positions = list(range(len(years)))
        x_labels = [str(year) for year in years]
        if len(x_labels) > 20:
            step = max(1, len(x_labels) // 20)
            x_positions = x_positions[::step]
            x_labels = x_labels[::step]
        plt.xticks(x_positions, x_labels, rotation=45, ha="right")
        plt.xlabel("Year")
        plt.ylabel("Keyword")
        plt.title("Top keyword trends over time (normalized)")
        save_figure(plt, figures_dir / "14_keyword_trend_heatmap.png")

    return vectorizer, matrix, terms, keyword_total_df, keyword_by_year_df


def compute_group_keyword_profiles(df, pd, np, group_col: str, keyword_matrix, keyword_terms, top_n: int):
    if keyword_matrix is None or keyword_terms is None or len(df) == 0:
        return pd.DataFrame(columns=[group_col, "term", "frequency", "rank"])
    rows = []
    for group_value, group in df.groupby(group_col):
        idx = list(group.index)
        if not idx:
            continue
        counts = np.asarray(keyword_matrix[idx].sum(axis=0)).ravel()
        if counts.size == 0:
            continue
        top_indices = np.argsort(counts)[::-1]
        rank = 1
        for term_idx in top_indices:
            frequency = int(counts[term_idx])
            if frequency <= 0:
                break
            rows.append({group_col: group_value, "term": keyword_terms[term_idx], "frequency": frequency, "rank": rank})
            rank += 1
            if rank > top_n:
                break
    if not rows:
        return pd.DataFrame(columns=[group_col, "term", "frequency", "rank"])
    return pd.DataFrame(rows).sort_values([group_col, "rank"])


def score_rule_patterns(text: object, patterns: Sequence[Tuple[str, int]]) -> Tuple[int, List[str]]:
    text_clean = clean_text(text).lower()
    if not text_clean:
        return 0, []
    score = 0
    matches = []
    for pattern, weight in patterns:
        if re.search(pattern, text_clean, flags=re.IGNORECASE):
            score += int(weight)
            matches.append(pattern)
    return score, matches


def has_strong_rule_match(text: object, patterns: Sequence[str]) -> bool:
    text_clean = clean_text(text).lower()
    if not text_clean:
        return False
    return any(re.search(pattern, text_clean, flags=re.IGNORECASE) for pattern in patterns)


def classify_papers(df, pd, plt, figures_dir: Path, tables_dir: Path):
    rows = []
    primary_labels = []
    primary_display_labels = []
    for row in df.itertuples(index=False):
        title_text = clean_text(getattr(row, "title", ""))
        abstract_text = clean_text(getattr(row, "abstract", ""))
        journal_text = clean_text(getattr(row, "journal_clean", getattr(row, "journal", "")))
        scores: Dict[str, int] = {}
        matched_patterns: Dict[str, Dict[str, Any]] = {}
        strong_title_hits: Dict[str, bool] = {}
        for class_name, definition in CLASS_DEFINITIONS.items():
            title_score, title_matches = score_rule_patterns(title_text, definition.get("title_patterns", []))
            abstract_score, abstract_matches = score_rule_patterns(abstract_text, definition.get("abstract_patterns", []))
            journal_score, journal_matches = score_rule_patterns(journal_text, definition.get("journal_patterns", []))
            total_score = int(title_score + abstract_score + journal_score)
            scores[class_name] = total_score
            strong_title_hits[class_name] = has_strong_rule_match(title_text, definition.get("strong_title_patterns", []))
            matched_patterns[class_name] = {
                "title": title_matches,
                "abstract": abstract_matches,
                "journal": journal_matches,
                "base_score": total_score,
            }

        if strong_title_hits["methode_rejection_artefact"]:
            scores["methode_rejection_artefact"] += 4

        if strong_title_hits["methode_detection_microemboles"]:
            scores["methode_detection_microemboles"] += 4

        if strong_title_hits["instrumentation_doppler"]:
            scores["instrumentation_doppler"] += 3

        method_pressure = max(
            scores["methode_detection_microemboles"],
            scores["methode_rejection_artefact"],
            scores["instrumentation_doppler"],
        )

        if strong_title_hits["methode_detection_microemboles"] or strong_title_hits["methode_rejection_artefact"]:
            scores["etude_clinique"] = max(0, scores["etude_clinique"] - 5)
        elif strong_title_hits["instrumentation_doppler"]:
            scores["etude_clinique"] = max(0, scores["etude_clinique"] - 3)

        if method_pressure >= 8 and scores["etude_clinique"] < method_pressure + 4:
            scores["etude_clinique"] = max(0, scores["etude_clinique"] - 4)

        if strong_title_hits["etude_clinique"] and method_pressure < 10:
            scores["etude_clinique"] += 2

        if strong_title_hits["methode_detection_microemboles"] and scores["methode_detection_microemboles"] >= 10:
            scores["instrumentation_doppler"] = max(0, scores["instrumentation_doppler"] - 2)

        if strong_title_hits["methode_rejection_artefact"] and scores["methode_rejection_artefact"] >= scores["methode_detection_microemboles"] - 2:
            scores["methode_rejection_artefact"] += 2

        for class_name in CLASS_DEFINITIONS:
            matched_patterns[class_name]["adjusted_score"] = int(scores[class_name])
            matched_patterns[class_name]["strong_title_hit"] = bool(strong_title_hits[class_name])

        positive = [(class_name, score) for class_name, score in scores.items() if score > 0]
        if positive:
            primary_class = sorted(
                positive,
                key=lambda item: (-item[1], CLASS_DEFINITIONS[item[0]]["tie_break_rank"], item[0]),
            )[0][0]
            primary_display = CLASS_DEFINITIONS[primary_class]["display_name"]
        else:
            primary_class = "autre_non_classe"
            primary_display = UNCLASSIFIED_LABEL

        primary_labels.append(primary_class)
        primary_display_labels.append(primary_display)
        rows.append(
            {
                "paper_id": int(row.paper_id),
                "paper_uid": row.paper_uid,
                "year": int(row.year) if str(row.year) not in {"", "<NA>", "nan"} else None,
                "title": row.title,
                "primary_class": primary_class,
                "primary_class_display": primary_display,
                "score_etude_clinique": scores["etude_clinique"],
                "score_methode_detection_microemboles": scores["methode_detection_microemboles"],
                "score_methode_rejection_artefact": scores["methode_rejection_artefact"],
                "score_instrumentation_doppler": scores["instrumentation_doppler"],
                "matched_patterns": json.dumps(matched_patterns, ensure_ascii=True),
            }
        )

    classification_df = pd.DataFrame(rows)
    df["primary_class"] = primary_labels
    df["primary_class_display"] = primary_display_labels
    classification_df.to_csv(tables_dir / "paper_classification_scores.csv", index=False)

    class_total_df = (
        df["primary_class_display"]
        .value_counts()
        .rename_axis("paper_class")
        .reset_index(name="paper_count")
        .sort_values(["paper_count", "paper_class"], ascending=[False, True])
    )
    class_total_df.to_csv(tables_dir / "classification_total_counts.csv", index=False)

    dated = df[df["year"].notna()].copy()
    if dated.empty:
        class_by_year_df = pd.DataFrame(columns=["year", "paper_class", "paper_count"])
        class_pivot_df = pd.DataFrame()
    else:
        class_by_year_df = (
            dated.groupby(["year", "primary_class_display"])
            .size()
            .reset_index(name="paper_count")
            .rename(columns={"primary_class_display": "paper_class"})
            .sort_values(["year", "paper_count", "paper_class"], ascending=[True, False, True])
        )
        class_pivot_df = class_by_year_df.pivot(index="year", columns="paper_class", values="paper_count").fillna(0).astype(int)
    class_by_year_df.to_csv(tables_dir / "classification_by_year_counts.csv", index=False)
    class_pivot_df.to_csv(tables_dir / "classification_by_year_pivot.csv")

    class_author_exploded = (
        df[["paper_id", "primary_class_display", "author_list"]]
        .explode("author_list")
        .rename(columns={"primary_class_display": "paper_class", "author_list": "author"})
    )
    class_author_exploded["author"] = class_author_exploded["author"].fillna("").map(clean_text)
    class_author_exploded = class_author_exploded[class_author_exploded["author"] != ""].drop_duplicates(subset=["paper_id", "paper_class", "author"])
    class_author_counts_df = (
        class_author_exploded.groupby(["paper_class", "author"]).size().reset_index(name="paper_count").sort_values(
            ["paper_class", "paper_count", "author"], ascending=[True, False, True]
        )
    )
    class_author_counts_df.to_csv(tables_dir / "classification_author_counts.csv", index=False)

    class_journal_counts_df = (
        df.groupby(["primary_class_display", "journal_clean"]).size().reset_index(name="paper_count").rename(
            columns={"primary_class_display": "paper_class", "journal_clean": "journal"}
        )
    )
    class_journal_counts_df = class_journal_counts_df.sort_values(["paper_class", "paper_count", "journal"], ascending=[True, False, True])
    class_journal_counts_df.to_csv(tables_dir / "classification_journal_counts.csv", index=False)

    plt.figure(figsize=(10, 6))
    plot_df = class_total_df.sort_values("paper_count", ascending=True)
    plt.barh(plot_df["paper_class"], plot_df["paper_count"], color="#AF7AA1")
    plt.xlabel("Number of papers")
    plt.title("Paper classification totals")
    save_figure(plt, figures_dir / "15_classification_totals.png")

    if class_pivot_df is not None and not class_pivot_df.empty:
        plt.figure(figsize=(12, 7))
        years = class_pivot_df.index.astype(int).to_numpy()
        plt.stackplot(
            years,
            [class_pivot_df[col].to_numpy() for col in class_pivot_df.columns],
            labels=class_pivot_df.columns.tolist(),
            alpha=0.9,
        )
        plt.xlabel("Year")
        plt.ylabel("Paper count")
        plt.title("Paper classification over time")
        plt.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0))
        save_figure(plt, figures_dir / "16_classification_evolution.png")

    return df, classification_df, class_total_df, class_by_year_df, class_author_counts_df, class_journal_counts_df


def build_group_profiles(df, pd, np, group_col: str, keyword_matrix, keyword_terms, top_n: int):
    total_df = (
        df[group_col]
        .value_counts(dropna=False)
        .rename_axis(group_col)
        .reset_index(name="paper_count")
        .sort_values(["paper_count", group_col], ascending=[False, True])
    )
    dated = df[df["year"].notna()].copy()
    if dated.empty:
        by_year_df = pd.DataFrame(columns=["year", group_col, "paper_count"])
        pivot_df = pd.DataFrame()
    else:
        by_year_df = (
            dated.groupby(["year", group_col]).size().reset_index(name="paper_count").sort_values(
                ["year", "paper_count", group_col], ascending=[True, False, True]
            )
        )
        pivot_df = by_year_df.pivot(index="year", columns=group_col, values="paper_count").fillna(0).astype(int)

    journal_counts_df = (
        df.groupby([group_col, "journal_clean"]).size().reset_index(name="paper_count").rename(columns={"journal_clean": "journal"})
    )
    journal_counts_df = journal_counts_df.sort_values([group_col, "paper_count", "journal"], ascending=[True, False, True])

    author_exploded = df[["paper_id", group_col, "author_list"]].explode("author_list").rename(columns={"author_list": "author"})
    author_exploded["author"] = author_exploded["author"].fillna("").map(clean_text)
    author_exploded = author_exploded[author_exploded["author"] != ""].drop_duplicates(subset=["paper_id", group_col, "author"])
    author_counts_df = (
        author_exploded.groupby([group_col, "author"]).size().reset_index(name="paper_count").sort_values(
            [group_col, "paper_count", "author"], ascending=[True, False, True]
        )
    )

    keyword_profiles_df = compute_group_keyword_profiles(df, pd, np, group_col, keyword_matrix, keyword_terms, top_n=max(12, top_n))
    summary_rows = []
    for _, row in total_df.iterrows():
        group_value = row[group_col]
        journal_summary = stringify_top_entities(journal_counts_df[journal_counts_df[group_col] == group_value], "journal", 5)
        author_summary = stringify_top_entities(author_counts_df[author_counts_df[group_col] == group_value], "author", 6)
        keyword_summary = stringify_top_entities(
            keyword_profiles_df[keyword_profiles_df[group_col] == group_value].rename(columns={"frequency": "paper_count"}),
            "term",
            8,
        )
        sub = df[df[group_col] == group_value]
        years = sub["year"].dropna().astype(int) if "year" in sub.columns else []
        summary_rows.append(
            {
                group_col: group_value,
                "paper_count": int(row["paper_count"]),
                "first_year": int(years.min()) if len(years) else None,
                "last_year": int(years.max()) if len(years) else None,
                "top_authors": author_summary,
                "top_journals": journal_summary,
                "top_keywords": keyword_summary,
            }
        )
    return total_df, by_year_df, pivot_df, journal_counts_df, author_counts_df, keyword_profiles_df, pd.DataFrame(summary_rows)


def humanize_term(term: str) -> str:
    acronyms = {"tcd", "mes", "mca", "roc", "svm", "hits", "ai"}
    words = []
    for token in clean_text(term).split():
        if token.lower() in acronyms:
            words.append(token.upper())
        else:
            words.append(token.capitalize())
    return " ".join(words)


def safe_slug(text: str) -> str:
    slug = normalize_title_key(text).replace(" ", "_")
    return slug[:80] if slug else "cluster"


def build_cluster_labels(pd, cluster_summary_df, cluster_keyword_profiles_df, cluster_terms_df):
    rows = []
    used_labels = set()
    for _, row in cluster_summary_df.iterrows():
        cluster_id = int(row["cluster"])
        candidates = []
        if cluster_terms_df is not None and not cluster_terms_df.empty:
            match = cluster_terms_df[cluster_terms_df["cluster"] == cluster_id]
            if not match.empty:
                candidates.extend([clean_text(term) for term in clean_text(match.iloc[0].get("top_terms", "")).split(",")])
        if cluster_keyword_profiles_df is not None and not cluster_keyword_profiles_df.empty:
            subset = cluster_keyword_profiles_df[cluster_keyword_profiles_df["cluster"] == cluster_id].sort_values("rank")
            candidates.extend(subset["term"].tolist())

        ordered = []
        seen = set()
        for term in candidates:
            term_clean = clean_text(term)
            if not term_clean:
                continue
            key = term_clean.lower()
            if key in CLUSTER_LABEL_STOPWORDS:
                continue
            if key in seen:
                continue
            seen.add(key)
            ordered.append(term_clean)

        selected = []
        for term in ordered:
            if " " in term:
                selected.append(term)
            if len(selected) >= 2:
                break
        for term in ordered:
            term_tokens = set(term.split())
            if any(term_tokens.issubset(set(existing.split())) for existing in selected):
                continue
            if term not in selected:
                selected.append(term)
            if len(selected) >= 3:
                break
        selected = selected[:3]
        cluster_label = " / ".join(humanize_term(term) for term in selected) if selected else f"Theme {cluster_id}"
        if cluster_label in used_labels:
            suffix = 2
            unique_label = f"{cluster_label} [{suffix}]"
            while unique_label in used_labels:
                suffix += 1
                unique_label = f"{cluster_label} [{suffix}]"
            cluster_label = unique_label
        used_labels.add(cluster_label)
        rows.append(
            {
                "cluster": cluster_id,
                "cluster_label": cluster_label,
                "cluster_label_terms": "; ".join(selected),
            }
        )
    return pd.DataFrame(rows)


def plot_cluster_keyword_clouds(plt, np, cluster_keyword_profiles_df, cluster_labels_df, figures_dir: Path) -> None:
    cloud_dir = figures_dir / "cluster_keyword_clouds"
    cloud_dir.mkdir(parents=True, exist_ok=True)
    if cluster_labels_df is None or cluster_labels_df.empty:
        return

    palette = ["#4E79A7", "#F28E2B", "#59A14F", "#E15759", "#76B7B2", "#EDC948", "#B07AA1", "#9C755F"]
    base_positions = [(x, y) for y in [0.82, 0.64, 0.46, 0.28] for x in [0.16, 0.36, 0.56, 0.76]]

    for _, cluster_row in cluster_labels_df.iterrows():
        cluster_id = int(cluster_row["cluster"])
        cluster_label = cluster_row["cluster_label"]
        subset = cluster_keyword_profiles_df[cluster_keyword_profiles_df["cluster"] == cluster_id].sort_values("rank").head(16).copy()
        if subset.empty:
            continue

        rng = np.random.RandomState(42 + cluster_id)
        positions = base_positions.copy()
        rng.shuffle(positions)
        weights = subset["frequency"].astype(float).to_numpy()
        min_w = float(weights.min())
        max_w = float(weights.max())

        plt.figure(figsize=(10, 6))
        ax = plt.gca()
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")

        for idx, (_, keyword_row) in enumerate(subset.iterrows()):
            x, y = positions[idx]
            if max_w > min_w:
                size = 14 + 20 * ((float(keyword_row["frequency"]) - min_w) / (max_w - min_w))
            else:
                size = 22
            ax.text(
                x,
                y,
                humanize_term(keyword_row["term"]),
                fontsize=size,
                color=palette[idx % len(palette)],
                ha="center",
                va="center",
                rotation=0 if idx < 6 else (-18 if idx % 2 else 18),
                alpha=0.90,
                transform=ax.transAxes,
            )

        plt.title(cluster_label)
        save_figure(plt, cloud_dir / f"{cluster_id:02d}_{safe_slug(cluster_label)}.png")


def plot_cluster_summary_figures(plt, cluster_total_df, cluster_pivot_df, figures_dir: Path) -> None:
    if cluster_total_df is not None and not cluster_total_df.empty:
        plot_df = cluster_total_df.copy()
        plot_df["cluster_name"] = plot_df["cluster_label"].fillna(plot_df["cluster"].map(lambda value: f"Theme {value}"))
        plot_df = plot_df.sort_values(["paper_count", "cluster_name"], ascending=[True, True])
        plt.figure(figsize=(12, max(6, 0.5 * len(plot_df) + 2)))
        plt.barh(plot_df["cluster_name"], plot_df["paper_count"], color="#4E79A7")
        plt.xlabel("Number of papers")
        plt.title("Thematic cluster distribution")
        save_figure(plt, figures_dir / "13_cluster_sizes.png")

    if cluster_pivot_df is None or cluster_pivot_df.empty:
        return

    labeled_pivot = cluster_pivot_df.copy()
    labeled_pivot.columns = [clean_text(column) for column in labeled_pivot.columns]

    shares = labeled_pivot.div(labeled_pivot.sum(axis=1), axis=0).fillna(0.0)
    if not shares.empty:
        plt.figure(figsize=(13, 8))
        years = shares.index.astype(int).to_numpy()
        plt.stackplot(
            years,
            [shares[column].to_numpy() for column in shares.columns],
            labels=[str(column) for column in shares.columns],
            alpha=0.9,
        )
        plt.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0))
        plt.xlabel("Year")
        plt.ylabel("Share of yearly publications")
        plt.title("Cluster evolution over time")
        save_figure(plt, figures_dir / "15_cluster_evolution_area.png")

    heat_df = labeled_pivot.T
    if not heat_df.empty:
        plt.figure(figsize=(max(13, 0.35 * heat_df.shape[1] + 5), max(7, 0.55 * heat_df.shape[0] + 2)))
        im = plt.imshow(heat_df.to_numpy(), aspect="auto", cmap="OrRd", interpolation="nearest")
        plt.colorbar(im, label="Paper count")
        plt.yticks(range(len(heat_df.index)), heat_df.index.tolist())
        years_str = [str(int(year)) for year in heat_df.columns]
        x_positions = list(range(len(years_str)))
        if len(years_str) > 20:
            step = max(1, len(years_str) // 20)
            x_positions = x_positions[::step]
            years_str = years_str[::step]
        plt.xticks(x_positions, years_str, rotation=45, ha="right")
        plt.xlabel("Year")
        plt.ylabel("Cluster theme")
        plt.title("Cluster intensity over time")
        save_figure(plt, figures_dir / "16_cluster_heatmap_over_time.png")


def plot_reference_cluster_transition_heatmap(plt, pd, cluster_transitions_df, figures_dir: Path) -> None:
    if cluster_transitions_df is None or cluster_transitions_df.empty:
        return

    pivot_df = cluster_transitions_df.pivot(
        index="source_cluster_label",
        columns="target_cluster_label",
        values="citation_count",
    ).fillna(0)
    if pivot_df.empty:
        return

    row_order = pivot_df.sum(axis=1).sort_values(ascending=False).index.tolist()
    col_order = pivot_df.sum(axis=0).sort_values(ascending=False).index.tolist()
    pivot_df = pivot_df.loc[row_order, col_order]

    plt.figure(figsize=(max(11, 0.55 * len(col_order) + 6), max(8, 0.55 * len(row_order) + 4)))
    im = plt.imshow(pivot_df.to_numpy(), aspect="auto", cmap="PuBu", interpolation="nearest")
    plt.colorbar(im, label="Internal reference count")
    plt.yticks(range(len(pivot_df.index)), pivot_df.index.tolist())
    plt.xticks(range(len(pivot_df.columns)), pivot_df.columns.tolist(), rotation=45, ha="right")
    plt.xlabel("Cited cluster theme")
    plt.ylabel("Citing cluster theme")
    plt.title("Cluster transitions inside the corpus")
    save_figure(plt, figures_dir / "27_reference_cluster_transition_heatmap.png")


def plot_group_totals(plt, total_df, group_col: str, output_path: Path, title: str, color: str) -> None:
    if total_df is None or total_df.empty:
        return
    plot_df = total_df.sort_values("paper_count", ascending=True)
    plt.figure(figsize=(10, max(5, 0.4 * len(plot_df) + 1.5)))
    plt.barh(plot_df[group_col].astype(str), plot_df["paper_count"], color=color)
    plt.xlabel("Number of papers")
    plt.title(title)
    save_figure(plt, output_path)


def plot_group_evolution(plt, pivot_df, output_path: Path, title: str) -> None:
    if pivot_df is None or pivot_df.empty:
        return
    plt.figure(figsize=(12, 7))
    years = pivot_df.index.astype(int).to_numpy()
    plt.stackplot(
        years,
        [pivot_df[column].to_numpy() for column in pivot_df.columns],
        labels=[str(column) for column in pivot_df.columns],
        alpha=0.9,
    )
    plt.xlabel("Year")
    plt.ylabel("Paper count")
    plt.title(title)
    plt.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0))
    save_figure(plt, output_path)


def fetch_json(url: str, timeout: float) -> Dict[str, Any]:
    headers = {
        "User-Agent": "review-bibliometric-analysis/1.0",
        "Accept": "application/json",
    }
    request = urllib.request.Request(url, headers=headers)
    ssl_context = ssl.create_default_context()
    with urllib.request.urlopen(request, timeout=timeout, context=ssl_context) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        payload = response.read().decode(charset, errors="replace")
    return json.loads(payload)


def load_metadata_cache(cache_path: Path) -> Dict[str, Dict[str, Any]]:
    if not cache_path.exists():
        return {}
    try:
        data = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def save_metadata_cache(cache_path: Path, cache: Dict[str, Dict[str, Any]]) -> None:
    cache_path.write_text(json.dumps(cache, indent=2, ensure_ascii=True, sort_keys=True), encoding="utf-8")


def probe_external_metadata(timeout: float) -> bool:
    test_urls = [
        "https://api.openalex.org/works?per-page=1",
        "https://api.crossref.org/works?rows=1",
    ]
    for url in test_urls:
        try:
            fetch_json(url, timeout=max(1.0, min(timeout, 2.0)))
            return True
        except Exception:
            continue
    return False


def split_affiliation_chunks(text: str) -> List[str]:
    chunks = [normalize_entity_name(piece) for piece in re.split(r"[;,]", clean_text(text))]
    return [chunk for chunk in chunks if chunk]


def clean_location_candidate(value: str) -> str:
    text = clean_text(value)
    if not text:
        return ""
    text = re.sub(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\b\d{4,}\b", "", text)
    text = re.sub(r"\s+", " ", text).strip(" ,;()")
    return normalize_entity_name(text)


def is_city_like_candidate(value: str) -> bool:
    candidate = clean_location_candidate(value)
    if not candidate:
        return False
    lower = candidate.lower()
    candidate_key = re.sub(r"[^a-z]+", " ", lower).strip()
    if candidate_key in NON_CITY_LOCATION_STOPWORDS_KEYS:
        return False
    if any(keyword in lower for keyword in AFFILIATION_KEYWORDS):
        return False
    if " and " in lower:
        return False
    if any(keyword in lower for keyword in NON_CITY_ENTITY_KEYWORDS):
        return False
    if re.search(r"\d", candidate):
        return False
    if candidate.isupper() and len(candidate) <= 3:
        return False
    token_count = len(candidate.split())
    return 1 <= token_count <= 4


def extract_location_candidates_from_text(text: str) -> List[str]:
    raw_text = clean_text(text)
    if not raw_text:
        return []
    chunks = split_affiliation_chunks(raw_text)
    for match in re.finditer(r"\(([^()]+)\)", raw_text):
        chunks.extend(split_affiliation_chunks(match.group(1)))

    candidates = []
    for chunk in reversed(chunks):
        candidate = clean_location_candidate(chunk)
        if is_city_like_candidate(candidate):
            candidates.append(candidate)
    return unique_preserve_order(candidates)


def infer_city_from_institution_name(name: str) -> str:
    institution = normalize_entity_name(name)
    if not institution:
        return ""
    for pattern in INSTITUTION_CITY_PATTERNS:
        match = re.search(pattern, institution, flags=re.IGNORECASE)
        if not match:
            continue
        candidate = clean_location_candidate(match.group(1))
        if is_city_like_candidate(candidate):
            return candidate
    return ""


def guess_affiliation_location(chunks: Sequence[str], excluded_chunks: Sequence[str]) -> str:
    excluded = {clean_text(chunk).lower() for chunk in excluded_chunks if clean_text(chunk)}
    for chunk in reversed(chunks):
        lower = chunk.lower()
        if lower in excluded:
            continue
        if lower in AFFILIATION_LOCATION_STOPWORDS:
            continue
        if any(keyword in lower for keyword in AFFILIATION_KEYWORDS):
            continue
        if re.search(r"\d", chunk):
            continue
        token_count = len(chunk.split())
        if 1 <= token_count <= 4:
            return chunk
    return ""


def build_affiliation_context_label(raw_affiliation: str, institutions: Sequence[str]) -> str:
    chunks = split_affiliation_chunks(raw_affiliation)
    if not chunks:
        return ""

    team_chunk = first_non_empty(*(chunk for chunk in chunks if any(keyword in chunk.lower() for keyword in TEAM_LAB_KEYWORDS)))
    institution_chunk = first_non_empty(
        *(chunk for chunk in chunks if any(keyword in chunk.lower() for keyword in INSTITUTION_KEYWORDS))
    )
    if not institution_chunk:
        institution_chunk = first_non_empty(
            *(institution for institution in institutions if normalize_title_key(institution) in normalize_title_key(raw_affiliation))
        )
    if not institution_chunk:
        institution_chunk = first_non_empty(*institutions)

    parts = []
    if team_chunk and team_chunk.lower() != institution_chunk.lower():
        parts.append(team_chunk)
    if institution_chunk:
        parts.append(institution_chunk)
    location_chunk = guess_affiliation_location(chunks, parts)
    label = " - ".join(parts) if parts else first_non_empty(*chunks)
    if location_chunk and location_chunk.lower() not in label.lower():
        label = f"{label} ({location_chunk})"
    return label


def extract_team_lab_cities(raw_affiliations: Sequence[str], team_labs: Sequence[str], institutions: Sequence[str]) -> List[str]:
    candidates = []
    for raw in raw_affiliations:
        locations = extract_location_candidates_from_text(raw)
        if locations:
            candidates.append(locations[0])
    if not candidates:
        for institution in institutions:
            location = infer_city_from_institution_name(institution)
            if location:
                candidates.append(location)
    return unique_preserve_order(candidates)


def extract_institutions_from_affiliation(text: str) -> List[str]:
    clean = clean_text(text)
    if not clean:
        return []
    pieces = re.split(r"[;,]", clean)
    selected = []
    for piece in pieces:
        chunk = normalize_entity_name(piece)
        lower = chunk.lower()
        if any(keyword in lower for keyword in INSTITUTION_KEYWORDS):
            chunk = re.sub(r"\b\d{4,}\b", "", chunk)
            chunk = re.sub(r"\s+", " ", chunk).strip(" ,")
            if len(chunk) >= 4:
                selected.append(chunk)
    if not selected and len(clean) >= 4:
        selected = [normalize_entity_name(clean)]
    return unique_preserve_order(selected)


def extract_team_lab_entities(raw_affiliations: Sequence[str], institutions: Sequence[str]) -> List[str]:
    candidates = []
    for raw in raw_affiliations:
        text = clean_text(raw)
        if not text:
            continue
        label = build_affiliation_context_label(text, institutions)
        if label:
            candidates.append(label)
    if not candidates:
        candidates.extend(institutions)
    return unique_preserve_order(candidates)


def parse_crossref_work(item: Dict[str, Any]) -> Dict[str, Any]:
    institutions = []
    raw_affiliations = []
    for author in item.get("author", []) or []:
        for aff in author.get("affiliation", []) or []:
            aff_name = clean_text(aff.get("name", ""))
            if not aff_name:
                continue
            raw_affiliations.append(aff_name)
            institutions.extend(extract_institutions_from_affiliation(aff_name))

    references = []
    for ref in item.get("reference", []) or []:
        references.append(
            {
                "doi": normalize_doi(ref.get("DOI", "")),
                "title": first_non_empty(
                    ref.get("article-title", ""),
                    ref.get("volume-title", ""),
                    ref.get("journal-title", ""),
                    ref.get("series-title", ""),
                    ref.get("unstructured", ""),
                ),
                "year": extract_year_from_text(ref.get("year", "")),
                "author": normalize_author_name(ref.get("author", "")),
            }
        )

    return {
        "resolved_doi": normalize_doi(item.get("DOI", "")),
        "title": clean_text(first_non_empty(*(item.get("title", []) or [""]))),
        "institutions": unique_preserve_order(institutions),
        "raw_affiliations": unique_preserve_order(raw_affiliations),
        "references": references,
        "openalex_id": "",
        "referenced_openalex_ids": [],
        "sources": ["crossref"],
    }


def parse_openalex_work(work: Dict[str, Any]) -> Dict[str, Any]:
    institutions = []
    raw_affiliations = []
    for authorship in work.get("authorships", []) or []:
        for institution in authorship.get("institutions", []) or []:
            name = clean_text(institution.get("display_name", ""))
            if name:
                institutions.append(normalize_entity_name(name))
        for raw in authorship.get("raw_affiliation_strings", []) or []:
            raw_clean = clean_text(raw)
            if raw_clean:
                raw_affiliations.append(raw_clean)
                institutions.extend(extract_institutions_from_affiliation(raw_clean))

    return {
        "resolved_doi": normalize_doi(work.get("doi", "")),
        "title": clean_text(work.get("display_name", "")),
        "institutions": unique_preserve_order(institutions),
        "raw_affiliations": unique_preserve_order(raw_affiliations),
        "references": [],
        "openalex_id": clean_text(work.get("id", "")),
        "referenced_openalex_ids": unique_preserve_order(work.get("referenced_works", []) or []),
        "sources": ["openalex"],
    }


def merge_metadata_records(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "resolved_doi": first_non_empty(base.get("resolved_doi", ""), extra.get("resolved_doi", "")),
        "title": first_non_empty(base.get("title", ""), extra.get("title", "")),
        "institutions": unique_preserve_order(list(base.get("institutions", [])) + list(extra.get("institutions", []))),
        "raw_affiliations": unique_preserve_order(list(base.get("raw_affiliations", [])) + list(extra.get("raw_affiliations", []))),
        "references": list(base.get("references", [])) + list(extra.get("references", [])),
        "openalex_id": first_non_empty(base.get("openalex_id", ""), extra.get("openalex_id", "")),
        "referenced_openalex_ids": unique_preserve_order(
            list(base.get("referenced_openalex_ids", [])) + list(extra.get("referenced_openalex_ids", []))
        ),
        "sources": unique_preserve_order(list(base.get("sources", [])) + list(extra.get("sources", []))),
    }


def choose_best_crossref_item(items: Sequence[Dict[str, Any]], title_key: str) -> Optional[Dict[str, Any]]:
    best_item = None
    best_score = 0.0
    for item in items:
        item_title = clean_text(first_non_empty(*(item.get("title", []) or [""])))
        item_title_key = normalize_title_key(item_title)
        score = title_similarity(title_key, item_title_key)
        if score > best_score:
            best_score = score
            best_item = item
    if best_item is None or best_score < 0.80:
        return None
    return best_item


def choose_best_openalex_item(results: Sequence[Dict[str, Any]], title_key: str) -> Optional[Dict[str, Any]]:
    best_item = None
    best_score = 0.0
    for item in results:
        item_title_key = normalize_title_key(item.get("display_name", ""))
        score = title_similarity(title_key, item_title_key)
        if score > best_score:
            best_score = score
            best_item = item
    if best_item is None or best_score < 0.80:
        return None
    return best_item


def get_external_metadata_for_row(row, cache: Dict[str, Dict[str, Any]], timeout: float) -> Dict[str, Any]:
    lookup_key = normalize_doi(getattr(row, "doi_norm", "")) or f"title::{getattr(row, 'title_key', '')}"
    if lookup_key in cache:
        cached = dict(cache[lookup_key])
        cached["institutions"] = unique_preserve_order(cached.get("institutions", []))
        cached["raw_affiliations"] = unique_preserve_order(cached.get("raw_affiliations", []))
        cached["team_labs"] = extract_team_lab_entities(cached.get("raw_affiliations", []), cached.get("institutions", []))
        cached["team_lab_cities"] = extract_team_lab_cities(
            cached.get("raw_affiliations", []),
            cached.get("team_labs", []),
            cached.get("institutions", []),
        )
        cache[lookup_key] = cached
        return cached

    title = clean_text(getattr(row, "title", ""))
    title_key = normalize_title_key(title)
    doi_norm = normalize_doi(getattr(row, "doi_norm", ""))
    merged = {
        "resolved_doi": doi_norm,
        "title": title,
        "institutions": [],
        "raw_affiliations": [],
        "references": [],
        "openalex_id": "",
        "referenced_openalex_ids": [],
        "sources": [],
        "status": "not_found",
        "error": "",
    }
    errors = []

    if doi_norm:
        try:
            doi_encoded = urllib.parse.quote(doi_norm, safe="")
            crossref_payload = fetch_json(f"https://api.crossref.org/works/{doi_encoded}", timeout)
            merged = merge_metadata_records(merged, parse_crossref_work(crossref_payload.get("message", {})))
            merged["status"] = "ok"
        except Exception as exc:
            errors.append(f"crossref_doi:{type(exc).__name__}")

        try:
            doi_filter = urllib.parse.quote(f"https://doi.org/{doi_norm}", safe="")
            openalex_payload = fetch_json(f"https://api.openalex.org/works?filter=doi:{doi_filter}&per-page=1", timeout)
            results = openalex_payload.get("results", []) or []
            if results:
                merged = merge_metadata_records(merged, parse_openalex_work(results[0]))
                merged["status"] = "ok"
        except Exception as exc:
            errors.append(f"openalex_doi:{type(exc).__name__}")

    if merged["status"] != "ok" and title_key:
        try:
            query = urllib.parse.quote(title)
            crossref_payload = fetch_json(f"https://api.crossref.org/works?query.title={query}&rows=5", timeout)
            item = choose_best_crossref_item(crossref_payload.get("message", {}).get("items", []) or [], title_key)
            if item is not None:
                merged = merge_metadata_records(merged, parse_crossref_work(item))
                merged["status"] = "ok"
        except Exception as exc:
            errors.append(f"crossref_title:{type(exc).__name__}")

        try:
            query = urllib.parse.quote(title)
            openalex_payload = fetch_json(f"https://api.openalex.org/works?search={query}&per-page=5", timeout)
            item = choose_best_openalex_item(openalex_payload.get("results", []) or [], title_key)
            if item is not None:
                merged = merge_metadata_records(merged, parse_openalex_work(item))
                merged["status"] = "ok"
        except Exception as exc:
            errors.append(f"openalex_title:{type(exc).__name__}")

    merged["institutions"] = unique_preserve_order(merged.get("institutions", []))
    merged["raw_affiliations"] = unique_preserve_order(merged.get("raw_affiliations", []))
    merged["team_labs"] = extract_team_lab_entities(merged["raw_affiliations"], merged["institutions"])
    merged["team_lab_cities"] = extract_team_lab_cities(merged["raw_affiliations"], merged["team_labs"], merged["institutions"])
    merged["crossref_reference_count"] = int(len(merged.get("references", [])))
    merged["openalex_reference_count"] = int(len(merged.get("referenced_openalex_ids", [])))
    merged["reference_count"] = int(max(merged["crossref_reference_count"], merged["openalex_reference_count"]))
    merged["error"] = ";".join(errors)
    merged["metadata_sources"] = ", ".join(merged.get("sources", []))
    cache[lookup_key] = merged
    return merged


def enrich_with_external_metadata(df, pd, tables_dir: Path, cache_path: Path, enabled: bool, timeout: float, pause: float):
    cache = load_metadata_cache(cache_path)
    records = []
    metadata_enabled = enabled
    consecutive_failures = 0
    failure_cutoff = 3
    start_time = time.monotonic()
    if metadata_enabled:
        log("Probing external metadata services...")
        metadata_enabled = probe_external_metadata(timeout)
        if not metadata_enabled:
            log("External metadata services not reachable. Affiliations and references will be skipped.")

    total_papers = len(df)
    for row_number, row in enumerate(df.itertuples(index=False), start=1):
        if metadata_enabled and (time.monotonic() - start_time) > DEFAULT_METADATA_MAX_RUNTIME_SECONDS:
            log("External metadata time budget reached. Disabling further lookups for this run.")
            metadata_enabled = False
        if not metadata_enabled:
            record = {
                "resolved_doi": normalize_doi(row.doi_norm),
                "institutions": [],
                "raw_affiliations": [],
                "references": [],
                "openalex_id": "",
                "referenced_openalex_ids": [],
                "team_labs": [],
                "team_lab_cities": [],
                "reference_count": 0,
                "crossref_reference_count": 0,
                "openalex_reference_count": 0,
                "status": "disabled",
                "error": "",
                "metadata_sources": "",
            }
        else:
            try:
                lookup_key = normalize_doi(row.doi_norm) or f"title::{row.title_key}"
                if lookup_key not in cache:
                    log(f"Crossref {row_number}/{total_papers} | DOI={normalize_doi(row.doi_norm) or 'missing'}")
                record = get_external_metadata_for_row(row, cache=cache, timeout=timeout)
            except Exception as exc:
                record = {
                    "resolved_doi": normalize_doi(row.doi_norm),
                    "institutions": [],
                    "raw_affiliations": [],
                    "references": [],
                    "openalex_id": "",
                    "referenced_openalex_ids": [],
                    "team_labs": [],
                    "team_lab_cities": [],
                    "reference_count": 0,
                    "crossref_reference_count": 0,
                    "openalex_reference_count": 0,
                    "status": "error",
                    "error": type(exc).__name__,
                    "metadata_sources": "",
                }
            if record.get("status") == "ok":
                consecutive_failures = 0
            elif record.get("error") and not record.get("metadata_sources"):
                consecutive_failures += 1
            else:
                consecutive_failures = 0
            if metadata_enabled and consecutive_failures >= failure_cutoff:
                log("Too many consecutive external metadata failures. Disabling further lookups for this run.")
                metadata_enabled = False
            if pause > 0:
                time.sleep(pause)

        records.append(
            {
                "paper_id": int(row.paper_id),
                "paper_uid": row.paper_uid,
                "title": row.title,
                "doi_norm": normalize_doi(row.doi_norm),
                "metadata_status": record.get("status", "unknown"),
                "metadata_sources": record.get("metadata_sources", ""),
                "resolved_doi": record.get("resolved_doi", ""),
                "openalex_id": record.get("openalex_id", ""),
                "institution_count": len(record.get("institutions", [])),
                "team_lab_count": len(record.get("team_labs", [])),
                "team_city_count": len(record.get("team_lab_cities", [])),
                "reference_count": int(record.get("reference_count", 0)),
                "crossref_reference_count": int(record.get("crossref_reference_count", 0)),
                "openalex_reference_count": int(record.get("openalex_reference_count", 0)),
                "institutions_json": json.dumps(record.get("institutions", []), ensure_ascii=True),
                "team_labs_json": json.dumps(record.get("team_labs", []), ensure_ascii=True),
                "team_cities_json": json.dumps(record.get("team_lab_cities", []), ensure_ascii=True),
                "references_json": json.dumps(record.get("references", []), ensure_ascii=True),
                "referenced_openalex_ids_json": json.dumps(record.get("referenced_openalex_ids", []), ensure_ascii=True),
                "error": record.get("error", ""),
            }
        )

        idx = int(row.paper_id)
        df.at[idx, "institution_list"] = list(record.get("institutions", []))
        df.at[idx, "team_lab_list"] = list(record.get("team_labs", []))
        df.at[idx, "team_city_list"] = list(record.get("team_lab_cities", []))
        df.at[idx, "reference_details"] = list(record.get("references", []))
        df.at[idx, "referenced_openalex_ids"] = list(record.get("referenced_openalex_ids", []))
        df.at[idx, "openalex_id"] = clean_text(record.get("openalex_id", ""))
        df.at[idx, "metadata_status"] = clean_text(record.get("status", ""))
        df.at[idx, "metadata_sources"] = clean_text(record.get("metadata_sources", ""))
        df.at[idx, "reference_count"] = int(record.get("reference_count", 0))
        df.at[idx, "crossref_reference_count"] = int(record.get("crossref_reference_count", 0))
        df.at[idx, "openalex_reference_count"] = int(record.get("openalex_reference_count", 0))

    if enabled:
        save_metadata_cache(cache_path, cache)

    metadata_df = pd.DataFrame(records)
    metadata_df.to_csv(tables_dir / "external_metadata_audit.csv", index=False)
    summary_df = (
        metadata_df["metadata_status"]
        .value_counts()
        .rename_axis("metadata_status")
        .reset_index(name="paper_count")
        .sort_values(["paper_count", "metadata_status"], ascending=[False, True])
    )
    summary_df.to_csv(tables_dir / "external_metadata_status_summary.csv", index=False)
    return df, metadata_df, summary_df


def plot_team_lab_statistics(df, pd, plt, figures_dir: Path, tables_dir: Path, top_n: int):
    _, institution_total_df, institution_by_year_df, institution_pivot_df, institution_active_df = build_list_entity_tables(
        df=df,
        pd=pd,
        list_col="institution_list",
        entity_col="institution",
    )
    institution_total_df.to_csv(tables_dir / "institution_total_counts.csv", index=False)
    institution_by_year_df.to_csv(tables_dir / "institution_by_year_counts.csv", index=False)
    institution_pivot_df.to_csv(tables_dir / "institution_by_year_pivot.csv")
    institution_active_df.to_csv(tables_dir / "institution_active_years.csv", index=False)

    _, team_total_df, team_by_year_df, team_pivot_df, team_active_df = build_list_entity_tables(
        df=df,
        pd=pd,
        list_col="team_city_list",
        entity_col="team_lab",
    )
    team_total_df.to_csv(tables_dir / "team_lab_total_counts.csv", index=False)
    team_by_year_df.to_csv(tables_dir / "team_lab_by_year_counts.csv", index=False)
    team_pivot_df.to_csv(tables_dir / "team_lab_by_year_pivot.csv")
    team_active_df.to_csv(tables_dir / "team_lab_active_years.csv", index=False)

    plot_top_entity_bar(
        plt,
        team_total_df,
        entity_col="team_lab",
        output_path=figures_dir / "21_top_team_labs.png",
        title=f"Top {top_n} cities represented in team/lab affiliations",
        top_n=top_n,
        color="#9C755F",
    )
    plot_entity_year_heatmap(
        plt,
        team_pivot_df,
        output_path=figures_dir / "22_team_lab_heatmap_by_year.png",
        title=f"Top {min(top_n, 15)} affiliation cities over time",
        top_n=min(top_n, 15),
    )
    return institution_total_df, institution_by_year_df, team_total_df, team_by_year_df


def format_paper_node_label(author_list: Sequence[str], authors_raw: str, year: Optional[int]) -> str:
    first_author = clean_text(author_list[0]) if author_list else clean_text(authors_raw).split(";")[0].strip()
    if not first_author:
        first_author = "Unknown author"
    if year is None:
        return first_author
    return f"{first_author} ({year})"


def compute_internal_reference_graph_metrics(pd, nx, graph) -> Tuple[Any, Any]:
    active_nodes = [node for node in graph.nodes() if graph.degree(node) > 0]
    active_graph = graph.subgraph(active_nodes).copy()
    if active_graph.number_of_nodes() == 0:
        metrics_df = pd.DataFrame(
            [
                {"metric": "active_papers_in_reference_graph", "value": 0},
                {"metric": "internal_reference_edges", "value": 0},
                {"metric": "density", "value": 0.0},
                {"metric": "weakly_connected_components", "value": 0},
                {"metric": "largest_weak_component_size", "value": 0},
                {"metric": "average_total_degree", "value": 0.0},
                {"metric": "average_in_degree", "value": 0.0},
                {"metric": "average_out_degree", "value": 0.0},
                {"metric": "average_clustering_undirected", "value": 0.0},
                {"metric": "reciprocity", "value": 0.0},
            ]
        )
        node_metrics_df = pd.DataFrame(
            columns=[
                "paper_id",
                "display_label",
                "title",
                "year",
                "in_degree",
                "out_degree",
                "total_degree",
                "pagerank",
                "betweenness",
            ]
        )
        return metrics_df, node_metrics_df

    pagerank = nx.pagerank(active_graph, weight="weight")
    betweenness = nx.betweenness_centrality(active_graph, normalized=True, weight=None)
    node_rows = []
    for node in active_graph.nodes():
        node_rows.append(
            {
                "paper_id": int(node),
                "display_label": active_graph.nodes[node].get("display_label", ""),
                "title": active_graph.nodes[node].get("title", ""),
                "year": int(active_graph.nodes[node].get("year", -1)),
                "in_degree": int(active_graph.in_degree(node)),
                "out_degree": int(active_graph.out_degree(node)),
                "total_degree": int(active_graph.degree(node)),
                "pagerank": float(pagerank.get(node, 0.0)),
                "betweenness": float(betweenness.get(node, 0.0)),
            }
        )
    node_metrics_df = pd.DataFrame(node_rows).sort_values(
        ["pagerank", "in_degree", "out_degree"],
        ascending=[False, False, False],
    )

    undirected = active_graph.to_undirected()
    weak_components = list(nx.weakly_connected_components(active_graph))
    largest_component_nodes = max(weak_components, key=len) if weak_components else set()
    largest_component = active_graph.subgraph(largest_component_nodes).copy() if largest_component_nodes else nx.DiGraph()
    largest_component_undirected = largest_component.to_undirected()
    reciprocity = nx.reciprocity(active_graph)
    metrics_rows = [
        {"metric": "active_papers_in_reference_graph", "value": int(active_graph.number_of_nodes())},
        {"metric": "internal_reference_edges", "value": int(active_graph.number_of_edges())},
        {"metric": "density", "value": round(float(nx.density(active_graph)), 6)},
        {"metric": "weakly_connected_components", "value": int(len(weak_components))},
        {"metric": "largest_weak_component_size", "value": int(len(largest_component_nodes))},
        {"metric": "average_total_degree", "value": round(float(node_metrics_df["total_degree"].mean()), 4)},
        {"metric": "average_in_degree", "value": round(float(node_metrics_df["in_degree"].mean()), 4)},
        {"metric": "average_out_degree", "value": round(float(node_metrics_df["out_degree"].mean()), 4)},
        {"metric": "average_clustering_undirected", "value": round(float(nx.average_clustering(undirected)), 6)},
        {"metric": "reciprocity", "value": round(float(reciprocity or 0.0), 6)},
    ]
    if largest_component.number_of_nodes() > 1 and largest_component_undirected.number_of_nodes() > 1:
        metrics_rows.append(
            {
                "metric": "largest_component_average_shortest_path",
                "value": round(float(nx.average_shortest_path_length(largest_component_undirected)), 6),
            }
        )
        metrics_rows.append(
            {
                "metric": "largest_component_diameter",
                "value": int(nx.diameter(largest_component_undirected)),
            }
        )
    return pd.DataFrame(metrics_rows), node_metrics_df


def plot_internal_reference_network(plt, nx, graph, node_metrics_df, metrics_df, figures_dir: Path) -> None:
    import numpy as np
    from matplotlib.patches import Rectangle

    def centered_slots(count: int, step: float) -> List[float]:
        if count <= 1:
            return [0.0]
        start = -step * (count - 1) / 2.0
        return [start + idx * step for idx in range(count)]

    def format_label(node: int) -> str:
        return graph.nodes[node].get("display_label", str(node)).replace(" (", "\n(")

    def build_temporal_positions(
        graph_obj,
        *,
        seed_base: int,
        local_k_scale: float,
        iterations: int,
        component_gap: float,
        slot_step_small: float,
        slot_step_large: float,
        component_height_floor: float,
        component_height_pad: float,
        component_center_scale_factor: float,
        x_jitter_small: float,
        x_jitter_large: float,
        label_shift_x: int,
        label_shift_y: int,
    ) -> Tuple[Dict[int, Tuple[float, float]], Dict[int, Tuple[int, int]]]:
        graph_years = [graph_obj.nodes[node].get("year", -1) for node in graph_obj.nodes() if graph_obj.nodes[node].get("year", -1) >= 0]
        graph_min_year = int(min(graph_years)) if graph_years else 0
        positions_local: Dict[int, Tuple[float, float]] = {}
        label_offsets_local: Dict[int, Tuple[int, int]] = {}
        vertical_cursor_local = 0.0
        components_local = sorted(nx.weakly_connected_components(graph_obj), key=len, reverse=True)

        for component_index, component_nodes in enumerate(components_local):
            subgraph = graph_obj.subgraph(component_nodes).copy()
            local_pos = nx.spring_layout(
                subgraph.to_undirected(),
                seed=seed_base + component_index,
                k=local_k_scale / max(math.sqrt(subgraph.number_of_nodes()), 1.0),
                iterations=iterations,
            )
            x_jitter_scale = x_jitter_small if subgraph.number_of_nodes() <= 20 else x_jitter_large
            nodes_by_year: Dict[int, List[int]] = {}
            for node in subgraph.nodes():
                year = int(subgraph.nodes[node].get("year", graph_min_year))
                if year < 0:
                    year = graph_min_year
                nodes_by_year.setdefault(year, []).append(node)

            max_nodes_in_year = max((len(nodes) for nodes in nodes_by_year.values()), default=1)
            slot_step = slot_step_small if max_nodes_in_year <= 6 else slot_step_large
            component_height = max(component_height_floor, slot_step * (max_nodes_in_year + 2) + component_height_pad)
            y_offset = vertical_cursor_local - (component_height / 2.0)
            vertical_cursor_local -= component_height + component_gap
            component_center_scale = max(4.5, component_center_scale_factor * component_height)

            for year, nodes in sorted(nodes_by_year.items()):
                ordered_nodes = sorted(
                    nodes,
                    key=lambda node: (
                        round(float(local_pos[node][1]), 6),
                        graph_obj.nodes[node].get("display_label", ""),
                    ),
                )
                mean_local_y = sum(float(local_pos[node][1]) for node in ordered_nodes) / max(len(ordered_nodes), 1)
                slots = centered_slots(len(ordered_nodes), slot_step)
                for node_index, node in enumerate(ordered_nodes):
                    coords = local_pos[node]
                    positions_local[node] = (
                        float(year) + float(coords[0]) * x_jitter_scale,
                        y_offset
                        + mean_local_y * component_center_scale
                        + slots[node_index]
                        + (float(coords[1]) - mean_local_y) * 1.2,
                    )
                    x_shift = label_shift_x if node_index % 2 == 0 else -label_shift_x
                    y_shift = label_shift_y if node_index % 4 in {0, 1} else -label_shift_y
                    label_offsets_local[node] = (x_shift, y_shift)
        return positions_local, label_offsets_local

    def build_radial_label_offsets(
        positions_local: Dict[int, Tuple[float, float]],
        *,
        magnitude_x: int,
        magnitude_y: int,
    ) -> Dict[int, Tuple[int, int]]:
        if not positions_local:
            return {}
        center_x = sum(coords[0] for coords in positions_local.values()) / max(len(positions_local), 1)
        center_y = sum(coords[1] for coords in positions_local.values()) / max(len(positions_local), 1)
        offsets = {}
        for node, (x_coord, y_coord) in positions_local.items():
            dx = magnitude_x if x_coord >= center_x else -magnitude_x
            dy = magnitude_y if y_coord >= center_y else -magnitude_y
            offsets[node] = (dx, dy)
        return offsets

    def normalize_layout_positions(
        raw_positions: Dict[int, Tuple[float, float]],
        *,
        scale_x: float,
        scale_y: float,
    ) -> Dict[int, Tuple[float, float]]:
        if not raw_positions:
            return {}
        x_values = [float(coords[0]) for coords in raw_positions.values()]
        y_values = [float(coords[1]) for coords in raw_positions.values()]
        x_min, x_max = min(x_values), max(x_values)
        y_min, y_max = min(y_values), max(y_values)
        x_center = (x_min + x_max) / 2.0
        y_center = (y_min + y_max) / 2.0
        x_span = max(x_max - x_min, 1e-6)
        y_span = max(y_max - y_min, 1e-6)
        return {
            node: (
                ((float(coords[0]) - x_center) / x_span) * scale_x,
                ((float(coords[1]) - y_center) / y_span) * scale_y,
            )
            for node, coords in raw_positions.items()
        }

    def build_force_layout_positions(
        graph_obj,
        *,
        scale_x: float = 34.0,
        scale_y: float = 28.0,
        label_dx: int = 14,
        label_dy: int = 12,
        iterations: int = 1600,
    ) -> Tuple[Dict[int, Tuple[float, float]], Dict[int, Tuple[int, int]]]:
        raw_positions = nx.spring_layout(
            graph_obj.to_undirected(),
            seed=314,
            weight="weight",
            k=4.2 / max(math.sqrt(max(graph_obj.number_of_nodes(), 2)), 1.0),
            iterations=iterations,
        )
        positions_local = normalize_layout_positions(raw_positions, scale_x=scale_x, scale_y=scale_y)
        return positions_local, build_radial_label_offsets(positions_local, magnitude_x=label_dx, magnitude_y=label_dy)

    def build_sugiyama_like_positions(
        graph_obj,
        *,
        label_dx: int = 12,
        label_dy: int = 11,
        node_step_x: float = 6.2,
        layer_step_y: float = 7.0,
    ) -> Tuple[Dict[int, Tuple[float, float]], Dict[int, Tuple[int, int]], Dict[int, int]]:
        condensation = nx.condensation(graph_obj)
        topo_nodes = list(nx.topological_sort(condensation)) if condensation.number_of_nodes() > 0 else []
        layer_by_component: Dict[int, int] = {}
        for component in reversed(topo_nodes):
            successors = list(condensation.successors(component))
            if successors:
                layer_by_component[int(component)] = 1 + max(layer_by_component[int(succ)] for succ in successors)
            else:
                layer_by_component[int(component)] = 0

        mapping = condensation.graph.get("mapping", {})
        layer_map = {node: int(layer_by_component.get(int(mapping.get(node, 0)), 0)) for node in graph_obj.nodes()}
        max_layer = max(layer_map.values(), default=0)
        layer_widths = [sum(1 for value in layer_map.values() if value == layer) for layer in range(max_layer + 1)]
        max_layer_width = max(layer_widths, default=1)
        local_node_step_x = max(4.0, node_step_x - 0.05 * max(0, max_layer_width - 10))
        positions_local: Dict[int, Tuple[float, float]] = {}

        for layer in range(max_layer, -1, -1):
            layer_nodes = [node for node, value in layer_map.items() if value == layer]
            layer_nodes = sorted(
                layer_nodes,
                key=lambda node: (
                    graph_obj.nodes[node].get("year", 9999),
                    -float(pagerank_map.get(int(node), 0.0)),
                    graph_obj.nodes[node].get("display_label", ""),
                ),
            )
            for x_coord, node in zip(centered_slots(len(layer_nodes), local_node_step_x), layer_nodes):
                positions_local[node] = (x_coord, -float(layer) * layer_step_y)

        return positions_local, build_radial_label_offsets(positions_local, magnitude_x=label_dx, magnitude_y=label_dy), layer_map

    def build_circular_positions(
        graph_obj,
        *,
        radius_scale: float = 0.95,
        min_radius: float = 15.0,
        label_dx: int = 13,
        label_dy: int = 13,
    ) -> Tuple[Dict[int, Tuple[float, float]], Dict[int, Tuple[int, int]]]:
        ordered_nodes = sorted(
            graph_obj.nodes(),
            key=lambda node: (
                graph_obj.nodes[node].get("year", 9999),
                -float(pagerank_map.get(int(node), 0.0)),
                graph_obj.nodes[node].get("display_label", ""),
            ),
        )
        if not ordered_nodes:
            return {}, {}
        radius = max(min_radius, radius_scale * len(ordered_nodes))
        angles = np.linspace(np.pi / 2.0, np.pi / 2.0 - 2.0 * np.pi, num=len(ordered_nodes), endpoint=False)
        positions_local = {
            node: (radius * float(np.cos(angle)), radius * float(np.sin(angle)))
            for node, angle in zip(ordered_nodes, angles)
        }
        return positions_local, build_radial_label_offsets(positions_local, magnitude_x=label_dx, magnitude_y=label_dy)

    def select_zoom_nodes(graph_obj) -> List[int]:
        components_local = sorted(nx.weakly_connected_components(graph_obj), key=len, reverse=True)
        if not components_local:
            return []
        largest_component_nodes = list(components_local[0])
        if len(largest_component_nodes) <= 18:
            return largest_component_nodes

        component_graph = graph_obj.subgraph(largest_component_nodes).copy()
        undirected = component_graph.to_undirected()
        core_numbers = nx.core_number(undirected) if undirected.number_of_nodes() > 0 else {}
        ranked_nodes = sorted(
            component_graph.nodes(),
            key=lambda node: (
                core_numbers.get(node, 0),
                float(pagerank_map.get(int(node), 0.0)),
                int(component_graph.degree(node)),
                graph_obj.nodes[node].get("display_label", ""),
            ),
            reverse=True,
        )
        if not ranked_nodes:
            return []

        zoom_target = min(max(18, len(largest_component_nodes) // 5 + 8), 24)
        seed = ranked_nodes[0]
        selected = {seed}

        for candidate in ranked_nodes[1:]:
            try:
                path_nodes = nx.shortest_path(undirected, source=seed, target=candidate)
            except Exception:
                path_nodes = [candidate]
            if len(selected | set(path_nodes)) <= zoom_target + 3:
                selected.update(path_nodes)
            if len(selected) >= zoom_target:
                break

        for candidate in ranked_nodes[1:]:
            selected.add(candidate)
            if len(selected) >= zoom_target:
                break

        expanded = set(selected)
        for node in list(selected):
            for neighbor in undirected.neighbors(node):
                if len(expanded) >= zoom_target:
                    break
                expanded.add(neighbor)
            if len(expanded) >= zoom_target:
                break

        return sorted(
            expanded,
            key=lambda node: (
                graph_obj.nodes[node].get("year", 0),
                -float(pagerank_map.get(int(node), 0.0)),
                graph_obj.nodes[node].get("display_label", ""),
            ),
        )[:zoom_target]

    def draw_temporal_panel(
        ax,
        graph_obj,
        positions_local: Dict[int, Tuple[float, float]],
        label_offsets_local: Dict[int, Tuple[int, int]],
        *,
        title: str,
        node_size_scale: float,
        font_size: float,
        edge_alpha: float,
        label_alpha: float,
        label_nodes: Optional[List[int]] = None,
    ):
        edge_list = list(graph_obj.edges())
        nx.draw_networkx_edges(
            graph_obj,
            positions_local,
            ax=ax,
            edgelist=edge_list,
            width=[edge_width_map[(int(u), int(v))] * node_size_scale for u, v in edge_list],
            alpha=edge_alpha,
            edge_color="#7F5539",
            arrows=True,
            arrowstyle="-|>",
            arrowsize=12,
            connectionstyle="arc3,rad=0.06",
        )
        node_collection_local = nx.draw_networkx_nodes(
            graph_obj,
            positions_local,
            ax=ax,
            node_size=[node_size_map[int(node)] * node_size_scale for node in graph_obj.nodes()],
            node_color=[node_color_map[int(node)] for node in graph_obj.nodes()],
            cmap="YlOrRd",
            linewidths=0.8,
            edgecolors="white",
        )
        label_set = set(label_nodes) if label_nodes is not None else set(graph_obj.nodes())
        for node in graph_obj.nodes():
            if node not in label_set:
                continue
            dx, dy = label_offsets_local.get(node, (8, 8))
            ax.annotate(
                format_label(node),
                xy=positions_local[node],
                xytext=(dx, dy),
                textcoords="offset points",
                fontsize=font_size,
                color="#1F1F1F",
                ha="left" if dx >= 0 else "right",
                va="center",
                bbox={"facecolor": "white", "alpha": label_alpha, "edgecolor": "none", "pad": 0.16},
                arrowprops={"arrowstyle": "-", "color": "#A9A9A9", "lw": 0.35, "alpha": 0.55, "shrinkA": 0, "shrinkB": 4},
                annotation_clip=False,
                zorder=5,
            )
        panel_years = [graph_obj.nodes[node].get("year", -1) for node in graph_obj.nodes() if graph_obj.nodes[node].get("year", -1) >= 0]
        panel_min_year = int(min(panel_years)) if panel_years else min_year
        panel_max_year = int(max(panel_years)) if panel_years else max_year
        if len(set(panel_years)) <= 20:
            ax.set_xticks(sorted(set(panel_years)))
        else:
            step = max(1, math.ceil((panel_max_year - panel_min_year) / 12))
            ax.set_xticks(list(range(panel_min_year, panel_max_year + 1, step)))

        x_values_local = [coords[0] for coords in positions_local.values()]
        y_values_local = [coords[1] for coords in positions_local.values()]
        ax.set_xlim(min(x_values_local) - 1.8, max(x_values_local) + 1.8)
        ax.set_ylim(min(y_values_local) - 6.0, max(y_values_local) + 6.0)
        ax.set_xlabel("Publication year")
        ax.set_yticks([])
        ax.grid(axis="x", color="#DDDDDD", linewidth=0.6, alpha=0.8)
        ax.set_title(title)
        return node_collection_local

    def draw_network_panel(
        ax,
        graph_obj,
        positions_local: Dict[int, Tuple[float, float]],
        label_offsets_local: Dict[int, Tuple[int, int]],
        *,
        title: str,
        node_size_scale: float,
        font_size: float,
        edge_alpha: float,
        label_alpha: float,
        show_axis: bool = False,
    ):
        edge_list = list(graph_obj.edges())
        nx.draw_networkx_edges(
            graph_obj,
            positions_local,
            ax=ax,
            edgelist=edge_list,
            width=[edge_width_map[(int(u), int(v))] * node_size_scale for u, v in edge_list],
            alpha=edge_alpha,
            edge_color="#7F5539",
            arrows=True,
            arrowstyle="-|>",
            arrowsize=18 if node_size_scale >= 1.0 else 14,
            connectionstyle="arc3,rad=0.08",
        )
        node_collection_local = nx.draw_networkx_nodes(
            graph_obj,
            positions_local,
            ax=ax,
            node_size=[node_size_map[int(node)] * node_size_scale for node in graph_obj.nodes()],
            node_color=[node_color_map[int(node)] for node in graph_obj.nodes()],
            cmap="YlOrRd",
            linewidths=0.8,
            edgecolors="white",
        )
        for node in graph_obj.nodes():
            dx, dy = label_offsets_local.get(node, (8, 8))
            ax.annotate(
                format_label(node),
                xy=positions_local[node],
                xytext=(dx, dy),
                textcoords="offset points",
                fontsize=font_size,
                color="#1F1F1F",
                ha="left" if dx >= 0 else "right",
                va="center",
                bbox={"facecolor": "white", "alpha": label_alpha, "edgecolor": "none", "pad": 0.20},
                arrowprops={"arrowstyle": "-", "color": "#A9A9A9", "lw": 0.45, "alpha": 0.60, "shrinkA": 0, "shrinkB": 4},
                annotation_clip=False,
                zorder=5,
            )

        x_values_local = [coords[0] for coords in positions_local.values()]
        y_values_local = [coords[1] for coords in positions_local.values()]
        x_margin = max(3.5, 0.22 * (max(x_values_local) - min(x_values_local) + 1.0))
        y_margin = max(3.5, 0.22 * (max(y_values_local) - min(y_values_local) + 1.0))
        ax.set_xlim(min(x_values_local) - x_margin, max(x_values_local) + x_margin)
        ax.set_ylim(min(y_values_local) - y_margin, max(y_values_local) + y_margin)
        ax.margins(0.18)
        if show_axis:
            ax.set_xlabel("Layout X")
            ax.set_ylabel("Layout Y")
            ax.grid(color="#EAEAEA", linewidth=0.6, alpha=0.8)
        else:
            ax.axis("off")
        ax.set_title(title)
        return node_collection_local

    def get_ordered_nodes(graph_obj, layer_map: Dict[int, int]) -> List[int]:
        return sorted(
            graph_obj.nodes(),
            key=lambda node: (
                layer_map.get(node, 0),
                graph_obj.nodes[node].get("year", 9999),
                -float(pagerank_map.get(int(node), 0.0)),
                graph_obj.nodes[node].get("display_label", ""),
            ),
        )

    def build_short_labels(ordered_nodes: List[int], max_len: int) -> List[str]:
        labels = []
        for node in ordered_nodes:
            label = graph.nodes[node].get("display_label", str(node))
            labels.append(label if len(label) <= max_len else f"{label[:max_len - 3]}...")
        return labels

    def plot_single_layout_figure(
        graph_obj,
        positions_local: Dict[int, Tuple[float, float]],
        label_offsets_local: Dict[int, Tuple[int, int]],
        *,
        output_filename: str,
        title: str,
        node_size_scale: float,
        font_size: float,
        edge_alpha: float,
        label_alpha: float,
    ) -> None:
        fig, ax = plt.subplots(figsize=(34, 27))
        node_collection_local = draw_network_panel(
            ax,
            graph_obj,
            positions_local,
            label_offsets_local,
            title=title,
            node_size_scale=node_size_scale,
            font_size=font_size,
            edge_alpha=edge_alpha,
            label_alpha=label_alpha,
        )
        fig.colorbar(node_collection_local, ax=ax, fraction=0.028, pad=0.02, label="Incoming citations inside the corpus")
        fig.suptitle(title, y=0.985, fontsize=17)
        fig.subplots_adjust(left=0.03, right=0.97, top=0.95, bottom=0.04)
        save_figure(plt, figures_dir / output_filename, use_tight_layout=False)

    def plot_categorical_layout_figure(
        graph_obj,
        positions_local: Dict[int, Tuple[float, float]],
        label_offsets_local: Dict[int, Tuple[int, int]],
        *,
        category_attr: str,
        output_filename: str,
        title: str,
        legend_title: str,
        unknown_label: str,
        node_size_scale: float,
        font_size: float,
        edge_alpha: float,
        label_alpha: float,
    ) -> None:
        category_by_node = {}
        category_counts = Counter()
        for node in graph_obj.nodes():
            raw_value = clean_text(graph_obj.nodes[node].get(category_attr, ""))
            category_value = raw_value if raw_value else unknown_label
            category_by_node[node] = category_value
            category_counts[category_value] += 1

        ordered_categories = sorted(category_counts.keys(), key=lambda value: (-category_counts[value], value))
        palette = []
        for cmap_name in ["tab20", "tab20b", "tab20c"]:
            cmap = plt.get_cmap(cmap_name)
            palette.extend([cmap(idx) for idx in range(cmap.N)])
        category_colors = {
            category: palette[idx % len(palette)]
            for idx, category in enumerate(ordered_categories)
        }

        fig, ax = plt.subplots(figsize=(38, 28))
        edge_list = list(graph_obj.edges())
        nx.draw_networkx_edges(
            graph_obj,
            positions_local,
            ax=ax,
            edgelist=edge_list,
            width=[edge_width_map[(int(u), int(v))] * node_size_scale for u, v in edge_list],
            alpha=edge_alpha,
            edge_color="#7F5539",
            arrows=True,
            arrowstyle="-|>",
            arrowsize=16,
            connectionstyle="arc3,rad=0.08",
        )
        nx.draw_networkx_nodes(
            graph_obj,
            positions_local,
            ax=ax,
            node_size=[node_size_map[int(node)] * node_size_scale for node in graph_obj.nodes()],
            node_color=[category_colors[category_by_node[node]] for node in graph_obj.nodes()],
            linewidths=0.8,
            edgecolors="white",
        )
        for node in graph_obj.nodes():
            dx, dy = label_offsets_local.get(node, (8, 8))
            ax.annotate(
                format_label(node),
                xy=positions_local[node],
                xytext=(dx, dy),
                textcoords="offset points",
                fontsize=font_size,
                color="#1F1F1F",
                ha="left" if dx >= 0 else "right",
                va="center",
                bbox={"facecolor": "white", "alpha": label_alpha, "edgecolor": "none", "pad": 0.20},
                arrowprops={"arrowstyle": "-", "color": "#A9A9A9", "lw": 0.45, "alpha": 0.60, "shrinkA": 0, "shrinkB": 4},
                annotation_clip=False,
                zorder=5,
            )

        x_values_local = [coords[0] for coords in positions_local.values()]
        y_values_local = [coords[1] for coords in positions_local.values()]
        x_margin = max(3.5, 0.22 * (max(x_values_local) - min(x_values_local) + 1.0))
        y_margin = max(3.5, 0.22 * (max(y_values_local) - min(y_values_local) + 1.0))
        ax.set_xlim(min(x_values_local) - x_margin, max(x_values_local) + x_margin)
        ax.set_ylim(min(y_values_local) - y_margin, max(y_values_local) + y_margin)
        ax.margins(0.18)
        ax.axis("off")
        ax.set_title(title)

        legend_handles = [
            Rectangle((0, 0), 1, 1, facecolor=category_colors[category], edgecolor="white")
            for category in ordered_categories
        ]
        legend_labels = []
        for category in ordered_categories:
            display_label = category if len(category) <= 56 else f"{category[:53]}..."
            legend_labels.append(f"{display_label} ({category_counts[category]})")
        ax.legend(
            legend_handles,
            legend_labels,
            title=legend_title,
            loc="upper left",
            bbox_to_anchor=(1.01, 1.0),
            frameon=True,
            fontsize=9,
            title_fontsize=10,
        )
        fig.subplots_adjust(left=0.03, right=0.76, top=0.95, bottom=0.04)
        save_figure(plt, figures_dir / output_filename, use_tight_layout=False)

    def plot_matrix_views(graph_obj, layer_map: Dict[int, int]) -> None:
        if graph_obj.number_of_nodes() == 0:
            return
        ordered_nodes = get_ordered_nodes(graph_obj, layer_map)
        if not ordered_nodes:
            return

        short_labels = build_short_labels(ordered_nodes, max_len=36)

        adjacency_matrix = nx.to_numpy_array(graph_obj, nodelist=ordered_nodes, weight="weight", dtype=float)
        edges_local = list(graph_obj.edges())
        incidence_matrix = np.zeros((len(ordered_nodes), len(edges_local)), dtype=float)
        node_index = {node: idx for idx, node in enumerate(ordered_nodes)}
        for edge_index, (source, target) in enumerate(edges_local):
            incidence_matrix[node_index[source], edge_index] = -1.0
            incidence_matrix[node_index[target], edge_index] = 1.0

        fig, axes = plt.subplots(
            1,
            2,
            figsize=(26, 11),
            gridspec_kw={"width_ratios": [1.0, 1.35], "wspace": 0.18},
        )
        adjacency_ax, incidence_ax = axes

        adjacency_image = adjacency_ax.imshow(adjacency_matrix, cmap="Blues", interpolation="nearest", aspect="equal")
        adjacency_ax.set_title("Adjacency matrix of the dense internal-reference core")
        adjacency_ax.set_xticks(range(len(ordered_nodes)))
        adjacency_ax.set_yticks(range(len(ordered_nodes)))
        adjacency_ax.set_xticklabels(short_labels, rotation=90, fontsize=7)
        adjacency_ax.set_yticklabels(short_labels, fontsize=7)
        adjacency_ax.set_xlabel("Cited paper")
        adjacency_ax.set_ylabel("Citing paper")
        fig.colorbar(adjacency_image, ax=adjacency_ax, fraction=0.046, pad=0.02, label="Reference weight")

        incidence_image = incidence_ax.imshow(
            incidence_matrix,
            cmap="coolwarm",
            interpolation="nearest",
            aspect="auto",
            vmin=-1.0,
            vmax=1.0,
        )
        incidence_ax.set_title("Directed incidence matrix of the same dense core")
        incidence_ax.set_yticks(range(len(ordered_nodes)))
        incidence_ax.set_yticklabels(short_labels, fontsize=7)
        edge_labels = [f"e{idx + 1}" for idx in range(len(edges_local))]
        if len(edge_labels) <= 40:
            incidence_ax.set_xticks(range(len(edge_labels)))
            incidence_ax.set_xticklabels(edge_labels, rotation=90, fontsize=7)
        else:
            step = max(1, len(edge_labels) // 24)
            incidence_ax.set_xticks(list(range(0, len(edge_labels), step)))
            incidence_ax.set_xticklabels(edge_labels[::step], rotation=90, fontsize=7)
        incidence_ax.set_xlabel("Directed edge index (source=-1, target=+1)")
        incidence_ax.set_ylabel("Paper")
        fig.colorbar(incidence_image, ax=incidence_ax, fraction=0.046, pad=0.02, label="Incidence sign")

        fig.suptitle("Matrix views of the internal-reference dense core", y=0.985, fontsize=16)
        fig.subplots_adjust(left=0.09, right=0.98, top=0.92, bottom=0.17)
        save_figure(plt, figures_dir / "33_internal_reference_matrix_views.png", use_tight_layout=False)

    def plot_adjacency_matrix_figure(
        graph_obj,
        layer_map: Dict[int, int],
        *,
        output_filename: str,
        title: str,
    ) -> None:
        if graph_obj.number_of_nodes() == 0:
            return
        ordered_nodes = get_ordered_nodes(graph_obj, layer_map)
        if not ordered_nodes:
            return
        short_labels = build_short_labels(ordered_nodes, max_len=30)
        matrix = nx.to_numpy_array(graph_obj, nodelist=ordered_nodes, weight="weight", dtype=float)

        fig, ax = plt.subplots(figsize=(22, 20))
        image = ax.imshow(matrix, cmap="Blues", interpolation="nearest", aspect="equal")
        tick_step = 1 if len(ordered_nodes) <= 36 else max(1, len(ordered_nodes) // 36)
        tick_positions = list(range(0, len(ordered_nodes), tick_step))
        ax.set_xticks(tick_positions)
        ax.set_yticks(tick_positions)
        ax.set_xticklabels([short_labels[idx] for idx in tick_positions], rotation=90, fontsize=6)
        ax.set_yticklabels([short_labels[idx] for idx in tick_positions], fontsize=6)
        ax.set_xlabel("Cited paper")
        ax.set_ylabel("Citing paper")
        ax.set_title(title)
        fig.colorbar(image, ax=ax, fraction=0.028, pad=0.02, label="Reference weight")
        fig.subplots_adjust(left=0.18, right=0.96, top=0.94, bottom=0.22)
        save_figure(plt, figures_dir / output_filename, use_tight_layout=False)

    def plot_incidence_matrix_figure(
        graph_obj,
        layer_map: Dict[int, int],
        *,
        output_filename: str,
        title: str,
    ) -> None:
        if graph_obj.number_of_nodes() == 0 or graph_obj.number_of_edges() == 0:
            return
        ordered_nodes = get_ordered_nodes(graph_obj, layer_map)
        if not ordered_nodes:
            return
        short_labels = build_short_labels(ordered_nodes, max_len=30)
        edges_local = list(graph_obj.edges())
        incidence_matrix = np.zeros((len(ordered_nodes), len(edges_local)), dtype=float)
        node_index = {node: idx for idx, node in enumerate(ordered_nodes)}
        for edge_index, (source, target) in enumerate(edges_local):
            incidence_matrix[node_index[source], edge_index] = -1.0
            incidence_matrix[node_index[target], edge_index] = 1.0

        fig, ax = plt.subplots(figsize=(30, 16))
        image = ax.imshow(incidence_matrix, cmap="coolwarm", interpolation="nearest", aspect="auto", vmin=-1.0, vmax=1.0)
        y_tick_step = 1 if len(ordered_nodes) <= 36 else max(1, len(ordered_nodes) // 36)
        y_tick_positions = list(range(0, len(ordered_nodes), y_tick_step))
        ax.set_yticks(y_tick_positions)
        ax.set_yticklabels([short_labels[idx] for idx in y_tick_positions], fontsize=6)
        edge_labels = [f"e{idx + 1}" for idx in range(len(edges_local))]
        x_tick_step = 1 if len(edge_labels) <= 60 else max(1, len(edge_labels) // 48)
        x_tick_positions = list(range(0, len(edge_labels), x_tick_step))
        ax.set_xticks(x_tick_positions)
        ax.set_xticklabels([edge_labels[idx] for idx in x_tick_positions], rotation=90, fontsize=6)
        ax.set_xlabel("Directed edge index (source=-1, target=+1)")
        ax.set_ylabel("Paper")
        ax.set_title(title)
        fig.colorbar(image, ax=ax, fraction=0.028, pad=0.02, label="Incidence sign")
        fig.subplots_adjust(left=0.14, right=0.97, top=0.94, bottom=0.15)
        save_figure(plt, figures_dir / output_filename, use_tight_layout=False)

    active_nodes = [node for node in graph.nodes() if graph.degree(node) > 0]
    active_graph = graph.subgraph(active_nodes).copy()
    if active_graph.number_of_nodes() == 0:
        return

    min_layout_component_size = 5
    kept_layout_components = [
        component
        for component in nx.weakly_connected_components(active_graph)
        if len(component) >= min_layout_component_size
    ]
    if kept_layout_components:
        layout_nodes = sorted(set().union(*kept_layout_components))
        layout_graph = active_graph.subgraph(layout_nodes).copy()
    else:
        layout_graph = active_graph.copy()

    years = [layout_graph.nodes[node].get("year", -1) for node in layout_graph.nodes() if layout_graph.nodes[node].get("year", -1) >= 0]
    min_year = int(min(years)) if years else 0
    max_year = int(max(years)) if years else min_year

    positions, label_offsets = build_temporal_positions(
        layout_graph,
        seed_base=42,
        local_k_scale=5.4,
        iterations=500,
        component_gap=5.5,
        slot_step_small=2.3,
        slot_step_large=1.9,
        component_height_floor=20.0,
        component_height_pad=10.0,
        component_center_scale_factor=0.42,
        x_jitter_small=0.70,
        x_jitter_large=0.50,
        label_shift_x=8,
        label_shift_y=9,
    )

    pagerank_map = dict(zip(node_metrics_df["paper_id"], node_metrics_df["pagerank"]))
    in_degree_map = dict(zip(node_metrics_df["paper_id"], node_metrics_df["in_degree"]))
    pagerank_values = [float(pagerank_map.get(int(node), 0.0)) for node in layout_graph.nodes()]
    max_pagerank = max(pagerank_values) if pagerank_values else 0.0
    node_size_map = {
        int(node): 140 + (900 * (float(pagerank_map.get(int(node), 0.0)) / max_pagerank if max_pagerank > 0 else 0.25))
        for node in layout_graph.nodes()
    }
    node_color_map = {int(node): float(in_degree_map.get(int(node), 0)) for node in layout_graph.nodes()}
    edge_width_map = {
        (int(u), int(v)): 0.6 + 0.65 * float(layout_graph[u][v].get("weight", 1.0))
        for u, v in layout_graph.edges()
    }

    zoom_nodes = select_zoom_nodes(layout_graph)
    zoom_graph = layout_graph.subgraph(zoom_nodes).copy() if zoom_nodes else layout_graph.copy()
    force_positions, force_label_offsets = build_force_layout_positions(zoom_graph)
    hierarchical_positions, hierarchical_label_offsets, hierarchical_layers = build_sugiyama_like_positions(zoom_graph)
    circular_positions, circular_label_offsets = build_circular_positions(zoom_graph)

    fig = plt.figure(figsize=(40, 28))
    gs = fig.add_gridspec(
        2,
        5,
        width_ratios=[1.18, 1.18, 1.18, 0.10, 0.78],
        height_ratios=[1.0, 1.08],
        wspace=0.14,
        hspace=0.20,
    )
    overview_ax = fig.add_subplot(gs[0, 0:3])
    force_ax = fig.add_subplot(gs[1, 0])
    hierarchy_ax = fig.add_subplot(gs[1, 1])
    circular_ax = fig.add_subplot(gs[1, 2])
    cbar_ax = fig.add_subplot(gs[:, 3])
    metrics_ax = fig.add_subplot(gs[:, 4])

    top_overview_labels = (
        node_metrics_df.sort_values(["pagerank", "in_degree"], ascending=[False, False])["paper_id"].head(18).tolist()
        if node_metrics_df is not None and not node_metrics_df.empty
        else []
    )
    top_overview_labels = [node for node in top_overview_labels if node in layout_graph.nodes()]

    node_collection = draw_temporal_panel(
        overview_ax,
        layout_graph,
        positions,
        label_offsets,
        title="Overview of internal citation network",
        node_size_scale=0.95,
        font_size=5.7,
        edge_alpha=0.16,
        label_alpha=0.86,
        label_nodes=top_overview_labels,
    )

    if zoom_nodes:
        zoom_x = [positions[node][0] for node in zoom_nodes]
        zoom_y = [positions[node][1] for node in zoom_nodes]
        overview_ax.add_patch(
            Rectangle(
                (min(zoom_x) - 0.8, min(zoom_y) - 1.8),
                (max(zoom_x) - min(zoom_x)) + 1.6,
                (max(zoom_y) - min(zoom_y)) + 3.6,
                fill=False,
                linewidth=1.6,
                linestyle="--",
                edgecolor="#2F4B7C",
                alpha=0.9,
            )
        )
        nx.draw_networkx_nodes(
            layout_graph,
            positions,
            nodelist=zoom_nodes,
            ax=overview_ax,
            node_size=[node_size_map[int(node)] * 1.08 for node in zoom_nodes],
            node_color="none",
            edgecolors="#2F4B7C",
            linewidths=0.9,
        )

    draw_network_panel(
        force_ax,
        zoom_graph,
        force_positions,
        force_label_offsets,
        title="Dense core zoom: force-directed layout",
        node_size_scale=1.28,
        font_size=8.7,
        edge_alpha=0.34,
        label_alpha=0.92,
    )

    draw_network_panel(
        hierarchy_ax,
        zoom_graph,
        hierarchical_positions,
        hierarchical_label_offsets,
        title="Dense core zoom: hierarchical layout",
        node_size_scale=1.24,
        font_size=8.4,
        edge_alpha=0.32,
        label_alpha=0.92,
    )

    draw_network_panel(
        circular_ax,
        zoom_graph,
        circular_positions,
        circular_label_offsets,
        title="Dense core zoom: circular layout",
        node_size_scale=1.20,
        font_size=8.2,
        edge_alpha=0.30,
        label_alpha=0.90,
    )

    fig.colorbar(node_collection, cax=cbar_ax, label="Incoming citations inside the corpus")
    fig.suptitle("Internal citation network of papers present in the corpus", y=0.985, fontsize=16)

    metrics_lookup = dict(zip(metrics_df["metric"], metrics_df["value"]))
    summary_lines = [
        f"Active papers: {int(metrics_lookup.get('active_papers_in_reference_graph', 0))}",
        f"Edges: {int(metrics_lookup.get('internal_reference_edges', 0))}",
        f"Components: {int(metrics_lookup.get('weakly_connected_components', 0))}",
        f"Layout-filtered papers: {int(layout_graph.number_of_nodes())}",
        f"Layout-filtered components >= {min_layout_component_size}: {int(len(kept_layout_components))}",
        f"Density: {float(metrics_lookup.get('density', 0.0)):.4f}",
        f"Mean degree: {float(metrics_lookup.get('average_total_degree', 0.0)):.2f}",
        f"Reciprocity: {float(metrics_lookup.get('reciprocity', 0.0)):.3f}",
        f"Zoomed papers: {int(len(zoom_graph))}",
        "Zoom layouts:",
        "- Force-directed: local geometry",
        "- Hierarchical: citation flow",
        "- Circular: symmetry / hubs",
    ]
    if not node_metrics_df.empty:
        top_nodes = node_metrics_df.head(5)["display_label"].tolist()
        if top_nodes:
            summary_lines.append("Top PageRank:")
            summary_lines.extend(f"- {label}" for label in top_nodes)
    metrics_ax.axis("off")
    metrics_ax.text(
        0.0,
        1.0,
        "\n".join(summary_lines),
        transform=metrics_ax.transAxes,
        va="top",
        ha="left",
        fontsize=12,
        wrap=True,
        bbox={"facecolor": "white", "alpha": 0.95, "edgecolor": "#CCCCCC", "pad": 0.5},
    )
    fig.subplots_adjust(left=0.04, right=0.98, top=0.95, bottom=0.05, wspace=0.14, hspace=0.20)
    save_figure(plt, figures_dir / "32_internal_reference_network.png", use_tight_layout=False)
    plot_matrix_views(zoom_graph, hierarchical_layers)

    full_force_positions, full_force_label_offsets = build_force_layout_positions(
        layout_graph,
        scale_x=78.0,
        scale_y=64.0,
        label_dx=10,
        label_dy=9,
        iterations=2400,
    )
    full_hierarchical_positions, full_hierarchical_label_offsets, full_hierarchical_layers = build_sugiyama_like_positions(
        layout_graph,
        label_dx=9,
        label_dy=8,
        node_step_x=4.6,
        layer_step_y=6.8,
    )
    full_circular_positions, full_circular_label_offsets = build_circular_positions(
        layout_graph,
        radius_scale=0.62,
        min_radius=22.0,
        label_dx=10,
        label_dy=10,
    )

    plot_single_layout_figure(
        layout_graph,
        full_force_positions,
        full_force_label_offsets,
        output_filename="34_internal_reference_force_layout_full.png",
        title="Internal reference network: force-directed layout (components >= 5 papers)",
        node_size_scale=0.92,
        font_size=5.9,
        edge_alpha=0.20,
        label_alpha=0.80,
    )
    plot_categorical_layout_figure(
        layout_graph,
        full_force_positions,
        full_force_label_offsets,
        category_attr="cluster_label",
        output_filename="39_internal_reference_force_layout_full_identified_classes.png",
        title="Internal reference network: force-directed layout colored by identified classes",
        legend_title="Identified classes",
        unknown_label="classe identifiee inconnue",
        node_size_scale=0.92,
        font_size=5.9,
        edge_alpha=0.18,
        label_alpha=0.80,
    )
    plot_categorical_layout_figure(
        layout_graph,
        full_force_positions,
        full_force_label_offsets,
        category_attr="paper_class",
        output_filename="40_internal_reference_force_layout_full_imposed_classes.png",
        title="Internal reference network: force-directed layout colored by imposed classes",
        legend_title="Imposed classes",
        unknown_label="classe imposee inconnue",
        node_size_scale=0.92,
        font_size=5.9,
        edge_alpha=0.18,
        label_alpha=0.80,
    )
    plot_single_layout_figure(
        layout_graph,
        full_hierarchical_positions,
        full_hierarchical_label_offsets,
        output_filename="35_internal_reference_hierarchical_layout_full.png",
        title="Internal reference network: hierarchical layout (components >= 5 papers)",
        node_size_scale=0.92,
        font_size=5.8,
        edge_alpha=0.22,
        label_alpha=0.82,
    )
    plot_single_layout_figure(
        layout_graph,
        full_circular_positions,
        full_circular_label_offsets,
        output_filename="36_internal_reference_circular_layout_full.png",
        title="Internal reference network: circular layout (components >= 5 papers)",
        node_size_scale=0.90,
        font_size=5.6,
        edge_alpha=0.18,
        label_alpha=0.78,
    )
    plot_adjacency_matrix_figure(
        active_graph,
        full_hierarchical_layers,
        output_filename="37_internal_reference_adjacency_matrix_full.png",
        title="Internal reference adjacency matrix (full active graph)",
    )
    plot_incidence_matrix_figure(
        active_graph,
        full_hierarchical_layers,
        output_filename="38_internal_reference_incidence_matrix_full.png",
        title="Internal reference incidence matrix (full active graph)",
    )


def find_best_reference_title_match(reference_title: str, corpus_title_map: Dict[str, int], corpus_title_tokens: Dict[str, set]) -> Optional[int]:
    title_key = normalize_title_key(reference_title)
    if not title_key:
        return None
    if title_key in corpus_title_map:
        return corpus_title_map[title_key]

    ref_tokens = set(title_key.split())
    best_match = None
    best_score = 0.0
    for corpus_title_key, paper_id in corpus_title_map.items():
        token_overlap = len(ref_tokens & corpus_title_tokens[corpus_title_key])
        if token_overlap < 3:
            continue
        score = title_similarity(title_key, corpus_title_key)
        if score > best_score:
            best_score = score
            best_match = paper_id
    if best_score >= 0.92:
        return best_match
    return None


def build_internal_reference_graph(df, pd, nx, plt, figures_dir: Path, tables_dir: Path):
    doi_map = {}
    title_map = {}
    title_tokens = {}
    openalex_map = {}
    paper_lookup = {}

    for row in df.itertuples(index=False):
        paper_lookup[int(row.paper_id)] = row
        if clean_text(row.doi_norm):
            doi_map[normalize_doi(row.doi_norm)] = int(row.paper_id)
        if clean_text(row.title_key):
            title_map[row.title_key] = int(row.paper_id)
            title_tokens[row.title_key] = set(row.title_key.split())
        if clean_text(row.openalex_id):
            openalex_map[clean_text(row.openalex_id)] = int(row.paper_id)

    edge_map: Dict[Tuple[int, int], Dict[str, Any]] = {}
    matched_reference_rows = []

    for row in df.itertuples(index=False):
        source_id = int(row.paper_id)
        seen_targets = set()

        for openalex_ref in getattr(row, "referenced_openalex_ids", []) or []:
            target_id = openalex_map.get(clean_text(openalex_ref))
            if target_id is None or target_id == source_id:
                continue
            seen_targets.add((target_id, "openalex"))

        for ref in getattr(row, "reference_details", []) or []:
            target_id = None
            match_source = ""
            ref_doi = normalize_doi(ref.get("doi", ""))
            ref_title = clean_text(ref.get("title", ""))
            if ref_doi and ref_doi in doi_map:
                target_id = doi_map[ref_doi]
                match_source = "doi"
            elif ref_title:
                match = find_best_reference_title_match(ref_title, title_map, title_tokens)
                if match is not None:
                    target_id = match
                    match_source = "title"
            if target_id is None or target_id == source_id:
                continue
            seen_targets.add((int(target_id), match_source))

        for target_id, match_source in seen_targets:
            key = (source_id, int(target_id))
            target_row = paper_lookup[int(target_id)]
            shared_authors = sorted(set(getattr(row, "author_list", [])) & set(getattr(target_row, "author_list", [])))
            if key not in edge_map:
                source_year = int(row.year) if str(row.year) not in {"", "<NA>", "nan"} else None
                target_year = int(target_row.year) if str(target_row.year) not in {"", "<NA>", "nan"} else None
                edge_map[key] = {
                    "source_paper_id": source_id,
                    "target_paper_id": int(target_id),
                    "match_sources": set(),
                    "source_year": source_year,
                    "target_year": target_year,
                    "source_cluster": int(getattr(row, "cluster", -1)) if hasattr(row, "cluster") else None,
                    "target_cluster": int(getattr(target_row, "cluster", -1)) if hasattr(target_row, "cluster") else None,
                    "source_cluster_label": clean_text(getattr(row, "cluster_label", "")),
                    "target_cluster_label": clean_text(getattr(target_row, "cluster_label", "")),
                    "source_class": clean_text(getattr(row, "primary_class_display", "")),
                    "target_class": clean_text(getattr(target_row, "primary_class_display", "")),
                    "source_journal": clean_text(getattr(row, "journal_clean", "")),
                    "target_journal": clean_text(getattr(target_row, "journal_clean", "")),
                    "source_title": clean_text(getattr(row, "title", "")),
                    "target_title": clean_text(getattr(target_row, "title", "")),
                    "source_display_label": format_paper_node_label(getattr(row, "author_list", []), getattr(row, "authors", ""), source_year),
                    "target_display_label": format_paper_node_label(
                        getattr(target_row, "author_list", []),
                        getattr(target_row, "authors", ""),
                        target_year,
                    ),
                    "shared_authors": shared_authors,
                    "shared_author_count": len(shared_authors),
                    "reference_matches": 0,
                }
            edge_map[key]["match_sources"].add(match_source)
            edge_map[key]["reference_matches"] += 1
            matched_reference_rows.append(
                {
                    "source_paper_id": source_id,
                    "target_paper_id": int(target_id),
                    "match_source": match_source,
                    "source_title": clean_text(getattr(row, "title", "")),
                    "target_title": clean_text(getattr(target_row, "title", "")),
                }
            )

    edge_rows = []
    for edge in edge_map.values():
        source_year = edge["source_year"]
        target_year = edge["target_year"]
        edge["year_lag"] = int(source_year - target_year) if source_year is not None and target_year is not None else None
        edge["match_sources"] = ", ".join(sorted(edge["match_sources"]))
        edge["shared_authors"] = "; ".join(edge["shared_authors"])
        edge_rows.append(edge)

    edges_df = pd.DataFrame(edge_rows).sort_values(
        ["reference_matches", "source_paper_id", "target_paper_id"], ascending=[False, True, True]
    ) if edge_rows else pd.DataFrame(
        columns=[
            "source_paper_id",
            "target_paper_id",
            "match_sources",
            "source_year",
            "target_year",
            "source_cluster",
            "target_cluster",
            "source_cluster_label",
            "target_cluster_label",
            "source_class",
            "target_class",
            "source_journal",
            "target_journal",
            "source_title",
            "target_title",
            "source_display_label",
            "target_display_label",
            "shared_authors",
            "shared_author_count",
            "reference_matches",
            "year_lag",
        ]
    )
    matched_references_df = pd.DataFrame(matched_reference_rows)
    edges_df.to_csv(tables_dir / "internal_reference_edges.csv", index=False)
    matched_references_df.to_csv(tables_dir / "internal_reference_matches.csv", index=False)

    graph = nx.DiGraph()
    for row in df.itertuples(index=False):
        row_year = int(row.year) if str(row.year) not in {"", "<NA>", "nan"} else -1
        graph.add_node(
            int(row.paper_id),
            title=clean_text(row.title),
            year=row_year,
            cluster=int(getattr(row, "cluster", -1)) if hasattr(row, "cluster") else -1,
            cluster_label=clean_text(getattr(row, "cluster_label", "")),
            paper_class=clean_text(getattr(row, "primary_class_display", "")),
            journal=clean_text(getattr(row, "journal_clean", "")),
            display_label=format_paper_node_label(getattr(row, "author_list", []), getattr(row, "authors", ""), None if row_year < 0 else row_year),
        )
    for _, edge in edges_df.iterrows():
        graph.add_edge(
            int(edge["source_paper_id"]),
            int(edge["target_paper_id"]),
            weight=float(edge["reference_matches"]),
            match_sources=edge["match_sources"],
        )

    node_rows = []
    for node in graph.nodes():
        node_rows.append(
            {
                "paper_id": int(node),
                "title": graph.nodes[node].get("title", ""),
                "year": int(graph.nodes[node].get("year", -1)),
                "cluster": int(graph.nodes[node].get("cluster", -1)),
                "cluster_label": graph.nodes[node].get("cluster_label", ""),
                "paper_class": graph.nodes[node].get("paper_class", ""),
                "journal": graph.nodes[node].get("journal", ""),
                "display_label": graph.nodes[node].get("display_label", ""),
                "in_degree": int(graph.in_degree(node)),
                "out_degree": int(graph.out_degree(node)),
            }
        )
    nodes_df = pd.DataFrame(node_rows)
    nodes_df.to_csv(tables_dir / "internal_reference_nodes.csv", index=False)

    graph_metrics_df, node_metrics_df = compute_internal_reference_graph_metrics(pd, nx, graph)
    graph_metrics_df.to_csv(tables_dir / "internal_reference_graph_metrics.csv", index=False)
    node_metrics_df.to_csv(tables_dir / "internal_reference_node_metrics.csv", index=False)

    summary_df = pd.DataFrame(
        [
            {"metric": "papers_in_corpus", "value": int(len(df))},
            {"metric": "papers_with_metadata", "value": int((df["metadata_status"] == "ok").sum())},
            {"metric": "papers_with_internal_outgoing_links", "value": int((nodes_df["out_degree"] > 0).sum()) if not nodes_df.empty else 0},
            {"metric": "papers_with_internal_incoming_links", "value": int((nodes_df["in_degree"] > 0).sum()) if not nodes_df.empty else 0},
            {"metric": "self_citation_like_edges_shared_author_gt_0", "value": int((edges_df["shared_author_count"] > 0).sum()) if not edges_df.empty else 0},
        ]
    )
    summary_df = pd.concat([summary_df, graph_metrics_df], ignore_index=True)
    summary_df.to_csv(tables_dir / "internal_reference_summary.csv", index=False)

    if edges_df.empty:
        write_empty_csv(pd, tables_dir / "reference_time_lag_distribution.csv", ["year_lag", "citation_count"])
        write_empty_csv(pd, tables_dir / "reference_class_transitions.csv", ["source_class", "target_class", "citation_count"])
        write_empty_csv(
            pd,
            tables_dir / "reference_cluster_transitions.csv",
            ["source_cluster", "target_cluster", "source_cluster_label", "target_cluster_label", "citation_count"],
        )
        write_empty_csv(pd, tables_dir / "reference_author_overlap_distribution.csv", ["shared_author_count", "edge_count"])
        return graph, nodes_df, edges_df, summary_df

    lag_df = edges_df.dropna(subset=["year_lag"]).groupby("year_lag").size().reset_index(name="citation_count").sort_values("year_lag")
    lag_df.to_csv(tables_dir / "reference_time_lag_distribution.csv", index=False)

    class_transitions_df = (
        edges_df.groupby(["source_class", "target_class"]).size().reset_index(name="citation_count").sort_values(
            ["citation_count", "source_class", "target_class"], ascending=[False, True, True]
        )
    )
    class_transitions_df.to_csv(tables_dir / "reference_class_transitions.csv", index=False)

    cluster_transitions_df = (
        edges_df.groupby(["source_cluster", "target_cluster", "source_cluster_label", "target_cluster_label"])
        .size()
        .reset_index(name="citation_count")
        .sort_values(
            ["citation_count", "source_cluster_label", "target_cluster_label"],
            ascending=[False, True, True],
        )
    )
    cluster_transitions_df.to_csv(tables_dir / "reference_cluster_transitions.csv", index=False)

    author_overlap_df = edges_df.groupby("shared_author_count").size().reset_index(name="edge_count").sort_values("shared_author_count")
    author_overlap_df.to_csv(tables_dir / "reference_author_overlap_distribution.csv", index=False)
    edges_df[edges_df["shared_author_count"] > 0].to_csv(tables_dir / "reference_edges_with_shared_authors.csv", index=False)

    class_pivot = class_transitions_df.pivot(index="source_class", columns="target_class", values="citation_count").fillna(0)
    if not class_pivot.empty:
        plt.figure(figsize=(10, 8))
        im = plt.imshow(class_pivot.to_numpy(), aspect="auto", cmap="OrRd", interpolation="nearest")
        plt.colorbar(im, label="Internal reference count")
        plt.yticks(range(len(class_pivot.index)), class_pivot.index.tolist())
        plt.xticks(range(len(class_pivot.columns)), class_pivot.columns.tolist(), rotation=45, ha="right")
        plt.xlabel("Cited paper class")
        plt.ylabel("Citing paper class")
        plt.title("Method/class transitions inside the corpus")
        save_figure(plt, figures_dir / "26_reference_class_transition_heatmap.png")

    plot_reference_cluster_transition_heatmap(plt, pd, cluster_transitions_df, figures_dir)
    plot_internal_reference_network(plt, nx, graph, node_metrics_df, graph_metrics_df, figures_dir)
    return graph, nodes_df, edges_df, summary_df


def annotate_dataframe_with_reference_links(df, pd, edges_df):
    df = df.copy()
    default_values = {
        "references_in_corpus_count": 0,
        "references_in_corpus_paper_ids": "",
        "references_in_corpus_titles": "",
        "references_in_corpus_match_sources": "",
        "references_in_corpus_match_count": 0,
        "cited_by_corpus_count": 0,
        "cited_by_corpus_paper_ids": "",
        "cited_by_corpus_titles": "",
    }
    for column, default_value in default_values.items():
        df[column] = default_value

    if edges_df is not None and not edges_df.empty:
        for source_paper_id, group in edges_df.groupby("source_paper_id"):
            target_ids = sorted({int(value) for value in group["target_paper_id"].tolist()})
            mask = df["paper_id"] == int(source_paper_id)
            df.loc[mask, "references_in_corpus_count"] = len(target_ids)
            df.loc[mask, "references_in_corpus_paper_ids"] = "; ".join(str(value) for value in target_ids)
            df.loc[mask, "references_in_corpus_titles"] = "; ".join(unique_preserve_order(group["target_title"].tolist()))
            df.loc[mask, "references_in_corpus_match_sources"] = "; ".join(unique_preserve_order(group["match_sources"].tolist()))
            df.loc[mask, "references_in_corpus_match_count"] = int(group["reference_matches"].sum())

        for target_paper_id, group in edges_df.groupby("target_paper_id"):
            source_ids = sorted({int(value) for value in group["source_paper_id"].tolist()})
            mask = df["paper_id"] == int(target_paper_id)
            df.loc[mask, "cited_by_corpus_count"] = len(source_ids)
            df.loc[mask, "cited_by_corpus_paper_ids"] = "; ".join(str(value) for value in source_ids)
            df.loc[mask, "cited_by_corpus_titles"] = "; ".join(unique_preserve_order(group["source_title"].tolist()))

    integer_columns = [
        "reference_count",
        "crossref_reference_count",
        "openalex_reference_count",
        "references_in_corpus_count",
        "references_in_corpus_match_count",
        "cited_by_corpus_count",
    ]
    for column in integer_columns:
        if column not in df.columns:
            df[column] = 0
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    return df


def save_enriched_dataset(df, output_path: Path) -> None:
    export_df = df.copy()
    for list_col in ["author_list", "institution_list", "team_lab_list", "team_city_list", "referenced_openalex_ids"]:
        if list_col in export_df.columns:
            export_df[list_col] = export_df[list_col].map(to_semicolon_string)
    if "reference_details" in export_df.columns:
        export_df["reference_details"] = export_df["reference_details"].map(lambda value: json.dumps(value, ensure_ascii=True))
    export_df.to_csv(output_path, sep=";", index=False, encoding="utf-8")


def save_enriched_excel(df, output_path: Path) -> None:
    export_df = df.copy()
    for list_col in ["author_list", "institution_list", "team_lab_list", "team_city_list", "referenced_openalex_ids"]:
        if list_col in export_df.columns:
            export_df[list_col] = export_df[list_col].map(to_semicolon_string)
    preferred_columns = [
        "title",
        "authors",
        "journal",
        "published_date",
        "doi",
        "url",
        "issn",
        "abstract",
        "relevance",
        "n_authors",
        "institution_list",
        "reference_count",
        "references_in_corpus_count",
        "references_in_corpus_paper_ids",
        "paper_id",
        "cited_by_corpus_count",
        "cited_by_corpus_titles",
        "primary_class",
        "primary_class_display",
        "cluster",
        "cluster_label",
    ]
    for column in preferred_columns:
        if column not in export_df.columns:
            export_df[column] = ""
    export_df = export_df[preferred_columns]
    export_df.to_excel(output_path, index=False)


def generate_markdown_report(report_payload: Dict[str, Any], output_report_path: Path) -> None:
    df = report_payload["df"]
    quality_info = report_payload["quality_info"]
    author_counts = report_payload["author_counts"]
    journal_total_df = report_payload["journal_total_df"]
    keyword_total_df = report_payload["keyword_total_df"]
    cluster_summary_df = report_payload["cluster_summary_df"]
    class_summary_df = report_payload["class_summary_df"]
    team_total_df = report_payload["team_total_df"]
    metadata_summary_df = report_payload["metadata_summary_df"]
    reference_summary_df = report_payload["reference_summary_df"]
    input_path = report_payload["input_path"]

    total = len(df)
    dated = df[df["year"].notna()]
    min_year = int(dated["year"].min()) if not dated.empty else None
    max_year = int(dated["year"].max()) if not dated.empty else None
    abstract_rate = float(df["has_abstract"].mean() * 100.0) if total else 0.0
    unique_journals = int(df["journal_clean"].nunique()) if total else 0
    unique_authors = int(len(author_counts))

    lines: List[str] = []
    lines.append("# Rapport bibliometrique")
    lines.append("")
    lines.append("## Corpus")
    lines.append(f"- Fichier source: `{input_path.name}`")
    lines.append(f"- Papiers analyses: **{total}**")
    if min_year is not None and max_year is not None:
        lines.append(f"- Couverture temporelle: **{min_year} -> {max_year}**")
    lines.append(f"- Journaux uniques: **{unique_journals}**")
    lines.append(f"- Auteurs uniques parses: **{unique_authors}**")
    lines.append(f"- Couverture des abstracts: **{abstract_rate:.1f}%**")
    lines.append("")
    lines.append("## Qualite des donnees")
    for key, value in quality_info.items():
        lines.append(f"- {key}: **{value}**")
    lines.append("")
    lines.append("## Journaux")
    if journal_total_df is not None and not journal_total_df.empty:
        for _, row in journal_total_df.head(8).iterrows():
            lines.append(f"- {row['journal']}: {int(row['paper_count'])} papiers")
    else:
        lines.append("- Aucun journal exploitable.")
    lines.append("")
    lines.append("## Auteurs")
    if author_counts is not None and len(author_counts) > 0:
        for author, count in author_counts.head(8).items():
            lines.append(f"- {author}: {int(count)} papiers")
    else:
        lines.append("- Aucun auteur exploitable.")
    lines.append("")
    lines.append("## Equipes / laboratoires")
    if team_total_df is not None and not team_total_df.empty:
        for _, row in team_total_df.head(8).iterrows():
            lines.append(f"- {row['team_lab']}: {int(row['paper_count'])} papiers")
    else:
        lines.append("- Non disponibles sans metadonnees externes ou affiliations exploitables.")
    lines.append("")
    lines.append("## Mots cles")
    if keyword_total_df is not None and not keyword_total_df.empty:
        for _, row in keyword_total_df.head(12).iterrows():
            lines.append(f"- {row['term']}: {int(row['frequency'])}")
    else:
        lines.append("- Aucun mot cle exploitable.")
    lines.append("")
    lines.append("## Clusters thematiques")
    if cluster_summary_df is not None and not cluster_summary_df.empty:
        for _, row in cluster_summary_df.head(8).iterrows():
            cluster_label = row["cluster_label"] if clean_text(row.get("cluster_label", "")) else f"Theme {row['cluster']}"
            lines.append(
                f"- {cluster_label} ({int(row['paper_count'])} papiers): "
                f"mots cles = {row.get('top_keywords', '') or row.get('cluster_top_terms', '')}; "
                f"auteurs = {row.get('top_authors', '')}; journaux = {row.get('top_journals', '')}"
            )
    else:
        lines.append("- Aucun cluster disponible.")
    lines.append("")
    lines.append("## Classification des papiers")
    if class_summary_df is not None and not class_summary_df.empty:
        for _, row in class_summary_df.iterrows():
            lines.append(
                f"- {row['primary_class_display']} ({int(row['paper_count'])} papiers): "
                f"auteurs = {row.get('top_authors', '')}; journaux = {row.get('top_journals', '')}"
            )
    else:
        lines.append("- Aucune classification disponible.")
    lines.append("")
    lines.append("## Metadonnees externes")
    if metadata_summary_df is not None and not metadata_summary_df.empty:
        for _, row in metadata_summary_df.iterrows():
            lines.append(f"- {row['metadata_status']}: {int(row['paper_count'])} papiers")
    else:
        lines.append("- Non executees.")
    lines.append("")
    lines.append("## References internes au corpus")
    if reference_summary_df is not None and not reference_summary_df.empty:
        for _, row in reference_summary_df.iterrows():
            lines.append(f"- {row['metric']}: {row['value']}")
    else:
        lines.append("- Aucun lien interne detecte.")
    lines.append("")
    lines.append("## Notes")
    lines.append("- Les equipes/laboratoires sont inferes a partir des affiliations externes quand elles sont disponibles.")
    lines.append("- Les liens de references internes combinent OpenAlex, DOI exacts et rapprochement prudent par titre.")
    lines.append("- La classification est reglee par des regles de vocabulaire; elle doit etre relue sur les cas limites.")
    lines.append("")
    output_report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    base_dir = Path.cwd()
    if args.input:
        input_path = Path(args.input).expanduser().resolve()
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
    else:
        input_path = find_default_input(base_dir)
    output_dir = Path(args.output_dir).resolve()

    log(f"Input file: {input_path}")
    log(f"Output directory: {output_dir}")

    ensure_dependencies(input_path)
    deps = import_runtime_dependencies()
    np = deps["np"]
    pd = deps["pd"]
    plt = deps["plt"]
    nx = deps["nx"]
    MiniBatchKMeans = deps["MiniBatchKMeans"]
    TruncatedSVD = deps["TruncatedSVD"]
    CountVectorizer = deps["CountVectorizer"]
    TfidfVectorizer = deps["TfidfVectorizer"]
    ENGLISH_STOP_WORDS = deps["ENGLISH_STOP_WORDS"]
    silhouette_score = deps["silhouette_score"]
    NearestNeighbors = deps["NearestNeighbors"]

    plt.style.use("ggplot")
    out_dirs = ensure_output_dirs(output_dir)
    remove_stale_figure_outputs(out_dirs["figures"])
    cache_path = Path(args.metadata_cache).resolve() if args.metadata_cache else out_dirs["base"] / "metadata_cache.json"
    author_counts = pd.Series(dtype=int)
    journal_total_df = pd.DataFrame(columns=["journal", "paper_count"])
    keyword_total_df = pd.DataFrame(columns=["term", "frequency", "document_frequency"])
    cluster_summary_df = pd.DataFrame(columns=["cluster", "cluster_label", "paper_count"])
    class_summary_df = pd.DataFrame(columns=["primary_class_display", "paper_count"])
    team_total_df = pd.DataFrame(columns=["team_lab", "paper_count"])
    metadata_summary_df = pd.DataFrame(columns=["metadata_status", "paper_count"])
    reference_summary_df = pd.DataFrame(columns=["metric", "value"])
    keyword_matrix = None
    keyword_terms = None
    tfidf_matrix = None
    sample_idx: List[int] = []

    log("Loading dataset...")
    raw_df = load_dataset(pd, input_path)
    log(f"Raw rows: {len(raw_df)}")

    log("Preparing and cleaning metadata...")
    df, quality_info = prepare_dataframe(pd, raw_df)
    log(f"Rows after cleaning: {len(df)}")
    write_data_quality_tables(pd, df, out_dirs["tables"], quality_info)

    if RUN_TIMELINE_ANALYSIS:
        log("Building timeline plots...")
        plot_publication_timeline(df, pd, plt, out_dirs["figures"], out_dirs["tables"], rolling_window=args.rolling_window)
    else:
        log("Timeline analysis disabled in script config.")

    if RUN_JOURNAL_ANALYSIS:
        log("Building journal tables and plots...")
        journal_total_df, journal_by_year_df, journal_pivot_df = plot_journal_statistics(
            df=df,
            pd=pd,
            np=np,
            plt=plt,
            figures_dir=out_dirs["figures"],
            tables_dir=out_dirs["tables"],
            top_n=args.top_n,
        )
    else:
        log("Journal analysis disabled in script config.")

    if RUN_AUTHOR_ANALYSIS:
        log("Building author tables and plots...")
        _, author_counts = plot_author_statistics(
            df=df,
            pd=pd,
            plt=plt,
            figures_dir=out_dirs["figures"],
            tables_dir=out_dirs["tables"],
            top_n=args.top_n,
        )
    else:
        log("Author analysis disabled in script config.")

    if RUN_KEYWORD_ANALYSIS:
        log("Computing keyword statistics...")
        keyword_vectorizer, keyword_matrix, keyword_terms, keyword_total_df, _ = compute_keyword_statistics(
            df=df,
            pd=pd,
            np=np,
            CountVectorizer=CountVectorizer,
            ENGLISH_STOP_WORDS=ENGLISH_STOP_WORDS,
            plt=plt,
            figures_dir=out_dirs["figures"],
            tables_dir=out_dirs["tables"],
        )
    else:
        log("Keyword analysis disabled in script config.")
        keyword_vectorizer = None

    if RUN_CLASSIFICATION_ANALYSIS:
        log("Classifying papers...")
        (
            df,
            classification_scores_df,
            class_total_df,
            class_by_year_df,
            class_author_counts_df,
            class_journal_counts_df,
        ) = classify_papers(df, pd, plt, out_dirs["figures"], out_dirs["tables"])
    else:
        log("Classification analysis disabled in script config.")
        df["primary_class"] = "autre_non_classe"
        df["primary_class_display"] = UNCLASSIFIED_LABEL

    if RUN_CLUSTERING_ANALYSIS:
        log("Running thematic clustering...")
        df, tfidf_matrix, _, _, cluster_scores_df, cluster_terms_df = cluster_documents(
            df=df,
            pd=pd,
            np=np,
            TfidfVectorizer=TfidfVectorizer,
            MiniBatchKMeans=MiniBatchKMeans,
            TruncatedSVD=TruncatedSVD,
            silhouette_score=silhouette_score,
            plt=plt,
            figures_dir=out_dirs["figures"],
            tables_dir=out_dirs["tables"],
            random_state=args.random_state,
            k_min=args.k_min,
            k_max=args.k_max,
        )
    else:
        log("Clustering analysis disabled in script config.")
        df["cluster"] = -1
        df["cluster_label"] = "clustering desactive"
        cluster_terms_df = pd.DataFrame(columns=["cluster", "paper_count", "top_terms"])

    if RUN_CLUSTERING_ANALYSIS:
        log("Building cluster summaries...")
        (
            cluster_total_df,
            cluster_by_year_df,
            cluster_pivot_df,
            cluster_journal_counts_df,
            cluster_author_counts_df,
            cluster_keyword_profiles_df,
            cluster_summary_df,
        ) = build_group_profiles(
            df=df,
            pd=pd,
            np=np,
            group_col="cluster",
            keyword_matrix=keyword_matrix,
            keyword_terms=keyword_terms,
            top_n=args.top_n,
        )
        cluster_labels_df = build_cluster_labels(pd, cluster_summary_df, cluster_keyword_profiles_df, cluster_terms_df)
        cluster_label_map = dict(zip(cluster_labels_df["cluster"], cluster_labels_df["cluster_label"]))
        df["cluster_label"] = df["cluster"].map(cluster_label_map).fillna(df["cluster"].map(lambda value: f"Theme {value}"))
        cluster_total_df["cluster_label"] = cluster_total_df["cluster"].map(cluster_label_map)
        cluster_by_year_df["cluster_label"] = cluster_by_year_df["cluster"].map(cluster_label_map)
        cluster_journal_counts_df["cluster_label"] = cluster_journal_counts_df["cluster"].map(cluster_label_map)
        cluster_author_counts_df["cluster_label"] = cluster_author_counts_df["cluster"].map(cluster_label_map)
        cluster_keyword_profiles_df["cluster_label"] = cluster_keyword_profiles_df["cluster"].map(cluster_label_map)
        if cluster_terms_df is not None and not cluster_terms_df.empty:
            cluster_summary_df = cluster_summary_df.merge(cluster_terms_df, on=["cluster", "paper_count"], how="left")
        cluster_summary_df = cluster_summary_df.merge(cluster_labels_df, on="cluster", how="left")
        cluster_total_df.to_csv(out_dirs["tables"] / "cluster_total_counts.csv", index=False)
        cluster_by_year_df.to_csv(out_dirs["tables"] / "cluster_by_year_long.csv", index=False)
        cluster_pivot_df.to_csv(out_dirs["tables"] / "cluster_by_year_pivot.csv")
        cluster_pivot_df.rename(columns=cluster_label_map).to_csv(out_dirs["tables"] / "cluster_by_year_pivot_named.csv")
        cluster_journal_counts_df.to_csv(out_dirs["tables"] / "cluster_journal_counts.csv", index=False)
        cluster_author_counts_df.to_csv(out_dirs["tables"] / "cluster_author_counts.csv", index=False)
        cluster_keyword_profiles_df.to_csv(out_dirs["tables"] / "cluster_keyword_profiles.csv", index=False)
        cluster_labels_df.to_csv(out_dirs["tables"] / "cluster_labels.csv", index=False)
        cluster_summary_df.to_csv(out_dirs["tables"] / "cluster_summary.csv", index=False)
        plot_cluster_keyword_clouds(plt, np, cluster_keyword_profiles_df, cluster_labels_df, out_dirs["figures"])
        plot_cluster_summary_figures(
            plt,
            cluster_total_df=cluster_total_df,
            cluster_pivot_df=cluster_pivot_df.rename(columns=cluster_label_map),
            figures_dir=out_dirs["figures"],
        )
    else:
        log("Cluster summaries disabled because clustering is disabled.")

    if RUN_CLASSIFICATION_ANALYSIS:
        log("Building classification summaries...")
        (
            class_profile_total_df,
            class_profile_by_year_df,
            class_profile_pivot_df,
            class_profile_journal_counts_df,
            class_profile_author_counts_df,
            class_keyword_profiles_df,
            class_summary_df,
        ) = build_group_profiles(
            df=df,
            pd=pd,
            np=np,
            group_col="primary_class_display",
            keyword_matrix=keyword_matrix,
            keyword_terms=keyword_terms,
            top_n=args.top_n,
        )
        class_profile_total_df.to_csv(out_dirs["tables"] / "classification_total_counts_detailed.csv", index=False)
        class_profile_by_year_df.to_csv(out_dirs["tables"] / "classification_by_year_long.csv", index=False)
        class_profile_pivot_df.to_csv(out_dirs["tables"] / "classification_by_year_pivot_detailed.csv")
        class_profile_journal_counts_df.to_csv(out_dirs["tables"] / "classification_journal_counts_detailed.csv", index=False)
        class_profile_author_counts_df.to_csv(out_dirs["tables"] / "classification_author_counts_detailed.csv", index=False)
        class_keyword_profiles_df.to_csv(out_dirs["tables"] / "classification_keyword_profiles.csv", index=False)
        class_summary_df.to_csv(out_dirs["tables"] / "classification_summary.csv", index=False)
        plot_group_totals(
            plt,
            class_profile_total_df.rename(columns={"primary_class_display": "paper_class"}),
            group_col="paper_class",
            output_path=out_dirs["figures"] / "30_class_totals_detailed.png",
            title="Classification totals",
            color="#AF7AA1",
        )
        plot_group_evolution(
            plt,
            class_profile_pivot_df,
            output_path=out_dirs["figures"] / "31_class_evolution_counts.png",
            title="Classification counts over time",
        )
    else:
        log("Classification summaries disabled because classification is disabled.")

    if RUN_CLUSTERING_ANALYSIS:
        plot_group_totals(
            plt,
            cluster_total_df.rename(columns={"cluster_label": "cluster_name"}),
            group_col="cluster_name",
            output_path=out_dirs["figures"] / "28_cluster_totals.png",
            title="Cluster totals",
            color="#4E79A7",
        )
        plot_group_evolution(
            plt,
            cluster_pivot_df.rename(columns=cluster_label_map),
            output_path=out_dirs["figures"] / "29_cluster_evolution_counts.png",
            title="Cluster counts over time",
        )

    if RUN_EXTERNAL_METADATA_ENRICHMENT:
        log("Enriching with external metadata for affiliations and references...")
        df, metadata_df, metadata_summary_df = enrich_with_external_metadata(
            df=df,
            pd=pd,
            tables_dir=out_dirs["tables"],
            cache_path=cache_path,
            enabled=True,
            timeout=args.metadata_timeout,
            pause=args.metadata_request_pause,
        )
    else:
        log("External metadata enrichment disabled in script config.")

    if RUN_TEAM_LAB_ANALYSIS:
        log("Building institution/team-lab statistics...")
        institution_total_df, institution_by_year_df, team_total_df, team_by_year_df = plot_team_lab_statistics(
            df=df,
            pd=pd,
            plt=plt,
            figures_dir=out_dirs["figures"],
            tables_dir=out_dirs["tables"],
            top_n=args.top_n,
        )
    else:
        log("Institution/team-lab analysis disabled in script config.")

    if RUN_COAUTHOR_NETWORK:
        log("Building coauthorship network...")
        coauthor_graph, coauthor_nodes_df, coauthor_edges_df = build_coauthor_network(
            df=df,
            pd=pd,
            nx=nx,
            min_author_occurrences=args.min_author_occurrences,
            max_authors_per_paper=args.max_authors_per_paper,
        )
        coauthor_nodes_df.to_csv(out_dirs["networks"] / "coauthor_nodes.csv", index=False)
        coauthor_edges_df.to_csv(out_dirs["networks"] / "coauthor_edges.csv", index=False)
        if coauthor_graph.number_of_nodes() > 0:
            nx.write_gexf(coauthor_graph, out_dirs["networks"] / "coauthor_network.gexf")
        coauthor_centrality_df = compute_coauthor_centrality(nx, pd, coauthor_graph)
        coauthor_centrality_df.to_csv(out_dirs["tables"] / "coauthor_centrality.csv", index=False)
        if EXPORT_SUPPLEMENTARY_FIGURES:
            plot_coauthor_network(nx, np, plt, coauthor_graph, out_dirs["figures"])
    else:
        log("Coauthorship network disabled in script config.")

    if RUN_PAPER_SIMILARITY_NETWORK and tfidf_matrix is not None:
        log("Building paper similarity network...")
        paper_graph, paper_nodes_df, paper_edges_df, sample_idx = build_paper_similarity_network(
            df=df,
            pd=pd,
            np=np,
            nx=nx,
            NearestNeighbors=NearestNeighbors,
            tfidf_matrix=tfidf_matrix,
            max_docs=args.max_paper_network_docs,
            neighbors=args.neighbors,
            threshold=args.paper_sim_threshold,
            random_state=args.random_state,
        )
        paper_nodes_df.to_csv(out_dirs["networks"] / "paper_similarity_nodes.csv", index=False)
        paper_edges_df.to_csv(out_dirs["networks"] / "paper_similarity_edges.csv", index=False)
        if paper_graph.number_of_nodes() > 0:
            nx.write_gexf(paper_graph, out_dirs["networks"] / "paper_similarity_network.gexf")
        if EXPORT_SUPPLEMENTARY_FIGURES:
            plot_paper_similarity_network(nx, plt, np, paper_graph, out_dirs["figures"])
    else:
        log("Paper similarity network disabled in script config.")

    if RUN_INTERNAL_REFERENCE_ANALYSIS:
        log("Building internal reference graph...")
        reference_graph, reference_nodes_df, reference_edges_df, reference_summary_df = build_internal_reference_graph(
            df=df,
            pd=pd,
            nx=nx,
            plt=plt,
            figures_dir=out_dirs["figures"],
            tables_dir=out_dirs["tables"],
        )
        if reference_graph.number_of_nodes() > 0:
            nx.write_gexf(reference_graph, out_dirs["networks"] / "internal_reference_graph.gexf")
        df = annotate_dataframe_with_reference_links(df, pd, reference_edges_df)
    else:
        log("Internal reference analysis disabled in script config.")

    log("Saving enriched dataset...")
    save_enriched_dataset(df, out_dirs["tables"] / "cleaned_articles_with_analysis.csv")
    save_enriched_excel(df, out_dirs["base"] / f"{input_path.stem}_enriched.xlsx")

    log("Writing markdown report...")
    generate_markdown_report(
        report_payload={
            "df": df,
            "quality_info": quality_info,
            "author_counts": author_counts,
            "journal_total_df": journal_total_df,
            "keyword_total_df": keyword_total_df,
            "cluster_summary_df": cluster_summary_df,
            "class_summary_df": class_summary_df,
            "team_total_df": team_total_df,
            "metadata_summary_df": metadata_summary_df,
            "reference_summary_df": reference_summary_df,
            "input_path": input_path,
        },
        output_report_path=out_dirs["base"] / "review_report.md",
    )

    log("Done.")
    log(f"Figures: {out_dirs['figures']}")
    log(f"Tables: {out_dirs['tables']}")
    log(f"Networks: {out_dirs['networks']}")
    log(f"Paper network sample size: {len(sample_idx)}")


if __name__ == "__main__":
    main()
