#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Independent DOI list enrichment script.

Reads a DOI list from txt/csv/xlsx, fetches metadata from Crossref, optionally
completes abstracts from PubMed, and writes the result to an Excel file.

The script preserves all input rows:
- no row deletion
- no DOI deduplication in the output
- unresolved DOIs remain in the result with empty metadata fields
"""

from __future__ import annotations

import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from html import unescape
from pathlib import Path
from typing import Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import pandas as pd

BASE_URL = "https://api.crossref.org/works"

# PyCharm direct-run defaults.
# Edit these values if you want to click Run without any CLI argument.
DEFAULT_INPUT_FILE = "doi_input.txt"
DEFAULT_OUTPUT_FILE = "doi_enriched.xlsx"
DEFAULT_DOI_COLUMN = ""
DEFAULT_SHEET_NAME = ""
DEFAULT_CONFIG_FILE = "configs/theme.yaml"
DEFAULT_SKIP_PUBMED = False
DEFAULT_SKIP_RELEVANCE = False

REQUIRED_COLUMNS = [
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
CANONICAL_ALIASES = {
    "title": ["title"],
    "authors": ["authors"],
    "journal": ["journal"],
    "published_date": ["published_date", "published date", "publication_date", "date"],
    "doi": ["doi"],
    "url": ["url"],
    "issn": ["issn"],
    "abstract": ["abstract", "Abstract"],
    "source": ["source"],
    "relevance": ["relevance"],
    "label": ["label"],
}
DOI_PATTERN = re.compile(r"10\.\d{4,9}/\S+", flags=re.IGNORECASE)


def clean_xml_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]*>", " ", text)
    text = unescape(text)
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


def extract_authors(item: dict) -> str:
    authors = item.get("author", [])
    names = []
    for author in authors:
        given = author.get("given", "")
        family = author.get("family", "")
        full_name = " ".join([given, family]).strip()
        if full_name:
            names.append(full_name)
    return "; ".join(names)


def extract_abstract(item: dict) -> str:
    raw = item.get("abstract")
    if not raw:
        return ""
    text = re.sub(r"<[^>]+>", " ", raw)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _strip_or_empty(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _parse_yaml_scalar(value: str):
    value = value.strip()
    if not value:
        return ""
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    lower = value.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    if lower in {"null", "none"}:
        return None
    return value


def _load_simple_yaml(path: Path) -> dict:
    root: dict = {}
    stack = [(-1, root)]

    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            if not raw_line.strip() or raw_line.lstrip().startswith("#"):
                continue

            indent = len(raw_line) - len(raw_line.lstrip(" "))
            line = raw_line.strip()
            if ":" not in line:
                continue

            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()

            while stack and indent <= stack[-1][0]:
                stack.pop()

            current = stack[-1][1]
            if not value:
                current[key] = {}
                stack.append((indent, current[key]))
            elif value.startswith("- "):
                current[key] = [_parse_yaml_scalar(value[2:])]
            else:
                current[key] = _parse_yaml_scalar(value)

    return root


def load_config_file(path: Path) -> dict:
    if not path.exists():
        return {}

    try:
        import yaml as pyyaml  # type: ignore

        if hasattr(pyyaml, "safe_load"):
            with path.open("r", encoding="utf-8") as handle:
                data = pyyaml.safe_load(handle) or {}
            if isinstance(data, dict):
                return data
    except Exception:
        pass

    return _load_simple_yaml(path)


def _looks_like_doi(value: str) -> bool:
    return bool(DOI_PATTERN.search(_strip_or_empty(value)))


def normalize_doi(value) -> str:
    doi = _strip_or_empty(value)
    if not doi:
        return ""

    doi = doi.replace("https://doi.org/", "")
    doi = doi.replace("http://doi.org/", "")
    doi = doi.replace("https://dx.doi.org/", "")
    doi = doi.replace("http://dx.doi.org/", "")
    doi = re.sub(r"^doi:\s*", "", doi, flags=re.IGNORECASE)
    return doi.strip()


def _non_empty(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip() != ""


def _merge_alias_column(df: pd.DataFrame, target: str, source: str) -> pd.DataFrame:
    if target == source or source not in df.columns:
        return df

    if target not in df.columns:
        return df.rename(columns={source: target})

    mask_target_empty = ~_non_empty(df[target])
    mask_source_non_empty = _non_empty(df[source])
    mask_use_source = mask_target_empty & mask_source_non_empty
    if mask_use_source.any():
        df.loc[mask_use_source, target] = df.loc[mask_use_source, source]
    return df.drop(columns=[source])


def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    normalized = {str(col).strip().lower(): col for col in df.columns}

    for canonical, aliases in CANONICAL_ALIASES.items():
        for alias in aliases:
            source_col = normalized.get(alias.strip().lower())
            if source_col is not None:
                df = _merge_alias_column(df, canonical, source_col)
                normalized = {str(col).strip().lower(): col for col in df.columns}
    return df


def detect_doi_column(df: pd.DataFrame, explicit_column: Optional[str]) -> str:
    if explicit_column:
        if explicit_column not in df.columns:
            raise ValueError(f"Colonne DOI introuvable: {explicit_column}")
        return explicit_column

    for column in df.columns:
        if str(column).strip().lower() == "doi":
            return column

    doi_like_columns = []
    for column in df.columns:
        values = df[column].dropna().astype(str).head(25)
        if not values.empty and values.map(_looks_like_doi).mean() >= 0.6:
            doi_like_columns.append(column)
    if len(doi_like_columns) == 1:
        return doi_like_columns[0]

    raise ValueError(
        "Impossible de detecter automatiquement la colonne DOI. "
        "Utilisez --doi-column."
    )


def load_input_dataframe(input_path: Path, doi_column: Optional[str], sheet_name: Optional[str]) -> pd.DataFrame:
    suffix = input_path.suffix.lower()

    if suffix == ".txt":
        dois = []
        with input_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                value = line.strip()
                if not value or value.startswith("#"):
                    continue
                dois.append(value)
        return pd.DataFrame({"doi": dois})

    if suffix == ".csv":
        df = pd.read_csv(input_path, sep=None, engine="python")
        df = canonicalize_columns(df)
        try:
            detect_doi_column(df, doi_column)
            return df
        except ValueError:
            if len(df.columns) == 1 and _looks_like_doi(str(df.columns[0])):
                df = pd.read_csv(input_path, header=None, names=["doi"], sep=None, engine="python")
                return df
            raise

    if suffix in {".xlsx", ".xls"}:
        read_kwargs = {"sheet_name": sheet_name} if sheet_name else {}
        df = pd.read_excel(input_path, **read_kwargs)
        df = canonicalize_columns(df)
        try:
            detect_doi_column(df, doi_column)
            return df
        except ValueError:
            if len(df.columns) == 1 and _looks_like_doi(str(df.columns[0])):
                read_kwargs["header"] = None
                df = pd.read_excel(input_path, **read_kwargs)
                df.columns = ["doi"]
                return df
            raise

    raise ValueError("Format d'entree non supporte. Utilisez .txt, .csv ou .xlsx/.xls.")


def build_output_path(input_path: Path, output_path: Optional[str]) -> Path:
    if output_path:
        return Path(output_path)
    return input_path.with_name(f"{input_path.stem}_enriched.xlsx")


def make_unique_output_path(output_path: Path, input_path: Optional[Path] = None) -> Path:
    output_path = output_path.resolve()
    input_resolved = input_path.resolve() if input_path is not None else None

    if input_resolved is not None and output_path == input_resolved:
        output_path = output_path.with_name(f"{output_path.stem}_enriched{output_path.suffix or '.xlsx'}")

    if not output_path.suffix:
        output_path = output_path.with_suffix(".xlsx")

    if not output_path.exists():
        return output_path

    stem = output_path.stem
    suffix = output_path.suffix
    parent = output_path.parent

    if not stem.endswith("_new"):
        candidate = parent / f"{stem}_new{suffix}"
        if not candidate.exists():
            return candidate

    index = 2
    while True:
        candidate = parent / f"{stem}_new_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def fetch_crossref_item_by_doi(
    doi: str,
    mailto: str = "",
    timeout: int = 20,
    max_retries: int = 4,
    backoff_base: float = 2.0,
) -> tuple[Optional[dict], Optional[str]]:
    doi = normalize_doi(doi)
    if not doi:
        return None, "empty_doi"

    url = f"{BASE_URL}/{quote(doi, safe='')}"
    if mailto:
        url = f"{url}?mailto={quote(mailto)}"

    headers = {
        "Accept": "application/json",
        "User-Agent": f"doi-list-enrichment/1.0 (mailto:{mailto or 'n/a'})",
    }

    for attempt in range(max_retries):
        request = Request(url, headers=headers)
        try:
            with urlopen(request, timeout=timeout) as response:
                payload = json.load(response)
            item = payload.get("message")
            if not isinstance(item, dict):
                return None, "invalid_payload"
            return item, None
        except HTTPError as exc:
            if exc.code == 404:
                return None, "not_found"
            if exc.code in {429, 500, 502, 503, 504} and attempt < max_retries - 1:
                time.sleep(backoff_base ** attempt)
                continue
            return None, f"http_{exc.code}"
        except URLError:
            if attempt < max_retries - 1:
                time.sleep(backoff_base ** attempt)
                continue
            return None, "network_error"
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(backoff_base ** attempt)
                continue
            return None, "unexpected_error"

    return None, "retry_exhausted"


def _extract_title(item: dict) -> str:
    title_list = item.get("title", []) or []
    subtitle_list = item.get("subtitle", []) or []

    title = clean_xml_text(title_list[0]) if title_list else ""
    subtitle = clean_xml_text(subtitle_list[0]) if subtitle_list else ""

    if title and subtitle and subtitle.lower() not in title.lower():
        return f"{title}: {subtitle}"
    if title:
        return title
    return subtitle


def crossref_item_to_record(item: dict, fallback_doi: str) -> Dict[str, str]:
    published_date = extract_published_date(item)
    issn_values = []
    for value in item.get("ISSN", []) or []:
        cleaned = _strip_or_empty(value)
        if cleaned and cleaned not in issn_values:
            issn_values.append(cleaned)

    record = {
        "title": _extract_title(item),
        "authors": extract_authors(item),
        "journal": clean_xml_text(((item.get("container-title", []) or [""])[0])),
        "published_date": published_date.isoformat() if published_date else "",
        "doi": normalize_doi(item.get("DOI", fallback_doi)) or normalize_doi(fallback_doi),
        "url": _strip_or_empty(item.get("URL")) or f"https://doi.org/{quote(normalize_doi(fallback_doi), safe='/:')}",
        "issn": "; ".join(issn_values),
        "abstract": extract_abstract(item),
        "source": "crossref",
    }
    return record


def _ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for column in REQUIRED_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    return df


def _empty_record(doi: str, existing_source: str = "") -> Dict[str, str]:
    return {
        "title": "",
        "authors": "",
        "journal": "",
        "published_date": "",
        "doi": normalize_doi(doi),
        "url": "",
        "issn": "",
        "abstract": "",
        "source": existing_source or "doi_input",
    }


def enrich_dataframe_from_dois(
    df: pd.DataFrame,
    doi_column: str,
    mailto: str,
    crossref_timeout: int,
    crossref_workers: int,
    crossref_retries: int,
) -> tuple[pd.DataFrame, dict]:
    df = canonicalize_columns(df)
    df = _ensure_required_columns(df)

    original_columns = list(df.columns)
    df["doi"] = df[doi_column].map(normalize_doi)

    unique_dois = []
    seen = set()
    for doi in df["doi"].tolist():
        if doi not in seen:
            seen.add(doi)
            unique_dois.append(doi)

    print(f"[INFO] Lignes en entree : {len(df)}")
    print(f"[INFO] DOI uniques a interroger : {len(unique_dois)}")

    metadata_by_doi: Dict[str, Optional[dict]] = {}
    status_by_doi: Dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=max(1, int(crossref_workers))) as executor:
        future_to_doi = {
            executor.submit(
                fetch_crossref_item_by_doi,
                doi,
                mailto,
                crossref_timeout,
                crossref_retries,
            ): doi
            for doi in unique_dois
        }

        completed = 0
        total = len(future_to_doi)
        for future in as_completed(future_to_doi):
            doi = future_to_doi[future]
            try:
                item, status = future.result()
            except Exception:
                item, status = None, "unexpected_error"

            metadata_by_doi[doi] = item
            status_by_doi[doi] = status or "ok"
            completed += 1

            if completed == total or completed % 25 == 0:
                print(f"[INFO] Crossref {completed}/{total}")

    found_count = 0
    unresolved_count = 0

    for idx in df.index:
        doi = df.at[idx, "doi"]
        existing_source = _strip_or_empty(df.at[idx, "source"])
        item = metadata_by_doi.get(doi)

        if item:
            record = crossref_item_to_record(item, doi)
            found_count += 1
        else:
            record = _empty_record(doi, existing_source=existing_source)
            unresolved_count += 1

        for field, value in record.items():
            if field == "source":
                df.at[idx, field] = value if value else existing_source
                continue
            df.at[idx, field] = value

    if "relevance" in df.columns:
        df["relevance"] = df["relevance"].fillna("")
    if "label" in df.columns:
        df["label"] = df["label"].fillna("")

    ordered_columns = REQUIRED_COLUMNS + [col for col in original_columns if col not in REQUIRED_COLUMNS]
    ordered_columns = [col for col in ordered_columns if col in df.columns]
    df = df.loc[:, ordered_columns].fillna("")

    stats = {
        "rows_total": int(len(df)),
        "unique_dois": int(len(unique_dois)),
        "crossref_found_rows": int(found_count),
        "crossref_unresolved_rows": int(unresolved_count),
        "crossref_status_counts": pd.Series(status_by_doi).value_counts(dropna=False).to_dict(),
    }
    return df, stats


def maybe_enrich_pubmed(
    df: pd.DataFrame,
    cfg: dict,
    skip_pubmed: bool,
    batch_size: int,
    max_workers: int,
) -> tuple[pd.DataFrame, dict]:
    if skip_pubmed:
        print("[INFO] PubMed saute (--skip-pubmed).")
        return df, {"skipped": True}

    pubmed_cfg = (cfg or {}).get("pubmed", {}) or {}
    email = _strip_or_empty(pubmed_cfg.get("email"))
    api_key = _strip_or_empty(pubmed_cfg.get("api_key"))
    if not email:
        print("[INFO] PubMed saute: email absent de la config.")
        return df, {"skipped": True, "reason": "missing_email"}

    try:
        from s5_pubmed_abstracts import enrich_with_pubmed_abstracts
    except ModuleNotFoundError as exc:
        print(f"[INFO] PubMed saute: dependance manquante ({exc}).")
        return df, {"skipped": True, "reason": "missing_dependency"}

    print("[INFO] Enrichissement des abstracts via PubMed...")
    df_pubmed, stats = enrich_with_pubmed_abstracts(
        df=df,
        email=email,
        api_key=api_key,
        batch_size=batch_size,
        max_workers=max_workers,
    )
    return df_pubmed, stats


def maybe_score_relevance(
    df: pd.DataFrame,
    cfg: dict,
    skip_relevance: bool,
) -> tuple[pd.DataFrame, dict]:
    if skip_relevance:
        print("[INFO] Relevance saute (--skip-relevance).")
        return df, {"skipped": True}

    llm_cfg = (cfg or {}).get("llm", {}) or {}
    domain_description = _strip_or_empty(llm_cfg.get("domain_description"))
    model_name = _strip_or_empty(llm_cfg.get("model_name")) or "sentence-transformers/all-MiniLM-L12-v2"
    relevance_threshold_raw = llm_cfg.get("relevance_threshold", 0.7)

    try:
        relevance_threshold = float(relevance_threshold_raw)
    except Exception:
        relevance_threshold = 0.7

    if not domain_description:
        print("[INFO] Relevance saute: description domaine absente de la config.")
        return df, {"skipped": True, "reason": "missing_domain_description"}

    try:
        from s4_llm_relevance import score_articles_with_llm
    except ModuleNotFoundError as exc:
        print(f"[INFO] Relevance saute: dependance manquante ({exc}).")
        return df, {"skipped": True, "reason": "missing_dependency"}
    except Exception as exc:
        print(f"[INFO] Relevance saute: import impossible ({exc}).")
        return df, {"skipped": True, "reason": "import_error"}

    print("[INFO] Calcul du score de relevance...")
    try:
        df_scored, df_relevant = score_articles_with_llm(
            df=df,
            domain_description=domain_description,
            model_name=model_name,
            relevance_threshold=relevance_threshold,
        )
    except Exception as exc:
        print(f"[INFO] Relevance saute: echec du scoring ({exc}).")
        return df, {"skipped": True, "reason": "scoring_error"}

    df_scored = df_scored.sort_index()
    if "relevance" in df_scored.columns:
        df_scored["relevance"] = pd.to_numeric(df_scored["relevance"], errors="coerce")

    stats = {
        "skipped": False,
        "threshold": relevance_threshold,
        "model_name": model_name,
        "relevant_count": int(len(df_relevant)),
        "label_counts": df_scored["label"].fillna("").value_counts(dropna=False).to_dict() if "label" in df_scored.columns else {},
    }
    return df_scored, stats


def export_to_excel(df: pd.DataFrame, output_path: Path, stats: dict) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="articles")
        stats_rows = []
        for key, value in stats.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    stats_rows.append({"metric": f"{key}.{sub_key}", "value": sub_value})
            else:
                stats_rows.append({"metric": key, "value": value})
        pd.DataFrame(stats_rows).to_excel(writer, index=False, sheet_name="stats")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrichit une liste de DOI en recuperant les metadonnees Crossref et les abstracts PubMed."
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_FILE,
        help="Fichier d'entree .txt, .csv ou .xlsx/.xls. Defaut PyCharm direct-run: %(default)s",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT_FILE, help="Fichier Excel de sortie (.xlsx)")
    parser.add_argument("--doi-column", default=DEFAULT_DOI_COLUMN, help="Nom de la colonne DOI pour les entrees csv/xlsx")
    parser.add_argument("--sheet-name", default=DEFAULT_SHEET_NAME, help="Nom de feuille Excel a lire")
    parser.add_argument("--config", default=DEFAULT_CONFIG_FILE, help="Fichier YAML de configuration")
    parser.add_argument("--mailto", default="", help="Email a transmettre a Crossref")
    parser.add_argument(
        "--skip-pubmed",
        action="store_true",
        default=DEFAULT_SKIP_PUBMED,
        help="Desactive l'enrichissement PubMed",
    )
    parser.add_argument(
        "--skip-relevance",
        action="store_true",
        default=DEFAULT_SKIP_RELEVANCE,
        help="Desactive le calcul de relevance",
    )
    parser.add_argument("--crossref-timeout", type=int, default=20, help="Timeout Crossref en secondes")
    parser.add_argument("--crossref-workers", type=int, default=4, help="Nombre de workers Crossref")
    parser.add_argument("--crossref-retries", type=int, default=4, help="Nombre de tentatives Crossref")
    parser.add_argument("--pubmed-batch-size", type=int, default=100, help="Taille des batches PubMed")
    parser.add_argument("--pubmed-workers", type=int, default=6, help="Nombre de workers PubMed")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent

    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = base_dir / input_path
    input_path = input_path.resolve()
    if not input_path.exists():
        raise FileNotFoundError(
            f"Fichier introuvable: {input_path}. "
            f"Cree {DEFAULT_INPUT_FILE} a cote du script ou modifie DEFAULT_INPUT_FILE en haut du script."
        )

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = base_dir / config_path
    config_path = config_path.resolve()
    cfg = load_config_file(config_path)

    df_input = load_input_dataframe(
        input_path=input_path,
        doi_column=args.doi_column or None,
        sheet_name=args.sheet_name or None,
    )
    df_input = canonicalize_columns(df_input)
    doi_column = detect_doi_column(df_input, args.doi_column or None)

    mailto = args.mailto or _strip_or_empty(((cfg or {}).get("pubmed", {}) or {}).get("email"))
    df_crossref, crossref_stats = enrich_dataframe_from_dois(
        df=df_input,
        doi_column=doi_column,
        mailto=mailto,
        crossref_timeout=args.crossref_timeout,
        crossref_workers=args.crossref_workers,
        crossref_retries=args.crossref_retries,
    )

    df_final, pubmed_stats = maybe_enrich_pubmed(
        df=df_crossref,
        cfg=cfg,
        skip_pubmed=args.skip_pubmed,
        batch_size=args.pubmed_batch_size,
        max_workers=args.pubmed_workers,
    )
    df_final, relevance_stats = maybe_score_relevance(
        df=df_final,
        cfg=cfg,
        skip_relevance=args.skip_relevance,
    )

    output_path_arg = args.output or None
    if output_path_arg:
        output_path = Path(output_path_arg)
        if not output_path.is_absolute():
            output_path = base_dir / output_path
    else:
        output_path = build_output_path(input_path, None)
    output_path = make_unique_output_path(output_path, input_path=input_path)

    print(f"[INFO] Fichier d'entree : {input_path}")
    print(f"[INFO] Fichier de sortie : {output_path}")
    combined_stats = {
        **crossref_stats,
        "pubmed": pubmed_stats,
        "relevance": relevance_stats,
    }
    export_to_excel(df_final, output_path, combined_stats)

    print(f"[OK] Fichier Excel genere: {output_path}")


if __name__ == "__main__":
    main()
