#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Independent Excel -> BibTeX exporter.

Reads an Excel file containing bibliographic entries and writes a .bib file with
one BibTeX entry per row. All rows are exported.
"""

from __future__ import annotations

import argparse
import html
import re
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# PyCharm direct-run defaults.
DEFAULT_INPUT_BASENAME = "bibliography_input"
DEFAULT_OUTPUT_BASENAME = ""
DEFAULT_SHEET_NAME = ""
DEFAULT_INCLUDE_ABSTRACT = True
DEFAULT_INCLUDE_CUSTOM_FIELDS = True

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


def clean_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    text = html.unescape(str(value)).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    normalized_map = {
        "title": "title",
        "authors": "authors",
        "journal": "journal",
        "published_date": "published_date",
        "published date": "published_date",
        "publication_date": "published_date",
        "date": "published_date",
        "doi": "doi",
        "url": "url",
        "issn": "issn",
        "abstract": "abstract",
        "source": "source",
        "relevance": "relevance",
        "label": "label",
    }

    rename_map = {}
    for column in df.columns:
        key = str(column).strip().lower()
        if key in normalized_map:
            rename_map[column] = normalized_map[key]

    df = df.rename(columns=rename_map)
    for column in REQUIRED_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    return df


def resolve_excel_input(base_dir: Path, input_name: str) -> Path:
    raw_path = Path(input_name)
    if raw_path.is_absolute():
        candidates = [raw_path]
    else:
        candidates = [base_dir / raw_path]

    expanded_candidates: List[Path] = []
    for candidate in candidates:
        expanded_candidates.append(candidate)
        if not candidate.suffix:
            expanded_candidates.append(candidate.with_suffix(".xlsx"))
            expanded_candidates.append(candidate.with_suffix(".xls"))

    for candidate in expanded_candidates:
        if candidate.exists():
            return candidate.resolve()

    raise FileNotFoundError(
        f"Fichier Excel introuvable pour '{input_name}'. "
        "Utilise un nom avec ou sans extension .xlsx/.xls."
    )


def build_output_path(input_path: Path, output_name: str) -> Path:
    if output_name:
        output_path = Path(output_name)
        if not output_path.is_absolute():
            output_path = input_path.parent / output_path
    else:
        output_path = input_path.with_suffix(".bib")
    return make_unique_output_path(output_path, input_path=None)


def make_unique_output_path(output_path: Path, input_path: Optional[Path]) -> Path:
    output_path = output_path.resolve()
    input_resolved = input_path.resolve() if input_path is not None else None

    if input_resolved is not None and output_path == input_resolved:
        output_path = output_path.with_name(f"{output_path.stem}_new{output_path.suffix or '.bib'}")

    if not output_path.suffix:
        output_path = output_path.with_suffix(".bib")

    if not output_path.exists():
        return output_path

    stem = output_path.stem
    suffix = output_path.suffix
    parent = output_path.parent

    candidate = parent / f"{stem}_new{suffix}"
    if not candidate.exists():
        return candidate

    index = 2
    while True:
        candidate = parent / f"{stem}_new_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def read_excel_articles(input_path: Path, sheet_name: str) -> pd.DataFrame:
    read_kwargs = {"dtype": str}
    if sheet_name:
        read_kwargs["sheet_name"] = sheet_name
    df = pd.read_excel(input_path, **read_kwargs)
    df = canonicalize_columns(df)
    df = df.fillna("")
    return df


def normalize_for_key(value: str) -> str:
    text = clean_text(value)
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Za-z0-9]+", " ", text).strip()
    return text


def pick_first_author_surname(authors_raw: str) -> str:
    authors = [clean_text(part) for part in str(authors_raw).split(";") if clean_text(part)]
    if not authors:
        return "Unknown"
    first_author = authors[0]
    tokens = first_author.split()
    if not tokens:
        return "Unknown"
    return normalize_for_key(tokens[-1]) or "Unknown"


def pick_title_token(title: str) -> str:
    stopwords = {
        "the", "a", "an", "of", "and", "or", "for", "in", "on", "to", "with",
        "from", "by", "at", "state", "art",
    }
    normalized = normalize_for_key(title)
    tokens = [token for token in normalized.split() if token]
    for token in tokens:
        if token.lower() not in stopwords:
            return token[:24]
    return "Untitled"


def extract_year(published_date: str) -> str:
    text = clean_text(published_date)
    match = re.search(r"\b(19|20)\d{2}\b", text)
    return match.group(0) if match else "nd"


def extract_month(published_date: str) -> str:
    text = clean_text(published_date)
    match = re.match(r"^\d{4}-(\d{2})", text)
    return match.group(1) if match else ""


def build_citation_key(row: pd.Series, used_keys: Dict[str, int]) -> str:
    surname = pick_first_author_surname(row.get("authors", ""))
    year = extract_year(row.get("published_date", ""))
    title_token = pick_title_token(row.get("title", ""))
    base_key = f"{surname}{year}{title_token}"
    base_key = re.sub(r"[^A-Za-z0-9]+", "", base_key) or "Entry"

    count = used_keys.get(base_key, 0) + 1
    used_keys[base_key] = count
    if count == 1:
        return base_key
    return f"{base_key}{count}"


def bibtex_escape(value: str) -> str:
    text = clean_text(value)
    replacements = {
        "\\": r"\textbackslash{}",
        "{": r"\{",
        "}": r"\}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "~": r"\~{}",
        "^": r"\^{}",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return text


def format_authors_for_bibtex(authors_raw: str) -> str:
    authors = [clean_text(part) for part in str(authors_raw).split(";") if clean_text(part)]
    return " and ".join(bibtex_escape(author) for author in authors)


def choose_entry_type(row: pd.Series) -> str:
    journal = clean_text(row.get("journal", ""))
    doi = clean_text(row.get("doi", ""))
    if journal:
        return "article"
    if doi:
        return "misc"
    return "misc"


def build_bibtex_fields(
    row: pd.Series,
    include_abstract: bool,
    include_custom_fields: bool,
) -> List[tuple[str, str]]:
    fields: List[tuple[str, str]] = []

    title = clean_text(row.get("title", ""))
    authors = clean_text(row.get("authors", ""))
    journal = clean_text(row.get("journal", ""))
    published_date = clean_text(row.get("published_date", ""))
    doi = clean_text(row.get("doi", ""))
    url = clean_text(row.get("url", ""))
    issn = clean_text(row.get("issn", ""))
    abstract = clean_text(row.get("abstract", ""))
    source = clean_text(row.get("source", ""))
    relevance = clean_text(row.get("relevance", ""))
    label = clean_text(row.get("label", ""))

    year = extract_year(published_date)
    month = extract_month(published_date)

    if title:
        fields.append(("title", bibtex_escape(title)))
    if authors:
        fields.append(("author", format_authors_for_bibtex(authors)))
    if journal:
        fields.append(("journal", bibtex_escape(journal)))
    if year and year != "nd":
        fields.append(("year", year))
    if month:
        fields.append(("month", month))
    if doi:
        fields.append(("doi", bibtex_escape(doi)))
    if url:
        fields.append(("url", bibtex_escape(url)))
    if issn:
        fields.append(("issn", bibtex_escape(issn)))
    if published_date:
        fields.append(("note", bibtex_escape(f"Published date: {published_date}")))
    if include_abstract and abstract:
        fields.append(("abstract", bibtex_escape(abstract)))
    if include_custom_fields and source:
        fields.append(("source", bibtex_escape(source)))
    if include_custom_fields and relevance:
        fields.append(("relevance", bibtex_escape(relevance)))
    if include_custom_fields and label:
        fields.append(("label", bibtex_escape(label)))

    return fields


def row_to_bibtex_entry(
    row: pd.Series,
    citation_key: str,
    include_abstract: bool,
    include_custom_fields: bool,
) -> str:
    entry_type = choose_entry_type(row)
    fields = build_bibtex_fields(
        row=row,
        include_abstract=include_abstract,
        include_custom_fields=include_custom_fields,
    )

    lines = [f"@{entry_type}{{{citation_key},"]
    for field_name, field_value in fields:
        lines.append(f"  {field_name} = {{{field_value}}},")
    lines.append("}")
    return "\n".join(lines)


def write_bibtex_file(
    df: pd.DataFrame,
    output_path: Path,
    include_abstract: bool,
    include_custom_fields: bool,
) -> dict:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    entries: List[str] = []
    used_keys: Dict[str, int] = {}
    article_count = 0
    misc_count = 0

    for _, row in df.iterrows():
        citation_key = build_citation_key(row, used_keys)
        entry = row_to_bibtex_entry(
            row=row,
            citation_key=citation_key,
            include_abstract=include_abstract,
            include_custom_fields=include_custom_fields,
        )
        entries.append(entry)

        if choose_entry_type(row) == "article":
            article_count += 1
        else:
            misc_count += 1

    content = "\n\n".join(entries) + ("\n" if entries else "")
    output_path.write_text(content, encoding="utf-8")

    return {
        "entries_total": len(entries),
        "article_count": article_count,
        "misc_count": misc_count,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Exporte un fichier Excel bibliographique vers BibTeX.")
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_BASENAME,
        help="Nom du fichier Excel d'entree, avec ou sans extension .xlsx/.xls",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT_BASENAME, help="Nom du fichier BibTeX de sortie")
    parser.add_argument("--sheet-name", default=DEFAULT_SHEET_NAME, help="Nom de feuille Excel a lire")
    parser.add_argument(
        "--no-abstract",
        action="store_true",
        default=not DEFAULT_INCLUDE_ABSTRACT,
        help="N'inclut pas le champ abstract dans le BibTeX",
    )
    parser.add_argument(
        "--no-custom-fields",
        action="store_true",
        default=not DEFAULT_INCLUDE_CUSTOM_FIELDS,
        help="N'inclut pas source/relevance/label dans le BibTeX",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent

    input_path = resolve_excel_input(base_dir, args.input)
    output_path = build_output_path(input_path, args.output)

    print(f"[INFO] Fichier Excel d'entree : {input_path}")
    print(f"[INFO] Fichier BibTeX de sortie : {output_path}")

    df = read_excel_articles(input_path, args.sheet_name)
    stats = write_bibtex_file(
        df=df,
        output_path=output_path,
        include_abstract=not args.no_abstract,
        include_custom_fields=not args.no_custom_fields,
    )

    print(f"[OK] BibTeX genere : {output_path}")
    print(f"[INFO] Entrees exportees : {stats['entries_total']}")
    print(f"[INFO] Articles : {stats['article_count']}")
    print(f"[INFO] Misc : {stats['misc_count']}")


if __name__ == "__main__":
    main()
