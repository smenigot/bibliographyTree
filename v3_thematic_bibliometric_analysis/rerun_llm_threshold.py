# -*- coding: utf-8 -*-
"""
Script autonome pour reappliquer un relevance_threshold sur le CSV deja score.

Comportement:
- lit <prefix>_articles_scored.csv
- applique le threshold (pris depuis la config YAML, sauf override CLI)
- regenere <prefix>_articles_relevant_only.csv
- regenere le CSV/XLSX final nomme "..._articles_with_pubmed_abstracts_YYYYMMDD"

Important: ce script ne relance PAS PubMed.
"""

import argparse
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import load_config


def _resolve_default_paths(config_path: Path) -> tuple[Path, Path]:
    prefix = config_path.stem
    path_scored = Path(f"{prefix}_articles_scored.csv")
    path_relevant = Path(f"{prefix}_articles_relevant_only.csv")
    return path_scored, path_relevant


def _resolve_default_final_csv(config_path: Path) -> Path:
    """
    Priorite:
      1) dernier fichier existant <prefix>_articles_with_pubmed_abstracts_YYYYMMDD.csv
      2) sinon fichier date du jour UTC.
    """
    prefix = config_path.stem
    pattern = re.compile(
        rf"^{re.escape(prefix)}_articles_with_pubmed_abstracts_(\d{{8}})$"
    )

    candidates = []
    for p in Path(".").glob(f"{prefix}_articles_with_pubmed_abstracts_*.csv"):
        m = pattern.match(p.stem)
        if not m:
            continue
        candidates.append((m.group(1), p))

    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    date_tag = datetime.utcnow().strftime("%Y%m%d")
    return Path(f"{prefix}_articles_with_pubmed_abstracts_{date_tag}.csv")


def _ensure_threshold(threshold: float) -> float:
    if threshold < 0.0 or threshold > 1.0:
        raise ValueError("Le threshold doit etre dans [0, 1].")
    return float(threshold)


def _rebuild_from_scored(path_scored: Path, threshold: float, sep: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not path_scored.exists():
        raise FileNotFoundError(f"Fichier scored introuvable: {path_scored}")

    df_scored = pd.read_csv(path_scored, sep=sep, encoding="utf-8")
    if "relevance" not in df_scored.columns:
        raise ValueError(f"Colonne 'relevance' absente dans: {path_scored}")

    df_scored["relevance"] = pd.to_numeric(df_scored["relevance"], errors="coerce")
    df_scored["label"] = "hors_sujet"
    df_scored.loc[df_scored["relevance"] >= threshold, "label"] = "pertinent"
    df_scored = df_scored.sort_values("relevance", ascending=False)
    df_relevant = df_scored[df_scored["relevance"] >= threshold].copy()
    return df_scored, df_relevant


def _export_final_files(
    df_relevant: pd.DataFrame,
    path_final_csv: Path,
    sep: str,
) -> None:
    df_out = df_relevant.copy()
    df_out.to_csv(path_final_csv, sep=sep, index=False, encoding="utf-8")
    excel_path = path_final_csv.with_suffix(".xlsx")
    if "doi" in df_out.columns:
        df_out["doi"] = df_out["doi"].astype(str)
    df_out.to_excel(excel_path, index=False)
    print(f"[FINAL] CSV ecrit   : {path_final_csv}")
    print(f"[FINAL] Excel ecrit : {excel_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Reapplique un threshold sur le CSV scored et regenere les fichiers finaux CSV/XLSX."
    )
    parser.add_argument(
        "--config",
        default=os.getenv("BIBLIO_CONFIG", "configs/theme.yaml"),
        help="Chemin vers le fichier YAML de config.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=None,
        help="Valeur du threshold (0..1). Si absent, prend llm.relevance_threshold dans la config.",
    )
    parser.add_argument(
        "--scored-csv",
        default=None,
        help="Chemin du CSV scored (par defaut: <prefix>_articles_scored.csv).",
    )
    parser.add_argument(
        "--relevant-csv",
        default=None,
        help="Chemin du CSV relevant (par defaut: <prefix>_articles_relevant_only.csv).",
    )
    parser.add_argument(
        "--final-csv",
        default=None,
        help=(
            "Chemin du CSV final (..._articles_with_pubmed_abstracts_YYYYMMDD.csv). "
            "Par defaut: dernier fichier existant pour le prefix, sinon date du jour."
        ),
    )
    parser.add_argument(
        "--sep",
        default=";",
        help="Separateur CSV (par defaut: ';').",
    )

    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = Path(__file__).resolve().parent / config_path
    if not config_path.exists():
        raise FileNotFoundError(f"Config introuvable: {config_path}")

    cfg = load_config(str(config_path))
    llm_cfg = cfg.get("llm", {})
    threshold = args.threshold if args.threshold is not None else llm_cfg.get("relevance_threshold", 0.65)
    threshold = _ensure_threshold(float(threshold))

    default_scored, default_relevant = _resolve_default_paths(config_path)
    default_final = _resolve_default_final_csv(config_path)
    path_scored = Path(args.scored_csv) if args.scored_csv else default_scored
    path_relevant = Path(args.relevant_csv) if args.relevant_csv else default_relevant
    path_final = Path(args.final_csv) if args.final_csv else default_final

    print("====================================")
    print(" Rebuild threshold depuis scored")
    print("====================================")
    print(f"Config      : {config_path}")
    print(f"Threshold   : {threshold}")
    print(f"Scored CSV  : {path_scored}")
    print(f"Final CSV   : {path_final}")

    df_scored, df_relevant = _rebuild_from_scored(
        path_scored=path_scored,
        threshold=threshold,
        sep=args.sep,
    )

    df_scored.to_csv(path_scored, sep=args.sep, index=False, encoding="utf-8")
    df_relevant.to_csv(path_relevant, sep=args.sep, index=False, encoding="utf-8")

    print(f"[LLM] Articles scores   : {len(df_scored)}")
    print(f"[LLM] Articles pertinents (>= {threshold}): {len(df_relevant)}")
    print(f"[LLM] Scored ecrit   : {path_scored}")
    print(f"[LLM] Relevant ecrit : {path_relevant}")
    _export_final_files(
        df_relevant=df_relevant,
        path_final_csv=path_final,
        sep=args.sep,
    )


if __name__ == "__main__":
    main()
