# Repository Structure

This document describes the folder organization of the repository and the publication rules used to keep it reusable.

## Top-Level Folders

- `v1_graph_notebooks/`: historical notebook-based exploration of bibliographic graphs.
- `v2_automated_thematic_search/`: automated search pipeline for building a thematic corpus.
- `v3_thematic_bibliometric_analysis/`: advanced review-analysis toolkit built around a curated corpus.
- `docs/`: cross-repository documentation.

## Version 1

`v1_graph_notebooks/` contains the notebook material already prepared for Git publication:

- `arbre_genealogique_papier_avec_filtrage.ipynb`
- `arbre_genealogique_papier_sans_filtrage.ipynb`
- `recherche_articles_par_motscles_et_par_categories.ipynb`
- `exemple_graphe.png`

This folder is intentionally lightweight and keeps a single committed illustration for documentation purposes.

## Version 2

`v2_automated_thematic_search/` is organized around a script-based pipeline:

- `main.py`: entry point for running one or several local YAML configurations.
- `config.py`: YAML loader.
- `run_pipeline_for_config.py`: orchestrates the full workflow.
- `s1_*` to `s5_*`: stepwise collection, filtering, relevance scoring, and PubMed enrichment scripts.
- `configs/examples/`: tracked example configurations without secrets.
- `configs/`: local, non-tracked runnable configurations created by the user.
- `requirements.txt`: Python dependencies for this version only.

Generated outputs are not committed. They are produced in the version folder and ignored by Git.

## Version 3

`v3_thematic_bibliometric_analysis/` extends the collection logic and adds analysis utilities:

- `main.py`: entry point for local YAML configurations.
- `run_pipeline_for_config.py`: collection and thematic filtering pipeline.
- `rerun_llm_threshold.py`: rebuilds relevant subsets from existing scored files.
- `s2d_doi_list_enrichment.py`: enriches a DOI list into a review-ready spreadsheet.
- `s6_review_bibliometric_analysis.py`: generates figures, tables, networks, and a Markdown review report from a bibliography file.
- `s7_excel_to_bibtex.py`: exports a spreadsheet to BibTeX.
- `configs/examples/`: tracked example configuration without credentials.
- `configs/`: local, non-tracked runnable configurations created by the user.
- `requirements.txt`: Python dependencies for this version only.

As with version 2, generated outputs are intentionally excluded from version control.

## Publication Rules

- Do not commit personal API keys, e-mail addresses, or machine-specific settings.
- Do not commit generated CSV, XLSX, BibTeX, cache, or temporary folders.
- Keep reusable example configurations in `configs/examples/`.
- Keep runnable personal configurations in `configs/`.
- Keep new documentation in English to preserve a consistent public interface.
