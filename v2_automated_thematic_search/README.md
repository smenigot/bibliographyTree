# Version 2: Automated Thematic Search

Version 2 automates the collection of a topic-focused bibliography from journal categories, recent records, keyword filtering, semantic relevance scoring, and optional PubMed abstract enrichment.

## Pipeline Stages

- `s1_journals_issn.py`: retrieves journal ISSN sets from selected Scimago categories.
- `s2_crossref_recent.py`: collects recent Crossref articles by ISSN.
- `s2b_hal_theses_recent.py`: optionally adds HAL theses.
- `s2c_arxiv_recent.py`: optionally adds arXiv preprints.
- `s3_filter_keywords.py`: keeps records matching thematic patterns.
- `s4_llm_relevance.py`: scores relevance with sentence embeddings.
- `s5_pubmed_abstracts.py`: enriches the relevant subset with PubMed abstracts.
- `run_pipeline_for_config.py`: runs the full sequence for one YAML configuration.
- `main.py`: runs all local YAML files stored in `configs/`, or one file pointed to by `BIBLIO_CONFIG`.

## Repository Layout

- `configs/examples/`: tracked examples for biomedical engineering, education, AI, optics, signal processing, and ultrasound.
- `configs/`: local runnable YAML files. This directory is intentionally ignored by Git when it contains your own configurations.
- `requirements.txt`: version-specific dependencies.

## Installation

Use a dedicated Python environment, then install:

```bash
pip install -r requirements.txt
```

## Configuration Workflow

1. Start from one file in `configs/examples/`.
2. Duplicate it into `configs/`.
3. Replace placeholder fields such as `your.email@example.org`.
4. Adjust categories, keyword expressions, thresholds, and source toggles to your topic.

## Running the Pipeline

Run all local configurations stored in `configs/`:

```bash
python main.py
```

Run a single configuration explicitly by pointing `BIBLIO_CONFIG` to the file you want to execute.

With PowerShell:

```powershell
$env:BIBLIO_CONFIG = "configs/examples/config_ultrasound.yaml"
python .\main.py
```

With a POSIX shell:

```bash
BIBLIO_CONFIG=configs/examples/config_ultrasound.yaml python main.py
```

## Generated Outputs

Typical generated files include:

- `*_issns_and_titles_snapshot.csv`
- `*_articles_recent_full.csv`
- `*_articles_after_keywords.csv`
- `*_articles_scored.csv`
- `*_articles_relevant_only.csv`
- `*_articles_with_pubmed_abstracts_YYYYMMDD.csv`
- `*_articles_with_pubmed_abstracts_YYYYMMDD.xlsx`

These outputs are intentionally ignored by Git and are not part of the public repository.

## Data Sources

- Scimago Journal Rank
- Crossref
- HAL
- arXiv
- PubMed / NCBI
