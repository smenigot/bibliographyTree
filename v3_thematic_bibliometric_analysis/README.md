# Version 3: Thematic Bibliometric Analysis

Version 3 is designed for review-oriented work on a defined theme. It combines corpus building utilities with bibliometric, thematic, and network analysis scripts.

## Main Components

- `main.py`: runs local YAML configurations stored in `configs/`, or one file pointed to by `BIBLIO_CONFIG`.
- `run_pipeline_for_config.py`: collects and filters a thematic corpus with stronger control over date windows and sources.
- `rerun_llm_threshold.py`: rebuilds relevant subsets from an already scored CSV without rerunning the full pipeline.
- `s2d_doi_list_enrichment.py`: turns a DOI list into an enriched spreadsheet.
- `s6_review_bibliometric_analysis.py`: produces tables, figures, networks, and a Markdown review report from a bibliography file.
- `s7_excel_to_bibtex.py`: exports a spreadsheet to BibTeX.

## Configuration Strategy

- `configs/examples/` contains tracked, sanitized examples.
- `configs/` is the place for local runnable YAML files with your credentials and topic settings.
- The scripts expect you to provide your own NCBI contact e-mail and, if available, an API key.

## Installation

Use a dedicated Python environment, then install:

```bash
pip install -r requirements.txt
```

## Typical Workflow

1. Build or refine a thematic corpus with `main.py` and `run_pipeline_for_config.py`.
2. Optionally enrich a standalone DOI list with `s2d_doi_list_enrichment.py`.
3. If needed, adjust the relevance threshold with `rerun_llm_threshold.py`.
4. Run `s6_review_bibliometric_analysis.py` on the curated CSV or XLSX bibliography.
5. Export the final spreadsheet to BibTeX with `s7_excel_to_bibtex.py`.

## Running the Collection Pipeline

Run all local configurations stored in `configs/`:

```bash
python main.py
```

Run one configuration explicitly with PowerShell:

```powershell
$env:BIBLIO_CONFIG = "configs/examples/config_embole.yaml"
python .\main.py
```

Run one configuration explicitly with a POSIX shell:

```bash
BIBLIO_CONFIG=configs/examples/config_embole.yaml python main.py
```

## Running the Review Analysis

The review-analysis script now accepts any CSV, XLSX, or XLS file.

```bash
python s6_review_bibliometric_analysis.py --input path/to/bibliography.xlsx --output-dir review_analysis_output/my_theme
```

## DOI Enrichment Example

```bash
python s2d_doi_list_enrichment.py --input doi_input.txt --output doi_enriched.xlsx --config configs/my_theme.yaml
```

## BibTeX Export Example

```bash
python s7_excel_to_bibtex.py --input path/to/bibliography.xlsx --output my_theme.bib
```

## Generated Outputs

Typical generated files include:

- collected or filtered CSV/XLSX files from the pipeline
- enriched DOI spreadsheets
- review figures and tables
- network exports
- Markdown review reports
- BibTeX files

These outputs are intentionally ignored by Git and are not included in the public repository.
