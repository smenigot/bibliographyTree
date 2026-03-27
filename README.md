# Bibliography Toolkit

This repository gathers three complementary deliverables for theme-driven bibliographic exploration:

- `v1_graph_notebooks`: Jupyter notebooks for article genealogy graphs.
- `v2_automated_thematic_search`: an automated thematic literature collection pipeline.
- `v3_thematic_bibliometric_analysis`: a thematic review and bibliometric analysis toolkit.

The repository is intentionally publication-oriented. It includes code, notebooks, example configurations, and documentation, but excludes temporary files, generated corpora, result spreadsheets, caches, and personal credentials.

## Repository Map

- [`v1_graph_notebooks`](v1_graph_notebooks/README.md): notebook-based graph exploration.
- [`v2_automated_thematic_search`](v2_automated_thematic_search/README.md): automated collection and filtering pipeline.
- [`v3_thematic_bibliometric_analysis`](v3_thematic_bibliometric_analysis/README.md): review analysis, DOI enrichment, and BibTeX export.
- [`docs/repository-structure.md`](docs/repository-structure.md): full folder-level documentation.

## Reuse Principles

- Example YAML files are provided in `configs/examples/` with placeholders instead of secrets.
- Local runnable configurations should be created in each version folder under `configs/`.
- Generated CSV, XLSX, BibTeX, cache, and temporary files are ignored by Git.
- The only committed illustration is the graph example image used in the notebook documentation.

## Quick Start

1. Create a Python environment suitable for the version you want to run.
2. Install the matching `requirements.txt`.
3. Copy one example YAML file into the local `configs/` directory of the target version.
4. Replace placeholder values such as `your.email@example.org` with your own settings.
5. Run the version-specific entry point described in its README.

## Typical Workflows

- Use `v1_graph_notebooks` to explore citation and reference relationships around selected papers.
- Use `v2_automated_thematic_search` to build a topic-focused corpus from journal categories, keyword filters, and relevance scoring.
- Use `v3_thematic_bibliometric_analysis` to enrich a curated corpus, analyze it, generate review figures and tables, and export BibTeX.
