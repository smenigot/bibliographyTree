# main.py
# -*- coding: utf-8 -*-
import asyncio
import os
from pathlib import Path

from run_pipeline_for_config import run_pipeline_for_config


def main():
    base_dir = Path(__file__).resolve().parent
    config_dir = base_dir / "configs"
    explicit_config = os.getenv("BIBLIO_CONFIG", "").strip()

    os.chdir(base_dir)

    if explicit_config:
        cfg_path = Path(explicit_config)
        if not cfg_path.is_absolute():
            cfg_path = base_dir / cfg_path
        if not cfg_path.exists():
            print(f"Configuration file not found: {cfg_path}")
            return
        config_files = [cfg_path]
    else:
        config_files = sorted(config_dir.glob("*.yaml")) + sorted(config_dir.glob("*.yml"))

    if not config_files:
        print(f"No YAML configuration found in: {config_dir}")
        return

    for cfg_path in config_files:
        print("\n====================================")
        print(f" Running pipeline for: {cfg_path.name}")
        print("====================================\n")

        asyncio.run(
            run_pipeline_for_config(
                config_path=str(cfg_path),
                recompute_issn=True,
                recompute_crossref=True,
                recompute_keywords=True,
                recompute_llm=True,
                recompute_pubmed=False,
                use_hal=True,
                use_arxiv=True,
            )
        )


if __name__ == "__main__":
    main()
