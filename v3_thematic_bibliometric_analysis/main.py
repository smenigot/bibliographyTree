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
        all_config_files = sorted(config_dir.glob("*.yaml")) + sorted(config_dir.glob("*.yml"))
        include_test_cfg = os.getenv("BIBLIO_INCLUDE_TEST_CONFIGS", "0") == "1"

        if include_test_cfg:
            config_files = all_config_files
        else:
            config_files = [p for p in all_config_files if not p.stem.endswith("_test")]
            if not config_files and all_config_files:
                config_files = all_config_files
                print("No standard configuration found, falling back to _test files.")

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
                recompute_pubmed=True,
                use_hal=False,
                use_arxiv=False,
            )
        )


if __name__ == "__main__":
    main()
