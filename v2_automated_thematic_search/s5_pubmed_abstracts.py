# -*- coding: utf-8 -*-
# s5_pubmed_abstracts.py

import time
from typing import Optional, Dict, List, Tuple

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from Bio import Entrez
from tqdm import tqdm


def search_pmid_by_doi(doi: str) -> Optional[str]:
    if not isinstance(doi, str) or not doi.strip():
        return None
    term = f"{doi}[DOI]"
    try:
        handle = Entrez.esearch(db="pubmed", term=term)
        record = Entrez.read(handle)
        handle.close()
        ids = record.get("IdList", [])
        if not ids:
            return None
        return ids[0]
    except Exception:
        return None


def search_pmid_by_title(title: str) -> Optional[str]:
    if not isinstance(title, str) or not title.strip():
        return None
    title_clean = " ".join(title.split())
    term = f"\"{title_clean}\"[Title]"
    try:
        handle = Entrez.esearch(db="pubmed", term=term)
        record = Entrez.read(handle)
        handle.close()
        ids = record.get("IdList", [])
        if not ids:
            return None
        return ids[0]
    except Exception:
        return None


def find_pmid_for_article(idx: int, title: str, doi: Optional[str]) -> Tuple[int, Optional[str]]:
    pmid = None
    if doi and doi != "nan":
        pmid = search_pmid_by_doi(doi)
    if not pmid:
        pmid = search_pmid_by_title(title)
    return idx, pmid


def fetch_abstracts_for_pmids(pmids: List[str]) -> Dict[str, str]:
    """
    Récupère les abstracts PubMed pour une liste de PMIDs.
    Retourne un dict {pmid: abstract}.
    """
    from Bio import Entrez  # pour être sûr d'utiliser la config globale

    result: Dict[str, str] = {}
    if not pmids:
        return result

    id_str = ",".join(pmids)
    print(f"  [EFETCH] Récupération abstracts pour {len(pmids)} PMIDs...")
    try:
        handle = Entrez.efetch(db="pubmed", id=id_str, rettype="abstract", retmode="xml")
        records = Entrez.read(handle)
        handle.close()

        articles = records.get("PubmedArticle", [])
        print(f"  [EFETCH] {len(articles)} PubmedArticle reçus.")

        for art in articles:
            try:
                pmid = str(art["MedlineCitation"]["PMID"])
                article_data = art["MedlineCitation"]["Article"]

                if "Abstract" not in article_data:
                    continue

                abs_obj = article_data["Abstract"]["AbstractText"]

                if isinstance(abs_obj, list):
                    parts = []
                    for block in abs_obj:
                        if isinstance(block, str):
                            parts.append(block.strip())
                        elif isinstance(block, dict):
                            txt = block.get("_", "")
                        # Sinon ?
                            if txt:
                                parts.append(txt.strip())
                    abstract = " ".join(parts)
                elif isinstance(abs_obj, str):
                    abstract = abs_obj.strip()
                else:
                    abstract = ""

                abstract = " ".join(abstract.split())
                if abstract:
                    result[pmid] = abstract
            except Exception as e:
                print(f"  [EFETCH] Erreur parsing article : {e}")
                continue

    except Exception as e:
        print(f"  [EFETCH] Exception globale : {e}")

    print(f"  [EFETCH] {len(result)} abstracts parsés sur {len(pmids)} PMIDs demandés.")
    return result


def _lookup_and_update(
    df: pd.DataFrame,
    indices: List[int],
    allow_replace: bool,
    label: str,
    batch_size: int,
    max_workers: int,
) -> Tuple[int, int]:
    """
    indices : indices de lignes à traiter dans df
    allow_replace :
        - False : n'écrire que si l'abstract actuel est vide
        - True  : toujours écraser si on a un abstract PubMed
    label : étiquette pour les logs ("SANS abstract initial", "AVEC abstract initial")
    """
    if not indices:
        print(f"[{label}] Aucun article à traiter.")
        return 0, 0

    print(f"[{label}] Préparation des tâches pour {len(indices)} articles...")

    tasks: List[Tuple[int, str, Optional[str]]] = []
    for idx in indices:
        row = df.loc[idx]
        title = str(row["title"])
        doi = None if pd.isna(row.get("doi", None)) else str(row["doi"]).strip()
        tasks.append((idx, title, doi))

    # 1) Recherche PMIDs en parallèle
    index_pmid_pairs: List[Tuple[int, str]] = []
    print(f"[{label}] Recherche de PMIDs en parallèle...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(find_pmid_for_article, idx, title, doi): idx
            for (idx, title, doi) in tasks
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc=f"PMID {label}"):
            try:
                idx, pmid = future.result()
                if pmid:
                    index_pmid_pairs.append((idx, pmid))
            except Exception as e:
                print(f"[{label}] [PMID] Exception thread : {e}")

    print(f"[{label}] PMIDs trouvés pour {len(index_pmid_pairs)} articles sur {len(indices)}.\n")

    if not index_pmid_pairs:
        return 0, len(indices)

    # 2) Récup abstracts en batch
    pmids_all = [pmid for (_, pmid) in index_pmid_pairs]
    pmid_to_abstract: Dict[str, str] = {}

    for i in range(0, len(pmids_all), batch_size):
        batch = pmids_all[i:i + batch_size]
        print(f"[{label}] [BATCH] PMIDs {i} à {i + len(batch) - 1}")
        abs_batch = fetch_abstracts_for_pmids(batch)
        pmid_to_abstract.update(abs_batch)
        time.sleep(0.2)

    print(f"[{label}] Abstracts récupérés pour {len(pmid_to_abstract)} PMIDs.\n")

    # 3) Mise à jour df["abstract"]
    n_written = 0
    n_no_pubmed = 0

    for idx, pmid in index_pmid_pairs:
        new_abs = pmid_to_abstract.get(pmid)
        if not new_abs:
            n_no_pubmed += 1
            continue

        current_abs = df.at[idx, "abstract"]
        current_abs_str = "" if pd.isna(current_abs) else str(current_abs)

        if not allow_replace:
            if current_abs_str.strip() == "":
                df.at[idx, "abstract"] = new_abs
                n_written += 1
        else:
            df.at[idx, "abstract"] = new_abs
            n_written += 1

    print(f"[{label}] Résumé mise à jour :")
    print(f"  - Abstracts écrits/écrasés : {n_written}")
    print(f"  - Articles sans abstract PubMed : {n_no_pubmed}")
    print()

    return n_written, n_no_pubmed


def enrich_with_pubmed_abstracts(
    df: pd.DataFrame,
    email: str,
    api_key: str = "",
    max_articles: Optional[int] = None,
    batch_size: int = 100,
    max_workers: int = 6,
) -> Tuple[pd.DataFrame, dict]:
    """
    Complète/remplace les abstracts d'un DataFrame d'articles via PubMed.

    Colonnes requises : 'title', 'doi', 'abstract' (les autres sont laissées intactes).

    Retourne:
      - df_updated : DataFrame avec abstracts mis à jour
      - stats : dict avec quelques compteurs utiles
    """
    if "title" not in df.columns or "abstract" not in df.columns:
        raise ValueError("Le DataFrame doit contenir au moins les colonnes 'title' et 'abstract'.")

    # Config Entrez
    Entrez.email = email
    Entrez.api_key = api_key or None

    df = df.copy()

    # État initial des abstracts
    abstract_initial = df["abstract"].copy()
    abs_init_str = abstract_initial.fillna("").astype(str)

    mask_missing_initial = abs_init_str.str.strip() == ""
    mask_with_initial = ~mask_missing_initial

    idx_missing = df[mask_missing_initial].index.tolist()
    idx_with = df[mask_with_initial].index.tolist()

    print(f"[INFO] Articles SANS abstract initial : {len(idx_missing)}")
    print(f"[INFO] Articles AVEC abstract initial : {len(idx_with)}\n")

    if max_articles is not None:
        idx_missing = idx_missing[:max_articles]
        idx_with = idx_with[:max_articles]

    # PASS 1 : compléter abstracts manquants
    n_written_missing, n_no_pubmed_missing = _lookup_and_update(
        df,
        idx_missing,
        allow_replace=False,
        label="SANS abstract initial",
        batch_size=batch_size,
        max_workers=max_workers,
    )

    # PASS 2 : éventuellement améliorer abstracts existants
    n_written_with, n_no_pubmed_with = _lookup_and_update(
        df,
        idx_with,
        allow_replace=True,
        label="AVEC abstract initial",
        batch_size=batch_size,
        max_workers=max_workers,
    )

    # Garde-fou : ne jamais perdre un abstract initial non vide
    abs_final_str = df["abstract"].fillna("").astype(str)
    mask_lost = (abs_init_str.str.strip() != "") & (abs_final_str.str.strip() == "")
    n_restored = mask_lost.sum()
    if n_restored > 0:
        print(f"[SECURITE] Restauration de {n_restored} abstracts initiaux perdus par erreur.")
        df.loc[mask_lost, "abstract"] = abstract_initial[mask_lost]

    mask_missing_final = df["abstract"].isna() | (df["abstract"].astype(str).str.strip() == "")
    total_with_final = (~mask_missing_final).sum()

    print("[INFO] RÉCAP GLOBAL")
    print(f"  - Abstracts ajoutés pour articles SANS abstract initial : {n_written_missing}")
    print(f"  - Abstracts remplacés pour articles AVEC abstract initial : {n_written_with}")
    print(f"  - Abstracts initiaux restaurés (sécurité) : {n_restored}")
    print(f"  - Articles toujours sans abstract après PubMed : {mask_missing_final.sum()}")
    print(f"  - Total d'articles AVEC abstract après traitement : {total_with_final}\n")

    stats = {
        "written_missing": n_written_missing,
        "no_pubmed_missing": n_no_pubmed_missing,
        "written_with": n_written_with,
        "no_pubmed_with": n_no_pubmed_with,
        "restored_initial": n_restored,
        "still_missing": int(mask_missing_final.sum()),
        "total_with_abstract_final": int(total_with_final),
    }

    return df, stats
