# -*- coding: utf-8 -*-
# s4_llm_relevance.py

import gc
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer

try:
    import torch
except Exception:
    torch = None

_MODEL_CACHE = {"name": None, "model": None}


def _get_model(model_name: str) -> SentenceTransformer:
    cached_name = _MODEL_CACHE["name"]
    if cached_name == model_name and _MODEL_CACHE["model"] is not None:
        return _MODEL_CACHE["model"]

    _MODEL_CACHE["name"] = model_name
    _MODEL_CACHE["model"] = SentenceTransformer(model_name)
    return _MODEL_CACHE["model"]


def score_articles_with_llm(
    df: pd.DataFrame,
    domain_description: str,
    model_name: str = "sentence-transformers/all-MiniLM-L12-v2",
    relevance_threshold: float = 0.7,
    batch_size: int = 256,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calcule une pertinence par similarite cosinus entre chaque article et le texte
    de 'domain_description'. Ajoute les colonnes 'relevance' et 'label' au DataFrame.

    Retourne:
      - df_scored: DataFrame trie par pertinence decroissante
      - df_relevant: sous-ensemble avec relevance >= relevance_threshold
    """

    if df.empty:
        df = df.copy()
        df["relevance"] = np.nan
        df["label"] = ""
        # aucun pertinent si df vide
        return df, df.iloc[0:0]

    df = df.copy()
    df["title"] = df["title"].fillna("")

    if "abstract" in df.columns:
        df["abstract"] = df["abstract"].fillna("")
    else:
        df["abstract"] = ""

    # Texte a embedder : titre + abstract
    texts = (df["title"] + ". Abstract: " + df["abstract"]).tolist()

    # Modele d'embedding (cache simple pour eviter rechargements repetes)
    model = _get_model(model_name)

    if torch is not None:
        with torch.no_grad():
            # Embedding des centres d'interet
            emb_interest = model.encode(
                domain_description,
                normalize_embeddings=True,
            )

            # Embedding de tous les articles
            emb_texts = model.encode(
                texts,
                batch_size=batch_size,
                show_progress_bar=True,
                normalize_embeddings=True,
            )
    else:
        # Embedding des centres d'interet
        emb_interest = model.encode(
            domain_description,
            normalize_embeddings=True,
        )

        # Embedding de tous les articles
        emb_texts = model.encode(
            texts,
            batch_size=batch_size,
            show_progress_bar=True,
            normalize_embeddings=True,
        )

    emb_interest = np.asarray(emb_interest)
    emb_texts = np.asarray(emb_texts)

    # Similarite cosinus (produit scalaire car vecteurs normalises)
    cos_sims = np.dot(emb_texts, emb_interest)  # in [-1, 1]
    relevance = (cos_sims + 1.0) / 2.0          # remap in [0, 1]

    df["relevance"] = relevance
    df["label"] = np.where(
        df["relevance"] >= relevance_threshold,
        "pertinent",
        "hors_sujet",
    )

    # Tri decroissant
    df_scored = df.sort_values("relevance", ascending=False)

    # Sous-ensemble pertinent
    df_relevant = df_scored[df_scored["relevance"] >= relevance_threshold].copy()

    # Liberer au plus tot les gros buffers d'embeddings
    del emb_texts, emb_interest, cos_sims, relevance
    if torch is not None and torch.cuda.is_available():
        torch.cuda.empty_cache()
    gc.collect()

    return df_scored, df_relevant
