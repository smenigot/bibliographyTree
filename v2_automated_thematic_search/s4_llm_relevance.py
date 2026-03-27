# -*- coding: utf-8 -*-
# s4_llm_relevance.py

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer


def score_articles_with_llm(
    df: pd.DataFrame,
    domain_description: str,
    model_name: str = "sentence-transformers/all-MiniLM-L12-v2",
    relevance_threshold: float = 0.7,
    batch_size: int = 256,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calcule une pertinence par similarité cosinus entre chaque article et le texte
    de 'domain_description'. Ajoute les colonnes 'relevance' et 'label' au DataFrame.

    Retourne:
      - df_scored: DataFrame trié par pertinence décroissante
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

    # Texte à embedder : titre + abstract
    texts = (df["title"] + ". Abstract: " + df["abstract"]).tolist()

    # Modèle d'embedding
    model = SentenceTransformer(model_name)

    # Embedding des centres d'intérêt
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

    # Similarité cosinus (produit scalaire car vecteurs normalisés)
    cos_sims = np.dot(emb_texts, emb_interest)  # ∈ [-1, 1]
    relevance = (cos_sims + 1.0) / 2.0         # remap en [0, 1]

    df["relevance"] = relevance
    df["label"] = np.where(
        df["relevance"] >= relevance_threshold,
        "pertinent",
        "hors_sujet",
    )

    # Tri décroissant
    df_scored = df.sort_values("relevance", ascending=False)

    # Sous-ensemble pertinent
    df_relevant = df_scored[df_scored["relevance"] >= relevance_threshold].copy()

    return df_scored, df_relevant
