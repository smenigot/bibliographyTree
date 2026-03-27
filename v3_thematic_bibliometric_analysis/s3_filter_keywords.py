# -*- coding: utf-8 -*-
# s3_filter_keywords.py
import pandas as pd
import re

def filter_by_keywords(
    df: pd.DataFrame,
    include_keywords: list[str] | None = None,
    exclude_keywords: list[str] | None = None,
    search_in_title: bool = True,
    search_in_abstract: bool = True,
    use_regex: bool = True,
) -> pd.DataFrame:
    """
    Filtre un DataFrame d'articles selon mots-clés à inclure et à exclure.

    - include_keywords : liste de patterns (regex ou texte brut)
    - exclude_keywords : liste de patterns à exclure
    - if use_regex=True   -> patterns interprétés comme regex
    - if use_regex=False  -> patterns échappés comme texte brut
    """

    df = df.copy()

    # Champs nécessaires
    df["title"] = df["title"].fillna("")
    df["abstract"] = df["abstract"].fillna("") if "abstract" in df.columns else ""

    # Si aucune recherche demandée → on retourne le df original
    if not (search_in_title or search_in_abstract):
        return df

    # -------------------------
    # 1) Inclusion
    # -------------------------
    if include_keywords:
        cleaned_inc = [k for k in include_keywords if isinstance(k, str) and k.strip()]
    
        if use_regex:
            # on fait confiance aux regex de l'utilisateur, sans rajouter de groupes
            inc_pattern = "|".join(cleaned_inc)
        else:
            # on échappe les mots-clés pour faire une regex "littérale"
            inc_pattern = "|".join(re.escape(k) for k in cleaned_inc)
    
        # masques initialisés à False
        mask_inc = pd.Series(False, index=df.index)
    
        if search_in_title:
            mask_inc |= df["title"].fillna("").str.contains(
                inc_pattern, case=False, regex=True, na=False
            )
    
        if search_in_abstract:
            mask_inc |= df["abstract"].fillna("").str.contains(
                inc_pattern, case=False, regex=True, na=False
            )
    else:
        # Pas de mots d'inclusion → tout est potentiellement inclus
        mask_inc = pd.Series(True, index=df.index)
    
    df_filtered = df[mask_inc].copy()
    
    # -------------------------
    # 2) Exclusion
    # -------------------------
    if exclude_keywords:
        cleaned_exc = [k for k in exclude_keywords if isinstance(k, str) and k.strip()]
    
        if use_regex:
            exc_pattern = "|".join(cleaned_exc)
        else:
            exc_pattern = "|".join(re.escape(k) for k in cleaned_exc)
    
        mask_exc = pd.Series(False, index=df_filtered.index)
    
        if search_in_title:
            mask_exc |= df_filtered["title"].fillna("").str.contains(
                exc_pattern, case=False, regex=True, na=False
            )
    
        if search_in_abstract:
            mask_exc |= df_filtered["abstract"].fillna("").str.contains(
                exc_pattern, case=False, regex=True, na=False
            )
    
        df_filtered = df_filtered[~mask_exc].copy()


    return df_filtered
