# -*- coding: utf-8 -*-
# s3_filter_keywords.py
import re

import pandas as pd


def _to_noncapturing_groups(pattern: str) -> str:
    """Convertit les groupes capturants `( ... )` en `(?: ... )`."""
    chars: list[str] = []
    in_char_class = False
    escaped = False

    for i, char in enumerate(pattern):
        if escaped:
            chars.append(char)
            escaped = False
            continue

        if char == "\\":
            chars.append(char)
            escaped = True
            continue

        if char == "[" and not in_char_class:
            in_char_class = True
            chars.append(char)
            continue

        if char == "]" and in_char_class:
            in_char_class = False
            chars.append(char)
            continue

        if char == "(" and not in_char_class:
            next_char = pattern[i + 1] if i + 1 < len(pattern) else ""
            if next_char != "?":
                chars.append("(?:")
                continue

        chars.append(char)

    return "".join(chars)


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

    # Si aucune recherche demandée, on retourne le df original
    if not (search_in_title or search_in_abstract):
        return df

    # -------------------------
    # 1) Inclusion
    # -------------------------
    if include_keywords:
        cleaned_inc = [k for k in include_keywords if isinstance(k, str) and k.strip()]

        if use_regex:
            # str.contains émet un warning si la regex contient des groupes capturants.
            inc_pattern = "|".join(_to_noncapturing_groups(k) for k in cleaned_inc)
        else:
            inc_pattern = "|".join(re.escape(k) for k in cleaned_inc)

        mask_inc = pd.Series(False, index=df.index)

        if search_in_title:
            mask_inc |= df["title"].str.contains(
                inc_pattern, case=False, regex=True, na=False
            )

        if search_in_abstract:
            mask_inc |= df["abstract"].str.contains(
                inc_pattern, case=False, regex=True, na=False
            )
    else:
        mask_inc = pd.Series(True, index=df.index)

    df_filtered = df[mask_inc].copy()

    # -------------------------
    # 2) Exclusion
    # -------------------------
    if exclude_keywords:
        cleaned_exc = [k for k in exclude_keywords if isinstance(k, str) and k.strip()]

        if use_regex:
            exc_pattern = "|".join(_to_noncapturing_groups(k) for k in cleaned_exc)
        else:
            exc_pattern = "|".join(re.escape(k) for k in cleaned_exc)

        mask_exc = pd.Series(False, index=df_filtered.index)

        if search_in_title:
            mask_exc |= df_filtered["title"].str.contains(
                exc_pattern, case=False, regex=True, na=False
            )

        if search_in_abstract:
            mask_exc |= df_filtered["abstract"].str.contains(
                exc_pattern, case=False, regex=True, na=False
            )

        df_filtered = df_filtered[~mask_exc].copy()

    return df_filtered
