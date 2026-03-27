# s2_crossref_recent.py
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import re
from html import unescape

BASE_URL = "https://api.crossref.org/works"

def clean_xml_text(text: str) -> str:
    """
    Supprime toutes les balises XML/HTML et leur contenu lorsque présent.
    Nettoie également les entités HTML & les espaces multiples.
    """
    if not text:
        return ""

    # 1) Enlever toutes les balises <tag>...</tag> (et leur contenu si nécessaire)
    # Exemple : <italic>foo</italic> -> ""
    text = re.sub(r"<[^>]*>", " ", text)

    # 2) décoder entités HTML (&amp; -> &)
    text = unescape(text)

    # 3) nettoyer espaces multiples
    text = re.sub(r"\s+", " ", text).strip()

    return text


def extract_published_date(item: dict):
    for key in ["published-print", "published-online", "published"]:
        if key in item and "date-parts" in item[key]:
            parts = item[key]["date-parts"][0]
            year = parts[0]
            month = parts[1] if len(parts) > 1 else 1
            day = parts[2] if len(parts) > 2 else 1
            return datetime(year, month, day).date()
    return None


def extract_authors(item: dict):
    authors = item.get("author", [])
    names = []
    for a in authors:
        given = a.get("given", "")
        family = a.get("family", "")
        full = " ".join([given, family]).strip()
        if full:
            names.append(full)
    return "; ".join(names)


def extract_abstract(item: dict) -> str:
    raw = item.get("abstract")
    if not raw:
        return ""
    txt = re.sub(r"<[^>]+>", " ", raw)
    txt = unescape(txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def build_params(issn, from_date, mailto, max_rows_per_issn):
    return {
        "filter": f"issn:{issn},from-pub-date:{from_date.date().isoformat()}",
        "mailto": mailto,
        "sort": "published",
        "order": "desc",
        "rows": max_rows_per_issn,
    }


async def fetch_one_issn(
    session,
    issn,
    from_date,
    mailto,
    max_rows_per_issn,
    sem,
    max_retries: int = 5,
    backoff_base: float = 2.0,
):
    """
    Récupère les articles pour un ISSN donné, avec gestion des 429 et 5xx.
    """
    for attempt in range(max_retries):
        async with sem:
            try:
                params = build_params(issn, from_date, mailto, max_rows_per_issn)
                async with session.get(BASE_URL, params=params, timeout=20) as resp:
                    status = resp.status

                    # ----- Gestion rate limiting -----
                    if status == 429:
                        retry_after = resp.headers.get("Retry-After")
                        if retry_after:
                            try:
                                delay = float(retry_after)
                            except ValueError:
                                delay = backoff_base ** attempt
                        else:
                            delay = backoff_base ** attempt
                        print(f"[{issn}] HTTP 429 (rate limit). Attente {delay:.1f}s puis retry...")
                        await asyncio.sleep(delay)
                        continue  # on retente

                    # ----- Gestion erreurs serveur -----
                    if 500 <= status < 600:
                        delay = backoff_base ** attempt
                        print(f"[{issn}] HTTP {status}. Attente {delay:.1f}s puis retry...")
                        await asyncio.sleep(delay)
                        continue

                    # ----- Autres erreurs HTTP -----
                    if status != 200:
                        print(f"[{issn}] HTTP {status}. Abandon pour cet ISSN.")
                        return []

                    # Si on est là : status == 200
                    data = await resp.json()

            except Exception as e:
                delay = backoff_base ** attempt
                print(f"[{issn}] ERREUR réseau ({e}). Attente {delay:.1f}s puis retry...")
                await asyncio.sleep(delay)
                continue

        # Si on sort du with sans exception ni continue, on a "data"
        break
    else:
        # boucle épuisée
        print(f"[{issn}] Échec après {max_retries} tentatives.")
        return []

    items = data.get("message", {}).get("items", [])
    results = []
    for it in items:
        pub_date = extract_published_date(it)
        if pub_date is None:
            continue

        title_list = it.get("title", [])
        raw_title = title_list[0] if title_list else ""
        title = clean_xml_text(raw_title)

        container_title_list = it.get("container-title", [])
        journal = container_title_list[0] if container_title_list else ""

        authors = extract_authors(it)
        doi = it.get("DOI", "")
        url = it.get("URL", "")
        abstract = extract_abstract(it)

        results.append(
            {
                "title": title,
                "authors": authors,
                "journal": journal,
                "published_date": pub_date.isoformat(),
                "doi": doi,
                "url": url,
                "issn": issn,
                "abstract": abstract,
            }
        )

    print(f"[{issn}] {len(results)} articles")
    return results


async def fetch_recent_articles_for_issn_df(
    df_issn: pd.DataFrame,
    days_back: int = 31,
    mailto: str = "sebastien.menigot@eseo.fr",
    max_rows_per_issn: int = 1000,
    max_concurrent_requests: int = 3,
) -> pd.DataFrame:
    """
    Version ASYNCHRONE.
    Entrée : df_issn avec colonne 'issn' ou 'ISSN'
    Sortie : DataFrame des articles filtrés.
    """
    if "issn" in df_issn.columns:
        col = "issn"
    elif "ISSN" in df_issn.columns:
        col = "ISSN"
    else:
        raise ValueError("df_issn doit contenir une colonne 'issn' ou 'ISSN'.")

    issn_list = (
        df_issn[col]
        .astype(str)
        .str.strip()
        .replace({"": None})
        .dropna()
        .unique()
    )

    print(f"Nombre d'ISSN uniques : {len(issn_list)}")

    today = datetime.utcnow()
    from_date = today - timedelta(days=days_back)
    print(f"Articles depuis le {from_date.date().isoformat()}")

    sem = asyncio.Semaphore(max_concurrent_requests)

    # User-Agent recommandé par CrossRef
    headers = {
        "User-Agent": f"biblio-bot/1.0 (mailto:{mailto})"
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [
            fetch_one_issn(
                session=session,
                issn=issn,
                from_date=from_date,
                mailto=mailto,
                max_rows_per_issn=max_rows_per_issn,
                sem=sem,
            )
            for issn in issn_list
        ]
        all_results = await asyncio.gather(*tasks)

    records = [rec for sublist in all_results for rec in sublist]

    if not records:
        print("Aucun article trouvé.")
        return pd.DataFrame(
            columns=[
                "title",
                "authors",
                "journal",
                "published_date",
                "doi",
                "url",
                "issn",
                "abstract",
            ]
        )

    df_out = pd.DataFrame(records)
    df_out["published_date"] = pd.to_datetime(df_out["published_date"]).dt.date
    df_out = df_out[df_out["published_date"] >= from_date.date()]

    #pattern = r"ultrasound|ultrasonic|doppler|beamforming|time reversal|harmonic|photoacoustic"
    #df_out = df_out[df_out["title"].str.contains(pattern, case=False, na=False)]

    print(f"✅ Fini. {len(df_out)} articles retenus après filtrage local.")
    return df_out
