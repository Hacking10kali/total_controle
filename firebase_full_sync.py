#!/usr/bin/env python3
"""
firebase_full_sync.py — Vérifie tout le catalogue anime-sama vs Firestore (OtakuFlix).

- Animé absent → scrape complet + ajout Firestore
- Saison / épisodes manquants → scrape (priorité VF, sinon VOSTFR)
- Vérifie ids.jikan_id (MAL) + ids.kitsu_id + sync collection ids/
- En fin de run : reconstruction ids/jikan_id et ids/kitsu_id

Usage:
  python firebase_full_sync.py --dry-run --page-begin 1 --page-end 2
  python firebase_full_sync.py --page-begin 1 --page-end 43

Secret GitHub: FIREBASE_SERVICE_ACCOUNT_JSON
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path

import aiohttp
from playwright.async_api import async_playwright

BASE_URL = "https://anime-sama.to"
CATALOGUE_URL = "https://anime-sama.to/catalogue/?page={page}"
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "cfc454f98433e15eaa3b67f178fd8774")
TMDB_BASE = "https://api.themoviedb.org/3"
JIKAN_BASE = "https://api.jikan.moe/v4"
KITSU_BASE = "https://kitsu.io/api/edge"
EPISODE_DELAY = float(os.environ.get("EPISODE_DELAY", "0.25"))
JIKAN_DELAY = 0.35
JIKAN_MATCH_MIN = 0.50
KITSU_MATCH_MIN = 0.50
PREFERRED_LANGUES = ["VF", "VOSTFR"]
MAX_RETRIES = 3
MAX_EPISODES_PER_CHUNK = 50
STATE_FILE = Path(__file__).resolve().parent / "sync_progress.json"

_start = time.time()


def log(msg: str):
    e = int(time.time() - _start)
    print(f"[{e//60:02d}m{e%60:02d}s] {msg}", flush=True)


# ─── Firestore ────────────────────────────────────────────────
def _strip_diacritics(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def slugify_compact_id(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", _strip_diacritics(str(s)).lower())


def parse_episode_num(episode_title: str, fallback_num: int) -> int:
    m = re.search(r"(\d+)", episode_title or "")
    if not m:
        return fallback_num
    try:
        return int(m.group(1))
    except ValueError:
        return fallback_num


def build_saison_id(anime_id: str, titre_vignette: str, langue: str) -> str:
    return f"{anime_id}_{slugify_compact_id(titre_vignette)}_{langue.lower()}"


def scraped_episodes_to_firestore(episodes_in: list, start_num: int = 1) -> list:
    out = []
    for i, ep in enumerate(episodes_in):
        raw = ep.get("episode", "")
        out.append({
            "num": parse_episode_num(raw, start_num + i),
            "titre": raw,
            "lecteurs": ep.get("lecteurs", []),
        })
    return out


def anime_doc_from_scraped(anime_data: dict, anime_id: str) -> dict:
    saisons_out = []
    for saison in anime_data.get("saisons", []):
        eps = saison.get("episodes", []) or []
        tv = saison.get("titreVignette", "")
        lang = str(saison.get("langue", "") or "").lower()
        sid = build_saison_id(anime_id, tv, lang)
        saisons_out.append({
            "id": sid,
            "titreVignette": tv,
            "titreComplet": saison.get("titreComplet", tv),
            "langue": lang,
            "parts": math.ceil(len(eps) / MAX_EPISODES_PER_CHUNK) if eps else 0,
        })
    return {
        "nom": anime_data.get("nom"),
        "type": anime_data.get("type"),
        "genres": anime_data.get("genres", []),
        "langues": anime_data.get("langues", []),
        "image": anime_data.get("image"),
        "noms_alt": anime_data.get("noms_alt", []),
        "synopsis": anime_data.get("synopsis"),
        "bande_annonce": anime_data.get("bande_annonce"),
        "ids": anime_data.get("ids", {}),
        "saisons": saisons_out,
    }


def chunks_from_saison(anime_id: str, saison_id: str, episodes_in: list) -> list:
    if not episodes_in:
        return []
    eps = scraped_episodes_to_firestore(episodes_in)
    writes = []
    for part_index in range(math.ceil(len(eps) / MAX_EPISODES_PER_CHUNK)):
        part = part_index + 1
        start = part_index * MAX_EPISODES_PER_CHUNK + 1
        end = min((part_index + 1) * MAX_EPISODES_PER_CHUNK, len(eps))
        slice_eps = eps[start - 1 : end]
        writes.append((f"{saison_id}_part{part}", {
            "saisonId": saison_id, "animeId": anime_id, "part": part,
            "start": start, "end": end, "episodes": slice_eps, "count": len(slice_eps),
        }))
    return writes


def merge_episodes_into_chunks(existing_chunks: list, new_episodes: list):
    if not new_episodes or not existing_chunks:
        return [], False
    chunks = sorted(existing_chunks, key=lambda c: c.get("part", 0))
    all_eps = []
    for ch in chunks:
        all_eps.extend(ch.get("episodes", []))
    existing_nums = {int(e["num"]) for e in all_eps}
    for ep in new_episodes:
        if int(ep["num"]) not in existing_nums:
            all_eps.append(ep)
    if len(all_eps) == sum(len(c.get("episodes", [])) for c in chunks):
        return [], False
    all_eps.sort(key=lambda e: int(e["num"]))
    sid, aid = chunks[0].get("saisonId", ""), chunks[0].get("animeId", "")
    writes = []
    for part_index in range(math.ceil(len(all_eps) / MAX_EPISODES_PER_CHUNK)):
        part = part_index + 1
        start = part_index * MAX_EPISODES_PER_CHUNK + 1
        end = min((part_index + 1) * MAX_EPISODES_PER_CHUNK, len(all_eps))
        writes.append((f"{sid}_part{part}", {
            "saisonId": sid, "animeId": aid, "part": part,
            "start": start, "end": end,
            "episodes": all_eps[start - 1 : end], "count": end - start + 1,
        }))
    return writes, True


def _load_firebase_service_account() -> dict:
    raw = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON", "").strip().lstrip("\ufeff")
    if raw:
        return json.loads(raw)
    for key in ("GOOGLE_APPLICATION_CREDENTIALS", "FIREBASE_SERVICE_ACCOUNT"):
        path = os.environ.get(key, "").strip()
        if path and os.path.isfile(path):
            return json.loads(Path(path).read_text(encoding="utf-8-sig").strip())
    raise RuntimeError("Credentials Firebase manquants (FIREBASE_SERVICE_ACCOUNT_JSON).")


def init_firestore():
    import firebase_admin
    from firebase_admin import credentials, firestore

    if firebase_admin._apps:
        return firestore.client()
    firebase_admin.initialize_app(credentials.Certificate(_load_firebase_service_account()))
    return firestore.client()


class FirestoreSync:
    def __init__(self, db):
        self.db = db

    def get_anime(self, anime_id: str):
        doc = self.db.collection("animes").document(anime_id).get()
        return doc.to_dict() if doc.exists else None

    def get_saison_chunks(self, saison_id: str, parts: int) -> list:
        chunks = []
        for part in range(1, max(parts, 1) + 1):
            doc = self.db.collection("saison_chunks").document(f"{saison_id}_part{part}").get()
            if doc.exists:
                chunks.append(doc.to_dict())
        return chunks

    def count_episodes(self, saison_id: str, parts: int) -> int:
        return sum(len(c.get("episodes", [])) for c in self.get_saison_chunks(saison_id, parts))

    def upsert(self, collection: str, writes: list, merge: bool = True):
        if not writes:
            return
        col = self.db.collection(collection)
        batch, n = self.db.batch(), 0
        for doc_id, data in writes:
            batch.set(col.document(doc_id), data, merge=merge)
            n += 1
            if n >= 400:
                batch.commit()
                batch, n = self.db.batch(), 0
        if n:
            batch.commit()

    def write_full_anime(self, anime_id: str, anime_data: dict):
        chunk_writes = []
        for saison in anime_data.get("saisons", []):
            eps = saison.get("episodes", []) or []
            if not eps:
                continue
            tv = saison.get("titreVignette", "")
            lang = str(saison.get("langue", "") or "").lower()
            sid = build_saison_id(anime_id, tv, lang)
            chunk_writes.extend(chunks_from_saison(anime_id, sid, eps))
        self.upsert("animes", [(anime_id, anime_doc_from_scraped(anime_data, anime_id))], merge=False)
        self.upsert("saison_chunks", chunk_writes, merge=False)

    def append_episodes(self, anime_id, saison_id, titre_v, titre_c, langue, scraped_eps) -> int:
        anime_doc = self.get_anime(anime_id)
        if not anime_doc:
            return 0
        new_eps = scraped_episodes_to_firestore(scraped_eps)
        meta = next((s for s in anime_doc.get("saisons", []) if s.get("id") == saison_id), None)
        if meta:
            chunks = self.get_saison_chunks(saison_id, int(meta.get("parts", 0)))
            if not chunks:
                cw = chunks_from_saison(anime_id, saison_id, scraped_eps)
                for s in anime_doc["saisons"]:
                    if s["id"] == saison_id:
                        s["parts"] = len(cw)
                self.upsert("animes", [(anime_id, anime_doc)], merge=True)
                self.upsert("saison_chunks", cw, merge=False)
                return len(new_eps)
            cw, changed = merge_episodes_into_chunks(chunks, new_eps)
            if not changed:
                return 0
            np = max(c[1]["part"] for c in cw)
            if np != meta.get("parts"):
                for s in anime_doc["saisons"]:
                    if s["id"] == saison_id:
                        s["parts"] = np
                self.upsert("animes", [(anime_id, anime_doc)], merge=True)
            before = sum(len(c.get("episodes", [])) for c in chunks)
            self.upsert("saison_chunks", cw, merge=True)
            after = sum(len(c[1].get("episodes", [])) for c in cw)
            return max(0, after - before)
        cw = chunks_from_saison(anime_id, saison_id, scraped_eps)
        anime_doc.setdefault("saisons", []).append({
            "id": saison_id, "titreVignette": titre_v, "titreComplet": titre_c,
            "langue": langue.lower(), "parts": len(cw),
        })
        self.upsert("animes", [(anime_id, anime_doc)], merge=True)
        self.upsert("saison_chunks", cw, merge=False)
        return len(new_eps)

    def update_anime_ids(self, anime_id: str, ids: dict):
        anime_doc = self.get_anime(anime_id)
        if not anime_doc:
            return
        anime_doc["ids"] = ids
        self.upsert("animes", [(anime_id, anime_doc)], merge=True)

    def register_global_jikan_id(self, mal_id) -> bool:
        if mal_id is None:
            return False
        try:
            mal_id = int(mal_id)
        except (TypeError, ValueError):
            return False
        doc_ref = self.db.collection("ids").document("jikan_id")
        doc = doc_ref.get()
        current = list(doc.to_dict().get("jikan_id", [])) if doc.exists else []
        if mal_id in current:
            return False
        current.append(mal_id)
        current.sort()
        doc_ref.set({"jikan_id": current, "count": len(current)}, merge=True)
        return True

    def register_global_kitsu_id(self, kitsu_id) -> bool:
        if kitsu_id is None or str(kitsu_id).strip() in ("", "0", "None"):
            return False
        kid = str(kitsu_id).strip()
        doc_ref = self.db.collection("ids").document("kitsu_id")
        doc = doc_ref.get()
        current = [str(x).strip() for x in doc.to_dict().get("kitsu_id", [])] if doc.exists else []
        if kid in current:
            return False
        current.append(kid)
        current.sort(key=lambda x: int(x) if x.isdigit() else x)
        doc_ref.set({"kitsu_id": current, "count": len(current)}, merge=True)
        return True

    def mal_in_global_registry(self, mal_id) -> bool:
        try:
            mal_id = int(mal_id)
        except (TypeError, ValueError):
            return False
        doc = self.db.collection("ids").document("jikan_id").get()
        if not doc.exists:
            return False
        return mal_id in doc.to_dict().get("jikan_id", [])

    def kitsu_in_global_registry(self, kitsu_id) -> bool:
        kid = str(kitsu_id).strip()
        doc = self.db.collection("ids").document("kitsu_id").get()
        if not doc.exists:
            return False
        return kid in [str(x).strip() for x in doc.to_dict().get("kitsu_id", [])]

    def sync_global_ids_registry(self, ids: dict) -> dict:
        """Ajoute jikan_id + kitsu_id dans collection ids/ si absents."""
        report = {"jikan_added": False, "kitsu_added": False}
        if ids.get("jikan_id"):
            report["jikan_added"] = self.register_global_jikan_id(ids["jikan_id"])
        if ids.get("kitsu_id"):
            report["kitsu_added"] = self.register_global_kitsu_id(ids["kitsu_id"])
        return report

    def rebuild_global_ids_registry(self):
        """Reconstruit ids/jikan_id et ids/kitsu_id depuis toute la collection animes."""
        mal_ids: set[int] = set()
        kitsu_ids: set[str] = set()
        for doc in self.db.collection("animes").stream():
            ids = (doc.to_dict() or {}).get("ids") or {}
            j = ids.get("jikan_id")
            k = ids.get("kitsu_id")
            if j is not None:
                try:
                    mal_ids.add(int(j))
                except (TypeError, ValueError):
                    pass
            if k is not None and str(k).strip() not in ("", "0", "None"):
                kitsu_ids.add(str(k).strip())
        mal_sorted = sorted(mal_ids)
        kitsu_sorted = sorted(kitsu_ids, key=lambda x: int(x) if x.isdigit() else x)
        self.db.collection("ids").document("jikan_id").set({
            "jikan_id": mal_sorted, "count": len(mal_sorted),
        })
        self.db.collection("ids").document("kitsu_id").set({
            "kitsu_id": kitsu_sorted, "count": len(kitsu_sorted),
        })
        log(f"  ids/ reconstruit — jikan:{len(mal_sorted)} kitsu:{len(kitsu_sorted)}")
        return len(mal_sorted), len(kitsu_sorted)


# ─── Scraping ─────────────────────────────────────────────────
def new_ctx(browser):
    return browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        locale="fr-FR",
    )


def build_url(href):
    if not href:
        return None
    if href.startswith("http"):
        return href
    return BASE_URL + ("" if href.startswith("/") else "/") + href


def slug_from_url(url):
    m = re.search(r"/catalogue/([^/]+)/?$", url or "")
    return m.group(1) if m else None


def clean_title(title):
    t = re.sub(r"\s*(saison|season|partie|part|film)\s*\d*", "", title, flags=re.I)
    return re.sub(r"\s*\d+$", "", t).strip()


def normalize_title(s: str) -> str:
    s = _strip_diacritics((s or "").lower())
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def title_similarity(a: str, b: str) -> float:
    na, nb = normalize_title(a), normalize_title(b)
    if not na or not nb:
        return 0.0
    if na == nb or na in nb or nb in na:
        return 0.95
    return SequenceMatcher(None, na, nb).ratio()


def jikan_entry_titles(entry: dict) -> list[str]:
    titles = []
    for key in ("title", "title_english", "title_japanese", "title_french"):
        t = entry.get(key)
        if t and str(t).strip():
            titles.append(str(t).strip())
    for alt in entry.get("titles", []) or []:
        if isinstance(alt, dict) and alt.get("title"):
            titles.append(str(alt["title"]).strip())
    return titles


def best_title_match_score(entry: dict, reference_titles: list[str]) -> float:
    if not entry or not reference_titles:
        return 0.0
    jt = jikan_entry_titles(entry)
    return max(
        (title_similarity(ref, jtitle) for ref in reference_titles for jtitle in jt),
        default=0.0,
    )


def ids_differ(stored: dict, new: dict) -> bool:
    for key in ("jikan_id", "tmdb_id", "kitsu_id"):
        s, n = stored.get(key), new.get(key)
        if n is None or n == "" or n == 0:
            continue
        try:
            if int(s or 0) != int(n):
                return True
        except (TypeError, ValueError):
            if str(s) != str(n):
                return True
    for key in ("jikan_id", "tmdb_id", "kitsu_id"):
        n = new.get(key)
        s = stored.get(key)
        if n and not s:
            return True
    return False


def build_saison_url(anime_lien, titre, langue):
    slug = slug_from_url(anime_lien)
    if not slug:
        return None
    t = titre.lower().strip()
    if "film" in t:
        num = re.search(r"\d+", t)
        segment = "film" + (num.group() if num else "")
    else:
        s_num = re.search(r"saison\s*(\d+)", t)
        p_num = re.search(r"partie\s*(\d+)", t)
        if s_num:
            segment = "saison" + s_num.group(1) + (("-partie" + p_num.group(1)) if p_num else "")
        elif p_num:
            segment = "partie" + p_num.group(1)
        else:
            num = re.search(r"\d+", t)
            segment = "saison" + (num.group() if num else "1")
    return f"{BASE_URL}/catalogue/{slug}/{segment}/{langue.lower()}/"


def parse_info_rows(rows):
    info = {"genres": [], "type": None, "langues": []}
    for row in rows:
        label = row.get("label", "").lower()
        value = row.get("value", "").strip()
        if "genre" in label:
            info["genres"] = [g.strip() for g in value.split(",") if g.strip()]
        elif "type" in label:
            info["type"] = value
        elif "lang" in label:
            info["langues"] = normalize_langues_priority(
                [l.strip() for l in value.split(",") if l.strip()]
            )
    return info


def normalize_langues_priority(langues: list | None) -> list[str]:
    """VF toujours en premier pour OtakuFlix."""
    items = [str(l).strip() for l in (langues or []) if str(l).strip()]
    if not items:
        return list(PREFERRED_LANGUES)
    out: list[str] = []
    upper = {l.upper() for l in items}
    if "VF" in upper:
        out.append("VF")
    if "VOSTFR" in upper:
        out.append("VOSTFR")
    for lang in items:
        if lang.upper() not in ("VF", "VOSTFR"):
            out.append(lang)
    return out or list(PREFERRED_LANGUES)


async def pick_saison_url_vf_first(
    session, anime_lien: str, titre: str,
) -> tuple[str | None, str]:
    """Choisit l'URL saison : VF si disponible, sinon VOSTFR."""
    url_vf = build_saison_url(anime_lien, titre, "vf")
    url_vostfr = build_saison_url(anime_lien, titre, "vostfr")
    if url_vf and await check_url(session, url_vf):
        return url_vf, "vf"
    if url_vostfr and await check_url(session, url_vostfr):
        return url_vostfr, "vostfr"
    return url_vf or url_vostfr, "vf"


async def goto_page(page, url):
    for strategy in ("networkidle", "domcontentloaded", "load"):
        try:
            await page.goto(url, wait_until=strategy, timeout=45000)
            return True
        except Exception:
            pass
    return False


async def wait_select(page, selector, timeout=20000):
    for _ in range(MAX_RETRIES):
        try:
            await page.wait_for_selector(selector, timeout=timeout)
            n = await page.evaluate(
                f"() => document.querySelector('{selector}')?.options.length || 0"
            )
            if n > 0:
                return True
            await page.wait_for_timeout(800)
        except Exception:
            await page.wait_for_timeout(1000)
    return False


async def get_options(page, selector):
    for _ in range(MAX_RETRIES):
        opts = await page.evaluate(
            "() => { const s=document.querySelector('" + selector + "'); "
            "return s ? Array.from(s.options).map(o=>({value:o.value,label:o.text.trim()})) : []; }"
        )
        if opts:
            return opts
        await page.wait_for_timeout(800)
    return []


async def read_player(page):
    return await page.evaluate(
        "() => { const f=document.querySelector('#playerDF'); if(!f) return null;"
        "let s=f.getAttribute('src')||f.getAttribute('data-src');"
        "if(s&&s.length>10&&!s.includes('about:blank'))return s;"
        "for(const el of f.querySelectorAll('iframe,[src],[data-src]')){"
        "const v=el.getAttribute('src')||el.getAttribute('data-src')||'';"
        "if(v.length>10&&!v.includes('about:blank'))return v;} return null; }"
    )


async def wait_player(page, old_src="", timeout=6000):
    for attempt in range(MAX_RETRIES):
        try:
            await page.wait_for_function(
                "(old)=>{const f=document.querySelector('#playerDF');if(!f)return false;"
                "const srcs=[f.getAttribute('src')||'',f.getAttribute('data-src')||'',"
                "...[...f.querySelectorAll('iframe,[src],[data-src]')].map(e=>e.getAttribute('src')||e.getAttribute('data-src')||'')]"
                ".filter(s=>s.length>10&&!s.includes('about:blank'));return srcs.length>0&&srcs[0]!==old;}",
                arg=old_src, timeout=timeout,
            )
        except Exception:
            pass
        src = await read_player(page)
        if src and src != old_src:
            return src
        await page.wait_for_timeout(700 * (attempt + 1))
    return await read_player(page)


async def check_url(session, url):
    try:
        async with session.head(url, timeout=aiohttp.ClientTimeout(total=6), allow_redirects=True) as r:
            return r.status == 200
    except Exception:
        return False


async def jikan_get_by_mal_id(session, mal_id) -> dict | None:
    if not mal_id:
        return None
    await asyncio.sleep(JIKAN_DELAY)
    try:
        async with session.get(
            f"{JIKAN_BASE}/anime/{int(mal_id)}",
            timeout=aiohttp.ClientTimeout(total=12),
        ) as r:
            if r.status == 200:
                return (await r.json()).get("data")
    except Exception:
        pass
    return None


async def jikan_search(session, query: str, is_film: bool = False, limit: int = 8) -> list:
    if not query:
        return []
    await asyncio.sleep(JIKAN_DELAY)
    media = "movie" if is_film else "tv"
    urls = [
        f"{JIKAN_BASE}/anime?q={query}&type={media}&limit={limit}",
        f"{JIKAN_BASE}/anime?q={query}&limit={limit}",
    ]
    for url in urls:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status == 200:
                    data = (await r.json()).get("data", [])
                    if data:
                        return data
        except Exception:
            pass
    return []


async def jikan_search_best_match(session, reference_titles: list[str], is_film: bool = False):
    seen_mal, candidates = set(), []
    for title in reference_titles:
        query = clean_title(title)
        if not query:
            continue
        for entry in await jikan_search(session, query, is_film=is_film):
            mal = entry.get("mal_id")
            if not mal or mal in seen_mal:
                continue
            seen_mal.add(mal)
            score = best_title_match_score(entry, reference_titles)
            candidates.append((score, entry))
    if not candidates:
        return None, 0.0
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1], candidates[0][0]


async def get_jikan_id(session, title, is_film=False):
    entry, score = await jikan_search_best_match(session, [title], is_film=is_film)
    if entry and score >= JIKAN_MATCH_MIN:
        return entry.get("mal_id")
    return None


async def get_tmdb_id(session, title, is_film=False):
    query = clean_title(title)
    if not query:
        return None
    media = "movie" if is_film else "tv"
    try:
        async with session.get(
            f"{TMDB_BASE}/search/{media}?api_key={TMDB_API_KEY}&query={query}&language=fr-FR&page=1",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            results = (await r.json()).get("results", []) if r.status == 200 else []
            return results[0].get("id") if results else None
    except Exception:
        return None


async def get_kitsu_id(session, title, is_film=False):
    query = clean_title(title)
    if not query:
        return None
    hdrs = {"Accept": "application/vnd.api+json"}
    subtype = "movie" if is_film else "TV"
    try:
        async with session.get(
            f"{KITSU_BASE}/anime?filter[text]={query}&filter[subtype]={subtype}&page[limit]=1",
            headers=hdrs, timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            data = (await r.json()).get("data", []) if r.status == 200 else []
        if not data:
            async with session.get(f"{KITSU_BASE}/anime?filter[text]={query}&page[limit]=1",
                                   headers=hdrs, timeout=aiohttp.ClientTimeout(total=10)) as r2:
                data = (await r2.json()).get("data", []) if r2.status == 200 else []
        return data[0].get("id") if data else None
    except Exception:
        return None


async def fetch_ids(session, title, is_film=False, noms_alt=None):
    refs = [title] + [a for a in (noms_alt or []) if a]
    entry, score = await jikan_search_best_match(session, refs, is_film=is_film)
    t, k = await asyncio.gather(
        get_tmdb_id(session, title, is_film),
        get_kitsu_id(session, title, is_film),
    )
    j = entry.get("mal_id") if entry and score >= JIKAN_MATCH_MIN else None
    return {"jikan_id": j, "tmdb_id": t, "kitsu_id": k}


async def kitsu_get_by_id(session, kitsu_id) -> dict | None:
    if not kitsu_id:
        return None
    hdrs = {"Accept": "application/vnd.api+json"}
    try:
        async with session.get(
            f"{KITSU_BASE}/anime/{str(kitsu_id).strip()}",
            headers=hdrs,
            timeout=aiohttp.ClientTimeout(total=12),
        ) as r:
            if r.status == 200:
                return (await r.json()).get("data", {}).get("attributes")
    except Exception:
        pass
    return None


async def kitsu_search_best_match(session, reference_titles: list[str], is_film: bool = False):
    seen, candidates = set(), []
    hdrs = {"Accept": "application/vnd.api+json"}
    subtype = "movie" if is_film else "TV"
    for title in reference_titles:
        query = clean_title(title)
        if not query:
            continue
        for url in (
            f"{KITSU_BASE}/anime?filter[text]={query}&filter[subtype]={subtype}&page[limit]=8",
            f"{KITSU_BASE}/anime?filter[text]={query}&page[limit]=8",
        ):
            try:
                async with session.get(url, headers=hdrs, timeout=aiohttp.ClientTimeout(total=12)) as r:
                    if r.status != 200:
                        continue
                    for item in (await r.json()).get("data", []):
                        kid = item.get("id")
                        if not kid or kid in seen:
                            continue
                        seen.add(kid)
                        attrs = item.get("attributes") or {}
                        titles = [attrs.get("canonicalTitle"), attrs.get("titles", {}).get("en")]
                        titles = [t for t in titles if t]
                        score = max(
                            (title_similarity(ref, t) for ref in reference_titles for t in titles),
                            default=0.0,
                        )
                        candidates.append((score, kid, attrs.get("canonicalTitle")))
            except Exception:
                pass
    if not candidates:
        return None, 0.0
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1], candidates[0][0]


async def resolve_verified_ids(
    session,
    nom: str,
    noms_alt: list | None,
    type_str: str,
    stored: dict | None,
) -> tuple[dict, str]:
    """Retourne (ids, raison). jikan_id = MAL id ; vérifie aussi kitsu + tmdb."""
    stored = dict(stored or {})
    refs = [nom] + [a for a in (noms_alt or []) if a]
    refs = [r for r in refs if r]
    is_film = "film" in (type_str or "").lower()
    reasons: list[str] = []

    result = {
        "jikan_id": stored.get("jikan_id"),
        "tmdb_id": stored.get("tmdb_id"),
        "kitsu_id": stored.get("kitsu_id"),
    }

    stored_mal = stored.get("jikan_id")
    mal_ok = False
    if stored_mal:
        entry = await jikan_get_by_mal_id(session, stored_mal)
        if entry and best_title_match_score(entry, refs) >= 0.45:
            result["jikan_id"] = int(entry["mal_id"])
            mal_ok = True
            reasons.append(f"mal_ok")

    if not mal_ok:
        best, score = await jikan_search_best_match(session, refs, is_film=is_film)
        if best and score >= JIKAN_MATCH_MIN:
            result["jikan_id"] = int(best["mal_id"])
            reasons.append(f"mal_corrige({score:.2f})")

    extra = await fetch_ids(session, nom, is_film, noms_alt)
    if not result.get("tmdb_id") and extra.get("tmdb_id"):
        result["tmdb_id"] = extra.get("tmdb_id")

    stored_kitsu = stored.get("kitsu_id")
    kitsu_ok = False
    if stored_kitsu:
        attrs = await kitsu_get_by_id(session, stored_kitsu)
        if attrs:
            ktitles = [attrs.get("canonicalTitle"), (attrs.get("titles") or {}).get("en_jp")]
            ktitles = [t for t in ktitles if t]
            if max((title_similarity(r, t) for r in refs for t in ktitles), default=0) >= 0.45:
                result["kitsu_id"] = str(stored_kitsu).strip()
                kitsu_ok = True
                reasons.append("kitsu_ok")

    if not kitsu_ok:
        kid, kscore = await kitsu_search_best_match(session, refs, is_film=is_film)
        if kid and kscore >= KITSU_MATCH_MIN:
            result["kitsu_id"] = str(kid)
            reasons.append(f"kitsu_corrige({kscore:.2f})")
        elif extra.get("kitsu_id"):
            result["kitsu_id"] = str(extra.get("kitsu_id"))
            reasons.append("kitsu_search")

    reason = ", ".join(reasons) if reasons else "ids_introuvables"
    return result, reason


async def collect_lecteurs(page):
    lecteurs = []
    for lect in await get_options(page, "#selectLecteurs"):
        try:
            await page.select_option("#selectLecteurs", value=lect["value"])
            await page.wait_for_timeout(300)
        except Exception:
            continue
        src = None
        for wait_ms in (500, 800, 1200, 2000, 3000):
            await page.wait_for_timeout(wait_ms)
            src = await read_player(page)
            if src:
                break
        if src:
            lecteurs.append({"lecteur": lect["label"], "url": src})
    return lecteurs


async def scrape_saison_episodes(browser, saison_url):
    slug = "/".join(saison_url.rstrip("/").split("/")[-2:])
    for attempt in range(MAX_RETRIES):
        ctx = await new_ctx(browser)
        page = await ctx.new_page()
        episodes, success = [], False
        try:
            await goto_page(page, saison_url)
            if await wait_select(page, "#selectEpisodes", timeout=25000):
                await page.wait_for_timeout(500)
                eps_opts = await get_options(page, "#selectEpisodes")
                if eps_opts:
                    log(f"    scrape {len(eps_opts)} ep [{slug}]")
                    for ep in eps_opts:
                        try:
                            await page.select_option("#selectEpisodes", value=ep["value"])
                            await page.wait_for_timeout(400)
                            if await wait_select(page, "#selectLecteurs", timeout=10000):
                                lecteurs = await collect_lecteurs(page)
                            else:
                                src = await wait_player(page)
                                lecteurs = [{"lecteur": "default", "url": src}] if src else []
                            episodes.append({"episode": ep["label"], "lecteurs": lecteurs})
                        except Exception:
                            episodes.append({"episode": ep["label"], "lecteurs": []})
                        await asyncio.sleep(EPISODE_DELAY)
                    success = True
        except Exception as e:
            log(f"    error {slug}: {e}")
        finally:
            await ctx.close()
        if success:
            break
        await asyncio.sleep(5)
    return episodes


async def count_episodes_quick(browser, session, anime_lien, saison, langues) -> tuple[int, str, str | None]:
    """Compte les épisodes (priorité VF). Retourne (count, langue, url)."""
    titre = saison["titreVignette"]
    url_cible, langue = await pick_saison_url_vf_first(session, anime_lien, titre)
    if not url_cible:
        return 0, langue or "vf", None
    ctx = await new_ctx(browser)
    page = await ctx.new_page()
    count = 0
    try:
        await goto_page(page, url_cible)
        if await wait_select(page, "#selectEpisodes", timeout=15000):
            count = await page.evaluate(
                "() => document.querySelector('#selectEpisodes')?.options.length || 0"
            )
    except Exception:
        pass
    finally:
        await ctx.close()
    return int(count), langue, url_cible


async def scrape_detail(browser, url):
    ctx = await new_ctx(browser)
    page = await ctx.new_page()
    result = {}
    try:
        await goto_page(page, url)
        await page.wait_for_timeout(600)
        result = await page.evaluate(
            "() => {"
            "const img=document.querySelector('#coverOeuvre');"
            "const image=img?.getAttribute('src')||img?.getAttribute('data-src')||null;"
            "const alt=document.querySelector('#titreAlter');"
            "const nomsAlt=alt?alt.innerText.trim().split(',').map(s=>s.trim()).filter(Boolean):[];"
            "const syn=document.querySelector('p.text-sm.text-gray-300.leading-relaxed');"
            "const synopsis=syn?.innerText.trim()||null;"
            "const ifr=document.querySelector('#bandeannonce');"
            "const bandeAnnonce=ifr?(ifr.getAttribute('src')||ifr.getAttribute('data-src')):null;"
            "const cont=document.querySelector('.flex.flex-wrap.overflow-y-hidden.justify-start.bg-slate-900.bg-opacity-70.rounded.mt-2.h-auto');"
            "const saisons=[];"
            "if(cont){cont.querySelectorAll('a').forEach(a=>{"
            "let lbl=a.querySelector('.text-white.font-bold.text-center.absolute.w-28')"
            "||a.querySelector('[class*=\"font-bold\"][class*=\"text-center\"]');"
            "const tv=lbl?.innerText.trim()||a.innerText.trim();"
            "const tc=a.getAttribute('title')||a.getAttribute('aria-label')||tv;"
            "if(tv)saisons.push({titreVignette:tv,titreComplet:tc,isFilm:tv.toLowerCase().includes('film')});"
            "});}"
            "return{image,nomsAlt,synopsis,bandeAnnonce,saisons};}"
        )
    except Exception as e:
        log(f"detail error: {e}")
    finally:
        await ctx.close()
    return result


async def process_saison(browser, session, anime_nom, anime_lien, saison, langues_anime):
    titre = saison["titreVignette"]
    is_film = saison.get("isFilm", False)
    saison["ids"] = await fetch_ids(session, anime_nom + " " + titre, is_film=is_film)
    url_cible, langue_eff = await pick_saison_url_vf_first(session, anime_lien, titre)
    saison["langue"] = langue_eff
    saison["episodes"] = await scrape_saison_episodes(browser, url_cible) if url_cible else []
    return saison


async def scrape_full_anime(browser, session, catalogue_url, card_name="", langues=None):
    ctx = await new_ctx(browser)
    page = await ctx.new_page()
    nom = card_name
    try:
        await goto_page(page, catalogue_url)
        if not nom:
            nom = await page.evaluate("() => document.querySelector('h1')?.innerText?.trim() || ''")
    except Exception:
        pass
    finally:
        await ctx.close()
    anime = {
        "nom": nom or slug_from_url(catalogue_url).replace("-", " ").title(),
        "type": "Anime", "genres": [], "langues": normalize_langues_priority(langues),
        "lien": catalogue_url, "image": None, "noms_alt": [], "synopsis": None,
        "bande_annonce": None, "ids": {"jikan_id": None, "tmdb_id": None, "kitsu_id": None},
        "saisons": [],
    }
    detail = await scrape_detail(browser, catalogue_url)
    anime.update({
        "image": detail.get("image"), "noms_alt": detail.get("nomsAlt", []),
        "synopsis": detail.get("synopsis"), "bande_annonce": detail.get("bandeAnnonce"),
    })
    for s in detail.get("saisons", []):
        s.update({"ids": {"jikan_id": None, "tmdb_id": None, "kitsu_id": None},
                  "langue": None, "episodes": []})
    anime["saisons"] = detail.get("saisons", [])
    anime["ids"], _ = await resolve_verified_ids(
        session, anime["nom"], anime.get("noms_alt"), anime.get("type", "Anime"), {},
    )
    for s in anime["saisons"]:
        await process_saison(browser, session, anime["nom"], anime["lien"], s, anime["langues"])
    return anime


async def scrape_catalogue_page(browser, page_num: int) -> list:
    ctx = await new_ctx(browser)
    page = await ctx.new_page()
    await page.route("**/*.{png,jpg,jpeg,gif,webp,woff,woff2,ttf,mp4,mp3}", lambda r: r.abort())
    raw = []
    try:
        await page.goto(CATALOGUE_URL.format(page=page_num), wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_selector("div.catalog-card", timeout=15000)
        await page.evaluate("""
            async () => {
                const step = 600, delay = 200, height = document.body.scrollHeight;
                for (let y = 0; y < height; y += step) {
                    window.scrollTo(0, y);
                    await new Promise(r => setTimeout(r, delay));
                }
                window.scrollTo(0, 0);
            }
        """)
        await page.wait_for_timeout(1000)
        raw = await page.evaluate("""
            () => Array.from(document.querySelectorAll("div.shrink-0.catalog-card.card-base")).map(card => {
                const name = card.querySelector("h2.card-title")?.innerText.trim() || "Inconnu";
                let href = null;
                card.querySelectorAll("a[href]").forEach(a => {
                    const h = a.getAttribute("href");
                    if (h && h.includes("/catalogue/") && !href) href = h;
                });
                const infoRows = [];
                card.querySelectorAll("div.info-row span").forEach(span => {
                    const p = span.nextElementSibling;
                    if (p && p.tagName === "P")
                        infoRows.push({ label: span.innerText.trim(), value: p.innerText.trim() });
                });
                return { name, href, infoRows };
            })
        """)
    except Exception as e:
        log(f"catalogue p{page_num} error: {e}")
    finally:
        await ctx.close()

    animes = []
    for r in raw:
        info = parse_info_rows(r["infoRows"])
        if info["type"] and info["type"].strip() == "Scans":
            continue
        lien = build_url(r["href"])
        if not lien:
            continue
        animes.append({
            "nom": r["name"],
            "type": info["type"] or "Anime",
            "genres": info["genres"],
            "langues": normalize_langues_priority(info["langues"]),
            "lien": lien if lien.endswith("/") else lien + "/",
        })
    return animes


# ─── Sync logique ─────────────────────────────────────────────
def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "done": {},
        "stats": {"created": 0, "updated": 0, "ids_updated": 0, "ok": 0, "errors": 0},
    }


def save_state(state):
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


async def verify_and_update_ids(
    fs: FirestoreSync,
    session,
    anime_id: str,
    existing: dict,
    dry_run: bool,
    detail: dict | None = None,
) -> bool:
    nom = existing.get("nom", "")
    noms_alt = list(existing.get("noms_alt") or [])
    type_str = existing.get("type", "Anime")
    stored = dict(existing.get("ids") or {})

    if detail:
        noms_alt = noms_alt or detail.get("nomsAlt", []) or []

    new_ids, reason = await resolve_verified_ids(session, nom, noms_alt, type_str, stored)

    changed = ids_differ(stored, new_ids)
    mal = new_ids.get("jikan_id")
    kitsu = new_ids.get("kitsu_id")
    in_jikan = fs.mal_in_global_registry(mal) if mal else True
    in_kitsu = fs.kitsu_in_global_registry(kitsu) if kitsu else True

    if not changed and in_jikan and in_kitsu:
        log(f"    IDs OK — MAL={mal} kitsu={kitsu} (ids/ à jour)")
        return False

    log(
        f"    IDs ({reason}) — "
        f"MAL {stored.get('jikan_id')} → {mal} | kitsu {stored.get('kitsu_id')} → {kitsu} | "
        f"tmdb={new_ids.get('tmdb_id')}"
    )
    if not in_jikan and mal:
        log(f"    → MAL {mal} absent de ids/jikan_id")
    if not in_kitsu and kitsu:
        log(f"    → kitsu {kitsu} absent de ids/kitsu_id")

    if dry_run:
        return True

    fs.update_anime_ids(anime_id, new_ids)
    reg = fs.sync_global_ids_registry(new_ids)
    if reg["jikan_added"]:
        log(f"    + ids/jikan_id ← {mal}")
    if reg["kitsu_added"]:
        log(f"    + ids/kitsu_id ← {kitsu}")
    return changed or reg["jikan_added"] or reg["kitsu_added"]


async def sync_one_anime(browser, session, fs: FirestoreSync, anime: dict, dry_run: bool) -> str:
    anime_id = slug_from_url(anime["lien"])
    if not anime_id:
        return "skip"

    catalogue_url = anime["lien"]
    existing = fs.get_anime(anime_id)

    if not existing:
        log(f"  + CREATE {anime_id} ({anime['nom']})")
        if dry_run:
            return "would_create"
        data = await scrape_full_anime(
            browser, session, catalogue_url, anime["nom"], anime.get("langues"),
        )
        for key in ("type", "genres", "langues"):
            if anime.get(key):
                data[key] = anime[key] if key != "langues" else normalize_langues_priority(anime[key])
        data["langues"] = normalize_langues_priority(data.get("langues"))
        ids, reason = await resolve_verified_ids(
            session, data["nom"], data.get("noms_alt"), data.get("type", "Anime"), data.get("ids"),
        )
        data["ids"] = ids
        log(f"    IDs nouveau — MAL={ids.get('jikan_id')} kitsu={ids.get('kitsu_id')} ({reason})")
        fs.write_full_anime(anime_id, data)
        reg = fs.sync_global_ids_registry(ids)
        if reg["jikan_added"]:
            log(f"    + ids/jikan_id ← {ids.get('jikan_id')}")
        if reg["kitsu_added"]:
            log(f"    + ids/kitsu_id ← {ids.get('kitsu_id')}")
        return "created"

    log(f"  ~ CHECK {anime_id} ({existing.get('nom', anime['nom'])})")
    detail = await scrape_detail(browser, catalogue_url)
    ids_changed = await verify_and_update_ids(
        fs, session, anime_id, existing, dry_run, detail=detail,
    )
    existing = fs.get_anime(anime_id) or existing
    site_saisons = detail.get("saisons", [])
    if not site_saisons:
        return "no_saisons"

    langues = normalize_langues_priority(existing.get("langues") or anime.get("langues"))
    nom = existing.get("nom") or anime["nom"]
    added_total = 0
    need_scrape = []

    for s in site_saisons:
        site_count, langue, _url = await count_episodes_quick(
            browser, session, catalogue_url, s, langues,
        )
        sid = build_saison_id(anime_id, s["titreVignette"], langue)
        meta = next((x for x in existing.get("saisons", []) if x.get("id") == sid), None)
        fb_count = fs.count_episodes(sid, int(meta.get("parts", 0))) if meta else 0

        if not meta:
            log(f"    saison manquante: {sid} (site={site_count})")
            need_scrape.append((s, langue, site_count, fb_count, sid, True))
        elif site_count > fb_count:
            log(f"    ep manquants: {sid} site={site_count} fb={fb_count}")
            need_scrape.append((s, langue, site_count, fb_count, sid, False))
        else:
            log(f"    OK {sid} ({fb_count} ep)")

    if dry_run:
        return f"would_update_{len(need_scrape)}" if need_scrape else "ok"

    for s, langue, _sc, _fc, sid, _new_saison in need_scrape:
        saison = dict(s)
        saison.update({
            "ids": {"jikan_id": None, "tmdb_id": None, "kitsu_id": None},
            "langue": langue, "episodes": [],
        })
        url, langue = await pick_saison_url_vf_first(session, catalogue_url, s["titreVignette"])
        sid = build_saison_id(anime_id, s["titreVignette"], langue)
        if url:
            saison["episodes"] = await scrape_saison_episodes(browser, url)
        if saison["episodes"]:
            n = fs.append_episodes(
                anime_id, sid,
                s["titreVignette"], s.get("titreComplet", s["titreVignette"]),
                langue, saison["episodes"],
            )
            added_total += n
            log(f"    +{n} ep → {sid}")

    if added_total:
        return "updated"
    if ids_changed:
        return "ids_updated"
    return "ok"


async def run(page_begin: int, page_end: int, dry_run: bool, skip_done: bool):
    log("=== firebase_full_sync ===")
    state = load_state()
    stats = state.setdefault(
        "stats",
        {"created": 0, "updated": 0, "ids_updated": 0, "ok": 0, "errors": 0},
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        connector = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            fs = FirestoreSync(init_firestore())

            for page_num in range(page_begin, page_end + 1):
                log(f"--- Page catalogue {page_num}/{page_end} ---")
                animes = await scrape_catalogue_page(browser, page_num)
                log(f"  {len(animes)} animé(s)")

                for i, anime in enumerate(animes, 1):
                    anime_id = slug_from_url(anime["lien"]) or f"unknown_{i}"
                    key = f"{page_num}|{anime_id}"
                    if skip_done and state.get("done", {}).get(key):
                        continue

                    log(f"[p{page_num} {i}/{len(animes)}] {anime['nom']}")
                    try:
                        status = await sync_one_anime(browser, session, fs, anime, dry_run=dry_run)
                        if status == "created":
                            stats["created"] += 1
                        elif status == "updated":
                            stats["updated"] += 1
                        elif status == "ids_updated":
                            stats["ids_updated"] += 1
                        elif status.startswith("would_"):
                            pass
                        else:
                            stats["ok"] += 1
                        state.setdefault("done", {})[key] = status
                    except Exception as e:
                        log(f"  ERROR: {e}")
                        stats["errors"] += 1
                    if not dry_run:
                        save_state(state)

        await browser.close()

        if not dry_run:
            log("--- Reconstruction collection ids/ (jikan + kitsu) ---")
            try:
                fs.rebuild_global_ids_registry()
            except Exception as e:
                log(f"  WARN rebuild ids/: {e}")

    save_state(state)
    log(
        f"=== FIN — créés:{stats.get('created',0)} maj:{stats.get('updated',0)} "
        f"ids:{stats.get('ids_updated',0)} ok:{stats.get('ok',0)} err:{stats.get('errors',0)} ==="
    )


def main():
    ap = argparse.ArgumentParser(description="Sync catalogue anime-sama → Firestore")
    ap.add_argument("--dry-run", action="store_true", help="Liste sans écrire Firestore")
    ap.add_argument("--page-begin", type=int, default=int(os.environ.get("PAGE_BEGIN", "1")))
    ap.add_argument("--page-end", type=int, default=int(os.environ.get("PAGE_END", "43")))
    ap.add_argument("--no-skip", action="store_true", help="Re-vérifier même les animés déjà traités")
    args = ap.parse_args()

    has_json = bool(os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON", "").strip())
    has_file = any(
        os.path.isfile(os.environ.get(k, ""))
        for k in ("FIREBASE_SERVICE_ACCOUNT", "GOOGLE_APPLICATION_CREDENTIALS")
    )
    if not has_json and not has_file:
        print("ERROR: FIREBASE_SERVICE_ACCOUNT_JSON manquant", file=sys.stderr)
        return 2

    asyncio.run(run(args.page_begin, args.page_end, args.dry_run, skip_done=not args.no_skip))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
