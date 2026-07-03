#!/usr/bin/env python3
"""
test_anime_search.py — Script de test pour la recherche d'anime par nom.

Ce script teste la fonctionnalité de recherche d'anime quand les épisodes manquent:
1. Recherche l'anime par nom dans la base de données Firebase
2. Si trouvé, vérifie si les épisodes sont complets
3. Si incomplets, utilise la recherche par nom pour trouver l'URL correcte
4. Scrape les épisodes manquants

Usage:
  python test_anime_search.py --anime "hunter x hunter" --dry-run
  python test_anime_search.py --anime "one piece" --no-dry-run

Variables d'environnement requises:
  - FIREBASE_SERVICE_ACCOUNT_JSON: Credentials Firebase
  - TMDB_API_KEY: Clé API TMDB (optionnelle, utilise une clé par défaut)
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

# Ajouter le répertoire parent au path pour importer firebase_full_sync
sys.path.insert(0, str(Path(__file__).parent))

from firebase_full_sync import (
    init_firestore,
    FirestoreSync,
    slug_from_url,
    build_url,
    search_anime_by_name,
    count_episodes_quick,
    build_saison_id,
    scrape_saison_episodes,
    pick_saison_url_vf_first,
    normalize_langues_priority,
    scrape_detail,
    log,
)
from playwright.async_api import async_playwright
import aiohttp


async def test_anime_search(anime_name: str, dry_run: bool = True):
    """Test complet de la recherche d'anime par nom."""
    
    log(f"=== Test de recherche pour: {anime_name} ===")
    
    # Initialisation
    try:
        db = init_firestore()
        fs = FirestoreSync(db)
    except Exception as e:
        log(f"ERREUR: Impossible d'initialiser Firebase: {e}")
        return False
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-dev-shm-usage", "--disable-gpu"],
        )
        
        async with aiohttp.ClientSession() as session:
            # Étape 1: Rechercher l'anime par nom sur le site
            log(f"1. Recherche de l'anime '{anime_name}' sur le site...")
            searched_url = await search_anime_by_name(browser, anime_name)
            
            if not searched_url:
                log(f"  Aucun résultat trouvé pour '{anime_name}'")
                await browser.close()
                return False
            
            log(f"  URL trouvée: {searched_url}")
            
            # Étape 2: Vérifier si l'anime existe dans Firebase
            anime_id = slug_from_url(searched_url)
            if not anime_id:
                log(f"  Impossible d'extraire l'ID de l'URL")
                await browser.close()
                return False
            
            log(f"2. Vérification dans Firebase (ID: {anime_id})...")
            existing = fs.get_anime(anime_id)
            
            if not existing:
                log(f"  L'anime n'existe pas dans Firebase")
                log(f"  → Scrap complet nécessaire (non implémenté dans ce test)")
                await browser.close()
                return True
            
            log(f"  Anime trouvé dans Firebase: {existing.get('nom', 'N/A')}")
            
            # Étape 3: Scraper les détails du site
            log(f"3. Scraping des détails du site...")
            detail = await scrape_detail(browser, searched_url)
            site_saisons = detail.get("saisons", [])
            
            if not site_saisons:
                log(f"  Aucune saison trouvée sur le site")
                await browser.close()
                return True
            
            log(f"  {len(site_saisons)} saison(s) trouvée(s) sur le site")
            
            # Étape 4: Comparer les épisodes
            log(f"4. Comparaison des épisodes...")
            langues = normalize_langues_priority(existing.get("langues"))
            added_total = 0
            need_scrape = []
            
            for s in site_saisons:
                titre = s["titreVignette"]
                log(f"  Saison: {titre}")
                
                # Compter les épisodes sur le site
                site_count, langue, _url = await count_episodes_quick(
                    browser, session, searched_url, s, langues,
                )
                log(f"    Site: {site_count} épisodes ({langue})")
                
                # Vérifier dans Firebase
                sid = build_saison_id(anime_id, titre, langue)
                meta = next((x for x in existing.get("saisons", []) if x.get("id") == sid), None)
                fb_count = fs.count_episodes(sid, int(meta.get("parts", 0))) if meta else 0
                log(f"    Firebase: {fb_count} épisodes")
                
                # Déterminer si un rescrap est nécessaire
                if not meta:
                    log(f"    → Saison manquante dans Firebase")
                    need_scrape.append((s, langue, site_count, fb_count, sid, True))
                elif fb_count == 0:
                    log(f"    → Saison vide dans Firebase")
                    need_scrape.append((s, langue, site_count, fb_count, sid, False))
                elif site_count > fb_count:
                    log(f"    → {site_count - fb_count} épisode(s) manquant(s)")
                    need_scrape.append((s, langue, site_count, fb_count, sid, False))
                else:
                    log(f"    → OK ({fb_count} épisodes)")
            
            # Étape 5: Rescrap si nécessaire
            if need_scrape and not dry_run:
                log(f"5. Rescrap de {len(need_scrape)} saison(s)...")
                
                for s, langue, _sc, _fc, sid, _new_saison in need_scrape:
                    titre = s["titreVignette"]
                    log(f"  Scraping: {titre} ({langue})")
                    
                    # Choisir l'URL (VF ou VOSTFR)
                    url, langue_eff = await pick_saison_url_vf_first(
                        session, searched_url, titre
                    )
                    
                    if url:
                        log(f"    URL: {url}")
                        episodes = await scrape_saison_episodes(browser, url)
                        log(f"    {len(episodes)} épisode(s) scrapé(s)")
                        
                        if episodes:
                            n = fs.append_episodes(
                                anime_id, sid,
                                titre, s.get("titreComplet", titre),
                                langue_eff, episodes,
                            )
                            added_total += n
                            log(f"    +{n} épisode(s) ajouté(s) à Firebase")
                    else:
                        log(f"    Aucune URL trouvée pour cette saison")
            elif need_scrape and dry_run:
                log(f"5. MODE DRY-RUN: {len(need_scrape)} saison(s) à rescrap (pas d'écriture)")
            else:
                log(f"5. Aucun rescrap nécessaire")
            
            log(f"=== Résultat: {added_total} épisode(s) ajouté(s) ===")
        
        await browser.close()
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Test la recherche d'anime par nom et le rescrap d'épisodes"
    )
    parser.add_argument(
        "--anime",
        type=str,
        default=os.environ.get("ANIME_NAME", ""),
        help="Nom de l'anime à rechercher"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.environ.get("DRY_RUN", "true").lower() == "true",
        help="Mode dry-run (sans écriture Firebase)"
    )
    
    args = parser.parse_args()
    
    if not args.anime:
        print("ERREUR: Nom de l'anime requis (--anime ou variable ANIME_NAME)", file=sys.stderr)
        return 2
    
    # Vérifier les credentials Firebase
    has_json = bool(os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON", "").strip())
    has_file = any(
        os.path.isfile(os.environ.get(k, ""))
        for k in ("FIREBASE_SERVICE_ACCOUNT", "GOOGLE_APPLICATION_CREDENTIALS")
    )
    if not has_json and not has_file:
        print("ERREUR: FIREBASE_SERVICE_ACCOUNT_JSON manquant", file=sys.stderr)
        return 2
    
    success = asyncio.run(test_anime_search(args.anime, args.dry_run))
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
