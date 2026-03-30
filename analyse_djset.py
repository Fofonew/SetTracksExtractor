import os
import sys
import csv
import time
import json
import asyncio
import logging
import subprocess
import hashlib
import base64
import hmac
import uuid
import urllib.parse
from datetime import timedelta
from typing import List, Dict, Any, Optional, Tuple, Callable
from difflib import SequenceMatcher

import requests
from dotenv import load_dotenv
from shazamio import Shazam

load_dotenv()

# ===================== CONFIG =====================
SEGMENT_DURATION = 30
SKIP_INTERVAL = 120          # toutes les 2 min (au lieu de 1) -> 2x moins de requetes
MAX_CONCURRENCY = 2          # 2 en parallele max (plus doux pour Shazam)
SHAZAM_TIMEOUT = 20          # timeout par requete Shazam en secondes
SHAZAM_COOLDOWN = 1.0        # pause entre chaque segment pour eviter le rate-limit
AUDIO_EXT = ".mp3"
SIMILARITY_THRESHOLD = 0.75
DISCOGS_USER_AGENT = "SetTracksExtractor/1.0"
# ==================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("SetTracksExtractor")


# ──────────────────────────────────────────────────
#  Utilitaires
# ──────────────────────────────────────────────────

def ensure_tools() -> None:
    for tool in ["scdl", "ffmpeg"]:
        try:
            subprocess.run([tool, "-h"], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, check=False)
        except FileNotFoundError:
            raise RuntimeError(f"Outil requis introuvable : {tool}")


def segment_index_from_name(name: str) -> int:
    base = os.path.splitext(os.path.basename(name))[0]
    try:
        return int(base.split("_")[-1])
    except Exception:
        return 0


def seconds_to_hhmmss(seconds: int) -> str:
    return str(timedelta(seconds=seconds))


def safe_get(d: Dict, path: List[str], default=None):
    cur = d
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def similar(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def track_key(title: str, artist: str) -> Tuple[str, str]:
    return (title.lower().strip(), artist.lower().strip())


# ──────────────────────────────────────────────────
#  Progress callback
# ──────────────────────────────────────────────────

ProgressCallback = Callable[[str, int, str], None]

def _noop_progress(step: str, pct: int, msg: str) -> None:
    pass


# ──────────────────────────────────────────────────
#  Async subprocess helper
# ──────────────────────────────────────────────────

async def run_cmd(*args: str, check: bool = True, capture: bool = False) -> Optional[str]:
    if capture:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if check and proc.returncode != 0:
            raise RuntimeError(f"Commande echouee: {' '.join(args)}\n{stderr.decode()}")
        return stdout.decode().strip()
    else:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()
        if check and proc.returncode != 0:
            raise RuntimeError(f"Commande echouee: {' '.join(args)}")
        return None


# ──────────────────────────────────────────────────
#  Nettoyage
# ──────────────────────────────────────────────────

def cleanup_previous_files(work_dir: str, segments_dir: str) -> None:
    mp3_files = [f for f in os.listdir(work_dir) if f.endswith(".mp3")]
    for f in mp3_files:
        os.remove(os.path.join(work_dir, f))
    if mp3_files:
        log.info("Nettoyage : %d ancien(s) MP3 supprime(s)", len(mp3_files))
    if os.path.isdir(segments_dir):
        count = 0
        for f in os.listdir(segments_dir):
            os.remove(os.path.join(segments_dir, f))
            count += 1
        if count:
            log.info("Nettoyage : %d ancien(s) segment(s) supprime(s)", count)


# ──────────────────────────────────────────────────
#  Telechargement (async)
# ──────────────────────────────────────────────────

async def download_soundcloud(url: str, work_dir: str) -> str:
    log.info("=== ETAPE 1/5 : Telechargement ===")
    log.info("URL : %s", url)
    before = {f for f in os.listdir(work_dir) if f.endswith(".mp3")}

    proc = await asyncio.create_subprocess_exec(
        "scdl", "-l", url, "--onlymp3", "--path", work_dir,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    await proc.wait()
    if proc.returncode != 0:
        raise RuntimeError("Echec du telechargement SoundCloud")

    after = {f for f in os.listdir(work_dir) if f.endswith(".mp3")}
    new_files = after - before
    if new_files:
        latest = max(new_files, key=lambda f: os.path.getmtime(os.path.join(work_dir, f)))
    else:
        candidates = list(after)
        if not candidates:
            raise FileNotFoundError("Aucun MP3 telecharge.")
        latest = max(candidates, key=lambda f: os.path.getmtime(os.path.join(work_dir, f)))

    log.info("Fichier : %s", latest)
    return os.path.join(work_dir, latest)


# ──────────────────────────────────────────────────
#  Decoupage (async) avec progress
# ──────────────────────────────────────────────────

async def get_audio_duration(filename: str) -> float:
    output = await run_cmd(
        "ffprobe", "-v", "quiet", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", filename,
        capture=True,
    )
    return float(output)


async def cut_segments(filename: str, segments_dir: str,
                       on_progress: ProgressCallback = _noop_progress) -> float:
    log.info("=== ETAPE 2/5 : Decoupage ===")
    duration = await get_audio_duration(filename)
    log.info("Duree totale : %s", seconds_to_hhmmss(int(duration)))
    expected = int(duration / SEGMENT_DURATION) + 1
    on_progress("cutting", 18, f"Decoupage de {seconds_to_hhmmss(int(duration))} "
                f"en ~{expected} segments...")

    os.makedirs(segments_dir, exist_ok=True)
    for f in os.listdir(segments_dir):
        os.remove(os.path.join(segments_dir, f))

    # Lance ffmpeg avec stderr pour suivre la progression
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-i", filename,
        "-f", "segment",
        "-segment_time", str(SEGMENT_DURATION),
        "-c:a", "libmp3lame", "-q:a", "2",
        os.path.join(segments_dir, "part_%06d" + AUDIO_EXT),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )

    # Lit stderr en temps reel pour envoyer la progression
    last_pct = 18
    while True:
        line = await proc.stderr.readline()
        if not line:
            break
        text = line.decode(errors="ignore")
        if "time=" in text:
            try:
                t = text.split("time=")[1].split()[0]
                parts = t.split(":")
                secs = int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
                pct = 18 + int((secs / duration) * 7)  # 18-25%
                if pct > last_pct:
                    last_pct = pct
                    on_progress("cutting", pct,
                                f"Decoupage... {seconds_to_hhmmss(int(secs))} / "
                                f"{seconds_to_hhmmss(int(duration))}")
            except (IndexError, ValueError):
                pass

    await proc.wait()
    if proc.returncode != 0:
        raise RuntimeError("ffmpeg echoue")

    total = len([f for f in os.listdir(segments_dir) if f.endswith(AUDIO_EXT)])
    log.info("Segments crees : %d", total)
    return duration


def list_all_segments(segments_dir: str) -> List[str]:
    return sorted([f for f in os.listdir(segments_dir) if f.endswith(AUDIO_EXT)])


def select_sparse_segments(files: List[str]) -> List[str]:
    if not files:
        return []
    step = max(1, int(SKIP_INTERVAL / SEGMENT_DURATION))
    selected = files[::step]
    log.info("Segments selectionnes : %d / %d", len(selected), len(files))
    return selected


# ──────────────────────────────────────────────────
#  Shazam avec timeout
# ──────────────────────────────────────────────────

async def shazam_recognize(shazam: Shazam, path: str, retries: int = 2) -> Optional[Dict]:
    for attempt in range(retries):
        try:
            result = await asyncio.wait_for(
                shazam.recognize(path),
                timeout=SHAZAM_TIMEOUT,
            )
            return result
        except asyncio.TimeoutError:
            log.warning("Shazam timeout (%ds) tentative %d/%d",
                        SHAZAM_TIMEOUT, attempt + 1, retries)
        except Exception as e:
            log.warning("Shazam %d/%d : %s", attempt + 1, retries, e)
        if attempt < retries - 1:
            await asyncio.sleep(2)
    return None


# ──────────────────────────────────────────────────
#  Normalisation
# ──────────────────────────────────────────────────

def normalize_shazam(file_name: str, payload: Optional[Dict]) -> Optional[Dict[str, Any]]:
    if not payload:
        return None
    title = safe_get(payload, ["track", "title"])
    artist = safe_get(payload, ["track", "subtitle"])
    if not title or not artist:
        return None
    idx = segment_index_from_name(file_name)
    offset = idx * SEGMENT_DURATION
    isrc = safe_get(payload, ["track", "isrc"])
    spotify, apple = None, None
    hub = safe_get(payload, ["track", "hub"], {})
    for p in (hub.get("providers") or []):
        if isinstance(p, dict) and p.get("actions"):
            ptype = str(p.get("type", "")).lower()
            for a in p["actions"]:
                if a.get("uri"):
                    if ptype.startswith("spotify"):
                        spotify = a["uri"]
                    elif ptype.startswith("apple"):
                        apple = a["uri"]
    score = safe_get(payload, ["matches", 0, "score"])
    return {
        "source": "Shazam", "title": title, "artist": artist,
        "album": None, "label": None, "isrc": isrc,
        "spotify": spotify, "apple": apple,
        "confidence": score if isinstance(score, (int, float)) else None,
        "file_segment": file_name,
        "time_offset_seconds": offset,
        "time_offset_hhmmss": seconds_to_hhmmss(offset),
    }


# ──────────────────────────────────────────────────
#  Traitement sequentiel avec cooldown
# ──────────────────────────────────────────────────

async def process_segments_sequential(
    selected: List[str], segments_dir: str,
    on_progress: ProgressCallback,
) -> List[Dict[str, Any]]:
    """Traite les segments un par un avec cooldown pour eviter le rate-limit."""
    shazam = Shazam()
    all_results: List[Dict[str, Any]] = []
    total = len(selected)

    for i, file_name in enumerate(selected):
        path = os.path.join(segments_dir, file_name)
        idx = segment_index_from_name(file_name)
        offset = idx * SEGMENT_DURATION
        ts = seconds_to_hhmmss(offset)

        on_progress("recognition", 25 + int((i / total) * 55),
                    f"Analyse segment {i + 1}/{total} (position {ts})...")
        log.info("[%d/%d] Segment %s (position %s)", i + 1, total, file_name, ts)

        result = await shazam_recognize(shazam, path)
        res = normalize_shazam(file_name, result)

        if res:
            log.info("  -> %s - %s", res["artist"], res["title"])
            all_results.append(res)
        else:
            log.info("  -> pas de match")

        # Cooldown entre chaque requete
        if i < total - 1:
            await asyncio.sleep(SHAZAM_COOLDOWN)

    return all_results


# ──────────────────────────────────────────────────
#  Discogs + YouTube enrichment
# ──────────────────────────────────────────────────

def search_discogs_vinyl(artist: str, title: str) -> Optional[str]:
    """Cherche un vinyl sur Discogs. Retourne l'URL si trouve."""
    try:
        query = f"{artist} {title}"
        r = requests.get(
            "https://api.discogs.com/database/search",
            params={"q": query, "type": "release", "format": "Vinyl", "per_page": 1},
            headers={"User-Agent": DISCOGS_USER_AGENT},
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            results = data.get("results", [])
            if results:
                return results[0].get("uri", "").replace("//api.discogs.com", "//www.discogs.com")
        return None
    except Exception as e:
        log.debug("Discogs erreur pour %s - %s: %s", artist, title, e)
        return None


def youtube_search_url(artist: str, title: str) -> str:
    """Construit un lien de recherche YouTube."""
    query = f"{artist} {title}"
    return f"https://www.youtube.com/results?search_query={urllib.parse.quote_plus(query)}"


async def enrich_tracklist(
    tracklist: List[Dict[str, Any]],
    on_progress: ProgressCallback,
) -> List[Dict[str, Any]]:
    """Enrichit chaque track avec Discogs (vinyl) et YouTube."""
    total = len(tracklist)
    if not total:
        return tracklist

    on_progress("enrichment", 82, f"Recherche Discogs/YouTube pour {total} tracks...")
    log.info("=== ETAPE 4/5 : Enrichissement Discogs + YouTube ===")

    loop = asyncio.get_event_loop()

    for i, track in enumerate(tracklist):
        artist = track.get("artist", "")
        title = track.get("title", "")
        log.info("  [%d/%d] %s - %s", i + 1, total, artist, title)

        # YouTube (instantane, juste un lien)
        track["youtube"] = youtube_search_url(artist, title)

        # Discogs (requete API, dans un executor pour ne pas bloquer)
        discogs_url = await loop.run_in_executor(None, search_discogs_vinyl, artist, title)
        track["discogs_vinyl"] = discogs_url
        if discogs_url:
            log.info("    Vinyl trouve : %s", discogs_url)
        else:
            log.info("    Pas de vinyl trouve")

        pct = 82 + int(((i + 1) / total) * 8)  # 82-90%
        on_progress("enrichment", pct,
                    f"Enrichissement {i + 1}/{total} — {artist} - {title}"
                    + (" (vinyl!)" if discogs_url else ""))

        # Respect rate-limit Discogs (25 req/min)
        if i < total - 1:
            await asyncio.sleep(1.5)

    return tracklist


# ──────────────────────────────────────────────────
#  Fusion / dedup
# ──────────────────────────────────────────────────

def pick_best_per_segment(candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    grouped: Dict[Tuple[str, str], List[Dict]] = {}
    for c in candidates:
        key = track_key(c["title"], c["artist"])
        found = False
        for ek in grouped:
            if similar(key[0], ek[0]) > SIMILARITY_THRESHOLD and \
               similar(key[1], ek[1]) > SIMILARITY_THRESHOLD:
                grouped[ek].append(c)
                found = True
                break
        if not found:
            grouped[key] = [c]
    best_group = max(grouped.values(), key=len)
    return max(best_group, key=lambda c: c.get("confidence") or 0)


def deduplicate_tracklist(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not items:
        return []
    items.sort(key=lambda d: d.get("time_offset_seconds", 0))
    deduped: List[Dict[str, Any]] = []
    for item in items:
        key = track_key(item["title"], item["artist"])
        is_dup = False
        for existing in deduped:
            ek = track_key(existing["title"], existing["artist"])
            if similar(key[0], ek[0]) > SIMILARITY_THRESHOLD and \
               similar(key[1], ek[1]) > SIMILARITY_THRESHOLD:
                is_dup = True
                break
        if not is_dup:
            deduped.append(item)
    removed = len(items) - len(deduped)
    if removed:
        log.info("Deduplication : %d doublon(s) supprime(s)", removed)
    return deduped


# ──────────────────────────────────────────────────
#  CSV
# ──────────────────────────────────────────────────

FIELDS = [
    "time_offset_hhmmss", "time_offset_seconds", "title", "artist",
    "album", "label", "isrc", "spotify", "apple", "youtube",
    "discogs_vinyl", "source", "file_segment", "confidence",
]


def save_csv(rows: List[Dict], csv_path: str) -> None:
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in FIELDS})
    log.info("CSV : %s", csv_path)


# ──────────────────────────────────────────────────
#  Fonction principale
# ──────────────────────────────────────────────────

async def run_analysis(
    url: str,
    work_dir: str,
    on_progress: ProgressCallback = _noop_progress,
) -> Tuple[List[Dict[str, Any]], str]:
    ensure_tools()
    segments_dir = os.path.join(work_dir, "segments")
    csv_path = os.path.join(work_dir, "tracklist.csv")

    on_progress("cleanup", 0, "Nettoyage des fichiers precedents...")
    cleanup_previous_files(work_dir, segments_dir)

    on_progress("download", 5, "Telechargement depuis SoundCloud...")
    mp3file = await download_soundcloud(url, work_dir)
    on_progress("download", 15, "Telechargement termine.")

    on_progress("cutting", 18, "Decoupage en segments...")
    await cut_segments(mp3file, segments_dir, on_progress)
    on_progress("cutting", 25, "Decoupage termine.")

    all_segs = list_all_segments(segments_dir)
    selected = select_sparse_segments(all_segs)
    if not selected:
        on_progress("error", 100, "Aucun segment selectionne.")
        return [], csv_path

    on_progress("recognition", 25,
                f"Reconnaissance Shazam ({len(selected)} segments, ~{len(selected) * 2}s)...")
    log.info("=== ETAPE 3/5 : Reconnaissance (%d segments) ===", len(selected))

    # Traitement sequentiel avec cooldown (evite le rate-limit)
    all_results = await process_segments_sequential(selected, segments_dir, on_progress)

    log.info("Resultats bruts : %d matches sur %d segments", len(all_results), len(selected))

    # Dedup
    on_progress("finalize", 80, "Deduplication...")
    tracklist = deduplicate_tracklist(all_results)

    # Enrichissement Discogs + YouTube
    tracklist = await enrich_tracklist(tracklist, on_progress)

    # Export
    on_progress("finalize", 92, "Sauvegarde CSV...")
    save_csv(tracklist, csv_path)
    on_progress("done", 100, f"Termine ! {len(tracklist)} tracks identifiees.")

    return tracklist, csv_path


# ──────────────────────────────────────────────────
#  CLI standalone
# ──────────────────────────────────────────────────

async def main():
    def cli_progress(step: str, pct: int, msg: str) -> None:
        log.info("[%3d%%] %s", pct, msg)

    url = sys.argv[1].strip() if len(sys.argv) > 1 else input("URL SoundCloud : ").strip()
    work_dir = os.getcwd()
    tracklist, csv_path = await run_analysis(url, work_dir, on_progress=cli_progress)

    log.info("=" * 60)
    for i, r in enumerate(tracklist, 1):
        vinyl = " [VINYL]" if r.get("discogs_vinyl") else ""
        log.info("  %2d. [%s] %s - %s%s", i, r["time_offset_hhmmss"],
                 r["artist"], r["title"], vinyl)
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
