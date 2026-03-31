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
SKIP_INTERVAL    = 0   # analyse toutes les minutes → ~106 segments pour 1h45
MAX_CONCURRENCY  = 2     # 2 requetes Shazam en parallele
SHAZAM_TIMEOUT   = 20    # timeout par requete en secondes
SHAZAM_DELAY     = 0.8   # pause apres chaque requete (evite le rate-limit)
AUDIO_EXT        = ".mp3"
SIMILARITY_THRESHOLD  = 0.75
# Score Shazam : en dessous de ce seuil la track est marquee "incertaine"
# Le score brut Shazam est generalement entre 0 et ~1500 (non documente)
# On normalise sur 100 en divisant par 15 avec un cap a 100
CONFIDENCE_LOW    = 30   # en dessous : incertain (rouge)
CONFIDENCE_MEDIUM = 60   # en dessous : moyen (jaune), au dessus : bon (vert)
DISCOGS_USER_AGENT   = "SetTracksExtractor/1.0"
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
#  Nettoyage
# ──────────────────────────────────────────────────

def cleanup_previous_files(work_dir: str, segments_dir: str) -> None:
    mp3_files = [f for f in os.listdir(work_dir) if f.endswith(".mp3")]
    for f in mp3_files:
        os.remove(os.path.join(work_dir, f))
    if mp3_files:
        log.info("Nettoyage : %d ancien(s) MP3 supprime(s)", len(mp3_files))
    if os.path.isdir(segments_dir):
        count = sum(1 for f in os.listdir(segments_dir)
                    if os.remove(os.path.join(segments_dir, f)) is None)
        if count:
            log.info("Nettoyage : %d segment(s) supprime(s)", count)


# ──────────────────────────────────────────────────
#  Telechargement
# ──────────────────────────────────────────────────

async def download_soundcloud(url: str, work_dir: str) -> str:
    log.info("=== ETAPE 1/5 : Telechargement ===")
    log.info("URL : %s", url)
    before = {f for f in os.listdir(work_dir) if f.endswith(".mp3")}

    proc = await asyncio.create_subprocess_exec(
        "scdl", "-l", url, "--onlymp3", "--path", work_dir,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    await proc.wait()
    if proc.returncode != 0:
        raise RuntimeError("Echec du telechargement SoundCloud")

    after = {f for f in os.listdir(work_dir) if f.endswith(".mp3")}
    new_files = after - before
    candidates = list(new_files) if new_files else list(after)
    if not candidates:
        raise FileNotFoundError("Aucun MP3 telecharge.")
    latest = max(candidates, key=lambda f: os.path.getmtime(os.path.join(work_dir, f)))
    log.info("Fichier : %s", latest)
    return os.path.join(work_dir, latest)


# ──────────────────────────────────────────────────
#  Decoupage avec progression ffmpeg
# ──────────────────────────────────────────────────

async def get_audio_duration(filename: str) -> float:
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "quiet", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", filename,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    return float(stdout.decode().strip())


async def cut_segments(filename: str, segments_dir: str,
                       on_progress: ProgressCallback = _noop_progress,
                       segment_duration: int = SEGMENT_DURATION,
                       skip_interval: int = SKIP_INTERVAL) -> float:
    log.info("=== ETAPE 2/5 : Decoupage ===")
    duration = await get_audio_duration(filename)
    log.info("Duree totale : %s", seconds_to_hhmmss(int(duration)))

    n_total = int(duration / segment_duration) + 1
    n_selected = (int(duration / skip_interval) + 1) if skip_interval > 0 else n_total
    on_progress("cutting", 18,
                f"Decoupage de {seconds_to_hhmmss(int(duration))} → "
                f"~{n_total} segments, {n_selected} a analyser...")

    os.makedirs(segments_dir, exist_ok=True)
    for f in os.listdir(segments_dir):
        os.remove(os.path.join(segments_dir, f))

    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-i", filename,
        "-f", "segment",
        "-segment_time", str(segment_duration),
        "-c:a", "libmp3lame", "-q:a", "2",
        os.path.join(segments_dir, "part_%06d" + AUDIO_EXT),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )

    # Lit la progression depuis stderr de ffmpeg
    last_pct = 18
    while True:
        line = await proc.stderr.readline()
        if not line:
            break
        text = line.decode(errors="ignore")
        if "time=" in text:
            try:
                t = text.split("time=")[1].split()[0]
                h, m, s = t.split(":")
                secs = int(h) * 3600 + int(m) * 60 + float(s)
                pct = 18 + int((secs / duration) * 7)
                if pct > last_pct:
                    last_pct = pct
                    on_progress("cutting", pct,
                                f"Decoupage {seconds_to_hhmmss(int(secs))} "
                                f"/ {seconds_to_hhmmss(int(duration))}...")
            except Exception:
                pass

    await proc.wait()
    if proc.returncode != 0:
        raise RuntimeError("ffmpeg a echoue")

    total = len([f for f in os.listdir(segments_dir) if f.endswith(AUDIO_EXT)])
    log.info("Segments crees : %d", total)
    return duration


def list_all_segments(segments_dir: str) -> List[str]:
    return sorted([f for f in os.listdir(segments_dir) if f.endswith(AUDIO_EXT)])


def select_sparse_segments(files: List[str],
                           segment_duration: int = SEGMENT_DURATION,
                           skip_interval: int = SKIP_INTERVAL) -> List[str]:
    if not files:
        return []
    if skip_interval <= 0:
        log.info("Segments selectionnes : %d / %d (tous)", len(files), len(files))
        return files
    step = max(1, int(skip_interval / segment_duration))
    selected = files[::step]
    log.info("Segments selectionnes : %d / %d (1 tous les %d)", len(selected), len(files), step)
    return selected


# ──────────────────────────────────────────────────
#  Shazam avec timeout + delay
# ──────────────────────────────────────────────────

async def shazam_recognize(shazam: Shazam, path: str, sem: asyncio.Semaphore,
                           retries: int = 2) -> Optional[Dict]:
    async with sem:
        for attempt in range(retries):
            try:
                result = await asyncio.wait_for(
                    shazam.recognize(path),
                    timeout=SHAZAM_TIMEOUT,
                )
                # Pause apres chaque requete pour eviter le rate-limit
                await asyncio.sleep(SHAZAM_DELAY)
                return result
            except asyncio.TimeoutError:
                log.warning("Shazam timeout (%ds) tentative %d/%d — segment ignoré",
                            SHAZAM_TIMEOUT, attempt + 1, retries)
            except Exception as e:
                log.warning("Shazam erreur %d/%d : %s", attempt + 1, retries, e)
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
        return None


# ──────────────────────────────────────────────────
#  Normalisation
# ──────────────────────────────────────────────────

def normalize_shazam(file_name: str, payload: Optional[Dict],
                     segment_duration: int = SEGMENT_DURATION) -> Optional[Dict[str, Any]]:
    if not payload:
        return None
    title = safe_get(payload, ["track", "title"])
    artist = safe_get(payload, ["track", "subtitle"])
    if not title or not artist:
        return None
    idx = segment_index_from_name(file_name)
    offset = idx * segment_duration
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
    match = safe_get(payload, ["matches", 0], {})
    raw_score     = match.get("score")
    timeskew      = match.get("timeskew", 0)      # proche de 0 = bon alignement
    freqskew      = match.get("frequencyskew", 0) # proche de 0 = bon alignement

    # Normalisation du score sur 0-100
    confidence = None
    if isinstance(raw_score, (int, float)):
        # Score brut Shazam non documente, empiriquement ~0-1500
        # On cap a 100 apres division par 15
        confidence = min(100, round(raw_score / 15))
        # Penalite si timeskew ou freqskew eleves (mauvais alignement = match douteux)
        skew_penalty = min(20, int(abs(timeskew or 0) * 10 + abs(freqskew or 0) * 10))
        confidence = max(0, confidence - skew_penalty)

    uncertain = confidence is not None and confidence < CONFIDENCE_LOW

    if uncertain:
        log.warning("  Match incertain (score=%s, timeskew=%s, freqskew=%s) : %s - %s",
                    raw_score, timeskew, freqskew, title, artist)

    return {
        "source": "Shazam", "title": title, "artist": artist,
        "album": None, "label": None, "isrc": isrc,
        "spotify": spotify, "apple": apple,
        "confidence": confidence,
        "uncertain": uncertain,
        "file_segment": file_name,
        "time_offset_seconds": offset,
        "time_offset_hhmmss": seconds_to_hhmmss(offset),
        "youtube": None, "discogs_vinyl": None,
    }


# ──────────────────────────────────────────────────
#  Traitement concurrent des segments
# ──────────────────────────────────────────────────

async def process_segment(
    file_name: str, segments_dir: str,
    shazam: Shazam, sem: asyncio.Semaphore,
    segment_duration: int = SEGMENT_DURATION,
) -> Optional[Dict[str, Any]]:
    path = os.path.join(segments_dir, file_name)
    payload = await shazam_recognize(shazam, path, sem)
    return normalize_shazam(file_name, payload, segment_duration)


async def recognize_all(
    selected: List[str], segments_dir: str,
    on_progress: ProgressCallback,
    segment_duration: int = SEGMENT_DURATION,
) -> List[Dict[str, Any]]:
    log.info("=== ETAPE 3/5 : Reconnaissance Shazam (%d segments) ===", len(selected))
    shazam = Shazam()
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    total = len(selected)
    all_results: List[Dict[str, Any]] = []
    done = 0
    matched = 0

    tasks = {
        asyncio.ensure_future(
            process_segment(fn, segments_dir, shazam, sem, segment_duration)
        ): fn
        for fn in selected
    }

    for fut in asyncio.as_completed(tasks.keys()):
        res = await fut
        done += 1
        if res:
            matched += 1
            all_results.append(res)
            conf = res.get("confidence")
            conf_str = f" [score={conf}%{'⚠' if res.get('uncertain') else ''}]" if conf is not None else ""
            log.info("  [%d/%d] Match%s : %s - %s", done, total, conf_str, res["artist"], res["title"])
        else:
            log.info("  [%d/%d] Pas de match", done, total)

        pct = 25 + int((done / total) * 50)
        msg = f"Reconnaissance {done}/{total}"
        if res:
            msg += f" — {res['artist']} - {res['title']}"
        on_progress("recognition", pct, msg)

    log.info("%d matches sur %d segments", matched, total)
    return all_results


# ──────────────────────────────────────────────────
#  Discogs + YouTube
# ──────────────────────────────────────────────────

def search_discogs_vinyl(artist: str, title: str) -> Optional[str]:
    try:
        r = requests.get(
            "https://api.discogs.com/database/search",
            params={"q": f"{artist} {title}", "type": "release",
                    "format": "Vinyl", "per_page": 1},
            headers={"User-Agent": DISCOGS_USER_AGENT},
            timeout=10,
        )
        if r.status_code == 200:
            results = r.json().get("results", [])
            if results:
                uri = results[0].get("uri", "")
                return uri.replace("api.discogs.com", "www.discogs.com")
        return None
    except Exception as e:
        log.debug("Discogs erreur : %s", e)
        return None


def youtube_search_url(artist: str, title: str) -> str:
    return ("https://www.youtube.com/results?search_query="
            + urllib.parse.quote_plus(f"{artist} {title}"))


async def enrich_tracklist(
    tracklist: List[Dict[str, Any]],
    on_progress: ProgressCallback,
) -> List[Dict[str, Any]]:
    if not tracklist:
        return tracklist

    log.info("=== ETAPE 4/5 : Enrichissement Discogs + YouTube ===")
    total = len(tracklist)
    loop = asyncio.get_event_loop()

    for i, track in enumerate(tracklist):
        artist = track.get("artist", "")
        title = track.get("title", "")
        pct = 78 + int(((i + 1) / total) * 10)
        on_progress("enrichment", pct,
                    f"Discogs/YouTube {i + 1}/{total} — {artist} - {title}...")

        track["youtube"] = youtube_search_url(artist, title)
        discogs_url = await loop.run_in_executor(None, search_discogs_vinyl, artist, title)
        track["discogs_vinyl"] = discogs_url

        if discogs_url:
            log.info("  Vinyl : %s - %s => %s", artist, title, discogs_url)
        else:
            log.info("  Pas de vinyl : %s - %s", artist, title)

        # Respect du rate-limit Discogs (25 req/min)
        if i < total - 1:
            await asyncio.sleep(2.5)

    return tracklist


# ──────────────────────────────────────────────────
#  Deduplication
# ──────────────────────────────────────────────────

def deduplicate_tracklist(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not items:
        return []
    items.sort(key=lambda d: d.get("time_offset_seconds", 0))
    deduped: List[Dict[str, Any]] = []
    for item in items:
        key = track_key(item["title"], item["artist"])
        if not any(
            similar(key[0], track_key(e["title"], e["artist"])[0]) > SIMILARITY_THRESHOLD and
            similar(key[1], track_key(e["title"], e["artist"])[1]) > SIMILARITY_THRESHOLD
            for e in deduped
        ):
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
    "discogs_vinyl", "source", "file_segment", "confidence", "uncertain",
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
    segment_duration: int = SEGMENT_DURATION,
    skip_interval: int = SKIP_INTERVAL,
) -> Tuple[List[Dict[str, Any]], str]:
    ensure_tools()
    segments_dir = os.path.join(work_dir, "segments")
    csv_path = os.path.join(work_dir, "tracklist.csv")

    log.info("Config : segment=%ds, intervalle=%ds", segment_duration, skip_interval)

    on_progress("cleanup", 0, "Nettoyage des fichiers precedents...")
    cleanup_previous_files(work_dir, segments_dir)

    on_progress("download", 5, "Telechargement depuis SoundCloud...")
    mp3file = await download_soundcloud(url, work_dir)
    on_progress("download", 15, "Telechargement termine.")

    on_progress("cutting", 18, "Decoupage en segments...")
    await cut_segments(mp3file, segments_dir, on_progress, segment_duration, skip_interval)
    on_progress("cutting", 25, "Decoupage termine.")

    all_segs = list_all_segments(segments_dir)
    selected = select_sparse_segments(all_segs, segment_duration, skip_interval)
    if not selected:
        on_progress("error", 100, "Aucun segment selectionne.")
        return [], csv_path

    on_progress("recognition", 25,
                f"Reconnaissance Shazam sur {len(selected)} segments "
                f"(~{len(selected) * (SHAZAM_DELAY + 2) // MAX_CONCURRENCY:.0f}s)...")

    all_results = await recognize_all(selected, segments_dir, on_progress, segment_duration)

    on_progress("dedup", 76, "Deduplication...")
    tracklist = deduplicate_tracklist(all_results)
    log.info("%d tracks uniques apres dedup", len(tracklist))

    tracklist = await enrich_tracklist(tracklist, on_progress)

    on_progress("saving", 90, "Sauvegarde CSV...")
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
    tracklist, _ = await run_analysis(url, os.getcwd(), on_progress=cli_progress)

    log.info("=" * 60)
    for i, r in enumerate(tracklist, 1):
        vinyl = " [VINYL]" if r.get("discogs_vinyl") else ""
        log.info("  %2d. [%s] %s - %s%s",
                 i, r["time_offset_hhmmss"], r["artist"], r["title"], vinyl)
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
