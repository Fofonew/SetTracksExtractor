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
from datetime import timedelta
from typing import List, Dict, Any, Optional, Tuple, Callable
from difflib import SequenceMatcher

import requests
from dotenv import load_dotenv
from shazamio import Shazam

load_dotenv()

# ===================== CONFIG =====================
SEGMENT_DURATION = 30
SKIP_INTERVAL = 60
MAX_CONCURRENCY = 3
AUDIO_EXT = ".mp3"
SIMILARITY_THRESHOLD = 0.75
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
#  Progress callback type
# ──────────────────────────────────────────────────

# on_progress(step, pct, message)  — step: str, pct: 0-100, message: str
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
        count = 0
        for f in os.listdir(segments_dir):
            os.remove(os.path.join(segments_dir, f))
            count += 1
        if count:
            log.info("Nettoyage : %d ancien(s) segment(s) supprime(s)", count)


# ──────────────────────────────────────────────────
#  Telechargement
# ──────────────────────────────────────────────────

def download_soundcloud(url: str, work_dir: str) -> str:
    log.info("=== ETAPE 1/4 : Telechargement ===")
    log.info("URL : %s", url)
    before = {f for f in os.listdir(work_dir) if f.endswith(".mp3")}
    cmd = ["scdl", "-l", url, "--onlymp3", "--path", work_dir]
    subprocess.run(cmd, check=True)
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
#  Decoupage
# ──────────────────────────────────────────────────

def get_audio_duration(filename: str) -> float:
    cmd = [
        "ffprobe", "-v", "quiet", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", filename,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return float(result.stdout.strip())


def cut_segments(filename: str, segments_dir: str) -> None:
    log.info("=== ETAPE 2/4 : Decoupage ===")
    duration = get_audio_duration(filename)
    log.info("Duree totale : %s", seconds_to_hhmmss(int(duration)))

    os.makedirs(segments_dir, exist_ok=True)
    for f in os.listdir(segments_dir):
        os.remove(os.path.join(segments_dir, f))

    cmd = [
        "ffmpeg", "-y", "-i", filename,
        "-f", "segment",
        "-segment_time", str(SEGMENT_DURATION),
        "-c:a", "libmp3lame", "-q:a", "2",
        os.path.join(segments_dir, "part_%06d" + AUDIO_EXT),
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

    total = len([f for f in os.listdir(segments_dir) if f.endswith(AUDIO_EXT)])
    log.info("Segments crees : %d", total)


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
#  Services de reconnaissance
# ──────────────────────────────────────────────────

async def shazam_recognize(shazam: Shazam, path: str, retries: int = 3) -> Optional[Dict]:
    for attempt in range(retries):
        try:
            return await shazam.recognize(path)
        except Exception as e:
            log.warning("Shazam %d/%d : %s", attempt + 1, retries, e)
            if attempt < retries - 1:
                await asyncio.sleep(1.5 ** attempt)
    return None


def audd_recognize(path: str, token: str, retries: int = 3) -> Optional[Dict]:
    for attempt in range(retries):
        try:
            with open(path, "rb") as data:
                r = requests.post(
                    "https://api.audd.io/",
                    data={"api_token": token, "return": "apple_music,spotify"},
                    files={"file": data}, timeout=60,
                )
            if r.status_code == 429:
                time.sleep(1.5 ** attempt)
                continue
            return r.json()
        except Exception as e:
            log.warning("AudD %d/%d : %s", attempt + 1, retries, e)
            if attempt < retries - 1:
                time.sleep(1.5 ** attempt)
    return None


def acrcloud_recognize(path: str, host: str, access_key: str, access_secret: str,
                       retries: int = 3) -> Optional[Dict]:
    for attempt in range(retries):
        try:
            with open(path, "rb") as f:
                sample = f.read()
            timestamp = str(int(time.time()))
            string_to_sign = "POST\n/v1/identify\n" + access_key + "\naudio\n1\n" + timestamp
            sign = base64.b64encode(
                hmac.new(access_secret.encode("ascii"),
                         string_to_sign.encode("ascii"),
                         hashlib.sha1).digest()
            ).decode("ascii")
            r = requests.post(
                f"https://{host}/v1/identify",
                data={"access_key": access_key, "sample_bytes": len(sample),
                      "timestamp": timestamp, "signature": sign,
                      "data_type": "audio", "signature_version": "1"},
                files={"sample": ("segment.mp3", sample, "audio/mpeg")},
                timeout=30,
            )
            result = r.json()
            if result.get("status", {}).get("code") == 0:
                return result
            return None
        except Exception as e:
            log.warning("ACRCloud %d/%d : %s", attempt + 1, retries, e)
            if attempt < retries - 1:
                time.sleep(1.5 ** attempt)
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


def normalize_audd(file_name: str, payload: Optional[Dict]) -> Optional[Dict[str, Any]]:
    if not payload:
        return None
    res = payload.get("result")
    if not res:
        return None
    title, artist = res.get("title"), res.get("artist")
    if not title or not artist:
        return None
    idx = segment_index_from_name(file_name)
    offset = idx * SEGMENT_DURATION
    return {
        "source": "AudD", "title": title, "artist": artist,
        "album": res.get("album"), "label": res.get("label"),
        "isrc": res.get("isrc"),
        "spotify": safe_get(res, ["spotify", "external_urls", "spotify"]),
        "apple": safe_get(res, ["apple_music", "url"]),
        "confidence": res.get("score") or res.get("confidence"),
        "file_segment": file_name,
        "time_offset_seconds": offset,
        "time_offset_hhmmss": seconds_to_hhmmss(offset),
    }


def normalize_acrcloud(file_name: str, payload: Optional[Dict]) -> Optional[Dict[str, Any]]:
    if not payload:
        return None
    music = safe_get(payload, ["metadata", "music"])
    if not music:
        return None
    track = music[0]
    title = track.get("title")
    artist = ", ".join(a.get("name", "") for a in track.get("artists", []))
    if not title or not artist:
        return None
    idx = segment_index_from_name(file_name)
    offset = idx * SEGMENT_DURATION
    spotify_id = safe_get(track, ["external_metadata", "spotify", "track", "id"])
    spotify = f"https://open.spotify.com/track/{spotify_id}" if spotify_id else None
    return {
        "source": "ACRCloud", "title": title, "artist": artist,
        "album": safe_get(track, ["album", "name"]),
        "label": track.get("label"),
        "isrc": safe_get(track, ["external_ids", "isrc"]),
        "spotify": spotify, "apple": None,
        "confidence": track.get("score"),
        "file_segment": file_name,
        "time_offset_seconds": offset,
        "time_offset_hhmmss": seconds_to_hhmmss(offset),
    }


# ──────────────────────────────────────────────────
#  Traitement d'un segment
# ──────────────────────────────────────────────────

async def process_segment(
    file_name: str, segments_dir: str,
    shazam: Shazam, sem: asyncio.Semaphore,
    audd_token: Optional[str], acr_config: Optional[Dict[str, str]],
) -> List[Dict[str, Any]]:
    path = os.path.join(segments_dir, file_name)
    candidates: List[Dict[str, Any]] = []

    async with sem:
        shazam_payload = await shazam_recognize(shazam, path)
    res = normalize_shazam(file_name, shazam_payload)
    if res:
        candidates.append(res)

    if audd_token:
        res = normalize_audd(file_name, audd_recognize(path, audd_token))
        if res:
            candidates.append(res)

    if acr_config:
        res = normalize_acrcloud(file_name, acrcloud_recognize(
            path, acr_config["host"], acr_config["access_key"], acr_config["access_secret"]))
        if res:
            candidates.append(res)

    return candidates


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
    "album", "label", "isrc", "spotify", "apple", "source",
    "file_segment", "confidence",
]


def save_csv(rows: List[Dict], csv_path: str) -> None:
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in FIELDS})
    log.info("CSV : %s", csv_path)


# ──────────────────────────────────────────────────
#  Fonction principale (callable par le web)
# ──────────────────────────────────────────────────

async def run_analysis(
    url: str,
    work_dir: str,
    on_progress: ProgressCallback = _noop_progress,
) -> Tuple[List[Dict[str, Any]], str]:
    """
    Lance l'analyse complete. Retourne (tracklist, csv_path).
    on_progress(step, pct, message) est appele a chaque etape.
    """
    ensure_tools()
    segments_dir = os.path.join(work_dir, "segments")
    csv_path = os.path.join(work_dir, "tracklist.csv")

    # Nettoyage
    on_progress("cleanup", 0, "Nettoyage des fichiers precedents...")
    cleanup_previous_files(work_dir, segments_dir)

    # Telechargement
    on_progress("download", 5, "Telechargement depuis SoundCloud...")
    mp3file = download_soundcloud(url, work_dir)
    on_progress("download", 15, "Telechargement termine.")

    # Decoupage
    on_progress("cutting", 18, "Decoupage en segments...")
    cut_segments(mp3file, segments_dir)
    on_progress("cutting", 25, "Decoupage termine.")

    # Preparation
    all_segs = list_all_segments(segments_dir)
    selected = select_sparse_segments(all_segs)
    if not selected:
        on_progress("error", 100, "Aucun segment selectionne.")
        return [], csv_path

    audd_token = os.environ.get("AUDD_API_TOKEN", "").strip() or None
    acr_config = None
    acr_host = os.environ.get("ACRCLOUD_HOST", "").strip()
    acr_key = os.environ.get("ACRCLOUD_ACCESS_KEY", "").strip()
    acr_secret = os.environ.get("ACRCLOUD_ACCESS_SECRET", "").strip()
    if acr_host and acr_key and acr_secret:
        acr_config = {"host": acr_host, "access_key": acr_key, "access_secret": acr_secret}

    services = ["Shazam"]
    if audd_token:
        services.append("AudD")
    if acr_config:
        services.append("ACRCloud")
    on_progress("recognition", 25,
                f"Reconnaissance via {', '.join(services)} ({len(selected)} segments)...")

    shazam = Shazam()
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    all_results: List[Dict[str, Any]] = []
    done_count = 0
    total = len(selected)

    tasks = [
        process_segment(fn, segments_dir, shazam, sem, audd_token, acr_config)
        for fn in selected
    ]

    for fut in asyncio.as_completed(tasks):
        candidates = await fut
        best = pick_best_per_segment(candidates)
        if best:
            all_results.append(best)
            log.info("  Match : %s - %s", best["artist"], best["title"])
        done_count += 1
        pct = 25 + int((done_count / total) * 65)
        on_progress("recognition", pct,
                    f"Segment {done_count}/{total} analyse"
                    + (f" — {best['artist']} - {best['title']}" if best else ""))

    # Dedup + export
    on_progress("finalize", 92, "Deduplication...")
    tracklist = deduplicate_tracklist(all_results)
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
        log.info("  %2d. [%s] %s - %s (%s)",
                 i, r["time_offset_hhmmss"], r["artist"], r["title"], r["source"])
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
