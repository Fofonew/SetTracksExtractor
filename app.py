import os
import json
import uuid
import asyncio
import tempfile
import shutil
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from analyse_djset import run_analysis, FIELDS

app = FastAPI(title="SetTracksExtractor")
jobs: dict = {}
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    with open(os.path.join(BASE_DIR, "static", "index.html")) as f:
        return f.read()


@app.get("/api/analyze")
async def analyze(url: str) -> StreamingResponse:
    job_id = str(uuid.uuid4())[:8]
    work_dir = tempfile.mkdtemp(prefix=f"ste_{job_id}_")
    loop = asyncio.get_event_loop()

    async def event_stream() -> AsyncGenerator[str, None]:
        queue: asyncio.Queue = asyncio.Queue()

        def on_progress(step: str, pct: int, msg: str) -> None:
            loop.call_soon_threadsafe(
                queue.put_nowait,
                {"type": "progress", "step": step, "pct": pct, "message": msg},
            )

        async def run():
            try:
                tracklist, csv_path = await run_analysis(url, work_dir, on_progress=on_progress)
                rows = [{k: r.get(k) for k in FIELDS} for r in tracklist]
                queue.put_nowait({"type": "result", "job_id": job_id, "tracks": rows})
                jobs[job_id] = {"csv_path": csv_path, "work_dir": work_dir}
            except Exception as e:
                queue.put_nowait({"type": "error", "message": str(e)})
            finally:
                queue.put_nowait(None)

        async def heartbeat():
            """Envoie un ping toutes les 5s pour signaler que le serveur est vivant."""
            while True:
                await asyncio.sleep(5)
                queue.put_nowait({"type": "heartbeat"})

        # Lance l'analyse et le heartbeat en parallele
        run_task = asyncio.create_task(run())
        hb_task = asyncio.create_task(heartbeat())

        # Padding initial pour forcer le flush du buffer navigateur
        yield ": padding " + " " * 2048 + "\n\n"

        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield f"data: {json.dumps(item)}\n\n"
        finally:
            hb_task.cancel()

        await run_task

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/api/download/{job_id}")
async def download_csv(job_id: str):
    job = jobs.get(job_id)
    if not job or not os.path.isfile(job["csv_path"]):
        return {"error": "Job introuvable"}
    return FileResponse(job["csv_path"], filename="tracklist.csv", media_type="text/csv")


@app.on_event("shutdown")
def cleanup_jobs():
    for job in jobs.values():
        wd = job.get("work_dir")
        if wd and os.path.isdir(wd):
            shutil.rmtree(wd, ignore_errors=True)
