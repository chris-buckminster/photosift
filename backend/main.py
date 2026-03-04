"""
PhotoSift API Server
====================
FastAPI backend serving UI and exposing endpoints for
scanning, indexing, duplicate detection, and photo management.
"""

import os
import sys
import logging
import asyncio
from pathlib import Path
from io import BytesIO
from contextlib import asynccontextmanager
from threading import Thread

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from PIL import Image
from PIL import ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True  # Handle corrupt/truncated photos gracefully

from backend.database import init_db, get_scan_stats, get_duplicate_groups, clear_database, get_connection
from backend.scanner import run_scan, scan_progress, discover_photos
from backend.duplicates import find_duplicates, dup_progress, apply_duplicate_actions
from backend.organizer import generate_preview, execute_organize, organize_progress

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("photosift.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    logger.info("PhotoSift database initialized")
    # Register HEIC/HEIF support if available
    try:
        from pillow_heif import register_heif_opener
        register_heif_opener()
        logger.info("HEIC/HEIF support enabled")
    except ImportError:
        logger.warning("pillow-heif not installed — HEIC thumbnails will show placeholders. Run: pip install pillow-heif")
    yield


app = FastAPI(title="PhotoSift", version="0.5.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ROOT = Path(__file__).parent.parent
FRONTEND_DIR = PROJECT_ROOT / "frontend"


# ---------- Models ----------

class ScanRequest(BaseModel):
    path: str
    threshold: int = 6  # v0.2: raised from 4 to 6

class DuplicateAction(BaseModel):
    group_id: int
    keep_ids: list[int]
    action: str = "trash"

class DuplicateActionsRequest(BaseModel):
    actions: list[DuplicateAction]
    trash_dir: str | None = None

class OrganizeRequest(BaseModel):
    output_dir: str
    mode: str = "copy"  # "copy" or "move"


# ---------- Endpoints ----------

@app.get("/api/health")
async def health():
    return {"status": "ok", "version": "0.5.0"}


@app.get("/api/stats")
async def stats():
    try:
        return get_scan_stats()
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/scan/start")
async def start_scan(req: ScanRequest):
    if scan_progress["active"]:
        raise HTTPException(409, "A scan is already in progress")

    path = req.path.strip()
    if not os.path.isdir(path):
        raise HTTPException(400, f"Directory not found: {path}")

    def _run():
        try:
            run_scan(path)
            find_duplicates(threshold=req.threshold)
        except Exception as e:
            logger.error(f"Scan failed: {e}")
            scan_progress["phase"] = "error"
            scan_progress["active"] = False

    thread = Thread(target=_run, daemon=True)
    thread.start()
    return {"message": "Scan started", "path": path}


@app.get("/api/scan/progress")
async def get_scan_progress():
    if scan_progress["active"]:
        return {**scan_progress, "duplicate_scan": False}
    elif dup_progress["active"]:
        return {
            "active": True,
            "phase": f"duplicates_{dup_progress['phase']}",
            "total_files": dup_progress["total_photos"],
            "processed": dup_progress["processed"],
            "groups_found": dup_progress["groups_found"],
            "total_duplicates": dup_progress["total_duplicates"],
            "duplicate_scan": True,
            **{k: v for k, v in scan_progress.items() if k in ("indexed", "errors", "total_files", "root_path")}
        }
    else:
        return {**scan_progress, "duplicate_scan": False}


@app.post("/api/scan/discover")
async def discover_only(req: ScanRequest):
    path = req.path.strip()
    if not os.path.isdir(path):
        raise HTTPException(400, f"Directory not found: {path}")
    try:
        photos = discover_photos(path)
        return {"path": path, "photo_count": len(photos)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/duplicates")
async def get_duplicates(status: str = Query(default="pending")):
    try:
        groups = get_duplicate_groups(status)
        return {"groups": groups, "total": len(groups)}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/duplicates/resolve")
async def resolve_duplicates(req: DuplicateActionsRequest):
    try:
        actions = [a.model_dump() for a in req.actions]
        results = apply_duplicate_actions(actions, req.trash_dir)
        return results
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/duplicates/resolve-all")
async def resolve_all_duplicates():
    try:
        groups = get_duplicate_groups("pending")
        actions = []
        for g in groups:
            keep_ids = [m["photo_id"] for m in g["members"] if m["is_recommended_keep"]]
            if not keep_ids:
                keep_ids = [g["members"][0]["photo_id"]]
            actions.append({
                "group_id": g["group"]["id"],
                "keep_ids": keep_ids,
                "action": "trash"
            })
        results = apply_duplicate_actions(actions)
        return results
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/organize/preview")
async def organize_preview(output_dir: str = Query(default=None)):
    """Preview the organize operation without moving any files."""
    try:
        return generate_preview(output_dir)
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/organize/execute")
async def organize_execute(req: OrganizeRequest):
    """Execute the organize operation (copy or move photos into Year/Month folders)."""
    if organize_progress["active"]:
        raise HTTPException(409, "An organize operation is already in progress")
    if scan_progress["active"] or dup_progress["active"]:
        raise HTTPException(409, "Cannot organize while a scan is in progress")

    output_dir = req.output_dir.strip()
    if not output_dir:
        raise HTTPException(400, "Output directory is required")

    mode = req.mode if req.mode in ("copy", "move") else "copy"

    def _run():
        try:
            execute_organize(output_dir, mode)
        except Exception as e:
            logger.error(f"Organize failed: {e}")
            organize_progress["phase"] = "error"
            organize_progress["active"] = False

    thread = Thread(target=_run, daemon=True)
    thread.start()
    return {"message": "Organize started", "output_dir": output_dir, "mode": mode}


@app.get("/api/organize/progress")
async def get_organize_progress():
    """Poll organize operation progress."""
    return {**organize_progress}


@app.get("/api/photos")
async def list_photos(
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=50, ge=1, le=200),
    sort: str = Query(default="date_taken"),
    order: str = Query(default="desc"),
    search: str = Query(default=None),
):
    try:
        with get_connection() as conn:
            offset = (page - 1) * per_page
            where_clause = ""
            params = []
            if search:
                where_clause = "WHERE file_name LIKE ? OR file_path LIKE ?"
                params = [f"%{search}%", f"%{search}%"]

            safe_sort = sort if sort in ("date_taken", "file_name", "file_size", "indexed_at") else "date_taken"
            safe_order = "ASC" if order.lower() == "asc" else "DESC"

            count_row = conn.execute(f"SELECT COUNT(*) as cnt FROM photos {where_clause}", params).fetchone()
            rows = conn.execute(
                f"SELECT * FROM photos {where_clause} ORDER BY {safe_sort} {safe_order} LIMIT ? OFFSET ?",
                params + [per_page, offset]
            ).fetchall()

            return {
                "photos": [dict(r) for r in rows],
                "total": count_row["cnt"],
                "page": page,
                "per_page": per_page,
                "pages": (count_row["cnt"] + per_page - 1) // per_page,
            }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/photos/gps")
async def get_gps_photos():
    """Return all photos with GPS coordinates for map display."""
    try:
        with get_connection() as conn:
            rows = conn.execute("""
                SELECT id, file_name, file_path, gps_lat, gps_lon,
                       date_taken, width, height, file_size
                FROM photos
                WHERE gps_lat IS NOT NULL AND gps_lon IS NOT NULL
                ORDER BY date_taken ASC
            """).fetchall()

            total_photos = conn.execute("SELECT COUNT(*) as cnt FROM photos").fetchone()["cnt"]

            return {
                "photos": [dict(r) for r in rows],
                "total_with_gps": len(rows),
                "total_photos": total_photos,
            }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/photo/thumbnail/{photo_id}")
async def get_thumbnail(photo_id: int, size: int = Query(default=200, ge=50, le=800)):
    try:
        with get_connection() as conn:
            photo = conn.execute("SELECT file_path, file_ext FROM photos WHERE id = ?", (photo_id,)).fetchone()
        if not photo:
            return _placeholder_image(size, "Not found")
        file_path = photo["file_path"]
        if not os.path.exists(file_path):
            return _placeholder_image(size, "Missing")

        ext = (photo["file_ext"] or "").lower()

        try:
            with Image.open(file_path) as img:
                # Handle all image modes -> RGB
                if img.mode in ("RGBA", "P", "PA"):
                    img = img.convert("RGBA")
                    bg = Image.new("RGBA", img.size, (40, 40, 50, 255))
                    bg.paste(img, mask=img.split()[3])
                    img = bg.convert("RGB")
                elif img.mode != "RGB":
                    img = img.convert("RGB")

                img.thumbnail((size, size), Image.LANCZOS)
                buf = BytesIO()
                img.save(buf, format="JPEG", quality=85)
                buf.seek(0)
                return Response(content=buf.read(), media_type="image/jpeg")

        except Exception as e:
            logger.debug(f"Thumbnail failed for {file_path}: {e}")
            return _placeholder_image(size, os.path.splitext(os.path.basename(file_path))[1].upper())

    except HTTPException:
        raise
    except Exception as e:
        return _placeholder_image(size, "Error")


def _placeholder_image(size: int, label: str = "?") -> Response:
    """Generate a dark placeholder thumbnail with a text label."""
    img = Image.new("RGB", (size, size), (30, 31, 40))
    try:
        from PIL import ImageDraw, ImageFont
        draw = ImageDraw.Draw(img)
        # Draw a subtle border
        draw.rectangle([0, 0, size-1, size-1], outline=(60, 62, 80), width=1)
        # Draw centered text
        lines = label.split("\n")
        y_start = size // 2 - (len(lines) * 8)
        for i, line in enumerate(lines):
            bbox = draw.textbbox((0, 0), line)
            tw = bbox[2] - bbox[0]
            x = (size - tw) // 2
            draw.text((x, y_start + i * 16), line, fill=(100, 102, 120))
    except Exception:
        pass
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=85)
    buf.seek(0)
    return Response(content=buf.read(), media_type="image/jpeg")


@app.get("/api/photo/full/{photo_id}")
async def get_full_photo(photo_id: int):
    try:
        with get_connection() as conn:
            photo = conn.execute("SELECT file_path, file_ext FROM photos WHERE id = ?", (photo_id,)).fetchone()
        if not photo:
            raise HTTPException(404, "Photo not found")
        file_path = photo["file_path"]
        if not os.path.exists(file_path):
            raise HTTPException(404, "File not found on disk")

        media_types = {
            ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
            ".gif": "image/gif", ".webp": "image/webp", ".bmp": "image/bmp",
            ".tiff": "image/tiff", ".tif": "image/tiff",
        }
        media_type = media_types.get(photo["file_ext"], "application/octet-stream")
        return FileResponse(file_path, media_type=media_type)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/reset")
async def reset_library():
    if scan_progress["active"] or dup_progress["active"]:
        raise HTTPException(409, "Cannot reset while a scan is in progress")
    clear_database()
    return {"message": "Library reset successfully"}


# ---------- Serve Frontend ----------

@app.get("/")
async def serve_frontend():
    index_path = FRONTEND_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return {"message": "PhotoSift API is running. Frontend not found at " + str(index_path)}

if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR / "static"), check_dir=False), name="static")

    @app.get("/{path:path}")
    async def catch_all(path: str):
        static_path = FRONTEND_DIR / path
        if static_path.exists() and static_path.is_file():
            return FileResponse(static_path)
        return FileResponse(FRONTEND_DIR / "index.html")
