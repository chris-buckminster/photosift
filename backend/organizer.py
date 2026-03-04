"""
PhotoSift Organizer Engine
==========================
Organizes photos into Year/Month folder structure based on date metadata.
Supports preview mode (dry run), copy or move operations, and handles
undated photos by placing them in an 'Undated' folder.
"""

import os
import shutil
import logging
from pathlib import Path
from datetime import datetime
from collections import defaultdict

from backend.database import get_connection

logger = logging.getLogger("photosift.organizer")

MONTH_NAMES = {
    1: "01 - January", 2: "02 - February", 3: "03 - March",
    4: "04 - April", 5: "05 - May", 6: "06 - June",
    7: "07 - July", 8: "08 - August", 9: "09 - September",
    10: "10 - October", 11: "11 - November", 12: "12 - December",
}

# Progress tracking
organize_progress = {
    "active": False,
    "phase": "idle",
    "total": 0,
    "processed": 0,
    "copied": 0,
    "moved": 0,
    "skipped": 0,
    "errors": 0,
    "current_file": "",
    "error_log": [],
}


def reset_organize_progress():
    organize_progress.update({
        "active": False, "phase": "idle",
        "total": 0, "processed": 0, "copied": 0,
        "moved": 0, "skipped": 0, "errors": 0,
        "current_file": "", "error_log": [],
    })


def get_destination_folder(date_taken: str) -> str:
    """
    Given an ISO date string, return the relative folder path.
    e.g. '2019-07-14T10:30:00' -> '2019/07 - July'
    """
    if not date_taken:
        return "Undated"
    try:
        dt = datetime.fromisoformat(date_taken)
        year = str(dt.year)
        month = MONTH_NAMES.get(dt.month, f"{dt.month:02d}")
        return os.path.join(year, month)
    except (ValueError, TypeError):
        return "Undated"


def get_safe_filename(dest_dir: Path, filename: str) -> Path:
    """
    Return a destination path that doesn't collide with existing files.
    If 'IMG_001.jpg' exists, returns 'IMG_001_2.jpg', then 'IMG_001_3.jpg', etc.
    """
    dest = dest_dir / filename
    if not dest.exists():
        return dest

    stem = Path(filename).stem
    ext = Path(filename).suffix
    counter = 2
    while True:
        new_name = f"{stem}_{counter}{ext}"
        dest = dest_dir / new_name
        if not dest.exists():
            return dest
        counter += 1


def generate_preview(output_dir: str = None) -> dict:
    """
    Generate a preview of what the organize operation will do.
    Returns folder structure with file counts, sizes, and sample filenames.
    Does NOT touch any files.
    """
    with get_connection() as conn:
        photos = conn.execute("""
            SELECT id, file_path, file_name, file_size, date_taken, date_taken_source
            FROM photos
            ORDER BY date_taken ASC
        """).fetchall()

    if not photos:
        return {"folders": [], "total_photos": 0, "total_size": 0, "undated_count": 0}

    # Group photos by destination folder
    folder_map = defaultdict(lambda: {"files": [], "size": 0})

    for photo in photos:
        folder = get_destination_folder(photo["date_taken"])
        folder_map[folder]["files"].append({
            "id": photo["id"],
            "file_name": photo["file_name"],
            "file_path": photo["file_path"],
            "file_size": photo["file_size"],
            "date_taken": photo["date_taken"],
            "date_taken_source": photo["date_taken_source"],
        })
        folder_map[folder]["size"] += photo["file_size"] or 0

    # Build structured preview
    folders = []
    total_size = 0
    undated_count = 0

    for folder_path in sorted(folder_map.keys()):
        data = folder_map[folder_path]
        count = len(data["files"])
        size = data["size"]
        total_size += size

        if folder_path == "Undated":
            undated_count = count

        # Include a few sample filenames for the UI
        samples = [f["file_name"] for f in data["files"][:5]]

        # Date range for this folder
        dates = [f["date_taken"] for f in data["files"] if f["date_taken"]]
        date_range = None
        if dates:
            date_range = {"earliest": min(dates), "latest": max(dates)}

        folders.append({
            "path": folder_path,
            "count": count,
            "size": size,
            "samples": samples,
            "date_range": date_range,
            "date_sources": _count_sources(data["files"]),
        })

    return {
        "folders": folders,
        "total_photos": len(photos),
        "total_size": total_size,
        "undated_count": undated_count,
        "folder_count": len(folders),
        "output_dir": output_dir,
    }


def _count_sources(files: list) -> dict:
    sources = defaultdict(int)
    for f in files:
        src = f.get("date_taken_source") or "none"
        sources[src] += 1
    return dict(sources)


def execute_organize(output_dir: str, mode: str = "copy") -> dict:
    """
    Execute the organize operation.

    output_dir: base directory where Year/Month folders will be created
    mode: 'copy' (keep originals) or 'move' (relocate files)

    Returns summary of results.
    """
    reset_organize_progress()
    organize_progress["active"] = True
    organize_progress["phase"] = "loading"

    output_base = Path(output_dir)
    output_base.mkdir(parents=True, exist_ok=True)

    with get_connection() as conn:
        photos = conn.execute("""
            SELECT id, file_path, file_name, file_size, date_taken
            FROM photos
            ORDER BY date_taken ASC
        """).fetchall()

    organize_progress["total"] = len(photos)
    organize_progress["phase"] = "organizing"

    results = {
        "total": len(photos),
        "copied": 0,
        "moved": 0,
        "skipped": 0,
        "errors": [],
        "folders_created": set(),
    }

    for photo in photos:
        organize_progress["processed"] += 1
        organize_progress["current_file"] = photo["file_name"]

        src = Path(photo["file_path"])
        if not src.exists():
            organize_progress["skipped"] += 1
            results["skipped"] += 1
            results["errors"].append(f"Source not found: {src}")
            continue

        # Determine destination
        folder = get_destination_folder(photo["date_taken"])
        dest_dir = output_base / folder
        dest_dir.mkdir(parents=True, exist_ok=True)
        results["folders_created"].add(str(dest_dir))

        dest_path = get_safe_filename(dest_dir, photo["file_name"])

        # Skip if source and dest are the same file
        try:
            if src.resolve() == dest_path.resolve():
                organize_progress["skipped"] += 1
                results["skipped"] += 1
                continue
        except (OSError, ValueError):
            pass

        try:
            if mode == "move":
                shutil.move(str(src), str(dest_path))
                organize_progress["moved"] += 1
                results["moved"] += 1
            else:
                shutil.copy2(str(src), str(dest_path))
                organize_progress["copied"] += 1
                results["copied"] += 1

            # Update database with new path
            with get_connection() as conn:
                conn.execute(
                    "UPDATE photos SET file_path = ? WHERE id = ?",
                    (str(dest_path), photo["id"])
                )

        except Exception as e:
            organize_progress["errors"] += 1
            results["errors"].append(f"{photo['file_name']}: {str(e)}")
            organize_progress["error_log"].append(f"{src}: {str(e)}")
            logger.error(f"Failed to organize {src}: {e}")

    organize_progress["phase"] = "complete"
    organize_progress["active"] = False

    results["folders_created"] = len(results["folders_created"])
    error_count = len(results["errors"])
    results["error_count"] = error_count
    if error_count > 20:
        results["errors"] = results["errors"][:20] + [f"... and {error_count - 20} more"]

    logger.info(
        f"Organize complete: {results['copied']} copied, {results['moved']} moved, "
        f"{results['skipped']} skipped, {error_count} errors"
    )
    return results
