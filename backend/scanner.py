"""
PhotoSift Scanner Engine
========================
Crawls directories, extracts EXIF metadata, computes perceptual hashes.
Designed to handle 28,000+ photos efficiently with batch processing.
"""

import os
import logging
from pathlib import Path
from datetime import datetime
from PIL import Image
from PIL import ImageFile
from PIL.ExifTags import TAGS, GPSTAGS
ImageFile.LOAD_TRUNCATED_IMAGES = True
import imagehash
from concurrent.futures import ThreadPoolExecutor, as_completed

from backend.database import insert_photos_batch, create_scan, update_scan, clear_duplicates

# Register HEIC/HEIF support if available
try:
    from pillow_heif import register_heif_opener
    register_heif_opener()
except ImportError:
    pass

logger = logging.getLogger("photosift.scanner")

SUPPORTED_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif",
    ".webp", ".heic", ".heif", ".raw", ".cr2", ".nef", ".arw",
    ".dng", ".orf", ".rw2", ".pef", ".srw"
}

scan_progress = {
    "active": False, "scan_id": None, "root_path": "",
    "total_files": 0, "processed": 0, "indexed": 0,
    "errors": 0, "current_file": "", "phase": "idle",
    "error_log": []
}


def reset_progress():
    scan_progress.update({
        "active": False, "scan_id": None, "root_path": "",
        "total_files": 0, "processed": 0, "indexed": 0,
        "errors": 0, "current_file": "", "phase": "idle",
        "error_log": []
    })


def discover_photos(root_path: str) -> list[str]:
    photos = []
    root = Path(root_path)
    if not root.exists():
        raise ValueError(f"Path does not exist: {root_path}")
    if not root.is_dir():
        raise ValueError(f"Path is not a directory: {root_path}")

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not d.startswith('.') and d.lower() not in {
            '$recycle.bin', 'system volume information', '__pycache__', 'node_modules', '.photosift_trash'
        }]
        for fname in filenames:
            ext = Path(fname).suffix.lower()
            if ext in SUPPORTED_EXTENSIONS:
                photos.append(os.path.join(dirpath, fname))
    return photos


def extract_exif_date(exif_data: dict) -> tuple[str, str]:
    date_fields = [
        ("DateTimeOriginal", "exif_original"),
        ("DateTimeDigitized", "exif_digitized"),
        ("DateTime", "exif_datetime"),
    ]
    for field, source in date_fields:
        val = exif_data.get(field)
        if val:
            try:
                dt = datetime.strptime(str(val), "%Y:%m:%d %H:%M:%S")
                return dt.isoformat(), source
            except (ValueError, TypeError):
                continue
    return None, None


def extract_gps(exif_data: dict) -> tuple[float, float]:
    gps_info = exif_data.get("GPSInfo")
    if not gps_info:
        return None, None

    try:
        gps_dict = {}
        for key, val in gps_info.items():
            tag_name = GPSTAGS.get(key, key)
            gps_dict[tag_name] = val

        def to_decimal(dms, ref):
            if not dms or len(dms) < 3:
                return None
            degrees = float(dms[0])
            minutes = float(dms[1])
            seconds = float(dms[2])
            decimal = degrees + minutes / 60.0 + seconds / 3600.0
            if ref in ("S", "W"):
                decimal = -decimal
            return decimal

        lat = to_decimal(gps_dict.get("GPSLatitude"), gps_dict.get("GPSLatitudeRef", "N"))
        lon = to_decimal(gps_dict.get("GPSLongitude"), gps_dict.get("GPSLongitudeRef", "E"))
        return lat, lon
    except Exception:
        return None, None


def process_single_photo(file_path: str, scan_id: int) -> dict:
    try:
        stat = os.stat(file_path)
        p = Path(file_path)

        result = {
            "file_path": str(file_path),
            "file_name": p.name,
            "file_size": stat.st_size,
            "file_ext": p.suffix.lower(),
            "width": None, "height": None,
            "date_taken": None, "date_taken_source": None,
            "gps_lat": None, "gps_lon": None,
            "camera_make": None, "camera_model": None,
            "phash": None, "ahash": None, "dhash": None,
            "scan_id": scan_id,
            "indexed_at": datetime.now().isoformat()
        }

        try:
            with Image.open(file_path) as img:
                result["width"] = img.width
                result["height"] = img.height

                try:
                    result["phash"] = str(imagehash.phash(img))
                    result["ahash"] = str(imagehash.average_hash(img))
                    result["dhash"] = str(imagehash.dhash(img))
                except Exception:
                    pass

                try:
                    raw_exif = img._getexif()
                    if raw_exif:
                        exif_data = {}
                        for tag_id, value in raw_exif.items():
                            tag_name = TAGS.get(tag_id, tag_id)
                            exif_data[tag_name] = value

                        date_taken, date_source = extract_exif_date(exif_data)
                        if date_taken:
                            result["date_taken"] = date_taken
                            result["date_taken_source"] = date_source

                        lat, lon = extract_gps(exif_data)
                        if lat is not None:
                            result["gps_lat"] = lat
                            result["gps_lon"] = lon

                        result["camera_make"] = str(exif_data.get("Make", "")).strip() or None
                        result["camera_model"] = str(exif_data.get("Model", "")).strip() or None
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"Could not open image {file_path}: {e}")

        if not result["date_taken"]:
            try:
                mtime = datetime.fromtimestamp(stat.st_mtime)
                if mtime.year >= 1990:
                    result["date_taken"] = mtime.isoformat()
                    result["date_taken_source"] = "file_modified"
            except Exception:
                pass

        return result

    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return None


def run_scan(root_path: str, batch_size: int = 50, max_workers: int = 4):
    reset_progress()
    scan_progress["active"] = True
    scan_progress["root_path"] = root_path
    scan_progress["phase"] = "discovering"

    # Clear old duplicate data — it will be recomputed after this scan
    clear_duplicates()

    logger.info(f"Discovering photos in {root_path}...")
    photo_paths = discover_photos(root_path)
    scan_progress["total_files"] = len(photo_paths)

    if not photo_paths:
        scan_progress["active"] = False
        scan_progress["phase"] = "complete"
        return 0

    scan_id = create_scan(root_path)
    scan_progress["scan_id"] = scan_id
    scan_progress["phase"] = "indexing"

    update_scan(scan_id, total_files=len(photo_paths))

    logger.info(f"Processing {len(photo_paths)} photos...")
    batch = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_path = {
            executor.submit(process_single_photo, path, scan_id): path
            for path in photo_paths
        }

        for future in as_completed(future_to_path):
            path = future_to_path[future]
            scan_progress["processed"] += 1
            scan_progress["current_file"] = os.path.basename(path)

            try:
                result = future.result()
                if result:
                    batch.append(result)
                    scan_progress["indexed"] += 1
                else:
                    scan_progress["errors"] += 1
            except Exception as e:
                scan_progress["errors"] += 1
                scan_progress["error_log"].append(f"{path}: {str(e)}")
                logger.error(f"Failed to process {path}: {e}")

            if len(batch) >= batch_size:
                insert_photos_batch(batch)
                batch = []

    if batch:
        insert_photos_batch(batch)

    update_scan(
        scan_id,
        completed_at=datetime.now().isoformat(),
        photos_found=scan_progress["indexed"],
        status="completed"
    )

    scan_progress["phase"] = "complete"
    scan_progress["active"] = False

    logger.info(f"Scan complete: {scan_progress['indexed']} photos indexed, {scan_progress['errors']} errors")
    return scan_progress["indexed"]
