"""
PhotoSift Duplicate Detection Engine
=====================================
Uses multi-hash perceptual comparison to find exact and near-duplicate photos,
including duplicates across different file formats (JPG vs PNG, etc).

v0.3.2 Changes:
- Pre-compute integer hash values at load time for O(1) hamming distance
- Group merge uses pure integer math (XOR + popcount), ~100x faster
- Progress tracking on merge phase
"""

import logging
from collections import defaultdict
from datetime import datetime

import imagehash

from backend.database import (
    get_connection,
    create_duplicate_group,
    resolve_duplicate_group,
    get_duplicate_groups,
    get_latest_scan_root,
    delete_photos_by_ids,
)

logger = logging.getLogger("photosift.duplicates")

dup_progress = {
    "active": False, "phase": "idle",
    "total_photos": 0, "processed": 0,
    "groups_found": 0, "total_duplicates": 0,
}


def reset_dup_progress():
    dup_progress.update({
        "active": False, "phase": "idle",
        "total_photos": 0, "processed": 0,
        "groups_found": 0, "total_duplicates": 0,
    })


def hex_to_int(hex_str: str) -> int:
    """Convert a hex hash string to an integer for fast bitwise comparison."""
    try:
        return int(hex_str, 16)
    except (ValueError, TypeError):
        return None


def hamming_int(a: int, b: int) -> int:
    """Hamming distance between two integers via XOR + popcount. ~100x faster than imagehash."""
    return bin(a ^ b).count('1')


def hamming_distance(hash1: str, hash2: str) -> int:
    """Compute Hamming distance between two hex hash strings (legacy, used by other modules)."""
    try:
        h1 = imagehash.hex_to_hash(hash1)
        h2 = imagehash.hex_to_hash(hash2)
        return h1 - h2
    except Exception:
        return 999


def precompute_int_hashes(photos: list[dict]) -> dict:
    """
    Pre-convert hex hash strings to integers for all photos.
    Returns dict mapping photo_id -> (phash_int, ahash_int, dhash_int)
    """
    lookup = {}
    for p in photos:
        lookup[p["id"]] = (
            hex_to_int(p.get("phash")),
            hex_to_int(p.get("ahash")),
            hex_to_int(p.get("dhash")),
        )
    return lookup


def are_duplicates_fast(id1: int, id2: int, int_hashes: dict, threshold: int) -> bool:
    """
    Fast multi-hash duplicate check using pre-computed integer hashes.
    Two photos are duplicates if at least 2 of 3 hash types match within threshold.
    Uses pure integer XOR + popcount — no library calls.
    """
    h1 = int_hashes.get(id1)
    h2 = int_hashes.get(id2)
    if not h1 or not h2:
        return False

    matches = 0
    for i in range(3):
        a = h1[i]
        b = h2[i]
        if a is not None and b is not None:
            if hamming_int(a, b) <= threshold:
                matches += 1
    return matches >= 2


def score_photo(photo: dict) -> float:
    """
    Score a photo for 'keepability'. Higher = better candidate to keep.
    Factors: resolution, file size, metadata completeness, EXIF date source.
    """
    score = 0.0

    w = photo.get("width") or 0
    h = photo.get("height") or 0
    megapixels = (w * h) / 1_000_000
    score += megapixels * 10

    size_mb = (photo.get("file_size") or 0) / (1024 * 1024)
    score += min(size_mb, 50)

    source = photo.get("date_taken_source")
    if source == "exif_original":
        score += 30
    elif source == "exif_digitized":
        score += 20
    elif source == "exif_datetime":
        score += 15
    elif source == "file_modified":
        score += 5

    if photo.get("gps_lat") is not None:
        score += 10

    if photo.get("camera_make"):
        score += 5

    ext = (photo.get("file_ext") or "").lower()
    if ext in (".png", ".tiff", ".tif", ".bmp"):
        score += 8
    elif ext in (".heic", ".heif"):
        score += 5

    return score


def find_duplicates(threshold: int = 6):
    """
    Find duplicate photos using multi-hash perceptual comparison.

    Uses 3 hash algorithms (phash, ahash, dhash) and requires at least
    2 of 3 to match within the threshold. This catches duplicates across
    different file formats (JPG vs PNG) and compression levels while
    keeping false positives low.

    threshold: maximum Hamming distance per hash to consider a match.
        0 = exact visual match only
        6 = recommended default (catches format conversions reliably)
        10+ = loose matching (catches crops, but more false positives)
    """
    reset_dup_progress()
    dup_progress["active"] = True
    dup_progress["phase"] = "loading"

    # Clear existing unresolved duplicate groups
    with get_connection() as conn:
        conn.execute("DELETE FROM duplicate_members WHERE group_id IN (SELECT id FROM duplicate_groups WHERE status = 'pending')")
        conn.execute("DELETE FROM duplicate_groups WHERE status = 'pending'")

    # Load all photos with hashes
    with get_connection() as conn:
        photos = conn.execute("""
            SELECT id, file_path, file_name, file_size, file_ext, width, height,
                   date_taken, date_taken_source, gps_lat, gps_lon,
                   camera_make, phash, ahash, dhash
            FROM photos
            WHERE phash IS NOT NULL
            ORDER BY id
        """).fetchall()

    photos = [dict(p) for p in photos]
    dup_progress["total_photos"] = len(photos)

    if len(photos) < 2:
        dup_progress["active"] = False
        dup_progress["phase"] = "complete"
        return 0

    # ── Pre-compute integer hashes for fast comparison ────────
    dup_progress["phase"] = "precomputing"
    int_hashes = precompute_int_hashes(photos)
    logger.info(f"Pre-computed integer hashes for {len(photos)} photos")

    # ── Pass 1: Exact hash match (fast) ──────────────────────
    dup_progress["phase"] = "comparing"
    composite_groups = defaultdict(list)
    for photo in photos:
        key = f"{photo['phash']}|{photo['ahash']}|{photo['dhash']}"
        composite_groups[key].append(photo)

    exact_groups = []
    seen_ids = set()
    for key, group in composite_groups.items():
        if len(group) >= 2:
            ids = tuple(sorted(p["id"] for p in group))
            if ids not in seen_ids:
                exact_groups.append(group)
                seen_ids.add(ids)

    logger.info(f"Pass 1: {len(exact_groups)} exact-match groups")

    # ── Pass 2: Near-duplicate multi-hash comparison ─────────
    exact_ids = set()
    for group in exact_groups:
        for p in group:
            exact_ids.add(p["id"])

    remaining = [p for p in photos if p["id"] not in exact_ids]
    logger.info(f"Pass 2: Checking {len(remaining)} non-exact photos for near-duplicates")

    near_groups = []
    if threshold > 0 and len(remaining) > 1:
        dup_progress["phase"] = "near_duplicate_scan"
        used = set()
        for i, p1 in enumerate(remaining):
            if p1["id"] in used:
                continue
            group = [p1]
            for j in range(i + 1, len(remaining)):
                p2 = remaining[j]
                if p2["id"] in used:
                    continue
                if are_duplicates_fast(p1["id"], p2["id"], int_hashes, threshold):
                    group.append(p2)
                    used.add(p2["id"])
            if len(group) >= 2:
                near_groups.append(group)
                used.add(p1["id"])

            if i % 100 == 0:
                dup_progress["processed"] = i + 1

    logger.info(f"Pass 2: {len(near_groups)} near-duplicate groups")

    # ── Pass 3: Merge groups that are near-duplicates of each other ──
    dup_progress["phase"] = "merging_groups"
    all_raw_groups = exact_groups + near_groups
    total_raw = len(all_raw_groups)
    dup_progress["total_photos"] = total_raw
    dup_progress["processed"] = 0
    logger.info(f"Pass 3: Merging across {total_raw} groups (fast int comparison)")

    merged_groups = []
    merged_flags = [False] * total_raw

    for i in range(total_raw):
        if merged_flags[i]:
            continue
        current = list(all_raw_groups[i])
        rep_i_id = current[0]["id"]
        for j in range(i + 1, total_raw):
            if merged_flags[j]:
                continue
            rep_j_id = all_raw_groups[j][0]["id"]
            if are_duplicates_fast(rep_i_id, rep_j_id, int_hashes, threshold):
                current.extend(all_raw_groups[j])
                merged_flags[j] = True
        merged_groups.append(current)

        if i % 50 == 0:
            dup_progress["processed"] = i

    all_groups = merged_groups
    logger.info(f"Pass 3: Merged down to {len(all_groups)} groups")

    # ── Save duplicate groups ────────────────────────────────
    dup_progress["phase"] = "saving"
    total_dupes = 0

    for group in all_groups:
        scored = [(score_photo(p), p) for p in group]
        scored.sort(key=lambda x: x[0], reverse=True)
        best = scored[0][1]

        group_hash = group[0]["phash"]
        photo_ids = [p["id"] for p in group]

        create_duplicate_group(group_hash, photo_ids, best["id"])
        total_dupes += len(group) - 1

    dup_progress["groups_found"] = len(all_groups)
    dup_progress["total_duplicates"] = total_dupes
    dup_progress["phase"] = "complete"
    dup_progress["active"] = False

    logger.info(f"Found {len(all_groups)} duplicate groups with {total_dupes} total duplicates")
    return len(all_groups)


def apply_duplicate_actions(actions: list[dict], trash_dir: str = None):
    """
    Apply user decisions on duplicate groups.
    - Moves trashed files to a single .photosift_trash folder at the scan root
    - Removes trashed photos from the database so Organize only sees survivors
    - Resolves the duplicate group as handled

    actions: list of {group_id, keep_ids: [int], action: 'delete' | 'trash'}
    """
    import shutil
    from pathlib import Path

    results = {"processed": 0, "deleted": 0, "trashed": 0, "errors": []}

    # Determine the single consolidated trash location
    if trash_dir:
        consolidated_trash = Path(trash_dir)
    else:
        scan_root = get_latest_scan_root()
        if scan_root:
            consolidated_trash = Path(scan_root) / ".photosift_trash"
        else:
            consolidated_trash = Path.home() / ".photosift" / "trash"

    consolidated_trash.mkdir(parents=True, exist_ok=True)
    logger.info(f"Trash folder: {consolidated_trash}")

    trashed_photo_ids = []

    for action in actions:
        group_id = action["group_id"]
        keep_ids = set(action["keep_ids"])
        mode = action.get("action", "trash")

        with get_connection() as conn:
            members = conn.execute("""
                SELECT dm.photo_id, p.file_path
                FROM duplicate_members dm
                JOIN photos p ON dm.photo_id = p.id
                WHERE dm.group_id = ?
            """, (group_id,)).fetchall()

        for member in members:
            if member["photo_id"] in keep_ids:
                continue

            file_path = Path(member["file_path"])
            if not file_path.exists():
                # File already gone — just clean up the database entry
                trashed_photo_ids.append(member["photo_id"])
                continue

            try:
                if mode == "delete":
                    file_path.unlink()
                    results["deleted"] += 1
                    trashed_photo_ids.append(member["photo_id"])
                else:
                    # Move to consolidated trash folder
                    dest = consolidated_trash / file_path.name
                    counter = 1
                    while dest.exists():
                        dest = consolidated_trash / f"{file_path.stem}_{counter}{file_path.suffix}"
                        counter += 1
                    shutil.move(str(file_path), str(dest))
                    results["trashed"] += 1
                    trashed_photo_ids.append(member["photo_id"])
            except Exception as e:
                results["errors"].append(f"{file_path}: {str(e)}")

        resolve_duplicate_group(group_id, list(keep_ids), mode)
        results["processed"] += 1

    # Remove trashed photos from the database so they don't appear in Organize
    if trashed_photo_ids:
        delete_photos_by_ids(trashed_photo_ids)
        logger.info(f"Removed {len(trashed_photo_ids)} trashed photos from database")

    results["trash_location"] = str(consolidated_trash)
    return results
