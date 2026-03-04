"""
PhotoSift Database Layer
========================
SQLite-backed storage for photo metadata, duplicate groups, and scan state.
"""

import sqlite3
import os
import json
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager

DB_PATH = None


def get_db_path():
    global DB_PATH
    if DB_PATH is None:
        data_dir = Path.home() / ".photosift"
        data_dir.mkdir(exist_ok=True)
        DB_PATH = str(data_dir / "photosift.db")
    return DB_PATH


def set_db_path(path: str):
    global DB_PATH
    DB_PATH = path


@contextmanager
def get_connection():
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_connection() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                root_path TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                total_files INTEGER DEFAULT 0,
                photos_found INTEGER DEFAULT 0,
                status TEXT DEFAULT 'running'
            );

            CREATE TABLE IF NOT EXISTS photos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT NOT NULL UNIQUE,
                file_name TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                file_ext TEXT NOT NULL,
                width INTEGER,
                height INTEGER,
                date_taken TEXT,
                date_taken_source TEXT,
                gps_lat REAL,
                gps_lon REAL,
                camera_make TEXT,
                camera_model TEXT,
                phash TEXT,
                ahash TEXT,
                dhash TEXT,
                scan_id INTEGER,
                indexed_at TEXT NOT NULL,
                FOREIGN KEY (scan_id) REFERENCES scans(id)
            );

            CREATE TABLE IF NOT EXISTS duplicate_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_hash TEXT NOT NULL,
                photo_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS duplicate_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                photo_id INTEGER NOT NULL,
                is_recommended_keep INTEGER DEFAULT 0,
                user_action TEXT,
                FOREIGN KEY (group_id) REFERENCES duplicate_groups(id),
                FOREIGN KEY (photo_id) REFERENCES photos(id)
            );

            CREATE INDEX IF NOT EXISTS idx_photos_phash ON photos(phash);
            CREATE INDEX IF NOT EXISTS idx_photos_ahash ON photos(ahash);
            CREATE INDEX IF NOT EXISTS idx_photos_dhash ON photos(dhash);
            CREATE INDEX IF NOT EXISTS idx_photos_file_path ON photos(file_path);
            CREATE INDEX IF NOT EXISTS idx_photos_date_taken ON photos(date_taken);
            CREATE INDEX IF NOT EXISTS idx_dup_members_group ON duplicate_members(group_id);
            CREATE INDEX IF NOT EXISTS idx_dup_members_photo ON duplicate_members(photo_id);
        """)


def create_scan(root_path: str) -> int:
    with get_connection() as conn:
        cursor = conn.execute(
            "INSERT INTO scans (root_path, started_at) VALUES (?, ?)",
            (root_path, datetime.now().isoformat())
        )
        return cursor.lastrowid


def update_scan(scan_id: int, **kwargs):
    with get_connection() as conn:
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [scan_id]
        conn.execute(f"UPDATE scans SET {sets} WHERE id = ?", vals)


def insert_photo(photo_data: dict) -> int:
    with get_connection() as conn:
        cols = ", ".join(photo_data.keys())
        placeholders = ", ".join("?" for _ in photo_data)
        cursor = conn.execute(
            f"INSERT OR REPLACE INTO photos ({cols}) VALUES ({placeholders})",
            list(photo_data.values())
        )
        return cursor.lastrowid


def insert_photos_batch(photos: list[dict]):
    if not photos:
        return
    with get_connection() as conn:
        cols = ", ".join(photos[0].keys())
        placeholders = ", ".join("?" for _ in photos[0])
        try:
            conn.executemany(
                f"INSERT OR REPLACE INTO photos ({cols}) VALUES ({placeholders})",
                [list(p.values()) for p in photos]
            )
        except Exception:
            # If batch fails, fall back to individual inserts
            for p in photos:
                try:
                    conn.execute(
                        f"INSERT OR REPLACE INTO photos ({cols}) VALUES ({placeholders})",
                        list(p.values())
                    )
                except Exception:
                    pass  # Skip this photo, don't crash the scan


def get_all_photos():
    with get_connection() as conn:
        return conn.execute("SELECT * FROM photos ORDER BY date_taken ASC").fetchall()


def get_photos_count():
    with get_connection() as conn:
        row = conn.execute("SELECT COUNT(*) as cnt FROM photos").fetchone()
        return row["cnt"]


def get_scan_stats():
    with get_connection() as conn:
        stats = {}
        row = conn.execute("SELECT COUNT(*) as cnt FROM photos").fetchone()
        stats["total_photos"] = row["cnt"]

        row = conn.execute("SELECT COUNT(*) as cnt FROM photos WHERE date_taken IS NOT NULL").fetchone()
        stats["with_date"] = row["cnt"]

        row = conn.execute("SELECT COUNT(*) as cnt FROM photos WHERE gps_lat IS NOT NULL").fetchone()
        stats["with_gps"] = row["cnt"]

        row = conn.execute("SELECT MIN(date_taken) as earliest, MAX(date_taken) as latest FROM photos WHERE date_taken IS NOT NULL").fetchone()
        stats["earliest_date"] = row["earliest"]
        stats["latest_date"] = row["latest"]

        row = conn.execute("SELECT SUM(file_size) as total FROM photos").fetchone()
        stats["total_size_bytes"] = row["total"] or 0

        row = conn.execute("SELECT COUNT(DISTINCT phash) as cnt FROM photos WHERE phash IS NOT NULL").fetchone()
        stats["unique_hashes"] = row["cnt"]

        row = conn.execute("SELECT COUNT(*) as cnt FROM duplicate_groups WHERE status = 'pending'").fetchone()
        stats["pending_duplicate_groups"] = row["cnt"]

        rows = conn.execute("""
            SELECT LOWER(file_ext) as ext, COUNT(*) as cnt
            FROM photos GROUP BY LOWER(file_ext) ORDER BY cnt DESC
        """).fetchall()
        stats["extensions"] = {r["ext"]: r["cnt"] for r in rows}

        rows = conn.execute("SELECT * FROM scans ORDER BY started_at DESC LIMIT 5").fetchall()
        stats["recent_scans"] = [dict(r) for r in rows]

        return stats


def get_duplicate_groups(status: str = None):
    with get_connection() as conn:
        if status:
            groups = conn.execute(
                "SELECT * FROM duplicate_groups WHERE status = ? ORDER BY photo_count DESC",
                (status,)
            ).fetchall()
        else:
            groups = conn.execute(
                "SELECT * FROM duplicate_groups ORDER BY photo_count DESC"
            ).fetchall()

        result = []
        for g in groups:
            members = conn.execute("""
                SELECT dm.*, p.file_path, p.file_name, p.file_size, p.width, p.height,
                       p.date_taken, p.date_taken_source
                FROM duplicate_members dm
                JOIN photos p ON dm.photo_id = p.id
                WHERE dm.group_id = ?
                ORDER BY dm.is_recommended_keep DESC, p.file_size DESC
            """, (g["id"],)).fetchall()
            result.append({
                "group": dict(g),
                "members": [dict(m) for m in members]
            })
        return result


def create_duplicate_group(group_hash: str, member_photo_ids: list[int], recommended_keep_id: int):
    with get_connection() as conn:
        cursor = conn.execute(
            "INSERT INTO duplicate_groups (group_hash, photo_count, created_at) VALUES (?, ?, ?)",
            (group_hash, len(member_photo_ids), datetime.now().isoformat())
        )
        group_id = cursor.lastrowid
        for pid in member_photo_ids:
            conn.execute(
                "INSERT INTO duplicate_members (group_id, photo_id, is_recommended_keep) VALUES (?, ?, ?)",
                (group_id, pid, 1 if pid == recommended_keep_id else 0)
            )
        return group_id


def resolve_duplicate_group(group_id: int, keep_photo_ids: list[int], action: str = "delete"):
    with get_connection() as conn:
        members = conn.execute(
            "SELECT * FROM duplicate_members WHERE group_id = ?", (group_id,)
        ).fetchall()

        for m in members:
            if m["photo_id"] in keep_photo_ids:
                conn.execute(
                    "UPDATE duplicate_members SET user_action = 'keep' WHERE id = ?",
                    (m["id"],)
                )
            else:
                conn.execute(
                    "UPDATE duplicate_members SET user_action = ? WHERE id = ?",
                    (action, m["id"])
                )

        conn.execute(
            "UPDATE duplicate_groups SET status = 'resolved' WHERE id = ?",
            (group_id,)
        )


def clear_database():
    with get_connection() as conn:
        conn.executescript("""
            DELETE FROM duplicate_members;
            DELETE FROM duplicate_groups;
            DELETE FROM photos;
            DELETE FROM scans;
        """)


def clear_duplicates():
    """Clear all duplicate data. Called before re-scanning since duplicates will be recomputed."""
    with get_connection() as conn:
        conn.executescript("""
            DELETE FROM duplicate_members;
            DELETE FROM duplicate_groups;
        """)


def get_latest_scan_root() -> str:
    """Get the root path from the most recent scan."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT root_path FROM scans ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        return row["root_path"] if row else None


def delete_photos_by_ids(photo_ids: list[int]):
    """Remove photos from the database entirely (after they've been trashed)."""
    if not photo_ids:
        return
    with get_connection() as conn:
        placeholders = ",".join("?" for _ in photo_ids)
        conn.execute(f"DELETE FROM duplicate_members WHERE photo_id IN ({placeholders})", photo_ids)
        conn.execute(f"DELETE FROM photos WHERE id IN ({placeholders})", photo_ids)
