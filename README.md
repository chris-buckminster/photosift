# PhotoSift

Smart photo organizer that finds duplicates, sorts by date, maps your memories, and runs 100% locally.

PhotoSift is built for people with thousands of unsorted photos accumulated over years. Point it at your photo folder, and it will:

- **Scan & Index** every photo, extracting dates, GPS, camera info, and visual fingerprints
- **Find Duplicates** using perceptual hashing — catches exact copies AND near-duplicates across different file formats (JPG vs PNG, different compression levels)
- **Review Duplicates** side-by-side with smart recommendations for which to keep
- **Auto-Organize** into clean Year/Month folders
- **Map View** — see all GPS-tagged photos on an interactive map with thumbnail markers (Apple Photos-style)

Everything runs locally. No cloud. No subscriptions. Your photos stay on your computer.

## Quick Start (Windows)

### Prerequisites

Python 3.10 or newer from https://python.org (check "Add Python to PATH" during install)

### Setup

1. Download or clone this repo
2. Double-click `setup.bat` to install dependencies
3. Double-click `start.bat` to launch PhotoSift
4. Your browser opens to http://localhost:8787

### Manual Setup (Any OS)

```bash
cd photosift
python -m venv venv
source venv/bin/activate   # or venv\Scripts\activate on Windows
pip install -r requirements.txt
python run.py
```

## Features

### Scan & Index
Enter the path to your photo folder. PhotoSift walks every subfolder, reads EXIF metadata (date taken, GPS coordinates, camera make/model), extracts resolution and file size, and computes three perceptual hashes (phash, ahash, dhash) for each photo.

### Duplicate Detection
Multi-hash algorithm requires 2 of 3 hash types to match, catching duplicates across different file formats and compression levels while keeping false positives near zero. Pre-computed integer hashes make comparison ~170x faster than standard libraries.

### Duplicate Review
Photos that look the same are grouped together. PhotoSift recommends which to keep based on resolution, file size, metadata completeness, EXIF date source quality, and GPS data. Review each group or accept all recommendations at once. Trashed files go to a single `.photosift_trash` folder at the scan root — nothing is permanently deleted.

### Auto-Organize
Sort your entire library into `Year/Month` folders (e.g., `2021/07 - July`). Preview the full folder structure before committing. Supports both copy and move modes.

### Map View
Interactive dark-themed map showing all GPS-tagged photos as thumbnail markers. Uses Leaflet.js with OpenStreetMap tiles — no API keys needed. Photos cluster into thumbnail groups when zoomed out and split apart as you zoom in. Click any photo to see a larger preview with metadata.

## Supported Formats

JPG, JPEG, PNG, GIF, BMP, TIFF, WebP, HEIC, HEIF, and RAW formats (CR2, NEF, ARW, DNG, ORF, RW2, PEF, SRW).

## Project Structure

```
photosift/
  backend/
    main.py          - FastAPI server and API endpoints
    scanner.py       - Photo scanning and metadata extraction
    duplicates.py    - Duplicate detection engine (multi-hash, integer-optimized)
    organizer.py     - Auto-organize into Year/Month folders
    database.py      - SQLite database layer
  frontend/
    index.html       - React UI (single-file, no build step)
  run.py             - Entry point
  setup.bat          - Windows dependency installer
  start.bat          - Windows launcher
  requirements.txt   - Python dependencies
```

## Privacy

- 100% local. No uploads, no telemetry, no internet required (except for map tiles).
- Database stored at `~/.photosift/photosift.db` (SQLite).
- Open source — inspect every line of code.

## Roadmap

- [x] Scan & index with EXIF extraction
- [x] Duplicate detection (multi-hash, cross-format)
- [x] Duplicate review with smart recommendations
- [x] Auto-organize into Year/Month folders
- [x] Interactive map view with thumbnail markers
- [ ] Face recognition and person tagging
- [ ] Video processing
- [ ] Search and filters
- [ ] UI polish and performance tuning

## License

AGPL-3.0
