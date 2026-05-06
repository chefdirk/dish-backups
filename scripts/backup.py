"""
dish — Dagelijkse DB backup naar Google Drive.

Workflow:
1. pg_dump → gecomprimeerd .sql.gz bestand lokaal
2. Upload naar Google Drive folder via service account
3. Verwijder backups ouder dan 30 dagen uit Drive
"""

import gzip
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.auth import default as google_auth_default
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

RETENTION_DAYS = 30
BACKUP_PREFIX = "dish-"
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# Expliciet pad naar pg_dump 17 — runner heeft v16 als default op PATH
PG_DUMP_BIN = "/usr/lib/postgresql/17/bin/pg_dump"


def fail(message: str, exit_code: int = 1) -> None:
    print(f"::error::{message}", flush=True)
    sys.exit(exit_code)


def log(message: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {message}", flush=True)


def run_pg_dump(db_url: str, output_path: Path) -> None:
    """Run pg_dump with --no-owner and --no-privileges for portable restores."""
    log("Starting pg_dump...")
    raw_path = output_path.with_suffix("")  # .sql intermediate

    # Verifieer dat we de juiste pg_dump gebruiken
    version_check = subprocess.run([PG_DUMP_BIN, "--version"], capture_output=True, text=True)
    log(f"Using: {version_check.stdout.strip()}")

    cmd = [
        PG_DUMP_BIN,
        "--no-owner",
        "--no-privileges",
        "--clean",
        "--if-exists",
        "--quote-all-identifiers",
        "--file", str(raw_path),
        db_url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        fail(f"pg_dump failed: {result.stderr}")

    log(f"pg_dump complete. Raw size: {raw_path.stat().st_size:,} bytes")

    log("Compressing with gzip...")
    with open(raw_path, "rb") as f_in, gzip.open(output_path, "wb", compresslevel=9) as f_out:
        shutil.copyfileobj(f_in, f_out)
    raw_path.unlink()

    log(f"Compressed size: {output_path.stat().st_size:,} bytes")


def get_drive_service():
    creds, _ = google_auth_default(scopes=DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def upload_to_drive(service, file_path: Path, folder_id: str) -> str:
    log(f"Uploading {file_path.name} to Drive folder {folder_id}...")
    media = MediaFileUpload(str(file_path), mimetype="application/gzip", resumable=True)
    metadata = {"name": file_path.name, "parents": [folder_id]}
    try:
        result = service.files().create(
            body=metadata, media_body=media, fields="id, name, size", supportsAllDrives=True
        ).execute()
    except HttpError as exc:
        fail(f"Drive upload failed: {exc}")
    log(f"Upload complete. File ID: {result['id']} | Size: {int(result.get('size', 0)):,} bytes")
    return result["id"]


def cleanup_old_backups(service, folder_id: str, retention_days: int) -> None:
    log(f"Cleaning up backups older than {retention_days} days...")
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    query = (
        f"'{folder_id}' in parents "
        f"and name contains '{BACKUP_PREFIX}' "
        f"and trashed = false"
    )
    try:
        response = service.files().list(
            q=query,
            fields="files(id, name, createdTime)",
            pageSize=100,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()
    except HttpError as exc:
        log(f"::warning::Could not list files for cleanup: {exc}")
        return

    deleted = 0
    for f in response.get("files", []):
        created = datetime.fromisoformat(f["createdTime"].replace("Z", "+00:00"))
        if created < cutoff:
            try:
                service.files().delete(fileId=f["id"], supportsAllDrives=True).execute()
                log(f"Deleted old backup: {f['name']}")
                deleted += 1
            except HttpError as exc:
                log(f"::warning::Failed to delete {f['name']}: {exc}")

    log(f"Cleanup complete. {deleted} old backups deleted.")


def main() -> None:
    db_url = os.environ.get("SUPABASE_DB_URL")
    folder_id = os.environ.get("GDRIVE_FOLDER_ID")
    if not db_url:
        fail("SUPABASE_DB_URL environment variable is not set.")
    if not folder_id:
        fail("GDRIVE_FOLDER_ID environment variable is not set.")

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    workdir = Path("/tmp/dish-backup")
    workdir.mkdir(parents=True, exist_ok=True)
    backup_path = workdir / f"{BACKUP_PREFIX}{timestamp}.sql.gz"

    run_pg_dump(db_url, backup_path)

    service = get_drive_service()
    upload_to_drive(service, backup_path, folder_id)
    cleanup_old_backups(service, folder_id, RETENTION_DAYS)

    log("Backup completed successfully.")


if __name__ == "__main__":
    main()
