"""
migration_engine.py  (v2 – Cloud SQL + GCS architecture)

PATCH CHANGES vs v1 (CSV CheckpointManager):
  1. Constructor signature changed:
       OLD: (source_drive_ops, dest_drive_ops, config, state_manager, checkpoint_manager)
       NEW: (source_auth, dest_auth, config, checkpoint, gcs_helper, run_id, get_conn)
     - source_auth / dest_auth  : DomainAuthManager auth objects (not drive ops)
     - checkpoint               : SQLStateManager  (replaces CheckpointManager)
     - gcs_helper               : SQLStateManager  (same object, GCS methods)
     - run_id                   : str  migration_runs.migration_id
     - get_conn                 : callable → raw DB connection for permissions migrator

  2. File routing:
       file_size < 50 MB  → in-memory buffer  (_migrate_via_memory)
       file_size >= 50 MB → GCS staging       (_migrate_via_gcs)

  3. All checkpoint calls updated:
       OLD: checkpoint.mark_success / mark_failure / should_skip_item
       NEW: sql_mgr.mark_done / mark_failed / should_skip_item
       Signature: (run_id, file_id, ...) for new calls

  4. Folder mapping stored in SQL via:
       sql_mgr.register_folder_mapping(run_id, source_id, dest_id)

  5. Permission migration uses hybrid model:
       - Fetch fresh from API
       - Apply via API
       - Track result in SQL (migration_permissions table)

  6. migrate_domain() no longer prompts for resume interactively —
     resume is automatic from SQL state.

  7. Per-user auth is re-delegated inside migrate_user() using
     source_auth.with_subject() / dest_auth.with_subject().

  8. No changes to folder hierarchy sort logic or drive creation.
"""

import io
import logging
import socket
import time
import json
import threading
import hashlib
import random
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

logger = logging.getLogger(__name__)
mimetypes.init()

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

LARGE_FILE_THRESHOLD_BYTES = 50 * 1024 * 1024   # 50 MB → route via GCS

RECOMMENDED_MAX_WORKERS    = 2
RECOMMENDED_PARALLEL_FILES = 4

IGNORED_MIME_TYPES = frozenset({
    "application/vnd.google-apps.script",
    "application/vnd.google-apps.form",
    "application/vnd.google-apps.site",
    "application/octet-stream",
})

GOOGLE_WORKSPACE_TYPES = {
    "application/vnd.google-apps.document": {
        "name": "Google Docs",
        "export_mime": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "extension": ".docx",
        "import_mime": "application/vnd.google-apps.document",
        "can_export": True,
        "native": True,
    },
    "application/vnd.google-apps.spreadsheet": {
        "name": "Google Sheets",
        "export_mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "extension": ".xlsx",
        "import_mime": "application/vnd.google-apps.spreadsheet",
        "can_export": True,
        "native": True,
    },
    "application/vnd.google-apps.presentation": {
        "name": "Google Slides",
        "export_mime": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "extension": ".pptx",
        "import_mime": "application/vnd.google-apps.presentation",
        "can_export": True,
        "native": True,
    },
    "application/vnd.google-apps.drawing": {
        "name": "Google Drawings",
        "export_mime": "image/svg+xml",
        "extension": ".svg",
        "import_mime": None,
        "can_export": True,
        "native": False,
        "fallback_mime": "application/pdf",
        "fallback_ext": ".pdf",
    },
    "application/vnd.google-apps.map": {
        "name": "Google My Maps",
        "export_mime": "application/vnd.google-earth.kmz",
        "extension": ".kmz",
        "import_mime": None,
        "can_export": True,
        "native": False,
    },
    "application/vnd.google-apps.jam": {
        "name": "Google Jamboard",
        "export_mime": "application/pdf",
        "extension": ".pdf",
        "import_mime": None,
        "can_export": True,
        "native": False,
    },
    "application/vnd.google-apps.folder": {
        "name": "Folder",
        "export_mime": None,
        "extension": None,
        "import_mime": None,
        "can_export": False,
        "native": True,
    },
}


def _fmt_bytes(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f} PB"


def _fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    return f"{seconds / 3600:.2f}h"

def _jitter(base_seconds: float, jitter_fraction: float = 0.25) -> float:
    """
    Return base_seconds ± up to jitter_fraction of base_seconds.
    Prevents thundering-herd when many threads retry simultaneously.
    Example: _jitter(4) → somewhere between 3.0 and 5.0 seconds.
    """
    jitter = base_seconds * jitter_fraction
    return base_seconds + random.uniform(-jitter, jitter)

# ─────────────────────────────────────────────────────────────────────────────
# MigrationEngine
# ─────────────────────────────────────────────────────────────────────────────

class MigrationEngine:
    """
    Production-grade migration engine.

    Routing:
      file < 50 MB  → downloaded to BytesIO, uploaded directly
      file >= 50 MB → streamed through GCS staging bucket

    State:
      All checkpoint state lives in Cloud SQL via SQLStateManager.
      No CSV files, no SQLite.
    """

    def __init__(
        self,
        source_auth,        # DomainAuthManager.source_auth
        dest_auth,          # DomainAuthManager.dest_auth
        config,             # Config object
        checkpoint,         # SQLStateManager (checkpoint role)
        gcs_helper,         # SQLStateManager (GCS role) — same object
        run_id: str,        # migration_runs.migration_id
        get_conn,           # callable → raw DB connection
    ):
        self.source_auth = source_auth
        self.dest_auth   = dest_auth
        self.config      = config
        self.sql_mgr     = checkpoint    # SQLStateManager
        self.gcs         = gcs_helper    # same SQLStateManager
        self.run_id      = run_id
        self.get_conn    = get_conn

        self.max_retries       = 5
        self.retry_delay       = 3
        self.connection_timeout = 600

        # Per-thread Drive service cache (keyed by email)
        self._drive_cache: Dict[str, object] = {}
        self._drive_cache_lock = threading.Lock()

        self.stats = {
            "total_files":            0,
            "successful":             0,
            "failed":                 0,
            "skipped":                0,
            "ignored":                0,
            "ignored_types":          {},
            "folders_created":        0,
            "folders_failed":         0,
            "bytes_transferred":      0,
            "collaborators_migrated": 0,
            "external_collaborators": 0,
            "connection_resets":      0,
            "gcs_routed":             0,
            "memory_routed":          0,
            "start_time":             None,
            "end_time":               None,
        }

        self._processed: Set[Tuple] = set()
        self._processed_lock = threading.Lock()
    # =========================================================================
    # Public: migrate_domain
    # =========================================================================

    def migrate_domain(
        self,
        user_mapping: Dict[str, str],
        max_workers: int = RECOMMENDED_MAX_WORKERS,
    ) -> Dict:
        """
        Migrate all users in parallel.

        FIX: Default max_workers reduced to 2 for 2-vCPU VM.
        The bottleneck is network I/O to Google APIs + MySQL pool connections,
        not CPU. More than 2 concurrent users thrashes the connection pool
        without improving throughput.
        """
        # Clamp to recommended ceiling for this VM size
        effective_workers = min(max_workers, RECOMMENDED_MAX_WORKERS)
        if max_workers > RECOMMENDED_MAX_WORKERS:
            logger.warning(
                f"max_workers={max_workers} requested but VM has 2 vCPUs. "
                f"Clamping to {RECOMMENDED_MAX_WORKERS} to avoid pool exhaustion."
            )

        logger.info(
            f"Starting domain migration: {len(user_mapping)} users, "
            f"{effective_workers} workers | run_id={self.run_id}"
        )
        self.stats["start_time"] = datetime.now()

        summary = {
            "total_users":                  len(user_mapping),
            "completed_users":              0,
            "failed_users":                 0,
            "total_files_migrated":         0,
            "total_files_failed":           0,
            "total_files_skipped":          0,
            "total_files_ignored":          0,
            "total_folders_created":        0,
            "total_folders_failed":         0,
            "total_collaborators_migrated": 0,
            "total_external_collaborators": 0,
            "accuracy_rate":                0.0,
            "user_results":                 [],
            "start_time":                   self.stats["start_time"].isoformat(),
            "end_time":                     None,
            "detailed_failures":            [],
        }

        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            futures = {
                executor.submit(self.migrate_user, src, dst): (src, dst)
                for src, dst in user_mapping.items()
            }

            for future in as_completed(futures):
                src_email, dst_email = futures[future]
                try:
                    user_result = future.result()
                    summary["user_results"].append(user_result)

                    if user_result["status"] == "completed":
                        summary["completed_users"] += 1
                    else:
                        summary["failed_users"] += 1

                    summary["total_files_migrated"]         += user_result["files_migrated"]
                    summary["total_files_failed"]           += user_result["files_failed"]
                    summary["total_files_skipped"]          += user_result["files_skipped"]
                    summary["total_files_ignored"]          += user_result.get("files_ignored", 0)
                    summary["total_folders_created"]        += user_result.get("folders_created", 0)
                    summary["total_folders_failed"]         += user_result.get("folders_failed", 0)
                    summary["total_collaborators_migrated"] += user_result.get("collaborators_migrated", 0)
                    summary["total_external_collaborators"] += user_result.get("external_collaborators", 0)

                    if user_result.get("errors"):
                        summary["detailed_failures"].extend(user_result["errors"])

                except Exception as exc:
                    logger.error(f"User migration failed {src_email}: {exc}", exc_info=True)
                    summary["failed_users"] += 1

        total = summary["total_files_migrated"] + summary["total_files_failed"]
        if total > 0:
            summary["accuracy_rate"] = (summary["total_files_migrated"] / total) * 100

        self.stats["end_time"] = datetime.now()
        summary["end_time"]         = self.stats["end_time"].isoformat()
        summary["duration_seconds"] = (
            self.stats["end_time"] - self.stats["start_time"]
        ).total_seconds()

        logger.info(
            f"Domain migration complete: {summary['accuracy_rate']:.2f}% accuracy | "
            f"GCS routed: {self.stats['gcs_routed']} | "
            f"Memory routed: {self.stats['memory_routed']}"
        )
        return summary

    # =========================================================================
    # Public: migrate_user
    # =========================================================================

    def migrate_user(self, source_email: str, dest_email: str) -> Dict:
        self.sql_mgr.start_user(self.run_id, source_email)
        logger.info(f"Starting user migration: {source_email} → {dest_email}")
 
        user_result = {
            "source_email":           source_email,
            "dest_email":             dest_email,
            "status":                 "in_progress",
            "files_total":            0,
            "files_migrated":         0,
            "files_failed":           0,
            "files_skipped":          0,
            "files_ignored":          0,
            "folders_total":          0,
            "folders_created":        0,
            "folders_failed":         0,
            "collaborators_migrated": 0,
            "external_collaborators": 0,
            "accuracy_rate":          0.0,
            "errors":                 [],
            "start_time":             datetime.now().isoformat(),
        }
 
        try:
            source_drive = self._get_drive_service_cached(self.source_auth, source_email)
            dest_drive   = self._get_drive_service_cached(self.dest_auth,   dest_email)
 
            existing_items = self.sql_mgr.load_user_items(source_email)
 
            if existing_items:
                logger.info(
                    f"  [RESUME] Found {len(existing_items)} items in SQL "
                    f"for {source_email}"
                )
                all_folders, all_files = self._split_items_from_records(existing_items)
                folder_mapping = self.sql_mgr.get_folder_mapping(self.run_id, source_email)
                logger.info(f"  [RESUME] Loaded {len(folder_mapping)} folder mappings from SQL")
 
                missing_folders = [
                    f for f in all_folders
                    if (f.get("id") or f.get("file_id")) not in folder_mapping
                ]
                if missing_folders:
                    logger.info(f"  [RESUME] Creating {len(missing_folders)} missing folders")
                    new_fm = self._build_folder_structure(
                        missing_folders, source_drive, dest_drive,
                        source_email, dest_email,
                    )
                    folder_mapping.update(new_fm)
            else:
                logger.info(f"  [FIRST RUN] Discovering files for {source_email}...")
                source_files = self._get_all_user_owned_files(source_drive, source_email)
 
                if not source_files:
                    logger.info(f"  No files found for {source_email}")
                    user_result["status"]        = "completed"
                    user_result["accuracy_rate"] = 100.0
                    user_result["end_time"]      = datetime.now().isoformat()
                    self.sql_mgr.finish_user(self.run_id, source_email, "completed")
                    return user_result
 
                self.sql_mgr.bulk_register_items(self.run_id, [
                    {**f, "source_email": source_email, "dest_email": dest_email}
                    for f in source_files
                ])
 
                all_folders = [
                    f for f in source_files
                    if f["mimeType"] == "application/vnd.google-apps.folder"
                ]
                all_files = [
                    f for f in source_files
                    if f["mimeType"] != "application/vnd.google-apps.folder"
                ]
 
                folder_mapping = self._build_folder_structure(
                    all_folders, source_drive, dest_drive,
                    source_email, dest_email,
                )
 
            user_result["files_total"]     = len(all_files)
            user_result["folders_total"]   = len(all_folders)
            user_result["folders_created"] = len(folder_mapping)
            user_result["folders_failed"]  = len(all_folders) - len(folder_mapping)
 
            files_done   = 0
            files_failed = 0
            bytes_moved  = 0
 
            # FIX 4: Tuned parallel_files for 2-vCPU VM.
            # 4 file threads per user = 8 threads total when 2 users run concurrently.
            # This is the sweet spot: enough to keep network saturated without
            # causing GIL thrashing or MySQL pool exhaustion.
            parallel_files = min(
                getattr(self.config, "PARALLEL_FILES", RECOMMENDED_PARALLEL_FILES),
                RECOMMENDED_PARALLEL_FILES,
            )
 
            def _should_process(file_info):
                fid   = file_info.get("id") or file_info.get("file_id") or file_info.get("source_item_id")
                fname = file_info.get("name") or file_info.get("file_name") or file_info.get("source_item_name", "")
                mime  = file_info.get("mimeType") or file_info.get("mime_type", "")
                fsize = int(file_info.get("size") or file_info.get("file_size_bytes") or 0)
                pids  = file_info.get("parents", [])
                if not pids and file_info.get("source_parent_id"):
                    pids = [file_info["source_parent_id"]]
 
                if mime in IGNORED_MIME_TYPES:
                    user_result["files_ignored"] += 1
                    self.sql_mgr.mark_ignored(self.run_id, fid, "Non-migratable MIME type")
                    return None
 
                should_skip, reason = self.sql_mgr.should_skip_item(fid)
                if should_skip:
                    if reason == "Already migrated":
                        user_result["files_skipped"] += 1
                    else:
                        user_result["files_ignored"] += 1
                    return None
 
                sig = (fid, fname, fsize)
                with self._processed_lock:
                    if sig in self._processed:
                        user_result["files_skipped"] += 1
                        return None
 
                return fid, fname, mime, fsize, pids
 
            def _migrate_one(file_info):
                parsed = _should_process(file_info)
                if parsed is None:
                    return None
 
                fid, fname, mime, fsize, pids = parsed
                self.sql_mgr.mark_in_progress(self.run_id, fid)
 
                dest_parent = folder_mapping.get(pids[0]) if pids else None
 
                result = self._migrate_file_v3(
                    fid, fname, mime, fsize, dest_parent,
                    source_drive, dest_drive,
                )
                result["_fid"]         = fid
                result["_fname"]       = fname
                result["_fsize"]       = fsize
                result["_sig"]         = (fid, fname, fsize)
                result["_dest_parent"] = dest_parent
                return result
 
            with ThreadPoolExecutor(max_workers=parallel_files) as file_executor:
                futures = {
                    file_executor.submit(_migrate_one, fi): fi
                    for fi in all_files
                }
 
                for future in as_completed(futures):
                    try:
                        result = future.result()
                    except Exception as exc:
                        fi    = futures[future]
                        fname = fi.get("name") or fi.get("file_name", "")
                        logger.error(f"  File future error [{fname}]: {exc}", exc_info=True)
                        user_result["files_failed"] += 1
                        files_failed += 1
                        continue
 
                    if result is None:
                        continue
 
                    fid         = result["_fid"]
                    fname       = result["_fname"]
                    fsize       = result["_fsize"]
                    sig         = result["_sig"]
                    dest_parent = result["_dest_parent"]
 
                    if result["success"]:
                        dest_id = result.get("dest_id")
                        self.sql_mgr.mark_done(
                            self.run_id, fid,
                            dest_item_id=dest_id,
                            dest_parent_id=dest_parent,
                        )
                        with self._processed_lock:
                            self._processed.add(sig)
                        user_result["files_migrated"] += 1
                        files_done  += 1
                        bytes_moved += fsize
 
                        if dest_id:
                            perm_r = self._migrate_permissions_hybrid(
                                fid, dest_id, fname, source_drive, dest_drive
                            )
                            user_result["collaborators_migrated"] += perm_r.get("migrated", 0)
                            user_result["external_collaborators"] += perm_r.get("external", 0)
 
                    elif result.get("ignored"):
                        user_result["files_ignored"] += 1
                        self.sql_mgr.mark_ignored(self.run_id, fid, result.get("error", ""))
 
                    else:
                        user_result["files_failed"] += 1
                        files_failed += 1
                        self.sql_mgr.mark_failed(
                            self.run_id, fid, result.get("error", "Unknown")
                        )
                        user_result["errors"].append({
                            "file":       fname,
                            "file_id":    fid,
                            "error":      result.get("error", ""),
                            "error_type": result.get("error_type", ""),
                            "user":       source_email,
                        })
 
            total_attempted = (
                user_result["files_total"]
                - user_result["files_skipped"]
                - user_result["files_ignored"]
            )
            user_result["accuracy_rate"] = (
                (user_result["files_migrated"] / total_attempted * 100)
                if total_attempted > 0 else 100.0
            )
            user_result["status"]   = "completed"
            user_result["end_time"] = datetime.now().isoformat()
 
            self.sql_mgr.finish_user(
                self.run_id, source_email, "completed",
                files_done=files_done,
                files_failed=files_failed,
                bytes_moved=bytes_moved,
            )
 
            logger.info(
                f"✓ {source_email}: "
                f"{user_result['files_migrated']}/{user_result['files_total']} "
                f"({user_result['accuracy_rate']:.1f}%)"
            )
 
        except Exception as exc:
            logger.error(f"User migration failed {source_email}: {exc}", exc_info=True)
            user_result["status"]   = "failed"
            user_result["end_time"] = datetime.now().isoformat()
            self.sql_mgr.finish_user(self.run_id, source_email, "failed")
 
        return user_result

    # =========================================================================
    # Core: _migrate_file_v3  (GCS / memory routing)
    # =========================================================================

    def _migrate_file_v3(
        self,
        file_id: str,
        file_name: str,
        mime_type: str,
        file_size: int,
        dest_parent_id: Optional[str],
        source_drive,
        dest_drive,
    ) -> Dict:
        empty = {"success": False, "dest_id": None, "ignored": False, "error": None}
 
        if mime_type in IGNORED_MIME_TYPES:
            return {**empty, "ignored": True, "error": "Non-migratable MIME type"}
 
        if mime_type in GOOGLE_WORKSPACE_TYPES:
            return self._migrate_workspace_file(
                file_id, file_name, mime_type, dest_parent_id,
                source_drive, dest_drive,
            )
 
        if file_size >= LARGE_FILE_THRESHOLD_BYTES:
            return self._migrate_via_gcs(
                file_id, file_name, mime_type, file_size,
                dest_parent_id, source_drive, dest_drive,
            )
        else:
            return self._migrate_via_memory(
                file_id, file_name, mime_type, file_size,
                dest_parent_id, source_drive, dest_drive,
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Memory path  (<50 MB)
    # ─────────────────────────────────────────────────────────────────────────

    def _migrate_via_memory(
        self,
        file_id: str,
        file_name: str,
        mime_type: str,
        file_size: int,
        dest_parent_id: Optional[str],
        source_drive,
        dest_drive,
    ) -> Dict:
        """
        FIX 2: Each call creates its own BytesIO and MediaIoBaseUpload.
        Never reuse or share upload media objects across calls/threads —
        that was causing Content-Range cross-contamination (HTTP 400 errors).
        """
        empty = {"success": False, "dest_id": None, "ignored": False, "error": None}
        last_error = ""
 
        for attempt in range(self.max_retries):
            wait = _jitter(2 ** attempt)   # exponential backoff with jitter
 
            try:
                # ── Download ──────────────────────────────────────────────
                request = source_drive.files().get_media(
                    fileId=file_id, supportsAllDrives=True
                )
                # FIX 2: fresh BytesIO per attempt — never reuse
                dl_buf = io.BytesIO()
                dl     = MediaIoBaseDownload(dl_buf, request, chunksize=20 * 1024 * 1024)
                done   = False
                while not done:
                    _, done = dl.next_chunk()
                dl_buf.seek(0)
                data = dl_buf.read()
                dl_buf.close()
 
                # ── Empty file ────────────────────────────────────────────
                if not data:
                    if file_size == 0:
                        # FIX 7: use simple create() for empty files, NOT resumable upload
                        meta = {"name": file_name}
                        if dest_parent_id:
                            meta["parents"] = [dest_parent_id]
                        f = dest_drive.files().create(
                            body=meta, fields="id",
                            supportsAllDrives=True,
                        ).execute()
                        self.stats["memory_routed"] += 1
                        return {**empty, "success": True, "dest_id": f["id"]}
                    else:
                        last_error = "Empty download for non-zero file"
                        logger.warning(f"  [MEM] {last_error} [{file_name}]")
                        if attempt < self.max_retries - 1:
                            time.sleep(wait)
                            continue
                        return {**empty, "error": last_error, "error_type": "empty_download"}
 
                # ── Upload ────────────────────────────────────────────────
                # FIX 2: fresh BytesIO + fresh MediaIoBaseUpload per upload attempt.
                # This is the key fix for HTTP 400 Content-Range mismatch errors.
                # The old code created upload_buf once and reused it across retries —
                # after the first attempt the seek position was wrong.
                meta = {"name": file_name}
                if dest_parent_id:
                    meta["parents"] = [dest_parent_id]
 
                upload_buf = io.BytesIO(data)   # fresh buffer per attempt
                media = MediaIoBaseUpload(
                    upload_buf,
                    mimetype=mime_type,
                    resumable=True,
                    chunksize=20 * 1024 * 1024,
                )
                f = dest_drive.files().create(
                    body=meta, media_body=media,
                    fields="id", supportsAllDrives=True,
                ).execute()
                upload_buf.close()
 
                self.stats["memory_routed"] += 1
                return {**empty, "success": True, "dest_id": f["id"]}
 
            except HttpError as exc:
                code = exc.resp.status
                last_error = str(exc)
 
                if code == 200:
                    # Resumable upload completion false-error from googleapiclient
                    try:
                        body = json.loads(exc.content.decode("utf-8"))
                        dest_id = body.get("id")
                        if dest_id:
                            self.stats["memory_routed"] += 1
                            return {**empty, "success": True, "dest_id": dest_id}
                    except Exception:
                        pass
                    self.stats["memory_routed"] += 1
                    return {**empty, "success": True, "dest_id": None}
 
                if code == 403 and (
                    "cannotDownload" in last_error
                    or "fileNotDownloadable" in last_error
                ):
                    logger.warning(f"  [MEM] Download restricted — skipping [{file_name}]")
                    return {**empty, "ignored": True, "error": "Download restricted"}
 
                if code in (429, 500, 503) and attempt < self.max_retries - 1:
                    logger.warning(f"  [MEM] HTTP {code}, retry {attempt+1} in {wait:.1f}s [{file_name}]")
                    time.sleep(wait)
                    continue
 
                logger.error(f"  [MEM] HTTP {code} [{file_name}]: {last_error}")
                return {**empty, "error": last_error, "error_type": f"http_{code}"}
 
            except (ConnectionResetError, ConnectionError, OSError) as exc:
                last_error = str(exc)
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"  [MEM] Retry {attempt+1}/{self.max_retries} "
                        f"[{file_name}]: {last_error} — wait {wait:.1f}s"
                    )
                    time.sleep(wait)
                else:
                    logger.error(f"  [MEM] Exhausted retries [{file_name}]: {last_error}")
 
            except Exception as exc:
                last_error = str(exc)
                logger.error(f"  [MEM] Unexpected [{file_name}]: {last_error}")
                # Don't retry unexpected errors — they're usually logic bugs
                return {**empty, "error": last_error, "error_type": "unexpected"}
 
        return {**empty, "error": last_error, "error_type": "memory_transfer_failed"}

    # ─────────────────────────────────────────────────────────────────────────
    # GCS path  (>=50 MB)
    # ─────────────────────────────────────────────────────────────────────────

    def _migrate_via_gcs(
        self,
        file_id: str,
        file_name: str,
        mime_type: str,
        file_size: int,
        dest_parent_id: Optional[str],
        source_drive,
        dest_drive,
    ) -> Dict:
        """
        Flow:
          1. Download file from source Drive → GCS staging bucket
          2. Upload from GCS bucket → destination Drive
          3. Delete from GCS bucket (cleanup)

        Key fixes vs v2:
          - Each retry uses a UNIQUE blob name (blob_name + attempt suffix).
            Old code reused the same blob name, so a partial/corrupt download
            from attempt 1 would poison attempt 2.
          - Stale/partial blobs are deleted before each retry starts.
          - Upload streams directly from GCS via blob.open('rb') —
            no full file load into RAM.
        """
        empty = {"success": False, "dest_id": None, "ignored": False, "error": None}
        last_error  = ""
        active_blob = None   # tracks the blob name for the current attempt

        for attempt in range(self.max_retries):
            wait = _jitter(2 ** attempt)

            # Use a unique blob name per attempt so a partial blob from a
            # previous attempt never interferes with the current one.
            attempt_blob = f"{self.run_id}/{file_id}/attempt_{attempt}"

            try:
                # ── STEP 1: Source Drive → GCS bucket ────────────────────
                logger.debug(f"  [GCS] Downloading [{file_name}] → bucket (attempt {attempt+1})")
                ok, blob_name, err = self.gcs.download_drive_to_gcs(
                    drive_svc  = source_drive,
                    file_id    = file_id,
                    file_name  = file_name,
                    run_id     = attempt_blob,   # unique per attempt
                    mime_type  = mime_type,
                )

                if not ok:
                    last_error = err or "GCS download failed"
                    # Clean up any partial blob that may have been written
                    if blob_name:
                        try:
                            self.gcs.delete_temp(blob_name)
                        except Exception:
                            pass
                    if attempt < self.max_retries - 1:
                        logger.warning(
                            f"  [GCS] Download failed (attempt {attempt+1}/{self.max_retries}) "
                            f"[{file_name}]: {last_error} — retrying in {wait:.1f}s"
                        )
                        time.sleep(wait)
                        continue
                    logger.error(f"  [GCS] Download exhausted retries [{file_name}]: {last_error}")
                    return {**empty, "error": last_error, "error_type": "gcs_download_failed"}

                active_blob = blob_name
                logger.debug(f"  [GCS] ✓ In bucket: {blob_name} ({_fmt_bytes(file_size)})")

                # ── STEP 2: GCS bucket → destination Drive ────────────────
                logger.debug(f"  [GCS] Uploading [{file_name}] → dest Drive")
                ok2, dest_id, err2 = self.gcs.upload_gcs_to_drive(
                    drive_svc = dest_drive,
                    blob_name = blob_name,
                    file_name = file_name,
                    mime_type = mime_type,
                    parent_id = dest_parent_id,
                )

                if not ok2:
                    last_error = err2 or "GCS upload failed"
                    # Delete the successfully-downloaded blob so next attempt
                    # re-downloads a fresh copy (don't retry upload with a blob
                    # whose session URL may be invalidated).
                    try:
                        self.gcs.delete_temp(blob_name)
                    except Exception:
                        pass
                    active_blob = None
                    if attempt < self.max_retries - 1:
                        logger.warning(
                            f"  [GCS] Upload failed (attempt {attempt+1}/{self.max_retries}) "
                            f"[{file_name}]: {last_error} — retrying in {wait:.1f}s"
                        )
                        time.sleep(wait)
                        continue
                    logger.error(f"  [GCS] Upload exhausted retries [{file_name}]: {last_error}")
                    return {**empty, "error": last_error, "error_type": "gcs_upload_failed"}

                # ── STEP 3: Cleanup GCS blob ──────────────────────────────
                try:
                    self.gcs.delete_temp(blob_name)
                    logger.debug(f"  [GCS] Blob deleted: {blob_name}")
                except Exception as del_exc:
                    # Non-fatal — blob will expire via lifecycle policy
                    logger.warning(f"  [GCS] Blob delete failed (non-fatal): {del_exc}")

                active_blob = None
                self.stats["gcs_routed"] += 1
                logger.info(f"  [GCS] ✓ {file_name} ({_fmt_bytes(file_size)}) → dest_id={dest_id}")
                return {**empty, "success": True, "dest_id": dest_id}

            except (ConnectionResetError, ConnectionError, OSError) as exc:
                last_error = str(exc)
                # Clean up partial blob before retry
                if active_blob:
                    try:
                        self.gcs.delete_temp(active_blob)
                    except Exception:
                        pass
                    active_blob = None
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"  [GCS] Connection error (attempt {attempt+1}/{self.max_retries}) "
                        f"[{file_name}]: {last_error} — retrying in {wait:.1f}s"
                    )
                    time.sleep(wait)
                else:
                    logger.error(f"  [GCS] Exhausted retries [{file_name}]: {last_error}")

            except Exception as exc:
                last_error = str(exc)
                logger.error(f"  [GCS] Unexpected [{file_name}]: {last_error}")
                # Clean up any blob from this attempt
                if active_blob:
                    try:
                        self.gcs.delete_temp(active_blob)
                    except Exception:
                        pass
                break   # Don't retry unexpected errors — likely a logic bug

        # Final safety cleanup if something slipped through
        if active_blob:
            try:
                self.gcs.delete_temp(active_blob)
            except Exception:
                pass

        return {**empty, "error": last_error, "error_type": "gcs_transfer_failed"}

    # ─────────────────────────────────────────────────────────────────────────
    # Google Workspace export path
    # ─────────────────────────────────────────────────────────────────────────

    def _migrate_workspace_file(
        self,
        file_id: str,
        file_name: str,
        mime_type: str,
        dest_parent_id: Optional[str],
        source_drive,
        dest_drive,
    ) -> Dict:
        empty = {"success": False, "dest_id": None, "ignored": False, "error": None}
 
        type_info = GOOGLE_WORKSPACE_TYPES.get(mime_type)
        if not type_info or not type_info.get("can_export"):
            return {**empty, "ignored": True, "error": f"Non-exportable: {mime_type}"}
 
        for attempt in range(self.max_retries):
            wait = _jitter(2 ** attempt)
 
            try:
                request = source_drive.files().export_media(
                    fileId=file_id, mimeType=type_info["export_mime"]
                )
                # FIX 2: fresh buffer per attempt
                dl_buf = io.BytesIO()
                dl     = MediaIoBaseDownload(dl_buf, request, chunksize=10 * 1024 * 1024)
                done   = False
                while not done:
                    _, done = dl.next_chunk()
                dl_buf.seek(0)
                data = dl_buf.read()
                dl_buf.close()
 
                if not data:
                    return {**empty, "error": "Empty export", "error_type": "empty_export"}
 
                meta = {"name": file_name}
                if dest_parent_id:
                    meta["parents"] = [dest_parent_id]
                if type_info.get("import_mime"):
                    meta["mimeType"] = type_info["import_mime"]
 
                # FIX 2: fresh BytesIO + MediaIoBaseUpload per attempt
                upload_buf = io.BytesIO(data)
                media = MediaIoBaseUpload(
                    upload_buf,
                    mimetype=type_info["export_mime"],
                    resumable=True,
                    chunksize=10 * 1024 * 1024,
                )
                f = dest_drive.files().create(
                    body=meta, media_body=media,
                    fields="id", supportsAllDrives=True,
                ).execute()
                upload_buf.close()
 
                self.stats["memory_routed"] += 1
                return {**empty, "success": True, "dest_id": f["id"]}
 
            except HttpError as exc:
                err  = str(exc)
                code = exc.resp.status
 
                if code == 200:
                    try:
                        body = json.loads(exc.content.decode("utf-8"))
                        dest_id = body.get("id")
                        if dest_id:
                            self.stats["memory_routed"] += 1
                            return {**empty, "success": True, "dest_id": dest_id}
                    except Exception:
                        pass
                    self.stats["memory_routed"] += 1
                    return {**empty, "success": True, "dest_id": None}
 
                if "exportSizeLimitExceeded" in err and "fallback_mime" in type_info:
                    return self._migrate_workspace_fallback(
                        file_id, file_name, type_info, dest_parent_id,
                        source_drive, dest_drive,
                    )
 
                if code in (429, 500, 503) and attempt < self.max_retries - 1:
                    logger.warning(f"  [WS] HTTP {code} retry [{file_name}] wait {wait:.1f}s")
                    time.sleep(wait)
                    continue
 
                logger.error(f"  [WS] HTTP {code} [{file_name}]: {err}")
                return {**empty, "error": err, "error_type": f"http_{code}"}
 
            except Exception as exc:
                err = str(exc)
                if attempt < self.max_retries - 1:
                    logger.warning(f"  [WS] Retry {attempt+1} [{file_name}]: {err}")
                    time.sleep(wait)
                else:
                    logger.error(f"  [WS] Failed [{file_name}]: {err}")
                    return {**empty, "error": err, "error_type": "workspace_export_failed"}
 
        return {**empty, "error": "Max retries exceeded", "error_type": "workspace_export_failed"}
 
    def _migrate_workspace_fallback(
        self,
        file_id: str,
        file_name: str,
        type_info: Dict,
        dest_parent_id: Optional[str],
        source_drive,
        dest_drive,
    ) -> Dict:
        empty = {"success": False, "dest_id": None, "ignored": False, "error": None}
        try:
            request = source_drive.files().export_media(
                fileId=file_id, mimeType=type_info["fallback_mime"]
            )
            dl_buf = io.BytesIO()
            dl     = MediaIoBaseDownload(dl_buf, request, chunksize=10 * 1024 * 1024)
            done   = False
            while not done:
                _, done = dl.next_chunk()
            dl_buf.seek(0)
 
            fallback_name = file_name + type_info["fallback_ext"]
            meta = {"name": fallback_name}
            if dest_parent_id:
                meta["parents"] = [dest_parent_id]
 
            media = MediaIoBaseUpload(
                dl_buf,
                mimetype=type_info["fallback_mime"],
                resumable=True,
                chunksize=10 * 1024 * 1024,
            )
            f = dest_drive.files().create(
                body=meta, media_body=media,
                fields="id", supportsAllDrives=True,
            ).execute()
 
            logger.info(f"  [WS-FALLBACK] ✓ {fallback_name}")
            return {**empty, "success": True, "dest_id": f["id"]}
 
        except Exception as exc:
            return {**empty, "error": str(exc), "error_type": "workspace_fallback_failed"}

    # =========================================================================
    # Permissions: hybrid model (fetch API → apply API → track SQL)
    # =========================================================================

    def _migrate_permissions_hybrid(
        self,
        source_id: str,
        dest_id: str,
        name: str,
        source_drive,
        dest_drive,
    ) -> Dict:
        result = {"migrated": 0, "failed": 0, "external": 0, "skipped": 0}
 
        try:
            resp = source_drive.permissions().list(
                fileId=source_id,
                fields="permissions(id,type,role,emailAddress,domain,displayName)",
                supportsAllDrives=True,
            ).execute()
            perms = resp.get("permissions", [])
            if len(perms) <= 1:
                return result
        except Exception as exc:
            logger.debug(f"  Permissions fetch failed [{name}]: {exc}")
            return result
 
        try:
            from permissions_migrator import EnhancedPermissionsMigrator
 
            pm = EnhancedPermissionsMigrator(
                source_drive, dest_drive,
                self.config.SOURCE_DOMAIN,
                self.config.DEST_DOMAIN,
            )
            pr = pm.migrate_permissions(source_id, dest_id, perms)
 
            result["migrated"] = pr.get("migrated", 0)
            result["failed"]   = pr.get("failed", 0)
            result["external"] = pr.get("external_users", 0)
            result["skipped"]  = pr.get("skipped", 0)
 
            for detail in pr.get("details", []):
                role           = detail.get("role", "")
                ptype          = detail.get("type", "user")
                status         = detail.get("status", "failed")
                classification = detail.get("classification", "external_domain")
                error          = detail.get("error", "")
                src_email      = detail.get("original_email", "")
                dst_email      = detail.get("target_email", "")
 
                if role == "owner" or status == "skipped":
                    continue
 
                valid_roles = {
                    "owner", "organizer", "fileOrganizer",
                    "writer", "commenter", "reader",
                }
                valid_classifications = {
                    "internal_both_domains", "internal_source_only",
                    "external_domain", "general_access",
                }
                if role not in valid_roles:
                    continue
                if classification not in valid_classifications:
                    classification = "external_domain"
 
                try:
                    self.sql_mgr.upsert_permission(
                        file_id         = dest_id,
                        item_type       = "FILE",
                        permission_type = ptype if ptype in ("user","group","domain","anyone") else "user",
                        source_email    = src_email,
                        dest_email      = dst_email,
                        role            = role,
                        classification  = classification,
                        is_inherited    = False,
                        parent_drive_id = None,
                    )
                    if status == "success":
                        self.sql_mgr.mark_permission_done(dest_id, dst_email, role)
                    elif status == "failed":
                        self.sql_mgr.mark_permission_failed(dest_id, dst_email, role, error)
                except Exception as exc:
                    logger.debug(f"  SQL permission track error [{name}]: {exc}")
 
        except ImportError:
            logger.error("EnhancedPermissionsMigrator not available")
        except Exception as exc:
            logger.debug(f"  Permission migration error [{name}]: {exc}")
 
        return result

    # =========================================================================
    # Folder structure
    # =========================================================================

    def _build_folder_structure(
        self,
        folders: List[Dict],
        source_drive,
        dest_drive,
        source_email: str,
        dest_email: str,
    ) -> Dict[str, str]:
        if not folders:
            return {}
 
        logger.info(f"  Building folder structure: {len(folders)} folders")
 
        id_set  = {f.get("id") or f.get("file_id") or f.get("source_item_id") for f in folders}
        visited = set()
        sorted_folders: List[Dict] = []
 
        def visit(folder: Dict):
            fid = folder.get("id") or folder.get("file_id") or folder.get("source_item_id")
            if fid in visited:
                return
            visited.add(fid)
            pids = folder.get("parents", [])
            if not pids and folder.get("source_parent_id"):
                pids = [folder["source_parent_id"]]
            if pids and pids[0] in id_set:
                parent = next(
                    (f for f in folders
                     if (f.get("id") or f.get("file_id") or f.get("source_item_id")) == pids[0]),
                    None,
                )
                if parent:
                    visit(parent)
            sorted_folders.append(folder)
 
        for f in folders:
            visit(f)
 
        folder_mapping: Dict[str, str] = {}
 
        for folder in sorted_folders:
            fid   = folder.get("id") or folder.get("file_id") or folder.get("source_item_id")
            fname = folder.get("name") or folder.get("file_name") or folder.get("source_item_name", "")
            pids  = folder.get("parents", [])
            if not pids and folder.get("source_parent_id"):
                pids = [folder["source_parent_id"]]
 
            self.sql_mgr.mark_in_progress(self.run_id, fid)
            dest_parent = folder_mapping.get(pids[0]) if pids else None
            dest_fid    = self._create_folder(fname, dest_parent, dest_drive)
 
            if dest_fid:
                folder_mapping[fid] = dest_fid
                self.sql_mgr.register_folder_mapping(self.run_id, fid, dest_fid)
                self.sql_mgr.mark_done(
                    self.run_id, fid,
                    dest_item_id=dest_fid,
                    dest_parent_id=dest_parent,
                )
                self.stats["folders_created"] += 1
            else:
                self.sql_mgr.mark_failed(self.run_id, fid, "Failed to create folder")
                self.stats["folders_failed"] += 1
                logger.error(f"    ✗ Folder failed: {fname}")
 
        return folder_mapping
 
    def _create_folder(
        self,
        folder_name: str,
        parent_id: Optional[str],
        dest_drive,
        max_retries: int = 3,
    ) -> Optional[str]:
        for attempt in range(max_retries):
            try:
                meta = {
                    "name":     folder_name,
                    "mimeType": "application/vnd.google-apps.folder",
                }
                if parent_id:
                    meta["parents"] = [parent_id]
                f = dest_drive.files().create(
                    body=meta, fields="id,name",
                    supportsAllDrives=True,
                ).execute()
                return f["id"]
            except HttpError as exc:
                if exc.resp.status == 409:
                    existing = self._find_existing_folder(folder_name, parent_id, dest_drive)
                    if existing:
                        return existing
                if attempt < max_retries - 1:
                    time.sleep(_jitter(2 ** attempt))
                else:
                    logger.error(f"Failed to create folder '{folder_name}': {exc}")
                    return None
            except Exception as exc:
                if attempt < max_retries - 1:
                    time.sleep(_jitter(2 ** attempt))
                else:
                    logger.error(f"Error creating folder '{folder_name}': {exc}")
                    return None
        return None
 
    def _find_existing_folder(
        self,
        name: str,
        parent_id: Optional[str],
        dest_drive,
    ) -> Optional[str]:
        try:
            q = (
                f"name='{name}' and "
                "mimeType='application/vnd.google-apps.folder' and "
                "trashed=false"
            )
            if parent_id:
                q += f" and '{parent_id}' in parents"
            resp = dest_drive.files().list(
                q=q, fields="files(id)", pageSize=5,
                supportsAllDrives=True,
            ).execute()
            files = resp.get("files", [])
            return files[0]["id"] if files else None
        except Exception:
            return None
 

    # =========================================================================
    # File discovery
    # =========================================================================

    def _get_all_user_owned_files(
        self, drive_service, user_email: str
    ) -> List[Dict]:
        files      = []
        page_token = None
        retries    = 0
        max_retries = 5
 
        while True:
            try:
                resp = drive_service.files().list(
                    q="trashed=false",
                    spaces="drive",
                    fields=(
                        "nextPageToken, files("
                        "id, name, mimeType, size, parents, "
                        "createdTime, modifiedTime, owners)"
                    ),
                    pageSize=100,
                    pageToken=page_token,
                    supportsAllDrives=False,
                    includeItemsFromAllDrives=False,
                ).execute()
 
                batch = resp.get("files", [])
                owned = [
                    f for f in batch
                    if any(
                        o.get("emailAddress") == user_email
                        for o in f.get("owners", [])
                    )
                ]
                files.extend(owned)
 
                page_token = resp.get("nextPageToken")
                if not page_token:
                    break
 
                retries = 0
                time.sleep(0.2)
 
            except HttpError as exc:
                code = exc.resp.status
                if code == 500 and retries < max_retries:
                    retries += 1
                    time.sleep(_jitter(2 ** retries))
                    continue
                elif code == 404:
                    logger.warning(f"Drive not found for {user_email}")
                    return []
                else:
                    logger.error(f"HTTP {code} fetching files for {user_email}: {exc}")
                    raise
 
            except Exception as exc:
                retries += 1
                if retries >= max_retries:
                    logger.error(f"Failed to fetch files for {user_email}: {exc}")
                    return files
                time.sleep(_jitter(2 ** retries))
 
        logger.info(f"  Discovered {len(files)} owned files for {user_email}")
        return files

    # =========================================================================
    # Auth helper
    # =========================================================================
    
    def _get_drive_service_cached(self, auth_obj, email: str):
        """
        FIX 5: Cache Drive service per email to avoid re-authenticating
        (which involves RSA signing + HTTP round-trip) on every file.
        Thread-safe via lock.
        """
        with self._drive_cache_lock:
            if email in self._drive_cache:
                return self._drive_cache[email]
 
        svc = self._get_drive_service(auth_obj, email)
 
        with self._drive_cache_lock:
            self._drive_cache[email] = svc
 
        return svc
 
    def _get_drive_service(self, auth_obj, email: str):
        from googleapiclient.discovery import build
        try:
            from auth import GoogleAuthManager
 
            if email.endswith(f"@{self.config.SOURCE_DOMAIN}"):
                creds_file = self.config.SOURCE_CREDENTIALS_FILE
            else:
                creds_file = self.config.DEST_CREDENTIALS_FILE
 
            user_auth = GoogleAuthManager(
                creds_file,
                self.config.SCOPES,
                delegate_email=email,
            )
            user_auth.authenticate()
            return user_auth.get_drive_service(user_email=email)
 
        except Exception as exc:
            logger.error(f"Auth delegation failed for {email}: {exc}")
            raise
    # =========================================================================
    # Helpers
    # =========================================================================

    def _split_items_from_records(self, records) -> Tuple[List[Dict], List[Dict]]:
        folders, files = [], []
        for r in records:
            item = {
                "id":               r.file_id,
                "file_id":          r.file_id,
                "name":             r.file_name,
                "mimeType":         r.mime_type,
                "size":             r.file_size,
                "parents":          [r.parent_id] if r.parent_id else [],
                "source_parent_id": r.parent_id,
            }
            if r.mime_type == "application/vnd.google-apps.folder":
                folders.append(item)
            else:
                files.append(item)
        return folders, files

    # =========================================================================
    # Report generation
    # =========================================================================

    def generate_report(self, summary: Dict, output_file: str):
        try:
            summary["gcs_routing_stats"] = {
                "gcs_routed":    self.stats["gcs_routed"],
                "memory_routed": self.stats["memory_routed"],
                "threshold_mb":  LARGE_FILE_THRESHOLD_BYTES // (1024 * 1024),
            }
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            logger.info(f"JSON report: {output_file}")
 
            txt = str(Path(output_file).with_suffix(".txt"))
            self._generate_text_report(summary, txt)
        except Exception as exc:
            logger.error(f"Failed to generate report: {exc}")
 
    def _generate_text_report(self, summary: Dict, output_file: str):
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write("=" * 80 + "\n")
                f.write("GOOGLE WORKSPACE DRIVE MIGRATION REPORT (v3 — 2-vCPU optimised)\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Date     : {summary.get('start_time','')}\n")
                f.write(f"Duration : {_fmt_duration(summary.get('duration_seconds',0))}\n")
                f.write(f"Accuracy : {summary.get('accuracy_rate',0):.2f}%\n\n")
 
                f.write("USERS\n" + "-"*40 + "\n")
                f.write(f"  Total     : {summary.get('total_users',0)}\n")
                f.write(f"  Completed : {summary.get('completed_users',0)}\n")
                f.write(f"  Failed    : {summary.get('failed_users',0)}\n\n")
 
                f.write("FILES\n" + "-"*40 + "\n")
                f.write(f"  Migrated : {summary.get('total_files_migrated',0)}\n")
                f.write(f"  Failed   : {summary.get('total_files_failed',0)}\n")
                f.write(f"  Skipped  : {summary.get('total_files_skipped',0)}\n")
                f.write(f"  Ignored  : {summary.get('total_files_ignored',0)}\n\n")
 
                gcs = summary.get("gcs_routing_stats", {})
                f.write("ROUTING\n" + "-"*40 + "\n")
                f.write(f"  Threshold : {gcs.get('threshold_mb',50)} MB\n")
                f.write(f"  Memory    : {gcs.get('memory_routed',0)}\n")
                f.write(f"  GCS       : {gcs.get('gcs_routed',0)}\n\n")
 
                f.write("=" * 80 + "\nEnd of Report\n" + "=" * 80 + "\n")
            logger.info(f"Text report: {output_file}")
        except Exception as exc:
            logger.error(f"Text report failed: {exc}")