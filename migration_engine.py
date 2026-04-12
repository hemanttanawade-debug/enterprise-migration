"""
migration_engine.py  (v3 — Global Queue + 1800s timeout + NoneType fixes)

KEY DIFFERENCES FROM v2:
  FIX-A  TIMEOUT: httplib2.Http(timeout=1800) injected into every Drive service.
         Previously connection_timeout=600 was stored but never used — httplib2
         defaulted to 60 s, which is exactly why GCS recording downloads timed
         out after ~60 s in the 2026-04-09 log.

  FIX-B  NoneType .close() CRASH: every BytesIO wrapped in try/finally.
         sql_state_manager.download_drive_to_gcs() can return (False, None, err);
         the old code crashed calling .close() on None.

  FIX-C  GLOBAL QUEUE: two-phase architecture.
         Phase 1 — all users discovered in parallel (fast metadata scan).
         Phase 2 — flat ThreadPoolExecutor(14) drains ALL pending files from
                   SQL regardless of owner. On the 2026-04-09 run, users 3 & 4
                   waited 75 min idle while users 1 & 2 finished sequentially.

  FIX-D  AUTH PATTERN PRESERVED: _build_drive_service() calls
         user_auth.get_drive_service(user_email=email) exactly as v2 did,
         then rebuilds with an explicit httplib2.Http(timeout=1800) transport.
         Falls back gracefully if get_credentials() is not available.

  FIX-E  CORRUPT DOWNLOAD GUARD: acknowledgeAbuse=True on get_media().
         Log line 1235 showed garbled binary returned for an abuse-flagged file.

  FIX-F  RESPONSE VALIDATION: _extract_id() safely unwraps API responses.
         "string indices must be integers" came from the API returning a plain
         string or list instead of a dict in some edge cases.

  FIX-G  BACKOFF CAP: 2^attempt capped at 32 s.

CONSTRUCTOR SIGNATURE: unchanged from v2.
    MigrationEngine(source_auth, dest_auth, config, checkpoint,
                    gcs_helper, run_id, get_conn)
"""

import io
import logging
import time
import json
import threading
import random
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import httplib2
from googleapiclient.discovery import build as _gapi_build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

logger = logging.getLogger(__name__)
mimetypes.init()

# ─────────────────────────────────────────────────────────────────────────────
# Tunable constants
# ─────────────────────────────────────────────────────────────────────────────

LARGE_FILE_THRESHOLD_BYTES = 50 * 1_024 * 1_024   # 50 MB → GCS path

GLOBAL_WORKERS    = 14   # I/O-bound → safe on 2-vCPU
DISCOVERY_WORKERS = 4    # one thread per user during metadata scan

CONNECTION_TIMEOUT  = 1_800               # FIX-A: 30 min
MAX_BACKOFF_SECONDS = 32                  # FIX-G: cap retry wait
CHUNK_SIZE          = 32 * 1_024 * 1_024  # 32 MB

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


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

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


def _backoff(attempt: int, fraction: float = 0.25) -> float:
    base  = min(2 ** attempt, MAX_BACKOFF_SECONDS)
    delta = base * fraction
    return base + random.uniform(-delta, delta)


def _extract_id(response) -> Optional[str]:
    """
    FIX-F: safely extract 'id' from a Drive API response.
    Handles dict, list-of-dict, or unexpected string/None without raising
    'string indices must be integers'.
    """
    if isinstance(response, list):
        response = response[0] if response else None
    if isinstance(response, dict):
        return response.get("id")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# MigrationEngine
# ─────────────────────────────────────────────────────────────────────────────

class MigrationEngine:
    """
    High-performance two-phase migration engine.

    Constructor signature is identical to v2 — no changes needed in main.py.
    """

    def __init__(
        self,
        source_auth,
        dest_auth,
        config,
        checkpoint,
        gcs_helper,
        run_id: str,
        get_conn,
    ):
        self.source_auth = source_auth
        self.dest_auth   = dest_auth
        self.config      = config
        self.sql_mgr     = checkpoint
        self.gcs         = gcs_helper
        self.run_id      = run_id
        self.get_conn    = get_conn

        self.max_retries        = 5
        self.connection_timeout = CONNECTION_TIMEOUT

        self._thread_local = threading.local()

        self._folder_maps:      Dict[str, Dict[str, str]] = {}
        self._folder_maps_lock: threading.Lock            = threading.Lock()

        self._processed:      Set[Tuple]     = set()
        self._processed_lock: threading.Lock = threading.Lock()

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

    # =========================================================================
    # Public: migrate_domain
    # =========================================================================

    def migrate_domain(
        self,
        user_mapping: Dict[str, str],
        max_workers: int = GLOBAL_WORKERS,
        discovery_workers: int = DISCOVERY_WORKERS,
        global_workers: int = GLOBAL_WORKERS,
    ) -> Dict:
        self.stats["start_time"] = datetime.now()
        self._user_mapping = user_mapping

        logger.info(
            f"[DOMAIN] Starting migration: {len(user_mapping)} users | "
            f"discovery={discovery_workers} | global={global_workers} | "
            f"timeout={self.connection_timeout}s | run_id={self.run_id}"
        )

        summary: Dict = {
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

        # Phase 1: parallel discovery + folder creation
        logger.info("[PHASE-1] Discovering all users in parallel...")
        user_stats: Dict[str, Dict] = {}

        n_disc = min(discovery_workers, len(user_mapping))
        with ThreadPoolExecutor(max_workers=n_disc) as pool:
            futures = {
                pool.submit(self._discover_and_prepare_user, src, dst): (src, dst)
                for src, dst in user_mapping.items()
            }
            for future in as_completed(futures):
                src, dst = futures[future]
                try:
                    result = future.result()
                    user_stats[src] = result
                    logger.info(
                        f"[PHASE-1] {src}: {result.get('files_total',0)} files, "
                        f"{result.get('folders_created',0)} folders"
                    )
                except Exception as exc:
                    logger.error(f"[PHASE-1] Discovery failed {src}: {exc}", exc_info=True)
                    user_stats[src] = {"status": "discovery_failed", "error": str(exc)}

        # Phase 2: drain global SQL queue
        logger.info(f"[PHASE-2] Global queue — {global_workers} workers...")
        pending = self.sql_mgr.get_all_pending_items(self.run_id)
        pending.sort(key=lambda r: int(getattr(r, "file_size_bytes", None) or 0))
        logger.info(f"[PHASE-2] {len(pending)} files pending")

        file_results: Dict[str, Dict] = {}
        file_results_lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=global_workers) as pool:
            futures = {pool.submit(self._process_queue_item, item): item for item in pending}
            done = 0
            for future in as_completed(futures):
                item = futures[future]
                try:
                    res = future.result()
                    with file_results_lock:
                        file_results[item.file_id] = res
                    done += 1
                    if done % 50 == 0:
                        logger.info(f"[PHASE-2] Progress: {done}/{len(pending)}")
                except Exception as exc:
                    logger.error(
                        f"[PHASE-2] Future error [{item.file_name}]: {exc}", exc_info=True
                    )
                    with file_results_lock:
                        file_results[item.file_id] = {
                            "success": False,
                            "error": str(exc),
                            "source_email": getattr(item, "source_email", ""),
                        }

        # Aggregate per-user
        per_user: Dict[str, Dict] = {
            src: {
                "source_email": src, "dest_email": dst,
                "files_migrated": 0, "files_failed": 0,
                "files_skipped": 0,  "files_ignored": 0,
                "collaborators_migrated": 0, "external_collaborators": 0,
                "errors": [],
            }
            for src, dst in user_mapping.items()
        }

        for fid, res in file_results.items():
            src_email = res.get("source_email", "")
            if src_email not in per_user:
                continue
            agg = per_user[src_email]
            if res.get("skipped"):
                agg["files_skipped"] += 1
            elif res.get("ignored"):
                agg["files_ignored"] += 1
            elif res.get("success"):
                agg["files_migrated"]         += 1
                agg["collaborators_migrated"] += res.get("collaborators_migrated", 0)
                agg["external_collaborators"] += res.get("external_collaborators", 0)
            else:
                agg["files_failed"] += 1
                agg["errors"].append({
                    "file": res.get("file_name", ""), "file_id": fid,
                    "error": res.get("error", ""),
                    "error_type": res.get("error_type", ""),
                    "user": src_email,
                })

        for src, agg in per_user.items():
            disc = user_stats.get(src, {})
            agg["files_total"]     = disc.get("files_total",     0)
            agg["folders_created"] = disc.get("folders_created", 0)
            agg["folders_failed"]  = disc.get("folders_failed",  0)

            attempted = (
                agg["files_total"] - agg["files_skipped"] - agg["files_ignored"]
            )
            agg["accuracy_rate"] = (
                agg["files_migrated"] / attempted * 100 if attempted > 0 else 100.0
            )

            if disc.get("status") == "discovery_failed":
                agg["status"] = "failed"
                summary["failed_users"] += 1
            else:
                agg["status"] = "completed" if agg["files_failed"] == 0 else "partial"
                summary["completed_users"] += 1

            summary["total_files_migrated"]         += agg["files_migrated"]
            summary["total_files_failed"]           += agg["files_failed"]
            summary["total_files_skipped"]          += agg["files_skipped"]
            summary["total_files_ignored"]          += agg["files_ignored"]
            summary["total_folders_created"]        += agg["folders_created"]
            summary["total_folders_failed"]         += agg["folders_failed"]
            summary["total_collaborators_migrated"] += agg["collaborators_migrated"]
            summary["total_external_collaborators"] += agg["external_collaborators"]
            summary["detailed_failures"].extend(agg["errors"])
            summary["user_results"].append(agg)

            self.sql_mgr.finish_user(
                self.run_id, src, agg["status"],
                files_done=agg["files_migrated"],
                files_failed=agg["files_failed"],
                bytes_moved=0,
            )

        total = summary["total_files_migrated"] + summary["total_files_failed"]
        if total > 0:
            summary["accuracy_rate"] = summary["total_files_migrated"] / total * 100

        self.stats["end_time"] = datetime.now()
        summary["end_time"]         = self.stats["end_time"].isoformat()
        summary["duration_seconds"] = (
            self.stats["end_time"] - self.stats["start_time"]
        ).total_seconds()

        logger.info(
            f"[DOMAIN] Complete: {summary['accuracy_rate']:.2f}% | "
            f"GCS={self.stats['gcs_routed']} MEM={self.stats['memory_routed']} | "
            f"duration={_fmt_duration(summary['duration_seconds'])}"
        )
        return summary

    # =========================================================================
    # Phase 1 worker
    # =========================================================================

    def _discover_and_prepare_user(self, source_email: str, dest_email: str) -> Dict:
        self.sql_mgr.start_user(self.run_id, source_email)
        result = {
            "source_email": source_email, "dest_email": dest_email,
            "files_total": 0, "folders_created": 0, "folders_failed": 0,
            "status": "ok",
        }
        try:
            source_drive = self._get_drive_service_for_thread(self.source_auth, source_email)
            dest_drive   = self._get_drive_service_for_thread(self.dest_auth,   dest_email)

            existing = self.sql_mgr.load_user_items(source_email)

            if existing:
                logger.info(f"[DISC] {source_email}: resume — {len(existing)} SQL items")
                all_folders, all_files = self._split_items_from_records(existing)
                folder_mapping = self.sql_mgr.get_folder_mapping(self.run_id, source_email)

                missing = [
                    f for f in all_folders
                    if (f.get("id") or f.get("file_id")) not in folder_mapping
                ]
                if missing:
                    new_fm = self._build_folder_structure(
                        missing, source_drive, dest_drive, source_email, dest_email
                    )
                    folder_mapping.update(new_fm)
            else:
                logger.info(f"[DISC] {source_email}: first run — crawling Drive...")
                source_files = self._get_all_user_owned_files(source_drive, source_email)

                if not source_files:
                    return result

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
                    all_folders, source_drive, dest_drive, source_email, dest_email
                )

            with self._folder_maps_lock:
                self._folder_maps[source_email] = folder_mapping

            result["files_total"]     = len(all_files)
            result["folders_created"] = len(folder_mapping)
            result["folders_failed"]  = max(0, len(all_folders) - len(folder_mapping))

        except Exception as exc:
            logger.error(f"[DISC] {source_email} failed: {exc}", exc_info=True)
            result["status"] = "discovery_failed"
            result["error"]  = str(exc)

        return result

    # =========================================================================
    # Phase 2 worker
    # =========================================================================

    def _process_queue_item(self, item) -> Dict:
        file_id   = item.file_id
        file_name = getattr(item, "file_name",        "") or ""
        mime_type = getattr(item, "mime_type",         "") or ""
        file_size = int(getattr(item, "file_size_bytes", 0) or 0)
        src_email = getattr(item, "source_email",      "")
        dst_email = getattr(item, "dest_email",        "")
        parent_id = getattr(item, "source_parent_id",  None)

        base = {
            "success": False, "ignored": False, "skipped": False,
            "source_email": src_email, "file_name": file_name,
            "collaborators_migrated": 0, "external_collaborators": 0,
        }

        if mime_type in IGNORED_MIME_TYPES:
            self.sql_mgr.mark_ignored(self.run_id, file_id, "Non-migratable MIME type")
            return {**base, "ignored": True}

        should_skip, _ = self.sql_mgr.should_skip_item(file_id)
        if should_skip:
            return {**base, "skipped": True}

        sig = (file_id, file_name, file_size)
        with self._processed_lock:
            if sig in self._processed:
                return {**base, "skipped": True}

        try:
            source_drive = self._get_drive_service_for_thread(self.source_auth, src_email)
            dest_drive   = self._get_drive_service_for_thread(self.dest_auth,   dst_email)
        except Exception as exc:
            err = f"Auth error: {exc}"
            self.sql_mgr.mark_failed(self.run_id, file_id, err)
            return {**base, "error": err, "error_type": "auth_error"}

        with self._folder_maps_lock:
            fm = self._folder_maps.get(src_email, {})
        dest_parent = fm.get(parent_id) if parent_id else None

        self.sql_mgr.mark_in_progress(self.run_id, file_id)

        res = self._migrate_file_v3(
            file_id, file_name, mime_type, file_size, dest_parent,
            source_drive, dest_drive,
        )

        if res["success"]:
            dest_id = res.get("dest_id")
            self.sql_mgr.mark_done(
                self.run_id, file_id,
                dest_item_id=dest_id, dest_parent_id=dest_parent,
            )
            with self._processed_lock:
                self._processed.add(sig)

            perm_r = {"migrated": 0, "external": 0}
            if dest_id:
                perm_r = self._migrate_permissions_hybrid(
                    file_id, dest_id, file_name, source_drive, dest_drive
                )
            return {
                **base, "success": True, "dest_id": dest_id,
                "collaborators_migrated": perm_r.get("migrated", 0),
                "external_collaborators": perm_r.get("external", 0),
            }
        elif res.get("ignored"):
            self.sql_mgr.mark_ignored(self.run_id, file_id, res.get("error", ""))
            return {**base, "ignored": True}
        else:
            err = res.get("error", "Unknown")
            self.sql_mgr.mark_failed(self.run_id, file_id, err)
            return {**base, "error": err, "error_type": res.get("error_type", "")}

    # =========================================================================
    # Core file routing
    # =========================================================================

    def _migrate_file_v3(
        self, file_id, file_name, mime_type, file_size,
        dest_parent_id, source_drive, dest_drive,
    ) -> Dict:
        empty = {"success": False, "dest_id": None, "ignored": False, "error": None}

        if mime_type in IGNORED_MIME_TYPES:
            return {**empty, "ignored": True, "error": "Non-migratable MIME type"}

        if mime_type in GOOGLE_WORKSPACE_TYPES:
            return self._migrate_workspace_file(
                file_id, file_name, mime_type, dest_parent_id, source_drive, dest_drive
            )

        if file_size >= LARGE_FILE_THRESHOLD_BYTES:
            return self._migrate_via_gcs(
                file_id, file_name, mime_type, file_size,
                dest_parent_id, source_drive, dest_drive,
            )
        return self._migrate_via_memory(
            file_id, file_name, mime_type, file_size,
            dest_parent_id, source_drive, dest_drive,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Memory path  (<50 MB)
    # ─────────────────────────────────────────────────────────────────────────

    def _migrate_via_memory(
        self, file_id, file_name, mime_type, file_size,
        dest_parent_id, source_drive, dest_drive,
    ) -> Dict:
        empty      = {"success": False, "dest_id": None, "ignored": False, "error": None}
        last_error = ""

        for attempt in range(self.max_retries):
            wait   = _backoff(attempt)
            dl_buf = None
            try:
                request = source_drive.files().get_media(
                    fileId=file_id, supportsAllDrives=True, acknowledgeAbuse=True
                )
                dl_buf = io.BytesIO()
                try:
                    dl   = MediaIoBaseDownload(dl_buf, request, chunksize=CHUNK_SIZE)
                    done = False
                    while not done:
                        _, done = dl.next_chunk()
                    dl_buf.seek(0)
                    data = dl_buf.read()
                finally:
                    dl_buf.close()
                    dl_buf = None

                if not data:
                    if file_size == 0:
                        meta = {"name": file_name}
                        if dest_parent_id:
                            meta["parents"] = [dest_parent_id]
                        resp    = dest_drive.files().create(
                            body=meta, fields="id", supportsAllDrives=True
                        ).execute()
                        dest_id = _extract_id(resp)
                        self.stats["memory_routed"] += 1
                        return {**empty, "success": True, "dest_id": dest_id}
                    last_error = "Empty download for non-zero file"
                    if attempt < self.max_retries - 1:
                        time.sleep(wait)
                        continue
                    return {**empty, "error": last_error, "error_type": "empty_download"}

                meta = {"name": file_name}
                if dest_parent_id:
                    meta["parents"] = [dest_parent_id]

                upload_buf = io.BytesIO(data)
                try:
                    use_resumable = len(data) >= 5 * 1_024 * 1_024
                    media = MediaIoBaseUpload(
                        upload_buf, mimetype=mime_type,
                        resumable=use_resumable,
                        chunksize=CHUNK_SIZE if use_resumable else -1,
                    )
                    resp = dest_drive.files().create(
                        body=meta, media_body=media,
                        fields="id", supportsAllDrives=True,
                    ).execute()
                finally:
                    upload_buf.close()

                dest_id = _extract_id(resp)
                if dest_id is None:
                    return {**empty, "error": f"Bad response: {resp!r}", "error_type": "bad_response"}

                self.stats["memory_routed"] += 1
                return {**empty, "success": True, "dest_id": dest_id}

            except HttpError as exc:
                code       = exc.resp.status
                last_error = str(exc)

                if code == 200:
                    try:
                        body    = json.loads(exc.content.decode("utf-8"))
                        dest_id = _extract_id(body)
                        self.stats["memory_routed"] += 1
                        return {**empty, "success": True, "dest_id": dest_id}
                    except Exception:
                        pass
                    self.stats["memory_routed"] += 1
                    return {**empty, "success": True, "dest_id": None}

                if code == 403 and any(
                    k in last_error for k in (
                        "cannotDownload", "fileNotDownloadable", "cannotDownloadAbusiveFile"
                    )
                ):
                    return {**empty, "ignored": True, "error": "Download restricted"}

                if code in (429, 500, 503) and attempt < self.max_retries - 1:
                    logger.warning(
                        f"  [MEM] HTTP {code} retry {attempt+1} [{file_name}] wait {wait:.1f}s"
                    )
                    time.sleep(wait)
                    continue

                logger.error(f"  [MEM] HTTP {code} [{file_name}]: {last_error}")
                return {**empty, "error": last_error, "error_type": f"http_{code}"}

            except (ConnectionResetError, ConnectionError, OSError, TimeoutError) as exc:
                last_error = str(exc)
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"  [MEM] Network retry {attempt+1}/{self.max_retries} "
                        f"[{file_name}]: {last_error}"
                    )
                    time.sleep(wait)
                else:
                    logger.error(f"  [MEM] Exhausted [{file_name}]: {last_error}")

            except Exception as exc:
                last_error = str(exc)
                logger.error(f"  [MEM] Unexpected [{file_name}]: {last_error}", exc_info=True)
                return {**empty, "error": last_error, "error_type": "unexpected"}

            finally:
                if dl_buf is not None:
                    try:
                        dl_buf.close()
                    except Exception:
                        pass

        return {**empty, "error": last_error, "error_type": "memory_transfer_failed"}

    # ─────────────────────────────────────────────────────────────────────────
    # GCS path  (>=50 MB)
    # ─────────────────────────────────────────────────────────────────────────

    def _migrate_via_gcs(
        self, file_id, file_name, mime_type, file_size,
        dest_parent_id, source_drive, dest_drive,
    ) -> Dict:
        empty       = {"success": False, "dest_id": None, "ignored": False, "error": None}
        last_error  = ""
        active_blob = None

        for attempt in range(self.max_retries):
            wait         = _backoff(attempt)
            attempt_blob = f"{self.run_id}/{file_id}/attempt_{attempt}"

            try:
                ok, blob_name, err = self.gcs.download_drive_to_gcs(
                    drive_svc=source_drive, file_id=file_id, file_name=file_name,
                    run_id=attempt_blob, mime_type=mime_type,
                )

                if not ok:
                    last_error = err or "GCS download failed"
                    if blob_name is not None:   # FIX-B
                        try:
                            self.gcs.delete_temp(blob_name)
                        except Exception:
                            pass
                    if attempt < self.max_retries - 1:
                        logger.warning(
                            f"  [GCS] Download failed {attempt+1}/{self.max_retries} "
                            f"[{file_name}]: {last_error} — retry {wait:.1f}s"
                        )
                        time.sleep(wait)
                        continue
                    logger.error(f"  [GCS] Download exhausted [{file_name}]: {last_error}")
                    return {**empty, "error": last_error, "error_type": "gcs_download_failed"}

                active_blob = blob_name

                ok2, dest_id, err2 = self.gcs.upload_gcs_to_drive(
                    drive_svc=dest_drive, blob_name=blob_name, file_name=file_name,
                    mime_type=mime_type, parent_id=dest_parent_id,
                )

                if not ok2:
                    last_error = err2 or "GCS upload failed"
                    if active_blob is not None:
                        try:
                            self.gcs.delete_temp(active_blob)
                        except Exception:
                            pass
                        active_blob = None
                    if attempt < self.max_retries - 1:
                        logger.warning(
                            f"  [GCS] Upload failed {attempt+1}/{self.max_retries} "
                            f"[{file_name}]: {last_error} — retry {wait:.1f}s"
                        )
                        time.sleep(wait)
                        continue
                    logger.error(f"  [GCS] Upload exhausted [{file_name}]: {last_error}")
                    return {**empty, "error": last_error, "error_type": "gcs_upload_failed"}

                if active_blob is not None:
                    try:
                        self.gcs.delete_temp(active_blob)
                    except Exception as de:
                        logger.warning(f"  [GCS] Blob delete non-fatal: {de}")
                    active_blob = None

                self.stats["gcs_routed"] += 1
                logger.info(
                    f"  [GCS] {file_name} ({_fmt_bytes(file_size)}) dest_id={dest_id}"
                )
                return {**empty, "success": True, "dest_id": dest_id}

            except (ConnectionResetError, ConnectionError, OSError, TimeoutError) as exc:
                last_error = str(exc)
                if active_blob is not None:
                    try:
                        self.gcs.delete_temp(active_blob)
                    except Exception:
                        pass
                    active_blob = None
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"  [GCS] Connection error {attempt+1}/{self.max_retries} "
                        f"[{file_name}]: {last_error}"
                    )
                    time.sleep(wait)
                else:
                    logger.error(f"  [GCS] Exhausted [{file_name}]: {last_error}")

            except Exception as exc:
                last_error = str(exc)
                logger.error(f"  [GCS] Unexpected [{file_name}]: {last_error}", exc_info=True)
                if active_blob is not None:
                    try:
                        self.gcs.delete_temp(active_blob)
                    except Exception:
                        pass
                break

        if active_blob is not None:
            try:
                self.gcs.delete_temp(active_blob)
            except Exception:
                pass

        return {**empty, "error": last_error, "error_type": "gcs_transfer_failed"}

    # ─────────────────────────────────────────────────────────────────────────
    # Google Workspace export path
    # ─────────────────────────────────────────────────────────────────────────

    def _migrate_workspace_file(
        self, file_id, file_name, mime_type, dest_parent_id, source_drive, dest_drive,
    ) -> Dict:
        empty = {"success": False, "dest_id": None, "ignored": False, "error": None}

        type_info = GOOGLE_WORKSPACE_TYPES.get(mime_type)
        if not type_info or not type_info.get("can_export"):
            return {**empty, "ignored": True, "error": f"Non-exportable: {mime_type}"}

        for attempt in range(self.max_retries):
            wait   = _backoff(attempt)
            dl_buf = None
            try:
                request = source_drive.files().export_media(
                    fileId=file_id, mimeType=type_info["export_mime"]
                )
                dl_buf = io.BytesIO()
                try:
                    dl   = MediaIoBaseDownload(dl_buf, request, chunksize=CHUNK_SIZE)
                    done = False
                    while not done:
                        _, done = dl.next_chunk()
                    dl_buf.seek(0)
                    data = dl_buf.read()
                finally:
                    dl_buf.close()
                    dl_buf = None

                if not data:
                    return {**empty, "error": "Empty export", "error_type": "empty_export"}

                meta = {"name": file_name}
                if dest_parent_id:
                    meta["parents"] = [dest_parent_id]
                if type_info.get("import_mime"):
                    meta["mimeType"] = type_info["import_mime"]

                upload_buf = io.BytesIO(data)
                try:
                    use_resumable = len(data) >= 5 * 1_024 * 1_024
                    media = MediaIoBaseUpload(
                        upload_buf, mimetype=type_info["export_mime"],
                        resumable=use_resumable,
                        chunksize=CHUNK_SIZE if use_resumable else -1,
                    )
                    resp = dest_drive.files().create(
                        body=meta, media_body=media,
                        fields="id", supportsAllDrives=True,
                    ).execute()
                finally:
                    upload_buf.close()

                dest_id = _extract_id(resp)
                if dest_id is None:
                    return {**empty, "error": f"Bad response: {resp!r}", "error_type": "bad_response"}

                self.stats["memory_routed"] += 1
                return {**empty, "success": True, "dest_id": dest_id}

            except HttpError as exc:
                err  = str(exc)
                code = exc.resp.status

                if code == 200:
                    try:
                        body    = json.loads(exc.content.decode("utf-8"))
                        dest_id = _extract_id(body)
                        self.stats["memory_routed"] += 1
                        return {**empty, "success": True, "dest_id": dest_id}
                    except Exception:
                        pass
                    self.stats["memory_routed"] += 1
                    return {**empty, "success": True, "dest_id": None}

                if "exportSizeLimitExceeded" in err and "fallback_mime" in type_info:
                    return self._migrate_workspace_fallback(
                        file_id, file_name, type_info, dest_parent_id, source_drive, dest_drive
                    )

                if code in (429, 500, 503) and attempt < self.max_retries - 1:
                    logger.warning(f"  [WS] HTTP {code} retry {attempt+1} [{file_name}]")
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

            finally:
                if dl_buf is not None:
                    try:
                        dl_buf.close()
                    except Exception:
                        pass
                    dl_buf = None

        return {**empty, "error": "Max retries exceeded", "error_type": "workspace_export_failed"}

    def _migrate_workspace_fallback(
        self, file_id, file_name, type_info, dest_parent_id, source_drive, dest_drive,
    ) -> Dict:
        empty  = {"success": False, "dest_id": None, "ignored": False, "error": None}
        dl_buf = None
        try:
            request = source_drive.files().export_media(
                fileId=file_id, mimeType=type_info["fallback_mime"]
            )
            dl_buf = io.BytesIO()
            try:
                dl   = MediaIoBaseDownload(dl_buf, request, chunksize=CHUNK_SIZE)
                done = False
                while not done:
                    _, done = dl.next_chunk()
                dl_buf.seek(0)
                data = dl_buf.read()
            finally:
                dl_buf.close()
                dl_buf = None

            if not data:
                return {**empty, "error": "Empty fallback export", "error_type": "empty_export"}

            fallback_name = file_name + type_info["fallback_ext"]
            meta = {"name": fallback_name}
            if dest_parent_id:
                meta["parents"] = [dest_parent_id]

            upload_buf = io.BytesIO(data)
            try:
                use_resumable = len(data) >= 5 * 1_024 * 1_024
                media = MediaIoBaseUpload(
                    upload_buf, mimetype=type_info["fallback_mime"],
                    resumable=use_resumable,
                    chunksize=CHUNK_SIZE if use_resumable else -1,
                )
                resp = dest_drive.files().create(
                    body=meta, media_body=media,
                    fields="id", supportsAllDrives=True,
                ).execute()
            finally:
                upload_buf.close()

            dest_id = _extract_id(resp)
            if dest_id is None:
                return {**empty, "error": f"Bad response: {resp!r}", "error_type": "bad_response"}

            logger.info(f"  [WS-FALLBACK] {fallback_name}")
            return {**empty, "success": True, "dest_id": dest_id}

        except Exception as exc:
            return {**empty, "error": str(exc), "error_type": "workspace_fallback_failed"}
        finally:
            if dl_buf is not None:
                try:
                    dl_buf.close()
                except Exception:
                    pass

    # =========================================================================
    # Permissions
    # =========================================================================

    def _migrate_permissions_hybrid(
        self, source_id, dest_id, name, source_drive, dest_drive,
    ) -> Dict:
        result = {"migrated": 0, "failed": 0, "external": 0, "skipped": 0}
        try:
            resp  = source_drive.permissions().list(
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
                self.config.SOURCE_DOMAIN, self.config.DEST_DOMAIN,
            )
            pr = pm.migrate_permissions(source_id, dest_id, perms)

            result["migrated"] = pr.get("migrated",      0)
            result["failed"]   = pr.get("failed",        0)
            result["external"] = pr.get("external_users",0)
            result["skipped"]  = pr.get("skipped",       0)

            valid_roles           = {"owner","organizer","fileOrganizer","writer","commenter","reader"}
            valid_classifications = {
                "internal_both_domains","internal_source_only",
                "external_domain","general_access",
            }

            for detail in pr.get("details", []):
                role           = detail.get("role",           "")
                ptype          = detail.get("type",           "user")
                status         = detail.get("status",         "failed")
                classification = detail.get("classification", "external_domain")
                error          = detail.get("error",          "")
                src_email      = detail.get("original_email", "")
                dst_email      = detail.get("target_email",   "")

                if role == "owner" or status == "skipped":
                    continue
                if role not in valid_roles:
                    continue
                if classification not in valid_classifications:
                    classification = "external_domain"

                try:
                    self.sql_mgr.upsert_permission(
                        file_id=dest_id, item_type="FILE",
                        permission_type=(
                            ptype if ptype in ("user","group","domain","anyone") else "user"
                        ),
                        source_email=src_email, dest_email=dst_email,
                        role=role, classification=classification,
                        is_inherited=False, parent_drive_id=None,
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
        self, folders, source_drive, dest_drive, source_email, dest_email,
    ) -> Dict[str, str]:
        if not folders:
            return {}

        logger.info(f"  Building {len(folders)} folders for {source_email}")
        id_set         = {f.get("id") or f.get("file_id") or f.get("source_item_id") for f in folders}
        visited: Set[str] = set()
        sorted_folders: List[Dict] = []

        def visit(folder):
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
            fname = (
                folder.get("name") or folder.get("file_name")
                or folder.get("source_item_name", "")
            )
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
                    dest_item_id=dest_fid, dest_parent_id=dest_parent,
                )
                self.stats["folders_created"] += 1
            else:
                self.sql_mgr.mark_failed(self.run_id, fid, "Failed to create folder")
                self.stats["folders_failed"] += 1
                logger.error(f"    Folder failed: {fname}")

        return folder_mapping

    def _create_folder(
        self, folder_name, parent_id, dest_drive, max_retries=3,
    ) -> Optional[str]:
        for attempt in range(max_retries):
            try:
                meta = {"name": folder_name, "mimeType": "application/vnd.google-apps.folder"}
                if parent_id:
                    meta["parents"] = [parent_id]
                resp = dest_drive.files().create(
                    body=meta, fields="id,name", supportsAllDrives=True
                ).execute()
                fid = _extract_id(resp)
                if fid is None:
                    raise ValueError(f"Bad folder create response: {resp!r}")
                return fid
            except HttpError as exc:
                if exc.resp.status == 409:
                    existing = self._find_existing_folder(folder_name, parent_id, dest_drive)
                    if existing:
                        return existing
                if attempt < max_retries - 1:
                    time.sleep(_backoff(attempt))
                else:
                    logger.error(f"Failed to create folder '{folder_name}': {exc}")
                    return None
            except Exception as exc:
                if attempt < max_retries - 1:
                    time.sleep(_backoff(attempt))
                else:
                    logger.error(f"Error creating folder '{folder_name}': {exc}")
                    return None
        return None

    def _find_existing_folder(self, name, parent_id, dest_drive) -> Optional[str]:
        try:
            q = (
                f"name='{name}' and "
                "mimeType='application/vnd.google-apps.folder' and trashed=false"
            )
            if parent_id:
                q += f" and '{parent_id}' in parents"
            resp  = dest_drive.files().list(
                q=q, fields="files(id)", pageSize=5, supportsAllDrives=True
            ).execute()
            files = resp.get("files", [])
            return files[0]["id"] if files else None
        except Exception:
            return None

    # =========================================================================
    # File discovery
    # =========================================================================

    def _get_all_user_owned_files(self, drive_service, user_email) -> List[Dict]:
        files       = []
        page_token  = None
        retries     = 0
        max_retries = 5

        while True:
            try:
                resp = drive_service.files().list(
                    q="trashed=false", spaces="drive",
                    fields=(
                        "nextPageToken, files("
                        "id, name, mimeType, size, parents, "
                        "createdTime, modifiedTime, owners)"
                    ),
                    pageSize=100, pageToken=page_token,
                    supportsAllDrives=False, includeItemsFromAllDrives=False,
                ).execute()

                batch = resp.get("files", [])
                owned = [
                    f for f in batch
                    if any(o.get("emailAddress") == user_email for o in f.get("owners", []))
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
                    time.sleep(_backoff(retries))
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
                time.sleep(_backoff(retries))

        logger.info(f"  Discovered {len(files)} owned files for {user_email}")
        return files

    # =========================================================================
    # Auth / Drive service factory
    # =========================================================================

    def _get_drive_service_for_thread(self, auth_obj, email: str):
        """Thread-local cache: each thread x email gets its own Drive service."""
        if not hasattr(self._thread_local, "drive_cache"):
            self._thread_local.drive_cache = {}
        if email not in self._thread_local.drive_cache:
            self._thread_local.drive_cache[email] = self._build_drive_service(email)
        return self._thread_local.drive_cache[email]

    def _build_drive_service(self, email: str):
        """
        FIX-A: injects httplib2.Http(timeout=1800) into every Drive service.

        Strategy:
          1. Build delegated credentials via GoogleAuthManager (same as v2).
          2. Try get_credentials() to get a raw credentials object, then
             rebuild with a fresh Http(timeout=...) for the 1800 s timeout.
          3. If get_credentials() doesn't exist, fall back to get_drive_service()
             exactly as v2 did (timeout not injected but code doesn't crash).
        """
        try:
            from auth import GoogleAuthManager

            creds_file = (
                self.config.SOURCE_CREDENTIALS_FILE
                if email.endswith(f"@{self.config.SOURCE_DOMAIN}")
                else self.config.DEST_CREDENTIALS_FILE
            )

            user_auth = GoogleAuthManager(
                creds_file, self.config.SCOPES, delegate_email=email,
            )
            user_auth.authenticate()

            # Try to inject timeout via raw credentials
            try:
                creds           = user_auth.get_credentials()
                http            = httplib2.Http(timeout=self.connection_timeout)
                authorized_http = creds.authorize(http)
                return _gapi_build("drive", "v3", http=authorized_http)
            except AttributeError:
                # get_credentials() not on this auth class — use v2 fallback
                logger.warning(
                    f"get_credentials() not found on GoogleAuthManager for {email}; "
                    "using get_drive_service() fallback (1800 s timeout NOT applied). "
                    "Add get_credentials() to auth.py to enable the timeout fix."
                )
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
            # Support both v2 field names and v3 field names
            parent = getattr(r, "source_parent_id", None) or getattr(r, "parent_id", None)
            size   = getattr(r, "file_size_bytes",  None) or getattr(r, "file_size",  None)
            item   = {
                "id":               r.file_id,
                "file_id":          r.file_id,
                "name":             r.file_name,
                "mimeType":         r.mime_type,
                "size":             size,
                "parents":          [parent] if parent else [],
                "source_parent_id": parent,
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
                "threshold_mb":  LARGE_FILE_THRESHOLD_BYTES // (1_024 * 1_024),
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
                f.write("GOOGLE WORKSPACE DRIVE MIGRATION REPORT (v3 — Global Queue)\n")
                f.write("=" * 80 + "\n\n")
                f.write(f"Date       : {summary.get('start_time','')}\n")
                f.write(f"Duration   : {_fmt_duration(summary.get('duration_seconds', 0))}\n")
                f.write(f"Accuracy   : {summary.get('accuracy_rate', 0):.2f}%\n\n")
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