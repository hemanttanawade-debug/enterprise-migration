"""
migration_engine_v4.py  —  ~/amey/migration_engine_v4.py

SQL-first migration engine. Reads all pending items from SQL
(pre-populated by discovery_engine.py) and transfers them to destination Drive.

KEY WIRING vs previous version:
  - SQLStateManager constructed with db_config DICT (not a callable)
  - GCS key resolved to absolute path from Config
  - Credential files resolved: Flask upload path first, then amey/ fallback
  - SQLStateManager IS the gcs_helper (it has download_drive_to_gcs etc.)
  - sql_mgr.migration_id set to run_id so SQL queries hit the right partition

PREREQUISITE:
    discovery_engine.run_discovery(run_id, user_mapping) must have been called
    first — this engine does NOT crawl Drive.

FIXES APPLIED
─────────────
FIX-1  IGNORED_MIME_TYPES: added application/vnd.google-apps.vid (legacy Google
       Video). The Drive API rejects get_media() for this type with "Use Export"
       — but it has no supported export format either, so it must be ignored.

FIX-2  GOOGLE_WORKSPACE_TYPES presentation: added fallback_mime/fallback_ext
       (PDF). When exportSizeLimitExceeded hits a large Slides file the engine
       now retries as a PDF instead of hard-failing.

FIX-3  GOOGLE_WORKSPACE_TYPES: added application/vnd.google-apps.shortcut with
       can_export=False so shortcuts are ignored cleanly (no 403 attempt).

FIX-4  _migrate_via_gcs(): added a per-file hard timeout (GCS_FILE_TIMEOUT,
       default 3600 s). Each GCS download+upload now runs inside a
       ThreadPoolExecutor(1) with future.result(timeout=...). If a 1 GB+ video
       stalls, the thread times out, the attempt is retried (or marked FAILED
       after max_retries), and the outer Phase-2 pool is never blocked
       indefinitely. Without this fix the gunicorn worker was killed mid-
       transfer, leaving all in-flight files stuck as IN_PROGRESS in SQL.

FEAT-5  Two-tier Phase-2 queue (XL-first strategy):
        Files > XLARGE_FILE_THRESHOLD_BYTES (600 MB) are submitted to a
        dedicated XL pool capped at XLARGE_WORKERS (4 threads) so they never
        starve the regular queue.  Once ALL XL files finish, a second pass runs
        all remaining files with the full GLOBAL_WORKERS (14) pool.  This
        ensures large transfers start immediately and the full bandwidth is
        reclaimed as soon as they complete.

FEAT-6  RAM-adaptive chunk size:
        _get_adaptive_chunk_size() reads available system RAM via psutil and
        returns a CHUNK_SIZE that scales between 8 MB (low RAM / < 512 MB
        available) and 256 MB (high RAM / >= 8 GB available).  Every download
        and upload in _migrate_via_memory() and _migrate_via_gcs() uses this
        value so transfers automatically saturate bandwidth on well-resourced
        machines while staying safe on constrained ones.
"""

import io
import logging
import time
import json
import threading
import random
import mimetypes
import concurrent.futures as _cf
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import psutil as _psutil
    _PSUTIL_AVAILABLE = True
except ImportError:
    _psutil = None
    _PSUTIL_AVAILABLE = False
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple

import httplib2
from googleapiclient.discovery import build as _gapi_build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

logger = logging.getLogger(__name__)
mimetypes.init()

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

LARGE_FILE_THRESHOLD_BYTES  = 5  * 1_024 * 1_024   # 40 MB  — GCS vs memory routing
XLARGE_FILE_THRESHOLD_BYTES = 600 * 1_024 * 1_024   # 600 MB — dedicated XL worker pool
MAX_FILE_SIZE_BYTES         = 5   * 1_024 * 1_024 * 1_024  # 5 GB  — hard ignore limit

GLOBAL_WORKERS   = 14   # Phase-2 full pool (used after XL pass)
XLARGE_WORKERS   = 14    # Dedicated workers reserved for >600 MB files
FOLDER_WORKERS   = 4
CONNECTION_TIMEOUT      = 1_800
MAX_BACKOFF_SECONDS     = 32
CHUNK_SIZE              = 32 * 1_024 * 1_024  # default; overridden by RAM probe at runtime

# FIX-4: Hard per-file timeout for GCS transfers (download + upload combined).
# 1 GB video over a typical GCP connection takes ~5–10 min; 3600 s is generous.
GCS_FILE_TIMEOUT = 3_600  # seconds


# ─────────────────────────────────────────────────────────────────────────────
# FEAT-6: RAM-adaptive chunk size
# ─────────────────────────────────────────────────────────────────────────────

def _get_adaptive_chunk_size() -> int:
    """
    Return a download/upload chunk size scaled to available system RAM.

    Available RAM  →  Chunk size
    ──────────────────────────────
    <  512 MB      →   8 MB   (safe floor for constrained environments)
    <  1   GB      →  16 MB
    <  2   GB      →  32 MB   (matches previous static default)
    <  4   GB      →  64 MB
    <  8   GB      → 128 MB
    >= 8   GB      → 256 MB   (saturates most GCP connections)

    Falls back to the static CHUNK_SIZE constant when psutil is unavailable.
    """
    if not _PSUTIL_AVAILABLE:
        return CHUNK_SIZE
    try:
        avail_bytes = _psutil.virtual_memory().available
        avail_mb    = avail_bytes / (1024 * 1024)
        if   avail_mb <  512: chunk_mb =  16
        elif avail_mb < 1024: chunk_mb =  16
        elif avail_mb < 2048: chunk_mb =  16
        elif avail_mb < 4096: chunk_mb =  16
        elif avail_mb < 8192: chunk_mb =  16
        else:                 chunk_mb =  16
        chunk = chunk_mb * 1024 * 1024
        logger.debug(
            f"[RAM-ADAPT] available={avail_mb:.0f} MB → chunk_size={chunk_mb} MB"
        )
        return chunk
    except Exception:
        return CHUNK_SIZE

# ─────────────────────────────────────────────────────────────────────────────
# FIX-1: Added application/vnd.google-apps.vid (legacy Google Video).
# The Drive API rejects get_media() for this type with:
#   "Only files with binary content can be downloaded. Use Export"
# but it also has no supported Export MIME type, so it must be ignored.
# ─────────────────────────────────────────────────────────────────────────────
IGNORED_MIME_TYPES = frozenset({
    "application/vnd.google-apps.script",
    "application/vnd.google-apps.form",
    "application/vnd.google-apps.site",
    "application/octet-stream",
    "application/vnd.google-apps.vid",       # FIX-1: legacy Google Video — no export API
})

# ─────────────────────────────────────────────────────────────────────────────
# FIX-2: Presentation now has fallback_mime/fallback_ext (PDF).
#         When exportSizeLimitExceeded is raised for a large Slides file the
#         engine retries as PDF via _workspace_fallback() instead of failing.
# FIX-3: Added application/vnd.google-apps.shortcut with can_export=False.
#         Shortcuts have no content — marking them non-exportable makes
#         _migrate_workspace_file() return ignored immediately instead of
#         attempting an export that always 403s.
# ─────────────────────────────────────────────────────────────────────────────
GOOGLE_WORKSPACE_TYPES = {
    "application/vnd.google-apps.document": {
        "export_mime": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "extension":   ".docx", "import_mime": "application/vnd.google-apps.document",
        "can_export":  True,
    },
    "application/vnd.google-apps.spreadsheet": {
        "export_mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "extension":   ".xlsx", "import_mime": "application/vnd.google-apps.spreadsheet",
        "can_export":  True,
    },
    # FIX-2: added fallback_mime + fallback_ext for oversized presentations
    "application/vnd.google-apps.presentation": {
        "export_mime":   "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "extension":     ".pptx", "import_mime": "application/vnd.google-apps.presentation",
        "can_export":    True,
        "fallback_mime": "application/pdf",   # used when exportSizeLimitExceeded
        "fallback_ext":  ".pdf",
    },
    "application/vnd.google-apps.drawing": {
        "export_mime": "image/svg+xml", "extension": ".svg", "import_mime": None,
        "can_export": True, "fallback_mime": "application/pdf", "fallback_ext": ".pdf",
    },
    "application/vnd.google-apps.map": {
        "export_mime": "application/vnd.google-earth.kmz", "extension": ".kmz",
        "import_mime": None, "can_export": True,
    },
    "application/vnd.google-apps.jam": {
        "export_mime": "application/pdf", "extension": ".pdf",
        "import_mime": None, "can_export": True,
    },
    "application/vnd.google-apps.folder": {
        "export_mime": None, "extension": None, "import_mime": None, "can_export": False,
    },
    # FIX-3: shortcuts point to other files — no exportable content
    "application/vnd.google-apps.shortcut": {
        "export_mime": None, "extension": None, "import_mime": None, "can_export": False,
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_bytes(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024: return f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f} PB"

def _fmt_duration(seconds: float) -> str:
    if seconds < 60:   return f"{seconds:.0f}s"
    if seconds < 3600: return f"{seconds / 60:.1f}m"
    return f"{seconds / 3600:.2f}h"

def _backoff(attempt: int) -> float:
    base = min(2 ** attempt, MAX_BACKOFF_SECONDS)
    return base + random.uniform(-base * 0.25, base * 0.25)

def _extract_id(response) -> Optional[str]:
    if isinstance(response, list): response = response[0] if response else None
    if isinstance(response, dict): return response.get("id")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Credential path resolution
# Flask saves creds to ~/flask-backend/uploads/credential/
# Config has relative paths like 'source_credentials.json'
# This resolves whichever exists, preferring Flask upload path.
# ─────────────────────────────────────────────────────────────────────────────

_FLASK_CRED_DIR = Path.home() / "flask-backend" / "uploads" / "credential"
_AMEY_DIR       = Path(__file__).parent


def _resolve_cred(flask_filename: str, config_relative: str) -> str:
    flask_path = _FLASK_CRED_DIR / flask_filename
    if flask_path.exists():
        return str(flask_path)
    abs_config = Path(config_relative)
    if not abs_config.is_absolute():
        abs_config = _AMEY_DIR / config_relative
    if abs_config.exists():
        return str(abs_config)
    raise FileNotFoundError(
        f"Credential not found at '{flask_path}' or '{abs_config}'. "
        f"Upload it via /api/config or place it in ~/amey/."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point — called by Flask route
# ─────────────────────────────────────────────────────────────────────────────

def run_migration(
    run_id:         str,
    user_mapping:   Dict[str, str],
    progress_cb:    Callable[[Dict], None] = None,
    folder_workers: int = FOLDER_WORKERS,
    global_workers: int = GLOBAL_WORKERS,
    xlarge_workers: int = XLARGE_WORKERS,
) -> Dict:
    """
    Entry point called by the Flask migration route.

    Prerequisite: discovery_engine.run_discovery(run_id, user_mapping) already ran.

    Args:
        run_id:         Unique run ID — must match the discovery run_id so SQL
                        queries hit the same migration_id partition.
        user_mapping:   { source_email: dest_email, ... }
        progress_cb:    Optional callable for SSE streaming. Called after every
                        file with dict: { success, ignored, skipped, error,
                                          file_name, source_email, done, total }
        folder_workers: Parallel threads for Phase 1 (default 4).
        global_workers: Parallel threads for Phase 2 regular files (default 14).
        xlarge_workers: Dedicated threads for files >600 MB in Phase 2 pass-1
                        (default 4). These complete first, then global_workers
                        threads are used for all remaining files.

    FEAT-5 — Two-tier Phase-2 queue:
        Files >600 MB are submitted to the xlarge_workers pool first and allowed
        to complete before regular files are processed. This prevents large files
        from blocking small ones AND ensures XL transfers start with dedicated
        bandwidth. Once the XL pass finishes, the full global_workers pool is
        released for regular files.

    FEAT-6 — RAM-adaptive streaming:
        Chunk sizes for all downloads/uploads scale automatically with available
        system RAM (8 MB – 256 MB). Install psutil for this feature; falls back
        to the static 32 MB default otherwise.
    """
    from config import Config
    from sql_state_manager import SQLStateManager
    from auth import GoogleAuthManager

    # ── Resolve absolute credential paths ─────────────────────────────────────
    src_creds = _resolve_cred("source_credentials.json", Config.SOURCE_CREDENTIALS_FILE)
    dst_creds = _resolve_cred("dest_credentials.json",   Config.DEST_CREDENTIALS_FILE)

    gcs_key = Config.GCS_SERVICE_ACCOUNT_FILE
    if not Path(gcs_key).is_absolute():
        gcs_key = str(_AMEY_DIR / gcs_key)

    logger.info(f"[migration] src_creds={src_creds}")
    logger.info(f"[migration] dst_creds={dst_creds}")
    logger.info(f"[migration] gcs_key={gcs_key}")

    # ── Build SQLStateManager with db_config dict (not a callable) ─────────────
    db_config = {
        "host":     Config.DB_HOST,
        "port":     Config.DB_PORT,
        "database": Config.DB_NAME,
        "user":     Config.DB_USER,
        "password": Config.DB_PASSWORD,
        # connect_timeout intentionally omitted here — SQLStateManager.__init__
        # passes it as a separate kwarg to MySQLConnectionPool. Including it in
        # db_config causes: "got multiple values for keyword argument 'connect_timeout'"
    }

    sql_mgr = SQLStateManager(
        db_config=db_config,
        gcs_bucket=Config.GCS_BUCKET_NAME,
        gcs_key_file=gcs_key,
        source_domain=Config.SOURCE_DOMAIN,
        dest_domain=Config.DEST_DOMAIN,
        gcs_prefix=Config.GCS_STAGING_PREFIX,
        migration_id=run_id,   # ← ties SQL queries to this run_id
    )

    # ── Build per-user auth managers ──────────────────────────────────────────
    source_auth = GoogleAuthManager(
        src_creds, Config.SCOPES,
        delegate_email=Config.SOURCE_ADMIN_EMAIL,
    )
    source_auth.authenticate()

    dest_auth = GoogleAuthManager(
        dst_creds, Config.SCOPES,
        delegate_email=Config.DEST_ADMIN_EMAIL,
    )
    dest_auth.authenticate()

    # ── Reset any IN_PROGRESS rows left by a previous crashed attempt ─────────
    # Without this, a fresh start after a crash silently skips stuck files because
    # get_all_pending_items() only fetches PENDING/FAILED rows (not IN_PROGRESS).
    _reset_conn = None
    try:
        _reset_conn = sql_mgr.get_conn()
        _reset_cur  = _reset_conn.cursor()
        _reset_cur.execute(
            "UPDATE migration_items "
            "SET status='PENDING', error_message=NULL "
            "WHERE migration_id=%s AND status='IN_PROGRESS' AND is_folder=0",
            (run_id,),
        )
        _reset_n = _reset_cur.rowcount
        _reset_conn.commit()
        if _reset_n:
            logger.warning(
                f"[migration] reset {_reset_n} IN_PROGRESS→PENDING for run_id={run_id} "
                f"(leftover from previous crashed attempt)"
            )
    except Exception as _reset_exc:
        logger.warning(f"[migration] IN_PROGRESS reset failed (safe fallback): {_reset_exc}")
    finally:
        if _reset_conn:
            try:
                _reset_conn.close()
            except Exception:
                pass

    # ── Run engine ────────────────────────────────────────────────────────────
    engine = MigrationEngine(
        source_auth=source_auth,
        dest_auth=dest_auth,
        config=Config,
        checkpoint=sql_mgr,
        gcs_helper=sql_mgr,        # SQLStateManager has download/upload/delete_temp
        run_id=run_id,
        get_conn=sql_mgr.get_conn,
        progress_cb=progress_cb,
    )

    return engine.migrate_domain(
        user_mapping=user_mapping,
        folder_workers=folder_workers,
        global_workers=global_workers,
        xlarge_workers=xlarge_workers,
    )


# ─────────────────────────────────────────────────────────────────────────────
# MigrationEngine
# ─────────────────────────────────────────────────────────────────────────────

class MigrationEngine:

    def __init__(
        self,
        source_auth,
        dest_auth,
        config,
        checkpoint,
        gcs_helper,
        run_id:      str,
        get_conn,
        progress_cb: Callable[[Dict], None] = None,
    ):
        self.source_auth = source_auth
        self.dest_auth   = dest_auth
        self.config      = config
        self.sql_mgr     = checkpoint
        self.gcs         = gcs_helper
        self.run_id      = run_id
        self.get_conn    = get_conn
        self.progress_cb = progress_cb

        self.max_retries        = 5
        self.connection_timeout = CONNECTION_TIMEOUT

        self._thread_local    = threading.local()
        self._folder_maps:      Dict[str, Dict[str, str]] = {}
        self._folder_maps_lock: threading.Lock            = threading.Lock()
        self._processed:        Set[Tuple]                = set()
        self._processed_lock:   threading.Lock            = threading.Lock()
        self._counter_lock      = threading.Lock()
        self._done_count        = 0
        self._total_count       = 0

        self.stats = {
            "total_files": 0, "successful": 0, "failed": 0,
            "skipped": 0, "ignored": 0, "folders_created": 0,
            "folders_failed": 0, "gcs_routed": 0, "memory_routed": 0,
            "start_time": None, "end_time": None,
        }

    # =========================================================================
    # Public
    # =========================================================================

    def migrate_domain(
        self,
        user_mapping:   Dict[str, str],
        folder_workers: int = FOLDER_WORKERS,
        global_workers: int = GLOBAL_WORKERS,
        xlarge_workers: int = XLARGE_WORKERS,
    ) -> Dict:
        self.stats["start_time"] = datetime.now()
        logger.info(
            f"[DOMAIN] v4 SQL-first | {len(user_mapping)} users | "
            f"run_id={self.run_id}"
        )

        summary: Dict = {
            "total_users": len(user_mapping), "completed_users": 0, "failed_users": 0,
            "total_files_migrated": 0, "total_files_failed": 0,
            "total_files_skipped": 0, "total_files_ignored": 0,
            "total_folders_created": 0, "total_folders_failed": 0,
            "total_collaborators_migrated": 0, "total_external_collaborators": 0,
            "accuracy_rate": 0.0, "user_results": [],
            "start_time": self.stats["start_time"].isoformat(),
            "end_time": None, "detailed_failures": [],
        }

        # ── Phase 1: create folder structure per user ──────────────────────────
        logger.info("[PHASE-1] Creating destination folder structures from SQL...")
        user_stats: Dict[str, Dict] = {}

        with ThreadPoolExecutor(max_workers=min(folder_workers, len(user_mapping))) as pool:
            futures = {
                pool.submit(self._prepare_user_folders, src, dst): (src, dst)
                for src, dst in user_mapping.items()
            }
            for future in as_completed(futures):
                src, dst = futures[future]
                try:
                    result = future.result()
                    user_stats[src] = result
                    logger.info(
                        f"[PHASE-1] {src}: {result.get('files_total', 0)} files | "
                        f"{result.get('folders_created', 0)} folders created"
                    )
                except Exception as exc:
                    logger.error(f"[PHASE-1] {src}: {exc}", exc_info=True)
                    user_stats[src] = {"status": "folder_prep_failed", "error": str(exc)}

        # ── Phase 2: two-tier drain — XL files first, then full pool ─────────
        pending = self.sql_mgr.get_all_pending_items(self.run_id)
        # Sort: largest files first so XL jobs start immediately
        pending.sort(
            key=lambda r: int(getattr(r, "file_size_bytes", None) or 0),
            reverse=True,
        )

        xl_items  = [r for r in pending
                     if int(getattr(r, "file_size_bytes", None) or 0) >= XLARGE_FILE_THRESHOLD_BYTES]
        reg_items = [r for r in pending
                     if int(getattr(r, "file_size_bytes", None) or 0) <  XLARGE_FILE_THRESHOLD_BYTES]

        self._total_count = len(pending)
        logger.info(
            f"[PHASE-2] {len(pending)} files pending | "
            f"XL(>{XLARGE_FILE_THRESHOLD_BYTES // (1024*1024)} MB): {len(xl_items)} files × {xlarge_workers} workers | "
            f"regular: {len(reg_items)} files × {global_workers} workers"
        )

        file_results: Dict[str, Dict] = {}
        file_results_lock = threading.Lock()

        def _drain(items: list, max_workers: int, label: str):
            """Submit items to a pool and collect results into file_results."""
            if not items:
                return
            done_local = 0
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(self._process_queue_item, item): item
                    for item in items
                }
                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        res = future.result()
                        with file_results_lock:
                            file_results[item.file_id] = res
                        done_local += 1
                        # progress log every 50 completions across both passes
                        with self._counter_lock:
                            total_done = self._done_count  # already incremented by _emit
                        if total_done % 50 == 0:
                            logger.info(
                                f"[PHASE-2/{label}] {total_done}/{len(pending)} complete"
                            )
                    except Exception as exc:
                        logger.error(
                            f"[PHASE-2/{label}] [{item.file_name}]: {exc}",
                            exc_info=True,
                        )
                        with file_results_lock:
                            file_results[item.file_id] = {
                                "success": False, "error": str(exc),
                                "source_email": getattr(item, "source_email", ""),
                                "file_name":    getattr(item, "file_name",    ""),
                            }

        # Pass 1: drain XL files with the dedicated pool (blocks until done)
        if xl_items:
            logger.info(
                f"[PHASE-2/XL] Starting {len(xl_items)} XL files "
                f"({xlarge_workers} workers) …"
            )
            _drain(xl_items, xlarge_workers, "XL")
            logger.info(
                f"[PHASE-2/XL] All XL files finished — "
                f"switching to full {global_workers}-worker pool"
            )

        # Pass 2: drain remaining files with full pool
        _drain(reg_items, global_workers, "REG")

        # ── Aggregate per-user ─────────────────────────────────────────────────
        per_user: Dict[str, Dict] = {
            src: {
                "source_email": src, "dest_email": dst,
                "files_migrated": 0, "files_failed": 0,
                "files_skipped": 0, "files_ignored": 0,
                "collaborators_migrated": 0, "external_collaborators": 0,
                "errors": [],
            }
            for src, dst in user_mapping.items()
        }

        for fid, res in file_results.items():
            src_email = res.get("source_email", "")
            if src_email not in per_user: continue
            agg = per_user[src_email]
            if res.get("skipped"):      agg["files_skipped"] += 1
            elif res.get("ignored"):    agg["files_ignored"] += 1
            elif res.get("success"):
                agg["files_migrated"]         += 1
                agg["collaborators_migrated"] += res.get("collaborators_migrated", 0)
                agg["external_collaborators"] += res.get("external_collaborators", 0)
            else:
                agg["files_failed"] += 1
                agg["errors"].append({
                    "file": res.get("file_name", ""), "file_id": fid,
                    "error": res.get("error", ""), "error_type": res.get("error_type", ""),
                    "user": src_email,
                })

        for src, agg in per_user.items():
            disc = user_stats.get(src, {})
            agg["files_total"]     = disc.get("files_total",     0)
            agg["folders_created"] = disc.get("folders_created", 0)
            agg["folders_failed"]  = disc.get("folders_failed",  0)

            attempted = agg["files_total"] - agg["files_skipped"] - agg["files_ignored"]
            agg["accuracy_rate"] = (
                agg["files_migrated"] / attempted * 100 if attempted > 0 else 100.0
            )

            if disc.get("status") == "folder_prep_failed":
                agg["status"] = "failed"; summary["failed_users"] += 1
            else:
                agg["status"] = "completed" if agg["files_failed"] == 0 else "partial"
                summary["completed_users"] += 1

            for k in ("files_migrated","files_failed","files_skipped","files_ignored",
                      "folders_created","folders_failed","collaborators_migrated","external_collaborators"):
                summary[f"total_{k}"] = summary.get(f"total_{k}", 0) + agg.get(k, 0)

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
            f"{_fmt_duration(summary['duration_seconds'])}"
        )
        return summary

    # =========================================================================
    # Phase 1 — folder creation from SQL (no Drive listing)
    # =========================================================================

    def _prepare_user_folders(self, source_email: str, dest_email: str) -> Dict:
        result = {
            "source_email": source_email, "dest_email": dest_email,
            "files_total": 0, "folders_created": 0, "folders_failed": 0, "status": "ok",
        }
        try:
            dest_drive  = self._get_drive_service_for_thread(self.dest_auth, dest_email)
            all_records = self.sql_mgr.load_user_items(source_email)

            if not all_records:
                logger.warning(
                    f"[PHASE-1] {source_email}: no SQL items — "
                    f"did discovery_engine run for run_id={self.run_id}?"
                )
                return result

            all_folders, all_files = self._split_items_from_records(all_records)
            result["files_total"] = len(all_files)
            logger.info(
                f"[PHASE-1] {source_email}: "
                f"{len(all_folders)} folders + {len(all_files)} files from SQL"
            )

            folder_mapping = self.sql_mgr.get_folder_mapping(self.run_id, source_email)
            missing = [
                f for f in all_folders
                if (f.get("id") or f.get("file_id")) not in folder_mapping
            ]
            if missing:
                new_fm = self._build_folder_structure(missing, dest_drive, source_email)
                folder_mapping.update(new_fm)

            with self._folder_maps_lock:
                self._folder_maps[source_email] = folder_mapping

            result["folders_created"] = len(folder_mapping)
            result["folders_failed"]  = max(0, len(all_folders) - len(folder_mapping))

        except Exception as exc:
            logger.error(f"[PHASE-1] {source_email}: {exc}", exc_info=True)
            result["status"] = "folder_prep_failed"
            result["error"]  = str(exc)

        return result

    # =========================================================================
    # Phase 2 — file transfer
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
            return self._emit({**base, "ignored": True})

        # FIX-7: Skip files exceeding the 5 GB hard size limit.
        if file_size > MAX_FILE_SIZE_BYTES:
            reason = f"File size {_fmt_bytes(file_size)} exceeds 5 GB limit — ignored"
            logger.warning(f"[SIZE-LIMIT] {file_name} ({file_id}): {reason}")
            self.sql_mgr.mark_ignored(self.run_id, file_id, reason)
            return self._emit({**base, "ignored": True})

        # Only skip genuinely DONE items (already migrated in a previous run).
        # We do NOT use should_skip_item() here because it also returns True for
        # IN_PROGRESS rows — those represent crashed transfers that MUST be retried.
        # The IN_PROGRESS reset in run_migration() converts them back to PENDING
        # before Phase 2 starts, but as belt-and-suspenders we guard here too.
        try:
            _skip_status = self.sql_mgr.get_item_status(file_id)
            if _skip_status == "DONE":
                return self._emit({**base, "skipped": True})
        except AttributeError:
            # Fallback: sql_mgr doesn't expose get_item_status — use original check
            should_skip, _ = self.sql_mgr.should_skip_item(file_id)
            if should_skip:
                return self._emit({**base, "skipped": True})
        except Exception:
            pass  # DB error on skip check → don't skip, attempt the transfer

        with self._processed_lock:
            if (file_id, file_name, file_size) in self._processed:
                return self._emit({**base, "skipped": True})

        try:
            source_drive = self._get_drive_service_for_thread(self.source_auth, src_email)
            dest_drive   = self._get_drive_service_for_thread(self.dest_auth,   dst_email)
        except Exception as exc:
            err = f"Auth error: {exc}"
            self.sql_mgr.mark_failed(self.run_id, file_id, err)
            return self._emit({**base, "error": err, "error_type": "auth_error"})

        with self._folder_maps_lock:
            fm = self._folder_maps.get(src_email, {})
        dest_parent = fm.get(parent_id) if parent_id else None

        self.sql_mgr.mark_in_progress(self.run_id, file_id)
        res = self._migrate_file(
            file_id, file_name, mime_type, file_size,
            dest_parent, source_drive, dest_drive,
        )

        if res["success"]:
            dest_id = res.get("dest_id")
            self.sql_mgr.mark_done(self.run_id, file_id, dest_item_id=dest_id, dest_parent_id=dest_parent)
            with self._processed_lock:
                self._processed.add((file_id, file_name, file_size))
            perm_r = {"migrated": 0, "external": 0}
            if dest_id:
                perm_r = self._migrate_permissions_hybrid(
                    file_id, dest_id, file_name, source_drive, dest_drive
                )
            return self._emit({
                **base, "success": True, "dest_id": dest_id,
                "collaborators_migrated": perm_r.get("migrated", 0),
                "external_collaborators": perm_r.get("external", 0),
            })
        elif res.get("ignored"):
            self.sql_mgr.mark_ignored(self.run_id, file_id, res.get("error", ""))
            return self._emit({**base, "ignored": True})
        else:
            err = res.get("error", "Unknown")
            self.sql_mgr.mark_failed(self.run_id, file_id, err)
            return self._emit({**base, "error": err, "error_type": res.get("error_type", "")})

    def _emit(self, result: Dict) -> Dict:
        """Thread-safe SSE progress callback. Returns result for chaining."""
        if self.progress_cb:
            with self._counter_lock:
                self._done_count += 1
                done, total = self._done_count, self._total_count
            try:
                self.progress_cb({**result, "done": done, "total": total})
            except Exception as exc:
                logger.debug(f"progress_cb error: {exc}")
        return result

    # =========================================================================
    # File routing
    # =========================================================================

    def _migrate_file(
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
        if file_size >= LARGE_FILE_THRESHOLD_BYTES and self.gcs:
            return self._migrate_via_gcs(
                file_id, file_name, mime_type, file_size,
                dest_parent_id, source_drive, dest_drive,
            )
        return self._migrate_via_memory(
            file_id, file_name, mime_type, file_size,
            dest_parent_id, source_drive, dest_drive,
        )

    # ── Memory path (<50 MB) ──────────────────────────────────────────────────

    def _migrate_via_memory(
        self, file_id, file_name, mime_type, file_size,
        dest_parent_id, source_drive, dest_drive,
    ) -> Dict:
        empty, last_error = {"success": False, "dest_id": None, "ignored": False, "error": None}, ""

        for attempt in range(self.max_retries):
            wait = _backoff(attempt); dl_buf = None
            chunk = _get_adaptive_chunk_size()
            try:
                req    = source_drive.files().get_media(
                    fileId=file_id, supportsAllDrives=True, acknowledgeAbuse=True
                )
                dl_buf = io.BytesIO()
                try:
                    dl = MediaIoBaseDownload(dl_buf, req, chunksize=chunk)
                    done = False
                    while not done: _, done = dl.next_chunk()
                    dl_buf.seek(0); data = dl_buf.read()
                finally:
                    dl_buf.close(); dl_buf = None

                if not data:
                    if file_size == 0:
                        meta = {"name": file_name}
                        if dest_parent_id: meta["parents"] = [dest_parent_id]
                        resp = dest_drive.files().create(body=meta, fields="id", supportsAllDrives=True).execute()
                        self.stats["memory_routed"] += 1
                        return {**empty, "success": True, "dest_id": _extract_id(resp)}
                    last_error = "Empty download for non-zero file"
                    if attempt < self.max_retries - 1: time.sleep(wait); continue
                    return {**empty, "error": last_error, "error_type": "empty_download"}

                meta = {"name": file_name}
                if dest_parent_id: meta["parents"] = [dest_parent_id]
                upload_buf = io.BytesIO(data)
                try:
                    use_resumable = len(data) >= 5 * 1_024 * 1_024
                    media = MediaIoBaseUpload(
                        upload_buf, mimetype=mime_type, resumable=use_resumable,
                        chunksize=chunk if use_resumable else -1,
                    )
                    resp = dest_drive.files().create(
                        body=meta, media_body=media, fields="id", supportsAllDrives=True,
                    ).execute()
                finally:
                    upload_buf.close()

                dest_id = _extract_id(resp)
                if dest_id is None:
                    return {**empty, "error": f"Bad response: {resp!r}", "error_type": "bad_response"}
                self.stats["memory_routed"] += 1
                return {**empty, "success": True, "dest_id": dest_id}

            except HttpError as exc:
                code = exc.resp.status; last_error = str(exc)
                if code == 200:
                    try:
                        body = json.loads(exc.content.decode("utf-8"))
                        self.stats["memory_routed"] += 1
                        return {**empty, "success": True, "dest_id": _extract_id(body)}
                    except Exception: pass
                    self.stats["memory_routed"] += 1
                    return {**empty, "success": True, "dest_id": None}
                if code == 403 and any(k in last_error for k in (
                    "cannotDownload","fileNotDownloadable","cannotDownloadAbusiveFile"
                )):
                    return {**empty, "ignored": True, "error": "Download restricted"}
                if code in (429, 500, 503) and attempt < self.max_retries - 1:
                    time.sleep(wait); continue
                return {**empty, "error": last_error, "error_type": f"http_{code}"}

            except (ConnectionResetError, ConnectionError, OSError, TimeoutError) as exc:
                last_error = str(exc)
                if attempt < self.max_retries - 1: time.sleep(wait)

            except Exception as exc:
                last_error = str(exc)
                logger.error(f"[MEM] [{file_name}]: {last_error}", exc_info=True)
                return {**empty, "error": last_error, "error_type": "unexpected"}

            finally:
                if dl_buf is not None:
                    try: dl_buf.close()
                    except Exception: pass

        return {**empty, "error": last_error, "error_type": "memory_transfer_failed"}

    # ── GCS path (>=50 MB) — uses SQLStateManager's helpers ──────────────────

    def _migrate_via_gcs(
        self, file_id, file_name, mime_type, file_size,
        dest_parent_id, source_drive, dest_drive,
    ) -> Dict:
        """
        FIX-4: Each attempt now runs inside a ThreadPoolExecutor(1) with a hard
        per-file timeout (GCS_FILE_TIMEOUT seconds, default 3600).

        Previously, a stalled GCS download for a 1 GB+ video would hang the
        outer Phase-2 thread forever until gunicorn killed the whole worker
        process, leaving all in-flight files stuck as IN_PROGRESS in SQL.

        Now: if download_drive_to_gcs() or upload_gcs_to_drive() doesn't finish
        within GCS_FILE_TIMEOUT seconds, a TimeoutError is raised in this thread,
        the attempt is logged, any partial GCS blob is cleaned up, and the retry
        loop continues (or the file is marked FAILED after max_retries).
        """
        empty      = {"success": False, "dest_id": None, "ignored": False, "error": None}
        last_error = ""
        active_blob = None

        for attempt in range(self.max_retries):
            wait         = _backoff(attempt)
            attempt_blob = f"{self.run_id}/{file_id}/attempt_{attempt}"
            active_blob  = None

            try:
                # ── Download: Drive → GCS (with hard timeout) ─────────────────
                with _cf.ThreadPoolExecutor(max_workers=1) as sub_pool:
                    dl_future = sub_pool.submit(
                        self.gcs.download_drive_to_gcs,
                        drive_svc=source_drive,
                        file_id=file_id,
                        file_name=file_name,
                        run_id=attempt_blob,
                        mime_type=mime_type,
                    )
                    try:
                        ok, blob_name, err = dl_future.result(timeout=GCS_FILE_TIMEOUT)
                    except _cf.TimeoutError:
                        last_error = (
                            f"GCS download timed out after {GCS_FILE_TIMEOUT}s "
                            f"[{_fmt_bytes(file_size)}]"
                        )
                        logger.warning(f"[GCS] [{file_name}] attempt {attempt+1}: {last_error}")
                        if attempt < self.max_retries - 1:
                            time.sleep(wait)
                            continue
                        return {**empty, "error": last_error, "error_type": "gcs_timeout"}

                if not ok:
                    last_error = err or "GCS download failed"
                    if blob_name:
                        try: self.gcs.delete_temp(blob_name)
                        except Exception: pass
                    if attempt < self.max_retries - 1:
                        time.sleep(wait)
                        continue
                    return {**empty, "error": last_error, "error_type": "gcs_download_failed"}

                active_blob = blob_name

                # ── Upload: GCS → Drive (with hard timeout) ───────────────────
                with _cf.ThreadPoolExecutor(max_workers=1) as sub_pool:
                    ul_future = sub_pool.submit(
                        self.gcs.upload_gcs_to_drive,
                        drive_svc=dest_drive,
                        blob_name=blob_name,
                        file_name=file_name,
                        mime_type=mime_type,
                        parent_id=dest_parent_id,
                    )
                    try:
                        ok2, dest_id, err2 = ul_future.result(timeout=GCS_FILE_TIMEOUT)
                    except _cf.TimeoutError:
                        last_error = (
                            f"GCS upload timed out after {GCS_FILE_TIMEOUT}s "
                            f"[{_fmt_bytes(file_size)}]"
                        )
                        logger.warning(f"[GCS] [{file_name}] attempt {attempt+1}: {last_error}")
                        if active_blob:
                            try: self.gcs.delete_temp(active_blob)
                            except Exception: pass
                            active_blob = None
                        if attempt < self.max_retries - 1:
                            time.sleep(wait)
                            continue
                        return {**empty, "error": last_error, "error_type": "gcs_timeout"}

                if not ok2:
                    last_error = err2 or "GCS upload failed"
                    if active_blob:
                        try: self.gcs.delete_temp(active_blob)
                        except Exception: pass
                        active_blob = None
                    if attempt < self.max_retries - 1:
                        time.sleep(wait)
                        continue
                    return {**empty, "error": last_error, "error_type": "gcs_upload_failed"}

                # ── Success ───────────────────────────────────────────────────
                if active_blob:
                    try: self.gcs.delete_temp(active_blob)
                    except Exception: pass
                    active_blob = None
                self.stats["gcs_routed"] += 1
                return {**empty, "success": True, "dest_id": dest_id}

            except (ConnectionResetError, ConnectionError, OSError, TimeoutError) as exc:
                last_error = str(exc)
                if active_blob:
                    try: self.gcs.delete_temp(active_blob)
                    except Exception: pass
                    active_blob = None
                if attempt < self.max_retries - 1:
                    time.sleep(wait)

            except Exception as exc:
                last_error = str(exc)
                logger.error(f"[GCS] [{file_name}]: {last_error}", exc_info=True)
                if active_blob:
                    try: self.gcs.delete_temp(active_blob)
                    except Exception: pass
                break

        if active_blob:
            try: self.gcs.delete_temp(active_blob)
            except Exception: pass
        return {**empty, "error": last_error, "error_type": "gcs_transfer_failed"}

    # ── Google Workspace export path ──────────────────────────────────────────

    def _migrate_workspace_file(
        self, file_id, file_name, mime_type, dest_parent_id, source_drive, dest_drive,
    ) -> Dict:
        empty = {"success": False, "dest_id": None, "ignored": False, "error": None}
        type_info = GOOGLE_WORKSPACE_TYPES.get(mime_type)
        if not type_info or not type_info.get("can_export"):
            return {**empty, "ignored": True, "error": f"Non-exportable: {mime_type}"}

        for attempt in range(self.max_retries):
            wait = _backoff(attempt); dl_buf = None
            chunk = _get_adaptive_chunk_size()
            try:
                req    = source_drive.files().export_media(fileId=file_id, mimeType=type_info["export_mime"])
                dl_buf = io.BytesIO()
                try:
                    dl = MediaIoBaseDownload(dl_buf, req, chunksize=chunk)
                    done = False
                    while not done: _, done = dl.next_chunk()
                    dl_buf.seek(0); data = dl_buf.read()
                finally:
                    dl_buf.close(); dl_buf = None

                if not data:
                    return {**empty, "error": "Empty export", "error_type": "empty_export"}

                meta = {"name": file_name}
                if dest_parent_id: meta["parents"] = [dest_parent_id]
                if type_info.get("import_mime"): meta["mimeType"] = type_info["import_mime"]

                upload_buf = io.BytesIO(data)
                try:
                    use_resumable = len(data) >= 5 * 1_024 * 1_024
                    media = MediaIoBaseUpload(
                        upload_buf, mimetype=type_info["export_mime"],
                        resumable=use_resumable, chunksize=chunk if use_resumable else -1,
                    )
                    resp = dest_drive.files().create(
                        body=meta, media_body=media, fields="id", supportsAllDrives=True,
                    ).execute()
                finally:
                    upload_buf.close()

                dest_id = _extract_id(resp)
                if dest_id is None:
                    return {**empty, "error": f"Bad response: {resp!r}", "error_type": "bad_response"}
                self.stats["memory_routed"] += 1
                return {**empty, "success": True, "dest_id": dest_id}

            except HttpError as exc:
                err = str(exc); code = exc.resp.status
                if code == 200:
                    try:
                        body = json.loads(exc.content.decode("utf-8"))
                        self.stats["memory_routed"] += 1
                        return {**empty, "success": True, "dest_id": _extract_id(body)}
                    except Exception: pass
                    self.stats["memory_routed"] += 1
                    return {**empty, "success": True, "dest_id": None}
                # FIX-2: exportSizeLimitExceeded now falls back to PDF for
                # presentations (and drawings) instead of hard-failing.
                if "exportSizeLimitExceeded" in err:
                    if "fallback_mime" in type_info:
                        logger.warning(
                            f"[WORKSPACE] [{file_name}] exportSizeLimitExceeded — "
                            f"retrying as {type_info['fallback_ext']}"
                        )
                        return self._workspace_fallback(
                            file_id, file_name, type_info, dest_parent_id, source_drive, dest_drive
                        )
                    # No fallback defined — mark ignored rather than failed
                    logger.warning(
                        f"[WORKSPACE] [{file_name}] exportSizeLimitExceeded and no "
                        f"fallback defined for {mime_type} — marking ignored"
                    )
                    return {**empty, "ignored": True, "error": f"exportSizeLimitExceeded: {err}"}
                if code in (429, 500, 503) and attempt < self.max_retries - 1:
                    time.sleep(wait); continue
                return {**empty, "error": err, "error_type": f"http_{code}"}

            except Exception as exc:
                err = str(exc)
                if attempt < self.max_retries - 1: time.sleep(wait)
                else: return {**empty, "error": err, "error_type": "workspace_export_failed"}

            finally:
                if dl_buf is not None:
                    try: dl_buf.close()
                    except Exception: pass
                    dl_buf = None

        return {**empty, "error": "Max retries exceeded", "error_type": "workspace_export_failed"}

    def _workspace_fallback(
        self, file_id, file_name, type_info, dest_parent_id, source_drive, dest_drive,
    ) -> Dict:
        empty = {"success": False, "dest_id": None, "ignored": False, "error": None}
        dl_buf = None
        try:
            req    = source_drive.files().export_media(fileId=file_id, mimeType=type_info["fallback_mime"])
            dl_buf = io.BytesIO()
            try:
                dl = MediaIoBaseDownload(dl_buf, req, chunksize=CHUNK_SIZE)
                done = False
                while not done: _, done = dl.next_chunk()
                dl_buf.seek(0); data = dl_buf.read()
            finally:
                dl_buf.close(); dl_buf = None

            if not data:
                return {**empty, "error": "Empty fallback export", "error_type": "empty_export"}

            meta = {"name": file_name + type_info["fallback_ext"]}
            if dest_parent_id: meta["parents"] = [dest_parent_id]
            upload_buf = io.BytesIO(data)
            try:
                use_resumable = len(data) >= 5 * 1_024 * 1_024
                media = MediaIoBaseUpload(
                    upload_buf, mimetype=type_info["fallback_mime"],
                    resumable=use_resumable, chunksize=CHUNK_SIZE if use_resumable else -1,
                )
                resp = dest_drive.files().create(
                    body=meta, media_body=media, fields="id", supportsAllDrives=True,
                ).execute()
            finally:
                upload_buf.close()

            dest_id = _extract_id(resp)
            if dest_id is None:
                return {**empty, "error": f"Bad response: {resp!r}", "error_type": "bad_response"}
            return {**empty, "success": True, "dest_id": dest_id}

        except Exception as exc:
            return {**empty, "error": str(exc), "error_type": "workspace_fallback_failed"}
        finally:
            if dl_buf is not None:
                try: dl_buf.close()
                except Exception: pass

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
            if len(perms) <= 1: return result
        except Exception as exc:
            logger.debug(f"Permissions fetch failed [{name}]: {exc}")
            return result

        try:
            from permissions_migrator import EnhancedPermissionsMigrator
            pm = EnhancedPermissionsMigrator(
                source_drive, dest_drive,
                self.config.SOURCE_DOMAIN, self.config.DEST_DOMAIN,
            )
            pr = pm.migrate_permissions(source_id, dest_id, perms)
            result.update({
                "migrated": pr.get("migrated", 0), "failed": pr.get("failed", 0),
                "external": pr.get("external_users", 0), "skipped": pr.get("skipped", 0),
            })

            valid_roles = {"owner","organizer","fileOrganizer","writer","commenter","reader"}
            valid_cls   = {"internal_both_domains","internal_source_only","external_domain","general_access"}

            for detail in pr.get("details", []):
                role   = detail.get("role", ""); ptype = detail.get("type", "user")
                status = detail.get("status", "failed"); cls = detail.get("classification", "external_domain")
                se     = detail.get("original_email", ""); de = detail.get("target_email", "")
                error  = detail.get("error", "")

                if role == "owner" or status == "skipped": continue
                if role not in valid_roles: continue
                if cls not in valid_cls: cls = "external_domain"

                try:
                    self.sql_mgr.upsert_permission(
                        file_id=dest_id, item_type="FILE",
                        permission_type=(ptype if ptype in ("user","group","domain","anyone") else "user"),
                        source_email=se, dest_email=de, role=role, classification=cls,
                        is_inherited=False, parent_drive_id=None,
                    )
                    if status == "success":  self.sql_mgr.mark_permission_done(dest_id, de, role)
                    elif status == "failed": self.sql_mgr.mark_permission_failed(dest_id, de, role, error)
                except Exception as exc:
                    logger.debug(f"SQL permission track [{name}]: {exc}")

        except ImportError:
            logger.error("EnhancedPermissionsMigrator not available")
        except Exception as exc:
            logger.debug(f"Permission migration error [{name}]: {exc}")

        return result

    # =========================================================================
    # Folder structure builder
    # =========================================================================

    def _build_folder_structure(
        self, folders, dest_drive, source_email,
    ) -> Dict[str, str]:
        if not folders: return {}

        id_set  = {f.get("id") or f.get("file_id") or f.get("source_item_id") for f in folders}
        visited: Set[str] = set(); sorted_folders: List[Dict] = []

        def visit(folder):
            fid = folder.get("id") or folder.get("file_id") or folder.get("source_item_id")
            if fid in visited: return
            visited.add(fid)
            pids = folder.get("parents", [])
            if not pids and folder.get("source_parent_id"): pids = [folder["source_parent_id"]]
            if pids and pids[0] in id_set:
                parent = next(
                    (f for f in folders
                     if (f.get("id") or f.get("file_id") or f.get("source_item_id")) == pids[0]),
                    None,
                )
                if parent: visit(parent)
            sorted_folders.append(folder)

        for f in folders: visit(f)

        folder_mapping: Dict[str, str] = {}
        for folder in sorted_folders:
            fid   = folder.get("id") or folder.get("file_id") or folder.get("source_item_id")
            fname = folder.get("name") or folder.get("file_name") or folder.get("source_item_name", "")
            pids  = folder.get("parents", [])
            if not pids and folder.get("source_parent_id"): pids = [folder["source_parent_id"]]

            self.sql_mgr.mark_in_progress(self.run_id, fid)
            dest_parent = folder_mapping.get(pids[0]) if pids else None
            dest_fid    = self._create_folder(fname, dest_parent, dest_drive)

            if dest_fid:
                folder_mapping[fid] = dest_fid
                self.sql_mgr.register_folder_mapping(self.run_id, fid, dest_fid)
                self.sql_mgr.mark_done(self.run_id, fid, dest_item_id=dest_fid, dest_parent_id=dest_parent)
                self.stats["folders_created"] += 1
            else:
                self.sql_mgr.mark_failed(self.run_id, fid, "Failed to create folder")
                self.stats["folders_failed"] += 1
                logger.error(f"  Folder failed: {fname}")

        return folder_mapping

    def _create_folder(self, name, parent_id, dest_drive, max_retries=3) -> Optional[str]:
        for attempt in range(max_retries):
            try:
                meta = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
                if parent_id: meta["parents"] = [parent_id]
                resp = dest_drive.files().create(body=meta, fields="id,name", supportsAllDrives=True).execute()
                fid  = _extract_id(resp)
                if fid is None: raise ValueError(f"Bad folder create response: {resp!r}")
                return fid
            except HttpError as exc:
                if exc.resp.status == 409:
                    existing = self._find_existing_folder(name, parent_id, dest_drive)
                    if existing: return existing
                if attempt < max_retries - 1: time.sleep(_backoff(attempt))
                else: logger.error(f"Failed to create folder '{name}': {exc}"); return None
            except Exception as exc:
                if attempt < max_retries - 1: time.sleep(_backoff(attempt))
                else: logger.error(f"Error creating folder '{name}': {exc}"); return None
        return None

    def _find_existing_folder(self, name, parent_id, dest_drive) -> Optional[str]:
        try:
            q = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_id: q += f" and '{parent_id}' in parents"
            resp  = dest_drive.files().list(q=q, fields="files(id)", pageSize=5, supportsAllDrives=True).execute()
            files = resp.get("files", [])
            return files[0]["id"] if files else None
        except Exception: return None

    # =========================================================================
    # Auth / Drive service factory
    # =========================================================================

    def _get_drive_service_for_thread(self, auth_obj, email: str):
        if not hasattr(self._thread_local, "drive_cache"):
            self._thread_local.drive_cache = {}
        if email not in self._thread_local.drive_cache:
            self._thread_local.drive_cache[email] = self._build_drive_service(email)
        return self._thread_local.drive_cache[email]

    def _build_drive_service(self, email: str):
        """
        Build a delegated Drive service for `email` using the service-account
        JSON directly — no dependency on GoogleAuthManager.get_credentials().

        Uses google-auth (ServiceAccountCredentials with domain delegation) so
        we control the httplib2.Http object and can apply the full 1800-second
        socket timeout that was previously silently dropped when get_credentials()
        raised AttributeError and the fallback path was used instead.

        Credential file resolution: Flask upload dir first, then amey/ fallback
        (unchanged from previous implementation via _resolve_cred).
        """
        from google.oauth2 import service_account as _sa

        is_source   = email.endswith(f"@{self.config.SOURCE_DOMAIN}")
        flask_name  = "source_credentials.json" if is_source else "dest_credentials.json"
        config_path = (
            self.config.SOURCE_CREDENTIALS_FILE if is_source
            else self.config.DEST_CREDENTIALS_FILE
        )
        creds_file = _resolve_cred(flask_name, config_path)

        # Build delegated credentials directly from the service-account file.
        # subject= is the user we impersonate (domain-wide delegation).
        creds = _sa.Credentials.from_service_account_file(
            creds_file,
            scopes=self.config.SCOPES,
            subject=email,
        )

        # Apply 1800s socket timeout via google_auth_httplib2.AuthorizedHttp.
        try:
            import google_auth_httplib2 as _gah
            timed_http      = httplib2.Http(timeout=self.connection_timeout)
            authorized_http = _gah.AuthorizedHttp(creds, http=timed_http)
            logger.debug(f"Drive service built for {email} | google_auth_httplib2 | timeout={self.connection_timeout}s")
            return _gapi_build("drive", "v3", http=authorized_http)
        except ImportError:
            logger.warning(f"google_auth_httplib2 not installed — no timeout for {email}")
            return _gapi_build("drive", "v3", credentials=creds)

    # =========================================================================
    # Helpers
    # =========================================================================

    def _split_items_from_records(self, records) -> Tuple[List[Dict], List[Dict]]:
        folders, files = [], []
        for r in records:
            parent = getattr(r, "source_parent_id", None) or getattr(r, "parent_id", None)
            size   = getattr(r, "file_size_bytes",  None) or getattr(r, "file_size",  None)
            item   = {
                "id": r.file_id, "file_id": r.file_id, "name": r.file_name,
                "mimeType": r.mime_type, "size": size,
                "parents": [parent] if parent else [], "source_parent_id": parent,
            }
            if r.mime_type == "application/vnd.google-apps.folder": folders.append(item)
            else: files.append(item)
        return folders, files
