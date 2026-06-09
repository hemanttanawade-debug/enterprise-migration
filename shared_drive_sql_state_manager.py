"""
shared_drive_sql_state_manager.py  —  Standalone, self-contained class.

No dependency on sql_state_manager.py.

All infrastructure (DB pool, GCS helpers, status transitions, summaries, etc.)
is defined directly in this file.  The class is focused exclusively on Shared
Drive discovery and migration — My Drive concerns are absent from the public API
but the internal helpers still support the full schema so mixed-mode databases
are never corrupted.

WHAT THIS CLASS PROVIDES
─────────────────────────
Core infrastructure (formerly in SQLStateManager):
  • MySQL connection pool with retry / reconnect backoff
  • GCS staging helpers (download_drive_to_gcs, upload_gcs_to_drive, etc.)
  • migration_runs CRUD (create_migration_run, finish_migration_run, …)
  • migration_items status transitions (mark_done, mark_failed, …)
  • migration_permissions helpers
  • shared_drive_members helpers

Shared Drive extras (formerly in SharedDriveSQLStateManager subclass):
  1. for_sd_discovery()         — lightweight factory (no GCS needed)
  2. load_shared_drive_items()  — query by source_shared_drive_id alone
  3. load_drive_items()         — routes to load_shared_drive_items()
  4. register_sd_items()        — clean SD-only insert (never touches email cols)
  5. register_discovered_items()— auto-detects drive-ID-as-email pattern
  6. get_all_pending_items()    — filters drive_type='SHARED_DRIVE'
  7. get_sd_pending_items()     — work queue for per-drive migration
  8. get_sd_folder_mapping()    — folder mapping scoped to one source drive
  9. get_sd_drive_summary()     — status counts for one drive (for dashboard)

SCHEMA CONTRACT
────────────────
Shared Drive rows written by this class:
    drive_type             = 'SHARED_DRIVE'
    source_shared_drive_id = <real drive ID>
    dest_shared_drive_id   = <real drive ID or ''>
    source_user_email      = ''   ← NOT NULL satisfied with empty string
    destination_user_email = ''   ← NOT NULL satisfied with empty string
"""

from __future__ import annotations

import io
import logging
import time
import uuid
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple

import mysql.connector
from mysql.connector import pooling
from google.cloud import storage as gcs_storage
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

logger = logging.getLogger(__name__)

FOLDER_MIME  = "application/vnd.google-apps.folder"
_DRIVE_TYPE  = "SHARED_DRIVE"

IGNORED_MIME_TYPES = frozenset({
    "application/vnd.google-apps.script",
    "application/vnd.google-apps.form",
    "application/vnd.google-apps.site",
    "application/octet-stream",
})

GOOGLE_WORKSPACE_EXPORT = {
    "application/vnd.google-apps.document": (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".docx",
        "application/vnd.google-apps.document",
    ),
    "application/vnd.google-apps.spreadsheet": (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xlsx",
        "application/vnd.google-apps.spreadsheet",
    ),
    "application/vnd.google-apps.presentation": (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ".pptx",
        "application/vnd.google-apps.presentation",
    ),
    "application/vnd.google-apps.drawing": ("image/svg+xml", ".svg", None),
    "application/vnd.google-apps.jam":     ("application/pdf", ".pdf", None),
    "application/vnd.google-apps.map":     ("application/vnd.google-earth.kmz", ".kmz", None),
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _s(value) -> str:
    """Safe string: None / whitespace / 'None' → ''."""
    if value is None:
        return ""
    s = str(value).strip()
    return "" if s.lower() == "none" else s


# ─────────────────────────────────────────────────────────────────────────────
# DTO
# ─────────────────────────────────────────────────────────────────────────────

class MigrationRecord:
    __slots__ = (
        "file_id", "file_name", "mime_type", "file_size",
        "parent_id", "dest_folder_id", "status",
        "source_user_email", "dest_user_email",
        "drive_type", "source_shared_drive_id", "dest_shared_drive_id",
        "source_item_id", "source_item_name", "source_parent_id",
        "file_size_bytes", "item_type",
        "source_email", "dest_email",
    )

    def __init__(self, row: dict):
        self.file_id                = row.get("file_id", "")
        self.file_name              = row.get("file_name") or ""
        self.mime_type              = row.get("mime_type") or ""
        self.file_size              = int(row.get("file_size") or 0)
        self.parent_id              = row.get("parent_id")
        self.dest_folder_id         = row.get("dest_folder_id")
        self.status                 = row.get("status") or "PENDING"
        self.source_user_email      = row.get("source_user_email") or ""
        self.dest_user_email        = row.get("destination_user_email") or ""
        self.drive_type             = row.get("drive_type") or _DRIVE_TYPE
        self.source_shared_drive_id = _s(row.get("source_shared_drive_id"))
        self.dest_shared_drive_id   = _s(row.get("dest_shared_drive_id"))

        self.source_item_id   = self.file_id
        self.source_item_name = self.file_name
        self.source_parent_id = self.parent_id
        self.file_size_bytes  = self.file_size
        self.item_type        = "folder" if self.mime_type == FOLDER_MIME else "file"

        self.source_email = self.source_user_email
        self.dest_email   = self.dest_user_email

    def to_dict(self) -> dict:
        return {
            "source_item_id":   self.file_id,
            "source_item_name": self.file_name,
            "mime_type":        self.mime_type,
            "file_size_bytes":  self.file_size,
            "source_parent_id": self.parent_id,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Main class  —  fully standalone, no base-class import
# ─────────────────────────────────────────────────────────────────────────────

class SharedDriveSQLStateManager:
    """
    Self-contained SQL state manager for Shared Drive discovery and migration.

    Instantiate via the factory for discovery:
        mgr = SharedDriveSQLStateManager.for_sd_discovery(migration_id=run_id)

    Or directly for a full migration run (includes GCS):
        mgr = SharedDriveSQLStateManager(
            db_config={...},
            gcs_bucket="my-bucket",
            gcs_key_file="/path/to/key.json",
            migration_id=run_id,
        )
    """

    DEFAULT_PREFIX       = "migration-staging/"
    LARGE_FILE_THRESHOLD = 50   # MB
    _SHARED_DRIVE_MEMBER_ROLES = frozenset({
        "organizer", "fileOrganizer", "writer", "commenter", "reader",
    })

    def __init__(
        self,
        db_config,
        gcs_bucket: str               = None,
        gcs_key_file: str             = None,
        source_domain: str            = "",
        dest_domain: str              = "",
        migration_id: str             = None,
        gcs_prefix: str               = DEFAULT_PREFIX,
        large_file_threshold_mb: int  = LARGE_FILE_THRESHOLD,
        source_shared_drive_id: str   = None,
        dest_shared_drive_id: str     = None,
        _allow_auto_id: bool          = False,
    ):
        # Backward-compat: accept a callable (Config.get_db_connection) as db_config
        if callable(db_config):
            try:
                from config import Config as _Cfg
                db_config = {
                    "host":     _Cfg.DB_HOST,
                    "port":     int(getattr(_Cfg, "DB_PORT", 3306)),
                    "database": _Cfg.DB_NAME,
                    "user":     _Cfg.DB_USER,
                    "password": _Cfg.DB_PASSWORD,
                }
                if not source_domain:
                    source_domain = getattr(_Cfg, "SOURCE_DOMAIN", "")
                if not dest_domain:
                    dest_domain   = getattr(_Cfg, "DEST_DOMAIN", "")
                if gcs_bucket is None:
                    gcs_bucket    = getattr(_Cfg, "GCS_BUCKET", None)
                if gcs_key_file is None:
                    gcs_key_file  = getattr(_Cfg, "GCS_KEY_FILE",
                                            getattr(_Cfg, "SOURCE_CREDENTIALS_FILE", None))
            except Exception as exc:
                raise TypeError(
                    f"SharedDriveSQLStateManager received a callable as db_config but "
                    f"could not auto-resolve settings from Config: {exc}"
                ) from exc

        if not migration_id:
            if not _allow_auto_id:
                raise ValueError(
                    "SharedDriveSQLStateManager requires an explicit migration_id. "
                    "Use SharedDriveSQLStateManager.for_sd_discovery() for discovery-only instances."
                )
            migration_id = str(uuid.uuid4())

        self.migration_id    = migration_id
        self.source_domain   = source_domain or ""
        self.dest_domain     = dest_domain   or ""
        self.gcs_bucket_name = gcs_bucket    or ""
        self.gcs_prefix      = gcs_prefix
        self.large_file_threshold_bytes = large_file_threshold_mb * 1024 * 1024

        self.source_shared_drive_id = _s(source_shared_drive_id)
        self.dest_shared_drive_id   = _s(dest_shared_drive_id)

        # This class is always in Shared Drive mode
        self._drive_type = _DRIVE_TYPE

        logger.info(
            f"SharedDriveSQLStateManager init | id={self.migration_id} | mode={_DRIVE_TYPE}"
            + (
                f" | src_drv={self.source_shared_drive_id}"
                f" | dst_drv={self.dest_shared_drive_id}"
                if self.source_shared_drive_id else ""
            )
        )

        # ── MySQL connection pool ─────────────────────────────────────────────
        self._pool = pooling.MySQLConnectionPool(
            pool_name=f"sd_migration_pool_{uuid.uuid4().hex[:8]}",
            pool_size=20,
            pool_reset_session=False,
            connection_timeout=10,
            connect_timeout=10,
            **db_config,
        )
        logger.info(
            f"SQL pool → {db_config.get('host')}:{db_config.get('port', 3306)}"
            f"/{db_config.get('database')}"
        )

        # ── GCS client (optional) ─────────────────────────────────────────────
        self._gcs    = None
        self._bucket = None
        if gcs_bucket and gcs_key_file:
            try:
                creds = service_account.Credentials.from_service_account_file(
                    gcs_key_file,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
                self._gcs    = gcs_storage.Client(credentials=creds, project=creds.project_id)
                self._bucket = self._gcs.bucket(gcs_bucket)
                logger.info(f"GCS client → bucket={gcs_bucket}")
            except Exception as exc:
                logger.warning(
                    f"GCS init skipped (bucket={gcs_bucket!r}): {exc}. "
                    f"GCS operations will fail if attempted."
                )
        else:
            logger.info("GCS client not initialised. Safe for discovery-only runs.")

        self._cache: Dict[str, MigrationRecord] = {}
        self.checkpoint_file = f"sql://{db_config.get('host')}/{self.migration_id}"

    # ─────────────────────────────────────────────────────────────────────────
    # Factory — discovery only (no GCS, no drive-ID pair needed up front)
    # ─────────────────────────────────────────────────────────────────────────

    @classmethod
    def for_sd_discovery(
        cls,
        migration_id: str  = None,
        db_config: Dict    = None,
        source_domain: str = "",
        dest_domain: str   = "",
    ) -> "SharedDriveSQLStateManager":
        """
        Lightweight constructor for Shared Drive discovery.
        GCS is NOT initialised — discovery only reads/writes SQL.

        If db_config is omitted it is read from config.Config automatically.

        Usage:
            from shared_drive_sql_state_manager import SharedDriveSQLStateManager
            sql_mgr = SharedDriveSQLStateManager.for_sd_discovery(migration_id=run_id)
        """
        if db_config is None:
            try:
                from config import Config as _Cfg
                db_config = {
                    "host":     _Cfg.DB_HOST,
                    "port":     int(getattr(_Cfg, "DB_PORT", 3306)),
                    "database": _Cfg.DB_NAME,
                    "user":     _Cfg.DB_USER,
                    "password": _Cfg.DB_PASSWORD,
                }
                source_domain = source_domain or getattr(_Cfg, "SOURCE_DOMAIN", "")
                dest_domain   = dest_domain   or getattr(_Cfg, "DEST_DOMAIN",   "")
            except Exception as exc:
                raise RuntimeError(
                    f"SharedDriveSQLStateManager.for_sd_discovery() could not load "
                    f"db settings from Config: {exc}"
                ) from exc

        return cls(
            db_config=db_config,
            gcs_bucket=None,
            gcs_key_file=None,
            source_domain=source_domain,
            dest_domain=dest_domain,
            migration_id=migration_id,
            _allow_auto_id=True,
        )

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def is_shared_drive_mode(self) -> bool:
        return True

    # ─────────────────────────────────────────────────────────────────────────
    # DB helpers
    # ─────────────────────────────────────────────────────────────────────────

    @contextmanager
    def _conn(self, retries: int = 6, wait: float = 0.5):
        """
        Get a connection from the pool with retry + reconnect backoff.

        FIX-A  conn.close() is isolated in its own try/except so it never
               triggers the retry loop.
        FIX-B  conn.ping(reconnect=True) heals stale connections before use.
        FIX-C  Pool-exhausted detection covers OperationalError variants.
        """
        _RETRYABLE = (
            "pool exhausted",
            "connection not available",
            "mysql connection not available",
        )

        last_exc = None
        for attempt in range(retries):
            conn = None
            try:
                conn = self._pool.get_connection()

                try:
                    conn.ping(reconnect=True, attempts=3, delay=1)
                except Exception as ping_exc:
                    logger.warning(
                        f"SQL ping failed (attempt {attempt+1}/{retries}): {ping_exc} — will retry"
                    )
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
                    raise ping_exc

                try:
                    yield conn
                    conn.commit()
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    raise
                finally:
                    if conn is not None:
                        try:
                            conn.close()
                        except Exception as close_exc:
                            logger.debug(
                                f"SQL conn.close() swallowed (connection already broken): {close_exc}"
                            )
                return

            except Exception as exc:
                last_exc = exc
                exc_str  = str(exc).lower()
                is_retryable = (
                    any(tok in exc_str for tok in _RETRYABLE)
                    or "PoolError" in type(exc).__name__
                )

                if is_retryable and attempt < retries - 1:
                    sleep_time = wait * (2 ** attempt)
                    logger.warning(
                        f"SQL retryable error (attempt {attempt+1}/{retries}) "
                        f"— retrying in {sleep_time:.1f}s | {exc}"
                    )
                    time.sleep(sleep_time)
                else:
                    raise

        raise last_exc

    def _execute(self, sql: str, params=(), many: bool = False) -> List[Dict]:
        """Execute sql with params. Large executemany batches are chunked."""
        if many and isinstance(params, (list, tuple)) and len(params) > 50:
            CHUNK = 50
            for i in range(0, len(params), CHUNK):
                chunk = params[i : i + CHUNK]
                with self._conn() as conn:
                    cur = conn.cursor(dictionary=True)
                    cur.executemany(sql, chunk)
                time.sleep(0.05)
            return []

        with self._conn() as conn:
            cur = conn.cursor(dictionary=True)
            if many:
                cur.executemany(sql, params)
            else:
                cur.execute(sql, params)
            try:
                return cur.fetchall() or []
            except Exception:
                return []

    def _one(self, sql: str, params=()) -> Optional[Dict]:
        rows = self._execute(sql, params)
        return rows[0] if rows else None

    def get_conn(self):
        """Return a raw connection from the pool. Caller is responsible for closing."""
        return self._pool.get_connection()

    # ─────────────────────────────────────────────────────────────────────────
    # migration_runs
    # ─────────────────────────────────────────────────────────────────────────

    def create_migration_run(self, total_items: int = 0, resume: bool = False) -> str:
        if resume:
            # On resume: only flip status back to RUNNING, preserve all other counters
            self._execute(
                """
                INSERT INTO migration_runs
                    (migration_id, source_domain, destination_domain,
                     start_time, status, total_items)
                VALUES (%s, %s, %s, NOW(), 'RUNNING', %s)
                ON DUPLICATE KEY UPDATE
                    status = 'RUNNING'
                """,
                (self.migration_id, self.source_domain, self.dest_domain, total_items),
            )
        else:
            self._execute(
                """
                INSERT INTO migration_runs
                    (migration_id, source_domain, destination_domain,
                     start_time, status, total_items)
                VALUES (%s, %s, %s, NOW(), 'RUNNING', %s)
                ON DUPLICATE KEY UPDATE
                    status      = 'RUNNING',
                    start_time  = NOW(),
                    total_items = VALUES(total_items)
                """,
                (self.migration_id, self.source_domain, self.dest_domain, total_items),
            )
        logger.info(f"migration_run ready: {self.migration_id} (resume={resume})")
        return self.migration_id

    def finish_migration_run(self, status: str = "COMPLETED"):
        valid = {"RUNNING", "COMPLETED", "FAILED", "STOPPED"}
        if status.upper() not in valid:
            status = "FAILED"
        self._execute(
            "UPDATE migration_runs SET status=%s, end_time=NOW() WHERE migration_id=%s",
            (status.upper(), self.migration_id),
        )

    def update_run_counters(self, **kwargs):
        """Atomically increment migration_runs counters."""
        col_map = {
            "completed":  "completed_items     = completed_items     + %s",
            "failed":     "failed_items        = failed_items        + %s",
            "skipped":    "skipped_items       = skipped_items       + %s",
            "ignored":    "ignored_items       = ignored_items       + %s",
            "size_bytes": "migrated_size_bytes = migrated_size_bytes + %s",
        }
        parts, vals = [], []
        for k, v in kwargs.items():
            if k in col_map:
                parts.append(col_map[k])
                vals.append(int(v))
        if not parts:
            return
        vals.append(self.migration_id)
        self._execute(
            f"UPDATE migration_runs "
            f"SET {', '.join(parts)}, last_processed_at=NOW() "
            f"WHERE migration_id=%s",
            tuple(vals),
        )

    # ── Run-level tracking hooks ──────────────────────────────────────────────

    def start_user(self, run_id, source_email: str):
        logger.debug(f"[run={run_id}] start_user: {source_email}")

    def finish_user(self, run_id, source_email: str, status: str,
                    files_done: int = 0, files_failed: int = 0, bytes_moved: int = 0):
        logger.info(
            f"[run={run_id}] finish_user: {source_email} | "
            f"status={status} | done={files_done} | failed={files_failed}"
        )
        self.update_run_counters(completed=files_done, failed=files_failed, size_bytes=bytes_moved)

    def upsert_user(self, run_id, source_email: str, dest_email: str):
        logger.debug(f"[run={run_id}] upsert_user: {source_email} → {dest_email}")

    def upsert_shared_drive(self, run_id, drive_id: str, drive_name: str):
        logger.debug(f"[run={run_id}] upsert_shared_drive: {drive_name} ({drive_id})")

    def finish_shared_drive(self, run_id, drive_id: str, status: str,
                            files_total: int = 0, files_done: int = 0):
        logger.info(
            f"[run={run_id}] finish_shared_drive: {drive_id} | "
            f"status={status} | total={files_total} | done={files_done}"
        )

    def finish_run(self, run_id, status: str):
        self.finish_migration_run(status.upper())

    # ─────────────────────────────────────────────────────────────────────────
    # migration_items  —  shared drive registration
    # ─────────────────────────────────────────────────────────────────────────

    def register_sd_items(
        self,
        items: List[Dict],
        source_drive_id: str,
        dest_drive_id: str,
    ) -> int:
        """
        Bulk-insert Shared Drive items into migration_items.

        Drive IDs are passed per-call so a single manager instance can safely
        serve multiple parallel drive-worker threads.

        Columns written:
            drive_type             = 'SHARED_DRIVE'
            source_shared_drive_id = source_drive_id
            dest_shared_drive_id   = dest_drive_id  (may be '' for discover-only)
            source_user_email      = ''
            destination_user_email = ''

        Returns number of rows in the batch (attempted).
        """
        if not items:
            return 0

        src_drv = _s(source_drive_id)
        dst_drv = _s(dest_drive_id)

        if not src_drv:
            raise ValueError("register_sd_items: source_drive_id must be non-empty")

        rows = []
        for item in items:
            mime      = item.get("mimeType") or item.get("mime_type") or ""
            status    = "IGNORED" if mime in IGNORED_MIME_TYPES else "PENDING"
            parents   = item.get("parents", [])
            parent_id = item.get("source_parent_id") or (parents[0] if parents else None)
            fid   = item.get("id") or item.get("source_item_id") or item.get("file_id") or ""
            fname = item.get("name") or item.get("source_item_name") or item.get("file_name") or ""
            fsize = int(
                item.get("file_size_bytes") or item.get("file_size") or item.get("size") or 0
            )
            is_fld = 1 if mime == FOLDER_MIME else 0

            rows.append((
                self.migration_id,   # migration_id
                self.source_domain,  # source_domain
                self.dest_domain,    # destination_domain
                _DRIVE_TYPE,         # drive_type = 'SHARED_DRIVE'
                src_drv,             # source_shared_drive_id
                dst_drv,             # dest_shared_drive_id
                "",                  # source_user_email      — NOT NULL → ''
                "",                  # destination_user_email — NOT NULL → ''
                fid,                 # file_id
                fname,               # file_name
                parent_id,           # parent_id
                mime,                # mime_type
                is_fld,              # is_folder
                status,              # status
                fsize,               # file_size
            ))

        self._execute(
            """
            INSERT IGNORE INTO migration_items
                (migration_id, source_domain, destination_domain,
                 drive_type, source_shared_drive_id, dest_shared_drive_id,
                 source_user_email, destination_user_email,
                 file_id, file_name, parent_id, mime_type,
                 is_folder, status, file_size)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
            many=True,
        )

        for row in rows:
            fid = row[8]
            self._cache[fid] = MigrationRecord({
                "file_id":                fid,
                "file_name":              row[9],
                "mime_type":              row[11],
                "file_size":              row[14],
                "parent_id":              row[10],
                "dest_folder_id":         None,
                "status":                 row[13],
                "source_user_email":      "",
                "destination_user_email": "",
                "drive_type":             _DRIVE_TYPE,
                "source_shared_drive_id": src_drv,
                "dest_shared_drive_id":   dst_drv,
            })

        logger.info(
            f"[SD-SQL] Registered {len(rows)} items | "
            f"src_drv={src_drv} dst_drv={dst_drv} | migration_id={self.migration_id}"
        )
        return len(rows)

    def register_discovered_items(
        self,
        items: List[Dict],
        source_email: str            = "",
        dest_email: str              = "",
        source_shared_drive_id: str  = None,
        dest_shared_drive_id: str    = None,
    ):
        """
        Register items for this Shared Drive manager.

        Auto-detects the engine's pattern of passing drive_id as source_email
        (no '@' character) and routes to register_sd_items() with correct
        drive-ID semantics — no changes needed in the discovery engine.

        For genuine My Drive calls (source_email contains '@') a safety warning
        is logged and the items are written with drive_type='SHARED_DRIVE' using
        the drive IDs from the instance (or empty string) since this class does
        not manage My Drive rows.
        """
        src_drv = _s(source_shared_drive_id) or ""
        dst_drv = _s(dest_shared_drive_id)   or ""

        if not src_drv:
            candidate = _s(source_email)
            if candidate and "@" not in candidate:
                src_drv = candidate
                dst_drv = dst_drv or _s(dest_email)

        if not src_drv:
            src_drv = self.source_shared_drive_id
            dst_drv = dst_drv or self.dest_shared_drive_id

        if not src_drv:
            logger.warning(
                "[SD-SQL] register_discovered_items: could not determine a Shared Drive ID. "
                "Items not inserted. Check caller."
            )
            return

        self.register_sd_items(items, src_drv, dst_drv)

    def bulk_register_items(self, run_id, items: List[Dict]):
        """Alias used by migration engines. Groups items by drive ID and registers them."""
        if not items:
            return
        from collections import defaultdict
        groups: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
        for item in items:
            src_drv = _s(item.get("source_shared_drive_id") or self.source_shared_drive_id)
            dst_drv = _s(item.get("dest_shared_drive_id")   or self.dest_shared_drive_id)
            groups[(src_drv, dst_drv)].append(item)

        for (src_drv, dst_drv), group in groups.items():
            self.register_sd_items(group, src_drv, dst_drv)

    # ─────────────────────────────────────────────────────────────────────────
    # migration_items  —  folder mapping
    # ─────────────────────────────────────────────────────────────────────────

    def register_folder_mapping(
        self,
        run_id_or_file_id,
        dest_folder_id_or_src=None,
        dest_folder_id_last=None,
    ):
        """
        Two call signatures:
          Old (v3):  register_folder_mapping(source_folder_id, dest_folder_id)
          New (v4):  register_folder_mapping(run_id, source_folder_id, dest_folder_id)
        """
        if dest_folder_id_last is not None:
            source_folder_id = dest_folder_id_or_src
            dest_folder_id   = dest_folder_id_last
        else:
            source_folder_id = run_id_or_file_id
            dest_folder_id   = dest_folder_id_or_src

        self._execute(
            """
            UPDATE migration_items
               SET dest_folder_id = %s
             WHERE migration_id = %s
               AND file_id      = %s
            """,
            (dest_folder_id, self.migration_id, source_folder_id),
        )
        if source_folder_id in self._cache:
            self._cache[source_folder_id].dest_folder_id = dest_folder_id

    def get_folder_mapping(self, run_id, source_email: str = None) -> Dict[str, str]:
        """Return {source_folder_id: dest_folder_id} for all mapped folders."""
        rows = self._execute(
            """
            SELECT file_id, dest_folder_id
              FROM migration_items
             WHERE migration_id  = %s
               AND is_folder     = 1
               AND dest_folder_id IS NOT NULL
            """,
            (self.migration_id,),
        )
        return {r["file_id"]: r["dest_folder_id"] for r in rows}

    def get_sd_folder_mapping(self, source_drive_id: str) -> Dict[str, str]:
        """
        Return {source_folder_id: dest_folder_id} for all mapped folders within
        the given source Shared Drive. Used by migration engines on resume to
        skip folder re-creation.
        """
        src_drv = _s(source_drive_id)
        if not src_drv:
            raise ValueError("get_sd_folder_mapping: source_drive_id must be non-empty")

        rows = self._execute(
            """
            SELECT file_id, dest_folder_id
              FROM migration_items
             WHERE migration_id           = %s
               AND source_shared_drive_id = %s
               AND is_folder              = 1
               AND dest_folder_id IS NOT NULL
            """,
            (self.migration_id, src_drv),
        )
        return {r["file_id"]: r["dest_folder_id"] for r in rows}

    # ─────────────────────────────────────────────────────────────────────────
    # migration_items  —  status queries
    # ─────────────────────────────────────────────────────────────────────────

    _SELECT_COLS = """
        SELECT file_id, file_name, mime_type, file_size,
               parent_id, dest_folder_id, status,
               source_user_email, destination_user_email,
               drive_type, source_shared_drive_id, dest_shared_drive_id
          FROM migration_items
    """

    def get_item_status(self, run_id, file_id: str) -> Optional[str]:
        record = self._cache.get(file_id)
        if record:
            return record.status
        row = self._one(
            "SELECT status FROM migration_items WHERE migration_id=%s AND file_id=%s",
            (self.migration_id, file_id),
        )
        if row:
            status = row["status"]
            if file_id in self._cache:
                self._cache[file_id].status = status
            return status
        return None

    def should_skip_item(self, file_id: str) -> Tuple[bool, str]:
        record = self._cache.get(file_id)
        if not record:
            row = self._one(
                """
                SELECT status, mime_type,
                       source_user_email, destination_user_email,
                       drive_type, source_shared_drive_id, dest_shared_drive_id
                  FROM migration_items
                 WHERE migration_id=%s AND file_id=%s
                """,
                (self.migration_id, file_id),
            )
            if row:
                record = MigrationRecord({"file_id": file_id, **row})
                self._cache[file_id] = record
            else:
                return False, ""

        if record.status == "DONE":
            return True, "Already migrated"
        if record.status == "IGNORED" or record.mime_type in IGNORED_MIME_TYPES:
            return True, "Non-migratable type"
        return False, ""

    def get_pending_items(
        self,
        run_id,
        source_email: str = None,
        item_types: tuple = ("file",),
    ) -> List[Dict]:
        """Return PENDING/FAILED items as dicts, filtered by Shared Drive rows only."""
        wants_folders = any("folder" in t for t in item_types)
        wants_files   = any("folder" not in t for t in item_types)

        if wants_folders and wants_files:
            folder_filter = ""
        elif wants_folders:
            folder_filter = "AND is_folder = 1"
        else:
            folder_filter = "AND is_folder = 0"

        rows = self._execute(
            f"""
            SELECT file_id, file_name, mime_type, file_size,
                   parent_id, dest_folder_id, status,
                   source_user_email, destination_user_email,
                   drive_type, source_shared_drive_id, dest_shared_drive_id
              FROM migration_items
             WHERE migration_id = %s
               AND drive_type   = 'SHARED_DRIVE'
               AND status IN ('PENDING', 'FAILED')
               {folder_filter}
            """,
            (self.migration_id,),
        )
        result = []
        for row in rows:
            rec = MigrationRecord(row)
            self._cache[rec.file_id] = rec
            result.append(rec.to_dict())
        return result

    def get_all_pending_items(self, migration_id: str = None) -> List[MigrationRecord]:
        """
        Return ALL PENDING/FAILED Shared Drive file rows for this migration_id.
        Filters by drive_type='SHARED_DRIVE' so My Drive rows are never included.
        """
        mid = _s(migration_id or self.migration_id)
        if not mid:
            raise ValueError("get_all_pending_items: migration_id must be non-empty")

        rows = self._execute(
            """
            SELECT file_id, file_name, mime_type, file_size,
                   parent_id, dest_folder_id, status,
                   source_user_email, destination_user_email,
                   drive_type, source_shared_drive_id, dest_shared_drive_id
              FROM migration_items
             WHERE migration_id = %s
               AND drive_type   = 'SHARED_DRIVE'
               AND status       IN ('PENDING', 'FAILED')
               AND is_folder    = 0
            """,
            (mid,),
        )
        records = []
        for row in rows:
            rec = MigrationRecord(row)
            self._cache[rec.file_id] = rec
            records.append(rec)

        logger.debug(
            f"[SD-SQL] get_all_pending_items: {len(records)} SHARED_DRIVE "
            f"files pending for migration_id={mid}"
        )
        return records

    def get_sd_pending_items(
        self,
        source_drive_id: str,
        include_folders: bool = False,
    ) -> List[MigrationRecord]:
        """
        Return PENDING/FAILED items for one source Shared Drive.

        Args:
            source_drive_id:  The source Shared Drive ID to filter by.
            include_folders:  If True, folders are included alongside files.
        """
        src_drv = _s(source_drive_id)
        if not src_drv:
            raise ValueError("get_sd_pending_items: source_drive_id must be non-empty")

        folder_clause = "" if include_folders else "AND is_folder = 0"

        rows = self._execute(
            f"""
            SELECT file_id, file_name, mime_type, file_size,
                   parent_id, dest_folder_id, status,
                   source_user_email, destination_user_email,
                   drive_type, source_shared_drive_id, dest_shared_drive_id
              FROM migration_items
             WHERE migration_id           = %s
               AND source_shared_drive_id = %s
               AND status IN ('PENDING', 'FAILED')
               {folder_clause}
            """,
            (self.migration_id, src_drv),
        )
        records = []
        for row in rows:
            rec = MigrationRecord(row)
            self._cache[rec.file_id] = rec
            records.append(rec)

        logger.debug(
            f"get_sd_pending_items: {len(records)} items for "
            f"src_drv={src_drv} migration_id={self.migration_id}"
        )
        return records

    def count_pending_items(self) -> int:
        row = self._one(
            "SELECT COUNT(*) AS n FROM migration_items "
            "WHERE migration_id=%s AND drive_type='SHARED_DRIVE' "
            "AND status IN ('PENDING','FAILED') AND is_folder=0",
            (self.migration_id,),
        )
        return int(row["n"]) if row else 0

    def migration_run_exists(self) -> bool:
        row = self._one(
            "SELECT 1 AS found FROM migration_runs WHERE migration_id=%s",
            (self.migration_id,),
        )
        return row is not None

    # ─────────────────────────────────────────────────────────────────────────
    # migration_items  —  cache loading (resume)
    # ─────────────────────────────────────────────────────────────────────────

    def _load_cache(self, rows: List[Dict]):
        for row in rows:
            self._cache[row["file_id"]] = MigrationRecord(row)

    def load_shared_drive_items(
        self,
        source_drive_id: str,
        dest_drive_id: str = None,
    ) -> List[MigrationRecord]:
        """
        Load all migration_items rows for a Shared Drive, keyed by
        source_shared_drive_id.

        Args:
            source_drive_id:  The source Shared Drive ID (required).
            dest_drive_id:    Optional; when omitted all rows for source_drive_id
                              under this migration_id are returned regardless of
                              dest_shared_drive_id.
        """
        src_drv = _s(source_drive_id)
        if not src_drv:
            raise ValueError("load_shared_drive_items: source_drive_id must be non-empty")

        dst_drv = _s(dest_drive_id) if dest_drive_id is not None else None

        if dst_drv:
            rows = self._execute(
                self._SELECT_COLS +
                " WHERE migration_id=%s"
                "   AND source_shared_drive_id=%s"
                "   AND dest_shared_drive_id=%s",
                (self.migration_id, src_drv, dst_drv),
            )
            logger.debug(
                f"load_shared_drive_items: {len(rows)} records for src={src_drv} dst={dst_drv}"
            )
        else:
            rows = self._execute(
                self._SELECT_COLS +
                " WHERE migration_id=%s"
                "   AND source_shared_drive_id=%s",
                (self.migration_id, src_drv),
            )
            logger.debug(
                f"load_shared_drive_items: {len(rows)} records for src={src_drv} (any dest)"
            )

        self._load_cache(rows)
        return [self._cache[r["file_id"]] for r in rows]

    def load_drive_items(
        self,
        source_shared_drive_id: str = None,
        dest_shared_drive_id: str   = None,
        **kwargs,
    ) -> List[MigrationRecord]:
        """
        Load items for a Shared Drive pair. Routes to load_shared_drive_items()
        so callers never hit a My Drive fallback.
        """
        src = source_shared_drive_id or self.source_shared_drive_id
        dst = dest_shared_drive_id   or self.dest_shared_drive_id

        if src:
            logger.debug(
                f"[SD-SQL] load_drive_items → load_shared_drive_items "
                f"src={src} dst={dst}"
            )
            return self.load_shared_drive_items(source_drive_id=src, dest_drive_id=dst)

        logger.warning(
            "[SD-SQL] load_drive_items called without any source_shared_drive_id — "
            "no items returned."
        )
        return []

    def load_items(
        self,
        source_email: str           = None,
        source_shared_drive_id: str = None,
        dest_shared_drive_id: str   = None,
    ) -> List[MigrationRecord]:
        """Convenience dispatcher — always routes to load_shared_drive_items()."""
        src_drv = _s(source_shared_drive_id) or self.source_shared_drive_id
        dst_drv = _s(dest_shared_drive_id)   or self.dest_shared_drive_id

        if src_drv:
            return self.load_shared_drive_items(src_drv, dst_drv or None)

        raise ValueError(
            "load_items() requires source_shared_drive_id for SharedDriveSQLStateManager."
        )

    # ─────────────────────────────────────────────────────────────────────────
    # migration_items  —  status transitions
    # ─────────────────────────────────────────────────────────────────────────

    def _set_status(self, file_id: str, status: str, error: str = ""):
        self._execute(
            """
            UPDATE migration_items
               SET status=%s, error_message=%s
             WHERE migration_id=%s AND file_id=%s
            """,
            (status, error[:65535], self.migration_id, file_id),
        )
        if file_id in self._cache:
            self._cache[file_id].status = status

    def mark_in_progress(self, run_id_or_file_id, file_id: str = None):
        fid = file_id if file_id is not None else run_id_or_file_id
        self._execute(
            "UPDATE migration_items SET status='IN_PROGRESS' "
            "WHERE migration_id=%s AND file_id=%s",
            (self.migration_id, fid),
        )
        if fid in self._cache:
            self._cache[fid].status = "IN_PROGRESS"

    def mark_done(
        self,
        run_id_or_file_id,
        file_id_or_dest_id=None,
        dest_item_id: str = None,
        dest_parent_id: str = None,
    ):
        if dest_item_id is not None:
            fid     = file_id_or_dest_id
            dest_id = dest_item_id
        else:
            fid     = run_id_or_file_id
            dest_id = file_id_or_dest_id

        self._execute(
            """
            UPDATE migration_items
               SET status='DONE',
                   dest_folder_id=COALESCE(%s, dest_folder_id),
                   migrated_at=NOW()
             WHERE migration_id=%s AND file_id=%s
            """,
            (dest_id, self.migration_id, fid),
        )
        if fid in self._cache:
            self._cache[fid].status = "DONE"
            if dest_id:
                self._cache[fid].dest_folder_id = dest_id
        self.update_run_counters(completed=1)

    def mark_success(self, file_id: str, dest_file_id: str = None):
        self.mark_done(file_id, dest_file_id)

    def mark_failed(
        self,
        run_id_or_file_id,
        file_id_or_error=None,
        error_message: str = "",
    ):
        if error_message:
            fid = file_id_or_error
            err = error_message
        else:
            fid = run_id_or_file_id
            err = file_id_or_error or ""

        self._execute(
            """
            UPDATE migration_items
               SET status='FAILED',
                   error_message=%s,
                   retry_count=retry_count+1
             WHERE migration_id=%s AND file_id=%s
            """,
            (str(err)[:65535], self.migration_id, fid),
        )
        if fid in self._cache:
            self._cache[fid].status = "FAILED"
        self.update_run_counters(failed=1)

    def mark_failure(self, file_id: str, error_message: str = ""):
        self.mark_failed(file_id, error_message)

    def mark_ignored(
        self,
        run_id_or_file_id,
        file_id_or_reason=None,
        reason: str = "",
    ):
        if reason:
            fid        = file_id_or_reason
            reason_str = reason
        else:
            fid        = run_id_or_file_id
            reason_str = file_id_or_reason or ""

        self._execute(
            """
            UPDATE migration_items
               SET status='IGNORED', error_message=%s
             WHERE migration_id=%s AND file_id=%s
            """,
            (str(reason_str)[:65535], self.migration_id, fid),
        )
        if fid in self._cache:
            self._cache[fid].status = "IGNORED"
        self.update_run_counters(ignored=1)

    def mark_skipped(self, file_id: str, reason: str = ""):
        self._execute(
            """
            UPDATE migration_items
               SET status='SKIPPED', error_message=%s
             WHERE migration_id=%s AND file_id=%s
            """,
            (reason[:65535], self.migration_id, file_id),
        )
        if file_id in self._cache:
            self._cache[file_id].status = "SKIPPED"
        self.update_run_counters(skipped=1)

    # ─────────────────────────────────────────────────────────────────────────
    # Summaries
    # ─────────────────────────────────────────────────────────────────────────

    def get_checkpoint_summary(self) -> Dict:
        row = self._one(
            """
            SELECT COUNT(*) AS total,
                   SUM(status='DONE')        AS done,
                   SUM(status='FAILED')      AS failed,
                   SUM(status='PENDING')     AS pending,
                   SUM(status='SKIPPED')     AS skipped,
                   SUM(status='IGNORED')     AS ignored,
                   SUM(status='IN_PROGRESS') AS in_progress
              FROM migration_items
             WHERE migration_id=%s AND drive_type='SHARED_DRIVE'
            """,
            (self.migration_id,),
        ) or {}
        return {
            "migration_id":  self.migration_id,
            "mode":          _DRIVE_TYPE,
            "total":         int(row.get("total") or 0),
            "done":          int(row.get("done") or 0),
            "failed":        int(row.get("failed") or 0),
            "pending":       int(row.get("pending") or 0),
            "skipped":       int(row.get("skipped") or 0),
            "ignored":       int(row.get("ignored") or 0),
            "in_progress":   int(row.get("in_progress") or 0),
            "completion_percentage": 0.0,
            "status_breakdown": {
                "pending": int(row.get("pending") or 0),
                "done":    int(row.get("done") or 0),
                "failed":  int(row.get("failed") or 0),
            },
        }

    def get_drive_checkpoint_summary(
        self,
        source_shared_drive_id: str = None,
        dest_shared_drive_id: str   = None,
    ) -> Dict:
        src_drv = _s(source_shared_drive_id) or self.source_shared_drive_id
        dst_drv = _s(dest_shared_drive_id)   or self.dest_shared_drive_id
        row = self._one(
            """
            SELECT COUNT(*) AS total,
                   SUM(status='DONE')        AS done,
                   SUM(status='FAILED')      AS failed,
                   SUM(status='PENDING')     AS pending,
                   SUM(status='SKIPPED')     AS skipped,
                   SUM(status='IGNORED')     AS ignored,
                   SUM(status='IN_PROGRESS') AS in_progress
              FROM migration_items
             WHERE migration_id=%s
               AND source_shared_drive_id=%s
               AND dest_shared_drive_id=%s
            """,
            (self.migration_id, src_drv, dst_drv),
        ) or {}
        return {k: int(v or 0) for k, v in row.items()} | {
            "migration_id":           self.migration_id,
            "source_shared_drive_id": src_drv,
            "dest_shared_drive_id":   dst_drv,
        }

    def get_sd_drive_summary(self, source_drive_id: str) -> Dict:
        """
        Return status-breakdown counts for a single source Shared Drive.
        Useful for the /api/shared-drive/discovery/status endpoint and
        post-migration reporting.
        """
        src_drv = _s(source_drive_id)
        if not src_drv:
            raise ValueError("get_sd_drive_summary: source_drive_id must be non-empty")

        row = self._one(
            """
            SELECT
                COUNT(*)                     AS total,
                SUM(status = 'PENDING')      AS pending,
                SUM(status = 'DONE')         AS done,
                SUM(status = 'FAILED')       AS failed,
                SUM(status = 'SKIPPED')      AS skipped,
                SUM(status = 'IGNORED')      AS ignored,
                SUM(status = 'IN_PROGRESS')  AS in_progress,
                SUM(is_folder = 0)           AS total_files,
                SUM(is_folder = 1)           AS total_folders,
                SUM(file_size)               AS total_size_bytes
              FROM migration_items
             WHERE migration_id           = %s
               AND source_shared_drive_id = %s
            """,
            (self.migration_id, src_drv),
        ) or {}

        return {
            "source_drive_id":  src_drv,
            "migration_id":     self.migration_id,
            "total":            int(row.get("total")            or 0),
            "pending":          int(row.get("pending")          or 0),
            "done":             int(row.get("done")             or 0),
            "failed":           int(row.get("failed")           or 0),
            "skipped":          int(row.get("skipped")          or 0),
            "ignored":          int(row.get("ignored")          or 0),
            "in_progress":      int(row.get("in_progress")      or 0),
            "total_files":      int(row.get("total_files")      or 0),
            "total_folders":    int(row.get("total_folders")    or 0),
            "total_size_bytes": int(row.get("total_size_bytes") or 0),
        }

    def print_summary(self):
        s = self.get_checkpoint_summary()
        logger.info(
            f"[SD-SQL] id={s['migration_id']} mode={s['mode']} "
            f"total={s['total']} done={s['done']} failed={s['failed']} "
            f"pending={s['pending']} ignored={s['ignored']}"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # migration_permissions
    # ─────────────────────────────────────────────────────────────────────────

    def upsert_permission(
        self,
        file_id: str,
        item_type: str,
        permission_type: str,
        source_email: str,
        dest_email: str,
        role: str,
        classification: str,
        is_inherited: bool = False,
        parent_drive_id: str = None,
    ):
        self._execute(
            """
            INSERT IGNORE INTO migration_permissions
                (migration_id, file_id, item_type, parent_drive_id,
                permission_type, source_email, destination_email,
                role, classification, is_inherited, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PENDING')
            """,
            (
                self.migration_id, file_id, item_type,
                parent_drive_id or "",
                permission_type, source_email or "", dest_email or "",
                role, classification, int(is_inherited),
            ),
        )

    def mark_permission_done(self, file_id: str, dest_email: str, role: str):
        self._execute(
            """
            UPDATE migration_permissions
               SET status='DONE', updated_at=NOW()
             WHERE migration_id=%s AND file_id=%s
               AND destination_email=%s AND role=%s
            """,
            (self.migration_id, file_id, dest_email, role),
        )

    def mark_permission_failed(self, file_id: str, dest_email: str, role: str, error: str):
        self._execute(
            """
            UPDATE migration_permissions
               SET status='FAILED',
                   error_message=%s,
                   retry_count=retry_count+1,
                   updated_at=NOW()
             WHERE migration_id=%s AND file_id=%s
               AND destination_email=%s AND role=%s
            """,
            (error[:65535], self.migration_id, file_id, dest_email, role),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # shared_drive_members
    # ─────────────────────────────────────────────────────────────────────────

    def upsert_shared_drive_member(
        self,
        source_drive_id: str,
        dest_drive_id: str,
        member_email: str,
        member_type: str,
        role: str,
    ):
        if role == "owner":
            logger.debug(f"Skipping 'owner' role for {member_email} — not in ENUM")
            return
        if role not in self._SHARED_DRIVE_MEMBER_ROLES:
            logger.warning(f"Unknown shared-drive role '{role}' — skipping")
            return

        src_drv = _s(source_drive_id)
        dst_drv = _s(dest_drive_id)
        if not src_drv or not dst_drv:
            raise ValueError(
                f"upsert_shared_drive_member needs non-empty drive IDs. "
                f"src={source_drive_id!r} dst={dest_drive_id!r}"
            )

        self._execute(
            """
            INSERT IGNORE INTO shared_drive_members
                (migration_id, source_drive_id, dest_shared_drive_id,
                 member_email, member_type, role, status)
            VALUES (%s,%s,%s,%s,%s,%s,'PENDING')
            """,
            (self.migration_id, src_drv, dst_drv, member_email, member_type, role),
        )

    def mark_member_done(self, dest_drive_id: str, member_email: str, role: str):
        self._execute(
            """
            UPDATE shared_drive_members
               SET status='DONE'
             WHERE migration_id=%s AND dest_shared_drive_id=%s
               AND member_email=%s AND role=%s
            """,
            (self.migration_id, dest_drive_id, member_email, role),
        )

    def mark_member_failed(self, dest_drive_id: str, member_email: str, role: str, error: str):
        self._execute(
            """
            UPDATE shared_drive_members
               SET status='FAILED', error_message=%s, retry_count=retry_count+1
             WHERE migration_id=%s AND dest_shared_drive_id=%s
               AND member_email=%s AND role=%s
            """,
            (error[:65535], self.migration_id, dest_drive_id, member_email, role),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # GCS staging helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _blob_name(self, run_id, file_id: str, suffix: str = "") -> str:
        return f"{self.gcs_prefix}{run_id}/{file_id}{suffix}"

    def should_use_gcs(self, file_size: int) -> bool:
        return file_size >= self.large_file_threshold_bytes

    def download_drive_to_gcs(
        self,
        drive_svc,
        file_id: str,
        file_name: str,
        run_id,
        mime_type: str,
        export_mime: str = None,
        chunk_size_bytes: int = 20 * 1024 * 1024,
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Download a Drive file chunk-by-chunk and stream each chunk directly
        into a GCS resumable upload. Peak RAM = one chunk (~20 MB).

        Returns (ok, blob_name, error_message).
        """
        suffix = ""
        if export_mime:
            for ws_mime, (exp_mime, ext, _) in GOOGLE_WORKSPACE_EXPORT.items():
                if exp_mime == export_mime:
                    suffix = ext
                    break

        blob_name = self._blob_name(run_id, file_id, suffix)

        if self._bucket is None:
            return False, None, "GCS not initialised (discovery-only instance)"

        try:
            if export_mime:
                request = drive_svc.files().export_media(fileId=file_id, mimeType=export_mime)
            else:
                # FIX: acknowledgeAbuse=True prevents 403 on flagged files
                request = drive_svc.files().get_media(
                    fileId=file_id, supportsAllDrives=True, acknowledgeAbuse=True)

            blob         = self._bucket.blob(blob_name)
            content_type = export_mime or mime_type or "application/octet-stream"
            dl_start     = time.time()

            with blob.open("wb", content_type=content_type) as gcs_stream:
                chunk_buf = io.BytesIO()
                try:
                    dl = MediaIoBaseDownload(chunk_buf, request, chunksize=chunk_size_bytes)
                    done = False
                    while not done:
                        if time.time() - dl_start > 1800:
                            raise TimeoutError("Drive download timeout (30 min)")
                        _, done = dl.next_chunk()
                        # Flush chunk to GCS then clear buffer for next chunk
                        chunk_buf.seek(0)
                        gcs_stream.write(chunk_buf.read())
                        chunk_buf.seek(0)
                        chunk_buf.truncate(0)
                finally:
                    # FIX: always close chunk_buf — prevents 'NoneType'.close()
                    # on retry when httplib2 drops the connection mid-chunk
                    try:
                        chunk_buf.close()
                    except Exception:
                        pass
                    chunk_buf = None

            logger.debug(f"GCS ↑ {blob_name}")
            return True, blob_name, None

        except Exception as exc:
            err = str(exc)
            logger.warning(f"download_drive_to_gcs failed [{file_name}]: {err}")
            try:
                self._bucket.blob(blob_name).delete()
            except Exception:
                pass
            return False, None, err

    def upload_gcs_to_drive(
        self,
        drive_svc,
        blob_name: str,
        file_name: str,
        mime_type: str,
        parent_id: Optional[str],
        import_mime: str = None,
        chunk_size_bytes: int = 20 * 1024 * 1024,
        drive_id: Optional[str] = None,
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Stream a GCS blob to destination Drive via manual resumable upload.
        Peak RAM = one chunk (~20 MB).

        Args:
            parent_id:  Destination folder ID. Pass None when the file belongs
                        at the Shared Drive root — do NOT pass the drive ID here
                        as it is not a folder and causes HTTP 404.
            drive_id:   Shared Drive ID. When parent_id is None, this is added
                        to the upload initiation URL as ?driveId= so the file
                        lands at the correct drive root instead of My Drive.

        Returns (ok, dest_file_id, error_message).
        """
        if self._bucket is None:
            return False, None, "GCS not initialised (discovery-only instance)"

        try:
            import json as _json
            from google.auth.transport.requests import AuthorizedSession

            blob = self._bucket.blob(blob_name)
            blob.reload()
            total_size = blob.size or 0

            meta = {"name": file_name}
            if parent_id:
                # parent_id is a real folder ID — include it as the parent.
                meta["parents"] = [parent_id]
            # When parent_id is None, "parents" is intentionally omitted entirely.
            # The raw resumable-upload endpoint (/upload/drive/v3/files?uploadType=resumable)
            # resolves every entry in parents[] as a *folder* ID.  A Shared Drive ID is a
            # *drive* container, not a folder — passing it here yields:
            #   HTTP 404 "File not found: <driveId>"
            # The correct mechanism for drive-root placement is the driveId= query
            # parameter on the initiation URL (see _drive_param below), which the
            # upload API understands as a Shared Drive target rather than a folder.
            if import_mime:
                meta["mimeType"] = import_mime

            try:
                credentials = drive_svc._http.credentials
            except AttributeError:
                credentials = drive_svc._http.http.credentials

            session = AuthorizedSession(credentials)

            # driveId in the URL routes the file to the correct Shared Drive root
            # when no folder parent exists.  Only appended when parent_id is absent —
            # if a real folder parent is present, the drive is inferred from it.
            _drive_param = f"&driveId={drive_id}" if drive_id and not parent_id else ""
            init_url = (
                "https://www.googleapis.com/upload/drive/v3/files"
                f"?uploadType=resumable&supportsAllDrives=true&fields=id{_drive_param}"
            )
            init_resp = session.post(
                init_url,
                headers={
                    "Content-Type":            "application/json; charset=UTF-8",
                    "X-Upload-Content-Type":   mime_type,
                    "X-Upload-Content-Length": str(total_size),
                },
                data=_json.dumps(meta).encode("utf-8"),
                allow_redirects=False,
            )

            if init_resp.status_code not in (200, 201):
                raise RuntimeError(
                    f"Resumable upload initiation failed: "
                    f"HTTP {init_resp.status_code} — {init_resp.text[:300]}"
                )

            session_uri = init_resp.headers.get("Location")
            if not session_uri:
                raise RuntimeError(
                    f"Resumable upload: no Location header. "
                    f"Response headers: {dict(init_resp.headers)}"
                )

            dest_file_id = None
            offset       = 0

            with blob.open("rb") as gcs_stream:
                while offset < total_size:
                    chunk = gcs_stream.read(chunk_size_bytes)
                    if not chunk:
                        break

                    end       = offset + len(chunk) - 1
                    total_str = str(total_size) if total_size > 0 else "*"

                    chunk_resp = session.put(
                        session_uri,
                        headers={
                            "Content-Range": f"bytes {offset}-{end}/{total_str}",
                            "Content-Type":  mime_type,
                        },
                        data=chunk,
                        allow_redirects=False,
                    )
                    code = chunk_resp.status_code

                    if code in (200, 201):
                        body = chunk_resp.json() if chunk_resp.content else {}
                        dest_file_id = body.get("id")
                        break
                    elif code == 308:
                        confirmed = chunk_resp.headers.get("Range", "")
                        offset = int(confirmed.split("-")[-1]) + 1 if confirmed else end + 1
                    else:
                        raise RuntimeError(
                            f"Chunk PUT failed at offset {offset}: "
                            f"HTTP {code} — {chunk_resp.text[:300]}"
                        )

            if not dest_file_id:
                raise RuntimeError(
                    f"Upload loop ended without a file id (offset={offset}, total={total_size})"
                )

            return True, dest_file_id, None

        except Exception as exc:
            err = str(exc)
            logger.warning(f"upload_gcs_to_drive failed [{file_name}]: {err}")
            return False, None, err

    def delete_temp(self, blob_name: str):
        if self._bucket is None:
            logger.debug("delete_temp: GCS not initialised — skipping")
            return
        try:
            self._bucket.blob(blob_name).delete()
            logger.debug(f"GCS ✗ {blob_name}")
        except Exception as exc:
            logger.warning(f"GCS delete failed [{blob_name}]: {exc}")

    def cleanup_run_temps(self, run_id):
        if self._bucket is None:
            logger.debug("cleanup_run_temps: GCS not initialised — skipping")
            return
        prefix = f"{self.gcs_prefix}{run_id}/"
        blobs  = list(self._bucket.list_blobs(prefix=prefix))
        if blobs:
            self._bucket.delete_blobs(blobs)
            logger.info(f"GCS cleanup: {len(blobs)} blobs deleted for run={run_id}")
        else:
            logger.info(f"GCS cleanup: nothing to delete for run={run_id}")

    # ── Low-level GCS primitives (backward compat) ────────────────────────────

    def _gcs_key(self, file_id: str) -> str:
        return f"{self.gcs_prefix}{self.migration_id}/{file_id}"

    def upload_to_gcs(self, file_id: str, data: bytes,
                      content_type: str = "application/octet-stream") -> str:
        key  = self._gcs_key(file_id)
        blob = self._bucket.blob(key)
        blob.upload_from_string(data, content_type=content_type)
        return f"gs://{self.gcs_bucket_name}/{key}"

    def download_from_gcs(self, file_id: str) -> bytes:
        return self._bucket.blob(self._gcs_key(file_id)).download_as_bytes()

    def download_stream_from_gcs(self, file_id: str) -> io.BytesIO:
        return io.BytesIO(self.download_from_gcs(file_id))

    def delete_from_gcs(self, file_id: str):
        self.delete_temp(self._gcs_key(file_id))

    def cleanup_gcs_for_migration(self):
        self.cleanup_run_temps(self.migration_id)
