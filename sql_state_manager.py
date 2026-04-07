"""
sql_state_manager.py  (v4 – schema-accurate, dual-engine)
──────────────────────────────────────────────────────────
Fixes vs v3:
  1. My Drive   engine → source_shared_drive_id = '', dest_shared_drive_id = ''
                         source_user_email / destination_user_email = real emails
  2. Shared Drive engine → source_shared_drive_id / dest_shared_drive_id = real IDs
                           source_user_email = ''  (NOT NULL → empty string, not NULL)
                           destination_user_email = ''
  3. load_user_items()  filters by source_user_email  (My Drive)
  4. load_drive_items() filters by source_shared_drive_id + dest_shared_drive_id
  5. get_pending_items() added — used by migration_engine.py
  6. get_folder_mapping() added — used by migration_engine.py
  7. bulk_register_items() added — alias expected by migration_engine.py
  8. start_user() / finish_user() added — run-level user tracking
  9. mark_done() / mark_ignored() signatures aligned with migration_engine.py calls
 10. get_item_status() added — used by migration_engine.py resume guard
 11. upsert_shared_drive() / finish_shared_drive() added for DomainMigrationEngine
 12. upsert_user() added for DomainMigrationEngine
 13. GCS helpers: download_drive_to_gcs / upload_gcs_to_drive / delete_temp /
     cleanup_run_temps — interface expected by FileMigrator / migration_engine.py
 14. All NOT NULL columns always filled; no None passed where '' is required
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


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _s(value) -> str:
    """
    Safe string: converts None / whitespace / literal 'None' → empty string ''.
    Used to satisfy NOT NULL VARCHAR columns that have no meaningful value.
    """
    if value is None:
        return ""
    s = str(value).strip()
    return "" if s.lower() == "none" else s


def _is_shared(src_drv: str, dst_drv: str) -> bool:
    """True only when BOTH drive IDs are non-empty."""
    return bool(src_drv and dst_drv)


# ─────────────────────────────────────────────────────────────────────────────
# DTO
# ─────────────────────────────────────────────────────────────────────────────

class MigrationRecord:
    __slots__ = (
        "file_id", "file_name", "mime_type", "file_size",
        "parent_id", "dest_folder_id", "status",
        "source_user_email", "dest_user_email",
        "drive_type", "source_shared_drive_id", "dest_shared_drive_id",
        # aliases used by migration_engine.py
        "source_item_id", "source_item_name", "source_parent_id",
        "file_size_bytes", "item_type",
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
        self.drive_type             = row.get("drive_type") or "MY_DRIVE"
        self.source_shared_drive_id = _s(row.get("source_shared_drive_id"))
        self.dest_shared_drive_id   = _s(row.get("dest_shared_drive_id"))

        # migration_engine.py reads these field names from pending-item dicts
        self.source_item_id   = self.file_id
        self.source_item_name = self.file_name
        self.source_parent_id = self.parent_id
        self.file_size_bytes  = self.file_size
        is_folder = (self.mime_type == "application/vnd.google-apps.folder")
        self.item_type = "folder" if is_folder else "file"

    def to_dict(self) -> dict:
        """Return dict compatible with migration_engine pending-item format."""
        return {
            "source_item_id":   self.file_id,
            "source_item_name": self.file_name,
            "mime_type":        self.mime_type,
            "file_size_bytes":  self.file_size,
            "source_parent_id": self.parent_id,
        }


# ─────────────────────────────────────────────────────────────────────────────
# MIME constants
# ─────────────────────────────────────────────────────────────────────────────

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
    "application/vnd.google-apps.drawing": (
        "image/svg+xml", ".svg", None,
    ),
    "application/vnd.google-apps.jam": (
        "application/pdf", ".pdf", None,
    ),
    "application/vnd.google-apps.map": (
        "application/vnd.google-earth.kmz", ".kmz", None,
    ),
}


# ─────────────────────────────────────────────────────────────────────────────
# Main class
# ─────────────────────────────────────────────────────────────────────────────

class SQLStateManager:
    """
    Unified checkpoint + GCS helper for:
      • migration_engine.py      (My Drive)
      • SharedDriveMigrator.py   (Shared Drive)

    Key invariant (schema NOT NULL enforcement):
      My Drive items    → source_user_email='alice@src', source_shared_drive_id=''
      Shared Drive items→ source_user_email='',           source_shared_drive_id='drv_abc'

    Constructor
    ───────────
    db_config               : dict  mysql.connector kwargs (host/port/database/user/password)
    gcs_bucket              : str
    gcs_key_file            : str   path to service-account JSON
    source_domain           : str
    dest_domain             : str
    migration_id            : str   optional; auto UUID4 if omitted
    gcs_prefix              : str
    large_file_threshold_mb : int   files >= this go through GCS
    source_shared_drive_id  : str   leave None/'' for My Drive migrations
    dest_shared_drive_id    : str   leave None/'' for My Drive migrations
    """

    DEFAULT_PREFIX       = "migration-staging/"
    LARGE_FILE_THRESHOLD = 50   # MB
    _SHARED_DRIVE_MEMBER_ROLES = frozenset({
        "organizer", "fileOrganizer", "writer", "commenter", "reader",
    })

    def __init__(
        self,
        db_config: Dict,
        gcs_bucket: str,
        gcs_key_file: str,
        source_domain: str,
        dest_domain: str,
        migration_id: str = None,
        gcs_prefix: str = DEFAULT_PREFIX,
        large_file_threshold_mb: int = LARGE_FILE_THRESHOLD,
        source_shared_drive_id: str = None,
        dest_shared_drive_id: str = None,
    ):
        self.migration_id    = migration_id or str(uuid.uuid4())
        self.source_domain   = source_domain
        self.dest_domain     = dest_domain
        self.gcs_bucket_name = gcs_bucket
        self.gcs_prefix      = gcs_prefix
        self.large_file_threshold_bytes = large_file_threshold_mb * 1024 * 1024

        # Normalise — always strings, never None
        self.source_shared_drive_id = _s(source_shared_drive_id)
        self.dest_shared_drive_id   = _s(dest_shared_drive_id)

        self._shared_drive_mode = _is_shared(
            self.source_shared_drive_id, self.dest_shared_drive_id
        )
        self._drive_type = "SHARED_DRIVE" if self._shared_drive_mode else "MY_DRIVE"

        logger.info(
            f"SQLStateManager init | id={self.migration_id} | mode={self._drive_type}"
            + (
                f" | src_drv={self.source_shared_drive_id}"
                f" | dst_drv={self.dest_shared_drive_id}"
                if self._shared_drive_mode else
                f" | {source_domain} → {dest_domain}"
            )
        )

        # ── MySQL connection pool ──────────────────────────────────────────
        # NEW — pool size = (max_users × parallel_files) + headroom
        # With 10 users × 5 files + folder threads = needs ~60; cap at MySQL's safe limit
        self._pool = pooling.MySQLConnectionPool(
            pool_name=f"migration_pool_{uuid.uuid4().hex[:8]}",  # unique name avoids pool name collision on re-init
            pool_size=32,          # safe ceiling: MySQL default max_connections=151
            pool_reset_session=True,
            connection_timeout=30, # wait up to 30s for a free connection before raising
            **db_config,
        )
        logger.info(
            f"SQL pool → {db_config.get('host')}:{db_config.get('port', 3306)}"
            f"/{db_config.get('database')}"
        )

        # ── GCS client ────────────────────────────────────────────────────
        creds = service_account.Credentials.from_service_account_file(
            gcs_key_file,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self._gcs    = gcs_storage.Client(credentials=creds, project=creds.project_id)
        self._bucket = self._gcs.bucket(gcs_bucket)
        logger.info(f"GCS client → bucket={gcs_bucket}")

        # ── In-memory cache ───────────────────────────────────────────────
        self._cache: Dict[str, MigrationRecord] = {}

        # Compat alias used by old code that reads checkpoint.checkpoint_file
        self.checkpoint_file = f"sql://{db_config.get('host')}/{self.migration_id}"

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def is_shared_drive_mode(self) -> bool:
        return self._shared_drive_mode

    # ─────────────────────────────────────────────────────────────────────────
    # DB helpers
    # ─────────────────────────────────────────────────────────────────────────

    @contextmanager
    def _conn(self, retries: int = 6, wait: float = 0.5):
        """
        Get a connection from the pool with retry backoff.
        Handles transient 'pool exhausted' without crashing the migration.
        """
        last_exc = None
        for attempt in range(retries):
            try:
                conn = self._pool.get_connection()
                try:
                    yield conn
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    conn.close()
                return
            except Exception as exc:
                last_exc = exc
                if "pool exhausted" in str(exc).lower() or "PoolError" in type(exc).__name__:
                    sleep_time = wait * (2 ** attempt)   # 0.5, 1, 2, 4, 8, 16s
                    logger.warning(
                        f"SQL pool exhausted (attempt {attempt+1}/{retries}) "
                        f"— retrying in {sleep_time:.1f}s"
                    )
                    time.sleep(sleep_time)
                else:
                    raise   # non-pool errors fail immediately
        raise last_exc

    def _execute(self, sql: str, params=(), many: bool = False) -> List[Dict]:
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
        """
        Return a raw connection from the pool.
        Used by EnhancedPermissionsMigrator(get_conn=mgr.get_conn).
        Caller is responsible for closing.
        """
        return self._pool.get_connection()

    # ─────────────────────────────────────────────────────────────────────────
    # migration_runs
    # ─────────────────────────────────────────────────────────────────────────

    def create_migration_run(self, total_items: int = 0) -> str:
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
        logger.info(f"migration_run ready: {self.migration_id}")
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
        """
        Atomically increment migration_runs counters.
        Accepted keys: completed | failed | skipped | ignored | size_bytes
        """
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

    # ── User-level run tracking (migration_engine.py interface) ──────────────

    def start_user(self, run_id, source_email: str):
        """Called by migration_engine.py at the start of each user."""
        logger.debug(f"[run={run_id}] start_user: {source_email}")
        # Extend: could upsert a user-level table here if needed

    def finish_user(
        self,
        run_id,
        source_email: str,
        status: str,
        files_done: int = 0,
        files_failed: int = 0,
        bytes_moved: int = 0,
    ):
        """Called by migration_engine.py when a user finishes."""
        logger.info(
            f"[run={run_id}] finish_user: {source_email} | "
            f"status={status} | done={files_done} | failed={files_failed}"
        )
        self.update_run_counters(
            completed=files_done,
            failed=files_failed,
            size_bytes=bytes_moved,
        )

    # ── Shared Drive run tracking (DomainMigrationEngine interface) ───────────

    def upsert_user(self, run_id, source_email: str, dest_email: str):
        """Called by DomainMigrationEngine before migrating a user."""
        logger.debug(f"[run={run_id}] upsert_user: {source_email} → {dest_email}")

    def upsert_shared_drive(self, run_id, drive_id: str, drive_name: str):
        """Called by DomainMigrationEngine / UserMigrator for each shared drive."""
        logger.debug(f"[run={run_id}] upsert_shared_drive: {drive_name} ({drive_id})")

    def finish_shared_drive(
        self,
        run_id,
        drive_id: str,
        status: str,
        files_total: int = 0,
        files_done: int = 0,
    ):
        logger.info(
            f"[run={run_id}] finish_shared_drive: {drive_id} | "
            f"status={status} | total={files_total} | done={files_done}"
        )

    def finish_run(self, run_id, status: str):
        self.finish_migration_run(status.upper())

    # ─────────────────────────────────────────────────────────────────────────
    # migration_items  —  registration
    # ─────────────────────────────────────────────────────────────────────────

    def register_discovered_items(
        self,
        items: List[Dict],
        source_email: str,
        dest_email: str,
        source_shared_drive_id: str = None,
        dest_shared_drive_id: str   = None,
    ):
        """
        Bulk-insert discovered files/folders.

        MY DRIVE call:
            mgr.register_discovered_items(items,
                source_email="alice@src.com", dest_email="alice@dst.com")
            → source_user_email='alice@src.com'
            → source_shared_drive_id=''   ← NOT NULL satisfied

        SHARED DRIVE call:
            mgr.register_discovered_items(items,
                source_email="svc@src.com", dest_email="svc@dst.com",
                source_shared_drive_id="drv_abc", dest_shared_drive_id="drv_xyz")
            → source_user_email=''        ← NOT NULL satisfied with empty string
            → source_shared_drive_id='drv_abc'

        INSERT IGNORE → safe for resume (won't overwrite existing rows).
        """
        if not items:
            return

        # Resolve which scope we're inserting for
        src_drv = _s(source_shared_drive_id) or self.source_shared_drive_id
        dst_drv = _s(dest_shared_drive_id)   or self.dest_shared_drive_id
        shared  = _is_shared(src_drv, dst_drv)
        drive_type = "SHARED_DRIVE" if shared else "MY_DRIVE"

        # FIX: for each engine, the "other" identifier column is empty string ''
        if shared:
            # Shared Drive engine: drive IDs are real, email is ''
            db_src_email = ""
            db_dst_email = ""
            db_src_drv   = src_drv
            db_dst_drv   = dst_drv
        else:
            # My Drive engine: emails are real, drive IDs are ''
            db_src_email = source_email
            db_dst_email = dest_email
            db_src_drv   = ""
            db_dst_drv   = ""

        rows = []
        for item in items:
            mime    = item.get("mimeType") or item.get("mime_type") or ""
            status  = "IGNORED" if mime in IGNORED_MIME_TYPES else "PENDING"
            parents = item.get("parents", [])
            # Support both Drive API format (parents list) and internal format (source_parent_id)
            parent_id = (
                item.get("source_parent_id")
                or (parents[0] if parents else None)
            )
            fid = item.get("id") or item.get("source_item_id") or ""
            fname = item.get("name") or item.get("source_item_name") or ""
            fsize = int(item.get("size") or item.get("file_size_bytes") or 0)
            is_fld = 1 if mime == "application/vnd.google-apps.folder" else 0

            rows.append((
                self.migration_id,   # migration_id
                self.source_domain,  # source_domain
                self.dest_domain,    # destination_domain
                drive_type,          # drive_type ENUM
                db_src_drv,          # source_shared_drive_id  ('' for My Drive)
                db_dst_drv,          # dest_shared_drive_id    ('' for My Drive)
                db_src_email,        # source_user_email       ('' for Shared Drive)
                db_dst_email,        # destination_user_email  ('' for Shared Drive)
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

        # Refresh in-memory cache
        for item, row in zip(items, rows):
            fid = row[8]   # file_id position in tuple
            self._cache[fid] = MigrationRecord({
                "file_id":                fid,
                "file_name":              row[9],
                "mime_type":              row[11],
                "file_size":              row[14],
                "parent_id":              row[10],
                "dest_folder_id":         None,
                "status":                 row[13],
                "source_user_email":      row[6],
                "destination_user_email": row[7],
                "drive_type":             drive_type,
                "source_shared_drive_id": row[4],
                "dest_shared_drive_id":   row[5],
            })

        logger.info(
            f"Registered {len(rows)} items | {drive_type} | "
            + (
                f"drv_src={db_src_drv} drv_dst={db_dst_drv}"
                if shared else
                f"src={source_email} dst={dest_email}"
            )
        )

    def bulk_register_items(self, run_id, items: List[Dict]):
        """
        Alias used by migration_engine.py (DomainMigrationEngine / UserMigrator).
        Items are in internal dict format with source_item_id, source_item_name etc.
        Resolves email from item["source_email"] field.
        """
        if not items:
            return
        # Group by (source_email, dest_email) — items from different users possible
        from collections import defaultdict
        groups: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
        for item in items:
            src_email = item.get("source_email") or item.get("source_user_email") or ""
            dst_email = item.get("dest_email")   or item.get("destination_user_email") or ""
            groups[(src_email, dst_email)].append(item)

        for (src_email, dst_email), group in groups.items():
            self.register_discovered_items(
                group,
                source_email=src_email,
                dest_email=dst_email,
                # No drive IDs → My Drive scope
            )

    # ─────────────────────────────────────────────────────────────────────────
    # migration_items  —  folder mapping
    # ─────────────────────────────────────────────────────────────────────────

    def register_folder_mapping(
        self,
        run_id_or_file_id,      # overloaded: (run_id, src_id) OR (src_id,) — see below
        dest_folder_id_or_src=None,
        dest_folder_id_last=None,
    ):
        """
        Two call signatures supported:
          Old (v3):  register_folder_mapping(source_folder_id, dest_folder_id)
          New (v4):  register_folder_mapping(run_id, source_folder_id, dest_folder_id)
        """
        if dest_folder_id_last is not None:
            # New 3-arg form: (run_id, src_id, dest_id)
            source_folder_id = dest_folder_id_or_src
            dest_folder_id   = dest_folder_id_last
        else:
            # Old 2-arg form: (src_id, dest_id)
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

    def get_folder_mapping(
        self, run_id, source_email: str = None
    ) -> Dict[str, str]:
        """
        Return {source_folder_id: dest_folder_id} for all folders that already
        have a dest_folder_id (i.e. were successfully created on a previous run).

        Used by migration_engine.py resume path.
        Filters by source_user_email for My Drive; by migration_id only for Shared Drive.
        """
        if source_email:
            rows = self._execute(
                """
                SELECT file_id, dest_folder_id
                  FROM migration_items
                 WHERE migration_id      = %s
                   AND source_user_email = %s
                   AND is_folder         = 1
                   AND dest_folder_id IS NOT NULL
                """,
                (self.migration_id, source_email),
            )
        else:
            rows = self._execute(
                """
                SELECT file_id, dest_folder_id
                  FROM migration_items
                 WHERE migration_id    = %s
                   AND is_folder       = 1
                   AND dest_folder_id IS NOT NULL
                """,
                (self.migration_id,),
            )
        return {r["file_id"]: r["dest_folder_id"] for r in rows}

    # ─────────────────────────────────────────────────────────────────────────
    # migration_items  —  status queries
    # ─────────────────────────────────────────────────────────────────────────

    def get_item_status(self, run_id, file_id: str) -> Optional[str]:
        """
        Return the current status string for a file_id, or None if not found.
        Checks in-memory cache first.
        """
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

    def get_pending_items(
        self,
        run_id,
        source_email: str = None,
        item_types: tuple = ("file",),
    ) -> List[Dict]:
        """
        Return items that are PENDING or FAILED (for retry) as dicts.
        Called by migration_engine.py to get the work queue.

        item_types: tuple of "file" | "folder" | "shared_drive_file" | "shared_drive_folder"
        Anything containing "folder" → is_folder=1, else is_folder=0.
        """
        # Determine is_folder filter
        wants_folders = any("folder" in t for t in item_types)
        wants_files   = any("folder" not in t for t in item_types)

        if wants_folders and wants_files:
            folder_filter = ""         # no is_folder restriction
            folder_params: tuple = ()
        elif wants_folders:
            folder_filter = "AND is_folder = 1"
            folder_params = ()
        else:
            folder_filter = "AND is_folder = 0"
            folder_params = ()

        if source_email:
            # FIX: My Drive engine filters by source_user_email
            rows = self._execute(
                f"""
                SELECT file_id, file_name, mime_type, file_size,
                       parent_id, dest_folder_id, status,
                       source_user_email, destination_user_email,
                       drive_type, source_shared_drive_id, dest_shared_drive_id
                  FROM migration_items
                 WHERE migration_id      = %s
                   AND source_user_email = %s
                   AND status IN ('PENDING', 'FAILED')
                   {folder_filter}
                """,
                (self.migration_id, source_email) + folder_params,
            )
        else:
            # FIX: Shared Drive engine — no user email filter
            rows = self._execute(
                f"""
                SELECT file_id, file_name, mime_type, file_size,
                       parent_id, dest_folder_id, status,
                       source_user_email, destination_user_email,
                       drive_type, source_shared_drive_id, dest_shared_drive_id
                  FROM migration_items
                 WHERE migration_id = %s
                   AND status IN ('PENDING', 'FAILED')
                   {folder_filter}
                """,
                (self.migration_id,) + folder_params,
            )

        # Cache and return as dicts compatible with migration_engine.py
        result = []
        for row in rows:
            rec = MigrationRecord(row)
            self._cache[rec.file_id] = rec
            result.append(rec.to_dict())
        return result

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

    # ─────────────────────────────────────────────────────────────────────────
    # migration_items  —  status transitions
    # ─────────────────────────────────────────────────────────────────────────

    def _set_status(self, file_id: str, status: str, error: str = ""):
        self._execute(
            """
            UPDATE migration_items
               SET status=%, error_message=%s
             WHERE migration_id=%s AND file_id=%s
            """.replace("=%,", "=%s,"),   # avoid f-string for %
            (status, error[:65535], self.migration_id, file_id),
        )
        if file_id in self._cache:
            self._cache[file_id].status = status

    def mark_in_progress(self, run_id_or_file_id, file_id: str = None):
        """
        Supports two signatures:
          migration_engine.py:   mark_in_progress(run_id, file_id)
          legacy / direct:       mark_in_progress(file_id)
        """
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
        """
        Supports:
          migration_engine.py:  mark_done(run_id, file_id, dest_item_id=x, dest_parent_id=y)
          legacy:               mark_done(file_id, dest_file_id)
        """
        if dest_item_id is not None:
            # New signature: mark_done(run_id, file_id, dest_item_id=, dest_parent_id=)
            fid     = file_id_or_dest_id
            dest_id = dest_item_id
        else:
            # Old signature: mark_done(file_id, dest_file_id)
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

    # Alias: mark_success used by SharedDriveMigrator / older code
    def mark_success(self, file_id: str, dest_file_id: str = None):
        self.mark_done(file_id, dest_file_id)

    def mark_failed(
        self,
        run_id_or_file_id,
        file_id_or_error=None,
        error_message: str = "",
    ):
        """
        Supports:
          migration_engine.py:  mark_failed(run_id, file_id, error_message)
          legacy:               mark_failed(file_id, error_message)
        """
        if error_message:
            # New 3-arg: mark_failed(run_id, file_id, error_message)
            fid = file_id_or_error
            err = error_message
        else:
            # Old 2-arg: mark_failed(file_id, error_str)
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

    # Alias for SharedDriveMigrator
    def mark_failure(self, file_id: str, error_message: str = ""):
        self.mark_failed(file_id, error_message)

    def mark_ignored(
        self,
        run_id_or_file_id,
        file_id_or_reason=None,
        reason: str = "",
    ):
        """
        Supports:
          migration_engine.py:  mark_ignored(run_id, file_id, reason)
          legacy:               mark_ignored(file_id, reason)
        """
        if reason:
            fid    = file_id_or_reason
            reason_str = reason
        else:
            fid    = run_id_or_file_id
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
    # Cache loading (resume)
    # ─────────────────────────────────────────────────────────────────────────

    def _load_cache(self, rows: List[Dict]):
        for row in rows:
            self._cache[row["file_id"]] = MigrationRecord(row)

    _SELECT_COLS = """
        SELECT file_id, file_name, mime_type, file_size,
               parent_id, dest_folder_id, status,
               source_user_email, destination_user_email,
               drive_type, source_shared_drive_id, dest_shared_drive_id
          FROM migration_items
    """

    def load_user_items(self, source_email: str) -> List[MigrationRecord]:
        """
        MY DRIVE engine: load all items for a specific user email.
        FIX: filters on source_user_email (not shared_drive_id).
        """
        if self._shared_drive_mode:
            logger.warning("load_user_items() called in SHARED_DRIVE mode")

        rows = self._execute(
            self._SELECT_COLS +
            " WHERE migration_id=%s AND source_user_email=%s",
            (self.migration_id, source_email),
        )
        self._load_cache(rows)
        logger.debug(f"load_user_items: {len(rows)} records for {source_email}")
        return [self._cache[r["file_id"]] for r in rows]

    def load_drive_items(
        self,
        source_shared_drive_id: str = None,
        dest_shared_drive_id: str   = None,
    ) -> List[MigrationRecord]:
        """
        SHARED DRIVE engine: load all items for a specific drive pair.
        FIX: filters on source_shared_drive_id + dest_shared_drive_id.
        """
        if not self._shared_drive_mode:
            logger.warning("load_drive_items() called in MY_DRIVE mode")

        src_drv = _s(source_shared_drive_id) or self.source_shared_drive_id
        dst_drv = _s(dest_shared_drive_id)   or self.dest_shared_drive_id

        if not src_drv or not dst_drv:
            raise ValueError(
                f"load_drive_items requires drive IDs. Got src={src_drv!r} dst={dst_drv!r}"
            )

        rows = self._execute(
            self._SELECT_COLS +
            " WHERE migration_id=%s"
            "   AND source_shared_drive_id=%s"
            "   AND dest_shared_drive_id=%s",
            (self.migration_id, src_drv, dst_drv),
        )
        self._load_cache(rows)
        logger.debug(f"load_drive_items: {len(rows)} records for {src_drv}")
        return list(self._cache.values())

    def load_items(
        self,
        source_email: str           = None,
        source_shared_drive_id: str = None,
        dest_shared_drive_id: str   = None,
    ) -> List[MigrationRecord]:
        """Convenience dispatcher — routes to the correct load method."""
        src_drv = _s(source_shared_drive_id) or self.source_shared_drive_id
        dst_drv = _s(dest_shared_drive_id)   or self.dest_shared_drive_id

        if _is_shared(src_drv, dst_drv):
            return self.load_drive_items(src_drv, dst_drv)
        if source_email:
            return self.load_user_items(source_email)
        raise ValueError("load_items() needs either drive ID pair or source_email.")

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
              FROM migration_items WHERE migration_id=%s
            """,
            (self.migration_id,),
        ) or {}
        return {
            "migration_id":   self.migration_id,
            "mode":           self._drive_type,
            "total":          int(row.get("total") or 0),
            "done":           int(row.get("done") or 0),
            "failed":         int(row.get("failed") or 0),
            "pending":        int(row.get("pending") or 0),
            "skipped":        int(row.get("skipped") or 0),
            "ignored":        int(row.get("ignored") or 0),
            "in_progress":    int(row.get("in_progress") or 0),
            "completion_percentage": 0.0,  # compat field
            "status_breakdown": {
                "pending":     int(row.get("pending") or 0),
                "done":        int(row.get("done") or 0),
                "failed":      int(row.get("failed") or 0),
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
            "migration_id": self.migration_id,
            "source_shared_drive_id": src_drv,
            "dest_shared_drive_id":   dst_drv,
        }

    def get_user_checkpoint_summary(self, source_email: str) -> Dict:
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
             WHERE migration_id=%s AND source_user_email=%s
            """,
            (self.migration_id, source_email),
        ) or {}
        return {k: int(v or 0) for k, v in row.items()} | {
            "migration_id": self.migration_id,
            "source_email": source_email,
        }

    def print_summary(self):
        s = self.get_checkpoint_summary()
        logger.info(
            f"[SQL] id={s['migration_id']} mode={s['mode']} "
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

    def mark_permission_failed(
        self, file_id: str, dest_email: str, role: str, error: str
    ):
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

    def mark_member_failed(
        self, dest_drive_id: str, member_email: str, role: str, error: str
    ):
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
    # (interface expected by FileMigrator / migration_engine._ws_via_gcs etc.)
    # ─────────────────────────────────────────────────────────────────────────

    def _blob_name(self, run_id, file_id: str, suffix: str = "") -> str:
        return f"{self.gcs_prefix}{run_id}/{file_id}{suffix}"

    def should_use_gcs(self, file_size: int) -> bool:
        return file_size >= self.large_file_threshold_bytes

    # ── High-level Drive ↔ GCS helpers used by migration_engine.py ────────────

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
        Download a file from Drive (or export a Workspace file) and stream
        directly to GCS.  Returns (ok, blob_name, error_message).
        """
        suffix = ""
        if export_mime:
            # Determine extension from MIME
            for ws_mime, (exp_mime, ext, _) in GOOGLE_WORKSPACE_EXPORT.items():
                if exp_mime == export_mime:
                    suffix = ext
                    break

        blob_name = self._blob_name(run_id, file_id, suffix)

        try:
            if export_mime:
                request = drive_svc.files().export_media(
                    fileId=file_id, mimeType=export_mime
                )
            else:
                request = drive_svc.files().get_media(
                    fileId=file_id, supportsAllDrives=True
                )

            buf      = io.BytesIO()
            dl       = MediaIoBaseDownload(buf, request, chunksize=chunk_size_bytes)
            done     = False
            dl_start = time.time()

            while not done:
                if time.time() - dl_start > 1800:
                    return False, None, "Download timeout (30 min)"
                _, done = dl.next_chunk()

            buf.seek(0)
            blob = self._bucket.blob(blob_name)
            blob.upload_from_file(
                buf,
                content_type=export_mime or mime_type or "application/octet-stream",
            )
            logger.debug(f"GCS ↑ {blob_name}")
            return True, blob_name, None

        except Exception as exc:
            err = str(exc)
            logger.warning(f"download_drive_to_gcs failed [{file_name}]: {err}")
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
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Download blob from GCS and upload to destination Drive.
        Returns (ok, dest_file_id, error_message).
        """
        try:
            blob = self._bucket.blob(blob_name)
            data = blob.download_as_bytes()

            meta = {"name": file_name}
            if parent_id:
                meta["parents"] = [parent_id]
            if import_mime:
                meta["mimeType"] = import_mime

            media = MediaIoBaseUpload(
                io.BytesIO(data),
                mimetype=mime_type,
                resumable=True,
                chunksize=chunk_size_bytes,
            )
            f = drive_svc.files().create(
                body=meta, media_body=media,
                fields="id", supportsAllDrives=True,
            ).execute()
            return True, f["id"], None

        except Exception as exc:
            err = str(exc)
            logger.warning(f"upload_gcs_to_drive failed [{file_name}]: {err}")
            return False, None, err

    def delete_temp(self, blob_name: str):
        """Delete one staging blob after confirmed migration."""
        try:
            self._bucket.blob(blob_name).delete()
            logger.debug(f"GCS ✗ {blob_name}")
        except Exception as exc:
            logger.warning(f"GCS delete failed [{blob_name}]: {exc}")

    def cleanup_run_temps(self, run_id):
        """Remove all staging blobs for a completed run."""
        prefix = f"{self.gcs_prefix}{run_id}/"
        blobs  = list(self._bucket.list_blobs(prefix=prefix))
        if blobs:
            self._bucket.delete_blobs(blobs)
            logger.info(f"GCS cleanup: {len(blobs)} blobs deleted for run={run_id}")
        else:
            logger.info(f"GCS cleanup: nothing to delete for run={run_id}")

    # ── Low-level GCS primitives (kept for backward compat) ───────────────────

    def _gcs_key(self, file_id: str) -> str:
        return f"{self.gcs_prefix}{self.migration_id}/{file_id}"

    def upload_to_gcs(
        self, file_id: str, data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
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