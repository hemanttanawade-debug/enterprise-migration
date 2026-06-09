"""
shared_drive_migrator.py  (v13 – GCS upload rate limiter + executor shutdown guard)

CRITICAL BUG FIXES vs v9:

  BUG-FIX-1  _verify_or_create_dest_drive_by_id() was creating a NEW Shared Drive
             named after the SOURCE drive (e.g. 'z') whenever drives().get() returned
             a 404 or 403.  Root causes:
               a) drives().get() was called WITHOUT useDomainAdminAccess=True, so the
                  dest service account got a false 404 even when the drive existed.
               b) On any access error the code fell through to create_dest_shared_drive(),
                  which created a brand-new drive instead of writing into the pre-mapped
                  destination ('hemant', 0AIlOaJWf3SCuUk9PVA).
             Fix: _verify_or_create_dest_drive_by_id() now:
               - Retries with useDomainAdminAccess=True first, then without.
               - NEVER creates a new drive — it only verifies the given ID and returns
                 None (which causes the drive pair to be skipped with a clear error log).

  BUG-FIX-2  _migrate_item_permissions() used self.source_drive (shared across all
             threads) for permissions().list().  httplib2 is NOT thread-safe, causing:
               - 240–950 second FILE.migrate_perms stalls (httplib2 connection corruption
                 → retry loops inside the HTTP layer)
               - 'NoneType' object has no attribute 'close' crashes (corrupted response)
             Fix: _migrate_item_permissions() now accepts optional src_drive/dst_drive
             parameters.  _process_queue_item() passes thread_src_drive/thread_dst_drive
             (per-thread cached services) into every call.

NEW in v12 – Discovery-First permission optimisation (PERF-8):

  PERF-8  _migrate_item_permissions() was called unconditionally on every folder
          and file, issuing one permissions().list() Drive API call per item even
          when that item's ACL was purely inherited from the Shared Drive root.
          For a 1 000-file drive this generated ~1 000 redundant API calls that
          saturated the quota (429 rate-limit errors) and wasted wall-clock time.

          Fix — Discovery-First approach:
            a) list_shared_drive_files() now requests two extra metadata fields:
                 hasExplicitRoles  – True only when the item carries at least one
                                     ACL entry beyond Shared Drive inheritance.
                 capabilities.canShare – guard to verify we have share rights
               These come for FREE in the existing files.list() response; no
               extra API round-trip is needed.

            b) _build_shared_drive_folder_structure() checks hasExplicitRoles
               on each folder dict before calling _migrate_item_permissions().
               If False, the item inherits from the drive root → skip the call.

            c) _process_queue_item() checks item.has_explicit_roles (persisted
               to SQL by the state manager during register_discovered_items).
               If False → skip _migrate_item_permissions() entirely.

            d) _migrate_item_permissions() accepts an optional has_explicit_roles
               kwarg as a fast-path override so callers that already know the
               answer don't repeat the check inside.

          Result: permissions().list() is called ONLY for the small minority of
          items that truly carry explicit overrides — drive members are already
          migrated via migrate_drive_members() and destination inheritance
          propagates those roles automatically.

NEW in v13 – GCS upload rate limiter + executor shutdown guard:

  FIX-GCS-1  upload_gcs_to_drive was fired by 14 workers simultaneously with
             zero rate control, exhausting the per-user Drive API quota for
             files.create (resumable upload initiation) immediately, producing
             a cascade of:
               HTTP 403 "User rate limit exceeded" / userRateLimitExceeded
             on every upload attempt and triggering PM2 SIGKILL (process stuck
             in retry sleeps → PM2 loses patience after 1600 ms → SIGKILL).

             Fix: module-level _GCS_UPLOAD_BUCKET (_UploadTokenBucket) shared
             across all 14 worker threads.  consume() is called once per GCS
             upload attempt, before sub_pool.submit(upload_gcs_to_drive).
             Rate defaults to 6 initiations/sec (safe below the 10 QPS default
             Drive quota, leaving headroom for concurrent memory-path uploads).
             Tune _GCS_UPLOAD_RATE if you obtain a quota increase.

  FIX-GCS-2  403 userRateLimitExceeded in upload error string is now treated
             identically to a 429: the retry wait is doubled to drain the quota
             window before the next attempt, instead of retrying immediately.

  FIX-GCS-3  "cannot schedule new futures after interpreter shutdown" crash:
             _migrate_via_gcs() creates nested ThreadPoolExecutor sub-pools
             (one for download, one for upload). When PM2 sends SIGTERM/SIGKILL
             mid-flight the outer pool's __exit__ begins tearing down threads
             while a worker is still inside _migrate_via_gcs() trying to
             sub_pool.submit() — Python raises RuntimeError: "cannot schedule
             new futures after interpreter shutdown".

             Fix: both sub_pool blocks are now wrapped in try/except RuntimeError.
             On shutdown the method returns a clean {"error_type": "executor_shutdown"}
             result so the outer pool can drain gracefully without an unhandled
             exception propagating to the PM2 log.

UNCHANGED from v12:
  - BUG-FIX-1 (verify-only dest drive, no spurious creation).
  - BUG-FIX-2 (per-thread src/dst Drive services, no shared httplib2).
  - Temporary source admin organizer + revoke after migration.
  - Temporary destination admin organizer + revoke after migration.
  - All PERF-1 through PERF-8 throughput improvements.
  - FIX-1/FIX-2/FIX-3 from permissions_migrator (organizer→fileOrganizer, NoneType guard).
  - RAM-adaptive chunk sizes (8–256 MB).
  - XL-first two-pass queue, IN_PROGRESS reset, thread-local Drive services.
"""

import concurrent.futures as _cf
import io
import logging
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Tunable constants
# ─────────────────────────────────────────────────────────────────────────────

LARGE_FILE_THRESHOLD_BYTES  = 5   * 1_024 * 1_024        # 5 MB   → GCS path
XLARGE_FILE_THRESHOLD_BYTES = 600 * 1_024 * 1_024        # 600 MB → XL dedicated pool
MAX_FILE_SIZE_BYTES         = 5   * 1_024 * 1_024 * 1_024  # 5 GB   → hard ignore limit

# ── Phase-2 file transfer workers (PERF-1: raised to match migration_engine_v4) ──
GLOBAL_WORKERS    = 14   # Phase-2 regular pool      (was 10 in v6)
XLARGE_WORKERS    = 14   # Phase-2 XL dedicated pool (was  6 in v6)

# ── Phase-1 discovery ────────────────────────────────────────────────────────
DISCOVERY_WORKERS = 8    # parallel drives during Phase 1

# ── Pre-flight and post-migration parallelism (PERF-4/5/6: new) ──────────────
PREFLIGHT_WORKERS = 8    # parallel _ensure_admin_access() calls  (was serial)
MEMBER_WORKERS    = 8    # parallel migrate_drive_members() calls (was serial)
CLEANUP_WORKERS   = 8    # parallel dest-organizer + revoke calls (was serial)

MAX_RETRIES       = 5
MAX_BACKOFF_S     = 32
CHUNK_SIZE        = 32 * 1_024 * 1_024   # default; overridden by RAM probe
GCS_FILE_TIMEOUT  = 3_600                # seconds — hard per-file GCS timeout

# ── Drive API listing page sizes (PERF-2: files page raised 200→1000) ────────
FILES_LIST_PAGE_SIZE  = 1000   # was 200 — 5× fewer round-trips per drive
DRIVES_LIST_PAGE_SIZE = 100    # unchanged — API max for drives.list

try:
    import psutil as _psutil
    _PSUTIL_AVAILABLE = True
except ImportError:
    _psutil = None
    _PSUTIL_AVAILABLE = False

# ─────────────────────────────────────────────────────────────────────────────
# FIX-GCS-1: GCS upload rate limiter
#
# Drive API quota for files.create (resumable upload initiation) is per-user
# and shared across all 14 worker threads.  Without throttling, 14 threads
# firing simultaneously exhaust the quota window in under 1 s, producing
# HTTP 403 "userRateLimitExceeded" on every upload and stalling the process
# long enough that PM2 sends SIGKILL.
#
# _GCS_UPLOAD_RATE  — resumable upload initiations per second, project-wide.
#                     Default Drive quota: ~10 QPS for files.create.
#                     We use 6 to leave headroom for memory-path uploads that
#                     also call files.create concurrently.
#                     Raise this if you have a quota increase approved in GCP.
# ─────────────────────────────────────────────────────────────────────────────
_GCS_UPLOAD_RATE  = 6    # upload initiations / second  ← tune after quota increase
_GCS_UPLOAD_BURST = 6    # max burst tokens (== rate → no burst spike allowed)


class _UploadTokenBucket:
    """
    Thread-safe token bucket for rate-limiting GCS→Drive upload initiations.

    One module-level instance (_GCS_UPLOAD_BUCKET) is shared across all worker
    threads.  consume() blocks only when the bucket is empty — callers that
    arrive when tokens are available pay zero wait time.
    """

    def __init__(self, rate: float = 6.0, burst: int = 6):
        self._rate   = float(rate)
        self._burst  = float(burst)
        self._tokens = float(burst)   # start full → first calls are free
        self._last   = time.monotonic()
        self._lock   = threading.Lock()

    def consume(self, n: int = 1) -> None:
        """Block until n tokens are available, then consume them."""
        with self._lock:
            now          = time.monotonic()
            elapsed      = now - self._last
            self._tokens = min(self._burst,
                               self._tokens + elapsed * self._rate)
            self._last   = now
            if self._tokens >= n:
                self._tokens -= n
                wait = 0.0
            else:
                wait = (n - self._tokens) / self._rate
                self._tokens = 0.0
        if wait > 0:
            time.sleep(wait)


# Module-level singleton — ONE bucket shared by ALL 14 upload worker threads.
_GCS_UPLOAD_BUCKET = _UploadTokenBucket(
    rate=_GCS_UPLOAD_RATE,
    burst=_GCS_UPLOAD_BURST,
)

# Import from sql_state_manager — GOOGLE_WORKSPACE_EXPORT kept for backwards
# compat in other modules; GOOGLE_WORKSPACE_TYPES defined locally below.
from sql_state_manager import IGNORED_MIME_TYPES as _BASE_IGNORED, GOOGLE_WORKSPACE_EXPORT


# ─────────────────────────────────────────────────────────────────────────────
# FIX-1: Augmented IGNORED_MIME_TYPES — adds legacy Google Video type.
# The Drive API rejects get_media() for 'vid' with "Use Export" but it also
# has no supported export MIME type, so it must be ignored entirely.
# ─────────────────────────────────────────────────────────────────────────────
IGNORED_MIME_TYPES = frozenset(_BASE_IGNORED) | frozenset({
    "application/vnd.google-apps.vid",
    "application/vnd.google-apps.script",
    "application/vnd.google-apps.form",
    "application/vnd.google-apps.site",
    "application/octet-stream",# FIX-1: legacy Google Video — no export API
})


# ─────────────────────────────────────────────────────────────────────────────
# FIX-3 / FIX-2: GOOGLE_WORKSPACE_TYPES dict (mirrors migration_engine_v4).
#
# Using this instead of the simpler GOOGLE_WORKSPACE_EXPORT tuple gives us:
#   - can_export=False  → immediate ignore for shortcuts, forms, etc.
#   - fallback_mime     → PDF retry when exportSizeLimitExceeded for large Slides
#   - Consistent with migration_engine_v4's workspace handling logic
# ─────────────────────────────────────────────────────────────────────────────
GOOGLE_WORKSPACE_TYPES = {
    "application/vnd.google-apps.document": {
        "export_mime": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "extension":   ".docx",
        "import_mime": "application/vnd.google-apps.document",
        "can_export":  True,
    },
    "application/vnd.google-apps.spreadsheet": {
        "export_mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "extension":   ".xlsx",
        "import_mime": "application/vnd.google-apps.spreadsheet",
        "can_export":  True,
    },
    # FIX-2: fallback_mime + fallback_ext for oversized presentations
    "application/vnd.google-apps.presentation": {
        "export_mime":   "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "extension":     ".pptx",
        "import_mime":   "application/vnd.google-apps.presentation",
        "can_export":    True,
        "fallback_mime": "application/pdf",
        "fallback_ext":  ".pdf",
    },
    "application/vnd.google-apps.drawing": {
        "export_mime":   "image/svg+xml",
        "extension":     ".svg",
        "import_mime":   None,
        "can_export":    True,
        "fallback_mime": "application/pdf",
        "fallback_ext":  ".pdf",
    },
    "application/vnd.google-apps.map": {
        "export_mime": "application/vnd.google-earth.kmz",
        "extension":   ".kmz",
        "import_mime": None,
        "can_export":  True,
    },
    "application/vnd.google-apps.jam": {
        "export_mime": "application/pdf",
        "extension":   ".pdf",
        "import_mime": None,
        "can_export":  True,
    },
    "application/vnd.google-apps.folder": {
        "export_mime": None, "extension": None, "import_mime": None, "can_export": False,
    },
    # FIX-3: shortcuts have no content — non-exportable so they're ignored cleanly
    "application/vnd.google-apps.shortcut": {
        "export_mime": None, "extension": None, "import_mime": None, "can_export": False,
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _backoff(attempt: int, fraction: float = 0.25) -> float:
    base  = min(2 ** attempt, MAX_BACKOFF_S)
    delta = base * fraction
    return base + random.uniform(-delta, delta)


def _fmt_bytes(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f} PB"


def _extract_id(response) -> Optional[str]:
    """Safely pull 'id' from a Drive API response (dict or list)."""
    if isinstance(response, list):
        response = response[0] if response else None
    if isinstance(response, dict):
        return response.get("id")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# FIX-1: RAM-adaptive chunk size — properly scaled (was returning 16 MB for
# every tier in v5/v6 due to a copy-paste error; now matches the documented
# table in migration_engine_v4).
#
# Available RAM  →  Chunk size
# ──────────────────────────────
# <  512 MB      →   8 MB   (safe floor)
# <  1   GB      →  16 MB
# <  2   GB      →  32 MB   (matches previous static default)
# <  4   GB      →  64 MB
# <  8   GB      → 128 MB
# >= 8   GB      → 256 MB   (saturates most GCP connections)
# ─────────────────────────────────────────────────────────────────────────────

def _get_adaptive_chunk_size() -> int:
    """
    Return a download/upload chunk size scaled to available system RAM.
    Falls back to CHUNK_SIZE when psutil is unavailable.
    """
    if not _PSUTIL_AVAILABLE:
        return CHUNK_SIZE
    try:
        avail_mb = _psutil.virtual_memory().available / (1024 * 1024)
        if   avail_mb <  512: chunk_mb =  8    # FIX: was hardcoded 16 for all tiers
        elif avail_mb < 1024: chunk_mb = 16
        elif avail_mb < 2048: chunk_mb = 32
        elif avail_mb < 4096: chunk_mb = 64
        elif avail_mb < 8192: chunk_mb = 128
        else:                 chunk_mb = 256
        logger.debug(
            f"[RAM-ADAPT] available={avail_mb:.0f} MB → chunk_size={chunk_mb} MB"
        )
        return chunk_mb * 1024 * 1024
    except Exception:
        return CHUNK_SIZE


# ─────────────────────────────────────────────────────────────────────────────
# SharedDriveMigrator
# ─────────────────────────────────────────────────────────────────────────────

class SharedDriveMigrator:
    """
    Handles Shared Drive → Shared Drive migration.

    Two-phase architecture:
      Phase 1 — per-drive discovery + folder creation (parallel drives).
      Phase 2 — XL-first two-pass global queue, sorted largest-first.

    v7: Full performance parity with migration_engine_v4.
        RAM-adaptive chunk sizes actually scale (8–256 MB).
        5 GB hard-ignore limit added.
        GOOGLE_WORKSPACE_TYPES with fallback PDF export for oversized Slides.
        pending list sorted largest-first before XL/regular split.
        cache_discovery=False on all Drive service builds.
    """

    def __init__(
        self,
        source_admin_drive,
        dest_admin_drive,
        source_domain: str,
        dest_domain: str,
        config,
        sql_mgr,
        run_id,
        parallel_files: int = 5,
    ):
        self.source_drive   = source_admin_drive
        self.dest_drive     = dest_admin_drive
        self.source_domain  = source_domain
        self.dest_domain    = dest_domain
        self.config         = config
        self.mgr            = sql_mgr
        self.run_id         = run_id
        self.parallel_files = parallel_files

        # FIX: try uppercase attrs first (real Config class uses SOURCE_ADMIN_EMAIL)
        self._admin_email: Optional[str] = (
            getattr(config, "SOURCE_ADMIN_EMAIL", None)
            or getattr(config, "source_admin_email", None)
            or getattr(config, "admin_email", None)
        )

        # Store credential paths so each worker thread can build its OWN
        # Drive service — httplib2 is NOT thread-safe; sharing one service
        # across threads causes concurrent next_chunk() to corrupt the connection.
        from pathlib import Path as _Path
        _FLASK_CRED_DIR = _Path.home() / "flask-backend" / "uploads" / "credential"
        self._src_creds_file: Optional[str] = None
        self._dst_creds_file: Optional[str] = None
        self._src_admin_email: Optional[str] = self._admin_email
        self._dst_admin_email: Optional[str] = (
            getattr(config, "DEST_ADMIN_EMAIL", None)
            or getattr(config, "dest_admin_email", None)
        )
        try:
            src_p = _FLASK_CRED_DIR / "source_credentials.json"
            dst_p = _FLASK_CRED_DIR / "dest_credentials.json"
            if src_p.exists():
                self._src_creds_file = str(src_p)
            elif hasattr(config, "SOURCE_CREDENTIALS_FILE"):
                self._src_creds_file = str(config.SOURCE_CREDENTIALS_FILE)
            if dst_p.exists():
                self._dst_creds_file = str(dst_p)
            elif hasattr(config, "DEST_CREDENTIALS_FILE"):
                self._dst_creds_file = str(config.DEST_CREDENTIALS_FILE)
        except Exception as _ce:
            logger.warning(f"[INIT] Could not resolve credential paths: {_ce}")

        self.stats = {
            "drives_total":            0,
            "drives_created":          0,
            "drives_failed":           0,
            "files_migrated":          0,
            "files_failed":            0,
            "files_skipped":           0,
            "files_ignored":           0,
            "folders_created":         0,
            "members_migrated":        0,
            "members_failed":          0,
            "gcs_routed":              0,
            "memory_routed":           0,
            "temp_memberships":        0,
            # PERF-8: Discovery-First permission optimisation counters
            "perms_skipped_inherited": 0,   # items whose ACL is purely inherited → no API call
            "perms_explicit_migrated": 0,   # items that truly had explicit ACL overrides
        }

        self._folder_maps: Dict[str, Dict[str, str]] = {}
        # FIX Bug-3: lock guards writes to _folder_maps from Phase-1 threads so
        # Phase-2 workers never read a partially-written mapping for a drive.
        self._folder_maps_lock = threading.Lock()

        # Build skip-set ONCE: source admin + dest admin emails must NEVER be
        # copied onto destination file/folder ACLs. They are TEMPORARY members
        # at the source/dest Shared Drive ROOT only, added purely for migration
        # access and revoked immediately after. Copying them to individual items
        # would leave permanent ACL pollution after the root membership is revoked.
        _skip_raw = {self._src_admin_email, self._dst_admin_email}
        self._perm_skip_emails: frozenset = frozenset(
            e.lower() for e in _skip_raw if e
        )
        if self._perm_skip_emails:
            logger.info(
                f"[INIT] Temp-admin skip-list (excluded from all dest item ACLs): "
                f"{self._perm_skip_emails}"
            )

        # FIX Bug-1: thread-local cache for Drive services — mirrors
        # migration_engine_v4._get_drive_service_for_thread().
        # Previously _build_thread_drive_svc() was called on EVERY file with no
        # caching, spending ~0.5–2 s per file on credential loading and HTTP
        # object creation.  With 14 workers all burning that overhead in the
        # first seconds, effective concurrency collapsed to ~1 transfer at a time.
        self._thread_local = threading.local()

    # =========================================================================
    # STEP 0: Temporary admin membership helpers
    # =========================================================================

    def _ensure_admin_access(
        self,
        drive_id: str,
        drive_name: str,
    ) -> Tuple[bool, Optional[str]]:
        if not self._admin_email:
            logger.warning(
                f"[SRC-MEMBERSHIP] No source_admin_email configured — "
                f"skipping membership check for '{drive_name}' ({drive_id}). "
                "Set config.SOURCE_ADMIN_EMAIL to enable automatic temporary membership."
            )
            return False, None

        try:
            resp = self.source_drive.permissions().list(
                fileId=drive_id,
                supportsAllDrives=True,
                useDomainAdminAccess=True,
                fields="permissions(id,emailAddress,type,role)",
            ).execute()
            existing_perms = resp.get("permissions", [])
        except HttpError as exc:
            logger.error(
                f"[SRC-MEMBERSHIP] Cannot list permissions for '{drive_name}': {exc}"
            )
            return False, None

        admin_lower = self._admin_email.lower()
        for perm in existing_perms:
            perm_email = (perm.get("emailAddress") or "").lower()
            if perm_email == admin_lower:
                logger.debug(
                    f"[SRC-MEMBERSHIP] Source admin '{self._admin_email}' already a "
                    f"member of '{drive_name}' (role='{perm.get('role')}') — no change."
                )
                return False, None

        logger.info(
            f"[SRC-MEMBERSHIP] Source admin '{self._admin_email}' is NOT a member of "
            f"'{drive_name}' ({drive_id}) — adding temporary 'manager' permission."
        )
        try:
            new_perm = self.source_drive.permissions().create(
                fileId=drive_id,
                supportsAllDrives=True,
                useDomainAdminAccess=True,
                sendNotificationEmail=False,
                body={
                    "type":         "user",
                    "role":         "organizer",
                    "emailAddress": self._admin_email,
                },
                fields="id",
            ).execute()

            # Guard: execute() can return None on HTTP-204 (empty body) responses
            perm_id = (new_perm or {}).get("id")
            if perm_id:
                logger.info(
                    f"[SRC-MEMBERSHIP] ✓ Temporary manager added to SOURCE '{drive_name}' "
                    f"(permissionId={perm_id}) — will be removed after migration."
                )
                self.stats["temp_memberships"] += 1
                return True, perm_id
            else:
                logger.warning(
                    f"[SRC-MEMBERSHIP] permissions.create returned no id for '{drive_name}'"
                )
                return False, None

        except HttpError as exc:
            logger.error(
                f"[SRC-MEMBERSHIP] Failed to add source admin to '{drive_name}': {exc}"
            )
            return False, None

    def _revoke_admin_access(
        self,
        drive_id: str,
        drive_name: str,
        permission_id: str,
    ) -> None:
        if not permission_id:
            return
        logger.info(
            f"[SRC-MEMBERSHIP] Removing temporary manager permission '{permission_id}' "
            f"from SOURCE '{drive_name}' ({drive_id})..."
        )
        try:
            self.source_drive.permissions().delete(
                fileId=drive_id,
                permissionId=permission_id,
                supportsAllDrives=True,
                useDomainAdminAccess=True,
            ).execute()
            logger.info(
                f"[SRC-MEMBERSHIP] ✓ Temporary manager removed from SOURCE '{drive_name}'"
            )
        except HttpError as exc:
            logger.warning(
                f"[SRC-MEMBERSHIP] Could not remove temp manager from "
                f"'{drive_name}' (permissionId={permission_id}): {exc} — "
                "remove manually if needed."
            )

    def _ensure_dest_admin_organizer(
        self,
        dest_drive_id: str,
        drive_name: str,
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Add the destination admin as a TEMPORARY Organizer on the destination
        Shared Drive — mirroring enterprise migration tools like CloudM.

        Returns: (was_added_temporarily, permission_id, original_role)

          was_added_temporarily=True  → admin was NOT pre-existing; we added
                                        them; caller MUST revoke after migration.
          was_added_temporarily=False → admin already existed (or error);
                                        do NOT revoke — preserve original role.
          permission_id               → id of the *newly created* permission
                                        (None when was_added_temporarily=False).
          original_role               → role the admin already had before we ran
                                        (None when admin was absent).

        CONTRACT:
          - Only touches the Shared Drive root membership (permissions.create
            fileId=sharedDriveId) — never individual file/folder ACLs.
          - Does NOT downgrade or modify a pre-existing organizer/manager role.
        """
        dest_admin_email: Optional[str] = (
            getattr(self.config, "DEST_ADMIN_EMAIL", None)
            or getattr(self.config, "dest_admin_email", None)
            or getattr(self.config, "admin_email", None)
        )

        if not dest_admin_email:
            logger.warning(
                f"[DEST-MEMBERSHIP] No dest_admin_email configured — "
                f"skipping organizer add on destination '{drive_name}' ({dest_drive_id}). "
                "Set config.DEST_ADMIN_EMAIL to enable."
            )
            return False, None, None

        try:
            resp = self.dest_drive.permissions().list(
                fileId=dest_drive_id,
                supportsAllDrives=True,
                useDomainAdminAccess=True,
                fields="permissions(id,emailAddress,type,role)",
            ).execute()
            existing_perms = resp.get("permissions", [])
        except HttpError as exc:
            logger.error(
                f"[DEST-MEMBERSHIP] Cannot list dest permissions for '{drive_name}': {exc}"
            )
            return False, None, None

        dest_admin_lower = dest_admin_email.lower()
        for perm in existing_perms:
            perm_email = (perm.get("emailAddress") or "").lower()
            if perm_email == dest_admin_lower:
                original_role = perm.get("role")
                logger.info(
                    f"[DEST-MEMBERSHIP] Dest admin '{dest_admin_email}' is already a "
                    f"member of destination '{drive_name}' "
                    f"(role='{original_role}') — preserving existing role, "
                    "will NOT revoke after migration."
                )
                # Pre-existing member: caller must NOT revoke
                return False, None, original_role

        # Admin is NOT a current member — add as temporary Organizer
        logger.info(
            f"[DEST-MEMBERSHIP] Dest admin '{dest_admin_email}' is NOT a member of "
            f"destination '{drive_name}' ({dest_drive_id}) — adding TEMPORARY "
            "'organizer' access for migration duration."
        )
        try:
            new_perm = self.dest_drive.permissions().create(
                fileId=dest_drive_id,
                supportsAllDrives=True,
                useDomainAdminAccess=True,
                sendNotificationEmail=False,
                body={
                    "type":         "user",
                    "role":         "organizer",
                    "emailAddress": dest_admin_email,
                },
                fields="id",
            ).execute()

            # Guard: execute() can return None on HTTP-204 (empty body) responses
            perm_id = (new_perm or {}).get("id")
            if perm_id:
                logger.info(
                    f"[DEST-MEMBERSHIP] ✓ Temporary organizer added to DESTINATION "
                    f"'{drive_name}' (permissionId={perm_id}) — "
                    "WILL be revoked after migration completes."
                )
                self.stats["temp_memberships"] += 1
                return True, perm_id, None
            else:
                logger.warning(
                    f"[DEST-MEMBERSHIP] permissions.create returned no id for "
                    f"destination '{drive_name}'"
                )
                return False, None, None

        except HttpError as exc:
            logger.error(
                f"[DEST-MEMBERSHIP] Failed to add dest admin to '{drive_name}': {exc}"
            )
            return False, None, None

    def _revoke_dest_admin_access(
        self,
        dest_drive_id: str,
        drive_name: str,
        permission_id: str,
    ) -> None:
        """Remove the temporary destination-admin Organizer added before migration."""
        if not permission_id:
            return
        logger.info(
            f"[DEST-MEMBERSHIP] Removing temporary organizer permission '{permission_id}' "
            f"from DESTINATION '{drive_name}' ({dest_drive_id})..."
        )
        try:
            self.dest_drive.permissions().delete(
                fileId=dest_drive_id,
                permissionId=permission_id,
                supportsAllDrives=True,
                useDomainAdminAccess=True,
            ).execute()
            logger.info(
                f"[DEST-MEMBERSHIP] ✓ Temporary organizer removed from DESTINATION "
                f"'{drive_name}'"
            )
        except HttpError as exc:
            logger.warning(
                f"[DEST-MEMBERSHIP] Could not remove temp organizer from "
                f"'{drive_name}' (permissionId={permission_id}): {exc} — "
                "remove manually if needed."
            )

    # =========================================================================
    # STEP 1: List all Shared Drives in source domain
    # =========================================================================

    def list_source_shared_drives(self) -> List[Dict]:
        """
        List all Shared Drives in the source domain.

        PERF-3: removed the 0.3 s per-page sleep that previously added
        0.3 s × N_pages of dead idle before Phase 1 could start.
        """
        drives     = []
        page_token = None

        logger.info("Listing all Shared Drives in source domain...")

        while True:
            try:
                resp = self.source_drive.drives().list(
                    pageSize=DRIVES_LIST_PAGE_SIZE,
                    pageToken=page_token,
                    fields="nextPageToken, drives(id, name, createdTime, restrictions)",
                    useDomainAdminAccess=True,
                ).execute()

                batch = resp.get("drives", [])
                drives.extend(batch)
                logger.info(f"  Found {len(batch)} drives (total: {len(drives)})")

                page_token = resp.get("nextPageToken")
                if not page_token:
                    break
                # PERF-3: no sleep between pages — removed 0.3 s × N_pages idle

            except HttpError as exc:
                logger.error(f"Failed to list shared drives: {exc}")
                raise

        logger.info(f"Total Shared Drives: {len(drives)}")
        return drives

    # =========================================================================
    # STEP 2: Create (or find) destination Shared Drive
    # =========================================================================

    def create_dest_shared_drive(
        self, drive_name: str, source_drive_id: str
    ) -> Optional[str]:
        existing = self._find_existing_shared_drive(drive_name)
        if existing:
            logger.info(f"Shared Drive already exists: {drive_name} → {existing}")
            return existing

        try:
            new_drive = self.dest_drive.drives().create(
                requestId=str(uuid.uuid4()),
                body={"name": drive_name},
                fields="id,name",
            ).execute()
            dest_id = new_drive["id"]
            logger.info(f"✓ Created Shared Drive: {drive_name} → {dest_id}")
            return dest_id

        except HttpError as exc:
            logger.error(f"Failed to create Shared Drive '{drive_name}': {exc}")
            return None

    def _find_existing_shared_drive(self, drive_name: str) -> Optional[str]:
        try:
            resp = self.dest_drive.drives().list(
                q=f"name='{drive_name}'",
                fields="drives(id, name)",
                useDomainAdminAccess=True,
            ).execute()
            drives = resp.get("drives", [])
            return drives[0]["id"] if drives else None
        except Exception as exc:
            logger.debug(f"_find_existing_shared_drive: {exc}")
            return None

    # =========================================================================
    # STEP 3: List all files/folders inside a Shared Drive
    # =========================================================================

    def list_shared_drive_files(self, drive_id: str) -> List[Dict]:
        """
        List all files/folders inside a Shared Drive.

        PERF-2: pageSize raised from 200 → 1000 (FILES_LIST_PAGE_SIZE) and the
        0.2 s per-page sleep removed.  For a drive with 1000 files this cuts
        5 pages + 5 × 0.2 s = 1 s of forced idle down to 1 page + 0 s idle.
        The Drive API enforces its own rate limits server-side; we don't need a
        client-side sleep that only adds latency without preventing 429s.
        """
        files      = []
        page_token = None

        logger.info(f"Listing files in Shared Drive: {drive_id}")

        while True:
            try:
                resp = self.source_drive.files().list(
                    q="trashed=false",
                    spaces="drive",
                    corpora="drive",
                    driveId=drive_id,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    fields=(
                        "nextPageToken, files("
                        "id, name, mimeType, size, parents, "
                        "createdTime, modifiedTime, "
                        # PERF-8 Discovery-First: these two fields arrive FREE in
                        # files.list and let us skip permissions().list() for the
                        # majority of items that carry only inherited ACLs.
                        "hasExplicitRoles, "
                        "capabilities/canShare)"
                    ),
                    pageSize=FILES_LIST_PAGE_SIZE,   # PERF-2: was 200
                    pageToken=page_token,
                ).execute()

                batch = resp.get("files", [])
                files.extend(batch)

                page_token = resp.get("nextPageToken")
                if not page_token:
                    break
                # PERF-2: no sleep between pages — removed 0.2 s × N_pages idle

            except HttpError as exc:
                logger.error(f"Failed to list files in drive {drive_id}: {exc}")
                raise

        logger.info(f"  {len(files)} items in drive {drive_id}")
        return files

    # =========================================================================
    # STEP 4: Migrate drive-level members
    # =========================================================================

    def migrate_drive_members(
        self,
        source_drive_id: str,
        dest_drive_id: str,
        drive_name: str,
    ) -> Dict:
        result = {"migrated": 0, "failed": 0, "skipped": 0}

        try:
            resp = self.source_drive.permissions().list(
                fileId=source_drive_id,
                supportsAllDrives=True,
                useDomainAdminAccess=True,
                fields="permissions(id,type,role,emailAddress,domain,displayName)",
            ).execute()
            all_permissions = resp.get("permissions", [])
            logger.info(f"  {len(all_permissions)} members in '{drive_name}'")

        except Exception as exc:
            logger.error(f"Failed to list members for '{drive_name}': {exc}")
            return result

        # Filter out temp-admin accounts from drive-level membership.
        # The source admin was added temporarily to the SOURCE drive root to allow
        # crawling — that temporary membership must NOT be mirrored onto the
        # DESTINATION drive root either.
        if self._perm_skip_emails:
            permissions = [
                p for p in all_permissions
                if (p.get("emailAddress") or "").lower() not in self._perm_skip_emails
            ]
            skipped = len(all_permissions) - len(permissions)
            if skipped:
                logger.info(
                    f"  [MEMBERS] Filtered {skipped} temp-admin permission(s) "
                    f"from drive-level migration for '{drive_name}'"
                )
        else:
            permissions = all_permissions

        try:
            from permissions_migrator import EnhancedPermissionsMigrator

            pm = EnhancedPermissionsMigrator(
                self.source_drive,
                self.dest_drive,
                self.source_domain,
                self.dest_domain,
            )

            pr = pm.migrate_permissions(
                source_drive_id,
                dest_drive_id,
                permissions,
                shared_drive_mode=True,
                is_drive_root=True,   # drive root: keep 'organizer', don't downgrade
            )

            result["migrated"] = pr.get("migrated", 0)
            result["failed"]   = pr.get("failed", 0)
            result["skipped"]  = pr.get("skipped", 0)

            for detail in pr.get("details", []):
                role       = detail.get("role", "")
                ptype      = detail.get("type", "user")
                status     = detail.get("status", "failed")
                error      = detail.get("error", "")
                dest_email = detail.get("target_email") or detail.get("email", "")

                if role == "owner" or not dest_email:
                    continue

                member_type = ptype if ptype in ("user", "group", "domain") else "user"
                if role not in ("organizer", "fileOrganizer", "writer", "commenter", "reader"):
                    continue

                try:
                    self.mgr.upsert_shared_drive_member(
                        source_drive_id,
                        dest_drive_id,
                        dest_email,
                        member_type,
                        role,
                    )
                    if status == "success":
                        self.mgr.mark_member_done(dest_drive_id, dest_email, role)
                    elif status == "failed":
                        self.mgr.mark_member_failed(
                            dest_drive_id, dest_email, role, error
                        )
                except Exception as exc:
                    logger.debug(f"  shared_drive_members upsert error: {exc}")

        except ImportError:
            logger.error("EnhancedPermissionsMigrator not available")

        logger.info(
            f"  Members: {result['migrated']} migrated, "
            f"{result['failed']} failed, {result['skipped']} skipped"
        )
        return result

    def _map_email(self, source_email: str) -> str:
        if source_email.endswith(f"@{self.source_domain}"):
            local = source_email.split("@")[0]
            return f"{local}@{self.dest_domain}"
        return source_email

    # =========================================================================
    # STEP 5: Folder structure builder
    # =========================================================================

    def _build_shared_drive_folder_structure(
        self,
        folders: List[Dict],
        source_drive_id: str,
        dest_drive_id: str,
    ) -> Dict[str, str]:
        folder_mapping: Dict[str, str] = {}
        sorted_folders = self._sort_folders_by_hierarchy(folders)

        for folder in sorted_folders:
            fid   = folder["id"]
            fname = folder["name"]
            pids  = folder.get("parents", [])

            cached = self.mgr._cache.get(fid)
            if cached and cached.dest_folder_id:
                folder_mapping[fid] = cached.dest_folder_id
                continue

            existing_status = self.mgr.get_item_status(self.run_id, fid)
            if existing_status == "DONE":
                cached_row = self.mgr._cache.get(fid)
                if cached_row and cached_row.dest_folder_id:
                    folder_mapping[fid] = cached_row.dest_folder_id
                    continue

            self.mgr.mark_in_progress(self.run_id, fid)

            dest_parent = dest_drive_id
            if pids:
                parent_src = pids[0]
                if parent_src == source_drive_id:
                    dest_parent = dest_drive_id
                else:
                    dest_parent = folder_mapping.get(parent_src, dest_drive_id)

            dest_fid = self._create_folder_in_shared_drive(fname, dest_parent)

            if dest_fid:
                folder_mapping[fid] = dest_fid
                self.mgr.register_folder_mapping(self.run_id, fid, dest_fid)
                self.mgr.mark_done(
                    self.run_id, fid,
                    dest_item_id=dest_fid,
                    dest_parent_id=dest_parent,
                )
                self.stats["folders_created"] += 1
                logger.debug(f"  ✓ Folder: {fname}")

                # ── PERF-8: Discovery-First permission gate ────────────────────
                # hasExplicitRoles is True only when the folder carries at least
                # one ACL entry that is NOT inherited from the Shared Drive root.
                # When False, drive-level members already propagate via inheritance
                # in the destination — no permissions().list() call needed.
                has_explicit = folder.get("hasExplicitRoles", False)
                if has_explicit:
                    self.stats["perms_explicit_migrated"] += 1
                    self._migrate_item_permissions(
                        fid, dest_fid, fname, "FOLDER", dest_drive_id,
                        has_explicit_roles=True,
                    )
                else:
                    self.stats["perms_skipped_inherited"] += 1
                    logger.debug(
                        f"  [PERMS-SKIP] Folder '{fname}' has no explicit roles "
                        f"— relying on Shared Drive inheritance (saved 1 API call)"
                    )
            else:
                self.mgr.mark_failed(self.run_id, fid, "Failed to create folder")
                logger.error(f"  ✗ Folder failed: {fname}")

        return folder_mapping

    def _create_folder_in_shared_drive(
        self,
        folder_name: str,
        parent_id: str,
        max_retries: int = 3,
    ) -> Optional[str]:
        for attempt in range(max_retries):
            try:
                meta = {
                    "name":     folder_name,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents":  [parent_id],
                }
                f = self.dest_drive.files().create(
                    body=meta, fields="id,name",
                    supportsAllDrives=True,
                ).execute()
                return f["id"]

            except HttpError as exc:
                if exc.resp.status == 409:
                    found = self._find_existing_folder(folder_name, parent_id)
                    if found:
                        return found
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to create folder '{folder_name}': {exc}")
                    return None
            except Exception as exc:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Error creating folder '{folder_name}': {exc}")
                    return None
        return None

    def _find_existing_folder(self, name: str, parent_id: str) -> Optional[str]:
        try:
            resp = self.dest_drive.files().list(
                q=(
                    f"name='{name}' and "
                    "mimeType='application/vnd.google-apps.folder' and "
                    f"trashed=false and '{parent_id}' in parents"
                ),
                fields="files(id)",
                pageSize=5,
                supportsAllDrives=True,
            ).execute()
            files = resp.get("files", [])
            return files[0]["id"] if files else None
        except Exception:
            return None

    def _sort_folders_by_hierarchy(self, folders: List[Dict]) -> List[Dict]:
        folder_ids = {f["id"] for f in folders}
        result:  List[Dict] = []
        visited: set        = set()

        def visit(folder: Dict):
            if folder["id"] in visited:
                return
            visited.add(folder["id"])
            pids = folder.get("parents", [])
            if pids and pids[0] in folder_ids:
                parent = next(
                    (f for f in folders if f["id"] == pids[0]), None
                )
                if parent:
                    visit(parent)
            result.append(folder)

        for f in folders:
            visit(f)
        return result

    # =========================================================================
    # STEP 6: Item permissions
    # =========================================================================

    def _migrate_item_permissions(
        self,
        source_id: str,
        dest_id: str,
        name: str,
        item_type: str,
        parent_drive_id: str,
        src_drive=None,   # BUG-FIX: accept thread-local service to avoid shared httplib2
        dst_drive=None,
        has_explicit_roles: Optional[bool] = None,  # PERF-8: Discovery-First fast path
    ):
        # BUG-FIX: use thread-local services when provided — self.source_drive is
        # shared across ALL threads and httplib2 is NOT thread-safe, causing the
        # 240-950s migrate_perms stalls and NoneType.close crashes seen in pm2 logs.
        _src = src_drive or self.source_drive
        _dst = dst_drive or self.dest_drive

        # ── PERF-8: Discovery-First fast path ─────────────────────────────────
        # If the caller already resolved hasExplicitRoles from the files.list()
        # metadata, honour it immediately — no Drive API call required.
        #
        # has_explicit_roles=False → item inherits all ACLs from the Shared Drive
        #   root which was already migrated by migrate_drive_members().  Destination
        #   inheritance propagates those roles automatically; there is nothing to do.
        #
        # has_explicit_roles=True  → item carries at least one explicit ACL override
        #   (e.g. a specific user was granted access directly to this file/folder).
        #   Fall through to the normal permissions().list() + migrate path below.
        #
        # has_explicit_roles=None  → caller did not supply the hint (e.g. resume run
        #   from SQL where the field may not have been stored).  Fall through to the
        #   old logic which fetches permissions and checks len(perms) <= 1.
        if has_explicit_roles is False:
            logger.debug(
                f"  [PERMS-SKIP] {item_type} '{name}' hasExplicitRoles=False "
                "— no explicit ACL overrides, relying on Shared Drive inheritance."
            )
            return

        try:
            resp = _src.permissions().list(
                fileId=source_id,
                fields="permissions(id,type,role,emailAddress,domain,displayName)",
                supportsAllDrives=True,
            ).execute()
            if resp is None:
                logger.debug(f"  [ITEM-PERMS] permissions.list returned None for [{name}] — skipping")
                return
            perms = resp.get("permissions", [])
            if len(perms) <= 1:
                return
        except Exception as exc:
            logger.warning(f"  Permissions list failed [{name}]: {exc}")
            return
        # Strip temp-admin emails from item-level ACLs using the instance-level
        # skip-set built in __init__. Source admin + dest admin must never appear
        # in destination file/folder ACLs — their access is drive-root only.
        if self._perm_skip_emails:
            filtered = [
                p for p in perms
                if (p.get("emailAddress") or "").lower() not in self._perm_skip_emails
            ]
            skipped_count = len(perms) - len(filtered)
            if skipped_count:
                logger.debug(
                    f"  [ITEM-PERMS] Excluded {skipped_count} temp-admin "
                    f"permission(s) from [{name}] — admin access is drive-root only."
                )
            perms = filtered

        if len(perms) <= 1:
            return

        try:
            from permissions_migrator import EnhancedPermissionsMigrator

            pm = EnhancedPermissionsMigrator(
                _src,   # BUG-FIX: thread-local, not self.source_drive
                _dst,   # BUG-FIX: thread-local, not self.dest_drive
                self.source_domain,
                self.dest_domain,
            )

            pr = pm.migrate_permissions(
                source_id,
                dest_id,
                perms,
                shared_drive_mode=True,
            )

            for detail in pr.get("details", []):
                role           = detail.get("role", "")
                ptype          = detail.get("type", "user")
                status         = detail.get("status", "failed")
                classification = detail.get("classification", "external_domain")
                error          = detail.get("error", "")
                source_email   = detail.get("original_email", "")
                dest_email     = detail.get("target_email", "")

                if role == "owner":
                    continue

                # NOTE: Do NOT skip 'organizer' here.
                # permissions_migrator.migrate_permissions() already downgrades
                # 'organizer' → 'fileOrganizer' for all item-level ACLs before
                # making any API call (shared_drive_mode=True, FIX-1).
                # The old guard here was incorrectly dropping fileOrganizer DB
                # upserts after the downgrade had already happened.

                valid_roles = {
                    "owner", "organizer", "fileOrganizer",
                    "writer", "commenter", "reader",
                }
                valid_classifications = {
                    "internal_both_domains", "internal_source_only",
                    "external_domain", "general_access",
                }
                valid_types = {"user", "group", "domain", "anyone"}

                if role not in valid_roles:
                    continue
                if ptype not in valid_types:
                    ptype = "user"
                if classification not in valid_classifications:
                    classification = "external_domain"

                try:
                    self.mgr.upsert_permission(
                        file_id         = dest_id,
                        item_type       = item_type,
                        permission_type = ptype,
                        source_email    = source_email,
                        dest_email      = dest_email,
                        role            = role,
                        classification  = classification,
                        is_inherited    = False,
                        parent_drive_id = parent_drive_id,
                    )
                    if status == "success":
                        self.mgr.mark_permission_done(dest_id, dest_email, role)
                    elif status == "failed":
                        self.mgr.mark_permission_failed(dest_id, dest_email, role, error)
                except Exception as exc:
                    logger.debug(f"  migration_permissions upsert error [{name}]: {exc}")

            if pr.get("migrated", 0) > 0:
                logger.debug(
                    f"  Permissions [{name}]: "
                    f"{pr['migrated']} migrated, {pr.get('failed', 0)} failed"
                )

        except ImportError:
            logger.error("EnhancedPermissionsMigrator not available")
        except Exception as exc:
            logger.warning(f"  Permission migration error [{name}]: {exc}")

    # =========================================================================
    # Phase 1 worker: discover one drive + build its folder structure
    # =========================================================================

    def _discover_and_prepare_drive(
        self,
        src_id: str,
        dst_id: str,
        drive_name: str,
    ) -> Dict:
        result = {
            "source_id":       src_id,
            "dest_id":         dst_id,
            "name":            drive_name,
            "files_total":     0,
            "folders_created": 0,
            "status":          "ok",
        }

        try:
            existing = self.mgr.load_drive_items(
                source_shared_drive_id=src_id,
                dest_shared_drive_id=dst_id,
            )

            if existing:
                logger.info(
                    f"[DISC] {drive_name}: resume — {len(existing)} SQL items"
                )
                folders = [
                    r for r in existing
                    if r.mime_type == "application/vnd.google-apps.folder"
                ]
                files   = [
                    r for r in existing
                    if r.mime_type != "application/vnd.google-apps.folder"
                ]
                folder_dicts = [
                    {
                        "id":       r.file_id,
                        "name":     r.file_name,
                        "mimeType": r.mime_type,
                        "parents":  [r.source_parent_id] if r.source_parent_id else [],
                    }
                    for r in folders
                ]
                folder_mapping = self.mgr.get_folder_mapping(self.run_id)
                missing = [
                    f for f in folder_dicts
                    if f["id"] not in folder_mapping
                ]
                if missing:
                    new_fm = self._build_shared_drive_folder_structure(
                        missing, src_id, dst_id
                    )
                    folder_mapping.update(new_fm)
            else:
                logger.info(
                    f"[DISC] {drive_name}: first run — crawling drive..."
                )
                all_items = self.list_shared_drive_files(src_id)
                if not all_items:
                    return result

                self.mgr.register_discovered_items(
                    all_items,
                    source_email="",
                    dest_email="",
                    source_shared_drive_id=src_id,
                    dest_shared_drive_id=dst_id,
                )

                folders = [
                    f for f in all_items
                    if f["mimeType"] == "application/vnd.google-apps.folder"
                ]
                files = [
                    f for f in all_items
                    if f["mimeType"] != "application/vnd.google-apps.folder"
                ]

                folder_mapping = self._build_shared_drive_folder_structure(
                    folders, src_id, dst_id
                )

            # FIX Bug-3: guard write with lock so Phase-2 workers never read a
            # partially-written mapping when drives finish discovery at different times.
            with self._folder_maps_lock:
                self._folder_maps[src_id] = folder_mapping

            result["files_total"]     = len(files)
            result["folders_created"] = len(folder_mapping)
            logger.info(
                f"[DISC] {drive_name}: {len(files)} files, "
                f"{len(folder_mapping)} folders"
            )

        except Exception as exc:
            logger.error(
                f"[DISC] {drive_name} ({src_id}) failed: {exc}", exc_info=True
            )
            result["status"] = "discovery_failed"
            result["error"]  = str(exc)

        return result

    # =========================================================================
    # Thread-local Drive service builder
    # =========================================================================

    def _get_thread_drive_svc(self, kind: str = "source"):
        """
        Return a per-thread CACHED Drive v3 service (build once, reuse forever).

        FIX Bug-1: previously _build_thread_drive_svc() built a *brand-new*
        service on every single file — credential loading + httplib2.Http
        creation + Discovery fetch costs ~0.5-2 s each time.  With 14 workers
        all doing this in their first calls, all threads were blocked in setup
        instead of transferring, making migration appear single-threaded.

        Solution (mirrors migration_engine_v4._get_drive_service_for_thread):
          - Use threading.local() to store one service per thread per kind.
          - Build it only on the first call from that thread; reuse on all
            subsequent calls.  httplib2 is still NOT shared across threads
            (each thread has its own Http object), so thread-safety is preserved.
        """
        cache_attr = f"_drive_svc_{kind}"
        cached = getattr(self._thread_local, cache_attr, None)
        if cached is not None:
            return cached

        import httplib2
        from google.oauth2 import service_account as _sa
        from googleapiclient.discovery import build as _gapi_build

        creds_file  = self._src_creds_file  if kind == "source" else self._dst_creds_file
        admin_email = self._src_admin_email  if kind == "source" else self._dst_admin_email

        if not creds_file or not admin_email:
            logger.warning(
                f"[THREAD-SVC] No creds for kind={kind!r} — "
                "falling back to shared service (thread-unsafe)"
            )
            svc = self.source_drive if kind == "source" else self.dest_drive
            setattr(self._thread_local, cache_attr, svc)
            return svc

        creds = _sa.Credentials.from_service_account_file(
            creds_file,
            scopes=self.config.SCOPES,
            subject=admin_email,
        )
        try:
            import google_auth_httplib2 as _gah
            http = _gah.AuthorizedHttp(creds, http=httplib2.Http(timeout=1800))
            svc = _gapi_build("drive", "v3", http=http, cache_discovery=False)
        except ImportError:
            svc = _gapi_build("drive", "v3", credentials=creds, cache_discovery=False)

        setattr(self._thread_local, cache_attr, svc)
        logger.debug(
            f"[THREAD-SVC] Built+cached Drive svc kind={kind!r} "
            f"thread={threading.current_thread().name}"
        )
        return svc

    # Backward-compat alias so any external callers are not broken
    def _build_thread_drive_svc(self, kind: str = "source"):
        return self._get_thread_drive_svc(kind)

    # =========================================================================
    # Workspace file helpers (FIX v7: mirrors migration_engine_v4 fully)
    # =========================================================================

    def _migrate_workspace_file(
        self,
        file_id: str,
        file_name: str,
        mime_type: str,
        dest_parent_id: Optional[str],
        dest_drive_id: str,
        src_drive,
        dst_drive,
    ) -> Dict:
        """Export a Google Workspace file, with PDF fallback for oversized exports."""
        empty     = {"success": False, "dest_id": None, "ignored": False, "error": None}
        type_info = GOOGLE_WORKSPACE_TYPES.get(mime_type)

        if not type_info or not type_info.get("can_export"):
            return {**empty, "ignored": True,
                    "error": f"Non-exportable workspace type: {mime_type}",
                    "error_type": "non_exportable"}

        for attempt in range(MAX_RETRIES):
            wait   = _backoff(attempt)
            chunk  = _get_adaptive_chunk_size()
            dl_buf = None
            try:
                req    = src_drive.files().export_media(
                    fileId=file_id, mimeType=type_info["export_mime"]
                )
                dl_buf = io.BytesIO()
                try:
                    dl   = MediaIoBaseDownload(dl_buf, req, chunksize=chunk)
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

                dest_name = file_name + type_info["extension"]
                meta      = {"name": dest_name}
                if dest_parent_id:
                    meta["parents"] = [dest_parent_id]
                elif dest_drive_id:
                    meta["parents"] = [dest_drive_id]
                if type_info.get("import_mime"):
                    meta["mimeType"] = type_info["import_mime"]

                upload_buf = io.BytesIO(data)
                try:
                    use_resumable = len(data) >= 5 * 1_024 * 1_024
                    media = MediaIoBaseUpload(
                        upload_buf, mimetype=type_info["export_mime"],
                        resumable=use_resumable,
                        chunksize=chunk if use_resumable else -1,
                    )
                    resp = dst_drive.files().create(
                        body=meta, media_body=media,
                        fields="id", supportsAllDrives=True,
                    ).execute()
                finally:
                    upload_buf.close()

                dest_id = _extract_id(resp)
                if dest_id is None:
                    return {**empty, "error": f"Bad response: {resp!r}",
                            "error_type": "bad_response"}
                self.stats["memory_routed"] += 1
                return {**empty, "success": True, "dest_id": dest_id}

            except HttpError as exc:
                err  = str(exc)
                code = exc.resp.status

                # FIX-2: exportSizeLimitExceeded → retry as PDF for Presentations/Drawings
                if "exportSizeLimitExceeded" in err:
                    if "fallback_mime" in type_info:
                        logger.warning(
                            f"  [WORKSPACE] [{file_name}] exportSizeLimitExceeded — "
                            f"retrying as {type_info['fallback_ext']}"
                        )
                        return self._workspace_fallback(
                            file_id, file_name, type_info,
                            dest_parent_id, dest_drive_id, src_drive, dst_drive,
                        )
                    logger.warning(
                        f"  [WORKSPACE] [{file_name}] exportSizeLimitExceeded and no "
                        f"fallback defined for {mime_type} — marking ignored"
                    )
                    return {**empty, "ignored": True,
                            "error": f"exportSizeLimitExceeded: {err}"}

                if code in (429, 500, 503) and attempt < MAX_RETRIES - 1:
                    time.sleep(wait)
                    continue
                return {**empty, "error": err, "error_type": f"http_{code}"}

            except Exception as exc:
                err = str(exc)
                if attempt < MAX_RETRIES - 1:
                    time.sleep(wait)
                else:
                    return {**empty, "error": err,
                            "error_type": "workspace_export_failed"}

            finally:
                if dl_buf is not None:
                    try:
                        dl_buf.close()
                    except Exception:
                        pass
                    dl_buf = None

        return {**empty, "error": "Max retries exceeded",
                "error_type": "workspace_export_failed"}

    def _workspace_fallback(
        self,
        file_id: str,
        file_name: str,
        type_info: Dict,
        dest_parent_id: Optional[str],
        dest_drive_id: str,
        src_drive,
        dst_drive,
    ) -> Dict:
        """Retry a workspace export as PDF when the primary format exceeds size limit."""
        empty  = {"success": False, "dest_id": None, "ignored": False, "error": None}
        dl_buf = None
        try:
            req    = src_drive.files().export_media(
                fileId=file_id, mimeType=type_info["fallback_mime"]
            )
            dl_buf = io.BytesIO()
            try:
                dl   = MediaIoBaseDownload(dl_buf, req, chunksize=CHUNK_SIZE)
                done = False
                while not done:
                    _, done = dl.next_chunk()
                dl_buf.seek(0)
                data = dl_buf.read()
            finally:
                dl_buf.close()
                dl_buf = None

            if not data:
                return {**empty, "error": "Empty fallback export",
                        "error_type": "empty_export"}

            meta = {"name": file_name + type_info["fallback_ext"]}
            if dest_parent_id:
                meta["parents"] = [dest_parent_id]
            elif dest_drive_id:
                meta["parents"] = [dest_drive_id]

            upload_buf = io.BytesIO(data)
            try:
                use_resumable = len(data) >= 5 * 1_024 * 1_024
                media = MediaIoBaseUpload(
                    upload_buf, mimetype=type_info["fallback_mime"],
                    resumable=use_resumable,
                    chunksize=CHUNK_SIZE if use_resumable else -1,
                )
                resp = dst_drive.files().create(
                    body=meta, media_body=media,
                    fields="id", supportsAllDrives=True,
                ).execute()
            finally:
                upload_buf.close()

            dest_id = _extract_id(resp)
            if dest_id is None:
                return {**empty, "error": f"Bad response: {resp!r}",
                        "error_type": "bad_response"}
            return {**empty, "success": True, "dest_id": dest_id}

        except Exception as exc:
            return {**empty, "error": str(exc),
                    "error_type": "workspace_fallback_failed"}
        finally:
            if dl_buf is not None:
                try:
                    dl_buf.close()
                except Exception:
                    pass

    # =========================================================================
    # Phase 2: per-item file migration worker
    # =========================================================================

    def _process_queue_item(self, item) -> Dict:
        file_id      = item.file_id
        file_name    = getattr(item, "file_name",             "") or ""
        mime_type    = getattr(item, "mime_type",              "") or ""
        file_size    = int(getattr(item, "file_size_bytes",    0) or 0)
        parent_id    = getattr(item, "source_parent_id",       None)
        src_drive_id = getattr(item, "source_shared_drive_id", "") or ""

        base = {
            "success": False, "ignored": False, "skipped": False,
            "source_drive_id": src_drive_id, "file_name": file_name,
        }

        # Ignore non-migratable MIME types
        if mime_type in IGNORED_MIME_TYPES:
            self.mgr.mark_ignored(self.run_id, file_id, "Non-migratable MIME type")
            return {**base, "ignored": True}

        # FIX (v7): Hard 5 GB size limit — mirrors migration_engine_v4 FIX-7.
        # Attempting files this large via the Drive API always times out or
        # exceeds memory; mark them ignored immediately.
        if file_size > MAX_FILE_SIZE_BYTES:
            reason = f"File size {_fmt_bytes(file_size)} exceeds 5 GB limit — ignored"
            logger.warning(f"[SIZE-LIMIT] {file_name} ({file_id}): {reason}")
            self.mgr.mark_ignored(self.run_id, file_id, reason)
            return {**base, "ignored": True}

        should_skip, _ = self.mgr.should_skip_item(file_id)
        if should_skip:
            return {**base, "skipped": True}

        fm           = self._folder_maps.get(src_drive_id, {})
        dst_drive_id = getattr(item, "dest_shared_drive_id", "") or ""

        _at_drive_root = (not parent_id) or (parent_id == src_drive_id)

        if _at_drive_root:
            # FIX-A: Root-level files must land in the Shared Drive root.
            # Passing None here causes the Drive API to place the file in the
            # Service Account's personal "My Drive" instead of the Shared Drive.
            # Always set parent = dest Shared Drive ID for root-level items.
            dest_parent = dst_drive_id
        else:
            dest_parent = fm.get(parent_id)
            if not dest_parent:
                # Safety lock: nested file's parent folder not yet created.
                # Block migration rather than letting the file land at drive root.
                reason = (
                    f"Safety lock: dest_folder_id missing for source parent "
                    f"'{parent_id}'. Phase-1 folder creation may be incomplete. "
                    "Re-run migration to retry after the parent folder is created."
                )
                logger.error(f"  [PARENT-LOCK] {file_name} ({file_id}): {reason}")
                self.mgr.mark_failed(self.run_id, file_id, reason)
                return {**base, "error": reason, "error_type": "missing_dest_folder"}

        # Build thread-local Drive services — httplib2 is NOT thread-safe.
        thread_src_drive = self._build_thread_drive_svc("source")
        thread_dst_drive = self._build_thread_drive_svc("dest")

        self.mgr.mark_in_progress(self.run_id, file_id)

        # FIX (v7): route workspace files through proper workspace handler
        # (with PDF fallback) instead of through the binary memory path.
        if mime_type in GOOGLE_WORKSPACE_TYPES:
            res = self._migrate_workspace_file(
                file_id, file_name, mime_type,
                dest_parent, dst_drive_id,
                src_drive=thread_src_drive, dst_drive=thread_dst_drive,
            )
        elif file_size >= LARGE_FILE_THRESHOLD_BYTES:
            res = self._migrate_via_gcs(
                file_id, file_name, mime_type, file_size, dest_parent, dst_drive_id,
                src_drive=thread_src_drive, dst_drive=thread_dst_drive,
            )
        else:
            res = self._migrate_via_memory(
                file_id, file_name, mime_type, file_size, dest_parent, dst_drive_id,
                src_drive=thread_src_drive, dst_drive=thread_dst_drive,
            )

        if res["success"]:
            dest_id = res.get("dest_id")
            self.mgr.mark_done(
                self.run_id, file_id,
                dest_item_id=dest_id,
                dest_parent_id=dest_parent,
            )
            if dest_id:
                # ── PERF-8: Discovery-First permission gate ────────────────────
                # Retrieve hasExplicitRoles that was stored on the queue item
                # by register_discovered_items() (sourced from files.list fields).
                # If the state manager didn't persist this field (older schema or
                # resume run) item.has_explicit_roles will be None — fall through
                # to the normal flow inside _migrate_item_permissions which uses
                # the len(perms) <= 1 guard as the fallback safety net.
                has_explicit = getattr(item, "has_explicit_roles", None)

                if has_explicit is False:
                    # Item's ACL is purely inherited from the Shared Drive root.
                    # Drive-level members were already migrated by
                    # migrate_drive_members(); no per-item API call needed.
                    self.stats["perms_skipped_inherited"] += 1
                    logger.debug(
                        f"  [PERMS-SKIP] FILE '{file_name}' hasExplicitRoles=False "
                        "— relying on Shared Drive inheritance (saved 1 API call)"
                    )
                else:
                    if has_explicit is True:
                        self.stats["perms_explicit_migrated"] += 1
                    self._migrate_item_permissions(
                        file_id, dest_id, file_name, "FILE", dst_drive_id,
                        src_drive=thread_src_drive, dst_drive=thread_dst_drive,
                        has_explicit_roles=has_explicit,
                    )
            return {**base, "success": True, "dest_id": dest_id}

        elif res.get("ignored"):
            self.mgr.mark_ignored(self.run_id, file_id, res.get("error", ""))
            return {**base, "ignored": True}
        else:
            err = res.get("error", "Unknown")
            self.mgr.mark_failed(self.run_id, file_id, err)
            return {**base, "error": err, "error_type": res.get("error_type", "")}

    # ─────────────────────────────────────────────────────────────────────────
    # Memory path  (< LARGE_FILE_THRESHOLD_BYTES, binary files only)
    # ─────────────────────────────────────────────────────────────────────────

    def _migrate_via_memory(
        self,
        file_id: str,
        file_name: str,
        mime_type: str,
        file_size: int,
        dest_parent_id: Optional[str],
        dest_drive_id: str = "",
        src_drive=None,
        dst_drive=None,
    ) -> Dict:
        empty      = {"success": False, "dest_id": None, "ignored": False, "error": None}
        last_error = ""

        _src = src_drive or self.source_drive
        _dst = dst_drive or self.dest_drive

        if mime_type == "application/vnd.google-apps.folder":
            return {**empty, "ignored": True, "error": "Folder in file queue",
                    "error_type": "folder_in_queue"}

        for attempt in range(MAX_RETRIES):
            wait   = _backoff(attempt)
            chunk  = _get_adaptive_chunk_size()   # FIX (v7): now returns real scaled sizes
            dl_buf = None                          # FIX: reset at TOP of every attempt
            try:
                request   = _src.files().get_media(
                    fileId=file_id, supportsAllDrives=True, acknowledgeAbuse=True
                )
                dest_name = file_name
                up_mime   = mime_type

                dl_buf = io.BytesIO()
                try:
                    dl   = MediaIoBaseDownload(dl_buf, request, chunksize=chunk)
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
                        meta = {"name": dest_name}
                        if dest_parent_id:
                            meta["parents"] = [dest_parent_id]
                        elif dest_drive_id:
                            meta["parents"] = [dest_drive_id]
                        resp    = _dst.files().create(
                            body=meta, fields="id", supportsAllDrives=True
                        ).execute()
                        dest_id = _extract_id(resp)
                        self.stats["memory_routed"] += 1
                        return {**empty, "success": True, "dest_id": dest_id}
                    last_error = "Empty download for non-zero file"
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(wait)
                        continue
                    return {**empty, "error": last_error,
                            "error_type": "empty_download"}

                meta = {"name": dest_name}
                if dest_parent_id:
                    meta["parents"] = [dest_parent_id]
                elif dest_drive_id:
                    meta["parents"] = [dest_drive_id]

                upload_buf = io.BytesIO(data)
                try:
                    use_resumable = len(data) >= 5 * 1_024 * 1_024
                    media = MediaIoBaseUpload(
                        upload_buf, mimetype=up_mime,
                        resumable=use_resumable,
                        chunksize=chunk if use_resumable else -1,
                    )
                    resp = _dst.files().create(
                        body=meta, media_body=media,
                        fields="id", supportsAllDrives=True,
                    ).execute()
                finally:
                    upload_buf.close()

                dest_id = _extract_id(resp)
                if dest_id is None:
                    return {**empty, "error": f"Bad response: {resp!r}",
                            "error_type": "bad_response"}

                self.stats["memory_routed"] += 1
                logger.debug(f"  [MEM] {file_name} ({_fmt_bytes(file_size)})")
                return {**empty, "success": True, "dest_id": dest_id}

            except HttpError as exc:
                code       = exc.resp.status
                last_error = str(exc)

                if code == 403 and any(
                    k in last_error for k in (
                        "cannotDownload", "fileNotDownloadable",
                        "cannotDownloadAbusiveFile", "exportSizeLimitExceeded",
                    )
                ):
                    return {**empty, "ignored": True, "error": "Download restricted",
                            "error_type": "download_restricted"}

                if code in (429, 500, 503) and attempt < MAX_RETRIES - 1:
                    logger.warning(
                        f"  [MEM] HTTP {code} retry {attempt+1} [{file_name}]"
                        f" wait {wait:.1f}s"
                    )
                    time.sleep(wait)
                    continue

                logger.error(f"  [MEM] HTTP {code} [{file_name}]: {last_error}")
                return {**empty, "error": last_error, "error_type": f"http_{code}"}

            except (ConnectionResetError, ConnectionError, OSError, TimeoutError) as exc:
                last_error = str(exc)
                if attempt < MAX_RETRIES - 1:
                    logger.warning(
                        f"  [MEM] Network retry {attempt+1}/{MAX_RETRIES}"
                        f" [{file_name}]: {last_error}"
                    )
                    time.sleep(wait)
                else:
                    logger.error(f"  [MEM] Exhausted [{file_name}]: {last_error}")

            except Exception as exc:
                last_error = str(exc)
                logger.error(
                    f"  [MEM] Unexpected [{file_name}]: {last_error}", exc_info=True
                )
                return {**empty, "error": last_error, "error_type": "unexpected"}

            finally:
                if dl_buf is not None:
                    try:
                        dl_buf.close()
                    except Exception:
                        pass

        return {**empty, "error": last_error, "error_type": "memory_transfer_failed"}

    # ─────────────────────────────────────────────────────────────────────────
    # GCS path  (>= LARGE_FILE_THRESHOLD_BYTES)
    # Hard per-file timeout (GCS_FILE_TIMEOUT seconds) on each attempt.
    # ─────────────────────────────────────────────────────────────────────────

    def _migrate_via_gcs(
        self,
        file_id: str,
        file_name: str,
        mime_type: str,
        file_size: int,
        dest_parent_id: Optional[str],
        dest_drive_id: str = "",
        src_drive=None,
        dst_drive=None,
    ) -> Dict:
        empty       = {"success": False, "dest_id": None, "ignored": False, "error": None}
        last_error  = ""
        active_blob = None

        _src = src_drive or self.source_drive
        _dst = dst_drive or self.dest_drive

        export_info = GOOGLE_WORKSPACE_TYPES.get(mime_type)
        # For GCS path, only binary files reach here (workspace files routed earlier)
        # but keep the export logic as safety net.
        if export_info and export_info.get("can_export"):
            export_mime = export_info["export_mime"]
            import_mime = export_info.get("import_mime")
            dest_name   = file_name + export_info["extension"]
            up_mime     = export_mime
        else:
            export_mime = None
            import_mime = None
            dest_name   = file_name
            up_mime     = mime_type

        for attempt in range(MAX_RETRIES):
            wait         = _backoff(attempt)
            attempt_blob = f"{self.run_id}/{file_id}/attempt_{attempt}"
            active_blob  = None

            try:
                # ── Download: Drive → GCS (with hard timeout) ─────────────────
                # FIX-GCS-3: RuntimeError guard — if PM2 sends SIGTERM while
                # this worker is mid-flight the outer pool's __exit__ begins
                # interpreter teardown; sub_pool.submit() then raises
                # RuntimeError "cannot schedule new futures after interpreter
                # shutdown".  Catch it and return a clean failure so the outer
                # pool drains without an unhandled exception in pm2 logs.
                try:
                    with _cf.ThreadPoolExecutor(max_workers=1) as sub_pool:
                        dl_future = sub_pool.submit(
                            self.mgr.download_drive_to_gcs,
                            drive_svc   = _src,
                            file_id     = file_id,
                            file_name   = file_name,
                            run_id      = attempt_blob,
                            mime_type   = up_mime,
                            export_mime = export_mime,
                        )
                        try:
                            ok, blob_name, err = dl_future.result(timeout=GCS_FILE_TIMEOUT)
                        except _cf.TimeoutError:
                            last_error = (
                                f"GCS download timed out after {GCS_FILE_TIMEOUT}s "
                                f"[{_fmt_bytes(file_size)}]"
                            )
                            logger.warning(
                                f"  [GCS] [{file_name}] attempt {attempt+1}: {last_error}"
                            )
                            if attempt < MAX_RETRIES - 1:
                                time.sleep(wait)
                                continue
                            return {**empty, "error": last_error,
                                    "error_type": "gcs_timeout"}
                except RuntimeError as _shutdown_exc:
                    # Interpreter is shutting down (PM2 restart / SIGKILL).
                    logger.warning(
                        f"  [GCS] Executor shutdown during download [{file_name}]: "
                        f"{_shutdown_exc}"
                    )
                    return {**empty, "error": "Executor shutdown during GCS download",
                            "error_type": "executor_shutdown"}

                if not ok:
                    last_error = err or "GCS download failed"
                    if blob_name is not None:
                        try:
                            self.mgr.delete_temp(blob_name)
                        except Exception:
                            pass
                    if attempt < MAX_RETRIES - 1:
                        logger.warning(
                            f"  [GCS] Download failed {attempt+1}/{MAX_RETRIES}"
                            f" [{file_name}]: {last_error} — retry {wait:.1f}s"
                        )
                        time.sleep(wait)
                        continue
                    return {**empty, "error": last_error,
                            "error_type": "gcs_download_failed"}

                active_blob = blob_name

                # ── Upload: GCS → Drive (with hard timeout) ───────────────────
                # FIX-GCS-1: consume one token before initiating the resumable
                # upload.  All 14 worker threads share _GCS_UPLOAD_BUCKET so
                # the project-wide rate stays at _GCS_UPLOAD_RATE/s regardless
                # of thread count — prevents userRateLimitExceeded 403 storms.
                _GCS_UPLOAD_BUCKET.consume()

                # FIX-GCS-3: RuntimeError guard (same reason as download block).
                try:
                    with _cf.ThreadPoolExecutor(max_workers=1) as sub_pool:
                        ul_future = sub_pool.submit(
                            self.mgr.upload_gcs_to_drive,
                            drive_svc   = _dst,
                            blob_name   = blob_name,
                            file_name   = dest_name,
                            mime_type   = up_mime,
                            parent_id   = dest_parent_id,   # None when at drive root
                            import_mime = import_mime,
                            drive_id    = dest_drive_id,    # used in URL when parent_id is None
                        )
                        try:
                            ok2, dest_id, err2 = ul_future.result(timeout=GCS_FILE_TIMEOUT)
                        except _cf.TimeoutError:
                            last_error = (
                                f"GCS upload timed out after {GCS_FILE_TIMEOUT}s "
                                f"[{_fmt_bytes(file_size)}]"
                            )
                            logger.warning(
                                f"  [GCS] [{file_name}] attempt {attempt+1}: {last_error}"
                            )
                            if active_blob:
                                try:
                                    self.mgr.delete_temp(active_blob)
                                except Exception:
                                    pass
                                active_blob = None
                            if attempt < MAX_RETRIES - 1:
                                time.sleep(wait)
                                continue
                            return {**empty, "error": last_error,
                                    "error_type": "gcs_timeout"}
                except RuntimeError as _shutdown_exc:
                    # Interpreter is shutting down (PM2 restart / SIGKILL).
                    logger.warning(
                        f"  [GCS] Executor shutdown during upload [{file_name}]: "
                        f"{_shutdown_exc}"
                    )
                    if active_blob is not None:
                        try:
                            self.mgr.delete_temp(active_blob)
                        except Exception:
                            pass
                    return {**empty, "error": "Executor shutdown during GCS upload",
                            "error_type": "executor_shutdown"}

                if not ok2:
                    last_error = err2 or "GCS upload failed"
                    # FIX-GCS-2: 403 userRateLimitExceeded means the quota window
                    # is full — double the retry wait so it has time to drain before
                    # the next attempt, instead of hammering it again immediately.
                    _is_rate_limited = (
                        "userRateLimitExceeded" in (last_error or "")
                        or "User rate limit exceeded" in (last_error or "")
                        or "userRateLimitExceeded" in (err2 or "")
                    )
                    if active_blob is not None:
                        try:
                            self.mgr.delete_temp(active_blob)
                        except Exception:
                            pass
                        active_blob = None
                    if attempt < MAX_RETRIES - 1:
                        retry_wait = wait * 2 if _is_rate_limited else wait
                        logger.warning(
                            f"  [GCS] Upload failed {attempt+1}/{MAX_RETRIES}"
                            f" [{file_name}]: {last_error} — "
                            f"{'rate-limit; ' if _is_rate_limited else ''}"
                            f"retry {retry_wait:.1f}s"
                        )
                        time.sleep(retry_wait)
                        continue
                    return {**empty, "error": last_error,
                            "error_type": "gcs_upload_failed"}

                if active_blob is not None:
                    try:
                        self.mgr.delete_temp(active_blob)
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
                        self.mgr.delete_temp(active_blob)
                    except Exception:
                        pass
                    active_blob = None
                if attempt < MAX_RETRIES - 1:
                    logger.warning(
                        f"  [GCS] Connection error {attempt+1}/{MAX_RETRIES}"
                        f" [{file_name}]: {last_error}"
                    )
                    time.sleep(wait)
                else:
                    logger.error(f"  [GCS] Exhausted [{file_name}]: {last_error}")

            except Exception as exc:
                last_error = str(exc)
                logger.error(
                    f"  [GCS] Unexpected [{file_name}]: {last_error}", exc_info=True
                )
                if active_blob is not None:
                    try:
                        self.mgr.delete_temp(active_blob)
                    except Exception:
                        pass
                break

        if active_blob is not None:
            try:
                self.mgr.delete_temp(active_blob)
            except Exception:
                pass

        return {**empty, "error": last_error, "error_type": "gcs_transfer_failed"}

    # =========================================================================
    # MAIN: Migrate all (or filtered) Shared Drives
    # =========================================================================

    def migrate_all_shared_drives(
        self,
        drive_filter: List[str] = None,
        drive_id_mapping: Dict[str, str] = None,
        resume: bool = False,
    ) -> Dict:
        summary = {
            "total_drives":             0,
            "drives_migrated":          0,
            "drives_failed":            0,
            "total_files_migrated":     0,
            "total_files_failed":       0,
            "total_files_ignored":      0,
            "total_files_skipped":      0,
            "total_folders_created":    0,
            "total_members_migrated":   0,
            "total_temp_memberships":   0,
            "drive_results":            [],
        }

        # ── Resolve drive triples ─────────────────────────────────────────────
        drive_triples: List[Tuple[str, str, str]] = []

        if drive_id_mapping:
            logger.info(
                f"ID-based Shared Drive migration: "
                f"{len(drive_id_mapping)} drive pair(s) from CSV"
            )
            for src_id, dst_id in drive_id_mapping.items():
                try:
                    drv_meta   = self.source_drive.drives().get(
                        driveId=src_id, fields="name"
                    ).execute()
                    drive_name = drv_meta.get("name", src_id)
                except Exception:
                    drive_name = src_id

                dest_check = self._verify_or_create_dest_drive_by_id(
                    dst_id, drive_name, src_id
                )
                if not dest_check:
                    logger.error(
                        f"Cannot access or create dest drive {dst_id} — skipping {src_id}"
                    )
                    summary["drives_failed"] += 1
                    summary["drive_results"].append({
                        "name": drive_name, "source_id": src_id, "dest_id": dst_id,
                        "status": "failed", "files_migrated": 0, "files_failed": 0,
                        "files_ignored": 0, "folders_created": 0, "members_migrated": 0,
                    })
                    continue
                drive_triples.append((src_id, dest_check, drive_name))

        else:
            source_drives = self.list_source_shared_drives()
            if drive_filter:
                source_drives = [d for d in source_drives if d["name"] in drive_filter]
                logger.info(f"Filtered to {len(source_drives)} drives by name")
            for drive in source_drives:
                src_id     = drive["id"]
                drive_name = drive["name"]
                dest_id    = self.create_dest_shared_drive(drive_name, src_id)
                if not dest_id:
                    logger.error(f"Could not create/find dest drive: {drive_name}")
                    summary["drives_failed"] += 1
                    summary["drive_results"].append({
                        "name": drive_name, "source_id": src_id, "dest_id": None,
                        "status": "failed", "files_migrated": 0, "files_failed": 0,
                        "files_ignored": 0, "folders_created": 0, "members_migrated": 0,
                    })
                    continue
                drive_triples.append((src_id, dest_id, drive_name))

        self.stats["drives_total"] = len(drive_triples) + summary["drives_failed"]
        summary["total_drives"]    = self.stats["drives_total"]

        if not drive_triples:
            logger.warning("No drives available for migration after setup.")
            return summary

        # ── Ensure SOURCE admin access (PERF-4: parallel, was serial for-loop) ──
        temp_perms: Dict[str, Optional[str]] = {}       # src_id → perm_id (None = pre-existing)
        temp_perms_lock = threading.Lock()

        # ── Ensure DESTINATION admin access (TEMPORARY — mirrors CloudM behaviour) ──
        # Tracks: whether admin was pre-existing, original role, and new perm_id to revoke.
        # Structure per dst_id: {"was_temp": bool, "perm_id": str|None, "original_role": str|None}
        dest_temp_perms: Dict[str, Dict] = {}
        dest_temp_perms_lock = threading.Lock()

        logger.info(
            f"[SRC-MEMBERSHIP] Checking source admin membership for "
            f"{len(drive_triples)} source drive(s) "
            f"({min(PREFLIGHT_WORKERS, len(drive_triples))} workers)..."
        )
        logger.info(
            f"[DEST-MEMBERSHIP] Checking destination admin membership for "
            f"{len(drive_triples)} destination drive(s) "
            f"({min(PREFLIGHT_WORKERS, len(drive_triples))} workers)..."
        )

        def _preflight_one(triple):
            src_id, dst_id, drive_name = triple
            # Source admin (existing logic)
            was_temp, perm_id = self._ensure_admin_access(src_id, drive_name)
            with temp_perms_lock:
                temp_perms[src_id] = perm_id if was_temp else None
            # Destination admin (new: temporary access)
            d_was_temp, d_perm_id, d_orig_role = self._ensure_dest_admin_organizer(
                dst_id, drive_name
            )
            with dest_temp_perms_lock:
                dest_temp_perms[dst_id] = {
                    "was_temp":      d_was_temp,
                    "perm_id":       d_perm_id,
                    "original_role": d_orig_role,
                }

        with ThreadPoolExecutor(max_workers=min(PREFLIGHT_WORKERS, len(drive_triples))) as pool:
            # Use submit+as_completed instead of pool.map so a single drive failure
            # does NOT abort the remaining preflight tasks (pool.map re-raises on
            # first exception, blocking all other drives from getting access).
            _pf_futures = {pool.submit(_preflight_one, t): t for t in drive_triples}
            for _pf_f in as_completed(_pf_futures):
                try:
                    _pf_f.result()
                except Exception as _pf_exc:
                    _t = _pf_futures[_pf_f]
                    logger.warning(
                        f"[PREFLIGHT] Drive '{_t[2]}' preflight error "
                        f"(non-fatal, migration continues): {_pf_exc}"
                    )

        temp_count = sum(1 for p in temp_perms.values() if p is not None)
        dest_temp_count = sum(
            1 for d in dest_temp_perms.values() if d.get("was_temp")
        )
        summary["total_temp_memberships"] = temp_count
        if temp_count:
            logger.info(
                f"[SRC-MEMBERSHIP] Temporary manager access granted for "
                f"{temp_count} source drive(s). Will revoke after migration."
            )
        if dest_temp_count:
            logger.info(
                f"[DEST-MEMBERSHIP] Temporary organizer access granted for "
                f"{dest_temp_count} destination drive(s). Will revoke after migration."
            )

        # ── Migrate drive-level members (PERF-5: parallel, was serial for-loop) ──
        member_results: Dict[str, Dict] = {}
        member_results_lock = threading.Lock()
        member_stats_delta  = [0]  # accumulated migrated count, updated under lock

        def _migrate_members_one(triple):
            src_id, dst_id, drive_name = triple
            self.mgr.upsert_shared_drive(self.run_id, src_id, drive_name)
            mr = self.migrate_drive_members(src_id, dst_id, drive_name)
            with member_results_lock:
                member_results[src_id]     = mr
                member_stats_delta[0]     += mr.get("migrated", 0)

        with ThreadPoolExecutor(max_workers=min(MEMBER_WORKERS, len(drive_triples))) as pool:
            _mb_futures = {pool.submit(_migrate_members_one, t): t for t in drive_triples}
            for _mb_f in as_completed(_mb_futures):
                try:
                    _mb_f.result()
                except Exception as _mb_exc:
                    _t = _mb_futures[_mb_f]
                    logger.warning(
                        f"[MEMBER-MIGRATE] Drive '{_t[2]}' member migration error "
                        f"(non-fatal): {_mb_exc}"
                    )

        self.stats["members_migrated"]        = member_stats_delta[0]
        summary["total_members_migrated"]     = member_stats_delta[0]

        # ── PHASE 1: Parallel discovery + folder creation ─────────────────────
        logger.info(
            f"[PHASE-1] Discovering {len(drive_triples)} drives "
            f"with {min(DISCOVERY_WORKERS, len(drive_triples))} workers..."
        )
        disc_results: Dict[str, Dict] = {}

        n_disc = min(DISCOVERY_WORKERS, len(drive_triples))
        with ThreadPoolExecutor(max_workers=n_disc) as pool:
            futures = {
                pool.submit(
                    self._discover_and_prepare_drive, src_id, dst_id, drive_name
                ): (src_id, dst_id, drive_name)
                for src_id, dst_id, drive_name in drive_triples
            }
            for future in as_completed(futures):
                src_id, dst_id, drive_name = futures[future]
                try:
                    res = future.result()
                    disc_results[src_id] = res
                    logger.info(
                        f"[PHASE-1] {drive_name}: "
                        f"{res.get('files_total', 0)} files, "
                        f"{res.get('folders_created', 0)} folders"
                    )
                except Exception as exc:
                    logger.error(
                        f"[PHASE-1] Discovery failed {drive_name}: {exc}",
                        exc_info=True,
                    )
                    disc_results[src_id] = {
                        "status":    "discovery_failed",
                        "error":     str(exc),
                        "source_id": src_id,
                        "dest_id":   dst_id,
                        "name":      drive_name,
                    }

        # ── PHASE 2: Reset IN_PROGRESS rows left by a previous crash ─────────
        try:
            _reset_conn = self.mgr.get_conn()
            try:
                _reset_cur = _reset_conn.cursor()
                _reset_cur.execute(
                    "UPDATE migration_items "
                    "SET status='PENDING', error_message=NULL "
                    "WHERE migration_id=%s AND status='IN_PROGRESS' AND is_folder=0",
                    (self.run_id,),
                )
                _reset_n = _reset_cur.rowcount
                _reset_conn.commit()
                if _reset_n:
                    logger.warning(
                        f"[PHASE-2] Reset {_reset_n} IN_PROGRESS→PENDING for "
                        f"run_id={self.run_id} (leftover from previous crashed attempt)"
                    )
            finally:
                try:
                    _reset_conn.close()
                except Exception:
                    pass
        except Exception as _reset_exc:
            logger.warning(f"[PHASE-2] IN_PROGRESS reset failed (safe fallback): {_reset_exc}")

        # ── PHASE 2: XL-first two-pass queue ─────────────────────────────────
        pending = self.mgr.get_all_pending_items(self.run_id)

        # FIX (v7): Sort largest-first before the XL/regular split so XL jobs
        # start immediately — mirrors migration_engine_v4's sort() call.
        pending.sort(
            key=lambda r: int(getattr(r, "file_size_bytes", None) or 0),
            reverse=True,
        )

        xl_items  = [r for r in pending
                     if int(getattr(r, "file_size_bytes", None) or 0) >= XLARGE_FILE_THRESHOLD_BYTES]
        reg_items = [r for r in pending
                     if int(getattr(r, "file_size_bytes", None) or 0) <  XLARGE_FILE_THRESHOLD_BYTES]

        logger.info(
            f"[PHASE-2] {len(pending)} files pending | "
            f"XL(>{XLARGE_FILE_THRESHOLD_BYTES // (1024*1024)} MB): "
            f"{len(xl_items)} × {XLARGE_WORKERS} workers | "
            f"regular: {len(reg_items)} × {GLOBAL_WORKERS} workers"
        )

        file_results: Dict[str, Dict] = {}
        file_results_lock = threading.Lock()
        done_count = [0]

        def _drain(items: list, max_workers: int, label: str):
            if not items:
                return
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
                            done_count[0] += 1
                            if done_count[0] % 50 == 0:
                                logger.info(
                                    f"[PHASE-2/{label}] "
                                    f"{done_count[0]}/{len(pending)} complete"
                                )
                    except Exception as exc:
                        logger.error(
                            f"[PHASE-2/{label}] Future error [{item.file_name}]: {exc}",
                            exc_info=True,
                        )
                        with file_results_lock:
                            file_results[item.file_id] = {
                                "success":         False,
                                "error":           str(exc),
                                "error_type":      "future_error",
                                "source_drive_id": getattr(
                                    item, "source_shared_drive_id", ""
                                ),
                            }

        # Pass 1: XL files first with dedicated pool (blocks until all XL done)
        if xl_items:
            logger.info(
                f"[PHASE-2/XL] Starting {len(xl_items)} XL files "
                f"({XLARGE_WORKERS} workers)..."
            )
            _drain(xl_items, XLARGE_WORKERS, "XL")
            logger.info(
                f"[PHASE-2/XL] All XL files finished — "
                f"switching to full {GLOBAL_WORKERS}-worker pool"
            )

        # Pass 2: remaining files with full pool
        _drain(reg_items, GLOBAL_WORKERS, "REG")

        # ── Post-migration cleanup (PERF-6: both loops merged + parallelised) ──
        # Cleanup covers THREE actions per drive:
        #   1. Revoke DESTINATION admin temp organizer (if we added it; skip if pre-existing)
        #   2. Revoke SOURCE admin temp manager        (existing behaviour)
        # Both run in parallel across drives.
        logger.info(
            "[CLEANUP] Revoking temporary source + destination admin access "
            f"({min(CLEANUP_WORKERS, len(drive_triples))} workers)..."
        )

        def _cleanup_one(triple):
            src_id, dst_id, drive_name = triple

            # ── Revoke destination admin temp organizer (if we added it) ──────
            d_info = dest_temp_perms.get(dst_id, {})
            if d_info.get("was_temp") and d_info.get("perm_id"):
                try:
                    self._revoke_dest_admin_access(dst_id, drive_name, d_info["perm_id"])
                except Exception as exc:
                    logger.warning(
                        f"[CLEANUP] dest temp organizer revoke failed for "
                        f"'{drive_name}': {exc}"
                    )
            elif not d_info.get("was_temp") and d_info.get("original_role"):
                logger.debug(
                    f"[CLEANUP] Dest admin was pre-existing on '{drive_name}' "
                    f"(role='{d_info['original_role']}') — not revoking."
                )

            # ── Revoke source admin temp manager ─────────────────────────────
            perm_id = temp_perms.get(src_id)
            if perm_id:
                try:
                    self._revoke_admin_access(src_id, drive_name, perm_id)
                except Exception as exc:
                    logger.warning(
                        f"[CLEANUP] source revoke failed for '{drive_name}': {exc}"
                    )

        with ThreadPoolExecutor(max_workers=min(CLEANUP_WORKERS, len(drive_triples))) as pool:
            _cl_futures = {pool.submit(_cleanup_one, t): t for t in drive_triples}
            for _cl_f in as_completed(_cl_futures):
                try:
                    _cl_f.result()
                except Exception as _cl_exc:
                    _t = _cl_futures[_cl_f]
                    logger.warning(
                        f"[CLEANUP] Drive '{_t[2]}' cleanup error "
                        f"(non-fatal, check permissions manually): {_cl_exc}"
                    )

        # ── Aggregate results per drive ────────────────────────────────────────
        per_drive: Dict[str, Dict] = {
            src_id: {
                "name":                    drive_name,
                "source_id":               src_id,
                "dest_id":                 dst_id,
                "status":                  "failed",
                "files_migrated":          0,
                "files_failed":            0,
                "files_skipped":           0,
                "files_ignored":           0,
                "folders_created":         disc_results.get(src_id, {}).get("folders_created", 0),
                "members_migrated":        member_results.get(src_id, {}).get("migrated", 0),
                "src_temp_manager":        temp_perms.get(src_id) is not None,
                "dest_temp_organizer":     dest_temp_perms.get(dst_id, {}).get("was_temp", False),
                "dest_admin_pre_existing": not dest_temp_perms.get(dst_id, {}).get("was_temp", False)
                                           and dest_temp_perms.get(dst_id, {}).get("original_role") is not None,
            }
            for src_id, dst_id, drive_name in drive_triples
        }

        for fid, res in file_results.items():
            drv = res.get("source_drive_id", "")
            if drv not in per_drive:
                continue
            agg = per_drive[drv]
            if res.get("skipped"):
                agg["files_skipped"] += 1
            elif res.get("ignored"):
                agg["files_ignored"] += 1
            elif res.get("success"):
                agg["files_migrated"] += 1
                self.stats["files_migrated"] += 1
            else:
                agg["files_failed"] += 1
                self.stats["files_failed"] += 1

        for src_id, agg in per_drive.items():
            disc = disc_results.get(src_id, {})
            if disc.get("status") == "discovery_failed":
                agg["status"] = "failed"
                summary["drives_failed"] += 1
                try:
                    self.mgr.finish_shared_drive(self.run_id, src_id, "failed")
                except Exception:
                    pass
            else:
                agg["status"] = (
                    "completed" if agg["files_failed"] == 0 else "partial"
                )
                summary["drives_migrated"] += 1
                try:
                    self.mgr.finish_shared_drive(
                        self.run_id, src_id, "completed",
                        files_total=agg["files_migrated"] + agg["files_failed"],
                        files_done=agg["files_migrated"],
                    )
                except Exception:
                    pass

            summary["total_files_migrated"]  += agg["files_migrated"]
            summary["total_files_failed"]    += agg["files_failed"]
            summary["total_files_ignored"]   += agg["files_ignored"]
            summary["total_files_skipped"]   += agg["files_skipped"]
            summary["total_folders_created"] += agg["folders_created"]
            summary["drive_results"].append(agg)

            icon     = "✓" if agg["status"] == "completed" else "✗"
            src_tag  = " [src-temp-manager]"       if agg.get("src_temp_manager")        else ""
            dst_tag  = " [dest-temp-organizer]"    if agg.get("dest_temp_organizer")     else ""
            pre_tag  = " [dest-admin-pre-existing]" if agg.get("dest_admin_pre_existing") else ""
            logger.info(
                f"  {icon} {agg['name']} ({src_id}){src_tag}{dst_tag}{pre_tag}: "
                f"{agg['files_migrated']} migrated | "
                f"{agg['files_failed']} failed | "
                f"{agg['folders_created']} folders | "
                f"{agg['members_migrated']} members"
            )

        logger.info(
            f"[DONE] Shared Drive migration complete | "
            f"drives={summary['drives_migrated']}/{summary['total_drives']} | "
            f"files={summary['total_files_migrated']} migrated, "
            f"{summary['total_files_failed']} failed | "
            f"GCS={self.stats['gcs_routed']} MEM={self.stats['memory_routed']} | "
            f"src_temp_managers_used={summary['total_temp_memberships']} | "
            f"dest_temp_organizers_added={dest_temp_count} | "
            f"dest_temp_organizers_revoked={dest_temp_count} | "
            f"[PERF-8] perms_skipped_inherited={self.stats['perms_skipped_inherited']} "
            f"perms_explicit_migrated={self.stats['perms_explicit_migrated']}"
        )

        return summary

    # =========================================================================
    # Dest drive verification helper (ID-mapped mode)
    # =========================================================================

    def _verify_or_create_dest_drive_by_id(
        self,
        dest_drive_id: str,
        drive_name: str,
        source_drive_id: str,
    ) -> Optional[str]:
        """
        Verify the pre-mapped destination Shared Drive is accessible.

        BUG-FIX (v10): In ID-mapped mode (drive_id_mapping CSV) this method
        must NEVER create a new Shared Drive.  The previous code called
        create_dest_shared_drive() on any 404, which silently created a brand-
        new drive named after the SOURCE drive (e.g. 'z') instead of writing
        into the already-existing destination drive ('hemant').

        Root causes fixed:
          1. drives().get() was called WITHOUT useDomainAdminAccess=True so the
             dest service account got a 403/404 even when the drive existed,
             triggering the spurious creation.
          2. On any error the old code created; now it only logs + returns None.
        """
        # Try with admin access first, then without (some SA setups reject the flag)
        for use_admin in (True, False):
            try:
                kwargs = {"driveId": dest_drive_id, "fields": "id,name"}
                if use_admin:
                    kwargs["useDomainAdminAccess"] = True
                meta = self.dest_drive.drives().get(**kwargs).execute()
                actual_name = meta.get("name", dest_drive_id)
                logger.info(
                    f"[DEST-VERIFY] ✓ Destination drive accessible: "
                    f"'{actual_name}' ({dest_drive_id}) "
                    f"← source='{drive_name}' | useDomainAdminAccess={use_admin}"
                )
                return dest_drive_id

            except HttpError as exc:
                code = exc.resp.status
                if code == 400 and use_admin:
                    # SA doesn't support useDomainAdminAccess on drives.get — retry without
                    logger.debug(
                        f"[DEST-VERIFY] drives.get useDomainAdminAccess=True → 400 "
                        f"for {dest_drive_id}, retrying without flag"
                    )
                    continue
                if code == 404:
                    logger.error(
                        f"[DEST-VERIFY] ✗ Destination drive NOT FOUND: {dest_drive_id} "
                        f"(mapped destination for source '{drive_name}'). "
                        "Verify your drive_id_mapping CSV — the destination ID must "
                        "already exist in the destination Google Workspace domain. "
                        "This migration will SKIP this drive pair."
                    )
                    return None   # BUG-FIX: never create — only use the mapped drive
                logger.error(
                    f"[DEST-VERIFY] Cannot access dest drive {dest_drive_id} "
                    f"HTTP {code}: {exc}"
                )
                return None

            except Exception as exc:
                logger.error(
                    f"[DEST-VERIFY] Unexpected error accessing dest drive "
                    f"{dest_drive_id}: {exc}"
                )
                return None

        return None
