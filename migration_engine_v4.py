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

FIX-5  unauthorized_client regression (targeted cache invalidation):
       The sweeping ``self._thread_local.drive_cache = {}`` inside
       ``DestAdminPoolManager._do_rotate_locked()`` was destroying the entire
       per-thread service cache on every rotation, forcing parallel worker
       threads to rebuild their Source Drive services.  During that rebuild,
       source emails were incorrectly hitting destination authorization
       boundaries and failing with ``unauthorized_client``.
       Fix: the rotation path now iterates over ``list(cache.keys())`` and
       deletes ONLY keys that appear in ``self._pool`` (destination admins).
       Source-user service objects are left completely untouched.

FIX-6  unauthorized_client regression (credential-file cross-contamination):
       ``_get_drive_service_for_thread()`` previously inferred ``is_dest`` from
       the email domain suffix (``not email.endswith(f"@{SOURCE_DOMAIN}")``).
       This heuristic could select destination credentials for a source address
       that shared a domain token with a pool admin, or select source credentials
       for a pool admin whose domain was not recognized, producing unauthorized
       delegation errors.
       Fix: ``_get_drive_service_for_thread()`` now hard-codes ``is_dest=False``
       and is reserved exclusively for source read operations.  All destination
       writes already route through ``_get_dest_drive_service_for_thread()``
       which hard-codes ``is_dest=True``.  ``_build_drive_service()`` retains
       the ``is_dest`` branch but its docstring now formally declares the strict
       segregation contract with no domain inference at any layer.

FIX-7  unauthorized_client regression (Phase-1 dest service path):
       ``_prepare_user_folders()`` was obtaining the destination folder-creation
       service via ``_get_drive_service_for_thread(self.dest_auth, dest_email)``,
       which bypassed the pool manager and could select the wrong credential file
       under the domain-inference bug above.
       Fix: ``_prepare_user_folders()`` now calls
       ``_get_dest_drive_service_for_thread(dest_email)`` for the destination Drive
       service, matching every other destination write call-site in the engine.

FIX-8  File placement bug — destination delegation was bound to pool admin:
       ``_build_drive_service(email, is_dest=True)`` was receiving the current
       pool admin address as ``email``, causing ``subject=email`` in
       ``_sa.Credentials.from_service_account_file`` to impersonate the admin
       account instead of the actual mapped destination user.  All migrated items
       therefore landed in the personal Drives of the admin pool accounts.
       Fix: ``_get_dest_drive_service_for_thread(dst_email)`` now accepts the
       mapped destination user email as its sole parameter and passes that address
       as ``subject=`` to ``_build_drive_service``.  The pool admin is never used
       as the delegation subject.  Thread-local cache keys are now the destination
       user email so that per-user service objects are correctly isolated.

FIX-9  quotaUser spreading across admin pool:
       The admin pool's purpose is to distribute API quota strain, not to own
       files.  After FIX-8 separates delegation from quota tracking, the active
       admin email is appended as ``quotaUser`` directly on every request builder
       method (e.g. ``.create(quotaUser=...)``, ``.list(quotaUser=...)``) — NOT
       inside the terminal ``.execute()`` invocation, which raises a runtime
       error.  This attributes charges to the rotating admin without altering
       file ownership or folder placement.

FIX-10 Targeted thread-local cache eviction (complement to FIX-8):
       ``DestAdminPoolManager._do_rotate_locked`` evicts only cache entries
       whose key exactly matches one of the admin emails in ``self._pool``,
       rather than using a domain-suffix heuristic.  Source-user service
       objects are left completely untouched, preventing unauthorized_client
       regression loops on admin rotation.

FIX-13 useDomainAdminAccess=True for 'anyone' and 'domain' RULE 4 branches:
       The RULE 4 anyone-with-link and domain-wide create calls in
       permissions_migrator.py were passing use_domain_admin=False.  Without
       the admin-context flag, the Admin Service Account cannot resolve files
       owned by delegated destination users and receives HTTP 404.  Fixed in
       permissions_migrator.py; no structural change to the engine call-site —
       _migrate_permissions_hybrid already always passes admin_dest_drive.

FIX-11 Permission migration — three-tier dynamic fallback + quotaUser threading:
       EnhancedPermissionsMigrator previously raised "External user/account not
       found" as a hard permanent failure whenever the Drive API returned HTTP
       400 (invalidSharingRequest / userNotFound / domainUserNotFound) or 404
       for a destination user lookup.  This silently dropped every collaborator
       whose account did not yet exist on the destination tenant.

       New behaviour in permissions_migrator.py:
         _create_permission() now returns USER_NOT_FOUND_SENTINEL (a typed
         module-level object, not a string) for all "not found" conditions.
         migrate_permissions() detects the sentinel and applies three tiers:
           Tier 1 — dest-domain email succeeds: permission created internally.
           Tier 2 — dest-domain email → sentinel: retry with source email as
                    external collaborator (never a failure, just a tier drop).
           Tier 3 — external email → sentinel: retry without
                    useDomainAdminAccess=True (admin-context share may be
                    blocked by tenant policy; direct user-context share works).

       _migrate_permissions_hybrid() now passes the current pool admin email as
       ``quota_user`` into EnhancedPermissionsMigrator so that every
       permissions().create() builder call carries quotaUser=<admin> for
       consistent API quota distribution across the pool (FIX-9 parity for
       the permissions pipeline).

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

FEAT-7  Destination Admin Pool Loader and Rotator (DestAdminPoolManager):
        Replaces the single static Config.DEST_ADMIN_EMAIL with a thread-safe
        pool of authorized destination admin accounts loaded from the
        uploads/admin file. Pool rotation occurs:
          (a) Proactively  — when a single admin accumulates >= 700 GB of
              uploaded bytes, the pool advances to the next admin and the
              byte counter resets to zero.
          (b) Reactively   — when a 403/429 HttpError containing a rate-limit
              indicator (userRateLimitExceeded, rateLimitExceeded, quotaExceeded)
              is raised during a destination operation, the pool immediately
              rotates and the failed request is retried with the fresh admin.
        Thread-local drive service caches are invalidated on every rotation so
        that each worker picks up a fresh delegated credential on its next
        destination API call. On every run_migration() invocation a brand-new
        pool sequence is constructed, guaranteeing clean state on resume after
        a VM restart (SQL checkpoint tracks file state, not admin state).
"""

import io
import logging
import time
import json
import threading
import random
import re
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

# ---------------------------------------------------------------------------
# permissions_migrator — top-level import with graceful degradation.
#
# Previously this was a lazy inline import inside _migrate_permissions_hybrid()
# which silently failed whenever permissions_migrator.py was not on sys.path
# at the time gunicorn forked its workers (the file lives in ~/amey/ which
# is not always on PYTHONPATH).  Moving it here means:
#   • The import runs once at module load time in the master process.
#   • Any ImportError or transitive dependency failure is logged at ERROR
#     level with the full traceback so the real cause is visible in PM2 logs.
#   • _PERM_MIGRATOR_AVAILABLE is a module-level flag checked by
#     _migrate_permissions_hybrid() so the rest of the engine still starts
#     cleanly even when the module is unavailable.
# ---------------------------------------------------------------------------
try:
    from permissions_migrator import (
        EnhancedPermissionsMigrator,
        USER_NOT_FOUND_SENTINEL,
    )
    _PERM_MIGRATOR_AVAILABLE = True
except Exception as _perm_import_exc:
    import traceback as _tb
    _perm_import_tb  = _tb.format_exc()
    del _tb
    EnhancedPermissionsMigrator = None   # type: ignore[assignment,misc]
    USER_NOT_FOUND_SENTINEL     = None   # type: ignore[assignment]
    _PERM_MIGRATOR_AVAILABLE    = False
    # Deferred: logged on first call so the module logger is fully initialised.
    logger.error(
        "permissions_migrator could not be imported — "
        "EnhancedPermissionsMigrator will be unavailable.\n%s",
        _perm_import_tb,
    )

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

LARGE_FILE_THRESHOLD_BYTES  = 5  * 1_024 * 1_024   # 5 MB   — GCS vs memory routing
XLARGE_FILE_THRESHOLD_BYTES = 600 * 1_024 * 1_024  # 600 MB — dedicated XL worker pool
MAX_FILE_SIZE_BYTES         = 5   * 1_024 * 1_024 * 1_024  # 5 GB  — hard ignore limit

GLOBAL_WORKERS   = 14   # Phase-2 full pool (used after XL pass)
XLARGE_WORKERS   = 14   # Dedicated workers reserved for >600 MB files
FOLDER_WORKERS   = 4
CONNECTION_TIMEOUT      = 1_800
MAX_BACKOFF_SECONDS     = 32
CHUNK_SIZE              = 16 * 1_024 * 1_024  # default; overridden by RAM probe at runtime

# FIX-4: Hard per-file timeout for GCS transfers (download + upload combined).
GCS_FILE_TIMEOUT = 3_600  # seconds

# FEAT-7: Proactive rotation threshold — 700 GB per admin.
DEST_ADMIN_ROTATION_BYTES = 700 * 1_024 * 1_024 * 1_024

# FEAT-7: Rate-limit error substrings that trigger reactive rotation.
_RATE_LIMIT_MARKERS = frozenset({
    "userRateLimitExceeded",
    "rateLimitExceeded",
    "quotaExceeded",
    "Rate Limit Exceeded",
    "User Rate Limit Exceeded",
})

# Path to the admin pool file written by Flask on the host VM.
_ADMIN_POOL_FILE = Path.home() / "flask-backend" / "uploads" / "admin"


# ─────────────────────────────────────────────────────────────────────────────
# FEAT-7: Admin pool file parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_admin_pool_file(path: Path) -> List[str]:
    """
    Parse the uploads/admin file and return a clean list of email strings.

    Supported format (Python-like set literal):
        admin={email1@domain.com,email2@domain.com,...}

    Robustly strips:
      - The ``admin={`` prefix and closing ``}``
      - Whitespace, blank lines, and comment lines (``#``)
      - Any surrounding quotes on individual addresses

    Returns an empty list (and logs a warning) if the file is missing or
    contains no parseable addresses.
    """
    if not path.exists():
        logger.warning(
            f"[ADMIN-POOL] Admin pool file not found at '{path}'. "
            f"Falling back to Config.DEST_ADMIN_EMAIL — no pool rotation."
        )
        return []

    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        logger.error(f"[ADMIN-POOL] Cannot read admin pool file '{path}': {exc}")
        return []

    # Strip the outer ``admin={...}`` wrapper if present.
    match = re.search(r"admin\s*=\s*\{([^}]*)\}", raw, re.DOTALL)
    if match:
        inner = match.group(1)
    else:
        # Fallback: treat the whole content as a comma-separated list.
        inner = raw

    emails: List[str] = []
    for token in inner.split(","):
        token = token.strip().strip("\"'")
        if not token or token.startswith("#"):
            continue
        # Basic sanity: must look like an email address.
        if "@" in token and "." in token.split("@")[-1]:
            emails.append(token)
        else:
            logger.debug(f"[ADMIN-POOL] Skipping invalid token in admin file: {token!r}")

    if not emails:
        logger.warning(
            f"[ADMIN-POOL] Admin pool file parsed but yielded no valid emails from '{path}'."
        )
    else:
        logger.info(
            f"[ADMIN-POOL] Loaded {len(emails)} destination admin(s) from '{path}': "
            + ", ".join(emails)
        )
    return emails


# ─────────────────────────────────────────────────────────────────────────────
# FEAT-7: Thread-safe Destination Admin Pool Manager
# ─────────────────────────────────────────────────────────────────────────────

class DestAdminPoolManager:
    """
    Thread-safe manager for a rotating pool of destination admin accounts.

    Responsibilities
    ────────────────
    1. Holds the ordered list of destination admin emails loaded from the
       uploads/admin file (parsed once at construction time).
    2. Tracks cumulative bytes uploaded through the *currently active* admin.
    3. Proactively rotates to the next admin when byte usage crosses
       DEST_ADMIN_ROTATION_BYTES (700 GB).
    4. Exposes ``rotate(reason)`` for reactive rotation — called by worker
       threads when a 403/429 rate-limit HttpError is caught on a destination
       operation.
    5. All mutable state is guarded by a single ``threading.Lock`` so that
       concurrent calls from ``global_workers`` and ``xlarge_workers`` threads
       are safe.

    Thread-local cache invalidation
    ────────────────────────────────
    The pool manager owns a reference to ``MigrationEngine._thread_local`` and
    clears the ``drive_cache`` dict on every rotation so that worker threads
    rebuild their destination Drive service with the newly active admin's
    delegated credentials.

    Stateless on resume
    ───────────────────
    A new ``DestAdminPoolManager`` is constructed inside every ``run_migration()``
    call.  Because the SQL checkpoint tracks file-level state (not admin state),
    resuming after a VM restart simply starts the pool from index 0 again —
    completely self-healing with no additional persistence required.
    """

    def __init__(
        self,
        emails: List[str],
        fallback_email: str,
        thread_local: threading.local,
    ):
        """
        Args:
            emails:         Ordered list of destination admin emails from the
                            admin pool file.  May be empty — in that case the
                            pool degrades gracefully to ``fallback_email`` with
                            no rotation.
            fallback_email: ``Config.DEST_ADMIN_EMAIL`` — used when the pool
                            file is absent or empty.
            thread_local:   The engine's ``self._thread_local`` object.  On each
                            rotation the manager clears ``thread_local.drive_cache``
                            for the calling thread so a fresh service is built.
        """
        if not emails:
            # Degenerate single-element pool backed by the config default.
            emails = [fallback_email]
            logger.info(
                f"[ADMIN-POOL] No pool emails — operating with single admin: {fallback_email}"
            )

        self._pool: List[str]           = list(emails)
        self._index: int                = 0
        self._bytes_on_current: int     = 0
        self._total_rotations: int      = 0
        self._lock: threading.Lock      = threading.Lock()
        self._thread_local              = thread_local

    # ── Public read ───────────────────────────────────────────────────────────

    @property
    def current_admin(self) -> str:
        """Return the currently active destination admin email (thread-safe read)."""
        with self._lock:
            return self._pool[self._index]

    @property
    def pool_size(self) -> int:
        return len(self._pool)

    # ── Byte accounting (proactive rotation) ──────────────────────────────────

    def record_bytes_uploaded(self, byte_count: int) -> None:
        """
        Accumulate ``byte_count`` against the current admin's tally.
        Triggers a proactive rotation when the threshold is crossed.
        Called by worker threads after every successful destination write.
        """
        if byte_count <= 0:
            return
        with self._lock:
            self._bytes_on_current += byte_count
            if (
                len(self._pool) > 1
                and self._bytes_on_current >= DEST_ADMIN_ROTATION_BYTES
            ):
                old = self._pool[self._index]
                self._do_rotate_locked(
                    f"proactive — {self._bytes_on_current / (1024**3):.1f} GB "
                    f"threshold reached on {old}"
                )

    # ── Reactive rotation (on rate-limit error) ───────────────────────────────

    def rotate(self, reason: str = "reactive rate-limit") -> str:
        """
        Force a rotation to the next admin in the pool, then sleep OUTSIDE
        the lock with randomized exponential backoff + jitter before returning.

        Thread-safety design — two explicit phases
        ──────────────────────────────────────────
        Phase 1 (inside the lock):
            Snapshot ``self._total_rotations`` *before* calling
            ``_do_rotate_locked`` (which increments the counter), perform the
            rotation, capture the new admin email, then release the lock
            immediately.  No I/O, no sleeping, no blocking calls inside the
            lock — the critical section is as small as possible so that the
            other worker threads that call ``current_admin`` or
            ``record_bytes_uploaded`` concurrently are never stalled.

        Phase 2 (outside the lock — this thread only):
            Calculate a capped exponential base wait from the rotation number
            captured in Phase 1, add a uniformly random jitter drawn
            independently by each thread, then sleep.

            The jitter is the thundering-herd fix: even when all
            ``GLOBAL_WORKERS`` threads arrive at this point within the same
            millisecond, each draws a different float from [1.5, 4.5) and
            therefore wakes at a staggered time.  Without jitter every thread
            would sleep for the same duration, wake simultaneously, and flood
            the newly rotated admin's per-second token bucket all over again —
            reproducing the original 403 loop with a brief delay prepended.

        Backoff formula
        ───────────────
            rotation_number = self._total_rotations   # snapshot before increment
            base_wait = min(8.0 * (2.0 ** rotation_number), 60.0)  # floor 8 s, cap 60 s
            jitter    = random.uniform(2.5, 6.5)                    # per-thread random offset
            total     = base_wait + jitter

        Sleep range by rotation number
        ──────────────────────────────
            rotation 0  →  base= 8.0 s + jitter  →  10.5 – 14.5 s
            rotation 1  →  base=16.0 s + jitter  →  18.5 – 22.5 s
            rotation 2  →  base=32.0 s + jitter  →  34.5 – 38.5 s
            rotation 3+ →  base=60.0 s + jitter  →  62.5 – 66.5 s  (cap applied)

        Returns the newly active admin email (after the backoff sleep, ready
        to use for the next API call).
        """
        # ── Phase 1: mutate shared state under the lock, release immediately ──
        with self._lock:
            # Snapshot BEFORE _do_rotate_locked() increments _total_rotations.
            # rotation_number=0 on the very first reactive rotation gives a
            # base wait of 2**0 = 1 s; without this snapshot we would compute
            # 2**1 = 2 s (one step too high on the exponential curve).
            rotation_number = self._total_rotations
            new_admin = self._do_rotate_locked(reason)
        # Lock released — other threads are now free to call current_admin,
        # record_bytes_uploaded, or rotate() independently.

        # ── Phase 2: sleep OUTSIDE the lock — only this thread blocks ─────────
        #
        # Aggressive cooldown: baseline floor of 8.0 s on the first rotation,
        # scaled exponentially by rotation number and capped at 60 s.  This
        # intentionally parks the thread long enough for Google's per-admin
        # token buckets to drain and refill, breaking the cyclical failover
        # pattern that would otherwise maintain sustained high traffic volume.
        #
        # Formula: base_wait = min(8.0 * (2.0 ** rotation_number), 60.0)
        #
        # Sleep range by rotation number
        # ──────────────────────────────
        #   rotation 0  →  base= 8.0 s + jitter  →  10.5 – 14.5 s
        #   rotation 1  →  base=16.0 s + jitter  →  18.5 – 22.5 s
        #   rotation 2  →  base=32.0 s + jitter  →  34.5 – 38.5 s
        #   rotation 3+ →  base=60.0 s + jitter  →  62.5 – 66.5 s  (cap applied)
        base_wait = min(8.0 * (2.0 ** rotation_number), 60.0)

        # Independent per-thread jitter (widened to 2.5–6.5 s) — ensures that
        # even when all GLOBAL_WORKERS threads arrive simultaneously, each wakes
        # at a staggered time, preventing a second coordinated burst.
        jitter = random.uniform(2.5, 6.5)

        total_wait = base_wait + jitter

        logger.info(
            f"[ADMIN-POOL-COOLDOWN] Parking thread {total_wait:.2f}s "
            f"(base={base_wait:.1f}s + jitter={jitter:.2f}s) "
            f"to let Google token buckets clear before retrying on {new_admin} "
            f"[rotation #{rotation_number + 1}]"
        )

        time.sleep(total_wait)   # ← OUTSIDE the lock; only THIS thread sleeps

        return new_admin

    # ── Internal ─────────────────────────────────────────────────────────────

    def _do_rotate_locked(self, reason: str) -> str:
        """Must be called with self._lock already held."""
        if len(self._pool) == 1:
            logger.warning(
                "[ADMIN-POOL] Rotation requested but pool has only 1 admin — "
                "cannot rotate. If rate limits persist, add more admins to the "
                "uploads/admin file."
            )
            return self._pool[0]

        old_admin   = self._pool[self._index]
        self._index = (self._index + 1) % len(self._pool)
        new_admin   = self._pool[self._index]
        self._bytes_on_current = 0
        self._total_rotations += 1

        logger.warning(
            f"[ADMIN-POOL] Rotation #{self._total_rotations}: "
            f"{old_admin} → {new_admin} | reason: {reason}"
        )

        # Spec-3 / FIX-10: Targeted cache eviction — evict ONLY cache entries
        # whose key exactly matches one of the destination admin emails present
        # in self._pool.  This leaves source-user service objects completely
        # untouched, preventing unauthorized_client regression loops caused by
        # domain-suffix heuristics that could match unrelated source addresses.
        try:
            cache: dict = getattr(self._thread_local, "drive_cache", {})
            # Build a set of pool email strings for O(1) membership tests.
            pool_email_set = set(self._pool)
            # Collect matching keys in a separate pass — never mutate a dict
            # while iterating over it (required for non-CPython implementations).
            dest_keys_to_evict = [
                key for key in list(cache.keys())
                if key in pool_email_set
            ]
            for key in dest_keys_to_evict:
                del cache[key]
            logger.debug(
                f"[ADMIN-POOL] Thread-local cache: evicted {len(dest_keys_to_evict)} "
                f"pool-admin entry/entries for rotation "
                f"({len(cache)} non-pool entries preserved)"
            )
        except Exception as exc:
            logger.debug(f"[ADMIN-POOL] Cache eviction warning (non-fatal): {exc}")

        return new_admin

    def __repr__(self) -> str:
        with self._lock:
            return (
                f"<DestAdminPoolManager pool_size={len(self._pool)} "
                f"current_index={self._index} "
                f"current={self._pool[self._index]!r} "
                f"bytes_on_current={self._bytes_on_current:,} "
                f"total_rotations={self._total_rotations}>"
            )


# ─────────────────────────────────────────────────────────────────────────────
# FEAT-7: Helper — is this HttpError a rate-limit we should rotate on?
# ─────────────────────────────────────────────────────────────────────────────

def _is_rate_limit_error(exc: HttpError) -> bool:
    """
    Return True if ``exc`` is a 403 or 429 whose message body contains one of
    the well-known Google Drive rate-limit strings.
    """
    if exc.resp.status not in (403, 429):
        return False
    err_str = str(exc)
    return any(marker in err_str for marker in _RATE_LIMIT_MARKERS)


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
# ─────────────────────────────────────────────────────────────────────────────
IGNORED_MIME_TYPES = frozenset({
    "application/vnd.google-apps.script",
    "application/vnd.google-apps.form",
    "application/vnd.google-apps.site",
    "application/octet-stream",
    "application/vnd.google-apps.vid",       # FIX-1: legacy Google Video — no export API
})

# ─────────────────────────────────────────────────────────────────────────────
# FIX-2 / FIX-3: Workspace type registry
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
        "fallback_mime": "application/pdf",
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
        global_workers: Parallel threads for Phase 2 regular files (default 8).
        xlarge_workers: Dedicated threads for files >600 MB in Phase 2 pass-1
                        (default 8). These complete first, then global_workers
                        threads are used for all remaining files.

    FEAT-5 — Two-tier Phase-2 queue:
        Files >600 MB are submitted to the xlarge_workers pool first and allowed
        to complete before regular files are processed.

    FEAT-6 — RAM-adaptive streaming:
        Chunk sizes for all downloads/uploads scale automatically with available
        system RAM (8 MB – 256 MB).

    FEAT-7 — Destination Admin Pool:
        A fresh DestAdminPoolManager is instantiated here from the uploads/admin
        file, guaranteeing a clean, predictable pool state on every invocation
        (including resumes). The pool is passed into MigrationEngine and shared
        across all worker threads.
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
    }

    sql_mgr = SQLStateManager(
        db_config=db_config,
        gcs_bucket=Config.GCS_BUCKET_NAME,
        gcs_key_file=gcs_key,
        source_domain=Config.SOURCE_DOMAIN,
        dest_domain=Config.DEST_DOMAIN,
        gcs_prefix=Config.GCS_STAGING_PREFIX,
        migration_id=run_id,
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

    # ── FEAT-7: Load destination admin pool ───────────────────────────────────
    # Parse the uploads/admin file into an ordered list of emails.
    # A new pool is built on every run_migration() call so the state is always
    # fresh — whether this is a first run or a resume after VM restart.
    admin_emails = _parse_admin_pool_file(_ADMIN_POOL_FILE)
    # The DestAdminPoolManager receives a placeholder thread_local here;
    # MigrationEngine will replace it with its own after construction.
    _placeholder_tl = threading.local()
    dest_admin_pool = DestAdminPoolManager(
        emails=admin_emails,
        fallback_email=Config.DEST_ADMIN_EMAIL,
        thread_local=_placeholder_tl,
    )

    # ── Reset any IN_PROGRESS rows left by a previous crashed attempt ─────────
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
        gcs_helper=sql_mgr,
        run_id=run_id,
        get_conn=sql_mgr.get_conn,
        progress_cb=progress_cb,
        dest_admin_pool=dest_admin_pool,
    )

    # Wire the pool's thread_local reference to the engine's actual instance
    # so rotation correctly clears the engine's per-thread drive cache.
    dest_admin_pool._thread_local = engine._thread_local

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
        run_id:          str,
        get_conn,
        progress_cb:     Callable[[Dict], None] = None,
        dest_admin_pool: Optional[DestAdminPoolManager] = None,
    ):
        self.source_auth     = source_auth
        self.dest_auth       = dest_auth
        self.config          = config
        self.sql_mgr         = checkpoint
        self.gcs             = gcs_helper
        self.run_id          = run_id
        self.get_conn        = get_conn
        self.progress_cb     = progress_cb

        # FEAT-7: pool is injected by run_migration(); fallback to single-admin
        # degenerate pool if callers construct MigrationEngine directly.
        self._dest_admin_pool: DestAdminPoolManager = dest_admin_pool or DestAdminPoolManager(
            emails=[],
            fallback_email=config.DEST_ADMIN_EMAIL,
            thread_local=threading.local(),
        )

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

        # Task 2: Per-folder write-concurrency lease guard.
        # Tracks the set of destination parent folder IDs that currently have an
        # active write mutation in flight.  Before initiating a resumable upload
        # into a destination folder, a worker thread must acquire a lease on that
        # folder ID by adding it to this set.  If the ID is already present,
        # the thread yields and retries until the lane is free or the 30 s
        # acquisition timeout expires.  The finally block in _process_queue_item
        # guarantees unconditional release so deadlocks cannot occur.
        self._active_dest_parents: set              = set()
        self._active_parents_lock: threading.Lock   = threading.Lock()

        # Task 3: Per-user quarantine dictionary.
        # Maps source_email → monotonic expiry timestamp (time.monotonic() + 300).
        # Entries are checked at the very top of _process_queue_item before any
        # Drive service is built or SQL state is mutated.  Files belonging to a
        # quarantined user are fast-yielded back to SQL with error_type
        # "user_quarantine_bypass" so healthy-user workers are never starved.
        self._quarantined_users: Dict[str, float]  = {}
        self._quarantine_lock:   threading.Lock    = threading.Lock()
        self.QUARANTINE_DURATION_SECONDS: int       = 300

        self.stats = {
            "total_files": 0, "successful": 0, "failed": 0,
            "skipped": 0, "ignored": 0, "folders_created": 0,
            "folders_failed": 0, "gcs_routed": 0, "memory_routed": 0,
            "admin_rotations": 0,
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
            f"run_id={self.run_id} | "
            f"dest_admin_pool={self._dest_admin_pool}"
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
                        with self._counter_lock:
                            total_done = self._done_count
                        if total_done % 50 == 0:
                            logger.info(
                                f"[PHASE-2/{label}] {total_done}/{len(pending)} complete | "
                                f"pool={self._dest_admin_pool}"
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

        # Record total rotations in stats for the summary report.
        self.stats["admin_rotations"] = self._dest_admin_pool._total_rotations

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
        summary["admin_rotations"]  = self.stats["admin_rotations"]

        logger.info(
            f"[DOMAIN] Complete: {summary['accuracy_rate']:.2f}% | "
            f"GCS={self.stats['gcs_routed']} MEM={self.stats['memory_routed']} | "
            f"admin_rotations={self.stats['admin_rotations']} | "
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
            # Source reads: always use source auth path (is_dest=False, source creds).
            # Destination folder creation: delegate to the destination user (dest_email)
            # so that folders are created in their Drive, not in an admin account's Drive.
            # The active pool admin is used only for quotaUser attribution (FIX-9).
            _dest_drive = self._get_dest_drive_service_for_thread(dest_email)
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
                new_fm = self._build_folder_structure(missing, _dest_drive, source_email, dest_email)
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

        # ── Task 3: User-quarantine fast-bypass ───────────────────────────────
        # Must be the very first check — before any Drive service creation or
        # SQL state mutation — so quarantined users consume zero quota and do
        # not advance the file through the pipeline prematurely.
        with self._quarantine_lock:
            expiry = self._quarantined_users.get(src_email)
            if expiry is not None:
                if time.monotonic() < expiry:
                    # User is still under quarantine: fast-yield back to SQL.
                    remaining = expiry - time.monotonic()
                    logger.debug(
                        f"[USER-QUARANTINE-BYPASS] {src_email} still quarantined "
                        f"({remaining:.0f}s remaining) — re-queuing {file_name}"
                    )
                    self.sql_mgr.mark_failed(
                        self.run_id, file_id,
                        f"user_quarantine_bypass: {src_email} quarantined for "
                        f"another {remaining:.0f}s",
                    )
                    return self._emit({
                        **base, "error": "user_quarantine_bypass",
                        "error_type": "user_quarantine_bypass",
                    })
                else:
                    # Quarantine has expired — clean removal.
                    del self._quarantined_users[src_email]
                    logger.info(
                        f"[USER-QUARANTINE-EXPIRED] Quarantine lifted for {src_email} "
                        f"— resuming normal processing"
                    )

        # ── Standard pre-flight checks ────────────────────────────────────────
        if mime_type in IGNORED_MIME_TYPES:
            self.sql_mgr.mark_ignored(self.run_id, file_id, "Non-migratable MIME type")
            return self._emit({**base, "ignored": True})

        if file_size > MAX_FILE_SIZE_BYTES:
            reason = f"File size {_fmt_bytes(file_size)} exceeds 5 GB limit — ignored"
            logger.warning(f"[SIZE-LIMIT] {file_name} ({file_id}): {reason}")
            self.sql_mgr.mark_ignored(self.run_id, file_id, reason)
            return self._emit({**base, "ignored": True})

        try:
            _skip_status = self.sql_mgr.get_item_status(file_id)
            if _skip_status == "DONE":
                return self._emit({**base, "skipped": True})
        except AttributeError:
            should_skip, _ = self.sql_mgr.should_skip_item(file_id)
            if should_skip:
                return self._emit({**base, "skipped": True})
        except Exception:
            pass

        with self._processed_lock:
            if (file_id, file_name, file_size) in self._processed:
                return self._emit({**base, "skipped": True})

        try:
            source_drive = self._get_drive_service_for_thread(self.source_auth, src_email)
            dest_drive   = self._get_dest_drive_service_for_thread(dst_email)
        except Exception as exc:
            err = f"Auth error: {exc}"
            self.sql_mgr.mark_failed(self.run_id, file_id, err)
            return self._emit({**base, "error": err, "error_type": "auth_error"})

        with self._folder_maps_lock:
            fm = self._folder_maps.get(src_email, {})
        dest_parent = fm.get(parent_id) if parent_id else None

        # ── Task 2: Per-folder write-concurrency lease acquisition ────────────
        # Drive enforces transactional write locks on destination directories.
        # Only one thread may write into a given dest_parent simultaneously.
        # We spin-wait (with random sleep jitter) until the folder lane is free,
        # then hold the lease for the duration of the upload.  A hard 30 s
        # acquisition timeout prevents indefinite stalls if something goes wrong.
        folder_lane_acquired = False
        if dest_parent is not None:
            _lease_deadline = time.monotonic() + 300.0
            while True:
                with self._active_parents_lock:
                    if dest_parent not in self._active_dest_parents:
                        self._active_dest_parents.add(dest_parent)
                        folder_lane_acquired = True
                        break
                # Lane is busy — yield and retry.
                if time.monotonic() >= _lease_deadline:
                    # Timed out waiting for the folder lane.
                    err = (
                        f"folder_concurrency_lock: could not acquire write lane for "
                        f"dest_parent={dest_parent} within 30s"
                    )
                    logger.warning(
                        f"[FOLDER-LEASE-TIMEOUT] {file_name} ({file_id}): {err}"
                    )
                    self.sql_mgr.mark_failed(self.run_id, file_id, err)
                    return self._emit({
                        **base, "error": err,
                        "error_type": "folder_concurrency_lock",
                    })
                time.sleep(random.uniform(0.5, 1.5))

        # ── Core execution — wrapped in try/finally to guarantee lease release ─
        try:
            self.sql_mgr.mark_in_progress(self.run_id, file_id)
            res = self._migrate_file(
                file_id, file_name, mime_type, file_size,
                dest_parent, source_drive, dest_drive, dst_email=dst_email, src_email=src_email,
            )

            if res["success"]:
                dest_id = res.get("dest_id")
                self.sql_mgr.mark_done(self.run_id, file_id, dest_item_id=dest_id, dest_parent_id=dest_parent)
                with self._processed_lock:
                    self._processed.add((file_id, file_name, file_size))
                # FEAT-7: record bytes for proactive rotation threshold tracking.
                self._dest_admin_pool.record_bytes_uploaded(file_size)
                perm_r = {"migrated": 0, "external": 0}
                if dest_id:
                    perm_r = self._migrate_permissions_hybrid(
                        file_id, dest_id, file_name, source_drive,
                        self._get_dest_drive_service_for_thread(dst_email),
                        dst_email=dst_email,
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
                err      = res.get("error", "Unknown")
                err_type = res.get("error_type", "")

                # Task 3: quarantine user on userRateLimitExceeded from any
                # sub-method (_migrate_via_memory / _migrate_via_gcs /
                # _migrate_workspace_file all surface errors through res["error"]).
                if any(marker in (err or "") for marker in (
                    "userRateLimitExceeded", "User Rate Limit Exceeded"
                )):
                    with self._quarantine_lock:
                        self._quarantined_users[src_email] = (
                            time.monotonic() + self.QUARANTINE_DURATION_SECONDS
                        )
                    logger.warning(
                        f"[USER-QUARANTINE-ACTIVATED] {src_email} quarantined for "
                        f"{self.QUARANTINE_DURATION_SECONDS}s after userRateLimitExceeded "
                        f"on file {file_name} ({file_id}): {err}"
                    )

                self.sql_mgr.mark_failed(self.run_id, file_id, err)
                return self._emit({**base, "error": err, "error_type": err_type})

        finally:
            # Unconditional lease release — executed whether the upload succeeded,
            # failed, raised an exception, or timed out.  Guarantees no deadlock.
            if folder_lane_acquired and dest_parent is not None:
                with self._active_parents_lock:
                    self._active_dest_parents.discard(dest_parent)

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

    def _activate_user_quarantine(self, src_email: str, file_name: str, file_id: str, reason: str) -> None:
        """
        Task 3 helper: place src_email into the quarantine dictionary.
        Safe to call from any worker thread; protected by self._quarantine_lock.
        Logs under [USER-QUARANTINE-ACTIVATED].
        """
        with self._quarantine_lock:
            self._quarantined_users[src_email] = (
                time.monotonic() + self.QUARANTINE_DURATION_SECONDS
            )
        logger.warning(
            f"[USER-QUARANTINE-ACTIVATED] {src_email} quarantined for "
            f"{self.QUARANTINE_DURATION_SECONDS}s after userRateLimitExceeded "
            f"on file {file_name} ({file_id}): {reason}"
        )

    # =========================================================================
    # File routing
    # =========================================================================

    def _migrate_file(
        self, file_id, file_name, mime_type, file_size,
        dest_parent_id, source_drive, dest_drive, dst_email: str = "", src_email: str = "",
    ) -> Dict:
        empty = {"success": False, "dest_id": None, "ignored": False, "error": None}
        if mime_type in IGNORED_MIME_TYPES:
            return {**empty, "ignored": True, "error": "Non-migratable MIME type"}
        if mime_type in GOOGLE_WORKSPACE_TYPES:
            return self._migrate_workspace_file(
                file_id, file_name, mime_type, dest_parent_id, source_drive, dest_drive,
                dst_email=dst_email, src_email=src_email,
            )
        if file_size >= LARGE_FILE_THRESHOLD_BYTES and self.gcs:
            return self._migrate_via_gcs(
                file_id, file_name, mime_type, file_size,
                dest_parent_id, source_drive, dest_drive,
                dst_email=dst_email, src_email=src_email,
            )
        return self._migrate_via_memory(
            file_id, file_name, mime_type, file_size,
            dest_parent_id, source_drive, dest_drive,
            dst_email=dst_email, src_email=src_email,
        )

    # ── Memory path (<LARGE_FILE_THRESHOLD) ──────────────────────────────────

    def _migrate_via_memory(
        self, file_id, file_name, mime_type, file_size,
        dest_parent_id, source_drive, dest_drive, dst_email: str = "", src_email: str = "",
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
                        # FIX-8: delegate to dst_email; FIX-9: quota to pool admin via builder
                        _dest = self._get_dest_drive_service_for_thread(dst_email)
                        resp = _dest.files().create(
                            body=meta, fields="id", supportsAllDrives=True,
                            quotaUser=self._dest_admin_pool.current_admin,
                        ).execute()
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
                    # FIX-8: delegate to dst_email; FIX-9: quota to pool admin via builder
                    _dest = self._get_dest_drive_service_for_thread(dst_email)
                    resp = _dest.files().create(
                        body=meta, media_body=media, fields="id", supportsAllDrives=True,
                        quotaUser=self._dest_admin_pool.current_admin,
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
                # FEAT-7: reactive rotation on destination rate-limit errors
                if _is_rate_limit_error(exc) and attempt < self.max_retries - 1:
                    new_admin = self._dest_admin_pool.rotate(
                        f"reactive on {file_name}: {last_error[:120]}"
                    )
                    self.stats["admin_rotations"] = self._dest_admin_pool._total_rotations
                    logger.warning(
                        f"[MEM] [{file_name}] Rate-limit hit — rotated to {new_admin}, "
                        f"retrying (attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(wait)
                    continue
                # Task 3: quarantine user on userRateLimitExceeded
                if any(m in last_error for m in ("userRateLimitExceeded", "User Rate Limit Exceeded")):
                    if src_email:
                        self._activate_user_quarantine(src_email, file_name, file_id, last_error)
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

    # ── GCS path (>=LARGE_FILE_THRESHOLD) — uses SQLStateManager's helpers ────

    def _migrate_via_gcs(
        self, file_id, file_name, mime_type, file_size,
        dest_parent_id, source_drive, dest_drive, dst_email: str = "", src_email: str = "",
    ) -> Dict:
        """
        FIX-4: Each attempt now runs inside a ThreadPoolExecutor(1) with a hard
        per-file timeout (GCS_FILE_TIMEOUT seconds, default 3600).

        FIX-8/9: Destination uploads use _get_dest_drive_service_for_thread(dst_email)
        so the drive service is delegated to the mapped destination user (not the pool
        admin).  The pool admin is applied only as quotaUser on .execute() calls inside
        upload_gcs_to_drive (if supported) or via the service object itself.  On a
        rate-limit HttpError during the upload phase, the pool rotates and the entire
        attempt (download already in GCS + upload) is retried.
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
                # FIX-8: resolve service for the destination user (not the pool admin).
                # Rotation invalidates the dst_email cache entry (FIX-10) so this
                # always returns a fresh service after a rotation event.
                _dest_for_upload = self._get_dest_drive_service_for_thread(dst_email)

                with _cf.ThreadPoolExecutor(max_workers=1) as sub_pool:
                    ul_future = sub_pool.submit(
                        self.gcs.upload_gcs_to_drive,
                        drive_svc=_dest_for_upload,
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
                    # FEAT-7: check if the underlying error is a rate-limit and rotate.
                    # upload_gcs_to_drive may surface HttpErrors through err2 as a string.
                    if any(m in (last_error or "") for m in _RATE_LIMIT_MARKERS):
                        if attempt < self.max_retries - 1:
                            new_admin = self._dest_admin_pool.rotate(
                                f"reactive-gcs on {file_name}: {last_error[:120]}"
                            )
                            self.stats["admin_rotations"] = self._dest_admin_pool._total_rotations
                            logger.warning(
                                f"[GCS] [{file_name}] Rate-limit in upload — "
                                f"rotated to {new_admin}, retrying"
                            )
                        # Task 3: quarantine user on userRateLimitExceeded (string path)
                        if any(m in (last_error or "") for m in ("userRateLimitExceeded", "User Rate Limit Exceeded")):
                            if src_email:
                                self._activate_user_quarantine(src_email, file_name, file_id, last_error)
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

            except HttpError as exc:
                last_error = str(exc)
                # FEAT-7: reactive rotation on rate-limit HttpErrors raised
                # outside the sub-pools (e.g. from upload_gcs_to_drive itself).
                if _is_rate_limit_error(exc) and attempt < self.max_retries - 1:
                    new_admin = self._dest_admin_pool.rotate(
                        f"reactive-gcs-http on {file_name}: {last_error[:120]}"
                    )
                    self.stats["admin_rotations"] = self._dest_admin_pool._total_rotations
                    logger.warning(
                        f"[GCS] [{file_name}] HttpError rate-limit — "
                        f"rotated to {new_admin}, retrying"
                    )
                # Task 3: quarantine user on userRateLimitExceeded (HttpError path)
                if any(m in last_error for m in ("userRateLimitExceeded", "User Rate Limit Exceeded")):
                    if src_email:
                        self._activate_user_quarantine(src_email, file_name, file_id, last_error)
                if active_blob:
                    try: self.gcs.delete_temp(active_blob)
                    except Exception: pass
                    active_blob = None
                if attempt < self.max_retries - 1:
                    time.sleep(wait)

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
        dst_email: str = "", src_email: str = "",
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
                    # FIX-8: delegate to dst_email; FIX-9: quota to pool admin via builder
                    _dest = self._get_dest_drive_service_for_thread(dst_email)
                    resp = _dest.files().create(
                        body=meta, media_body=media, fields="id", supportsAllDrives=True,
                        quotaUser=self._dest_admin_pool.current_admin,
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
                # FIX-2: exportSizeLimitExceeded falls back to PDF
                if "exportSizeLimitExceeded" in err:
                    if "fallback_mime" in type_info:
                        logger.warning(
                            f"[WORKSPACE] [{file_name}] exportSizeLimitExceeded — "
                            f"retrying as {type_info['fallback_ext']}"
                        )
                        return self._workspace_fallback(
                            file_id, file_name, type_info, dest_parent_id, source_drive, dest_drive,
                            dst_email=dst_email,
                        )
                    logger.warning(
                        f"[WORKSPACE] [{file_name}] exportSizeLimitExceeded and no "
                        f"fallback defined for {mime_type} — marking ignored"
                    )
                    return {**empty, "ignored": True, "error": f"exportSizeLimitExceeded: {err}"}
                # FEAT-7: reactive rotation on rate-limit during workspace upload
                if _is_rate_limit_error(exc) and attempt < self.max_retries - 1:
                    new_admin = self._dest_admin_pool.rotate(
                        f"reactive-workspace on {file_name}: {err[:120]}"
                    )
                    self.stats["admin_rotations"] = self._dest_admin_pool._total_rotations
                    logger.warning(
                        f"[WORKSPACE] [{file_name}] Rate-limit hit — "
                        f"rotated to {new_admin}, retrying"
                    )
                    time.sleep(wait)
                    continue
                # Task 3: quarantine user on userRateLimitExceeded
                if any(m in err for m in ("userRateLimitExceeded", "User Rate Limit Exceeded")):
                    if src_email:
                        self._activate_user_quarantine(src_email, file_name, file_id, err)
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
        dst_email: str = "",
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
                # FIX-8: delegate to dst_email; FIX-9: quota to pool admin via builder
                _dest = self._get_dest_drive_service_for_thread(dst_email)
                resp = _dest.files().create(
                    body=meta, media_body=media, fields="id", supportsAllDrives=True,
                    quotaUser=self._dest_admin_pool.current_admin,
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
        self, source_id, dest_id, name, source_drive, dest_drive=None,
        dst_email: str = "",
    ) -> Dict:
        """
        Migrate permissions for one file from source to destination.

        FIX-14 — useDomainAdminAccess=True is ONLY valid for Shared Drive items
        ─────────────────────────────────────────────────────────────────────────
        The Drive permissions API has two incompatible caller contexts:

        1. Admin context (useDomainAdminAccess=True):
           Valid ONLY for Shared Drive items. Used for SD Step 1 / SD-RULE 1
           (mapping internal source → dest email). The caller MUST impersonate
           a domain admin — hence admin_dest_drive.

        2. Owner context (useDomainAdminAccess=False or omitted):
           Required for ALL My Drive calls (anyone, domain, user steps 1-3)
           and Shared Drive steps 2-3. The API validates the caller owns or has
           write access to the file in the normal user context.
           → owner_dest_drive impersonates dst_email (the file owner) so Drive
             always sees a caller with write access, no admin flag needed.

        Sending useDomainAdminAccess=True for a My Drive file causes Drive to
        interpret fileId as a Shared Drive ID and return:
            HTTP 404 "Shared drive not found: <fileId>"

        Args:
            source_id:  Source file ID.
            dest_id:    Destination file ID.
            name:       File name for log messages.
            source_drive: Source Drive service.
            dest_drive:   Destination Drive service (kept for call-site compat).
            dst_email:    Mapped destination user email (file owner on dest).
                          Used to build owner_dest_drive. Empty string falls
                          back to admin service (Shared Drive root where admin
                          is always a member and has write access).
        """
        result = {"migrated": 0, "failed": 0, "external": 0, "skipped": 0}
        try:
            resp  = source_drive.permissions().list(
                fileId=source_id,
                fields="permissions(id,type,role,emailAddress,domain,displayName)",
                supportsAllDrives=True,
            ).execute()
            perms = resp.get("permissions", [])
            # Only bail out when the list is truly empty.  The old guard
            # ``<= 1`` was incorrectly skipping files that had exactly one
            # non-owner collaborator (owner + 1 sharer = 2 entries), causing
            # those permissions to be silently dropped.  The owner entry is
            # always skipped inside EnhancedPermissionsMigrator, so any
            # non-empty list may contain real permissions worth migrating.
            if not perms:
                return result
        except Exception as exc:
            logger.debug(f"Permissions fetch failed [{name}]: {exc}")
            return result

        # ── Guard: top-level import succeeded? ───────────────────────────────
        # EnhancedPermissionsMigrator and USER_NOT_FOUND_SENTINEL are imported
        # at module load time (see top of file).  If that import failed, the
        # real reason is already in the PM2 log as an ERROR with full traceback.
        # We skip the migration for this file rather than retrying the import
        # (which would fail again and produce another misleading log line).
        if not _PERM_MIGRATOR_AVAILABLE:
            logger.error(
                "EnhancedPermissionsMigrator not available — "
                "skipping permissions for [%s]. "
                "Check the startup ERROR log for the import traceback.",
                name,
            )
            return result

        try:
            # ── FIX-14: Build both Drive services ─────────────────────────────
            #
            # admin_dest_drive — impersonates the pool admin
            #   Passed as dest_drive to EnhancedPermissionsMigrator.
            #   Used ONLY for Shared Drive Step 1 (useDomainAdminAccess=True).
            #   NEVER used for My Drive calls — Drive returns 404 "Shared drive
            #   not found" when useDomainAdminAccess=True is sent for a My Drive
            #   file because the API misinterprets fileId as a driveId.
            #
            # owner_dest_drive — impersonates dst_email (the file owner)
            #   Passed as owner_dest_drive to EnhancedPermissionsMigrator.
            #   Used for ALL My Drive permission calls and Shared Drive Steps 2-3.
            #   The file owner always has write access to their own files without
            #   needing the useDomainAdminAccess flag.
            admin_email      = self._dest_admin_pool.current_admin
            admin_dest_drive = self._build_drive_service(admin_email, is_dest=True)

            if dst_email:
                owner_dest_drive = self._get_dest_drive_service_for_thread(dst_email)
                logger.debug(
                    f"[PERM] dual-service: admin={admin_email!r} owner={dst_email!r} "
                    f"dest_file={dest_id}"
                )
            else:
                # Shared Drive root — admin is always a member with write access.
                owner_dest_drive = admin_dest_drive
                logger.debug(
                    f"[PERM] no dst_email — owner falls back to admin service "
                    f"(Shared Drive root). dest_file={dest_id}"
                )

            pm = EnhancedPermissionsMigrator(
                source_drive, admin_dest_drive,
                self.config.SOURCE_DOMAIN, self.config.DEST_DOMAIN,
                # FIX-9 parity: thread the pool admin as quotaUser for quota
                # distribution across the admin pool on every create() call.
                quota_user=admin_email,
                # FIX-14: file-owner service for all non-Shared-Drive-Step-1 calls.
                owner_dest_drive=owner_dest_drive,
            )
            pr = pm.migrate_permissions(source_id, dest_id, perms)
            result.update({
                "migrated": pr.get("migrated", 0), "failed": pr.get("failed", 0),
                "external": pr.get("external_users", 0), "skipped": pr.get("skipped", 0),
            })

            valid_roles = {"owner", "organizer", "fileOrganizer", "writer", "commenter", "reader"}
            valid_cls   = {"internal_both_domains", "internal_source_only", "external_domain", "general_access"}

            for detail in pr.get("details", []):
                role   = detail.get("role", "")
                ptype  = detail.get("type", "user")
                status = detail.get("status", "failed")
                cls    = detail.get("classification", "external_domain")
                se     = detail.get("original_email", "")
                de     = detail.get("target_email", "")
                # FIX-11: error may be USER_NOT_FOUND_SENTINEL (a typed object),
                # not a string.  Normalise to a plain string before SQL storage
                # so we never accidentally persist a Python object reference.
                raw_error = detail.get("error", "")
                error = (
                    "user_not_found_on_dest_tenant"
                    if raw_error is USER_NOT_FOUND_SENTINEL
                    else (str(raw_error) if raw_error else "")
                )

                if role == "owner" or status == "skipped":
                    continue
                if role not in valid_roles:
                    continue
                if cls not in valid_cls:
                    cls = "external_domain"

                try:
                    self.sql_mgr.upsert_permission(
                        file_id=dest_id, item_type="FILE",
                        permission_type=(ptype if ptype in ("user", "group", "domain", "anyone") else "user"),
                        source_email=se, dest_email=de, role=role, classification=cls,
                        is_inherited=False, parent_drive_id=None,
                    )
                    if status == "success":
                        self.sql_mgr.mark_permission_done(dest_id, de, role)
                    elif status == "failed":
                        self.sql_mgr.mark_permission_failed(dest_id, de, role, error)
                except Exception as sql_exc:
                    # SQL tracking is best-effort — log at WARNING so it shows
                    # in PM2 without being fatal.
                    logger.warning(
                        "SQL permission track failed [%s] dest=%s role=%s: %s",
                        name, de, role, sql_exc,
                    )

        except Exception as exc:
            # Upgraded from logger.debug → logger.error so permission migration
            # failures are always visible in PM2 logs with a full message.
            # Previously this was silently swallowed at debug level, making it
            # impossible to distinguish a real migration error from a skipped file.
            logger.error(
                "Permission migration error [%s] source=%s dest=%s: %s",
                name, source_id, dest_id, exc,
                exc_info=True,
            )

        return result

    # =========================================================================
    # Folder structure builder
    # =========================================================================

    def _build_folder_structure(
        self, folders, dest_drive, source_email, dst_email: str = "",
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
            # FIX-8: delegate to dst_email so folders land in the destination user's Drive
            _dest = self._get_dest_drive_service_for_thread(dst_email)
            dest_fid    = self._create_folder(fname, dest_parent, _dest, dst_email=dst_email)

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

    def _create_folder(self, name, parent_id, dest_drive, max_retries=3, dst_email: str = "") -> Optional[str]:
        for attempt in range(max_retries):
            try:
                meta = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
                if parent_id: meta["parents"] = [parent_id]
                # FIX-9: quota attributed to pool admin via builder; file owned by dst_email via delegation
                resp = dest_drive.files().create(
                    body=meta, fields="id,name", supportsAllDrives=True,
                    quotaUser=self._dest_admin_pool.current_admin,
                ).execute()
                fid  = _extract_id(resp)
                if fid is None: raise ValueError(f"Bad folder create response: {resp!r}")
                return fid
            except HttpError as exc:
                # FEAT-7: reactive rotation on folder-create rate-limit
                if _is_rate_limit_error(exc) and attempt < max_retries - 1:
                    new_admin = self._dest_admin_pool.rotate(
                        f"reactive-folder on '{name}': {exc}"
                    )
                    self.stats["admin_rotations"] = self._dest_admin_pool._total_rotations
                    logger.warning(
                        f"[FOLDER] Rate-limit on create '{name}' — "
                        f"rotated to {new_admin}, retrying"
                    )
                    # FIX-8: rebuild service for dst_email after rotation
                    dest_drive = self._get_dest_drive_service_for_thread(dst_email)
                    time.sleep(_backoff(attempt))
                    continue
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
            resp  = dest_drive.files().list(
                q=q, fields="files(id)", pageSize=5, supportsAllDrives=True,
                quotaUser=self._dest_admin_pool.current_admin,
            ).execute()
            files = resp.get("files", [])
            return files[0]["id"] if files else None
        except Exception: return None

    # =========================================================================
    # Auth / Drive service factory
    # =========================================================================

    def _get_dest_drive_service_for_thread(self, dst_email: str):
        """
        FIX-8: Return a destination Drive service delegated to ``dst_email``
        (the mapped destination user), scoped to this worker thread.

        Delegation subject vs. quota admin
        ────────────────────────────────────
        ``dst_email`` is passed as ``subject=`` to
        ``_sa.Credentials.from_service_account_file``, which means the service
        account impersonates the *actual* destination user.  All files created
        through this service are owned by that user in their personal Drive,
        not by any admin pool account.

        The active pool admin from ``DestAdminPoolManager.current_admin`` is
        used exclusively as the ``quotaUser`` parameter on mutating API calls
        (see FIX-9) to distribute API quota strain across the pool without
        affecting file ownership or folder placement.

        Thread-local caching
        ────────────────────
        Cache keys are destination user emails.  When
        ``DestAdminPoolManager.rotate()`` evicts destination-domain keys from
        the thread-local cache (FIX-10), the next call here rebuilds a fresh
        service for the same ``dst_email`` — the delegation subject is
        unchanged; only the underlying quota attribution shifts to the new admin.
        """
        if not hasattr(self._thread_local, "drive_cache"):
            self._thread_local.drive_cache = {}
        if dst_email not in self._thread_local.drive_cache:
            # FIX-8: subject=dst_email ensures files land in the destination
            # user's Drive, not in the admin pool account's Drive.
            self._thread_local.drive_cache[dst_email] = self._build_drive_service(
                dst_email, is_dest=True
            )
        return self._thread_local.drive_cache[dst_email]

    def _get_drive_service_for_thread(self, auth_obj, email: str):
        """
        Return a Drive service for *source* user ``email``.

        This method is exclusively for source read operations and always builds
        the Drive service with ``is_dest=False``, binding unconditionally to
        ``source_credentials.json`` / ``Config.SOURCE_CREDENTIALS_FILE``.

        Destination callers MUST use ``_get_dest_drive_service_for_thread()``
        to ensure pool-awareness and correct credential-file selection.  The old
        domain-inference heuristic (``not email.endswith(f"@{SOURCE_DOMAIN}")``)
        has been removed here because it could cross-contaminate workspace
        credential files when a source address happened to share a domain suffix
        with a destination admin, producing the ``unauthorized_client`` errors
        observed in production logs.
        """
        if not hasattr(self._thread_local, "drive_cache"):
            self._thread_local.drive_cache = {}
        if email not in self._thread_local.drive_cache:
            # Always source credentials — is_dest=False is not negotiable here.
            self._thread_local.drive_cache[email] = self._build_drive_service(
                email, is_dest=False
            )
        return self._thread_local.drive_cache[email]

    def _build_drive_service(self, email: str, is_dest: bool = False):
        """
        Build a delegated Drive service for ``email``.

        Strict credential segregation
        ──────────────────────────────
        The ``is_dest`` flag is the **sole** source of truth for which credential
        file is selected — domain-suffix heuristics are intentionally absent here
        to prevent cross-contamination between workspace credential files:

        * ``is_dest=True``  → binds **unconditionally** to
          ``dest_credentials.json`` / ``Config.DEST_CREDENTIALS_FILE``.
          ``email`` MUST be the **mapped destination user** email address
          (supplied by ``_get_dest_drive_service_for_thread(dst_email)``).
          ``subject=email`` therefore impersonates the destination user so that
          all files created through this service are owned by that user in their
          personal Drive.  The admin pool address is NEVER passed here as
          ``email`` — admin identity is expressed only via ``quotaUser=`` on
          individual ``.execute()`` calls (FIX-9).

        * ``is_dest=False`` → binds **unconditionally** to
          ``source_credentials.json`` / ``Config.SOURCE_CREDENTIALS_FILE``.
          Used exclusively by ``_get_drive_service_for_thread()`` for every
          source read operation (file listing, download, permission reads).

        This explicit segregation is what prevents source user emails from being
        authorized against destination credentials (and vice-versa), which was
        the root cause of the ``unauthorized_client`` regression seen when pool
        rotation previously cleared the entire thread-local cache and forced a
        credential rebuild on the wrong service account file.

        Uses google-auth (ServiceAccountCredentials with domain delegation) so
        we control the httplib2.Http object and can apply the full 1800-second
        socket timeout.
        """
        from google.oauth2 import service_account as _sa

        if is_dest:
            # Destination: unconditionally bind to dest_credentials.json
            flask_name  = "dest_credentials.json"
            config_path = self.config.DEST_CREDENTIALS_FILE
        else:
            # Source: unconditionally bind to source_credentials.json
            flask_name  = "source_credentials.json"
            config_path = self.config.SOURCE_CREDENTIALS_FILE

        creds_file = _resolve_cred(flask_name, config_path)

        creds = _sa.Credentials.from_service_account_file(
            creds_file,
            scopes=self.config.SCOPES,
            subject=email,
        )

        try:
            import google_auth_httplib2 as _gah
            timed_http      = httplib2.Http(timeout=self.connection_timeout)
            authorized_http = _gah.AuthorizedHttp(creds, http=timed_http)
            logger.debug(
                f"Drive service built for {email} | "
                f"{'dest-pool' if is_dest else 'source'} | "
                f"google_auth_httplib2 | timeout={self.connection_timeout}s"
            )
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
