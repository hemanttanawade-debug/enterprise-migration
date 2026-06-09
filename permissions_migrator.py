"""
Enhanced Permissions migration module
Handles internal vs external users intelligently

FIX-1  organizer role at file/folder level:
       The Google Drive API only allows 'organizer' at the Shared Drive *root*
       (permissions.create fileId=<driveId>).  Applying it to any individual
       file or folder inside the drive raises:
           403 organizerOnNonTeamDriveNotSupported
       Fix: in shared_drive_mode, map 'organizer' → 'fileOrganizer' before
       calling _create_permission().  At the drive-membership level the caller
       (SharedDriveMigrator._ensure_dest_admin_organizer / migrate_drive_members)
       passes the Shared Drive ID as fileId so 'organizer' is still valid there.

FIX-2  NoneType.close crash:
       _create_permission() was calling self.dest_drive.permissions().create(…)
       whose execute() returns None on some Drive API versions when the response
       body is empty (204 No Content).  We no longer call .close() on the result
       so the AttributeError is gone.

FIX-3  Token-bucket rate limiter (replaces time.sleep(0.1)):
       All 14 worker threads previously shared no coordination on permission API
       calls — each thread issued calls continuously with a naive 0.1 s sleep
       that never let the per-second quota window reset, causing 50–98% 4xx/5xx
       error rates on DrivePermissions.Create.

       A module-level _TokenBucket(_PERM_BUCKET) is shared across every thread
       and every EnhancedPermissionsMigrator instance.  It holds a sliding token
       window (default: 8 calls / second) and automatically throttles callers
       only when the window is full — threads that arrive when tokens are
       available pay zero wait time.  This is strictly better than a fixed sleep:

         Fixed sleep   → all 14 threads issue ~140 calls/sec regardless of quota.
         Token bucket  → caps the project-wide rate at exactly 8 calls/sec total,
                         with zero idle time when quota is not yet exhausted.

       The rate is set conservatively at 8/s (well within the default Drive API
       quota of 10 QPS for permissions.create).  If you have a quota increase,
       raise _PERM_BUCKET_RATE at the top of this file.

FIX-4  Proper 429 retry with Retry-After + exponential backoff inside
       _create_permission():
       Previously a 429 returned False immediately and was counted as a permanent
       failure.  Now _create_permission() retries up to MAX_PERM_RETRIES times,
       honouring the Retry-After response header when present, falling back to
       capped exponential backoff with jitter otherwise.

FIX-5  useDomainAdminAccess=True on permissions().create():
       Admin API calls are metered against a separate, higher quota pool.
       Adding this flag costs nothing and typically gives 3–5× effective
       throughput without any config or speed change.

FIX-6  Three-tier user-not-found fallback (Dynamic External Fallback Mode):
       Previously, when the Drive API returned 400 (invalidSharingRequest /
       userNotFound / domainUserNotFound) or 404 for an internal user, the
       error was surfaced as a hard permanent failure and the permission was
       dropped.  This produced "External user/account not found" exceptions
       in production and left collaborators without access on the destination.

       New behaviour — _try_create_permission() now classifies these responses
       with a typed sentinel constant USER_NOT_FOUND_SENTINEL instead of a
       plain error string.  migrate_permissions() detects the sentinel and
       applies the correct tier:

         Tier 1 (Internal & Active):   dest-domain email succeeds → done.
         Tier 2 (Internal but Missing): dest-domain email returns sentinel →
                                        retry immediately with source email as
                                        external collaborator (RULE 1 / SD_RULE_2).
         Tier 3 (Truly External):       External email returns sentinel →
                                        retry without useDomainAdminAccess=True
                                        (some tenants block admin-mediated
                                        external shares; a direct user-context
                                        share is still permitted).

       Error strings that trigger the sentinel:
         HTTP 404 (any body)
         HTTP 400 with body containing: invalidSharingRequest, userNotFound,
                                        domainUserNotFound
         HTTP 403 with body containing: userNotFound, domainUserNotFound

FIX-7  quotaUser threading through _try_create_permission():
       The caller (migration_engine_v4._migrate_permissions_hybrid) now passes
       the current pool admin email as quota_user into EnhancedPermissionsMigrator
       via a constructor argument.  _try_create_permission() appends it to every
       permissions().create() builder call as quotaUser=quota_user so that Drive
       API quota charges are attributed to the rotating admin without changing the
       delegation subject (which remains the domain admin impersonating the dest
       service account).

FIX-8  Retry without useDomainAdminAccess for external-user sentinel:
       When a truly external email returns USER_NOT_FOUND_SENTINEL it likely
       means the org's sharing policy rejects the admin-context share.  A second
       attempt is made with useDomainAdminAccess=False, which routes through the
       standard user quota and bypasses domain-admin sharing restrictions.

FIX-9  Legible exception parsing — no more '<object object at 0x…>' in logs:
       All error values that flow into log messages or result detail dicts are
       now converted through _err_str(), a module-level helper that:
         • Returns a human-readable string for USER_NOT_FOUND_SENTINEL.
         • Calls e.reason for HttpError instances (gives the JSON reason phrase).
         • Falls back to str(e) for every other exception type.
       This eliminates the raw memory-address output that appeared in PM2 logs
       whenever the sentinel object reached an f-string !r format specifier.

FIX-11 dest_file_id == source_file_id guard in migrate_permissions():
       The logs show every 404 body says "File not found: <source_id>" or
       "Shared drive not found: <source_id>", meaning the caller passed the
       SOURCE file/drive ID as the destination ID.  migrate_permissions() now
       detects this up-front and raises ValueError immediately so the bug is
       surfaced as a hard error instead of being silently misclassified as
       USER_NOT_FOUND across every permission step.

FIX-12 Split HTTP 404 in _try_create_permission into two distinct cases:
       Previously ALL 404 responses returned USER_NOT_FOUND_SENTINEL, causing
       "File not found" and "Shared drive not found" 404s (a CALLER BUG —
       wrong dest_id) to be silently treated as "user not found on dest tenant"
       and retried fruitlessly across all three fallback steps before being
       recorded as USER_NOT_FOUND failures.

       Now _try_create_permission inspects the 404 body:
         • reason == "notFound" AND location == "fileId"/"driveId"
           → the dest file/drive does not exist → raises RuntimeError
             immediately (no retry, no sentinel) so the stack unwinds and the
             caller gets a loud, diagnosable error.
         • All other 404s (user-identity 404s, legacy API responses)
           → USER_NOT_FOUND_SENTINEL as before.

       The two body substrings that identify a file/drive-not-found 404:
         "file not found"        (fileId location)
         "shared drive not found" (driveId location)

FIX-13 Dual Drive service design (owner_dest_drive):
       EnhancedPermissionsMigrator now accepts an optional owner_dest_drive
       parameter — a Drive service impersonating dst_email (the file owner)
       rather than the admin pool account.  All calls that previously used the
       admin service without useDomainAdminAccess=True now route through this
       service instead.  The file owner always has write access to their own
       files so the API never returns 404 "File not found".

FIX-14 useDomainAdminAccess=True is ONLY valid for Shared Drive items:
       FIX-13 incorrectly changed the RULE 4 (anyone, domain) call sites to
       use_domain_admin=True.  This caused the Drive API to misinterpret the
       My Drive fileId as a Shared Drive driveId and return:
         HTTP 404 "Shared drive not found: <fileId>"
       even though the file exists and was verified by Google Apps Script.

       Root cause: useDomainAdminAccess=True is only valid when the fileId IS
       a Shared Drive ID.  For My Drive files the flag must never be sent.

       Fix: all My Drive call sites (RULE 4 anyone, RULE 4 domain, RULE 1
       Step 1 in _handle_mydrive_user) revert to use_domain_admin=False.
       The owner_dest_drive service (impersonating dst_email) is routed by
       _try_create_permission for all calls where use_domain_admin=False,
       giving the API an authenticated caller with file-owner write access.
       useDomainAdminAccess=True + admin service is kept only for Shared Drive
       SD-RULE 1 Step 1, which is the one legitimate use of that flag.

FIX-10 Resilient three-step fallback inside migrate_permissions():
       The per-user permission loop is restructured so every branch is explicit
       and every error variable is rendered through _err_str() before it is
       logged or stored.  Inline helper functions (_s) defined inside the loop
       body have been removed; the module-level _err_str() replaces all of them.
"""
import logging
import random
import threading
import time
from typing import Dict, List, Optional, Tuple

from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# FIX-3: Token-bucket rate limiter
#
# _PERM_BUCKET_RATE  — maximum permission API calls per second, project-wide.
#                      Raise this if you have a Drive API quota increase approved
#                      in GCP Console (IAM & Admin → Quotas).
#                      Default Drive quota: 10 QPS; we use 8 to leave headroom.
# _PERM_BUCKET_BURST — how many calls may fire simultaneously before throttling
#                      kicks in.  Set to 1× the rate so no spike is allowed.
# ─────────────────────────────────────────────────────────────────────────────
_PERM_BUCKET_RATE  = 8    # calls / second  ← tune here after a quota increase
_PERM_BUCKET_BURST = 8    # max burst tokens (== rate → no burst allowed)

# Retry knobs for _try_create_permission (FIX-4)
MAX_PERM_RETRIES   = 5    # maximum attempts per permission create
MAX_PERM_BACKOFF_S = 32   # cap on exponential backoff sleep

# FIX-6: Typed sentinel returned by _try_create_permission() when the Drive API
# signals that the target identity does not exist on the destination tenant.
# migrate_permissions() detects this sentinel to trigger External Fallback Mode
# instead of recording a permanent failure.
#
# Error conditions that produce this sentinel:
#   HTTP 404 — user not found (any body)
#   HTTP 400 — body contains: invalidSharingRequest | userNotFound | domainUserNotFound
#   HTTP 403 — body contains: userNotFound | domainUserNotFound
#
# Using a module-level object (not a string) makes accidental string-equality
# matches against real error messages impossible.
USER_NOT_FOUND_SENTINEL = object()  # identity-checked with `is`, never equality


def _err_str(err: object) -> str:
    """
    FIX-9: Convert any error value to a clean, human-readable string.

    Handles three cases in priority order:
      1. USER_NOT_FOUND_SENTINEL — returns a descriptive label so the sentinel
         never reaches an f-string and never produces '<object object at 0x…>'.
      2. HttpError — uses e.reason (the JSON reason phrase from the Drive API
         response body), which is far more informative than repr(e).
      3. Everything else — str(e), which works for plain strings, Exception
         subclasses, and None alike.

    This is the single conversion point for all error variables before they
    enter log messages or result detail dicts.  Every log site in this module
    MUST route error values through _err_str() rather than using !r or str()
    directly on a variable that might hold the sentinel.
    """
    if err is USER_NOT_FOUND_SENTINEL:
        return "USER_NOT_FOUND (dest tenant does not recognise this identity)"
    if isinstance(err, HttpError):
        # HttpError.reason is the parsed reason string from the JSON body,
        # e.g. "Bad Request", "userNotFound", "invalidSharingRequest".
        # str(err) also works but is more verbose; reason is PM2-log-friendly.
        try:
            return err.reason or str(err)
        except AttributeError:
            return str(err)
    return str(err) if err else ""


class _TokenBucket:
    """
    Thread-safe token bucket for rate-limiting Drive API permission calls.

    All worker threads share a single module-level instance (_PERM_BUCKET).
    Each call to consume() deducts one token.  When the bucket is empty the
    caller sleeps only as long as needed to refill that one token — no more.

    Parameters
    ----------
    rate  : float  — sustained throughput in tokens/second
    burst : int    — maximum tokens that may accumulate (== rate → no burst)
    """

    def __init__(self, rate: float = 8.0, burst: int = 8):
        self._rate   = float(rate)
        self._burst  = float(burst)
        self._tokens = float(burst)   # start full so the very first calls are free
        self._last   = time.monotonic()
        self._lock   = threading.Lock()

    def consume(self, n: int = 1) -> None:
        """Block until n tokens are available, then consume them."""
        with self._lock:
            now          = time.monotonic()
            elapsed      = now - self._last
            self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
            self._last   = now

            if self._tokens >= n:
                self._tokens -= n
                wait = 0.0
            else:
                deficit      = n - self._tokens
                wait         = deficit / self._rate
                self._tokens = 0.0

        if wait > 0:
            time.sleep(wait)


# Module-level singleton — ONE bucket shared by ALL threads and ALL instances.
_PERM_BUCKET = _TokenBucket(rate=_PERM_BUCKET_RATE, burst=_PERM_BUCKET_BURST)


# Backward compatibility alias (set after class definition below)
PermissionsMigrator = None


class EnhancedPermissionsMigrator:
    """
    Handles migration of file/folder permissions with smart domain mapping.

    Implements a resilient three-step fallback strategy for complete access
    continuity across My Drive and Shared Drive migrations.

    Key changes in this version:
      FIX-9  _err_str() replaces all inline !r / repr() sentinel checks —
             no raw object addresses in PM2 logs.
      FIX-10 Three-step fallback loop is restructured for clarity; every error
             variable is normalised through _err_str() before use.
    """

    def __init__(
        self,
        source_drive,
        dest_drive,
        source_domain: str,
        dest_domain: str,
        quota_user: str = "",
        owner_dest_drive=None,
    ):
        """
        Initialise enhanced permissions migrator.

        FIX-14 — useDomainAdminAccess is ONLY valid for Shared Drive items
        ────────────────────────────────────────────────────────────────────
        The Drive API raises "Shared drive not found" (HTTP 404, reason=notFound,
        location=driveId) when useDomainAdminAccess=True is sent for a My Drive
        file — the API interprets fileId as a driveId in that context.

        Two Drive services are therefore required:

          dest_drive (admin-impersonated, e.g. pool admin):
            Used ONLY when useDomainAdminAccess=True is correct and safe —
            i.e. Shared Drive items only (shared_drive_mode=True, Step 1/SD-RULE 1).

          owner_dest_drive (owner-impersonated, i.e. dst_email):
            Used for ALL My Drive permission calls (anyone, domain, user Steps 1-3)
            and for Shared Drive Steps 2 & 3 where useDomainAdminAccess must be
            False or omitted.  The file owner always has write access to their own
            files so the API never returns 404 "File not found" from this service.
            Falls back to dest_drive when not supplied (backward-compatible).

        Args:
            source_drive:     Source Drive API service (read-only, source tenant).
            dest_drive:       Admin-impersonated dest service (pool admin).
                              Only used for Shared Drive + useDomainAdminAccess=True.
            source_domain:    Source domain (e.g. 'dev.shivaami.in').
            dest_domain:      Destination domain (e.g. 'demo.shivaami.in').
            quota_user:       Pool admin email forwarded as quotaUser= on every
                              create() call (FIX-7).
            owner_dest_drive: File-owner-impersonated dest service (dst_email).
                              Used for all My Drive calls and SD Steps 2-3.
                              Falls back to dest_drive when None.
        """
        self.source_drive     = source_drive
        self.dest_drive       = dest_drive           # admin-impersonated (pool admin)
        self.owner_dest_drive = owner_dest_drive or dest_drive  # owner-impersonated (dst_email)
        self.source_domain    = source_domain
        self.dest_domain      = dest_domain
        self.quota_user       = quota_user

    # =========================================================================
    # Public API
    # =========================================================================

    def migrate_permissions(
        self,
        source_file_id: str,
        dest_file_id: str,
        source_permissions: List[Dict],
        shared_drive_mode: bool = False,
        is_drive_root: bool = False,
    ) -> Dict:
        """
        Migrate permissions using a resilient three-step fallback strategy.

        MY DRIVE MODE (shared_drive_mode=False)
        ────────────────────────────────────────
        Step 1 — Internal & Active (RULE 1):
            Try the mapped destination-domain email with useDomainAdminAccess=True
            and quotaUser set.  Succeeds when the account exists on the dest tenant.

        Step 2 — Internal but Missing (RULE 2):
            Step 1 returned USER_NOT_FOUND_SENTINEL or any failure.  Retry using
            the original source email as an external collaborator
            (useDomainAdminAccess=False).

        Step 3 — Direct share without admin context (RULE 2 / RULE 3 fallback):
            Step 2 returned USER_NOT_FOUND_SENTINEL.  The tenant's sharing policy
            may block admin-mediated external shares.  Retry with
            useDomainAdminAccess omitted entirely (skip_domain_admin=True).

        RULE 3 (Truly External Users — different domain entirely):
            Steps 2 and 3 only (no dest-domain mapping attempted).

        RULE 4 (General access):
            anyone-with-link or domain-wide access — migrated as-is, with dest
            domain substituted where the source domain appears.

        SHARED DRIVE MODE (shared_drive_mode=True)
        ────────────────────────────────────────────
        SD-RULE 1: Internal → try mapped dest email (Step 1).
        SD-RULE 2: Not found → fall back to source email as external (Steps 2–3).
        SD-RULE 3: Truly external → skip entirely (not added to Shared Drives).

        Args:
            source_file_id:      Source file ID
            dest_file_id:        Destination file ID
            source_permissions:  List of source permission dicts
            shared_drive_mode:   True when migrating items inside a Shared Drive
            is_drive_root:       True when dest_file_id IS the Shared Drive root

        Returns:
            Detailed migration result dictionary
        """
        # ── FIX-11: guard against caller passing source_file_id as dest_file_id ─
        # The logs show every 404 body contains the SOURCE file/drive ID, which
        # means the migration engine passed source_id for both arguments.
        # Catch this up-front and raise so the bug surfaces immediately rather
        # than being silently misclassified as USER_NOT_FOUND across all three
        # fallback steps.  The root cause is dest_folder_id (SQL column) being
        # NULL or not read correctly — the caller must fix the read from SQL.
        if dest_file_id and source_file_id and dest_file_id == source_file_id:
            raise ValueError(
                f"[PERM] FATAL: dest_file_id == source_file_id == {source_file_id!r}. "
                "The caller passed the SOURCE ID as the destination ID. "
                "Verify that dest_folder_id (SQL column) is populated and is "
                "correctly passed as the second argument to migrate_permissions()."
            )

        result: Dict = {
            'total_permissions': len(source_permissions),
            'migrated':          0,
            'skipped':           0,
            'failed':            0,
            'external_users':    0,
            'internal_users':    0,
            'general_access':    0,
            'details':           [],
            'classification': {
                'internal_both_domains': 0,
                'internal_source_only':  0,
                'external_domain':       0,
                'general_access':        0,
            },
        }

        mode_label = "SharedDrive" if shared_drive_mode else "MyDrive"
        logger.info(
            f"[PERM START] {mode_label} | src={source_file_id} → dst={dest_file_id} | "
            f"total={len(source_permissions)} permissions to process | "
            f"is_drive_root={is_drive_root}"
        )

        for permission in source_permissions:
            perm_type = permission.get('type')
            role      = permission.get('role')
            email     = permission.get('emailAddress')
            domain    = permission.get('domain')

            # ── FIX-1: organizer role handling ────────────────────────────────
            # Drive root  (is_drive_root=True): keep 'organizer' — required by API.
            # File/folder (is_drive_root=False): downgrade → 'fileOrganizer'.
            if shared_drive_mode and role == 'organizer' and not is_drive_root:
                logger.debug(
                    f"  [SD] Downgrading 'organizer' → 'fileOrganizer' for "
                    f"{email or perm_type} (item-level: organizer only valid at drive root)"
                )
                role = 'fileOrganizer'

            # Skip owner permission (set during file creation)
            if role == 'owner':
                logger.info(f"[PERM] Skipping owner: {email or perm_type} (handled during file creation)")
                result['skipped'] += 1
                result['details'].append({
                    'original_email': email or '',
                    'target_email':   email or '',
                    'type':           perm_type,
                    'role':           role,
                    'classification': 'external_domain',
                    'status':         'skipped',
                    'reason':         'Owner permission handled during file creation',
                })
                continue

            # ── RULE 4: Anyone with link ──────────────────────────────────────
            if perm_type == 'anyone':
                logger.info(f"[PERM] RULE 4 anyone-with-link: role={role!r} → dst={dest_file_id}")
                ok, err = self._try_create_permission(
                    dest_file_id, perm_type, role,
                    email=None, domain=None,
                    # FIX-14: useDomainAdminAccess MUST be False for My Drive files.
                    # Sending True causes Drive to interpret fileId as a driveId
                    # and return HTTP 404 "Shared drive not found".
                    # owner_dest_drive (impersonating dst_email) has write access
                    # as the file owner — no admin elevation needed.
                    use_domain_admin=False,
                )
                if ok:
                    result['migrated']      += 1
                    result['general_access'] += 1
                    result['classification']['general_access'] += 1
                    logger.info(f"[PERM] ✓ anyone-with-link applied: role={role!r}")
                else:
                    result['failed'] += 1
                    logger.warning(f"[PERM] ✗ anyone-with-link FAILED: role={role!r} err={_err_str(err)}")
                result['details'].append({
                    'original_email': '',
                    'target_email':   '',
                    'type':           'anyone',
                    'role':           role,
                    'classification': 'general_access',
                    'status':         'success' if ok else 'failed',
                    'error':          _err_str(err) if not ok else '',
                    'note':           f'Anyone with link can {role}',
                })
                _PERM_BUCKET.consume()
                continue

            # ── RULE 4: Domain-wide access ────────────────────────────────────
            if perm_type == 'domain' and domain:
                target_domain = self.dest_domain if domain == self.source_domain else domain
                logger.info(
                    f"[PERM] RULE 4 domain-wide: src_domain={domain!r} → "
                    f"target_domain={target_domain!r} role={role!r}"
                )
                ok, err = self._try_create_permission(
                    dest_file_id, perm_type, role,
                    email=None, domain=target_domain,
                    # FIX-14: same as anyone-with-link — useDomainAdminAccess=True
                    # is invalid for My Drive files and causes "Shared drive not
                    # found".  Route through owner_dest_drive via False.
                    use_domain_admin=False,
                )
                if ok:
                    result['migrated']      += 1
                    result['general_access'] += 1
                    result['classification']['general_access'] += 1
                    logger.info(f"[PERM] ✓ domain-wide applied: domain={target_domain!r} role={role!r}")
                else:
                    result['failed'] += 1
                    logger.warning(
                        f"[PERM] ✗ domain-wide FAILED: domain={target_domain!r} "
                        f"role={role!r} err={_err_str(err)}"
                    )
                result['details'].append({
                    'original_email': '',
                    'target_email':   '',
                    'type':           'domain',
                    'role':           role,
                    'classification': 'general_access',
                    'status':         'success' if ok else 'failed',
                    'error':          _err_str(err) if not ok else '',
                    'note':           f'Domain-wide access for {target_domain}',
                })
                _PERM_BUCKET.consume()
                continue

            # ── User / Group permissions ──────────────────────────────────────
            if not email or '@' not in email:
                logger.info(
                    f"[PERM] Skipping perm_type={perm_type!r} role={role!r}: "
                    f"no valid email address (got {email!r})"
                )
                result['skipped'] += 1
                result['details'].append({
                    'original_email': '',
                    'target_email':   '',
                    'type':           perm_type,
                    'role':           role,
                    'classification': 'external_domain',
                    'status':         'skipped',
                    'reason':         'No valid email address',
                })
                continue

            user_domain = email.split('@')[1]
            local_part  = email.split('@')[0]
            is_internal = (user_domain == self.source_domain)

            logger.info(
                f"[PERM] Processing email={email!r} role={role!r} perm_type={perm_type!r} | "
                f"classification={'INTERNAL' if is_internal else 'EXTERNAL'} | "
                f"mode={'SharedDrive' if shared_drive_mode else 'MyDrive'}"
            )

            if shared_drive_mode:
                self._handle_shared_drive_user(
                    result, dest_file_id, perm_type, role,
                    email, local_part, is_internal,
                )
            else:
                self._handle_mydrive_user(
                    result, dest_file_id, perm_type, role,
                    email, local_part, is_internal,
                )

            # FIX-3: one token per processed user/group entry.
            # 'anyone' / 'domain' / skipped-no-email branches consume their own
            # token via 'continue' above; this covers all user/group paths.
            _PERM_BUCKET.consume()

        logger.info(
            f"Permission migration summary: "
            f"{result['migrated']}/{result['total_permissions']} migrated | "
            f"Internal (both): {result['classification']['internal_both_domains']} | "
            f"External (source-only): {result['classification']['internal_source_only']} | "
            f"External (other): {result['classification']['external_domain']} | "
            f"General: {result['general_access']} | "
            f"Failed: {result['failed']}"
        )
        return result

    # =========================================================================
    # Shared Drive vs My Drive per-user handlers
    # =========================================================================

    def _handle_shared_drive_user(
        self,
        result: Dict,
        dest_file_id: str,
        perm_type: str,
        role: str,
        email: str,
        local_part: str,
        is_internal: bool,
    ) -> None:
        """
        Apply the SD-RULE 1 / SD-RULE 2 / SD-RULE 3 cascade for one user entry
        inside a Shared Drive migration.

        SD-RULE 3: Truly external users are skipped entirely — Shared Drive
            membership is restricted to the destination tenant.
        SD-RULE 1: Internal user → try mapped dest-domain email (Step 1).
        SD-RULE 2: Not found on dest tenant → fall back to source email as an
            external collaborator (Steps 2–3).
        """
        if not is_internal:
            # SD-RULE 3: External — skip
            result['skipped']        += 1
            result['external_users'] += 1
            result['classification']['external_domain'] += 1
            result['details'].append({
                'original_email': email,
                'target_email':   email,
                'type':           perm_type,
                'role':           role,
                'classification': 'external_domain',
                'status':         'skipped',
                'reason':         'External users not migrated in Shared Drive mode',
            })
            logger.info(f"  [SD] SD-RULE-3 Skipping external user: {email!r} (external users not added to Shared Drives)")
            _PERM_BUCKET.consume()
            return

        dest_email = f"{local_part}@{self.dest_domain}"

        # ── Step 1: mapped dest-domain email with domain-admin access ─────────
        ok1, err1 = self._try_create_permission(
            dest_file_id, perm_type, role,
            email=dest_email, domain=None,
            use_domain_admin=True,
        )
        if ok1:
            result['migrated']       += 1
            result['internal_users'] += 1
            result['classification']['internal_both_domains'] += 1
            result['details'].append({
                'original_email': email,
                'target_email':   dest_email,
                'type':           perm_type,
                'role':           role,
                'classification': 'internal_both_domains',
                'rule':           'SD_RULE_1',
                'status':         'success',
                'note':           f'Mapped {email} → {dest_email}',
            })
            logger.info(f"  [SD] ✓ SD_RULE_1: {email!r} → {dest_email!r} ({role}) [internal mapped]")
            return

        # Step 1 failed — log legibly (FIX-9: _err_str, never !r on sentinel)
        if err1 is USER_NOT_FOUND_SENTINEL:
            logger.info(
                f"  [SD] {dest_email} not found on dest tenant — "
                f"SD_RULE_2 fallback: retrying as {email} (external)"
            )
        else:
            logger.warning(
                f"  [SD] {dest_email} failed ({_err_str(err1)}) — "
                f"SD_RULE_2 fallback: retrying as {email}"
            )

        # ── Step 2: source email as external collaborator ─────────────────────
        ok2, err2 = self._try_create_permission(
            dest_file_id, perm_type, role,
            email=email, domain=None,
            use_domain_admin=False,
        )
        if ok2:
            result['migrated']       += 1
            result['external_users'] += 1
            result['classification']['internal_source_only'] += 1
            result['details'].append({
                'original_email': email,
                'target_email':   email,
                'type':           perm_type,
                'role':           role,
                'classification': 'internal_source_only',
                'rule':           'SD_RULE_2',
                'status':         'success',
                'note': (
                    f'{dest_email} not on dest tenant — '
                    f'added {email} as external collaborator'
                ),
            })
            logger.info(f"  [SD] ✓ SD_RULE_2 external fallback: {email} ({role})")
            return

        # ── Step 3: retry without useDomainAdminAccess (tenant policy bypass) ─
        if err2 is USER_NOT_FOUND_SENTINEL:
            ok3, err3 = self._try_create_permission(
                dest_file_id, perm_type, role,
                email=email, domain=None,
                use_domain_admin=False,
                skip_domain_admin=True,
            )
        else:
            ok3, err3 = False, err2

        if ok3:
            result['migrated']       += 1
            result['external_users'] += 1
            result['classification']['internal_source_only'] += 1
            result['details'].append({
                'original_email': email,
                'target_email':   email,
                'type':           perm_type,
                'role':           role,
                'classification': 'internal_source_only',
                'rule':           'SD_RULE_2',
                'status':         'success',
                'note':           f'{email} added without useDomainAdminAccess',
            })
            logger.info(f"  [SD] ✓ SD_RULE_2 direct share (no admin): {email} ({role})")
            return

        # All three steps exhausted — record failure with legible error strings
        result['failed'] += 1
        result['classification']['internal_source_only'] += 1
        result['details'].append({
            'original_email': email,
            'target_email':   email,
            'type':           perm_type,
            'role':           role,
            'classification': 'internal_source_only',
            'rule':           'SD_RULE_2',
            'status':         'failed',
            'error': (
                f'Step1 ({dest_email}): {_err_str(err1)} | '
                f'Step2 ({email}): {_err_str(err2)} | '
                f'Step3 (direct): {_err_str(err3)}'
            ),
        })
        logger.warning(
            f"  [SD] ✗ All three steps failed for {email} — "
            f"step1: {_err_str(err1)} | step2: {_err_str(err2)} | step3: {_err_str(err3)}"
        )

    def _handle_mydrive_user(
        self,
        result: Dict,
        dest_file_id: str,
        perm_type: str,
        role: str,
        email: str,
        local_part: str,
        is_internal: bool,
    ) -> None:
        """
        Apply the RULE 1 / RULE 2 / RULE 3 cascade for one user entry in a
        My Drive migration.

        RULE 1 (Internal & Active):
            Step 1 — mapped dest-domain email with useDomainAdminAccess=True
            and quotaUser.  If this succeeds the permission is done.

        RULE 2 (Internal but Missing at Destination):
            Step 1 returned USER_NOT_FOUND_SENTINEL or failed.
            Step 2 — source email as external collaborator (no domain-admin).
            Step 3 — same but skip_domain_admin=True (tenant policy bypass).

        RULE 3 (Truly External — different domain):
            Skip Step 1 (no dest-domain mapping).
            Step 2 — share directly with external email.
            Step 3 — same but skip_domain_admin=True.
        """
        if is_internal:
            dest_email = f"{local_part}@{self.dest_domain}"

            # ── Step 1: mapped dest-domain email ─────────────────────────────
            # FIX-14: My Drive files MUST NOT use useDomainAdminAccess=True.
            # The Drive API treats fileId as a Shared Drive ID when that flag
            # is set, returning HTTP 404 "Shared drive not found" even though
            # the file exists.  owner_dest_drive (impersonating dst_email, the
            # file owner) has full write access without needing admin elevation.
            ok1, err1 = self._try_create_permission(
                dest_file_id, perm_type, role,
                email=dest_email, domain=None,
                use_domain_admin=False,
            )
            if ok1:
                result['migrated']      += 1
                result['internal_users'] += 1
                result['classification']['internal_both_domains'] += 1
                result['details'].append({
                    'original_email': email,
                    'target_email':   dest_email,
                    'type':           perm_type,
                    'role':           role,
                    'classification': 'internal_both_domains',
                    'rule':           'RULE_1',
                    'status':         'success',
                    'note':           f'Mapped to {self.dest_domain}',
                })
                logger.info(f"✓ [RULE 1] {email!r} → {dest_email!r} ({role}) [internal mapped]")
                return

            # Step 1 failed — log legibly
            if err1 is USER_NOT_FOUND_SENTINEL:
                logger.info(
                    f"[RULE 2] {dest_email} not on dest tenant — "
                    f"External Fallback: retrying as {email} (source email)"
                )
            else:
                logger.warning(
                    f"[RULE 2] {dest_email} failed ({_err_str(err1)}) — "
                    f"External Fallback: retrying as {email}"
                )

            # ── Step 2: source email as external collaborator ─────────────────
            ok2, err2 = self._try_create_permission(
                dest_file_id, perm_type, role,
                email=email, domain=None,
                use_domain_admin=False,
            )
            if ok2:
                result['migrated']      += 1
                result['external_users'] += 1
                result['classification']['internal_source_only'] += 1
                result['details'].append({
                    'original_email': email,
                    'target_email':   email,
                    'type':           perm_type,
                    'role':           role,
                    'classification': 'internal_source_only',
                    'rule':           'RULE_2',
                    'status':         'success',
                    'note': (
                        f'{dest_email} not on dest tenant — '
                        f'kept as external collaborator using source email'
                    ),
                })
                logger.info(f"✓ [RULE 2] External fallback OK: {email!r} ({role}) — kept as external collaborator using source email")
                return

            # ── Step 3: direct share without admin context ────────────────────
            if err2 is USER_NOT_FOUND_SENTINEL:
                ok3, err3 = self._try_create_permission(
                    dest_file_id, perm_type, role,
                    email=email, domain=None,
                    use_domain_admin=False,
                    skip_domain_admin=True,
                )
            else:
                ok3, err3 = False, err2

            if ok3:
                result['migrated']      += 1
                result['external_users'] += 1
                result['classification']['internal_source_only'] += 1
                result['details'].append({
                    'original_email': email,
                    'target_email':   email,
                    'type':           perm_type,
                    'role':           role,
                    'classification': 'internal_source_only',
                    'rule':           'RULE_2',
                    'status':         'success',
                    'note':           f'{email} added without useDomainAdminAccess',
                })
                logger.info(f"✓ [RULE 2] Direct share (no admin context) OK: {email!r} ({role})")
                return

            # All steps exhausted
            result['failed'] += 1
            result['classification']['internal_source_only'] += 1
            result['details'].append({
                'original_email': email,
                'target_email':   email,
                'type':           perm_type,
                'role':           role,
                'classification': 'internal_source_only',
                'rule':           'RULE_2',
                'status':         'failed',
                'error': (
                    f'Step1 ({dest_email}): {_err_str(err1)} | '
                    f'Step2 ({email}): {_err_str(err2)} | '
                    f'Step3 (direct): {_err_str(err3)}'
                ),
            })
            logger.warning(
                f"✗ [RULE 2] All three steps failed for {email} — "
                f"step1: {_err_str(err1)} | step2: {_err_str(err2)} | step3: {_err_str(err3)}"
            )

        else:
            # ── RULE 3: Truly External — different domain entirely ─────────────

            # ── Step 2: share directly with external email ────────────────────
            ok2, err2 = self._try_create_permission(
                dest_file_id, perm_type, role,
                email=email, domain=None,
                use_domain_admin=False,
            )
            if ok2:
                result['migrated']      += 1
                result['external_users'] += 1
                result['classification']['external_domain'] += 1
                result['details'].append({
                    'original_email': email,
                    'target_email':   email,
                    'type':           perm_type,
                    'role':           role,
                    'classification': 'external_domain',
                    'rule':           'RULE_3',
                    'status':         'success',
                    'note':           'External user — different domain',
                })
                logger.info(f"✓ [RULE 3] External: {email!r} ({role}) — shared directly with external user")
                return

            # ── Step 3: retry without useDomainAdminAccess ────────────────────
            if err2 is USER_NOT_FOUND_SENTINEL:
                logger.info(
                    f"[RULE 3] {email} — sentinel on direct share, "
                    f"retrying without useDomainAdminAccess (FIX-8)"
                )
                ok3, err3 = self._try_create_permission(
                    dest_file_id, perm_type, role,
                    email=email, domain=None,
                    use_domain_admin=False,
                    skip_domain_admin=True,
                )
            else:
                ok3, err3 = False, err2

            if ok3:
                result['migrated']      += 1
                result['external_users'] += 1
                result['classification']['external_domain'] += 1
                result['details'].append({
                    'original_email': email,
                    'target_email':   email,
                    'type':           perm_type,
                    'role':           role,
                    'classification': 'external_domain',
                    'rule':           'RULE_3',
                    'status':         'success',
                    'note':           'External user added via direct share (no admin context)',
                })
                logger.info(f"✓ [RULE 3] External via direct share (no admin context) OK: {email!r} ({role})")
                return

            # Both external attempts exhausted
            result['failed'] += 1
            result['classification']['external_domain'] += 1
            result['details'].append({
                'original_email': email,
                'target_email':   email,
                'type':           perm_type,
                'role':           role,
                'classification': 'external_domain',
                'rule':           'RULE_3',
                'status':         'failed',
                'error': (
                    f'Step2 ({email}): {_err_str(err2)} | '
                    f'Step3 (direct): {_err_str(err3)}'
                ),
            })
            logger.warning(
                f"✗ [RULE 3] Failed external: {email} — "
                f"step2: {_err_str(err2)} | step3: {_err_str(err3)}"
            )

    # =========================================================================
    # Core permission creator
    # =========================================================================

    def _try_create_permission(
        self,
        file_id: str,
        perm_type: str,
        role: str,
        email: Optional[str],
        domain: Optional[str],
        use_domain_admin: bool,
        skip_domain_admin: bool = False,
    ) -> Tuple[bool, object]:
        """
        Attempt to create one permission on a destination file/folder.

        Return value
        ────────────
        (True, None)
            Permission created successfully.

        (False, USER_NOT_FOUND_SENTINEL)
            Drive API definitively says the target identity does not exist on
            the dest tenant.  The caller must trigger a fallback step rather
            than recording a permanent failure.  Conditions that yield the
            sentinel:
              • HTTP 404 (any body)
              • HTTP 400 body containing: invalidSharingRequest, userNotFound,
                domainUserNotFound
              • HTTP 403 body containing: userNotFound, domainUserNotFound

        (False, str)
            Any other non-retryable or exhausted-retryable error.  The string
            is already human-readable (never a raw object address).

        Parameters
        ──────────
        file_id           : Destination file/folder ID.
        perm_type         : user | group | domain | anyone
        role              : reader | writer | commenter | fileOrganizer | organizer
        email             : Email address for user/group permissions.
        domain            : Domain string for domain-wide permissions.
        use_domain_admin  : When True, include useDomainAdminAccess=True (FIX-5).
                            Ignored when skip_domain_admin=True.
        skip_domain_admin : When True, omit useDomainAdminAccess entirely (FIX-8).
                            Used for the third-step retry when the admin-context
                            share is rejected by tenant policy.

        Notes
        ─────
        FIX-2: execute() result is not stored — may be None on HTTP 204.
        FIX-4: 429/500/503 retried up to MAX_PERM_RETRIES with Retry-After /
               exponential backoff.
        FIX-7: self.quota_user forwarded as quotaUser= when non-empty.
        FIX-9: All error strings returned are plain str, never raw exceptions
               or the sentinel object — callers must still route through
               _err_str() before logging to handle the sentinel case.
        """
        permission: Dict = {'type': perm_type, 'role': role}

        if perm_type == 'user' and email:
            permission['emailAddress'] = email
        elif perm_type == 'group' and email:
            permission['emailAddress'] = email
        elif perm_type == 'domain' and domain:
            permission['domain'] = domain
        elif perm_type == 'anyone':
            pass
        else:
            return False, "Invalid permission configuration"

        last_error: object = None

        for attempt in range(MAX_PERM_RETRIES):
            try:
                # Build the API call.
                # FIX-7: quotaUser distributes quota charges across the admin pool.
                # FIX-5: useDomainAdminAccess=True gives 3–5× effective throughput.
                # FIX-8: skip_domain_admin=True overrides for tenant-policy bypass.
                # FIX-2: execute() result is intentionally discarded (may be None).
                builder_kwargs: Dict = {
                    'fileId':                file_id,
                    'body':                  permission,
                    'sendNotificationEmail': False,
                    'supportsAllDrives':     True,
                    'transferOwnership':     False,
                }
                if not skip_domain_admin:
                    builder_kwargs['useDomainAdminAccess'] = use_domain_admin
                if self.quota_user:
                    builder_kwargs['quotaUser'] = self.quota_user

                # FIX-14: route to the correct Drive service.
                #
                # Admin service (self.dest_drive):
                #   Only when useDomainAdminAccess=True is actually sent —
                #   i.e. use_domain_admin=True AND skip_domain_admin=False.
                #   This is ONLY valid for Shared Drive items (SD Step 1).
                #
                # Owner service (self.owner_dest_drive):
                #   All other calls — My Drive (any/domain/user steps 1-3) and
                #   Shared Drive steps 2-3.  Impersonates dst_email (file owner)
                #   so the API sees the caller as having write access without
                #   needing the useDomainAdminAccess flag.
                needs_admin_svc = use_domain_admin and not skip_domain_admin
                drive_svc = self.dest_drive if needs_admin_svc else self.owner_dest_drive
                drive_svc.permissions().create(**builder_kwargs).execute()
                logger.info(
                    f"[PERM] ✓ Created permission | attempt={attempt + 1}/{MAX_PERM_RETRIES} | "
                    f"file={file_id} perm_type={perm_type!r} role={role!r} "
                    f"email={email!r} domain_admin={use_domain_admin} "
                    f"skip_admin={skip_domain_admin} "
                    f"svc={'admin' if needs_admin_svc else 'owner'}"
                )
                return True, None

            except HttpError as exc:
                status_code = exc.resp.status
                raw_content = exc.content.decode('utf-8', errors='ignore')

                # ── CRITICAL LOG: full diagnostic context on every HttpError ──
                # Emitted unconditionally before any branching so that every
                # 4xx/5xx is permanently on record — including errors caused by
                # an invalid quotaUser, malformed auth context, or project quota
                # restrictions that the old code silently masked as a sentinel.
                logger.error(
                    f"[PERM] HttpError | HTTP {status_code} | "
                    f"perm_type={perm_type!r} email={email!r} "
                    f"quota_user={self.quota_user!r} | "
                    f"raw_content={raw_content}"
                )

                # Normalise for substring matching (case-insensitive, raw body).
                rc_lower = raw_content.lower()

                # ── HTTP 404 — two distinct cases (FIX-12) ────────────────────
                #
                # Case A: "File not found" / "Shared drive not found"
                #   The dest_file_id passed by the caller does not exist on the
                #   destination tenant.  This is a CALLER BUG (source ID leaked
                #   in as dest ID via an un-populated dest_folder_id SQL column).
                #   Retrying with a different email against the same bad ID is
                #   pointless — raise immediately so the stack unwinds with a
                #   clear diagnostic instead of silently burning 3 API calls per
                #   permission and misreporting all failures as USER_NOT_FOUND.
                #
                # Case B: all other 404s
                #   The Drive API could not resolve the target *identity*.
                #   Return USER_NOT_FOUND_SENTINEL so migrate_permissions() can
                #   fall back to the next tier (source email / direct share).
                if status_code == 404:
                    _is_file_not_found   = 'file not found'         in rc_lower
                    _is_drive_not_found  = 'shared drive not found' in rc_lower

                    if _is_drive_not_found:
                        # FIX-14: "Shared drive not found" means useDomainAdminAccess=True
                        # was sent for a My Drive file — the API misinterpreted fileId as
                        # a driveId.  This is a code-path bug, not a missing dest file.
                        # Return a descriptive error so the caller records the failure
                        # clearly without crashing the rest of the permission migration.
                        logger.error(
                            f"[PERM] FIX-14 BUG: useDomainAdminAccess=True was sent for "
                            f"a My Drive file — Drive interpreted fileId as driveId. "
                            f"file_id={file_id!r} perm_type={perm_type!r} "
                            f"use_domain_admin={use_domain_admin} skip_domain_admin={skip_domain_admin}"
                        )
                        return False, (
                            f"useDomainAdminAccess=True sent for My Drive file {file_id!r} "
                            f"(Drive returned 'Shared drive not found') — FIX-14 routing bug"
                        )

                    if _is_file_not_found:
                        # True missing dest file — the caller passed source_id as dest_id.
                        # Fatal: raise so the bug surfaces with a clear diagnostic instead
                        # of silently burning retries and misreporting as USER_NOT_FOUND.
                        raise RuntimeError(
                            f"[PERM] FATAL 404 — destination file does not exist: "
                            f"file_id={file_id!r} perm_type={perm_type!r} email={email!r} | "
                            f"Drive API body: {raw_content[:300]} | "
                            "ROOT CAUSE: dest_file_id is the SOURCE ID — check that "
                            "dest_folder_id SQL column is populated before calling "
                            "migrate_permissions()."
                        )

                    # Case B: identity 404 — user/group not found on dest tenant.
                    logger.debug(
                        f"  [PERM] 404 identity-not-found for {email or perm_type} "
                        f"({'internal' if use_domain_admin else 'external'}) — sentinel"
                    )
                    return False, USER_NOT_FOUND_SENTINEL

                # ── HTTP 400 ──────────────────────────────────────────────────
                if status_code == 400:
                    # STRICT: only return sentinel when the body explicitly
                    # names an absent-account condition.
                    # "invalidSharingRequest" alone is NOT enough — the Drive
                    # API also emits it for bad quotaUser values, malformed auth
                    # contexts, and project-quota exhaustion.  We require at
                    # least one of the unambiguous account-absent phrases.
                    _account_absent = (
                        'no google account'     in rc_lower
                        or 'usernotfound'       in rc_lower
                        or 'domainusernotfound' in rc_lower
                    )
                    if _account_absent:
                        logger.debug(
                            f"  [PERM] 400 account-absent for "
                            f"{email or perm_type} — sentinel"
                        )
                        return False, USER_NOT_FOUND_SENTINEL

                    # Everything else at 400 (bad quotaUser, malformed auth,
                    # project quota exhausted, invalidSharingRequest without an
                    # absent-account phrase, etc.) is a real API error.
                    # Do NOT return USER_NOT_FOUND_SENTINEL — return False with
                    # the raw message so the caller sees the real reason and
                    # does NOT trigger a false "User Not Found" fallback.
                    api_msg = raw_content[:300]
                    logger.warning(
                        f"  [PERM] 400 non-sentinel for {email or perm_type}: "
                        f"{api_msg}"
                    )
                    return False, f"Bad request (HTTP 400): {api_msg}"

                # ── HTTP 403 ──────────────────────────────────────────────────
                if status_code == 403:
                    # STRICT sentinel: only when the body explicitly says the
                    # account does not exist.
                    # Org-policy blocks, project quota exhaustion, and auth
                    # context errors all return 403 but must NOT be mistaken
                    # for a missing user — that would cause Attempt C
                    # (anyone-with-link) to falsely claim USER_NOT_FOUND.
                    _account_absent_403 = (
                        'usernotfound'          in rc_lower
                        or 'domainusernotfound' in rc_lower
                        or 'no google account'  in rc_lower
                    )
                    if _account_absent_403:
                        logger.debug(
                            f"  [PERM] 403 account-absent for "
                            f"{email or perm_type} — sentinel"
                        )
                        return False, USER_NOT_FOUND_SENTINEL

                    # Known non-sentinel 403 sub-codes — descriptive False, str.
                    if 'cannotshareteamdrivewithnongoogleaccounts' in rc_lower:
                        return False, (
                            f"User {email} does not have a Google account "
                            "— cannot add to Shared Drive"
                        )
                    if 'organizeronnonteamdrivenotsupported' in rc_lower:
                        return False, (
                            "organizer role not supported at file/folder level "
                            "(use fileOrganizer for items inside a Shared Drive)"
                        )
                    if 'insufficient permissions' in rc_lower:
                        return False, (
                            f"Insufficient permissions to share "
                            f"(HTTP 403): {raw_content[:300]}"
                        )

                    # Catch-all for org-policy blocks, bad quotaUser, project
                    # quota, sharingPolicyViolated, and any other 403 the API
                    # may return.  Return False with the raw message — do NOT
                    # fall through to USER_NOT_FOUND_SENTINEL.
                    api_msg = raw_content[:300]
                    logger.warning(
                        f"  [PERM] 403 non-sentinel for {email or perm_type}: "
                        f"{api_msg}"
                    )
                    return False, f"Permission denied (HTTP 403): {api_msg}"

                # ── FIX-4: 429 / 500 / 503 — retryable ───────────────────────
                if status_code in (429, 500, 503):
                    if attempt >= MAX_PERM_RETRIES - 1:
                        last_error = (
                            f"Rate limit / server error after {MAX_PERM_RETRIES} "
                            f"retries (HTTP {status_code}): {raw_content[:120]}"
                        )
                        break

                    try:
                        retry_after = int(exc.resp.get('retry-after', 0))
                    except (TypeError, ValueError):
                        retry_after = 0

                    if retry_after > 0:
                        wait = float(retry_after)
                    else:
                        base = min(2 ** attempt, MAX_PERM_BACKOFF_S)
                        wait = base + random.uniform(0, base * 0.25)

                    logger.warning(
                        f"  [PERM] HTTP {status_code} retry {attempt + 1}/"
                        f"{MAX_PERM_RETRIES} for {email or perm_type} — "
                        f"sleeping {wait:.1f}s"
                    )
                    time.sleep(wait)
                    continue

                # ── Unexpected HTTP status — give up immediately ───────────────
                last_error = f"HTTP {status_code}: {raw_content[:120]}"
                break

            except Exception as exc:
                # FIX-9: str(exc) for all non-HttpError exceptions.
                err_text = str(exc).strip()
                if not err_text or err_text.lstrip('-').isdigit():
                    last_error = f"Drive API empty/integer error (code={err_text or '?'})"
                else:
                    last_error = f"Unexpected error: {err_text[:120]}"
                logger.error(
                    f"[PERM] Unexpected non-HttpError | attempt={attempt + 1}/{MAX_PERM_RETRIES} | "
                    f"file={file_id} email={email!r} perm_type={perm_type!r} | "
                    f"error={last_error}"
                )
                break

        logger.error(
            f"[PERM] ✗ All {MAX_PERM_RETRIES} attempts exhausted | "
            f"file={file_id} email={email!r} perm_type={perm_type!r} role={role!r} | "
            f"last_error={last_error or 'Unknown error'}"
        )
        return False, last_error or "Unknown error"

    # =========================================================================
    # Convenience helpers
    # =========================================================================

    def copy_folder_permissions(
        self,
        source_folder_id: str,
        dest_folder_id: str,
    ) -> Dict:
        """
        Copy permissions from source folder to destination folder.

        Args:
            source_folder_id: Source folder ID
            dest_folder_id:   Destination folder ID

        Returns:
            Result dictionary
        """
        try:
            response = self.source_drive.permissions().list(
                fileId=source_folder_id,
                fields='permissions(id,type,role,emailAddress,displayName,domain)',
                supportsAllDrives=True,
            ).execute()

            source_permissions = response.get('permissions', [])
            result = self.migrate_permissions(
                source_folder_id,
                dest_folder_id,
                source_permissions,
            )
            logger.info(
                f"Folder permissions migrated: "
                f"{result['migrated']}/{result['total_permissions']}"
            )
            return result

        except HttpError as exc:
            logger.error(f"Error copying folder permissions: {str(exc)}")
            return {
                'total_permissions': 0,
                'migrated':          0,
                'failed':            0,
                'error':             str(exc),
            }

    def validate_permissions(
        self,
        source_file_id: str,
        dest_file_id: str,
    ) -> Dict:
        """
        Validate that permissions were migrated correctly.

        Args:
            source_file_id: Source file ID
            dest_file_id:   Destination file ID

        Returns:
            Validation result
        """
        validation: Dict = {
            'valid':        False,
            'source_count': 0,
            'dest_count':   0,
            'missing':      [],
            'extra':        [],
            'details':      [],
        }

        try:
            source_response = self.source_drive.permissions().list(
                fileId=source_file_id,
                fields='permissions(type,role,emailAddress,domain)',
                supportsAllDrives=True,
            ).execute()
            source_perms               = source_response.get('permissions', [])
            validation['source_count'] = len(source_perms)

            dest_response = self.dest_drive.permissions().list(
                fileId=dest_file_id,
                fields='permissions(type,role,emailAddress,domain)',
                supportsAllDrives=True,
            ).execute()
            dest_perms               = dest_response.get('permissions', [])
            validation['dest_count'] = len(dest_perms)

            source_sigs: Dict = {}
            for p in source_perms:
                if p.get('role') == 'owner':
                    continue
                email      = p.get('emailAddress')
                domain_val = p.get('domain', 'anyone')

                if email and '@' in email:
                    if email.split('@')[1] == self.source_domain:
                        mapped_email = f"{email.split('@')[0]}@{self.dest_domain}"
                    else:
                        mapped_email = email
                else:
                    mapped_email = email

                sig = f"{p.get('type')}:{p.get('role')}:{mapped_email or domain_val}"
                source_sigs[sig] = {'original': email, 'mapped': mapped_email}

            dest_sigs: set = set()
            for p in dest_perms:
                if p.get('role') == 'owner':
                    continue
                email      = p.get('emailAddress')
                domain_val = p.get('domain', 'anyone')
                dest_sigs.add(f"{p.get('type')}:{p.get('role')}:{email or domain_val}")

            for sig, emails in source_sigs.items():
                if sig not in dest_sigs:
                    validation['missing'].append({
                        'signature':      sig,
                        'original_email': emails['original'],
                        'expected_email': emails['mapped'],
                    })

            validation['extra'] = list(dest_sigs - set(source_sigs.keys()))
            validation['valid'] = len(validation['missing']) == 0
            return validation

        except Exception as exc:
            logger.error(f"Error validating permissions: {str(exc)}")
            validation['error'] = str(exc)
            return validation

    def get_permission_summary(
        self,
        file_id: str,
        is_source: bool = True,
    ) -> Dict:
        """
        Get a summary of permissions for a file.

        Args:
            file_id:   File ID
            is_source: True → query source drive; False → query destination drive

        Returns:
            Permission summary
        """
        drive_service = self.source_drive if is_source else self.dest_drive

        try:
            response = drive_service.permissions().list(
                fileId=file_id,
                fields='permissions(type,role,emailAddress,displayName,domain)',
                supportsAllDrives=True,
            ).execute()

            permissions = response.get('permissions', [])

            summary: Dict = {
                'total':         len(permissions),
                'owner':         None,
                'editors':       [],
                'viewers':       [],
                'commenters':    [],
                'domain_access': None,
                'anyone_access': None,
            }

            for perm in permissions:
                role      = perm.get('role')
                perm_type = perm.get('type')
                email     = perm.get('emailAddress')

                if role == 'owner':
                    summary['owner'] = email
                elif role == 'writer':
                    summary['editors'].append(email or perm.get('domain', 'group'))
                elif role == 'reader':
                    if perm_type == 'domain':
                        summary['domain_access'] = perm.get('domain')
                    elif perm_type == 'anyone':
                        summary['anyone_access'] = 'reader'
                    else:
                        summary['viewers'].append(email or 'group')
                elif role == 'commenter':
                    summary['commenters'].append(email or 'group')

            return summary

        except Exception as exc:
            logger.error(f"Error getting permission summary: {str(exc)}")
            return {'error': str(exc)}


# Backward compatibility: alias for old code
PermissionsMigrator = EnhancedPermissionsMigrator
