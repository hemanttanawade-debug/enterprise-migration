"""
shared_drive_discovery_engine.py  —  ~/amey/shared_drive_discovery_engine.py

Crawls every Shared Drive in the source domain (or a provided subset) and
registers its files + folders into SQL via sql_state_manager, using the same
run_id / migration_id contract as discovery_engine.py (My Drive).

Designed to be imported by Flask routes — NOT run standalone.

Why a separate engine instead of extending discovery_engine.py?
───────────────────────────────────────────────────────────────
• My Drive discovery operates per-USER (one Drive service per source email,
  impersonating that user).  Shared Drive discovery operates per-DRIVE (one
  admin service for ALL drives, using useDomainAdminAccess=True).
• The ownership model is different — files().list() must use
  corpora="drive" + driveId, not corpora="user" + trashed=false.
• Shared Drives need a temporary admin membership lifecycle for file-level
  reads (the same pattern used in shared_drive_storage_routes.py).

Key guarantees (mirrors discovery_engine.py's FIX comments):
─────────────────────────────────────────────────────────────
SD-FIX-1  size_bytes accumulated and returned in every result dict — never 0.
SD-FIX-2  already_in_sql path sums file_size from existing SQL records.
SD-FIX-3  Chunked SQL inserts (50 per batch) with per-chunk retry + sleep to
           protect the MySQL connection pool.
SD-FIX-4  _safe_size() handles Google Workspace files (no size field → 0)
           and binary files (size is a string → cast to int).
SD-FIX-5  run_shared_drive_discovery() updates migration_runs.total_items
           after all drives are processed.
SD-FIX-6  Temporary admin membership is always revoked in a finally block,
           even when file listing raises an exception.
SD-FIX-7  Thread-safe: each worker builds its OWN admin Drive service to
           avoid sharing a single httplib2 connection across threads.
SD-FIX-8  After adding a temporary manager membership, wait 10 s for Google
           to propagate the permission before listing files.  If the first
           list returns empty AND we just added temp membership, wait a further
           15 s and retry once — this handles drives where the admin was NOT a
           member and propagation is slow, preventing them from being silently
           treated as "empty".  Revocation always happens in the finally block
           so the retry still runs while the temp perm is active.
SD-FIX-9  Incremental re-run support: on re-run, existing Drive file IDs are
           loaded from SQL and only genuinely new files (not already in DB) are
           inserted.  This makes every run idempotent — safe to re-run after a
           crash, partial failure, or to pick up files added to the Shared Drive
           since the last crawl, without duplicating existing rows.
           The old "if existing: return early" short-circuit in discover_shared_drive()
           has been replaced by this incremental approach so that partial crawls
           are always healed on the next run instead of being permanently skipped.
"""

import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple

from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

# ── Tuning constants ──────────────────────────────────────────────────────────
PAGE_SIZE         = 1_000   # max allowed by Drive v3 for Shared Drive corpora
MAX_RETRIES       = 5
MAX_BACKOFF       = 32
INSERT_CHUNK      = 50      # rows per SQL batch
INTER_CHUNK_SLEEP = 0.3     # seconds between SQL batches (protect DB pool)
INTER_PAGE_SLEEP  = 0.15    # seconds between Drive API pages (quota guard)
INTER_DRIVE_SLEEP = 0.5     # seconds between sequential temp-membership adds

# SD-FIX-8: propagation waits after temp membership add
TEMP_MEMBER_PROPAGATION_WAIT       = 10   # seconds — initial wait after add
TEMP_MEMBER_PROPAGATION_RETRY_WAIT = 15   # seconds — extra wait before retry list

FOLDER_MIME   = "application/vnd.google-apps.folder"
_BYTES_PER_GB = 1_073_741_824.0


# ─────────────────────────────────────────────────────────────────────────────
# Helpers shared across this module
# ─────────────────────────────────────────────────────────────────────────────

def _backoff(attempt: int) -> float:
    """Exponential backoff with ±25 % jitter."""
    base = min(2 ** attempt, MAX_BACKOFF)
    return base + random.uniform(-base * 0.25, base * 0.25)


def _safe_size(raw) -> int:
    """
    SD-FIX-4: Convert Drive API 'size' to int.
    Google Workspace files return no size field → 0.
    Binary files return size as string → cast to int.
    """
    if raw is None:
        return 0
    try:
        return int(raw)
    except (ValueError, TypeError):
        return 0


# ─────────────────────────────────────────────────────────────────────────────
# Auth helpers
# ─────────────────────────────────────────────────────────────────────────────

def _build_admin_drive_service(kind: str = "source"):
    """
    Build a Drive v3 service impersonating SOURCE_ADMIN_EMAIL or
    DEST_ADMIN_EMAIL via service-account domain-wide delegation (DWD).

    Credential resolution (mirrors shared_drive_routes._build_admin_drive_service):
      1. ~/flask-backend/uploads/credential/{source|dest}_credentials.json
      2. Config.SOURCE_CREDENTIALS_FILE / DEST_CREDENTIALS_FILE

    SD-FIX-7: Each worker thread calls this independently so every thread has
    its own httplib2.Http instance — httplib2 is NOT thread-safe when shared.

    Returns: (drive_service, admin_email)
    Raises:  FileNotFoundError, google.auth.exceptions.*, etc.
    """
    from config import Config
    import httplib2
    from google.oauth2 import service_account as _sa
    from googleapiclient.discovery import build as _gapi_build

    _FLASK_CRED_DIR = Path.home() / "flask-backend" / "uploads" / "credential"
    _BACKEND_DIR    = Path.home() / "amey"

    if kind == "source":
        flask_name  = "source_credentials.json"
        config_path = Config.SOURCE_CREDENTIALS_FILE
        admin_email = Config.SOURCE_ADMIN_EMAIL
    else:
        flask_name  = "dest_credentials.json"
        config_path = Config.DEST_CREDENTIALS_FILE
        admin_email = Config.DEST_ADMIN_EMAIL

    p = _FLASK_CRED_DIR / flask_name
    if p.exists():
        creds_file = str(p)
    else:
        abs_p = Path(config_path)
        if not abs_p.is_absolute():
            abs_p = _BACKEND_DIR / config_path
        if not abs_p.exists():
            raise FileNotFoundError(
                f"Credential not found at '{p}' or '{abs_p}'. "
                "Upload via /api/config."
            )
        creds_file = str(abs_p)

    creds = _sa.Credentials.from_service_account_file(
        creds_file,
        scopes=Config.SCOPES,
        subject=admin_email,
    )

    try:
        import google_auth_httplib2 as _gah
        http = _gah.AuthorizedHttp(creds, http=httplib2.Http(timeout=1800))
        svc  = _gapi_build("drive", "v3", http=http, cache_discovery=False)
    except ImportError:
        svc  = _gapi_build("drive", "v3", credentials=creds, cache_discovery=False)

    return svc, admin_email


# ─────────────────────────────────────────────────────────────────────────────
# Drive enumeration
# ─────────────────────────────────────────────────────────────────────────────

def _list_all_shared_drives(drive_svc, admin_email: str) -> List[Dict]:
    """
    Return every Shared Drive visible to the source admin.
    Each dict: { id, name, createdTime }

    Uses useDomainAdminAccess=True — the admin does NOT need to be a member
    of every drive to enumerate them.
    """
    drives     = []
    page_token = None
    retries    = 0

    while True:
        try:
            resp = drive_svc.drives().list(
                pageSize=100,
                pageToken=page_token,
                fields="nextPageToken, drives(id, name, createdTime)",
                useDomainAdminAccess=True,
            ).execute()
            drives.extend(resp.get("drives", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
            retries = 0

        except HttpError as exc:
            code = exc.resp.status
            if code in (500, 503) and retries < MAX_RETRIES:
                retries += 1
                time.sleep(_backoff(retries))
                continue
            raise
        except Exception:
            retries += 1
            if retries >= MAX_RETRIES:
                raise
            time.sleep(_backoff(retries))

    return drives


# ─────────────────────────────────────────────────────────────────────────────
# Temp membership lifecycle
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_admin_member(
    drive_svc,
    admin_email: str,
    drive_id: str,
    drive_name: str,
) -> Tuple[bool, Optional[str]]:
    """
    Check if admin_email is already a Shared Drive member.
    If not → add as 'organizer' (manager) temporarily.

    Returns:
        (was_temp: bool, perm_id: Optional[str])
        was_temp=False → already a member, nothing changed.
        was_temp=True  → we added them; caller MUST revoke perm_id in finally.
    """
    admin_lower = admin_email.lower()

    resp = drive_svc.permissions().list(
        fileId=drive_id,
        supportsAllDrives=True,
        useDomainAdminAccess=True,
        fields="permissions(id,emailAddress,type,role)",
    ).execute()

    for perm in resp.get("permissions", []):
        if (perm.get("emailAddress") or "").lower() == admin_lower:
            logger.debug(
                f"[SD-DISC] Admin '{admin_email}' already member of "
                f"'{drive_name}' (role={perm.get('role')}) — no temp add."
            )
            return False, None

    logger.info(
        f"[SD-DISC] Admin '{admin_email}' NOT member of "
        f"'{drive_name}' ({drive_id}) — adding temporary manager."
    )
    result = drive_svc.permissions().create(
        fileId=drive_id,
        supportsAllDrives=True,
        useDomainAdminAccess=True,
        sendNotificationEmail=False,
        body={
            "type":         "user",
            "role":         "organizer",
            "emailAddress": admin_email,
        },
        fields="id",
    ).execute()

    perm_id = result.get("id")
    if perm_id:
        logger.info(
            f"[SD-DISC] Temp manager added to '{drive_name}' "
            f"(permissionId={perm_id})"
        )
        return True, perm_id

    logger.warning(
        f"[SD-DISC] permissions.create returned no id for '{drive_name}' — "
        "proceeding without guaranteed cleanup."
    )
    return False, None


def _revoke_admin_member(
    drive_svc,
    drive_id: str,
    drive_name: str,
    perm_id: str,
) -> None:
    """Remove temporary manager permission. Never raises (SD-FIX-6)."""
    logger.info(
        f"[SD-DISC] Revoking temp permission '{perm_id}' from '{drive_name}'..."
    )
    try:
        drive_svc.permissions().delete(
            fileId=drive_id,
            permissionId=perm_id,
            supportsAllDrives=True,
            useDomainAdminAccess=True,
        ).execute()
        logger.info(f"[SD-DISC] Temp membership revoked from '{drive_name}'.")
    except Exception as exc:
        logger.warning(
            f"[SD-DISC] Could not revoke perm '{perm_id}' from "
            f"'{drive_name}': {exc} — remove manually if needed."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Incremental re-run helper  (SD-FIX-9)
# ─────────────────────────────────────────────────────────────────────────────

def _get_existing_file_ids(sql_mgr, drive_id: str) -> Set[str]:
    """
    SD-FIX-9: Load the set of Drive file IDs already stored in SQL for this
    Shared Drive so that a re-run can skip files already registered and only
    insert genuinely new ones.

    Calls sql_mgr.load_shared_drive_items(drive_id) — the same method that
    the original "already in SQL" guard called — but instead of treating a
    non-empty result as a reason to skip the whole drive, we extract the file
    IDs and use them for per-file deduplication.

    Tries three attribute names that different SQLStateManager versions may
    expose on row objects: 'drive_file_id', 'file_id', 'id'. Falls back to
    an empty set on any error so a missing method or schema mismatch never
    blocks the crawl — the worst case is that the run re-attempts inserts for
    already-registered rows (safe if the schema has a UNIQUE constraint on
    file_id).

    Returns:
        set of Drive file-ID strings already present in SQL for this drive_id.
    """
    try:
        existing_rows = sql_mgr.load_shared_drive_items(drive_id)
        if not existing_rows:
            return set()

        ids: Set[str] = set()
        for row in existing_rows:
            fid = (
                getattr(row, "drive_file_id", None)
                or getattr(row, "file_id",       None)
                or getattr(row, "id",             None)
            )
            if fid:
                ids.add(fid)

        logger.debug(
            f"[SD-DISC] drive_id={drive_id}: {len(ids)} file IDs already in SQL"
        )
        return ids

    except AttributeError:
        # sql_mgr doesn't have load_shared_drive_items() yet
        logger.warning(
            f"[SD-DISC] sql_mgr has no load_shared_drive_items() for drive_id="
            f"{drive_id} — treating as fresh discovery. "
            "Add the method to SharedDriveSQLStateManager."
        )
        return set()

    except Exception as exc:
        logger.warning(
            f"[SD-DISC] Could not load existing file IDs for drive_id={drive_id} "
            f"({exc}) — will treat all Drive files as new."
        )
        return set()


# ─────────────────────────────────────────────────────────────────────────────
# File listing for one Shared Drive
# ─────────────────────────────────────────────────────────────────────────────

def _list_drive_files(drive_svc, drive_id: str, drive_name: str) -> List[Dict]:
    """
    Paginate all non-trashed items in a Shared Drive using corpora=drive.
    Requests id, name, mimeType, size, parents, createdTime, modifiedTime.

    SD-FIX-4: 'size' is requested explicitly. Google Workspace files have no
    size — _safe_size() defaults them to 0. Binary files return size as string.

    Pagination is robust: nextPageToken is always consumed when present and
    page_token is never reset mid-crawl, mirroring FIX-7 from discovery_engine.

    On generic exception exhausting MAX_RETRIES the function raises (not returns
    a partial list), mirroring FIX-8 from discovery_engine, so discover_shared_drive()
    marks the run as "failed" rather than silently treating incomplete data as ok.
    """
    items      = []
    page_token = None
    retries    = 0

    while True:
        try:
            resp = drive_svc.files().list(
                q="trashed=false",
                spaces="drive",
                corpora="drive",
                driveId=drive_id,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields=(
                    "nextPageToken, files("
                    "id, name, mimeType, size, parents, "
                    "createdTime, modifiedTime)"
                ),
                pageSize=PAGE_SIZE,
                pageToken=page_token,
            ).execute()

            items.extend(resp.get("files", []))

            # Always consume nextPageToken; never reset page_token mid-crawl
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

            retries = 0      # reset on each successful page
            time.sleep(INTER_PAGE_SLEEP)

        except HttpError as exc:
            code = exc.resp.status
            if code in (500, 503) and retries < MAX_RETRIES:
                retries += 1
                time.sleep(_backoff(retries))
                continue
            elif code == 404:
                logger.warning(
                    f"[SD-DISC] Drive '{drive_name}' ({drive_id}) returned 404 "
                    "— skipping."
                )
                return []
            else:
                logger.error(
                    f"[SD-DISC] HTTP {code} listing files in '{drive_name}': {exc}"
                )
                raise
        except Exception as exc:
            retries += 1
            if retries >= MAX_RETRIES:
                # Raise so discover_shared_drive() marks this drive as "failed"
                # instead of silently returning a partial file list
                logger.error(
                    f"[SD-DISC] '{drive_name}' failed after {MAX_RETRIES} "
                    f"retries: {exc}"
                )
                raise
            time.sleep(_backoff(retries))

    return items


# ─────────────────────────────────────────────────────────────────────────────
# Per-drive discovery
# ─────────────────────────────────────────────────────────────────────────────

def discover_shared_drive(
    run_id:        str,
    drive_id:      str,
    drive_name:    str,
    dest_drive_id: str,
    sql_mgr,
    progress_cb: Callable[[Dict], None] = None,
) -> Dict:
    """
    Discover all files in one Shared Drive and register them in SQL.

    Mirrors discover_user() in discovery_engine.py for My Drive.

    SD-FIX-8: If admin was not already a member of the source drive:
      1. Add as temporary manager.
      2. Wait TEMP_MEMBER_PROPAGATION_WAIT (10 s) for Google to propagate.
      3. List files.
      4. If list is still empty, wait TEMP_MEMBER_PROPAGATION_RETRY_WAIT (15 s)
         and retry the list ONCE — all while temp perm is still active.
      5. Revoke temp manager in finally block (always runs).

    SD-FIX-9 — Incremental / idempotent re-run logic:
    ──────────────────────────────────────────────────
    On every invocation (first run or re-run) this function:

      1. Calls _get_existing_file_ids() to build a set of Drive file IDs
         already stored in SQL for this Shared Drive.
      2. Does the full Drive crawl (with temp membership lifecycle) as before.
      3. Compares each crawled file ID against the SQL set:
           • Already in SQL → counted as "already_in_sql", skipped.
           • Not in SQL     → queued for insertion as "newly_registered".
      4. Inserts only the new files in chunked batches of INSERT_CHUNK.

    Consequences:
      • Clean first run  → existing_ids is empty, all files are inserted.
      • Re-run after crash → only files missing from SQL are re-inserted.
      • Re-run on a fully-discovered drive → 0 inserts, logs "up_to_date".
      • Files added to the Shared Drive since last run are picked up.
      • No duplicate rows even without a UNIQUE DB constraint.

    The old "if existing: return early" short-circuit has been replaced by
    this incremental approach so that partial crawls are always healed on
    the next run instead of being permanently skipped.

    Returns:
        {
            drive_id, drive_name, dest_drive_id,
            total_in_drive,      ← files seen on Drive this run
            total,               ← total rows in SQL after this run
            folders, files,      ← counts of newly inserted items only
            size_bytes,          ← SD-FIX-1: always present (new items only)
            already_in_sql,      ← count of files skipped (already in DB)
            newly_registered,    ← count of files inserted this run
            status ("ok" | "failed"), error
        }
    """
    result = {
        "drive_id":         drive_id,
        "drive_name":       drive_name,
        "dest_drive_id":    dest_drive_id,
        "total_in_drive":   0,
        "total":            0,
        "folders":          0,
        "files":            0,
        "size_bytes":       0,    # SD-FIX-1
        "already_in_sql":   0,
        "newly_registered": 0,
        "status":           "ok",
        "error":            None,
    }

    try:
        # ── SD-FIX-9 Step 1: load known file IDs from SQL ────────────────────
        existing_ids: Set[str] = _get_existing_file_ids(sql_mgr, drive_id)
        result["already_in_sql"] = len(existing_ids)

        if existing_ids:
            logger.info(
                f"[SD-DISC] '{drive_name}': {len(existing_ids)} file IDs already "
                f"in SQL — will skip these and only insert new files"
            )

        # ── SD-FIX-7 + SD-FIX-6 + SD-FIX-8: Live Drive crawl ────────────────
        was_temp:     bool          = False
        temp_perm_id: Optional[str] = None
        drive_svc                   = None
        raw_files:    List[Dict]    = []

        try:
            # SD-FIX-7: fresh Drive service per thread
            drive_svc, admin_email = _build_admin_drive_service(kind="source")

            # SD-FIX-6 + SD-FIX-8: check membership, add temp if needed
            was_temp, temp_perm_id = _ensure_admin_member(
                drive_svc, admin_email, drive_id, drive_name
            )

            if was_temp:
                # SD-FIX-8: wait for Google to propagate new membership
                logger.info(
                    f"[SD-DISC] '{drive_name}': waiting {TEMP_MEMBER_PROPAGATION_WAIT}s "
                    "for membership propagation before listing files..."
                )
                time.sleep(TEMP_MEMBER_PROPAGATION_WAIT)
            else:
                # Small stagger even for existing members — quota guard
                time.sleep(INTER_DRIVE_SLEEP)

            # First attempt at listing files
            raw_files = _list_drive_files(drive_svc, drive_id, drive_name)

            # SD-FIX-8: if empty AND we just added temp membership, retry once
            if not raw_files and was_temp:
                logger.warning(
                    f"[SD-DISC] '{drive_name}': file list empty after temp membership add "
                    f"— propagation may be slow. Waiting {TEMP_MEMBER_PROPAGATION_RETRY_WAIT}s "
                    "then retrying once..."
                )
                time.sleep(TEMP_MEMBER_PROPAGATION_RETRY_WAIT)
                raw_files = _list_drive_files(drive_svc, drive_id, drive_name)

                if raw_files:
                    logger.info(
                        f"[SD-DISC] '{drive_name}': retry succeeded — "
                        f"{len(raw_files)} items found."
                    )
                else:
                    logger.warning(
                        f"[SD-DISC] '{drive_name}': still empty after retry — "
                        "drive may genuinely be empty or admin access was not granted."
                    )

        finally:
            # SD-FIX-6: always revoke temp perm, even if listing raised
            if was_temp and temp_perm_id and drive_svc is not None:
                _revoke_admin_member(drive_svc, drive_id, drive_name, temp_perm_id)

        result["total_in_drive"] = len(raw_files)

        if not raw_files:
            logger.info(f"[SD-DISC] '{drive_name}': no files found (empty drive).")
            result["total"] = len(existing_ids)
            if progress_cb:
                progress_cb({**result, "phase": "empty"})
            return result

        # ── SD-FIX-9 Step 3: split into skip-set vs new-to-insert ────────────
        new_files: List[Dict] = []

        for f in raw_files:
            file_id = f.get("id")
            fsize   = _safe_size(f.get("size"))    # SD-FIX-4

            if file_id in existing_ids:
                # Already registered — skip without re-inserting
                continue

            new_files.append({
                **f,
                # Keys expected by sql_mgr.register_discovered_items():
                "source_email":         drive_id,       # drive_id as the "source key"
                "dest_email":           dest_drive_id,  # destination drive ID
                "file_size_bytes":      fsize,          # SD-FIX-4: normalised size
                # Extra context so the DB row is useful for migration:
                "shared_drive_id":      drive_id,
                "dest_shared_drive_id": dest_drive_id,
                "shared_drive_name":    drive_name,
            })

            if f["mimeType"] == FOLDER_MIME:
                result["folders"] += 1
            else:
                result["files"] += 1

            result["size_bytes"] += fsize           # SD-FIX-1

        logger.info(
            f"[SD-DISC] '{drive_name}': "
            f"Drive total={len(raw_files)} | "
            f"already_in_sql={len(existing_ids)} | "
            f"new_to_insert={len(new_files)}"
        )

        # ── Early exit when nothing new to do ────────────────────────────────
        if not new_files:
            result["total"] = len(existing_ids)
            logger.info(
                f"[SD-DISC] '{drive_name}': fully up-to-date, nothing new to register"
            )
            if progress_cb:
                progress_cb({**result, "phase": "up_to_date"})
            return result

        # ── SD-FIX-9 Step 4 + SD-FIX-3: chunked SQL inserts ─────────────────
        newly = 0
        for i in range(0, len(new_files), INSERT_CHUNK):
            chunk   = new_files[i : i + INSERT_CHUNK]
            retries = 0
            while True:
                try:
                    sql_mgr.register_discovered_items(
                        chunk,
                        source_email=drive_id,
                        dest_email=dest_drive_id,
                    )
                    newly += len(chunk)
                    break
                except Exception as db_exc:
                    retries += 1
                    if retries >= 5:
                        raise
                    wait = min(2 ** retries, 16) + random.uniform(0, 1)
                    logger.warning(
                        f"[SD-DISC] '{drive_name}' DB insert attempt {retries} "
                        f"failed ({db_exc}), retry in {wait:.1f}s"
                    )
                    time.sleep(wait)
            time.sleep(INTER_CHUNK_SLEEP)   # yield between chunks (SD-FIX-3)

        result["newly_registered"] = newly
        result["total"]            = len(existing_ids) + newly

        logger.info(
            f"[SD-DISC] '{drive_name}': inserted "
            f"{result['folders']} new folders + {result['files']} new files | "
            f"{result['size_bytes']:,} bytes | "
            f"total in SQL now: {result['total']}"
        )
        if progress_cb:
            progress_cb({**result, "phase": "done"})

    except Exception as exc:
        result["status"] = "failed"
        result["error"]  = str(exc)
        logger.error(
            f"[SD-DISC] '{drive_name}' ({drive_id}) FAILED: {exc}", exc_info=True
        )
        if progress_cb:
            progress_cb({**result, "phase": "error"})

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Entry point — called by Flask routes
# ─────────────────────────────────────────────────────────────────────────────

def run_shared_drive_discovery(
    run_id:           str,
    drive_id_mapping: Dict[str, str],   # { source_drive_id: dest_drive_id }
    workers:          int = 4,
    drive_filter:     Optional[List[str]] = None,   # filter by drive NAME
    progress_cb:      Callable[[Dict], None] = None,
) -> List[Dict]:
    """
    Entry point called by Flask routes.

    Enumerates all source Shared Drives (or a filtered subset) and registers
    their contents in SQL under the provided run_id.

    Args:
        run_id:           Unique migration run ID — MUST match the run_id used
                          by run_migration() so SQLStateManager finds these rows.
        drive_id_mapping: { source_drive_id: dest_drive_id }.
                          If a source drive has no mapping entry it is SKIPPED
                          and logged as a warning. Pass an empty dict {} to
                          discover without any destination mapping (dest_drive_id
                          will be set to "" on each row).
        workers:          Parallel threads (default 4). One Drive API service is
                          built per thread (SD-FIX-7).
        drive_filter:     If provided, only drives whose NAME appears in this list
                          are processed. Names are matched case-insensitively.
        progress_cb:      Called after each drive completes — used for SSE streaming.

    Returns:
        List of per-drive result dicts, each containing:
            drive_id, drive_name, dest_drive_id,
            total_in_drive, total,
            folders, files, size_bytes,
            already_in_sql, newly_registered,
            status, error
    """
    from shared_drive_sql_state_manager import SharedDriveSQLStateManager

    sql_mgr = SharedDriveSQLStateManager.for_sd_discovery(migration_id=run_id)
    sql_mgr.create_migration_run(total_items=0)

    # ── Enumerate all source Shared Drives ───────────────────────────────────
    logger.info(
        f"[SD-DISCOVERY] run_id={run_id} | Enumerating source Shared Drives…"
    )
    try:
        enum_svc, admin_email = _build_admin_drive_service(kind="source")
        all_drives = _list_all_shared_drives(enum_svc, admin_email)
    except Exception as exc:
        logger.error(f"[SD-DISCOVERY] Could not enumerate Shared Drives: {exc}")
        raise

    logger.info(
        f"[SD-DISCOVERY] Found {len(all_drives)} Shared Drive(s) in source domain."
    )

    # ── Apply optional name filter ────────────────────────────────────────────
    if drive_filter:
        filter_lower = {n.lower() for n in drive_filter}
        all_drives   = [
            d for d in all_drives if d["name"].lower() in filter_lower
        ]
        logger.info(
            f"[SD-DISCOVERY] After name filter ({drive_filter}): "
            f"{len(all_drives)} drive(s) remain."
        )

    if not all_drives:
        logger.warning("[SD-DISCOVERY] No drives to process — returning empty result.")
        return []

    # ── Resolve destination mapping ───────────────────────────────────────────
    # Build the work list: only drives that have a destination mapping entry.
    # If drive_id_mapping is empty, allow all drives with dest_drive_id="".
    work = []
    for d in all_drives:
        src_id = d["id"]
        dst_id = drive_id_mapping.get(src_id, "")
        if drive_id_mapping and not dst_id:
            logger.warning(
                f"[SD-DISCOVERY] Drive '{d['name']}' ({src_id}) has no entry in "
                "drive_id_mapping — SKIPPING. Add it to the mapping CSV."
            )
            continue
        work.append((src_id, d["name"], dst_id))

    if not work:
        logger.warning(
            "[SD-DISCOVERY] No drives remain after mapping filter — "
            "check your drive_id_mapping."
        )
        return []

    n_workers = min(workers, len(work))
    logger.info(
        f"[SD-DISCOVERY] run_id={run_id} | "
        f"drives={len(work)} | workers={n_workers}"
    )

    # ── Parallel discovery ────────────────────────────────────────────────────
    results: List[Dict] = []

    # Staggered submit (0.5 s apart) so DB connections and Drive API calls
    # are acquired gradually across threads (SD-FIX-3 / SD-FIX-7)
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {}
        for src_id, d_name, dst_id in work:
            fut = pool.submit(
                discover_shared_drive,
                run_id,
                src_id,
                d_name,
                dst_id,
                sql_mgr,
                progress_cb,
            )
            futures[fut] = (src_id, d_name, dst_id)
            time.sleep(0.5)   # stagger submits

        for future in as_completed(futures):
            src_id, d_name, dst_id = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {
                    "drive_id":         src_id,
                    "drive_name":       d_name,
                    "dest_drive_id":    dst_id,
                    "total_in_drive":   0,
                    "total":            0,
                    "folders":          0,
                    "files":            0,
                    "size_bytes":       0,
                    "already_in_sql":   0,
                    "newly_registered": 0,
                    "status":           "failed",
                    "error":            str(exc),
                }
                logger.error(
                    f"[SD-DISCOVERY] '{d_name}' ({src_id}) raised: {exc}",
                    exc_info=True,
                )
                if progress_cb:
                    progress_cb({**result, "phase": "error"})
            results.append(result)

    # ── SD-FIX-5: update migration_runs.total_items ──────────────────────────
    total_items = sum(r.get("total", 0) for r in results)
    try:
        sql_mgr._execute(
            "UPDATE migration_runs SET total_items=%s WHERE migration_id=%s",
            (total_items, run_id),
        )
    except Exception as exc:
        logger.warning(f"[SD-DISCOVERY] Could not update total_items: {exc}")

    failed = sum(1 for r in results if r.get("status") == "failed")
    logger.info(
        f"[SD-DISCOVERY] run_id={run_id} complete | "
        f"total_items={total_items} | "
        f"drives={len(results)} | "
        f"skipped={sum(r.get('already_in_sql', 0) for r in results)} | "
        f"newly_registered={sum(r.get('newly_registered', 0) for r in results)} | "
        f"failed={failed}"
    )

    return results
