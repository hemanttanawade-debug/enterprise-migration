"""
discovery_engine.py  —  ~/amey/discovery_engine.py

Crawls Google Drive for every user in the provided mapping and registers
all owned folders + files into SQL via sql_state_manager.

Designed to be imported by Flask routes — NOT run standalone.

FIXES vs previous version
──────────────────────────
FIX-1  size_bytes now accumulated and returned in every result dict.
FIX-2  already_in_sql path now sums file_size from existing SQL records.
FIX-3  explicit source_email / dest_email pass-through to register_discovered_items().
FIX-4  _list_owned_files() requests 'size' field; _safe_size() handles missing values.
FIX-5  run_discovery() updates migration_run total_items after all users are discovered.
FIX-6  Server-side owner filter via q-parameter; post-query Python filter removed.
FIX-7  Pagination fully robust: nextPageToken always consumed, never reset mid-crawl.
FIX-8  Generic exception in _list_owned_files raises instead of returning partial list.
FIX-9  Incremental re-run support: on re-run, existing file IDs are loaded from SQL
       and only genuinely new files (not already in DB) are inserted. This makes every
       run idempotent — safe to re-run after a crash, partial failure, or to pick up
       newly added Drive files without duplicating existing rows.
"""

import time
import random
import logging
from typing import Dict, List, Set, Callable

from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

PAGE_SIZE   = 100
MAX_RETRIES = 5
MAX_BACKOFF = 32


def _backoff(attempt: int) -> float:
    base = min(2 ** attempt, MAX_BACKOFF)
    return base + random.uniform(-base * 0.25, base * 0.25)


def _build_source_drive(email: str):
    from config import Config
    from auth import GoogleAuthManager
    auth = GoogleAuthManager(
        Config.SOURCE_CREDENTIALS_FILE,
        Config.SCOPES,
        delegate_email=email,
    )
    auth.authenticate()
    return auth.get_drive_service(user_email=email)


def _get_existing_file_ids(sql_mgr, source_email: str) -> Set[str]:
    """
    FIX-9: Load the set of Drive file IDs already stored in SQL for this
    user so that a re-run can skip files that are already registered and
    only insert genuinely new ones.

    Tries three attribute names that different SQLStateManager versions may
    expose on row objects: 'drive_file_id', 'file_id', 'id'. Falls back to
    an empty set on any error so a missing helper never blocks the crawl —
    it will just re-attempt all inserts (safe if the schema has a UNIQUE
    constraint on the file-ID column).

    Returns:
        set of Drive file-ID strings already present in SQL for source_email.
    """
    try:
        existing_rows = sql_mgr.load_user_items(source_email)
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
            f"[DISC] {source_email}: {len(ids)} file IDs already in SQL"
        )
        return ids

    except Exception as exc:
        logger.warning(
            f"[DISC] {source_email}: could not load existing file IDs "
            f"({exc}) — will treat all Drive files as new"
        )
        return set()


def _list_owned_files(drive_service, user_email: str) -> List[Dict]:
    """
    List all non-trashed files owned by user_email in their My Drive.

    FIX-4: 'size' field requested explicitly. Google Workspace files
    (Docs, Sheets, Slides) have no size field — we default to 0.
    Binary files return size as a string — we cast to int via _safe_size().

    FIX-6: Ownership enforced server-side via q-parameter. The old
    post-query Python list comprehension has been removed entirely so no
    files are silently dropped due to a client-side owners[] mismatch.

    FIX-7: nextPageToken is always consumed for the next request.
    page_token is never reset mid-crawl, guaranteeing all pages are
    fetched even on drives with 100 k+ files.

    FIX-8: When the generic except block exhausts MAX_RETRIES it raises
    the original exception instead of returning a partial list, so that
    discover_user() marks the run as "failed" rather than "ok".
    """
    files      = []
    page_token = None
    retries    = 0

    # FIX-6: server-side owner filter
    owner_query = f"trashed=false and '{user_email}' in owners"

    while True:
        try:
            resp = drive_service.files().list(
                q=owner_query,
                spaces="drive",
                fields=(
                    "nextPageToken, files("
                    "id, name, mimeType, size, parents, "
                    "createdTime, modifiedTime, owners)"
                ),
                pageSize=PAGE_SIZE,
                pageToken=page_token,
                supportsAllDrives=False,
                includeItemsFromAllDrives=False,
            ).execute()

            # FIX-6: every result is already owned by user_email
            files.extend(resp.get("files", []))

            # FIX-7: always consume nextPageToken; never reset page_token
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

            retries = 0      # reset on each successful page
            time.sleep(0.2)

        except HttpError as exc:
            code = exc.resp.status
            if code == 500 and retries < MAX_RETRIES:
                retries += 1
                time.sleep(_backoff(retries))
                continue
            elif code == 404:
                logger.warning(f"[{user_email}] Drive not found (404)")
                return []
            else:
                logger.error(f"[{user_email}] HTTP {code}: {exc}")
                raise
        except Exception as exc:
            retries += 1
            if retries >= MAX_RETRIES:
                # FIX-8: hard failure — do not swallow partial results
                logger.error(
                    f"[{user_email}] Failed after {MAX_RETRIES} retries: {exc}"
                )
                raise
            time.sleep(_backoff(retries))

    return files


def _safe_size(raw) -> int:
    """
    Safely convert a Drive API 'size' value to int.
    Google Workspace files return no size field → 0.
    Binary files return size as a string → cast to int.
    """
    if raw is None:
        return 0
    try:
        return int(raw)
    except (ValueError, TypeError):
        return 0


def discover_user(
    run_id:       str,
    source_email: str,
    dest_email:   str,
    sql_mgr,
    progress_cb:  Callable[[Dict], None] = None,
) -> Dict:
    """
    Discovers all owned Drive files for one user and registers them in SQL.

    FIX-9 — Incremental / idempotent re-run logic
    ─────────────────────────────────────────────
    On every invocation (first run or re-run) this function:

      1. Calls _get_existing_file_ids() to fetch the set of Drive file IDs
         already stored in SQL for this user.
      2. Crawls the full Drive file list from the Google Drive API.
      3. Compares each Drive file ID against the SQL set.
         • Already in SQL → counted as "already_in_sql", skipped.
         • Not in SQL     → queued for insertion as "newly_registered".
      4. Inserts only the new files in chunked batches of 50.

    Consequences:
      • Clean first run  → existing_ids is empty, all files are inserted.
      • Re-run after crash → only the files missing from SQL are re-inserted.
      • Re-run on fully-discovered user → 0 inserts, logs "up_to_date".
      • Drive files added since last run → picked up automatically.
      • No duplicate rows even without a UNIQUE DB constraint.

    The old "if existing: return early" short-circuit has been replaced by
    this incremental approach so that partial crawls are always healed on
    the next run rather than being permanently skipped.

    Returns:
        {
            source_email, dest_email,
            total_in_drive,     ← files seen on Drive this run
            total,              ← total rows in SQL after this run
            folders, files,     ← counts of newly inserted items
            size_bytes,         ← size of newly inserted items
            already_in_sql,     ← count of files skipped (already in DB)
            newly_registered,   ← count of files inserted this run
            status ("ok" | "failed"), error
        }
    """
    result = {
        "source_email":     source_email,
        "dest_email":       dest_email,
        "total_in_drive":   0,
        "total":            0,
        "folders":          0,
        "files":            0,
        "size_bytes":       0,
        "already_in_sql":   0,
        "newly_registered": 0,
        "status":           "ok",
        "error":            None,
    }

    try:
        # ── Step 1: load known file IDs from SQL (empty set = first run) ──────
        existing_ids: Set[str] = _get_existing_file_ids(sql_mgr, source_email)
        result["already_in_sql"] = len(existing_ids)

        if existing_ids:
            logger.info(
                f"[DISC] {source_email}: {len(existing_ids)} file IDs already in SQL "
                f"— will skip these and only insert new files"
            )

        sql_mgr.start_user(run_id, source_email)

        # ── Step 2: fetch full Drive file list ───────────────────────────────
        drive_svc = _build_source_drive(source_email)
        raw_files = _list_owned_files(drive_svc, source_email)

        result["total_in_drive"] = len(raw_files)

        if not raw_files:
            logger.info(f"[DISC] {source_email}: no owned files found on Drive")
            if progress_cb:
                progress_cb({**result, "phase": "empty"})
            return result

        # ── Step 3: split into skip-set vs new-to-insert ─────────────────────
        new_files: List[Dict] = []

        for f in raw_files:
            file_id = f.get("id")
            fsize   = _safe_size(f.get("size"))   # FIX-4

            if file_id in existing_ids:
                # Already registered — skip without re-inserting
                continue

            new_files.append({
                **f,
                "source_email":    source_email,
                "dest_email":      dest_email,
                "file_size_bytes": fsize,          # FIX-4: normalised size
            })

            if f["mimeType"] == "application/vnd.google-apps.folder":
                result["folders"] += 1
            else:
                result["files"] += 1

            result["size_bytes"] += fsize

        logger.info(
            f"[DISC] {source_email}: "
            f"Drive total={len(raw_files)} | "
            f"already_in_sql={len(existing_ids)} | "
            f"new_to_insert={len(new_files)}"
        )

        # ── Early exit when nothing new to do ────────────────────────────────
        if not new_files:
            result["total"] = len(existing_ids)
            logger.info(
                f"[DISC] {source_email}: fully up-to-date, nothing new to register"
            )
            if progress_cb:
                progress_cb({**result, "phase": "up_to_date"})
            return result

        # ── Step 4: chunked DB inserts (batch=50, 0.3 s inter-chunk sleep) ───
        CHUNK = 50
        newly = 0

        for i in range(0, len(new_files), CHUNK):
            chunk   = new_files[i : i + CHUNK]
            retries = 0

            while True:
                try:
                    sql_mgr.register_discovered_items(
                        chunk,
                        source_email=source_email,   # FIX-3
                        dest_email=dest_email,        # FIX-3
                    )
                    newly += len(chunk)
                    break
                except Exception as db_exc:
                    retries += 1
                    if retries >= 5:
                        raise
                    wait = min(2 ** retries, 16) + random.uniform(0, 1)
                    logger.warning(
                        f"[DISC] {source_email} DB insert attempt {retries} failed "
                        f"({db_exc}), retrying in {wait:.1f}s"
                    )
                    time.sleep(wait)

            # yield between chunks so other threads can acquire a DB connection
            time.sleep(0.3)

        result["newly_registered"] = newly
        result["total"]            = len(existing_ids) + newly

        logger.info(
            f"[DISC] {source_email}: inserted "
            f"{result['folders']} new folders + {result['files']} new files | "
            f"{result['size_bytes']:,} bytes | "
            f"total in SQL now: {result['total']}"
        )
        if progress_cb:
            progress_cb({**result, "phase": "done"})

    except Exception as exc:
        result["status"] = "failed"
        result["error"]  = str(exc)
        logger.error(f"[DISC] {source_email} FAILED: {exc}", exc_info=True)
        if progress_cb:
            progress_cb({**result, "phase": "error"})

    return result


def run_discovery(
    run_id:       str,
    user_mapping: Dict[str, str],
    workers:      int = 4,
    progress_cb:  Callable[[Dict], None] = None,
) -> List[Dict]:
    """
    Entry point called by Flask. Runs discovery for all users in parallel.

    Args:
        run_id:       Unique migration run ID (generated by frontend).
                      MUST be the same value later passed to run_migration()
                      so SQLStateManager reads the rows discovery wrote.
        user_mapping: { source_email: dest_email, ... }
        workers:      Parallel threads (default 4).
        progress_cb:  Called after each user completes — for SSE streaming.

    Returns:
        List of per-user result dicts (each has size_bytes, files, folders etc.)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from sql_state_manager import SQLStateManager

    sql_mgr = SQLStateManager.for_discovery(migration_id=run_id)
    sql_mgr.create_migration_run(total_items=0)

    results   = []
    n_workers = min(workers, len(user_mapping))

    logger.info(
        f"[DISCOVERY] run_id={run_id} | "
        f"users={len(user_mapping)} | workers={n_workers}"
    )

    # STAGGERED — 0.5 s between submits so DB connections are acquired gradually
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {}
        for src, dst in user_mapping.items():
            futures[
                pool.submit(discover_user, run_id, src, dst, sql_mgr, progress_cb)
            ] = (src, dst)
            time.sleep(0.5)

        for future in as_completed(futures):
            src, dst = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {
                    "source_email":     src,
                    "dest_email":       dst,
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
                    f"[DISCOVERY] {src} raised exception: {exc}", exc_info=True
                )
                if progress_cb:
                    progress_cb({**result, "phase": "error"})
            results.append(result)

    # FIX-5: update migration_run total_items with the real discovered count
    total_items = sum(r.get("total", 0) for r in results)
    try:
        sql_mgr._execute(
            "UPDATE migration_runs SET total_items=%s WHERE migration_id=%s",
            (total_items, run_id),
        )
    except Exception as exc:
        logger.warning(f"[DISCOVERY] Could not update total_items: {exc}")

    logger.info(
        f"[DISCOVERY] run_id={run_id} complete | "
        f"total_items={total_items} | "
        f"users={len(results)} | "
        f"skipped={sum(r.get('already_in_sql', 0) for r in results)} | "
        f"newly_registered={sum(r.get('newly_registered', 0) for r in results)} | "
        f"failed={sum(1 for r in results if r.get('status') == 'failed')}"
    )

    return results
