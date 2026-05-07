"""
discovery_engine.py  —  ~/amey/discovery_engine.py

Crawls Google Drive for every user in the provided mapping and registers
all owned folders + files into SQL via sql_state_manager.

Designed to be imported by Flask routes — NOT run standalone.

FIXES vs previous version
──────────────────────────
FIX-1  size_bytes now accumulated and returned in every result dict.
       discovery_routes._accumulate() reads result["size_bytes"] to build
       the totals shown on the frontend. Previously this key was never set
       so total_size_bytes was always 0.

FIX-2  already_in_sql path now sums file_size from existing SQL records and
       returns size_bytes correctly instead of 0.

FIX-3  bulk_register_items() receives items with source_email / dest_email
       already set on each item dict (was already done) — confirmed correct.
       Added explicit source_email / dest_email pass-through to
       register_discovered_items() so My Drive schema constraint is satisfied
       (source_user_email NOT NULL → must be real email, not '').

FIX-4  _list_owned_files() now requests file_size_bytes-compatible field
       'size' AND falls back gracefully for files that have no size
       (Google Workspace files return no size — treated as 0).

FIX-5  run_discovery() updates migration_run total_items after all users
       are discovered so migration_runs.total_items reflects the real count.
"""

import time
import random
import logging
from typing import Dict, List, Callable

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


def _list_owned_files(drive_service, user_email: str) -> List[Dict]:
    """
    List all non-trashed files owned by user_email in their My Drive.

    FIX-4: 'size' field requested explicitly. Google Workspace files
    (Docs, Sheets, Slides) do not have a size field — we default to 0.
    Binary files return size as a string — we cast to int safely.
    """
    files, page_token, retries = [], None, 0

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
                pageSize=PAGE_SIZE,
                pageToken=page_token,
                supportsAllDrives=False,
                includeItemsFromAllDrives=False,
            ).execute()

            owned = [
                f for f in resp.get("files", [])
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
                logger.error(
                    f"[{user_email}] Failed after {MAX_RETRIES} retries: {exc}"
                )
                return files
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

    progress_cb is called after each phase so Flask can stream SSE updates.

    Returns:
test 
        {
            source_email, dest_email,
            total, folders, files,
            size_bytes,                  ← FIX-1: always present
            already_in_sql, newly_registered,
            status ("ok" | "failed"), error
        }
    """
    result = {
        "source_email":     source_email,
        "dest_email":       dest_email,
        "total":            0,
        "folders":          0,
        "files":            0,
        "size_bytes":       0,           # FIX-1: initialised to 0, always set
        "already_in_sql":   0,
        "newly_registered": 0,
        "status":           "ok",
        "error":            None,
    }

    try:
        existing = sql_mgr.load_user_items(source_email)
        if existing:
            folders    = 0
            size_bytes = 0
            for r in existing:
                if getattr(r, "mime_type", "") == "application/vnd.google-apps.folder":
                    folders += 1
                # FIX-2: sum sizes from existing SQL records
                size_bytes += int(getattr(r, "file_size_bytes", 0) or
                                  getattr(r, "file_size",       0) or 0)

            result.update({
                "already_in_sql": len(existing),
                "total":          len(existing),
                "folders":        folders,
                "files":          len(existing) - folders,
                "size_bytes":     size_bytes,     # FIX-2: was always 0
            })
            logger.info(
                f"[DISC] {source_email}: already in SQL "
                f"({result['folders']} folders, {result['files']} files, "
                f"{result['size_bytes']:,} bytes) — skipping"
            )
            if progress_cb:
                progress_cb({**result, "phase": "skipped"})
            return result

        sql_mgr.start_user(run_id, source_email)
        drive_svc = _build_source_drive(source_email)
        raw_files = _list_owned_files(drive_svc, source_email)

        if not raw_files:
            logger.info(f"[DISC] {source_email}: no owned files found")
            if progress_cb:
                progress_cb({**result, "phase": "empty"})
            return result

        annotated  = []
        size_total = 0

        for f in raw_files:
            fsize = _safe_size(f.get("size"))   # FIX-4: safe int cast
            size_total += fsize
            annotated.append({
                **f,
                "source_email":   source_email,
                "dest_email":     dest_email,
                # Normalise size so register_discovered_items() always has it
                "file_size_bytes": fsize,
            })
            if f["mimeType"] == "application/vnd.google-apps.folder":
                result["folders"] += 1
            else:
                result["files"] += 1

        # FIX-3 + THROTTLE: chunk inserts into batches of 50 with a small
        # inter-batch sleep so the MySQL connection pool isn't hammered by
        # all worker threads inserting large payloads simultaneously.
        CHUNK = 50
        newly = 0
        for i in range(0, len(annotated), CHUNK):
            chunk = annotated[i : i + CHUNK]
            retries = 0
            while True:
                try:
                    sql_mgr.register_discovered_items(
                        chunk,
                        source_email=source_email,
                        dest_email=dest_email,
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
            time.sleep(0.3)   # yield between chunks so other threads can get a connection

        result["total"]            = len(annotated)
        result["newly_registered"] = newly
        result["size_bytes"]       = size_total    # FIX-1: set total size

        logger.info(
            f"[DISC] {source_email}: registered "
            f"{result['folders']} folders + {result['files']} files | "
            f"{result['size_bytes']:,} bytes"
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

    # Use for_discovery(migration_id=run_id) so every row written to SQL
    # carries migration_id=run_id. The old pattern
    #   SQLStateManager(Config.get_db_connection)
    # auto-generated a random UUID, causing migration Phase 1 to find zero
    # SQL items and fall back to a live Drive re-crawl.
    sql_mgr = SQLStateManager.for_discovery(migration_id=run_id)
    sql_mgr.create_migration_run(total_items=0)

    results   = []
    n_workers = min(workers, len(user_mapping))

    logger.info(
        f"[DISCOVERY] run_id={run_id} | "
        f"users={len(user_mapping)} | workers={n_workers}"
    )

    # STAGGERED — 0.5 s between each submit so DB connections are acquired
    # gradually instead of all at once
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
                    "total":            0,
                    "folders":          0,
                    "files":            0,
                    "size_bytes":       0,
                    "already_in_sql":   0,
                    "newly_registered": 0,
                    "status":           "failed",
                    "error":            str(exc),
                }
                logger.error(f"[DISCOVERY] {src} raised exception: {exc}", exc_info=True)
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
        f"failed={sum(1 for r in results if r.get('status') == 'failed')}"
    )

    return results
