"""
shared_drive_migrator.py  (v3 – Global Queue + GCS/Memory routing)

PATCH CHANGES vs v2:
  1. TWO-PHASE ARCHITECTURE (mirrors migration_engine.py v3):
       Phase 1 — Discovery: list files, build folder structure, register items in SQL.
                 Runs per-drive in parallel (DISCOVERY_WORKERS drives at once).
       Phase 2 — Global Queue: flat ThreadPoolExecutor drains ALL pending files
                 from SQL across all drives simultaneously (GLOBAL_WORKERS).
                 Previously each drive ran its own sequential/per-drive pool,
                 meaning idle workers while one large drive blocked the rest.

  2. GCS / MEMORY ROUTING (mirrors migration_engine.py):
       _migrate_one_file_v3() replaces the old _migrate_one_file():
         - file_size < 50 MB  → _migrate_via_memory()   (BytesIO, direct upload)
         - file_size >= 50 MB → _migrate_via_gcs()       (stream through GCS bucket)
       Both paths use mgr.download_drive_to_gcs() / mgr.upload_gcs_to_drive() for
       the GCS path — same helpers migration_engine.py uses.

  3. PERMISSION LOGIC UNCHANGED:
       _migrate_item_permissions() — not touched.
       migrate_drive_members()     — not touched.
       migrate_all_shared_drives() — structure preserved; phase wiring added.

  4. FOLDER BUILDING UNCHANGED:
       _build_shared_drive_folder_structure() — not touched.
       _create_folder_in_shared_drive()       — not touched.
       _sort_folders_by_hierarchy()           — not touched.

  5. SQL INTERFACE UNCHANGED:
       All mgr.* calls identical to v2 — no new methods required from
       sql_state_manager.py or main.py.

CONSTRUCTOR SIGNATURE: unchanged from v2.
    SharedDriveMigrator(source_admin_drive, dest_admin_drive,
                        source_domain, dest_domain, config,
                        sql_mgr, run_id, parallel_files)
"""

import io
import logging
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Tunable constants  (mirrors migration_engine.py values)
# ─────────────────────────────────────────────────────────────────────────────

LARGE_FILE_THRESHOLD_BYTES = 50 * 1_024 * 1_024   # 50 MB → GCS path
GLOBAL_WORKERS    = 14    # I/O-bound workers for Phase 2 file queue
DISCOVERY_WORKERS = 4     # parallel drives during Phase 1 discovery
MAX_RETRIES       = 5
MAX_BACKOFF_S     = 32
CHUNK_SIZE        = 32 * 1_024 * 1_024   # 32 MB per chunk

from sql_state_manager import IGNORED_MIME_TYPES, GOOGLE_WORKSPACE_EXPORT


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


# ─────────────────────────────────────────────────────────────────────────────
# SharedDriveMigrator
# ─────────────────────────────────────────────────────────────────────────────

class SharedDriveMigrator:
    """
    Handles Shared Drive → Shared Drive migration.

    Two-phase architecture (v3):
      Phase 1 — per-drive discovery + folder creation (parallel drives).
      Phase 2 — global file queue drained by a flat thread pool.

    Key differences from My Drive:
      - Ownership lives at drive level, not per-file
      - All API calls need supportsAllDrives=True
      - Members are drive-level permissions
      - File permissions are additive overrides on top of drive membership

    Constructor
    ───────────
    source_admin_drive  : Drive service authenticated as source domain admin
    dest_admin_drive    : Drive service authenticated as dest domain admin
    source_domain       : str  e.g. 'dev.shivaami.in'
    dest_domain         : str  e.g. 'demo.shivaami.in'
    config              : Config object
    sql_mgr             : SQLStateManager instance (checkpoint + GCS helper)
    run_id              : int / str — current migration_runs.migration_id
    parallel_files      : int — ignored in v3 (GLOBAL_WORKERS used instead),
                          kept for constructor compatibility
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
        parallel_files: int = 5,   # compat — superseded by GLOBAL_WORKERS
    ):
        self.source_drive   = source_admin_drive
        self.dest_drive     = dest_admin_drive
        self.source_domain  = source_domain
        self.dest_domain    = dest_domain
        self.config         = config
        self.mgr            = sql_mgr
        self.run_id         = run_id
        self.parallel_files = parallel_files   # kept for compat

        self.stats = {
            "drives_total":     0,
            "drives_created":   0,
            "drives_failed":    0,
            "files_migrated":   0,
            "files_failed":     0,
            "files_skipped":    0,
            "files_ignored":    0,
            "folders_created":  0,
            "members_migrated": 0,
            "members_failed":   0,
            "gcs_routed":       0,
            "memory_routed":    0,
        }

        # {source_drive_id: {source_folder_id: dest_folder_id}}
        self._folder_maps: Dict[str, Dict[str, str]] = {}

    # =========================================================================
    # STEP 1: List all Shared Drives in source domain
    # =========================================================================

    def list_source_shared_drives(self) -> List[Dict]:
        drives     = []
        page_token = None

        logger.info("Listing all Shared Drives in source domain...")

        while True:
            try:
                resp = self.source_drive.drives().list(
                    pageSize=100,
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

                time.sleep(0.3)

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
                        "createdTime, modifiedTime)"
                    ),
                    pageSize=200,
                    pageToken=page_token,
                ).execute()

                batch = resp.get("files", [])
                files.extend(batch)

                page_token = resp.get("nextPageToken")
                if not page_token:
                    break

                time.sleep(0.2)

            except HttpError as exc:
                logger.error(f"Failed to list files in drive {drive_id}: {exc}")
                raise

        logger.info(f"  {len(files)} items in drive {drive_id}")
        return files

    # =========================================================================
    # STEP 4: Migrate drive-level members — UNCHANGED from v2
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
            permissions = resp.get("permissions", [])
            logger.info(f"  {len(permissions)} members in '{drive_name}'")

        except Exception as exc:
            logger.error(f"Failed to list members for '{drive_name}': {exc}")
            return result

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
        """Map source-domain email to dest domain; keep external as-is."""
        if source_email.endswith(f"@{self.source_domain}"):
            local = source_email.split("@")[0]
            return f"{local}@{self.dest_domain}"
        return source_email

    # =========================================================================
    # STEP 5: Folder structure builder — UNCHANGED from v2
    # =========================================================================

    def _build_shared_drive_folder_structure(
        self,
        folders: List[Dict],
        source_drive_id: str,
        dest_drive_id: str,
    ) -> Dict[str, str]:
        """
        Build folder hierarchy inside destination Shared Drive.
        Returns {source_folder_id: dest_folder_id}.
        """
        folder_mapping: Dict[str, str] = {}
        sorted_folders = self._sort_folders_by_hierarchy(folders)

        for folder in sorted_folders:
            fid   = folder["id"]
            fname = folder["name"]
            pids  = folder.get("parents", [])

            # Resume: if already created, reuse
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

            # Resolve destination parent
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

                self._migrate_item_permissions(fid, dest_fid, fname, "FOLDER", dest_drive_id)
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

    def _find_existing_folder(
        self, name: str, parent_id: str
    ) -> Optional[str]:
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
        """Topological sort — parents always before children."""
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
    # STEP 6: Item permissions — UNCHANGED from v2
    # =========================================================================

    def _migrate_item_permissions(
        self,
        source_id: str,
        dest_id: str,
        name: str,
        item_type: str,
        parent_drive_id: str,
    ):
        """
        Hybrid model:
        1. Fetch permissions from source API (always fresh)
        2. Apply to destination via EnhancedPermissionsMigrator
        3. Track each result in migration_permissions SQL table
        """
        try:
            resp = self.source_drive.permissions().list(
                fileId=source_id,
                fields="permissions(id,type,role,emailAddress,domain,displayName)",
                supportsAllDrives=True,
            ).execute()
            perms = resp.get("permissions", [])
            if len(perms) <= 1:
                return
        except Exception as exc:
            logger.warning(f"  Permissions list failed [{name}]: {exc}")
            return

        try:
            from permissions_migrator import EnhancedPermissionsMigrator

            pm = EnhancedPermissionsMigrator(
                self.source_drive,
                self.dest_drive,
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

                if item_type in ("FILE", "FOLDER") and role == "organizer":
                    logger.debug(
                        f"  Skipping 'organizer' role for [{name}] "
                        "— only valid at drive level"
                    )
                    continue

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
        """
        Phase 1 worker (one per Shared Drive).
        - Lists all items from source drive.
        - Registers them in SQL (INSERT IGNORE — safe for resume).
        - Builds folder hierarchy on destination.
        - Stores folder_map in self._folder_maps[src_id].

        Returns a summary dict used for Phase 2 aggregation.
        """
        result = {
            "source_id":      src_id,
            "dest_id":        dst_id,
            "name":           drive_name,
            "files_total":    0,
            "folders_created": 0,
            "status":         "ok",
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
    # Phase 2: per-item file migration (memory OR GCS path)
    # =========================================================================

    def _process_queue_item(self, item) -> Dict:
        """
        Phase 2 worker.  `item` is a MigrationRecord from sql_state_manager.

        Routing:
          - ignored MIME          → mark_ignored, return
          - already done/skipped  → return skipped
          - size < 50 MB          → _migrate_via_memory()
          - size >= 50 MB         → _migrate_via_gcs()
        After success, calls _migrate_item_permissions().
        """
        file_id      = item.file_id
        file_name    = getattr(item, "file_name",        "") or ""
        mime_type    = getattr(item, "mime_type",         "") or ""
        file_size    = int(getattr(item, "file_size_bytes", 0) or 0)
        parent_id    = getattr(item, "source_parent_id",  None)
        src_drive_id = getattr(item, "source_shared_drive_id", "") or ""

        base = {
            "success": False, "ignored": False, "skipped": False,
            "source_drive_id": src_drive_id, "file_name": file_name,
        }

        # Ignored MIME types
        if mime_type in IGNORED_MIME_TYPES:
            self.mgr.mark_ignored(self.run_id, file_id, "Non-migratable MIME type")
            return {**base, "ignored": True}

        # Resume guard
        should_skip, _ = self.mgr.should_skip_item(file_id)
        if should_skip:
            return {**base, "skipped": True}

        # Resolve destination parent from per-drive folder map
        fm          = self._folder_maps.get(src_drive_id, {})
        dst_drive_id = getattr(item, "dest_shared_drive_id", "") or ""
        dest_parent  = fm.get(parent_id, dst_drive_id) if parent_id else dst_drive_id

        self.mgr.mark_in_progress(self.run_id, file_id)

        # Route by size
        if file_size >= LARGE_FILE_THRESHOLD_BYTES:
            res = self._migrate_via_gcs(
                file_id, file_name, mime_type, file_size, dest_parent
            )
        else:
            res = self._migrate_via_memory(
                file_id, file_name, mime_type, file_size, dest_parent
            )

        if res["success"]:
            dest_id = res.get("dest_id")
            self.mgr.mark_done(
                self.run_id, file_id,
                dest_item_id=dest_id,
                dest_parent_id=dest_parent,
            )
            if dest_id:
                self._migrate_item_permissions(
                    file_id, dest_id, file_name, "FILE", dst_drive_id
                )
            return {**base, "success": True, "dest_id": dest_id}

        elif res.get("ignored"):
            self.mgr.mark_ignored(self.run_id, file_id, res.get("error", ""))
            return {**base, "ignored": True}
        else:
            err = res.get("error", "Unknown")
            self.mgr.mark_failed(self.run_id, file_id, err)
            return {**base, "error": err}

    # ─────────────────────────────────────────────────────────────────────────
    # Memory path  (< 50 MB)
    # ─────────────────────────────────────────────────────────────────────────

    def _migrate_via_memory(
        self,
        file_id: str,
        file_name: str,
        mime_type: str,
        file_size: int,
        dest_parent_id: str,
    ) -> Dict:
        """
        Download into BytesIO, upload directly to destination.
        Google Workspace files are exported first (same logic as migration_engine.py).
        """
        empty      = {"success": False, "dest_id": None, "ignored": False, "error": None}
        last_error = ""

        export_info      = GOOGLE_WORKSPACE_EXPORT.get(mime_type)
        is_workspace     = export_info is not None

        # Non-exportable workspace types (folder etc.)
        if mime_type == "application/vnd.google-apps.folder":
            return {**empty, "ignored": True, "error": "Folder in file queue"}

        for attempt in range(MAX_RETRIES):
            wait   = _backoff(attempt)
            dl_buf = None
            try:
                # ── Download ─────────────────────────────────────────────────
                if is_workspace:
                    export_mime, ext, import_mime = export_info
                    request   = self.source_drive.files().export_media(
                        fileId=file_id, mimeType=export_mime
                    )
                    dest_name = file_name + ext
                    up_mime   = export_mime
                    dest_mime = import_mime   # re-import as Workspace type if possible
                else:
                    request   = self.source_drive.files().get_media(
                        fileId=file_id, supportsAllDrives=True, acknowledgeAbuse=True
                    )
                    dest_name = file_name
                    up_mime   = mime_type
                    dest_mime = None

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
                        # Zero-byte file — create empty placeholder
                        meta = {"name": dest_name, "parents": [dest_parent_id]}
                        if dest_mime:
                            meta["mimeType"] = dest_mime
                        resp    = self.dest_drive.files().create(
                            body=meta, fields="id", supportsAllDrives=True
                        ).execute()
                        dest_id = resp.get("id")
                        self.stats["memory_routed"] += 1
                        return {**empty, "success": True, "dest_id": dest_id}
                    last_error = "Empty download for non-zero file"
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(wait)
                        continue
                    return {**empty, "error": last_error}

                # ── Upload ───────────────────────────────────────────────────
                meta = {"name": dest_name, "parents": [dest_parent_id]}
                if dest_mime:
                    meta["mimeType"] = dest_mime

                upload_buf = io.BytesIO(data)
                try:
                    use_resumable = len(data) >= 5 * 1_024 * 1_024
                    media = MediaIoBaseUpload(
                        upload_buf, mimetype=up_mime,
                        resumable=use_resumable,
                        chunksize=CHUNK_SIZE if use_resumable else -1,
                    )
                    resp = self.dest_drive.files().create(
                        body=meta, media_body=media,
                        fields="id", supportsAllDrives=True,
                    ).execute()
                finally:
                    upload_buf.close()

                dest_id = resp.get("id") if isinstance(resp, dict) else None
                if dest_id is None:
                    return {**empty, "error": f"Bad response: {resp!r}"}

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
                    return {**empty, "ignored": True, "error": "Download restricted"}

                if code in (429, 500, 503) and attempt < MAX_RETRIES - 1:
                    logger.warning(
                        f"  [MEM] HTTP {code} retry {attempt+1} [{file_name}]"
                        f" wait {wait:.1f}s"
                    )
                    time.sleep(wait)
                    continue

                logger.error(f"  [MEM] HTTP {code} [{file_name}]: {last_error}")
                return {**empty, "error": last_error}

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
                return {**empty, "error": last_error}

            finally:
                if dl_buf is not None:
                    try:
                        dl_buf.close()
                    except Exception:
                        pass

        return {**empty, "error": last_error}

    # ─────────────────────────────────────────────────────────────────────────
    # GCS path  (>= 50 MB)
    # ─────────────────────────────────────────────────────────────────────────

    def _migrate_via_gcs(
        self,
        file_id: str,
        file_name: str,
        mime_type: str,
        file_size: int,
        dest_parent_id: str,
    ) -> Dict:
        """
        Stream source → GCS → destination.
        Uses mgr.download_drive_to_gcs() and mgr.upload_gcs_to_drive() —
        the same helpers used by migration_engine.py.
        """
        empty       = {"success": False, "dest_id": None, "ignored": False, "error": None}
        last_error  = ""
        active_blob = None

        export_info = GOOGLE_WORKSPACE_EXPORT.get(mime_type)
        export_mime = export_info[0] if export_info else None
        import_mime = export_info[2] if export_info else None
        dest_name   = (file_name + export_info[1]) if export_info else file_name
        up_mime     = export_mime if export_info else mime_type

        for attempt in range(MAX_RETRIES):
            wait         = _backoff(attempt)
            attempt_blob = f"{self.run_id}/{file_id}/attempt_{attempt}"

            try:
                ok, blob_name, err = self.mgr.download_drive_to_gcs(
                    drive_svc  = self.source_drive,
                    file_id    = file_id,
                    file_name  = file_name,
                    run_id     = attempt_blob,
                    mime_type  = up_mime,
                    export_mime= export_mime,
                )

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
                    return {**empty, "error": last_error}

                active_blob = blob_name

                ok2, dest_id, err2 = self.mgr.upload_gcs_to_drive(
                    drive_svc  = self.dest_drive,
                    blob_name  = blob_name,
                    file_name  = dest_name,
                    mime_type  = up_mime,
                    parent_id  = dest_parent_id,
                    import_mime= import_mime,
                )

                if not ok2:
                    last_error = err2 or "GCS upload failed"
                    if active_blob is not None:
                        try:
                            self.mgr.delete_temp(active_blob)
                        except Exception:
                            pass
                        active_blob = None
                    if attempt < MAX_RETRIES - 1:
                        logger.warning(
                            f"  [GCS] Upload failed {attempt+1}/{MAX_RETRIES}"
                            f" [{file_name}]: {last_error} — retry {wait:.1f}s"
                        )
                        time.sleep(wait)
                        continue
                    return {**empty, "error": last_error}

                # Success — clean up staging blob
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

        return {**empty, "error": last_error}

    # =========================================================================
    # MAIN: Migrate all (or filtered) Shared Drives — two-phase
    # =========================================================================

    def migrate_all_shared_drives(
        self,
        drive_filter: List[str] = None,
        drive_id_mapping: Dict[str, str] = None,
    ) -> Dict:
        """
        Main entry point for Shared Drive migration.

        Phase 1 — Discovery (parallel per drive):
          • List files, register in SQL, build folder hierarchy.

        Phase 2 — Global queue (flat GLOBAL_WORKERS pool):
          • Drain all PENDING/FAILED files from SQL across every drive.
          • Route each file to memory (<50 MB) or GCS (>=50 MB).
          • Apply file-level permissions after each successful upload.

        Args:
            drive_filter     : optional list of drive names (None = all).
                               Ignored when drive_id_mapping is provided.
            drive_id_mapping : optional {source_drive_id: dest_drive_id} from CSV.
        """
        summary = {
            "total_drives":           0,
            "drives_migrated":        0,
            "drives_failed":          0,
            "total_files_migrated":   0,
            "total_files_failed":     0,
            "total_files_ignored":    0,
            "total_files_skipped":    0,
            "total_folders_created":  0,
            "total_members_migrated": 0,
            "drive_results":          [],
        }

        # ── Resolve the list of (src_id, dst_id, drive_name) triples ─────────
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

        # ── Step: migrate drive-level members for all drives (serial) ─────────
        # Done before Phase 1 so members exist before any file permissions land.
        member_results: Dict[str, Dict] = {}
        for src_id, dst_id, drive_name in drive_triples:
            self.mgr.upsert_shared_drive(self.run_id, src_id, drive_name)
            mr = self.migrate_drive_members(src_id, dst_id, drive_name)
            member_results[src_id] = mr
            self.stats["members_migrated"] += mr.get("migrated", 0)
            summary["total_members_migrated"] += mr.get("migrated", 0)

        # ── PHASE 1: Parallel discovery + folder creation ──────────────────────
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
                        "status": "discovery_failed",
                        "error":  str(exc),
                        "source_id": src_id,
                        "dest_id":   dst_id,
                        "name":      drive_name,
                    }

        # ── PHASE 2: Global file queue ─────────────────────────────────────────
        pending = self.mgr.get_all_pending_items(self.run_id)
        # Sort smallest-first so fast files don't queue behind large ones
        pending.sort(key=lambda r: int(getattr(r, "file_size_bytes", None) or 0))
        logger.info(
            f"[PHASE-2] Global queue — {len(pending)} files, "
            f"{GLOBAL_WORKERS} workers..."
        )

        # {file_id: result_dict}
        file_results: Dict[str, Dict] = {}

        with ThreadPoolExecutor(max_workers=GLOBAL_WORKERS) as pool:
            futures = {
                pool.submit(self._process_queue_item, item): item
                for item in pending
            }
            done = 0
            for future in as_completed(futures):
                item = futures[future]
                try:
                    res = future.result()
                    file_results[item.file_id] = res
                    done += 1
                    if done % 50 == 0:
                        logger.info(
                            f"[PHASE-2] Progress: {done}/{len(pending)}"
                        )
                except Exception as exc:
                    logger.error(
                        f"[PHASE-2] Future error [{item.file_name}]: {exc}",
                        exc_info=True,
                    )
                    file_results[item.file_id] = {
                        "success": False, "error": str(exc),
                        "source_drive_id": getattr(
                            item, "source_shared_drive_id", ""
                        ),
                    }

        # ── Aggregate results per drive ────────────────────────────────────────
        per_drive: Dict[str, Dict] = {
            src_id: {
                "name":            drive_name,
                "source_id":       src_id,
                "dest_id":         dst_id,
                "status":          "failed",
                "files_migrated":  0,
                "files_failed":    0,
                "files_skipped":   0,
                "files_ignored":   0,
                "folders_created": disc_results.get(src_id, {}).get("folders_created", 0),
                "members_migrated": member_results.get(src_id, {}).get("migrated", 0),
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

            icon = "✓" if agg["status"] == "completed" else "✗"
            logger.info(
                f"  {icon} {agg['name']} ({src_id}): "
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
            f"GCS={self.stats['gcs_routed']} MEM={self.stats['memory_routed']}"
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
        try:
            self.dest_drive.drives().get(driveId=dest_drive_id).execute()
            logger.debug(f"Dest drive {dest_drive_id} accessible")
            return dest_drive_id
        except Exception as exc:
            code = getattr(getattr(exc, "resp", None), "status", None)
            if code == 404:
                logger.info(
                    f"Dest drive {dest_drive_id} not found — "
                    f"creating '{drive_name}' in dest domain"
                )
                return self.create_dest_shared_drive(drive_name, source_drive_id)
            logger.error(f"Cannot access dest drive {dest_drive_id}: {exc}")
            return None