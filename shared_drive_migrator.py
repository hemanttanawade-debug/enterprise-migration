"""
shared_drive_migrator.py  (v2 – Cloud SQL + GCS architecture)

PATCH CHANGES vs v1:
  1. Removed old CSV CheckpointManager import — uses SQLStateManager
  2. File transfer calls mgr._migrate_file_v3() (patched engine method)
     instead of old _migrate_file_with_enhanced_retry()
  3. migrate_drive_members() now routes through EnhancedPermissionsMigrator
     with migration_id + get_conn for SQL tracking
  4. register_discovered_items() passes real drive IDs via source_shared_drive_id /
     dest_shared_drive_id instead of fake "shared_drive:xxx" email strings
  5. Folder creation marks in_progress / done / failed in SQL
  6. Per-file permission migration after successful upload (hybrid model)
  7. GCS temp cleanup after each drive completes
  8. ThreadPoolExecutor for parallel file migration per drive
  9. All checkpoint calls use (run_id, file_id) signature
 10. No behaviour changes to drive listing, folder hierarchy sort, or drive creation
"""

import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

from googleapiclient.errors import HttpError
from permissions_migrator import EnhancedPermissionsMigrator

logger = logging.getLogger(__name__)


class SharedDriveMigrator:
    """
    Handles Shared Drive → Shared Drive migration.

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
    parallel_files      : int — concurrent file transfers per drive (default 5)
    """

    def __init__(
        self,
        source_admin_drive,
        dest_admin_drive,
        source_domain: str,
        dest_domain: str,
        config,
        sql_mgr,           # SQLStateManager — replaces old CheckpointManager
        run_id,
        parallel_files: int = 5,
    ):
        self.source_drive  = source_admin_drive
        self.dest_drive    = dest_admin_drive
        self.source_domain = source_domain
        self.dest_domain   = dest_domain
        self.config        = config
        self.mgr           = sql_mgr        # checkpoint + GCS combined
        self.run_id        = run_id
        self.parallel_files = parallel_files

        self.stats = {
            "drives_total":    0,
            "drives_created":  0,
            "drives_failed":   0,
            "files_migrated":  0,
            "files_failed":    0,
            "files_skipped":   0,
            "files_ignored":   0,
            "folders_created": 0,
            "members_migrated": 0,
            "members_failed":   0,
        }

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
    # STEP 4: Migrate drive-level members via EnhancedPermissionsMigrator
    # PATCH: was inline permissions().create() loop — now uses the patched
    #        permissions migrator so results are tracked in migration_permissions
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

        # PATCH: route through EnhancedPermissionsMigrator for SQL tracking
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

            # Write each member result to shared_drive_members table
            for detail in pr.get("details", []):
                role       = detail.get("role", "")
                ptype      = detail.get("type", "user")
                status     = detail.get("status", "failed")
                error      = detail.get("error", "")
                dest_email = detail.get("target_email") or detail.get("email", "")

                # Skip owner and invalid roles
                if role == "owner" or not dest_email:
                    continue

                # Validate against DB enum
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
    # STEP 5: Folder structure builder
    # PATCH: marks folders in SQL checkpoint (in_progress / done / failed)
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
        PATCH: checkpoint calls now use run_id; no migration_engine dependency.
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

            # PATCH: mark in progress in SQL
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
                # PATCH: register mapping + mark done in SQL
                self.mgr.register_folder_mapping(self.run_id, fid, dest_fid)
                self.mgr.mark_done(
                    self.run_id, fid,
                    dest_item_id=dest_fid,
                    dest_parent_id=dest_parent,
                )
                self.stats["folders_created"] += 1
                logger.debug(f"  ✓ Folder: {fname}")

                # Migrate folder-level permissions
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
                    # Try to find the existing one
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
    # STEP 6: Migrate files in a Shared Drive
    # PATCH: file transfer now calls mgr._migrate_file_v3 (GCS/memory routing)
    #        instead of old _migrate_file_with_enhanced_retry
    # =========================================================================

    def migrate_shared_drive_files(
        self,
        source_drive_id: str,
        dest_drive_id: str,
        drive_name: str,
    ) -> Dict:
        result = {
            "drive_name":     drive_name,
            "files_migrated": 0,
            "files_failed":   0,
            "files_skipped":  0,
            "files_ignored":  0,
            "folders_created": 0,
        }

        # List all items
        all_items = self.list_shared_drive_files(source_drive_id)
        if not all_items:
            logger.info(f"No files in drive: {drive_name}")
            return result

        # PATCH: register with real drive IDs (not fake email strings)
        self.mgr.register_discovered_items(
            all_items,
            source_email="",              # empty — Shared Drive has no user email
            dest_email="",
            source_shared_drive_id=source_drive_id,
            dest_shared_drive_id=dest_drive_id,
        )

        folders = [f for f in all_items
                   if f["mimeType"] == "application/vnd.google-apps.folder"]
        files   = [f for f in all_items
                   if f["mimeType"] != "application/vnd.google-apps.folder"]

        logger.info(f"  '{drive_name}': {len(folders)} folders, {len(files)} files")

        # Build folder structure (SQL-aware, no engine dependency)
        folder_mapping = self._build_shared_drive_folder_structure(
            folders, source_drive_id, dest_drive_id
        )
        result["folders_created"] = len(folder_mapping)

        # Migrate files in parallel
        with ThreadPoolExecutor(max_workers=self.parallel_files) as ex:
            futures = {}
            for file_info in files:
                fid      = file_info["id"]
                fname    = file_info["name"]
                mime     = file_info["mimeType"]
                fsize    = int(file_info.get("size") or 0)
                pids     = file_info.get("parents", [])
                dest_par = folder_mapping.get(pids[0], dest_drive_id) if pids else dest_drive_id

                fut = ex.submit(
                    self._migrate_one_file,
                    fid, fname, mime, fsize, dest_par, dest_drive_id,
                )
                futures[fut] = file_info

            for fut in futures:
                file_info = futures[fut]
                fid   = file_info["id"]
                fname = file_info["name"]
                mime  = file_info["mimeType"]
                try:
                    r = fut.result()
                    if r["skipped"]:
                        result["files_skipped"] += 1
                    elif r["ignored"]:
                        result["files_ignored"] += 1
                    elif r["success"]:
                        result["files_migrated"] += 1
                        self.stats["files_migrated"] += 1
                        # PATCH: file-level permissions after successful upload
                        if r.get("dest_id"):
                            self._migrate_item_permissions(
                                fid, r["dest_id"], fname,
                                "FILE", dest_drive_id,
                            )
                    else:
                        result["files_failed"] += 1
                        self.stats["files_failed"] += 1
                except Exception as exc:
                    logger.error(f"  Future error [{fname}]: {exc}")
                    result["files_failed"] += 1

        return result

    def _migrate_one_file(
        self,
        file_id: str,   
        file_name: str,
        mime_type: str,
        file_size: int,
        dest_parent_id: str,
        dest_drive_id: str,
    ) -> Dict:
        empty = {"success": False, "dest_id": None,
                "skipped": False, "ignored": False, "error": None}

        # Resume / skip check
        should_skip, reason = self.mgr.should_skip_item(file_id)
        if should_skip:
            return {**empty, "skipped": True}

        self.mgr.mark_in_progress(self.run_id, file_id)

        # Ignored MIME types
        from sql_state_manager import IGNORED_MIME_TYPES, GOOGLE_WORKSPACE_EXPORT
        # Assuming these are defined in a module like sql_state_manager or similar
        from sql_state_manager import IGNORED_MIME_TYPES, GOOGLE_WORKSPACE_EXPORT # Make sure this module and constants exist
        if mime_type in IGNORED_MIME_TYPES:
            self.mgr.mark_ignored(self.run_id, file_id, "Non-migratable MIME type")
            return {**empty, "ignored": True}

        import io
        from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

        max_retries = 3
        last_error  = ""

        for attempt in range(max_retries):
            try:
                # Determine if export needed (Google Workspace files)
                from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
                export_info = GOOGLE_WORKSPACE_EXPORT.get(mime_type)

                if export_info:
                    export_mime, ext, import_mime = export_info
                    request = self.source_drive.files().export_media(
                        fileId=file_id, mimeType=export_mime
                    )
                    dest_name        = file_name + ext
                    upload_mime      = export_mime
                    dest_import_mime = import_mime
                else:
                    request = self.source_drive.files().get_media(
                        fileId=file_id, supportsAllDrives=True
                    )
                    dest_name        = file_name
                    upload_mime      = mime_type
                    dest_import_mime = None

                # Download into memory
                buf  = io.BytesIO()
                dl   = MediaIoBaseDownload(buf, request, chunksize=20 * 1024 * 1024)
                done = False
                while not done:
                    _, done = dl.next_chunk()
                buf.seek(0)

                # Build metadata for destination
                meta = {
                    "name":    dest_name,
                    "parents": [dest_parent_id],
                }
                if dest_import_mime:
                    meta["mimeType"] = dest_import_mime

                # Upload to destination
                media = MediaIoBaseUpload(
                    buf,
                    mimetype=upload_mime,
                    resumable=True,
                    chunksize=20 * 1024 * 1024,
                )
                f = self.dest_drive.files().create(
                    body=meta,
                    media_body=media,
                    fields="id",
                    supportsAllDrives=True,
                ).execute()

                dest_id = f["id"]
                self.mgr.mark_done(
                    self.run_id, file_id,
                    dest_item_id=dest_id,
                    dest_parent_id=dest_parent_id,
                )
                logger.info(f"  ✓ Migrated [{file_name}]")
                return {**empty, "success": True, "dest_id": dest_id}

            except Exception as exc:
                last_error = str(exc)
                if attempt < max_retries - 1:
                    wait = 2 ** attempt   # 1s, 2s, 4s
                    logger.warning(
                        f"  Retry {attempt + 1}/{max_retries} [{file_name}]: "
                        f"{last_error} — retrying in {wait}s"
                    )
                    time.sleep(wait)
                else:
                    logger.error(
                        f"  File transfer failed after {max_retries} attempts "
                        f"[{file_name}]: {last_error}"
                    )

        self.mgr.mark_failed(self.run_id, file_id, last_error)
        return {**empty, "error": last_error}

    # =========================================================================
    # STEP 7: File/Folder permissions (hybrid model)
    # PATCH: always fetch fresh from API, apply via API, track in SQL
    # =========================================================================

    def _migrate_item_permissions(
        self,
        source_id: str,
        dest_id: str,
        name: str,
        item_type: str,            # "FILE" or "FOLDER"
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
                return   # Only owner/organizer — nothing to migrate
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

            # Write each permission result to migration_permissions table
            for detail in pr.get("details", []):
                role           = detail.get("role", "")
                ptype          = detail.get("type", "user")
                status         = detail.get("status", "failed")
                classification = detail.get("classification", "external_domain")
                error          = detail.get("error", "")
                source_email   = detail.get("original_email", "")
                dest_email     = detail.get("target_email", "")

                # Skip owner
                if role == "owner":
                    continue

                # organizer is drive-level only — skip for file/folder permissions
                if item_type in ("FILE", "FOLDER") and role == "organizer":
                    logger.debug(
                        f"  Skipping 'organizer' role for [{name}] "
                        "— only valid at drive level"
                    )
                    continue

                # Validate enums against DB schema
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
    # MAIN: Migrate all (or filtered) Shared Drives
    # =========================================================================

    def migrate_all_shared_drives(
        self,
        drive_filter: List[str] = None,
        drive_id_mapping: Dict[str, str] = None,
    ) -> Dict:
        """
        Main entry point for Shared Drive migration.
    
        Args:
            drive_filter     : optional list of drive names to migrate (None = all).
                            Ignored when drive_id_mapping is provided.
            drive_id_mapping : optional {source_drive_id: dest_drive_id} from CSV.
                            When provided, migrates exactly these drive pairs
                            without listing source drives first.
        """
        summary = {
            "total_drives":           0,
            "drives_migrated":        0,
            "drives_failed":          0,
            "total_files_migrated":   0,
            "total_files_failed":     0,
            "total_files_ignored":    0,
            "total_folders_created":  0,
            "total_members_migrated": 0,
            "drive_results":          [],
        }
    
        # ── Branch 1: ID-based mapping from CSV ──────────────────────────────────
        if drive_id_mapping:
            import logging
            logger = logging.getLogger(__name__)
    
            logger.info(
                f"ID-based Shared Drive migration: "
                f"{len(drive_id_mapping)} drive pair(s) from CSV"
            )
            self.stats["drives_total"] = len(drive_id_mapping)
            summary["total_drives"]    = len(drive_id_mapping)
    
            for src_id, dst_id in drive_id_mapping.items():
                drive_result = {
                    "name":             f"drive:{src_id}",   # name resolved below
                    "source_id":        src_id,
                    "dest_id":          dst_id,
                    "status":           "failed",
                    "files_migrated":   0,
                    "files_failed":     0,
                    "files_ignored":    0,
                    "folders_created":  0,
                    "members_migrated": 0,
                }
    
                try:
                    # Resolve source drive name for logging
                    try:
                        drv_meta = self.source_drive.drives().get(
                            driveId=src_id, fields="name"
                        ).execute()
                        drive_name = drv_meta.get("name", src_id)
                        drive_result["name"] = drive_name
                    except Exception:
                        drive_name = src_id
    
                    logger.info("=" * 60)
                    logger.info(f"Migrating (ID-mapped): {drive_name}")
                    logger.info(f"  src={src_id}  dst={dst_id}")
                    logger.info("=" * 60)
    
                    self.mgr.upsert_shared_drive(self.run_id, src_id, drive_name)
    
                    # Ensure dest drive exists (create if it was pre-created externally)
                    # If dst_id is a known existing drive we just use it; if it
                    # doesn't exist yet we create one with the source drive name.
                    dest_check_id = self._verify_or_create_dest_drive_by_id(
                        dst_id, drive_name, src_id
                    )
                    if not dest_check_id:
                        logger.error(
                            f"Cannot access or create dest drive {dst_id} — skipping"
                        )
                        summary["drives_failed"] += 1
                        summary["drive_results"].append(drive_result)
                        continue
                    dst_id = dest_check_id  # may be same or newly created
                    drive_result["dest_id"] = dst_id
    
                    # 1. Drive-level members
                    member_result = self.migrate_drive_members(src_id, dst_id, drive_name)
                    drive_result["members_migrated"]   = member_result["migrated"]
                    summary["total_members_migrated"] += member_result["migrated"]
                    self.stats["members_migrated"]    += member_result["migrated"]
    
                    # 2. Files + folder structure + file permissions
                    file_result = self.migrate_shared_drive_files(src_id, dst_id, drive_name)
    
                    drive_result["files_migrated"]  = file_result["files_migrated"]
                    drive_result["files_failed"]    = file_result["files_failed"]
                    drive_result["files_ignored"]   = file_result["files_ignored"]
                    drive_result["folders_created"] = file_result["folders_created"]
                    drive_result["status"]          = "completed"
    
                    summary["drives_migrated"]       += 1
                    summary["total_files_migrated"]  += file_result["files_migrated"]
                    summary["total_files_failed"]    += file_result["files_failed"]
                    summary["total_files_ignored"]   += file_result["files_ignored"]
                    summary["total_folders_created"] += file_result["folders_created"]
    
                    self.mgr.update_run_counters(
                        completed=file_result["files_migrated"],
                        failed=file_result["files_failed"],
                        ignored=file_result["files_ignored"],
                    )
                    self.mgr.finish_shared_drive(
                        self.run_id, src_id, "completed",
                        files_total=(
                            file_result["files_migrated"] + file_result["files_failed"]
                        ),
                        files_done=file_result["files_migrated"],
                    )
    
                    logger.info(
                        f"✓ '{drive_name}': "
                        f"{file_result['files_migrated']} migrated, "
                        f"{file_result['files_failed']} failed, "
                        f"{file_result['folders_created']} folders"
                    )
    
                except Exception as exc:
                    import logging as _log
                    _log.getLogger(__name__).error(
                        f"Drive {src_id} failed: {exc}", exc_info=True
                    )
                    summary["drives_failed"] += 1
                    self.stats["drives_failed"] += 1
                    try:
                        self.mgr.finish_shared_drive(self.run_id, src_id, "failed")
                    except Exception:
                        pass
    
                summary["drive_results"].append(drive_result)
    
            return summary
        import logging
        logger = logging.getLogger(__name__)
    
        source_drives = self.list_source_shared_drives()
        self.stats["drives_total"] = len(source_drives)
        summary["total_drives"]    = len(source_drives)
    
        if drive_filter:
            source_drives = [d for d in source_drives if d["name"] in drive_filter]
            logger.info(f"Filtered to {len(source_drives)} drives by name")
    
        for drive in source_drives:
            src_id     = drive["id"]
            drive_name = drive["name"]
    
            logger.info("=" * 60)
            logger.info(f"Migrating Shared Drive: {drive_name} ({src_id})")
            logger.info("=" * 60)
    
            drive_result = {
                "name":             drive_name,
                "source_id":        src_id,
                "dest_id":          None,
                "status":           "failed",
                "files_migrated":   0,
                "files_failed":     0,
                "files_ignored":    0,
                "folders_created":  0,
                "members_migrated": 0,
            }
    
            try:
                dest_id = self.create_dest_shared_drive(drive_name, src_id)
                if not dest_id:
                    logger.error(f"Could not create/find dest drive: {drive_name}")
                    summary["drives_failed"] += 1
                    summary["drive_results"].append(drive_result)
                    continue
    
                drive_result["dest_id"] = dest_id
                self.mgr.upsert_shared_drive(self.run_id, src_id, drive_name)
    
                member_result = self.migrate_drive_members(src_id, dest_id, drive_name)
                drive_result["members_migrated"]   = member_result["migrated"]
                summary["total_members_migrated"] += member_result["migrated"]
                self.stats["members_migrated"]    += member_result["migrated"]
    
                file_result = self.migrate_shared_drive_files(src_id, dest_id, drive_name)
    
                drive_result["files_migrated"]  = file_result["files_migrated"]
                drive_result["files_failed"]    = file_result["files_failed"]
                drive_result["files_ignored"]   = file_result["files_ignored"]
                drive_result["folders_created"] = file_result["folders_created"]
                drive_result["status"]          = "completed"
    
                summary["drives_migrated"]       += 1
                summary["total_files_migrated"]  += file_result["files_migrated"]
                summary["total_files_failed"]    += file_result["files_failed"]
                summary["total_files_ignored"]   += file_result["files_ignored"]
                summary["total_folders_created"] += file_result["folders_created"]
    
                self.mgr.update_run_counters(
                    completed=file_result["files_migrated"],
                    failed=file_result["files_failed"],
                    ignored=file_result["files_ignored"],
                )
                self.mgr.finish_shared_drive(
                    self.run_id, src_id, "completed",
                    files_total=(
                        file_result["files_migrated"] + file_result["files_failed"]
                    ),
                    files_done=file_result["files_migrated"],
                )
    
                logger.info(
                    f"✓ '{drive_name}': "
                    f"{file_result['files_migrated']} migrated, "
                    f"{file_result['files_failed']} failed, "
                    f"{file_result['folders_created']} folders"
                )
    
            except Exception as exc:
                logger.error(f"Drive '{drive_name}' failed: {exc}", exc_info=True)
                summary["drives_failed"] += 1
                self.stats["drives_failed"] += 1
                try:
                    self.mgr.finish_shared_drive(self.run_id, src_id, "failed")
                except Exception:
                    pass
    
            summary["drive_results"].append(drive_result)
    
        return summary
    def _verify_or_create_dest_drive_by_id(
        self,
        dest_drive_id: str,
        drive_name: str,
        source_drive_id: str,
    ) -> Optional[str]:
        """
        Verify a dest drive ID is accessible on the destination domain.
        If accessible, return it as-is.
        If not found (404), create a new drive with drive_name and return the new ID.
        Returns None on unrecoverable error.
        """
        import logging
        logger = logging.getLogger(__name__)
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
 
 
 
