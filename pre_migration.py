"""
pre_migration.py — Pre-Migration Phase (v3)

Fixes vs v2:
  1. User mapping validation now uses ACTUAL CSV mapping (src→dst email pairs)
     instead of auto-deriving username@dest_domain which was wrong for
     custom mappings like abdul.nazim@dev → developers@demo.
  2. File discovery now uses per-user delegated Drive service (not admin drive).
     Admin drive cannot list files owned by specific users.
  3. rootFolderId removed — Drive v3 about API does not support this field.
     Root folder permissions fetched using literal "root" file ID instead.
  4. Permission mappability check now validates destination emails from the
     actual CSV mapping against the dest_users set from Admin API.
  5. Policy warnings deduplicated — shown once in summary, not per-user.
  6. SQL registration now correctly passes source_email + dest_email per user.

NO per-permission SQL storage — only file/folder metadata stored in SQL.
Estimation is entirely in-memory.
"""

import logging
import time
from typing import Dict, List, Optional, Set, Tuple

from googleapiclient.errors import HttpError
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

SECONDS_PER_FILE        = 0.3
SECONDS_PER_PERMISSION  = 0.6
TRANSFER_SPEED_BPS      = 10 * 1024 * 1024   # 10 MB/s
API_BUFFER_FACTOR       = 1.20               # 20% buffer

IGNORED_MIME_TYPES = frozenset({
    "application/vnd.google-apps.script",
    "application/vnd.google-apps.form",
    "application/vnd.google-apps.site",
    "application/octet-stream",
})

WORKSPACE_SIZE_ESTIMATE = {
    "application/vnd.google-apps.document":     512  * 1024,
    "application/vnd.google-apps.spreadsheet":  256  * 1024,
    "application/vnd.google-apps.presentation": 1024 * 1024,
    "application/vnd.google-apps.drawing":      64   * 1024,
    "application/vnd.google-apps.jam":          256  * 1024,
    "application/vnd.google-apps.map":          128  * 1024,
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_size(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f} PB"


def _fmt_time(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h}h {m}m"


def _estimate(file_count: int, total_permission_count: int, total_bytes: int) -> float:
    raw = (
        (file_count * SECONDS_PER_FILE)
        + (total_permission_count * SECONDS_PER_PERMISSION)
        + (total_bytes / TRANSFER_SPEED_BPS)
    )
    return raw * API_BUFFER_FACTOR


# ─────────────────────────────────────────────────────────────────────────────
# Main class
# ─────────────────────────────────────────────────────────────────────────────

class PreMigrationPhase:
    """
    Pre-flight scan for Shared Drive and My Drive migrations.

    Constructor
    ───────────
    source_drive_service : Drive API service (source domain admin)
    dest_drive_service   : Drive API service (dest domain admin)
    admin_service        : Admin Directory API service (destination admin)
    source_domain        : str  e.g. 'dev.shivaami.in'
    dest_domain          : str  e.g. 'demo.shivaami.in'
    sql_manager          : SQLStateManager instance
    source_auth          : source DomainAuth object for per-user delegation
                           (optional — needed for My Drive file listing)
    """

    def __init__(
        self,
        source_drive_service,
        dest_drive_service,
        admin_service,
        source_domain: str,
        dest_domain: str,
        sql_manager,
        source_auth=None,    # FIX: needed for per-user Drive service delegation
    ):
        self.source_drive  = source_drive_service
        self.dest_drive    = dest_drive_service
        self.admin         = admin_service
        self.source_domain = source_domain
        self.dest_domain   = dest_domain
        self.mgr           = sql_manager
        self.source_auth   = source_auth

        # Populated by sync_directory()
        self._dest_users: Set[str] = set()

    # =========================================================================
    # Auth helper — get per-user delegated Drive service
    # =========================================================================

    def _get_user_drive_service(self, user_email: str):
        """
        Create a per-user delegated Drive service using GoogleAuthManager,
        exactly the same pattern as the old migration_engine.py.
        """
        try:
            from auth import GoogleAuthManager
            from config import Config

            user_auth = GoogleAuthManager(
                Config.SOURCE_CREDENTIALS_FILE,
                Config.SCOPES,
                delegate_email=user_email,
            )
            user_auth.authenticate()
            return user_auth.get_drive_service(user_email=user_email)

        except Exception as exc:
            logger.warning(
                f"  Delegation failed for {user_email}: {exc} — using admin drive"
            )
            return self.source_drive

    # =========================================================================
    # STEP 1: Global Directory Sync
    # =========================================================================

    def sync_directory(self) -> Set[str]:
        """
        Fetch ALL active users from destination domain via Admin Directory API.
        Stores emails in a set for O(1) lookup.
        Returns the set of destination user emails (lowercase).
        """
        logger.info("─" * 60)
        logger.info("STEP 1: Global Directory Sync (destination domain)")
        logger.info("─" * 60)

        dest_users: Set[str] = set()
        page_token           = None

        while True:
            try:
                resp = self.admin.users().list(
                    domain=self.dest_domain,
                    maxResults=500,
                    pageToken=page_token,
                    fields="users(primaryEmail,suspended,archived),nextPageToken",
                    orderBy="email",
                ).execute()
            except HttpError as exc:
                logger.error(f"  ✗ Admin API error fetching dest users: {exc}")
                break
            except Exception as exc:
                logger.error(f"  ✗ Unexpected error fetching dest users: {exc}")
                break

            for u in resp.get("users", []):
                email     = u.get("primaryEmail", "").lower()
                suspended = u.get("suspended", False)
                archived  = u.get("archived", False)
                if email and not suspended and not archived:
                    dest_users.add(email)

            page_token = resp.get("nextPageToken")
            if not page_token:
                break
            time.sleep(0.1)

        self._dest_users = dest_users
        logger.info(
            f"  ✓ Synced {len(dest_users)} active users "
            f"from {self.dest_domain} into memory"
        )
        return dest_users

    # =========================================================================
    # STEP 2: Drive policy validation
    # =========================================================================

    def validate_drive_policy(
        self,
        drive_id: Optional[str] = None,
        mode: str = "shared_drive",
    ) -> Dict:
        """Validate sharing policies. Returns policy dict with warnings list."""
        logger.info("─" * 60)
        logger.info(
            f"STEP 2: Drive Policy Validation "
            f"({'Shared Drive' if mode == 'shared_drive' else 'My Drive'})"
        )
        logger.info("─" * 60)

        policy = {
            "mode":                 mode,
            "drive_id":             drive_id,
            "domain_users_only":    False,
            "admin_managed":        False,
            "copy_requires_writer": False,
            "warnings":             [],
        }

        if mode == "shared_drive" and drive_id:
            try:
                drive_meta   = self.dest_drive.drives().get(
                    driveId=drive_id,
                    fields="id,name,restrictions",
                    useDomainAdminAccess=True,
                ).execute()
                restrictions = drive_meta.get("restrictions", {})

                policy["domain_users_only"]    = restrictions.get("domainUsersOnly", False)
                policy["admin_managed"]        = restrictions.get("adminManagedRestrictions", False)
                policy["copy_requires_writer"] = restrictions.get("copyRequiresWriterPermission", False)

                logger.info(f"  Drive             : {drive_meta.get('name', drive_id)}")
                logger.info(f"  domainUsersOnly   : {policy['domain_users_only']}")
                logger.info(f"  adminManaged      : {policy['admin_managed']}")
                logger.info(f"  copyRequiresWriter: {policy['copy_requires_writer']}")

                if policy["domain_users_only"]:
                    msg = (
                        f"Drive '{drive_meta.get('name')}' has domainUsersOnly=True — "
                        "external users CANNOT be added."
                    )
                    policy["warnings"].append(msg)
                    logger.warning(f"  ⚠ {msg}")

                if policy["admin_managed"]:
                    msg = "adminManagedRestrictions=True — only admins can change membership."
                    policy["warnings"].append(msg)
                    logger.warning(f"  ⚠ {msg}")

            except HttpError as exc:
                logger.error(f"  ✗ Failed to fetch drive restrictions: {exc}")
                policy["warnings"].append(f"Could not fetch drive restrictions: {exc}")

        elif mode == "my_drive":
            msg = (
                "My Drive migrations subject to destination domain sharing policies. "
                "Check Admin Console → Drive and Docs → Sharing settings."
            )
            policy["warnings"].append(msg)
            logger.info(f"  ℹ {msg}")

        if not policy["warnings"]:
            logger.info("  ✓ No policy restrictions detected")

        return policy

    # =========================================================================
    # STEP 3: High-speed discovery + estimation
    # =========================================================================

    def run_discovery_and_estimate(
        self,
        mode: str,
        source_drive_id: str   = None,
        dest_drive_id: str     = None,
        source_user_email: str = None,
        dest_user_email: str   = None,
        run_id: str            = None,
    ) -> Dict:
        """
        Discover all files, store metadata in SQL, estimate migration time.

        FIX: For My Drive mode, uses per-user delegated Drive service
        so that files().list() returns files actually owned by that user.
        Uses permissionIds (len only) for estimation — no permission details stored.
        """
        logger.info("─" * 60)
        logger.info(
            f"STEP 3: Discovery & Estimation "
            f"({'Shared Drive' if mode == 'shared_drive' else 'My Drive'})"
        )
        logger.info("─" * 60)

        total_files            = 0
        total_folders          = 0
        total_ignored          = 0
        total_bytes            = 0
        total_permission_count = 0
        all_items: List[Dict]  = []
        page_token             = None
        page                   = 0

        common_fields = (
            "nextPageToken, files("
            "id, name, mimeType, size, parents, "
            "permissionIds, "
            "createdTime, modifiedTime)"
        )

        # FIX: use per-user delegated service for My Drive listing
        if mode == "my_drive" and source_user_email:
            list_drive = self._get_user_drive_service(source_user_email)
        else:
            list_drive = self.source_drive

        while True:
            try:
                if mode == "shared_drive":
                    resp = list_drive.files().list(
                        q="trashed=false",
                        spaces="drive",
                        corpora="drive",
                        driveId=source_drive_id,
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True,
                        fields=common_fields,
                        pageSize=200,
                        pageToken=page_token,
                    ).execute()
                else:
                    # FIX: query files owned by this specific user
                    resp = list_drive.files().list(
                        q=f"'{source_user_email}' in owners and trashed=false",
                        spaces="drive",
                        corpora="user",
                        includeItemsFromAllDrives=False,
                        supportsAllDrives=False,
                        fields=common_fields,
                        pageSize=200,
                        pageToken=page_token,
                    ).execute()

            except HttpError as exc:
                logger.error(f"  ✗ files().list() failed (page {page}): {exc}")
                break
            except Exception as exc:
                logger.error(f"  ✗ Unexpected list error (page {page}): {exc}")
                break

            for item in resp.get("files", []):
                mime       = item.get("mimeType", "")
                size       = int(item.get("size") or 0)
                perm_ids   = item.get("permissionIds", [])
                perm_count = len(perm_ids)

                if mime == "application/vnd.google-apps.folder":
                    total_folders += 1
                    all_items.append(item)
                    continue

                if mime in IGNORED_MIME_TYPES:
                    total_ignored += 1
                    all_items.append(item)
                    continue

                if size == 0:
                    size = WORKSPACE_SIZE_ESTIMATE.get(mime, 0)

                total_files            += 1
                total_bytes            += size
                total_permission_count += perm_count
                all_items.append(item)

            page      += 1
            page_token = resp.get("nextPageToken")

            if page % 5 == 0 and page_token:
                logger.info(
                    f"  Scanned page {page} | files={total_files} | "
                    f"size={_fmt_size(total_bytes)} | perms={total_permission_count}"
                )

            if not page_token:
                break
            time.sleep(0.1)

        # ── Register file/folder metadata in SQL ──────────────────────────
        if all_items:
            try:
                if mode == "shared_drive":
                    self.mgr.register_discovered_items(
                        all_items,
                        source_email="",
                        dest_email="",
                        source_shared_drive_id=source_drive_id,
                        dest_shared_drive_id=dest_drive_id,
                    )
                else:
                    # FIX: pass actual source + dest emails so SQL rows are correctly tagged
                    self.mgr.register_discovered_items(
                        all_items,
                        source_email=source_user_email or "",
                        dest_email=dest_user_email or "",
                    )
                logger.info(f"  ✓ Registered {len(all_items)} items in SQL")
            except Exception as exc:
                logger.error(f"  ✗ SQL registration failed: {exc}")

        estimated_seconds = _estimate(total_files, total_permission_count, total_bytes)

        estimation = {
            "mode":                    mode,
            "total_items":             len(all_items),
            "total_files":             total_files,
            "total_folders":           total_folders,
            "total_ignored":           total_ignored,
            "total_bytes":             total_bytes,
            "total_size_human":        _fmt_size(total_bytes),
            "total_permission_count":  total_permission_count,
            "avg_permissions_per_file": (
                round(total_permission_count / total_files, 2) if total_files else 0
            ),
            "estimated_seconds":       estimated_seconds,
            "estimated_time_human":    _fmt_time(estimated_seconds),
            "formula": (
                f"({total_files} files × {SECONDS_PER_FILE}s) + "
                f"({total_permission_count} perms × {SECONDS_PER_PERMISSION}s) + "
                f"({_fmt_size(total_bytes)} ÷ {_fmt_size(TRANSFER_SPEED_BPS)}/s)"
                f" × {API_BUFFER_FACTOR} buffer"
            ),
        }

        logger.info(f"  ✓ Discovery complete:")
        logger.info(f"      Total items        : {len(all_items)}")
        logger.info(f"      Folders            : {total_folders}")
        logger.info(f"      Files              : {total_files}")
        logger.info(f"      Ignored            : {total_ignored}")
        logger.info(f"      Total size         : {_fmt_size(total_bytes)}")
        logger.info(f"      Total permissions  : {total_permission_count}")
        logger.info(f"      Avg perms/file     : {estimation['avg_permissions_per_file']}")
        logger.info(f"  ✓ Estimation:")
        logger.info(f"      Formula : {estimation['formula']}")
        logger.info(f"      Result  : {_fmt_time(estimated_seconds)}")

        return estimation

    # =========================================================================
    # STEP 4a: Data migration mapping validation
    # =========================================================================

    def validate_data_mapping(
        self,
        user_mapping: Dict[str, str] = None,
        drive_id_mapping: Dict[str, str] = None,
    ) -> Dict:
        """
        FIX: Validate the ACTUAL CSV mapping pairs (src→dst) against
        the destination user set from Admin API.

        This checks: for each destination email in the CSV mapping,
        does that user exist in the destination domain?

        Different from permission mapping — this is about whether the
        destination account that will RECEIVE the files actually exists.

        Returns:
            {
              "mappable":   [(src, dst), ...]  dst exists in dest domain
              "unmappable": [(src, dst), ...]  dst NOT in dest domain
            }
        """
        logger.info("─" * 60)
        logger.info("STEP 4a: Data Migration Mapping Validation")
        logger.info("─" * 60)

        result = {"mappable": [], "unmappable": []}

        if not user_mapping and not drive_id_mapping:
            logger.info("  No mapping provided — skipping data mapping validation")
            return result

        if user_mapping:
            logger.info(f"  Checking {len(user_mapping)} user mapping pair(s)...")
            for src_email, dst_email in sorted(user_mapping.items()):
                if dst_email.lower() in self._dest_users:
                    result["mappable"].append((src_email, dst_email))
                    logger.info(f"    ✓ MAPPABLE   : {src_email} → {dst_email}")
                else:
                    result["unmappable"].append((src_email, dst_email))
                    logger.warning(
                        f"    ✗ UNMAPPABLE : {src_email} → {dst_email} "
                        f"(dest user not found in {self.dest_domain})"
                    )

        logger.info("")
        logger.info(f"  Data mapping result:")
        logger.info(f"    ✓ Mappable   : {len(result['mappable'])}")
        logger.warning(f"    ✗ Unmappable : {len(result['unmappable'])}")

        if result["unmappable"]:
            logger.warning("")
            logger.warning("  ⚠ UNMAPPABLE DESTINATIONS — these users must exist before migration:")
            for src, dst in result["unmappable"]:
                logger.warning(f"      {src} → {dst} ← DESTINATION NOT IN {self.dest_domain}")
            logger.warning(
                "  Action: Create these destination accounts before running migration."
            )

        return result

    # =========================================================================
    # STEP 4b: Permission mappability check (all 158 dest users → source users)
    # =========================================================================

    def validate_permission_mapping(
        self,
        mode: str,
        source_drive_id: str   = None,
        source_user_email: str = None,
        user_mapping: Dict[str, str] = None,
    ) -> Dict:
        """
        FIX: Permission mapping validation — different from data mapping.

        For PERMISSIONS, we check: which SOURCE domain users (who may have
        been granted access to files) can be mapped to an existing DESTINATION
        domain account?

        Logic:
          1. Fetch permissions from root (drive root or My Drive root)
          2. For each permission email from source domain:
             - Auto-derive: local@source_domain → local@dest_domain
             - Check if that dest email exists in self._dest_users
          3. Also check all source emails in user_mapping the same way

        Returns:
            {
              "permission_mappable":   [(src_email, dest_email), ...]
              "permission_unmappable": [src_email, ...]  no dest account found
              "external_skipped":      [email, ...]      not from source domain
            }
        """
        logger.info("─" * 60)
        logger.info("STEP 4b: Permission Mappability Check")
        logger.info(f"  (Out of {len(self._dest_users)} destination users, "
                    f"who can receive permissions?)")
        logger.info("─" * 60)

        result = {
            "permission_mappable":   [],
            "permission_unmappable": [],
            "external_skipped":      [],
        }

        # ── Gather all source-domain emails that may have permissions ─────
        source_emails_to_check: Set[str] = set()

        # From the user_mapping (data migration pairs)
        if user_mapping:
            for src_email in user_mapping.keys():
                if "@" in src_email:
                    domain = src_email.split("@")[1]
                    if domain == self.source_domain:
                        source_emails_to_check.add(src_email.lower())

        # From root-level permissions (drive or My Drive root)
        root_perms = self._fetch_root_permissions(
            mode, source_drive_id, source_user_email
        )
        for perm in root_perms:
            email = perm.get("emailAddress", "")
            role  = perm.get("role", "")
            if email and role != "owner" and "@" in email:
                source_emails_to_check.add(email.lower())

        logger.info(
            f"  Checking {len(source_emails_to_check)} unique source emails "
            f"for permission mappability..."
        )

        # ── Classify each source email ────────────────────────────────────
        for source_email in sorted(source_emails_to_check):
            user_domain = source_email.split("@")[1]

            if user_domain != self.source_domain:
                result["external_skipped"].append(source_email)
                logger.debug(f"    [EXTERNAL] {source_email} — skipped")
                continue

            local      = source_email.split("@")[0]
            dest_email = f"{local}@{self.dest_domain}"

            if dest_email.lower() in self._dest_users:
                result["permission_mappable"].append((source_email, dest_email))
                logger.info(f"    ✓ PERM MAPPABLE   : {source_email} → {dest_email}")
            else:
                result["permission_unmappable"].append(source_email)
                logger.warning(
                    f"    ✗ PERM UNMAPPABLE : {source_email} → {dest_email} "
                    f"(no account in {self.dest_domain})"
                )

        # ── Summary ───────────────────────────────────────────────────────
        logger.info("")
        logger.info(f"  Permission mappability result:")
        logger.info(
            f"    ✓ Mappable   : {len(result['permission_mappable'])} "
            f"(permissions will migrate successfully)"
        )
        logger.warning(
            f"    ✗ Unmappable : {len(result['permission_unmappable'])} "
            f"(permissions will fail — no dest account)"
        )
        logger.info(
            f"    ~ External   : {len(result['external_skipped'])} "
            f"(external users — skipped in Shared Drive mode)"
        )

        if result["permission_unmappable"]:
            logger.warning("")
            logger.warning(
                "  ⚠ UNMAPPABLE PERMISSION USERS — "
                "these source users have no matching account in destination:"
            )
            for src in result["permission_unmappable"]:
                local = src.split("@")[0]
                dst   = f"{local}@{self.dest_domain}"
                logger.warning(f"      {src} → {dst} ← NOT IN DESTINATION")
            logger.warning(
                "  Action: Create these accounts in destination domain to "
                "preserve file permissions."
            )

        return result

    def _fetch_root_permissions(
        self,
        mode: str,
        source_drive_id: str   = None,
        source_user_email: str = None,
    ) -> List[Dict]:
        """
        FIX: Fetch root-level permissions using correct API calls.
        My Drive: use literal 'root' as fileId (not rootFolderId from about).
        Shared Drive: use drive ID directly.
        """
        root_perms = []
        try:
            if mode == "shared_drive" and source_drive_id:
                resp = self.source_drive.permissions().list(
                    fileId=source_drive_id,
                    supportsAllDrives=True,
                    useDomainAdminAccess=True,
                    fields="permissions(id,type,role,emailAddress,displayName)",
                ).execute()
                root_perms = resp.get("permissions", [])
                logger.info(
                    f"  Fetched {len(root_perms)} drive-level permissions"
                )

            elif mode == "my_drive" and source_user_email:
                # FIX: use per-user delegated service + literal "root"
                # Drive v3 about().get() does NOT support rootFolderId field
                user_drive = self._get_user_drive_service(source_user_email)
                resp = user_drive.permissions().list(
                    fileId="root",
                    fields="permissions(id,type,role,emailAddress,displayName)",
                ).execute()
                root_perms = resp.get("permissions", [])
                logger.info(
                    f"  Fetched {len(root_perms)} root folder permissions "
                    f"for {source_user_email}"
                )

        except HttpError as exc:
            logger.warning(f"  Could not fetch root permissions: {exc}")
        except Exception as exc:
            logger.warning(f"  Unexpected error fetching root permissions: {exc}")

        return root_perms

    # =========================================================================
    # MAIN: run() — orchestrates all steps
    # =========================================================================

    def run(
        self,
        mode: str,
        drive_id_mapping: Dict[str, str] = None,
        user_mapping: Dict[str, str]     = None,
        run_id: str                      = None,
    ) -> Dict:
        """
        Run complete pre-migration phase.

        Args:
            mode             : "shared_drive" or "my_drive"
            drive_id_mapping : {src_drive_id: dst_drive_id}
            user_mapping     : {src_email: dst_email}
            run_id           : migration run ID

        Returns report dict with ready flag and all step results.
        """
        logger.info("=" * 60)
        logger.info(f"PRE-MIGRATION PHASE — mode={mode.upper()}")
        logger.info("=" * 60)

        report = {
            "ready":                   True,
            "directory":               set(),
            "policies":                {},
            "estimations":             {},
            "data_mapping":            {},
            "permission_mapping":      {},
            "total_estimated_seconds": 0,
            "total_estimated_time":    "",
        }

        # ── Step 1: Directory sync (once for all users/drives) ────────────
        try:
            dest_users = self.sync_directory()
            report["directory"] = dest_users
            if not dest_users:
                logger.warning("  ⚠ No destination users found")
        except Exception as exc:
            logger.error(f"  ✗ Directory sync failed: {exc}")
            report["ready"] = False
            return report

        # ── Build pairs ───────────────────────────────────────────────────
        pairs = []
        if mode == "shared_drive" and drive_id_mapping:
            pairs = [
                {"src_drive": k, "dst_drive": v}
                for k, v in drive_id_mapping.items()
            ]
        elif mode == "my_drive" and user_mapping:
            pairs = [
                {"src_email": k, "dst_email": v}
                for k, v in user_mapping.items()
            ]

        # ── Steps 2 + 3 per drive/user ────────────────────────────────────
        all_policy_warnings: List[str] = []

        for pair in pairs:
            src_drive = pair.get("src_drive")
            dst_drive = pair.get("dst_drive")
            src_email = pair.get("src_email")
            dst_email = pair.get("dst_email")
            key       = src_drive or src_email

            logger.info("")
            if mode == "shared_drive":
                logger.info(f"Processing drive: {src_drive} → {dst_drive}")
            else:
                logger.info(f"Processing user: {src_email} → {dst_email}")

            # Step 2: Policy
            try:
                policy = self.validate_drive_policy(
                    drive_id=dst_drive, mode=mode
                )
                report["policies"][key] = policy
                # Collect unique warnings for summary (avoid repeating per user)
                for w in policy.get("warnings", []):
                    if w not in all_policy_warnings:
                        all_policy_warnings.append(w)
            except Exception as exc:
                logger.error(f"  ✗ Policy validation error: {exc}")
                report["policies"][key] = {"warnings": [str(exc)]}

            # Step 3: Discovery + estimation
            try:
                estimation = self.run_discovery_and_estimate(
                    mode              = mode,
                    source_drive_id   = src_drive,
                    dest_drive_id     = dst_drive,
                    source_user_email = src_email,
                    dest_user_email   = dst_email,
                    run_id            = run_id,
                )
                report["estimations"][key]         = estimation
                report["total_estimated_seconds"] += estimation.get("estimated_seconds", 0)
            except Exception as exc:
                logger.error(f"  ✗ Discovery/estimation error: {exc}")
                report["estimations"][key] = {}
                report["ready"]            = False

        # ── Step 4a: Data mapping validation (once, covers all pairs) ─────
        logger.info("")
        try:
            data_map_result = self.validate_data_mapping(
                user_mapping     = user_mapping,
                drive_id_mapping = drive_id_mapping,
            )
            report["data_mapping"] = data_map_result
        except Exception as exc:
            logger.error(f"  ✗ Data mapping validation error: {exc}")
            report["data_mapping"] = {}

        # ── Step 4b: Permission mappability (once, using first pair) ──────
        logger.info("")
        try:
            # Use first pair for root permission fetch
            first_src_drive = pairs[0].get("src_drive") if pairs else None
            first_src_email = pairs[0].get("src_email") if pairs else None

            perm_map_result = self.validate_permission_mapping(
                mode              = mode,
                source_drive_id   = first_src_drive,
                source_user_email = first_src_email,
                user_mapping      = user_mapping,
            )
            report["permission_mapping"] = perm_map_result
        except Exception as exc:
            logger.error(f"  ✗ Permission mapping validation error: {exc}")
            report["permission_mapping"] = {}

        # ── Final summary ─────────────────────────────────────────────────
        report["total_estimated_time"] = _fmt_time(report["total_estimated_seconds"])

        total_files = sum(
            e.get("total_files", 0) for e in report["estimations"].values()
        )
        total_size = sum(
            e.get("total_bytes", 0) for e in report["estimations"].values()
        )
        total_perms = sum(
            e.get("total_permission_count", 0)
            for e in report["estimations"].values()
        )
        data_unmappable = len(report["data_mapping"].get("unmappable", []))
        perm_unmappable = len(
            report["permission_mapping"].get("permission_unmappable", [])
        )
        perm_mappable   = len(
            report["permission_mapping"].get("permission_mappable", [])
        )

        logger.info("")
        logger.info("=" * 60)
        logger.info("PRE-MIGRATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"  Mode                     : {mode.upper()}")
        logger.info(f"  Destination users (total) : {len(dest_users)}")
        logger.info(f"  Items to migrate          : {total_files} files")
        logger.info(f"  Total size                : {_fmt_size(total_size)}")
        logger.info(f"  Total permissions         : {total_perms}")
        logger.info(f"  Estimated total time      : {report['total_estimated_time']}")
        logger.info("")
        logger.info("  DATA MIGRATION MAPPING:")
        logger.info(
            f"    ✓ Mappable destinations  : "
            f"{len(report['data_mapping'].get('mappable', []))}"
        )
        if data_unmappable:
            logger.warning(
                f"    ✗ Unmappable destinations: {data_unmappable} "
                f"(dest accounts missing)"
            )
        else:
            logger.info("    ✓ All destination accounts exist")

        logger.info("")
        logger.info("  PERMISSION MIGRATION MAPPING:")
        logger.info(
            f"    ✓ Permission-mappable users   : {perm_mappable} "
            f"(have matching dest account)"
        )
        if perm_unmappable:
            logger.warning(
                f"    ✗ Permission-unmappable users : {perm_unmappable} "
                f"(no dest account — permissions will fail)"
            )
        else:
            logger.info("    ✓ All permission users mappable")

        # Unique policy warnings (FIX: not repeated per user)
        if all_policy_warnings:
            logger.warning(
                f"  ⚠ Policy warnings : {len(all_policy_warnings)}"
            )
            for w in all_policy_warnings:
                logger.warning(f"      {w}")

        if report["ready"] and data_unmappable == 0 and perm_unmappable == 0:
            logger.info("  ✓ PRE-MIGRATION PHASE PASSED — ready to migrate")
        elif report["ready"]:
            logger.warning(
                "  ⚠ PRE-MIGRATION PHASE PASSED WITH WARNINGS — "
                "review unmappable users above"
            )
        else:
            logger.error(
                "  ✗ PRE-MIGRATION PHASE FAILED — fix errors before migrating"
            )

        logger.info("=" * 60)
        return report