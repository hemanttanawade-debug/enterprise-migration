"""
User management module for fetching and mapping users
Patch v2: CSV mapping now supports optional shared drive ID columns
          so a single mapping file can define both user migrations
          and shared drive migrations.

Supported CSV formats
─────────────────────
Format A — users only (backward compatible):
    source,destination
    amey@dev.shivaami.in,htt@demo.shivaami.in

Format B — users + shared drives in one file:
    source,destination,source_drive_id,dest_drive_id
    amey@dev.shivaami.in,htt@demo.shivaami.in,,
    bob@dev.shivaami.in,rob@demo.shivaami.in,,
    ,,0ABCdriveID1,0XYZdriveID2
    ,,0ABCdriveID3,0XYZdriveID3

Rules:
  - If source + destination are present  → user mapping row
  - If source_drive_id + dest_drive_id are present → shared drive mapping row
  - A row may have ALL four columns filled (user AND a drive associated with them)
    but the two mappings are always returned separately
  - Blank cells or missing columns are tolerated silently
"""

import csv
import logging
from typing import Dict, List, NamedTuple, Optional, Tuple

from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Result container returned by import_mapping()
# ─────────────────────────────────────────────────────────────────────────────

class MappingResult(NamedTuple):
    """
    Returned by import_user_mapping() and import_mapping().

    user_mapping  : {source_email: dest_email}
    drive_mapping : {source_drive_id: dest_drive_id}
    """
    user_mapping:  Dict[str, str]
    drive_mapping: Dict[str, str]


# ─────────────────────────────────────────────────────────────────────────────
# Column name aliases recognised in import
# ─────────────────────────────────────────────────────────────────────────────

_SRC_USER_COLS  = {"source", "source email", "from", "src", "src_email"}
_DST_USER_COLS  = {"destination", "destination email", "to", "dst", "dst_email", "dest", "dest_email"}
_SRC_DRIVE_COLS = {"source_drive_id", "src_drive_id", "source drive id", "src drive id"}
_DST_DRIVE_COLS = {"dest_drive_id", "dst_drive_id", "destination_drive_id",
                   "dest drive id", "dst drive id", "destination drive id"}

_HEADER_KEYWORDS = {
    *_SRC_USER_COLS, *_DST_USER_COLS, *_SRC_DRIVE_COLS, *_DST_DRIVE_COLS
}


def _pick(row: dict, candidates: set) -> str:
    """Return the first non-blank value whose key (case-insensitive) is in candidates."""
    for k, v in row.items():
        if k.strip().lower() in candidates and v and v.strip():
            return v.strip()
    return ""


class UserManager:
    """Manages user operations across domains"""

    def __init__(
        self,
        source_admin_service,
        dest_admin_service,
        source_domain: str,
        dest_domain: str,
    ):
        self.source_admin  = source_admin_service
        self.dest_admin    = dest_admin_service
        self.source_domain = source_domain
        self.dest_domain   = dest_domain

    # ─────────────────────────────────────────────────────────────────────────
    # User fetching
    # ─────────────────────────────────────────────────────────────────────────

    def get_all_users(self, domain: str, admin_service) -> List[Dict]:
        users      = []
        page_token = None

        try:
            while True:
                logger.info(f"Fetching users from {domain}...")
                response = admin_service.users().list(
                    domain=domain,
                    maxResults=500,
                    orderBy="email",
                    pageToken=page_token,
                    fields=(
                        "users(primaryEmail,name,suspended,"
                        "archived,orgUnitPath,id),nextPageToken"
                    ),
                ).execute()

                batch = response.get("users", [])
                users.extend(batch)
                logger.info(f"  Retrieved {len(batch)} users (total: {len(users)})")

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            logger.info(f"Total users from {domain}: {len(users)}")
            return users

        except HttpError as exc:
            logger.error(f"Error fetching users from {domain}: {exc}")
            raise

    def get_source_users(
        self, filter_suspended: bool = True, filter_archived: bool = True
    ) -> List[Dict]:
        users = self.get_all_users(self.source_domain, self.source_admin)

        if filter_suspended or filter_archived:
            original = len(users)
            if filter_suspended:
                users = [u for u in users if not u.get("suspended", False)]
            if filter_archived:
                users = [u for u in users if not u.get("archived", False)]
            logger.info(f"Filtered out {original - len(users)} suspended/archived users")

        return users

    def get_dest_users(self) -> List[Dict]:
        return self.get_all_users(self.dest_domain, self.dest_admin)

    # ─────────────────────────────────────────────────────────────────────────
    # User mapping (auto-generated from domain lists)
    # ─────────────────────────────────────────────────────────────────────────

    def create_user_mapping(
        self,
        source_users: List[Dict],
        dest_users: List[Dict],
        mapping_strategy: str = "email",
    ) -> Dict[str, str]:
        mapping     = {}
        dest_emails = {u["primaryEmail"] for u in dest_users}

        for user in source_users:
            src_email = user["primaryEmail"]
            local     = src_email.split("@")[0]

            if mapping_strategy == "email":
                dst_email = f"{local}@{self.dest_domain}"
                if dst_email in dest_emails:
                    mapping[src_email] = dst_email
                    logger.debug(f"  Mapped: {src_email} → {dst_email}")
                else:
                    logger.warning(f"  No destination user for {src_email}")

        logger.info(f"Created mapping for {len(mapping)} users")
        return mapping

    # ─────────────────────────────────────────────────────────────────────────
    # Verification helpers
    # ─────────────────────────────────────────────────────────────────────────

    def verify_user_exists(self, email: str, admin_service) -> bool:
        try:
            admin_service.users().get(userKey=email).execute()
            return True
        except HttpError as exc:
            if exc.resp.status == 404:
                return False
            raise

    def get_user_info(self, email: str, admin_service) -> Optional[Dict]:
        try:
            return admin_service.users().get(
                userKey=email,
                fields=(
                    "primaryEmail,name,suspended,archived,"
                    "orgUnitPath,id,creationTime"
                ),
            ).execute()
        except HttpError as exc:
            logger.error(f"Error fetching user info for {email}: {exc}")
            return None

    # ─────────────────────────────────────────────────────────────────────────
    # CSV export
    # ─────────────────────────────────────────────────────────────────────────

    def export_user_mapping(
        self,
        mapping: Dict[str, str],
        filename: str,
        drive_mapping: Dict[str, str] = None,
    ):
        """
        Export user mapping (and optionally drive mapping) to CSV.

        If drive_mapping is provided the file will use the four-column format
        so it can be re-imported with import_mapping().

        Args:
            mapping       : {source_email: dest_email}
            filename      : output path
            drive_mapping : optional {source_drive_id: dest_drive_id}
        """
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            if drive_mapping:
                # Four-column format
                writer.writerow(
                    ["source", "destination", "source_drive_id", "dest_drive_id"]
                )
                for src, dst in sorted(mapping.items()):
                    writer.writerow([src, dst, "", ""])
                for src_drv, dst_drv in sorted(drive_mapping.items()):
                    writer.writerow(["", "", src_drv, dst_drv])
            else:
                # Two-column format (backward compatible)
                writer.writerow(["source", "destination"])
                for src, dst in sorted(mapping.items()):
                    writer.writerow([src, dst])

        logger.info(f"User mapping exported to {filename}")
        if drive_mapping:
            logger.info(
                f"  {len(mapping)} user rows, {len(drive_mapping)} drive rows"
            )

    # ─────────────────────────────────────────────────────────────────────────
    # CSV import — PATCH: now returns MappingResult with both mappings
    # ─────────────────────────────────────────────────────────────────────────

    def import_mapping(self, filename: str) -> MappingResult:
        """
        Import a mapping CSV that may contain user rows, shared drive rows, or both.

        Returns MappingResult(user_mapping, drive_mapping).

        Supported column names (case-insensitive):
          User source  : source | source email | from | src | src_email
          User dest    : destination | destination email | to | dst | dest | dest_email
          Drive source : source_drive_id | src_drive_id | source drive id | src drive id
          Drive dest   : dest_drive_id | dst_drive_id | destination_drive_id | dest drive id

        Example CSV (four-column mixed):
            source,destination,source_drive_id,dest_drive_id
            amey@dev.shivaami.in,htt@demo.shivaami.in,,
            bob@dev.shivaami.in,rob@demo.shivaami.in,,
            ,,0ABCdriveID1,0XYZdriveID2

        Example CSV (two-column, users only — backward compatible):
            source,destination
            amey@dev.shivaami.in,htt@demo.shivaami.in
        """
        user_mapping:  Dict[str, str] = {}
        drive_mapping: Dict[str, str] = {}

        with open(filename, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()
            f.seek(0)

            has_header = any(
                kw in first_line.lower() for kw in _HEADER_KEYWORDS
            )

            if has_header:
                reader = csv.DictReader(f)
                for row in reader:
                    # Normalise keys to lowercase for lookup
                    norm = {k.strip().lower(): v for k, v in row.items() if k}

                    src_user  = _pick(norm, _SRC_USER_COLS)
                    dst_user  = _pick(norm, _DST_USER_COLS)
                    src_drive = _pick(norm, _SRC_DRIVE_COLS)
                    dst_drive = _pick(norm, _DST_DRIVE_COLS)

                    if src_user and dst_user:
                        user_mapping[src_user] = dst_user

                    if src_drive and dst_drive:
                        drive_mapping[src_drive] = dst_drive

            else:
                # No header: assume two-column user mapping (backward compat)
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 2 and row[0].strip() and row[1].strip():
                        user_mapping[row[0].strip()] = row[1].strip()

        logger.info(
            f"Imported from '{filename}': "
            f"{len(user_mapping)} user rows, "
            f"{len(drive_mapping)} drive rows"
        )
        return MappingResult(
            user_mapping=user_mapping,
            drive_mapping=drive_mapping,
        )

    def import_user_mapping(self, filename: str) -> Dict[str, str]:
        """
        Backward-compatible wrapper.
        Returns only the user_mapping dict (ignores drive rows).
        Existing callers in main.py / migration_engine.py are unaffected.
        """
        result = self.import_mapping(filename)
        if result.drive_mapping:
            logger.info(
                f"  Note: {len(result.drive_mapping)} shared drive row(s) found "
                f"in '{filename}' — use import_mapping() to access them."
            )
        return result.user_mapping