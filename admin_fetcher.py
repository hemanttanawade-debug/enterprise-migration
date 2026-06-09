import os
import logging
import tempfile
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)

def fetch_and_store_admin_email(dest_domain: str, dest_admin_email: str) -> str:
    BASE_BACKEND_DIR = Path("/home/hemant_tanawade/flask-backend")
    ADMIN_FILE = BASE_BACKEND_DIR / "uploads" / "admin"
    DEST_CREDENTIALS_FILE = BASE_BACKEND_DIR / "uploads" / "credential" / "dest_credentials.json"

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        ADMIN_SCOPES = [
            "https://www.googleapis.com/auth/admin.directory.user.readonly",
        ]

        logger.info(f"[ADMIN_FETCH] Loading credentials from: {DEST_CREDENTIALS_FILE}")
        logger.info(f"[ADMIN_FETCH] Delegating as: {dest_admin_email}")
        logger.info(f"[ADMIN_FETCH] Credentials file exists: {DEST_CREDENTIALS_FILE.exists()}")

        creds = service_account.Credentials.from_service_account_file(
            str(DEST_CREDENTIALS_FILE),
            scopes=ADMIN_SCOPES,
        ).with_subject(dest_admin_email)

        directory = build(
            "admin", "directory_v1",
            credentials=creds,
            cache_discovery=False,
            static_discovery=False
        )

        super_admin_emails: List[str] = []
        page_token = None
        page_num = 0

        while True:
            try:
                kwargs = dict(
                    customer="my_customer",
                    maxResults=500,
                )
                if page_token:
                    kwargs["pageToken"] = page_token

                logger.info(f"[ADMIN_FETCH] Fetching page {page_num + 1} from Admin SDK ...")
                resp  = directory.users().list(**kwargs).execute()
                users = resp.get("users") or []
                page_num += 1

                logger.info(f"[ADMIN_FETCH] Page {page_num}: got {len(users)} total users")

                for user in users:
                    email     = user.get("primaryEmail", "")
                    is_admin  = user.get("isAdmin", False)
                    suspended = user.get("suspended", False)
                    archived  = user.get("archived", False)

                    # Log every user so you can see what the API is returning
                    logger.info(
                        f"[ADMIN_FETCH] User: {email} | "
                        f"isAdmin={is_admin} | suspended={suspended} | archived={archived}"
                    )

                    if is_admin is True and not suspended and not archived:
                        if email:
                            logger.info(f"[ADMIN_FETCH] >>> Super admin found: {email}")
                            super_admin_emails.append(email)

                    if len(super_admin_emails) >= 6:
                        break

                page_token = resp.get("nextPageToken")
                if not page_token or len(super_admin_emails) >= 6:
                    break

            except Exception as page_exc:
                logger.error(f"[ADMIN_FETCH] Page fetch FAILED: {page_exc}", exc_info=True)
                break

        # Limit to 6
        super_admin_emails = super_admin_emails[:6]

        logger.info(
            f"[ADMIN_FETCH] Total super admins found: {len(super_admin_emails)} → {super_admin_emails}"
        )

        # ── If API returned nothing, do NOT silently write fallback email ──────
        # Log clearly so you know it hit the fallback
        if not super_admin_emails:
            logger.error(
                f"[ADMIN_FETCH] *** FALLBACK TRIGGERED *** "
                f"No super admins from API. Writing dest_admin_email as fallback: {dest_admin_email}. "
                f"Check: 1) domain-wide delegation enabled? "
                f"2) {dest_admin_email} is a real super admin? "
                f"3) scope authorized in Admin Console?"
            )
            ADMIN_FILE.parent.mkdir(parents=True, exist_ok=True)
            ADMIN_FILE.write_text(dest_admin_email, encoding="utf-8")
            os.chmod(ADMIN_FILE, 0o600)
            return dest_admin_email

        ADMIN_FILE.parent.mkdir(parents=True, exist_ok=True)
        new_content = ",".join(super_admin_emails)

        if ADMIN_FILE.exists() and ADMIN_FILE.read_text(encoding="utf-8") == new_content:
            logger.debug(f"[ADMIN_FETCH] uploads/admin unchanged — skipping rewrite")
        else:
            tmp_fd, tmp_path = tempfile.mkstemp(
                dir=str(ADMIN_FILE.parent), prefix=".admin_tmp_"
            )
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
                    fh.write(new_content)
                os.chmod(tmp_path, 0o600)
                os.replace(tmp_path, str(ADMIN_FILE))
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise

            logger.info(f"[ADMIN_FETCH] uploads/admin written: {new_content}")

        return new_content

    except Exception as exc:
        logger.error(
            f"[ADMIN_FETCH] *** OUTER EXCEPTION — FALLBACK TRIGGERED *** {exc}",
            exc_info=True   # this prints full stack trace so you see exactly what failed
        )
        try:
            ADMIN_FILE.parent.mkdir(parents=True, exist_ok=True)
            ADMIN_FILE.write_text(dest_admin_email, encoding="utf-8")
            os.chmod(ADMIN_FILE, 0o600)
        except Exception as write_err:
            logger.error(f"[ADMIN_FETCH] Emergency write failed: {write_err}")

        return dest_admin_email
