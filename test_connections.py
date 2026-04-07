"""
Connection & Auth Test Suite for GWS Drive Migration
Run this before starting any migration to verify all systems are reachable.

Usage:
    python test_connections.py

All tests run independently — a failure in one does not skip the others.
Final summary shows pass/fail for every check.
"""
import sys
import traceback
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ── Colour helpers (no external deps) ────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):   print(f"  {GREEN}✓{RESET}  {msg}")
def fail(msg): print(f"  {RED}✗{RESET}  {msg}")
def warn(msg): print(f"  {YELLOW}!{RESET}  {msg}")
def info(msg): print(f"  {CYAN}→{RESET}  {msg}")

def section(title):
    print(f"\n{BOLD}{'─'*60}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'─'*60}{RESET}")

# ── Result tracker ────────────────────────────────────────────────────────────
results = {}   # { test_name: True | False }

def record(name, passed):
    results[name] = passed


# ════════════════════════════════════════════════════════════════════════════
# TEST 1 — Config loads and validates
# ════════════════════════════════════════════════════════════════════════════
def test_config():
    section("TEST 1 · Config validation")
    try:
        from config import Config
        Config.print_config()
        Config.validate()
        ok("Config loaded and validated")
        record("Config validation", True)
    except FileNotFoundError as e:
        fail(f"Missing credentials file: {e}")
        record("Config validation", False)
    except ValueError as e:
        fail(f"Missing required config value: {e}")
        record("Config validation", False)
    except Exception as e:
        fail(f"Unexpected config error: {e}")
        traceback.print_exc()
        record("Config validation", False)


# ════════════════════════════════════════════════════════════════════════════
# TEST 2 — Cloud SQL connection
# ════════════════════════════════════════════════════════════════════════════
def test_cloud_sql():
    section("TEST 2 · Cloud SQL (mysql.connector)")
    try:
        from config import Config
        info(f"Host:     {Config.DB_HOST}:{Config.DB_PORT}")
        info(f"Database: {Config.DB_NAME}")
        info(f"User:     {Config.DB_USER}")

        from auth import GoogleAuthManager
        conn = GoogleAuthManager.get_db_connection()

        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()
        ok(f"Connected — MySQL version: {version[0]}")

        cursor.execute("SELECT DATABASE()")
        db = cursor.fetchone()
        ok(f"Active database: {db[0]}")

        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        if tables:
            ok(f"Tables found: {', '.join(tables)}")
        else:
            warn("No tables found yet — run your schema migration first")

        cursor.close()
        conn.close()
        record("Cloud SQL", True)

    except Exception as e:
        fail(f"Cloud SQL connection failed: {e}")
        info("Check: Is your IP whitelisted in GCP → Cloud SQL → Authorized Networks?")
        info("Check: Are DB_HOST / DB_USER / DB_PASSWORD correct in config.py?")
        traceback.print_exc()
        record("Cloud SQL", False)


# ════════════════════════════════════════════════════════════════════════════
# TEST 3 — GCS bucket access
# ════════════════════════════════════════════════════════════════════════════
def test_gcs_bucket():
    section("TEST 3 · GCS bucket (hemant-bucket)")
    try:
        from config import Config
        info(f"Bucket: {Config.GCS_BUCKET_NAME}  ({Config.GCS_BUCKET_REGION})")
        info(f"SA key: {Config.GCS_SERVICE_ACCOUNT_FILE}")

        from auth import GoogleAuthManager
        client = GoogleAuthManager.get_gcs_client()
        ok("GCS client created")

        bucket = client.bucket(Config.GCS_BUCKET_NAME)

        # Light read — list at most 1 object
        blobs = list(bucket.list_blobs(max_results=1))
        ok(f"Bucket readable — objects found: {len(blobs)}")

        # Write + read + delete test using a small marker object
        test_blob_name = f"{Config.GCS_STAGING_PREFIX}_test_connection_check.txt"
        blob = bucket.blob(test_blob_name)
        blob.upload_from_string("migration connection test", content_type="text/plain")
        ok(f"Write OK → {test_blob_name}")

        data = blob.download_as_text()
        assert data == "migration connection test", "Read data mismatch"
        ok("Read OK")

        blob.delete()
        ok("Delete OK — test object cleaned up")

        record("GCS bucket", True)

    except Exception as e:
        fail(f"GCS bucket test failed: {e}")
        info("Check: Does gcs_service_account.json have Storage Object Admin role?")
        info("Check: Is GCS_BUCKET_NAME correct in config.py?")
        traceback.print_exc()
        record("GCS bucket", False)


# ════════════════════════════════════════════════════════════════════════════
# TEST 4 — Source domain Drive + Admin SDK auth
# ════════════════════════════════════════════════════════════════════════════
def test_source_domain():
    section("TEST 4 · Source domain (dev.shivaami.in)")
    try:
        from config import Config
        from auth import GoogleAuthManager

        info(f"Domain:      {Config.SOURCE_DOMAIN}")
        info(f"Admin:       {Config.SOURCE_ADMIN_EMAIL}")
        info(f"Credentials: {Config.SOURCE_CREDENTIALS_FILE}")

        source_auth = GoogleAuthManager(
            credentials_file=Config.SOURCE_CREDENTIALS_FILE,
            scopes=Config.SCOPES,
            token_file='source_token.pickle',
            delegate_email=Config.SOURCE_ADMIN_EMAIL
        )
        source_auth.authenticate()
        ok(f"Auth type detected: {source_auth.auth_type}")

        # Drive API — admin account
        drive = source_auth.get_drive_service()
        about = drive.about().get(fields='user,storageQuota').execute()
        email = about.get('user', {}).get('emailAddress', 'unknown')
        quota = about.get('storageQuota', {})
        ok(f"Drive API OK — logged in as: {email}")
        if quota.get('limit'):
            used_gb  = int(quota.get('usage', 0)) / 1e9
            limit_gb = int(quota.get('limit', 0)) / 1e9
            info(f"Storage: {used_gb:.2f} GB used of {limit_gb:.2f} GB")

        # Admin SDK — list first user as smoke test
        admin = source_auth.get_admin_service()
        users = admin.users().list(
            domain=Config.SOURCE_DOMAIN,
            maxResults=3,
            orderBy='email'
        ).execute()
        user_list = users.get('users', [])
        if user_list:
            ok(f"Admin SDK OK — sample users: {[u['primaryEmail'] for u in user_list]}")
        else:
            warn("Admin SDK returned no users — check domain or admin permissions")

        record("Source domain", True)

    except Exception as e:
        fail(f"Source domain auth failed: {e}")
        info("Check: Is SOURCE_CREDENTIALS_FILE correct and does it have Drive + Admin scopes?")
        info("Check: For Service Accounts — is domain-wide delegation enabled in Google Admin?")
        traceback.print_exc()
        record("Source domain", False)


# ════════════════════════════════════════════════════════════════════════════
# TEST 5 — Destination domain Drive + Admin SDK auth
# ════════════════════════════════════════════════════════════════════════════
def test_dest_domain():
    section("TEST 5 · Destination domain (demo.shivaami.in)")
    try:
        from config import Config
        from auth import GoogleAuthManager

        info(f"Domain:      {Config.DEST_DOMAIN}")
        info(f"Admin:       {Config.DEST_ADMIN_EMAIL}")
        info(f"Credentials: {Config.DEST_CREDENTIALS_FILE}")

        dest_auth = GoogleAuthManager(
            credentials_file=Config.DEST_CREDENTIALS_FILE,
            scopes=Config.SCOPES,
            token_file='dest_token.pickle',
            delegate_email=Config.DEST_ADMIN_EMAIL
        )
        dest_auth.authenticate()
        ok(f"Auth type detected: {dest_auth.auth_type}")

        # Drive API
        drive = dest_auth.get_drive_service()
        about = drive.about().get(fields='user,storageQuota').execute()
        email = about.get('user', {}).get('emailAddress', 'unknown')
        quota = about.get('storageQuota', {})
        ok(f"Drive API OK — logged in as: {email}")
        if quota.get('limit'):
            used_gb  = int(quota.get('usage', 0)) / 1e9
            limit_gb = int(quota.get('limit', 0)) / 1e9
            info(f"Storage: {used_gb:.2f} GB used of {limit_gb:.2f} GB")

        # Admin SDK
        admin = dest_auth.get_admin_service()
        users = admin.users().list(
            domain=Config.DEST_DOMAIN,
            maxResults=3,
            orderBy='email'
        ).execute()
        user_list = users.get('users', [])
        if user_list:
            ok(f"Admin SDK OK — sample users: {[u['primaryEmail'] for u in user_list]}")
        else:
            warn("Admin SDK returned no users — check domain or admin permissions")

        record("Destination domain", True)

    except Exception as e:
        fail(f"Destination domain auth failed: {e}")
        info("Check: Is DEST_CREDENTIALS_FILE correct and does it have Drive + Admin scopes?")
        traceback.print_exc()
        record("Destination domain", False)


# ════════════════════════════════════════════════════════════════════════════
# TEST 6 — Domain-wide delegation (SA only — the check that often breaks silently)
# ════════════════════════════════════════════════════════════════════════════
def test_delegation():
    section("TEST 6 · Domain-wide delegation (Service Account)")
    try:
        from config import Config
        from auth import GoogleAuthManager

        source_auth = GoogleAuthManager(
            credentials_file=Config.SOURCE_CREDENTIALS_FILE,
            scopes=Config.SCOPES,
            delegate_email=Config.SOURCE_ADMIN_EMAIL
        )
        source_auth.authenticate()

        if source_auth.auth_type != 'service_account':
            warn("Source credentials are OAuth — skipping delegation test (SA only)")
            warn("Delegation test only applies to Service Account credentials")
            record("Delegation", None)  # None = skipped
            return

        # Ask for a test user interactively
        print()
        test_user = input(
            f"  Enter a NON-ADMIN user email from {Config.SOURCE_DOMAIN} to test delegation\n"
            f"  (or press Enter to skip): "
        ).strip()

        if not test_user:
            warn("Delegation test skipped — no test user provided")
            warn("This is the most common failure point — test it before running migration")
            record("Delegation", None)
            return

        info(f"Testing delegation for: {test_user}")
        delegated_drive = source_auth.get_drive_service(user_email=test_user)
        about = delegated_drive.about().get(fields='user').execute()
        impersonated_as = about.get('user', {}).get('emailAddress')
        ok(f"Delegation OK — successfully impersonated: {impersonated_as}")

        # Also list a few files from that user's Drive
        files = delegated_drive.files().list(
            pageSize=3,
            fields='files(id,name,mimeType)'
        ).execute()
        file_list = files.get('files', [])
        if file_list:
            ok(f"Can read user's Drive — sample files: {[f['name'] for f in file_list]}")
        else:
            warn("User's Drive appears empty or files not readable")

        record("Delegation", True)

    except Exception as e:
        fail(f"Delegation failed: {e}")
        info("Check: In Google Admin → Security → API Controls → Domain-wide Delegation")
        info("Check: The SA client_id is listed with all required scopes")
        traceback.print_exc()
        record("Delegation", False)


# ════════════════════════════════════════════════════════════════════════════
# TEST 7 — DomainAuthManager full integration test
# ════════════════════════════════════════════════════════════════════════════
def test_domain_auth_manager():
    section("TEST 7 · DomainAuthManager (full integration)")
    try:
        from config import Config
        from auth import DomainAuthManager

        source_config = {
            'credentials_file': Config.SOURCE_CREDENTIALS_FILE,
            'domain': Config.SOURCE_DOMAIN,
            'admin_email': Config.SOURCE_ADMIN_EMAIL,
        }
        dest_config = {
            'credentials_file': Config.DEST_CREDENTIALS_FILE,
            'domain': Config.DEST_DOMAIN,
            'admin_email': Config.DEST_ADMIN_EMAIL,
        }

        manager = DomainAuthManager(source_config, dest_config, Config.SCOPES)
        manager.authenticate_all()
        ok("authenticate_all() completed for both domains")

        src_services = manager.get_source_services()
        assert 'drive' in src_services and 'admin' in src_services
        ok("get_source_services() returned drive + admin")

        dst_services = manager.get_dest_services()
        assert 'drive' in dst_services and 'admin' in dst_services
        ok("get_dest_services() returned drive + admin")

        result = manager.test_connection()
        if result:
            ok("test_connection() passed")
        else:
            fail("test_connection() returned False")

        record("DomainAuthManager", result)

    except Exception as e:
        fail(f"DomainAuthManager test failed: {e}")
        traceback.print_exc()
        record("DomainAuthManager", False)


# ════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ════════════════════════════════════════════════════════════════════════════
def print_summary():
    print(f"\n{BOLD}{'═'*60}{RESET}")
    print(f"{BOLD}  SUMMARY{RESET}")
    print(f"{BOLD}{'═'*60}{RESET}")

    passed  = 0
    failed  = 0
    skipped = 0

    for name, result in results.items():
        if result is True:
            print(f"  {GREEN}PASS{RESET}  {name}")
            passed += 1
        elif result is False:
            print(f"  {RED}FAIL{RESET}  {name}")
            failed += 1
        else:
            print(f"  {YELLOW}SKIP{RESET}  {name}")
            skipped += 1

    print(f"\n  Total: {passed} passed  {failed} failed  {skipped} skipped")

    if failed == 0:
        print(f"\n  {GREEN}{BOLD}✓ All systems ready for migration{RESET}\n")
    else:
        print(f"\n  {RED}{BOLD}✗ Fix the failures above before running migration{RESET}\n")

    print(f"{'═'*60}\n")
    return failed == 0


# ════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print(f"\n{BOLD}{'═'*60}")
    print("  GWS MIGRATION — CONNECTION & AUTH TEST SUITE")
    print(f"{'═'*60}{RESET}")

    test_config()
    test_cloud_sql()
    test_gcs_bucket()
    test_source_domain()
    test_dest_domain()
    test_delegation()
    test_domain_auth_manager()

    all_passed = print_summary()
    sys.exit(0 if all_passed else 1)