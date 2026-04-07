"""
Configuration module for GWS Drive Migration
Hardcoded version - no .env dependency
"""
from pathlib import Path

class Config:
    """Configuration settings for Drive migration"""
    
    # OAuth 2.0 Scopes Required
    SCOPES = [
        'https://www.googleapis.com/auth/admin.directory.user.readonly',
        'https://www.googleapis.com/auth/admin.directory.domain.readonly',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive.metadata',
        'https://www.googleapis.com/auth/drive.readonly',
    ]
    
    # ============================================================================
    # SOURCE DOMAIN CONFIGURATION
    # ============================================================================
    SOURCE_DOMAIN = 'dev.shivaami.in'
    SOURCE_ADMIN_EMAIL = 'hemant@dev.shivaami.in'
    SOURCE_CREDENTIALS_FILE = 'source_credentials.json'
    
    # ============================================================================
    # DESTINATION DOMAIN CONFIGURATION
    # ============================================================================
    DEST_DOMAIN = 'demo.shivaami.in'
    DEST_ADMIN_EMAIL = 'developers@demo.shivaami.in'
    DEST_CREDENTIALS_FILE = 'dest_credentials.json'
    
    # ============================================================================
    # MIGRATION SETTINGS
    # ============================================================================
    BATCH_SIZE = 10              # Files per batch
    MAX_WORKERS = 4             # Parallel workers (adjust based on API quotas)
    PARALLEL_FILES = 8           # Level 2: parallel files per user
    RETRY_ATTEMPTS = 3           # Retry failed operations
    RETRY_DELAY = 5              # Seconds between retries
    
    # ============================================================================
    # FILE SETTINGS
    # ============================================================================
    MAX_FILE_SIZE_MB = 5120      # Skip files larger than this (5GB default)
    EXCLUDED_MIME_TYPES = []     # Add MIME types to exclude if needed
    
    # ============================================================================
    # LOGGING
    # ============================================================================
    LOG_LEVEL = 'INFO'           # DEBUG, INFO, WARNING, ERROR
    LOG_FILE = 'migration.log'
    
    # ============================================================================
    # CLOUD SQL CONFIGURATION (mysql.connector, Public IP + whitelist)
    # ============================================================================
    DB_HOST = '34.180.17.138'                  # ← Replace with your Cloud SQL Public IP
    DB_PORT = 3306
    DB_NAME = 'hemant'             # ← Replace with your database name
    DB_USER = 'root'                     # ← Replace with your DB user
    DB_PASSWORD = ''                     # ← Replace with your DB password
    DB_CONNECT_TIMEOUT = 30              # Seconds before connection timeout

    @classmethod
    def get_db_connection(cls):
        """
        Returns a mysql.connector connection using Cloud SQL public IP.
        Ensure your machine's IP is whitelisted in Cloud SQL > Connections > Authorized Networks.

        Usage:
            conn = Config.get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            conn.close()
        """
        import mysql.connector
        return mysql.connector.connect(
            host=cls.DB_HOST,
            port=cls.DB_PORT,
            database=cls.DB_NAME,
            user=cls.DB_USER,
            password=cls.DB_PASSWORD,
            connect_timeout=cls.DB_CONNECT_TIMEOUT,
            ssl_disabled=False,   # Cloud SQL enforces SSL by default; keep enabled
        )

    # ============================================================================
    # GCS BUCKET CONFIGURATION (Temporary migration staging)
    # ============================================================================
    # Bucket: hemant-bucket | Region: asia-south1 (Mumbai) | Class: Standard
    #
    # Purpose: Files are uploaded here as a temporary staging area during migration.
    #          Once a file is 100% confirmed migrated to the destination domain,
    #          it is deleted from the bucket to keep storage clean.
    #
    GCS_BUCKET_NAME = 'hemant-bucket'
    GCS_BUCKET_REGION = 'asia-south1'
    GCS_STAGING_PREFIX = 'migration-staging/'      # Folder prefix inside bucket
    GCS_TEMP_EXPIRY_HOURS = 48                     # Max hours a staged file should live
    GCS_SERVICE_ACCOUNT_FILE = 'gcs-project.json'  # ← SA key with Storage Object Admin

    @classmethod
    def get_gcs_staging_path(cls, user_email: str, file_id: str) -> str:
        """
        Returns the full GCS object path for a file being staged.

        Format: migration-staging/<user_email>/<file_id>
        This groups staged files by user so cleanup and tracking is easy.

        Args:
            user_email: Source user (e.g. hemant@dev.shivaami.in)
            file_id:    Google Drive file ID

        Returns:
            str: GCS object path, e.g. 'migration-staging/hemant@dev.shivaami.in/1aBcD...'
        """
        safe_email = user_email.replace('/', '_')  # Sanitize for path safety
        return f"{cls.GCS_STAGING_PREFIX}{safe_email}/{file_id}"

    # ============================================================================
    # DATABASE/STATE MANAGEMENT
    # ============================================================================
    RESUME_ON_FAILURE = True
    # NOTE: State is now tracked in Cloud SQL (DB_NAME above), not a local SQLite file.
    #       migration_state.db has been replaced by Cloud SQL tables.

    # ============================================================================
    # OUTPUT
    # ============================================================================
    REPORT_DIR = Path('reports')
    REPORT_DIR.mkdir(exist_ok=True)
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        print("\n" + "="*70)
        print(" "*20 + "VALIDATING CONFIGURATION")
        print("="*70)
        
        required_vars = [
            ('SOURCE_DOMAIN', cls.SOURCE_DOMAIN),
            ('SOURCE_ADMIN_EMAIL', cls.SOURCE_ADMIN_EMAIL),
            ('DEST_DOMAIN', cls.DEST_DOMAIN),
            ('DEST_ADMIN_EMAIL', cls.DEST_ADMIN_EMAIL),
            ('DB_HOST', cls.DB_HOST),
            ('DB_NAME', cls.DB_NAME),
            ('GCS_BUCKET_NAME', cls.GCS_BUCKET_NAME),
        ]
        
        missing = []
        print("\n📋 Required Configuration:")
        for name, value in required_vars:
            if not value:
                missing.append(name)
                print(f"   ✗ {name:<25} MISSING")
            else:
                print(f"   ✓ {name:<25} {value}")
        
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        
        # Check credentials files
        print(f"\n🔑 Credentials Files:")
        for label, path_str in [
            ("Source Drive",  cls.SOURCE_CREDENTIALS_FILE),
            ("Dest Drive",    cls.DEST_CREDENTIALS_FILE),
            ("GCS SA Key",    cls.GCS_SERVICE_ACCOUNT_FILE),
        ]:
            p = Path(path_str)
            status = "✓" if p.exists() else "✗ NOT FOUND"
            print(f"   {status}  {label:<15} {path_str}")
            if not p.exists() and label != "GCS SA Key":  # GCS key optional if using ADC
                raise FileNotFoundError(f"Credentials file not found: {path_str}")

        # Test Cloud SQL connectivity
        print(f"\n🗄️  Cloud SQL:")
        try:
            conn = cls.get_db_connection()
            conn.close()
            print(f"   ✓ Connected    {cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}")
        except Exception as e:
            print(f"   ✗ Connection failed: {e}")
            raise

        print("\n" + "="*70)
        print(" "*25 + "✓ Configuration Valid")
        print("="*70 + "\n")
        
        return True
    
    @classmethod
    def print_config(cls):
        """Print current configuration"""
        print("\n" + "="*70)
        print(" "*20 + "MIGRATION CONFIGURATION")
        print("="*70)
        
        print(f"\n📤 SOURCE DOMAIN")
        print(f"   Domain:      {cls.SOURCE_DOMAIN}")
        print(f"   Admin:       {cls.SOURCE_ADMIN_EMAIL}")
        print(f"   Credentials: {cls.SOURCE_CREDENTIALS_FILE}")
        
        print(f"\n📥 DESTINATION DOMAIN")
        print(f"   Domain:      {cls.DEST_DOMAIN}")
        print(f"   Admin:       {cls.DEST_ADMIN_EMAIL}")
        print(f"   Credentials: {cls.DEST_CREDENTIALS_FILE}")
        
        print(f"\n⚙️  MIGRATION SETTINGS")
        print(f"   Max Workers:     {cls.MAX_WORKERS} parallel threads")
        print(f"   Batch Size:      {cls.BATCH_SIZE} files per batch")
        print(f"   Retry Attempts:  {cls.RETRY_ATTEMPTS} times")
        print(f"   Max File Size:   {cls.MAX_FILE_SIZE_MB} MB")
        
        print(f"\n🗄️  CLOUD SQL (mysql.connector)")
        print(f"   Host:            {cls.DB_HOST}:{cls.DB_PORT}")
        print(f"   Database:        {cls.DB_NAME}")
        print(f"   User:            {cls.DB_USER}")
        print(f"   Connect Timeout: {cls.DB_CONNECT_TIMEOUT}s")
        print(f"   Auth:            Public IP + Authorized Networks whitelist")

        print(f"\n🪣  GCS BUCKET (Temp Staging)")
        print(f"   Bucket:          {cls.GCS_BUCKET_NAME}")
        print(f"   Region:          {cls.GCS_BUCKET_REGION}")
        print(f"   Staging Prefix:  {cls.GCS_STAGING_PREFIX}")
        print(f"   Temp Expiry:     {cls.GCS_TEMP_EXPIRY_HOURS} hours")
        print(f"   SA Key File:     {cls.GCS_SERVICE_ACCOUNT_FILE}")
        print(f"   Cleanup:         Auto-delete after 100% migration confirmed")

        print(f"\n📊 LOGGING & OUTPUT")
        print(f"   Log Level:       {cls.LOG_LEVEL}")
        print(f"   Log File:        {cls.LOG_FILE}")
        print(f"   Report Dir:      {cls.REPORT_DIR}")
        print(f"   Resume on Fail:  {cls.RESUME_ON_FAILURE}")
        
        print("="*70 + "\n")


if __name__ == "__main__":
    print("\n🧪 Testing Configuration...\n")
    Config.print_config()
    try:
        Config.validate()
        print("✅ Configuration test PASSED!\n")
    except Exception as e:
        print(f"\n❌ Configuration test FAILED: {e}\n")