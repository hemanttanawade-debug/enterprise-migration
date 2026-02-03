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
    MAX_WORKERS = 5              # Parallel workers (adjust based on API quotas)
    RETRY_ATTEMPTS = 3           # Retry failed operations
    RETRY_DELAY = 5              # Seconds between retries
    
    # ============================================================================
    # CHECKPOINT CONFIGURATION
    # ============================================================================
    CHECKPOINT_ENABLED = True                    # Enable checkpoint system
    CHECKPOINT_FOLDER = './migration_checkpoints'  # Checkpoint storage folder
    CHECKPOINT_AUTO_RESUME = True                # Auto-resume from latest checkpoint
    CHECKPOINT_CLEANUP_DAYS = 30                 # Delete checkpoints older than N days
    
    # Retry configuration for failed items
    MAX_RETRY_ATTEMPTS = 3                       # Maximum retries for failed items
    RETRY_DELAY_SECONDS = 60                     # Delay before retrying failed items
    
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
    # DATABASE/STATE MANAGEMENT
    # ============================================================================
    STATE_DB_FILE = 'migration_state.db'
    RESUME_ON_FAILURE = True
    
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
        ]
        
        # Check for missing values
        missing = []
        print("\n📋 Required Configuration:")
        for name, value in required_vars:
            if not value:
                missing.append(name)
                print(f"   ✗ {name:<20} MISSING")
            else:
                print(f"   ✓ {name:<20} {value}")
        
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        
        # Check credentials files exist
        print(f"\n🔑 Credentials Files:")
        
        source_creds_path = Path(cls.SOURCE_CREDENTIALS_FILE)
        if source_creds_path.exists():
            print(f"   ✓ Source:      {cls.SOURCE_CREDENTIALS_FILE}")
        else:
            print(f"   ✗ Source:      NOT FOUND ({cls.SOURCE_CREDENTIALS_FILE})")
            raise FileNotFoundError(f"Source credentials file not found: {cls.SOURCE_CREDENTIALS_FILE}")
        
        dest_creds_path = Path(cls.DEST_CREDENTIALS_FILE)
        if dest_creds_path.exists():
            print(f"   ✓ Destination: {cls.DEST_CREDENTIALS_FILE}")
        else:
            print(f"   ✗ Destination: NOT FOUND ({cls.DEST_CREDENTIALS_FILE})")
            raise FileNotFoundError(f"Destination credentials file not found: {cls.DEST_CREDENTIALS_FILE}")
        
        # Create checkpoint folder if enabled
        if cls.CHECKPOINT_ENABLED:
            try:
                Path(cls.CHECKPOINT_FOLDER).mkdir(parents=True, exist_ok=True)
                print(f"\n💾 Checkpoint System:")
                print(f"   ✓ Enabled:     {cls.CHECKPOINT_ENABLED}")
                print(f"   ✓ Folder:      {cls.CHECKPOINT_FOLDER}")
                print(f"   ✓ Auto-resume: {cls.CHECKPOINT_AUTO_RESUME}")
            except Exception as e:
                print(f"   ✗ Failed to create checkpoint folder: {e}")
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
        
        print(f"\n💾 CHECKPOINT SYSTEM")
        print(f"   Enabled:         {cls.CHECKPOINT_ENABLED}")
        if cls.CHECKPOINT_ENABLED:
            print(f"   Folder:          {cls.CHECKPOINT_FOLDER}")
            print(f"   Auto-resume:     {cls.CHECKPOINT_AUTO_RESUME}")
            print(f"   Cleanup Days:    {cls.CHECKPOINT_CLEANUP_DAYS}")
            print(f"   Max Retries:     {cls.MAX_RETRY_ATTEMPTS}")
        
        print(f"\n📊 LOGGING & OUTPUT")
        print(f"   Log Level:       {cls.LOG_LEVEL}")
        print(f"   Log File:        {cls.LOG_FILE}")
        print(f"   Report Dir:      {cls.REPORT_DIR}")
        print(f"   State DB:        {cls.STATE_DB_FILE}")
        print(f"   Resume on Fail:  {cls.RESUME_ON_FAILURE}")
        
        print("="*70 + "\n")


# Auto-validate on import in development
if __name__ == "__main__":
    print("\n🧪 Testing Configuration...\n")
    Config.print_config()
    try:
        Config.validate()
        print("✅ Configuration test PASSED!\n")
    except Exception as e:
        print(f"\n❌ Configuration test FAILED: {e}\n")