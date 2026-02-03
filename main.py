"""
Main application entry point for Google Workspace Drive Migration
With Checkpoint System Integration
"""
import argparse
import sys
from pathlib import Path
from datetime import datetime

from config import Config
from auth import DomainAuthManager
from users import UserManager
from drive_operations import DriveOperations
from migration_engine import MigrationEngine
from state_manager import StateManager
from logging_config import setup_logging, create_logger

# Import checkpoint system - FIXED IMPORT
from checkpoint_manager import CheckpointManager, MigrationStatus, CheckpointRecord


logger = None


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Google Workspace Drive Migration Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Full domain migration with checkpoints
  python main.py --mode full --max-workers 5
  
  # Resume from latest checkpoint
  python main.py --mode resume
  
  # Resume from specific checkpoint
  python main.py --mode resume --checkpoint-file migration_checkpoints/checkpoint.csv
  
  # Dry run (list users only)
  python main.py --mode dry-run
  
  # Migrate specific users
  python main.py --mode custom --user-mapping users.csv
  
  # Generate report only
  python main.py --mode report
  
  # List all checkpoints
  python main.py --mode checkpoint-list
  
  # Cleanup old checkpoints
  python main.py --mode checkpoint-cleanup --cleanup-days 30
        '''
    )
    
    parser.add_argument(
        '--mode',
        choices=['full', 'dry-run', 'custom', 'resume', 'report', 'validate', 
                 'checkpoint-list', 'checkpoint-cleanup', 'checkpoint-report'],
        default='full',
        help='Migration mode'
    )
    
    parser.add_argument(
        '--user-mapping',
        type=str,
        help='CSV file with user mapping (source,destination)'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        default=Config.MAX_WORKERS,
        help=f'Number of parallel workers (default: {Config.MAX_WORKERS})'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=Config.BATCH_SIZE,
        help=f'Batch size for processing (default: {Config.BATCH_SIZE})'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default=Config.LOG_LEVEL,
        help=f'Logging level (default: {Config.LOG_LEVEL})'
    )
    
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Start fresh migration (ignore checkpoints and previous state)'
    )
    
    parser.add_argument(
        '--checkpoint-file',
        type=str,
        help='Specific checkpoint file to resume from'
    )
    
    parser.add_argument(
        '--no-checkpoint',
        action='store_true',
        help='Disable checkpoint system for this run'
    )
    
    parser.add_argument(
        '--cleanup-days',
        type=int,
        default=Config.CHECKPOINT_CLEANUP_DAYS,
        help=f'Delete checkpoints older than N days (default: {Config.CHECKPOINT_CLEANUP_DAYS})'
    )
    
    parser.add_argument(
        '--filter-suspended',
        action='store_true',
        default=True,
        help='Filter out suspended users (default: True)'
    )
    
    parser.add_argument(
        '--filter-archived',
        action='store_true',
        default=True,
        help='Filter out archived users (default: True)'
    )
    
    return parser.parse_args()


def validate_setup():
    """Validate configuration and setup"""
    global logger
    logger.info("Validating setup...")
    
    try:
        Config.validate()
        logger.info("✓ Configuration validated")
        return True
    except Exception as e:
        logger.error(f"✗ Configuration validation failed: {e}")
        return False


def checkpoint_list_mode():
    """List all available checkpoints"""
    global logger
    from checkpoint_utils import list_all_checkpoints
    
    logger.info("="*80)
    logger.info("CHECKPOINT LIST")
    logger.info("="*80)
    
    checkpoints = list_all_checkpoints(Config.CHECKPOINT_FOLDER)
    
    if not checkpoints:
        logger.info("No checkpoints found")
        return True
    
    logger.info(f"\nFound {len(checkpoints)} checkpoint(s):\n")
    
    for i, cp in enumerate(checkpoints, 1):
        logger.info(f"{i}. {cp['filename']}")
        logger.info(f"   Source: {cp['source_domain']} → Dest: {cp['dest_domain']}")
        logger.info(f"   Modified: {cp['modified'].strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"   Size: {cp['size_mb']:.2f} MB")
        logger.info("")
    
    logger.info("="*80)
    return True


def checkpoint_cleanup_mode(cleanup_days, dry_run=True):
    """Cleanup old checkpoints"""
    global logger
    from checkpoint_utils import cleanup_old_checkpoints
    
    logger.info("="*80)
    logger.info(f"CHECKPOINT CLEANUP ({'DRY RUN' if dry_run else 'LIVE'})")
    logger.info("="*80)
    
    stats = cleanup_old_checkpoints(
        Config.CHECKPOINT_FOLDER,
        days_old=cleanup_days,
        dry_run=dry_run
    )
    
    logger.info(f"\nTotal checkpoints: {stats['total_checkpoints']}")
    logger.info(f"To delete: {stats['to_delete']}")
    logger.info(f"To keep: {stats['kept']}")
    
    if dry_run and stats.get('would_delete'):
        logger.info("\nWould delete:")
        for filename in stats['would_delete']:
            logger.info(f"  - {filename}")
        logger.info("\nRun with --no-dry-run to actually delete")
    elif not dry_run and stats.get('deleted_files'):
        logger.info("\nDeleted:")
        for filename in stats['deleted_files']:
            logger.info(f"  - {filename}")
    
    logger.info("="*80)
    return True


def checkpoint_report_mode(checkpoint_file=None):
    """Generate report from checkpoint"""
    global logger
    from checkpoint_utils import generate_checkpoint_report
    
    if not checkpoint_file:
        # Find latest checkpoint
        checkpoint_file = CheckpointManager.find_latest_checkpoint(
            Config.CHECKPOINT_FOLDER,
            Config.SOURCE_DOMAIN,
            Config.DEST_DOMAIN
        )
    
    if not checkpoint_file or not Path(checkpoint_file).exists():
        logger.error("No checkpoint file found")
        return False
    
    logger.info(f"Generating report for: {checkpoint_file}")
    generate_checkpoint_report(checkpoint_file)
    
    return True


def initialize_checkpoint_manager(args):
    """Initialize checkpoint manager based on mode and arguments"""
    global logger
    
    # Check if checkpoints are disabled
    if args.no_checkpoint or not Config.CHECKPOINT_ENABLED:
        logger.info("Checkpoint system disabled")
        return None
    
    # Determine checkpoint file
    checkpoint_file = None
    
    if args.mode == 'resume':
        if args.checkpoint_file:
            # Use specific checkpoint
            checkpoint_file = args.checkpoint_file
            if not Path(checkpoint_file).exists():
                logger.error(f"Checkpoint file not found: {checkpoint_file}")
                return None
        elif Config.CHECKPOINT_AUTO_RESUME:
            # Find latest checkpoint
            checkpoint_file = CheckpointManager.find_latest_checkpoint(
                Config.CHECKPOINT_FOLDER,
                Config.SOURCE_DOMAIN,
                Config.DEST_DOMAIN
            )
            if not checkpoint_file:
                logger.warning("No checkpoint found to resume from, starting fresh")
    
    # Initialize checkpoint manager
    checkpoint_mgr = CheckpointManager(
        source_domain=Config.SOURCE_DOMAIN,
        dest_domain=Config.DEST_DOMAIN,
        checkpoint_folder=Config.CHECKPOINT_FOLDER,
        resume_from=checkpoint_file
    )
    
    # Print summary
    checkpoint_mgr.print_summary()
    
    return checkpoint_mgr


def dry_run_mode(auth_manager):
    """Dry run - list users without migration"""
    global logger
    logger.info("="*80)
    logger.info("DRY RUN MODE - Listing users only")
    logger.info("="*80)
    
    # Get services
    source_services = auth_manager.get_source_services()
    dest_services = auth_manager.get_dest_services()
    
    # Initialize user manager
    user_mgr = UserManager(
        source_services['admin'],
        dest_services['admin'],
        Config.SOURCE_DOMAIN,
        Config.DEST_DOMAIN
    )
    
    # Get users
    logger.info("Fetching source users...")
    source_users = user_mgr.get_source_users(
        filter_suspended=True,
        filter_archived=True
    )
    
    logger.info("Fetching destination users...")
    dest_users = user_mgr.get_dest_users()
    
    # Create mapping
    user_mapping = user_mgr.create_user_mapping(source_users, dest_users)
    
    # Export mapping
    mapping_file = Config.REPORT_DIR / f'user_mapping_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    user_mgr.export_user_mapping(user_mapping, str(mapping_file))
    
    # Summary
    logger.info("="*80)
    logger.info("DRY RUN SUMMARY")
    logger.info(f"Source Users: {len(source_users)}")
    logger.info(f"Destination Users: {len(dest_users)}")
    logger.info(f"Mapped Users: {len(user_mapping)}")
    logger.info(f"Mapping File: {mapping_file}")
    logger.info("="*80)
    
    return user_mapping


def custom_migration_mode(auth_manager, mapping_file, max_workers, args):
    """Custom migration with user-provided mapping"""
    global logger
    logger.info("="*80)
    logger.info("CUSTOM MIGRATION MODE")
    logger.info(f"Mapping file: {mapping_file}")
    logger.info("="*80)
    
    # Initialize checkpoint manager
    checkpoint_mgr = initialize_checkpoint_manager(args)
    
    # Get services
    source_services = auth_manager.get_source_services()
    dest_services = auth_manager.get_dest_services()
    
    # Initialize user manager
    user_mgr = UserManager(
        source_services['admin'],
        dest_services['admin'],
        Config.SOURCE_DOMAIN,
        Config.DEST_DOMAIN
    )
    
    # Load user mapping from CSV
    logger.info(f"Loading user mapping from {mapping_file}")
    user_mapping = user_mgr.import_user_mapping(mapping_file)
    
    if not user_mapping:
        logger.error("No users found in mapping file!")
        return False
    
    logger.info(f"Loaded {len(user_mapping)} user mappings")
    
    # Verify users exist
    logger.info("Verifying users exist in both domains...")
    verified_mapping = {}
    
    for source_email, dest_email in user_mapping.items():
        # Verify source user
        if not user_mgr.verify_user_exists(source_email, source_services['admin']):
            logger.warning(f"Source user not found: {source_email} - Skipping")
            continue
        
        # Verify destination user
        if not user_mgr.verify_user_exists(dest_email, dest_services['admin']):
            logger.warning(f"Destination user not found: {dest_email} - Skipping")
            continue
        
        verified_mapping[source_email] = dest_email
        logger.info(f"✓ Verified: {source_email} -> {dest_email}")
    
    if not verified_mapping:
        logger.error("No valid user mappings found after verification!")
        return False
    
    logger.info(f"Proceeding with {len(verified_mapping)} verified users")
    
    # Initialize state manager
    state_db = Config.STATE_DB_FILE
    
    with StateManager(state_db) as state_mgr:
        # Add users to state
        for src, dst in verified_mapping.items():
            state_mgr.add_user(src, dst)
        
        # Initialize drive operations
        source_drive_ops = DriveOperations(source_services['drive'])
        dest_drive_ops = DriveOperations(dest_services['drive'])
        
        # Initialize migration engine with checkpoint manager
        migration_engine = MigrationEngine(
            source_drive_ops,
            dest_drive_ops,
            Config,
            state_mgr,
            checkpoint_manager=checkpoint_mgr
        )
        
        # Start migration run
        run_id = state_mgr.start_migration_run({
            'source_domain': Config.SOURCE_DOMAIN,
            'dest_domain': Config.DEST_DOMAIN,
            'total_users': len(verified_mapping),
            'max_workers': max_workers,
            'mode': 'custom',
            'mapping_file': mapping_file,
            'checkpoint_enabled': checkpoint_mgr is not None,
            'checkpoint_file': checkpoint_mgr.checkpoint_file if checkpoint_mgr else None
        })
        
        logger.info(f"Starting custom migration run ID: {run_id}")
        
        # Execute migration
        try:
            summary = migration_engine.migrate_domain(verified_mapping, max_workers)
            
            # Generate reports
            report_file = Config.REPORT_DIR / f'custom_migration_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            migration_engine.generate_report(summary, str(report_file))
            
            # Update state
            state_mgr.end_migration_run(run_id, 'completed', summary)
            
            # Print checkpoint summary if enabled
            if checkpoint_mgr:
                checkpoint_mgr.print_summary()
            
            logger.info("="*80)
            logger.info("CUSTOM MIGRATION COMPLETED")
            logger.info(f"Total Users: {summary['total_users']}")
            logger.info(f"Completed Users: {summary['completed_users']}")
            logger.info(f"Files Migrated: {summary['total_files_migrated']}")
            logger.info(f"Files Failed: {summary['total_files_failed']}")
            logger.info(f"Report: {report_file}")
            if checkpoint_mgr:
                logger.info(f"Checkpoint: {checkpoint_mgr.checkpoint_file}")
            logger.info("="*80)
            
            return True
            
        except Exception as e:
            logger.error(f"Custom migration failed: {e}", exc_info=True)
            state_mgr.end_migration_run(run_id, 'failed', {})
            
            # Print checkpoint summary even on failure
            if checkpoint_mgr:
                checkpoint_mgr.print_summary()
            
            return False


def full_migration_mode(auth_manager, max_workers, args):
    """Full domain migration with checkpoint support"""
    global logger
    
    # Initialize checkpoint manager
    checkpoint_mgr = initialize_checkpoint_manager(args)
    
    # Get services
    source_services = auth_manager.get_source_services()
    dest_services = auth_manager.get_dest_services()
    
    # Initialize managers
    user_mgr = UserManager(
        source_services['admin'],
        dest_services['admin'],
        Config.SOURCE_DOMAIN,
        Config.DEST_DOMAIN
    )
    
    # Get users
    logger.info("Fetching users from both domains...")
    source_users = user_mgr.get_source_users(
        filter_suspended=args.filter_suspended,
        filter_archived=args.filter_archived
    )
    dest_users = user_mgr.get_dest_users()
    
    # Create mapping
    user_mapping = user_mgr.create_user_mapping(source_users, dest_users)
    
    if not user_mapping:
        logger.error("No users to migrate!")
        return False
    
    # Initialize state manager
    state_db = Config.STATE_DB_FILE if not args.no_resume else f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    
    with StateManager(state_db) as state_mgr:
        # Add users to state
        for src, dst in user_mapping.items():
            state_mgr.add_user(src, dst)
        
        # Initialize drive operations
        source_drive_ops = DriveOperations(source_services['drive'])
        dest_drive_ops = DriveOperations(dest_services['drive'])
        
        # Initialize migration engine with checkpoint manager
        migration_engine = MigrationEngine(
            source_drive_ops,
            dest_drive_ops,
            Config,
            state_mgr,
            checkpoint_manager=checkpoint_mgr
        )
        
        # Start migration run
        run_id = state_mgr.start_migration_run({
            'source_domain': Config.SOURCE_DOMAIN,
            'dest_domain': Config.DEST_DOMAIN,
            'total_users': len(user_mapping),
            'max_workers': max_workers,
            'checkpoint_enabled': checkpoint_mgr is not None,
            'checkpoint_file': checkpoint_mgr.checkpoint_file if checkpoint_mgr else None
        })
        
        logger.info(f"Starting migration run ID: {run_id}")
        
        # Execute migration
        try:
            summary = migration_engine.migrate_domain(user_mapping, max_workers)
            
            # Generate reports
            report_file = Config.REPORT_DIR / f'migration_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            migration_engine.generate_report(summary, str(report_file))
            
            # Update state
            state_mgr.end_migration_run(run_id, 'completed', summary)
            
            # Print checkpoint summary if enabled
            if checkpoint_mgr:
                checkpoint_mgr.print_summary()
            
            logger.info(f"Migration completed! Report: {report_file}")
            if checkpoint_mgr:
                logger.info(f"Checkpoint: {checkpoint_mgr.checkpoint_file}")
            
            return True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}", exc_info=True)
            state_mgr.end_migration_run(run_id, 'failed', {})
            
            # Print checkpoint summary even on failure
            if checkpoint_mgr:
                checkpoint_mgr.print_summary()
            
            return False


def resume_migration_mode(auth_manager, max_workers, args):
    """Resume migration from checkpoint"""
    global logger
    logger.info("="*80)
    logger.info("RESUME MODE - Continuing from checkpoint")
    logger.info("="*80)
    
    # Initialize checkpoint manager (will auto-find latest or use specified)
    checkpoint_mgr = initialize_checkpoint_manager(args)
    
    if not checkpoint_mgr:
        logger.error("No checkpoint found to resume from!")
        logger.info("Use --checkpoint-file to specify a checkpoint, or start a new migration")
        return False
    
    if not checkpoint_mgr.is_resuming:
        logger.warning("No previous checkpoint found, starting fresh migration")
        return full_migration_mode(auth_manager, max_workers, args)
    
    # Get checkpoint summary
    summary = checkpoint_mgr.get_checkpoint_summary()
    
    logger.info(f"Resuming from: {checkpoint_mgr.checkpoint_file}")
    logger.info(f"Progress: {summary['completion_percentage']:.2f}% complete")
    logger.info(f"Pending: {summary['status_breakdown']['pending']} items")
    logger.info(f"Failed: {summary['status_breakdown']['failed']} items (will retry)")
    logger.info(f"Done: {summary['status_breakdown']['done']} items (will skip)")
    
    # Continue with full migration (will use checkpoint)
    return full_migration_mode(auth_manager, max_workers, args)


def report_mode():
    """Generate report from existing state"""
    global logger
    
    if not Path(Config.STATE_DB_FILE).exists():
        logger.error("No migration state found!")
        return False
    
    with StateManager(Config.STATE_DB_FILE) as state_mgr:
        report_file = Config.REPORT_DIR / f'state_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        state_mgr.export_state_report(str(report_file))
        
        progress = state_mgr.get_overall_progress()
        
        logger.info("="*80)
        logger.info("MIGRATION STATE REPORT")
        logger.info(f"Total Users: {progress.get('total_users', 0)}")
        logger.info(f"Completed Users: {progress.get('completed_users', 0)}")
        logger.info(f"Failed Users: {progress.get('failed_users', 0)}")
        logger.info(f"Total Files: {progress.get('total_files', 0)}")
        logger.info(f"Completed Files: {progress.get('completed_files', 0)}")
        logger.info(f"Failed Files: {progress.get('failed_files', 0)}")
        logger.info(f"Report saved: {report_file}")
        logger.info("="*80)
        
        return True


def validate_mode(auth_manager):
    """Validate setup and test connections"""
    global logger
    logger.info("="*80)
    logger.info("VALIDATION MODE")
    logger.info("="*80)
    
    # Test authentication
    logger.info("Testing authentication...")
    auth_manager.authenticate_all()
    
    # Test API connections
    logger.info("Testing API connections...")
    if auth_manager.test_connection():
        logger.info("✓ All connections successful")
    else:
        logger.error("✗ Connection test failed")
        return False
    
    # Test permissions
    logger.info("Validating permissions...")
    source_services = auth_manager.get_source_services()
    dest_services = auth_manager.get_dest_services()
    
    try:
        # Test source admin access
        source_services['admin'].users().list(
            domain=Config.SOURCE_DOMAIN,
            maxResults=1
        ).execute()
        logger.info("✓ Source admin access OK")
        
        # Test destination admin access
        dest_services['admin'].users().list(
            domain=Config.DEST_DOMAIN,
            maxResults=1
        ).execute()
        logger.info("✓ Destination admin access OK")
        
        logger.info("="*80)
        logger.info("✓ VALIDATION SUCCESSFUL")
        logger.info("="*80)
        return True
        
    except Exception as e:
        logger.error(f"✗ Permission validation failed: {e}")
        return False


def main():
    """Main application entry point"""
    global logger
    
    # Parse arguments
    args = parse_arguments()
    
    # Setup logging
    log_file = Config.REPORT_DIR / Config.LOG_FILE
    setup_logging(args.log_level, str(log_file))
    logger = create_logger(__name__)
    
    logger.info("="*80)
    logger.info("Google Workspace Drive Migration Tool with Checkpoint System")
    logger.info(f"Mode: {args.mode}")
    logger.info("="*80)
    
    # Validate setup
    if not validate_setup():
        sys.exit(1)
    
    # Handle checkpoint-only modes (no authentication needed)
    if args.mode == 'checkpoint-list':
        success = checkpoint_list_mode()
        sys.exit(0 if success else 1)
    
    if args.mode == 'checkpoint-cleanup':
        success = checkpoint_cleanup_mode(args.cleanup_days, dry_run=False)
        sys.exit(0 if success else 1)
    
    if args.mode == 'checkpoint-report':
        success = checkpoint_report_mode(args.checkpoint_file)
        sys.exit(0 if success else 1)
    
    # Report mode doesn't need authentication
    if args.mode == 'report':
        success = report_mode()
        sys.exit(0 if success else 1)
    
    # Initialize authentication
    logger.info("Initializing authentication...")
    auth_manager = DomainAuthManager(
        source_config={
            'domain': Config.SOURCE_DOMAIN,
            'credentials_file': Config.SOURCE_CREDENTIALS_FILE,
            'admin_email': Config.SOURCE_ADMIN_EMAIL
        },
        dest_config={
            'domain': Config.DEST_DOMAIN,
            'credentials_file': Config.DEST_CREDENTIALS_FILE,
            'admin_email': Config.DEST_ADMIN_EMAIL
        },
        scopes=Config.SCOPES
    )
    
    try:
        auth_manager.authenticate_all()
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        sys.exit(1)
    
    # Execute based on mode
    success = False
    
    if args.mode == 'validate':
        success = validate_mode(auth_manager)
    
    elif args.mode == 'dry-run':
        user_mapping = dry_run_mode(auth_manager)
        success = user_mapping is not None
    
    elif args.mode == 'full':
        success = full_migration_mode(auth_manager, args.max_workers, args)
    
    elif args.mode == 'resume':
        success = resume_migration_mode(auth_manager, args.max_workers, args)
    
    elif args.mode == 'custom':
        if not args.user_mapping or not Path(args.user_mapping).exists():
            logger.error("User mapping file required for custom mode!")
            sys.exit(1)
        
        success = custom_migration_mode(auth_manager, args.user_mapping, args.max_workers, args)
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()