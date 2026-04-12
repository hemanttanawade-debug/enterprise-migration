"""
main.py  (v2 – Cloud SQL + GCS architecture)

PATCH CHANGES vs v1:
  1. Replaced CSV CheckpointManager / StateManager (SQLite) with SQLStateManager
  2. Replaced old MigrationEngine constructor with patched constructor
     (source_auth, dest_auth, config, checkpoint, gcs_helper, run_id, get_conn)
  3. Added '--mode shared-drives' for Shared Drive migration
  4. Added '--skip-preflight' flag to bypass pre-migration phase
  5. Pre-migration phase (PreMigrationPhase) wired into:
       - full / custom  → my_drive mode
       - shared-drives  → shared_drive mode
  6. Removed interactive resume prompt
  7. report_mode() queries SQL via SQLStateManager.get_checkpoint_summary()
  8. All modes share _build_sql_manager() helper
  9. DomainAuthManager wiring unchanged
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict

from config import Config
from auth import DomainAuthManager
from users import UserManager
from logging_config import setup_logging, create_logger
from sql_state_manager import SQLStateManager

logger = None


# ─────────────────────────────────────────────────────────────────────────────
# Argument parsing
# ─────────────────────────────────────────────────────────────────────────────

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Google Workspace Drive Migration Tool (Cloud SQL + GCS)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full My Drive domain migration
  python main.py --mode full --max-workers 5

  # Migrate specific users from CSV
  python main.py --mode custom --user-mapping users.csv

  # Skip pre-migration preflight scan
  python main.py --mode custom --user-mapping users.csv --skip-preflight

  # Resume My Drive migration (picks up pending/failed items from SQL)
  python main.py --mode resume --migration-id <id>

  # Migrate all Shared Drives
  python main.py --mode shared-drives

  # Migrate specific Shared Drives by name
  python main.py --mode shared-drives --drive-filter "Sales,Marketing"

  # Dry run (list users only, no migration)
  python main.py --mode dry-run

  # Validate authentication and API access
  python main.py --mode validate

  # Print SQL migration progress report
  python main.py --mode report --migration-id abc-123
        """,
    )

    parser.add_argument(
        "--mode",
        choices=[
            "full", "dry-run", "custom", "resume",
            "shared-drives", "report", "validate",
        ],
        default="full",
        help="Migration mode",
    )

    parser.add_argument(
        "--user-mapping",
        type=str,
        help="CSV file with user mapping (source,destination)",
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=Config.MAX_WORKERS,
        help=f"Parallel user workers (default: {Config.MAX_WORKERS})",
    )

    parser.add_argument(
        "--parallel-files",
        type=int,
        default=getattr(Config, "PARALLEL_FILES", 5),
        help="Parallel file workers per user/drive (default: 5)",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=Config.LOG_LEVEL,
    )

    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh migration (use a new migration_id)",
    )

    parser.add_argument(
        "--migration-id",
        type=str,
        default=None,
        help="Resume a specific migration_id from SQL (default: latest)",
    )

    parser.add_argument(
        "--drive-filter",
        type=str,
        default=None,
        help="Comma-separated Shared Drive names to migrate (default: all)",
    )

    parser.add_argument(
        "--filter-suspended",
        action="store_true",
        default=True,
        help="Filter out suspended users (default: True)",
    )

    parser.add_argument(
        "--filter-archived",
        action="store_true",
        default=True,
        help="Filter out archived users (default: True)",
    )

    # PATCH: new flag to skip pre-migration preflight
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        default=False,
        help="Skip pre-migration phase (connection test, discovery, estimation)",
    )

    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _build_sql_manager(migration_id: str = None) -> SQLStateManager:
    """Create SQLStateManager for My Drive migrations (no drive IDs → MY_DRIVE mode)."""
    db_cfg = {
        "host":     Config.DB_HOST,
        "port":     Config.DB_PORT,
        "database": Config.DB_NAME,
        "user":     Config.DB_USER,
        "password": Config.DB_PASSWORD,
    }
    return SQLStateManager(
        db_config=db_cfg,
        gcs_bucket=Config.GCS_BUCKET_NAME,
        gcs_key_file=Config.GCS_SERVICE_ACCOUNT_FILE,
        source_domain=Config.SOURCE_DOMAIN,
        dest_domain=Config.DEST_DOMAIN,
        migration_id=migration_id,
        gcs_prefix=Config.GCS_STAGING_PREFIX,
        large_file_threshold_mb=50,
    )


def _build_auth_manager() -> DomainAuthManager:
    return DomainAuthManager(
        source_config={
            "domain":           Config.SOURCE_DOMAIN,
            "credentials_file": Config.SOURCE_CREDENTIALS_FILE,
            "admin_email":      Config.SOURCE_ADMIN_EMAIL,
        },
        dest_config={
            "domain":           Config.DEST_DOMAIN,
            "credentials_file": Config.DEST_CREDENTIALS_FILE,
            "admin_email":      Config.DEST_ADMIN_EMAIL,
        },
        scopes=Config.SCOPES,
    )


def _build_migration_engine(auth_manager, mgr: SQLStateManager, run_id: str):
    """
    Instantiate patched MigrationEngine.
    PATCH: constructor takes source_auth/dest_auth instead of drive_ops.
    """
    from migration_engine import MigrationEngine
    return MigrationEngine(
        source_auth = auth_manager.source_auth,
        dest_auth   = auth_manager.dest_auth,
        config      = Config,
        checkpoint  = mgr,          # SQLStateManager (checkpoint role)
        gcs_helper  = mgr,          # SQLStateManager (GCS role)
        run_id      = run_id,
        get_conn    = mgr.get_conn,
    )


def _get_user_mapping(auth_manager, mapping_file=None,
                      filter_suspended=True, filter_archived=True):
    """
    Returns (user_mapping, drive_mapping, user_mgr).
    Calls import_mapping() so both user + shared drive rows are parsed.
    """
    src_svc  = auth_manager.get_source_services()
    dst_svc  = auth_manager.get_dest_services()
    user_mgr = UserManager(
        src_svc["admin"], dst_svc["admin"],
        Config.SOURCE_DOMAIN, Config.DEST_DOMAIN,
    )

    drive_mapping = {}

    if mapping_file:
        result        = user_mgr.import_mapping(mapping_file)
        user_mapping  = result.user_mapping
        drive_mapping = result.drive_mapping
    else:
        src_users    = user_mgr.get_source_users(
            filter_suspended=filter_suspended,
            filter_archived=filter_archived,
        )
        dst_users    = user_mgr.get_dest_users()
        user_mapping = user_mgr.create_user_mapping(src_users, dst_users)

    return user_mapping, drive_mapping, user_mgr


def validate_setup() -> bool:
    global logger
    try:
        Config.validate()
        logger.info("✓ Configuration validated")
        return True
    except Exception as exc:
        logger.error(f"✗ Configuration validation failed: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Pre-migration phase helper
# ─────────────────────────────────────────────────────────────────────────────

def _run_preflight(
    auth_manager,
    mgr: SQLStateManager,
    run_id: str,
    mode: str,
    user_mapping: Dict[str, str] = None,
    drive_id_mapping: Dict[str, str] = None,
) -> bool:
    global logger
    try:
        from pre_migration import PreMigrationPhase

        src_svc = auth_manager.get_source_services()
        dst_svc = auth_manager.get_dest_services()

        pre = PreMigrationPhase(
            source_drive_service = src_svc["drive"],
            dest_drive_service   = dst_svc["drive"],
            admin_service        = dst_svc["admin"],
            source_domain        = Config.SOURCE_DOMAIN,
            dest_domain          = Config.DEST_DOMAIN,
            sql_manager          = mgr,
            source_auth          = auth_manager.source_auth,  # ← ADD THIS
        )

        pre_report = pre.run(
            mode             = mode,
            drive_id_mapping = drive_id_mapping,
            user_mapping     = user_mapping,
            run_id           = run_id,
        )

        if not pre_report["ready"]:
            logger.error("Pre-migration phase FAILED — aborting migration.")
            return False

        logger.info(
            f"Pre-migration phase PASSED | "
            f"Estimated migration time: {pre_report.get('total_estimated_time', 'N/A')}"
        )
        return True

    except ImportError:
        logger.warning("pre_migration.py not found — skipping preflight scan.")
        return True
    except Exception as exc:
        logger.error(f"Pre-migration phase error: {exc}", exc_info=True)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Mode: validate
# ─────────────────────────────────────────────────────────────────────────────

def validate_mode(auth_manager) -> bool:
    global logger
    logger.info("=" * 80)
    logger.info("VALIDATION MODE")
    logger.info("=" * 80)

    auth_manager.authenticate_all()

    if not auth_manager.test_connection():
        logger.error("✗ Connection test failed")
        return False
    logger.info("✓ All connections successful")

    src_svc = auth_manager.get_source_services()
    dst_svc = auth_manager.get_dest_services()

    try:
        src_svc["admin"].users().list(domain=Config.SOURCE_DOMAIN, maxResults=1).execute()
        logger.info("✓ Source admin access OK")
        dst_svc["admin"].users().list(domain=Config.DEST_DOMAIN, maxResults=1).execute()
        logger.info("✓ Destination admin access OK")
        logger.info("✓ VALIDATION SUCCESSFUL")
        return True
    except Exception as exc:
        logger.error(f"✗ Permission validation failed: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Mode: dry-run
# ─────────────────────────────────────────────────────────────────────────────

def dry_run_mode(auth_manager, args) -> bool:
    global logger
    logger.info("=" * 80)
    logger.info("DRY RUN MODE — Listing users only")
    logger.info("=" * 80)

    mapping, drive_mapping, user_mgr = _get_user_mapping(
        auth_manager,
        mapping_file=args.user_mapping,
        filter_suspended=args.filter_suspended,
        filter_archived=args.filter_archived,
    )

    mapping_file_out = (
        Config.REPORT_DIR
        / f"user_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    user_mgr.export_user_mapping(
        mapping, str(mapping_file_out),
        drive_mapping=drive_mapping if drive_mapping else None,
    )

    logger.info(f"Mapped users  : {len(mapping)}")
    if drive_mapping:
        logger.info(f"Mapped drives : {len(drive_mapping)}")
        for src_drv, dst_drv in drive_mapping.items():
            logger.info(f"  {src_drv} → {dst_drv}")
    logger.info(f"Mapping file  : {mapping_file_out}")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Mode: full / custom
# ─────────────────────────────────────────────────────────────────────────────

def full_migration_mode(auth_manager, args) -> bool:
    global logger
    logger.info("=" * 80)
    logger.info(
        "FULL MY DRIVE MIGRATION" if args.mode == "full"
        else "CUSTOM MY DRIVE MIGRATION"
    )
    logger.info("=" * 80)

    mapping, drive_mapping, user_mgr = _get_user_mapping(
        auth_manager,
        mapping_file=args.user_mapping if args.mode == "custom" else None,
        filter_suspended=args.filter_suspended,
        filter_archived=args.filter_archived,
    )

    # Auto-redirect: drive-only CSV → shared-drives mode
    if not mapping and drive_mapping:
        logger.info(
            f"No user rows found in CSV, but {len(drive_mapping)} shared drive "
            "mapping(s) detected — redirecting to shared-drives migration."
        )
        return shared_drives_mode(
            auth_manager, args,
            _preloaded_drive_mapping=drive_mapping,
        )

    if not mapping:
        logger.error("No users to migrate!")
        return False

    if args.mode == "custom":
        src_svc  = auth_manager.get_source_services()
        dst_svc  = auth_manager.get_dest_services()
        verified = {}
        for src, dst in mapping.items():
            if (user_mgr.verify_user_exists(src, src_svc["admin"])
                    and user_mgr.verify_user_exists(dst, dst_svc["admin"])):
                verified[src] = dst
                logger.info(f"✓ Verified: {src} → {dst}")
            else:
                logger.warning(f"  Skipping unverified: {src}")
        mapping = verified
        if not mapping:
            logger.error("No valid user mappings after verification!")
            return False

    if drive_mapping:
        logger.info(
            f"Note: {len(drive_mapping)} shared drive mapping(s) found in CSV. "
            "Run --mode shared-drives --user-mapping <same file> to migrate them."
        )

    migration_id = None if args.no_resume else args.migration_id
    mgr    = _build_sql_manager(migration_id=migration_id)
    run_id = mgr.create_migration_run(total_items=len(mapping))
    logger.info(f"Migration run: {run_id}")

    # ── PATCH: Pre-migration phase ────────────────────────────────────────
    if not args.skip_preflight:
        logger.info("Running pre-migration preflight scan...")
        ready = _run_preflight(
            auth_manager, mgr, run_id,
            mode         = "my_drive",
            user_mapping = mapping,
        )
        if not ready:
            mgr.finish_migration_run("FAILED")
            return False
    else:
        logger.info("Pre-migration preflight skipped (--skip-preflight)")
    # ─────────────────────────────────────────────────────────────────────

    engine = _build_migration_engine(auth_manager, mgr, run_id)
    
    try:
        summary = engine.migrate_domain(mapping, max_workers=args.max_workers)

        report_file = (
            Config.REPORT_DIR
            / f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        engine.generate_report(summary, str(report_file))
        mgr.finish_migration_run("COMPLETED")
        mgr.print_summary()

        logger.info("=" * 80)
        logger.info(f"MIGRATION COMPLETED — accuracy: {summary['accuracy_rate']:.2f}%")
        logger.info(f"Users: {summary['completed_users']}/{summary['total_users']}")
        logger.info(
            f"Files: {summary['total_files_migrated']} migrated, "
            f"{summary['total_files_failed']} failed"
        )
        logger.info(f"Report: {report_file}")
        logger.info("=" * 80)
        return True

    except Exception as exc:
        logger.error(f"Migration failed: {exc}", exc_info=True)
        mgr.finish_migration_run("FAILED")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Mode: resume
# ─────────────────────────────────────────────────────────────────────────────

def resume_migration_mode(auth_manager, args) -> bool:
    global logger
    logger.info("=" * 80)
    logger.info("RESUME MODE — continuing from SQL state")
    logger.info("=" * 80)

    if not args.migration_id:
        logger.error(
            "Resume requires --migration-id <id>. "
            "Check your migration_runs table."
        )
        return False

    mgr = _build_sql_manager(migration_id=args.migration_id)
    mgr.print_summary()

    if args.user_mapping:
        mapping, drive_mapping, user_mgr = _get_user_mapping(
            auth_manager, mapping_file=args.user_mapping
        )
        src_svc = auth_manager.get_source_services()
        dst_svc = auth_manager.get_dest_services()
        verified = {}
        for src, dst in mapping.items():
            if (user_mgr.verify_user_exists(src, src_svc["admin"])
                    and user_mgr.verify_user_exists(dst, dst_svc["admin"])):
                verified[src] = dst
        mapping = verified
        logger.info(f"Resuming {len(mapping)} users from mapping file")
    else:
        pending = mgr.get_pending_items(args.migration_id)
        if not pending:
            logger.info("No pending items — migration may already be complete.")
            mgr.print_summary()
            return True

        pairs: Dict = {}
        for item in pending:
            src = item.get("source_user_email") or item.get("source_email", "")
            dst = item.get("destination_user_email") or item.get("dest_email", "")
            if src and dst:
                pairs[src] = dst
        mapping = pairs
        logger.info(f"Auto-detected {len(mapping)} users from SQL pending items")

    if not mapping:
        logger.error("No user mapping available for resume.")
        return False

    engine = _build_migration_engine(auth_manager, mgr, args.migration_id)

    try:
        summary = engine.migrate_domain(mapping, max_workers=args.max_workers)

        report_file = (
            Config.REPORT_DIR
            / f"resume_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        engine.generate_report(summary, str(report_file))
        mgr.finish_migration_run("COMPLETED")
        mgr.print_summary()

        logger.info("=" * 80)
        logger.info("RESUME COMPLETED")
        logger.info(
            f"Files: {summary['total_files_migrated']} migrated, "
            f"{summary['total_files_failed']} failed"
        )
        logger.info(f"Report: {report_file}")
        logger.info("=" * 80)
        return True

    except Exception as exc:
        logger.error(f"Resume failed: {exc}", exc_info=True)
        mgr.finish_migration_run("FAILED")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Mode: shared-drives
# ─────────────────────────────────────────────────────────────────────────────

def shared_drives_mode(
    auth_manager,
    args,
    _preloaded_drive_mapping: dict = None,
) -> bool:
    """
    Migrate Shared Drives.

    Priority for drive mapping:
      1. Pre-loaded mapping (from full_migration_mode redirect)
      2. Drive IDs from --user-mapping CSV
      3. Drive names from --drive-filter
      4. None → migrate all Shared Drives
    """
    global logger
    logger.info("=" * 80)
    logger.info("SHARED DRIVE MIGRATION MODE")
    logger.info("=" * 80)

    from shared_drive_migrator import SharedDriveMigrator

    src_svc = auth_manager.get_source_services()
    dst_svc = auth_manager.get_dest_services()

    # Resolve drive mapping
    csv_drive_mapping: dict = _preloaded_drive_mapping or {}

    if csv_drive_mapping:
        logger.info(f"Drive mapping (pre-loaded): {len(csv_drive_mapping)} drive pair(s)")
        for src_drv, dst_drv in csv_drive_mapping.items():
            logger.info(f"  {src_drv} → {dst_drv}")

    elif args.user_mapping:
        from users import UserManager
        user_mgr = UserManager(
            src_svc["admin"], dst_svc["admin"],
            Config.SOURCE_DOMAIN, Config.DEST_DOMAIN,
        )
        result = user_mgr.import_mapping(args.user_mapping)
        csv_drive_mapping = result.drive_mapping

        if csv_drive_mapping:
            logger.info(f"Drive mapping from CSV: {len(csv_drive_mapping)} drive pair(s)")
            for src_drv, dst_drv in csv_drive_mapping.items():
                logger.info(f"  {src_drv} → {dst_drv}")
        else:
            logger.info("No drive rows found in CSV — using --drive-filter or all drives")

    drive_name_filter = None
    if not csv_drive_mapping and args.drive_filter:
        drive_name_filter = [
            d.strip() for d in args.drive_filter.split(",") if d.strip()
        ]
        logger.info(f"Drive name filter: {drive_name_filter}")

    parent_mgr = _build_sql_manager(migration_id=args.migration_id)
    run_id     = parent_mgr.create_migration_run()
    logger.info(f"Shared Drive migration run: {run_id}")

    # ── PATCH: Pre-migration phase for Shared Drives ──────────────────────
    if not args.skip_preflight and csv_drive_mapping:
        logger.info("Running pre-migration preflight scan...")
        ready = _run_preflight(
            auth_manager, parent_mgr, run_id,
            mode             = "shared_drive",
            drive_id_mapping = csv_drive_mapping,
        )
        if not ready:
            parent_mgr.finish_migration_run("FAILED")
            return False
    elif args.skip_preflight:
        logger.info("Pre-migration preflight skipped (--skip-preflight)")
    else:
        logger.info(
            "Pre-migration preflight skipped "
            "(no drive ID mapping — name-based or all-drives mode)"
        )
    # ─────────────────────────────────────────────────────────────────────

    migrator = SharedDriveMigrator(
        source_admin_drive = src_svc["drive"],
        dest_admin_drive   = dst_svc["drive"],
        source_domain      = Config.SOURCE_DOMAIN,
        dest_domain        = Config.DEST_DOMAIN,
        config             = Config,
        sql_mgr            = parent_mgr,
        run_id             = run_id,
        parallel_files     = args.parallel_files,
    )

    try:
        if csv_drive_mapping:
            summary = migrator.migrate_all_shared_drives(
                drive_filter    = None,
                drive_id_mapping = csv_drive_mapping,
            )
        else:
            summary = migrator.migrate_all_shared_drives(
                drive_filter = drive_name_filter,
            )

        parent_mgr.finish_migration_run("COMPLETED")
        parent_mgr.print_summary()

        logger.info("=" * 80)
        logger.info("SHARED DRIVE MIGRATION COMPLETED")
        logger.info(f"Drives: {summary['drives_migrated']}/{summary['total_drives']}")
        logger.info(
            f"Files:  {summary['total_files_migrated']} migrated, "
            f"{summary['total_files_failed']} failed"
        )
        logger.info(f"Members migrated: {summary['total_members_migrated']}")
        logger.info("=" * 80)

        for dr in summary["drive_results"]:
            icon = "✓" if dr["status"] == "completed" else "✗"
            logger.info(
                f"  {icon} {dr['name']} ({dr.get('source_id', '')}): "
                f"{dr['files_migrated']} files | "
                f"{dr['members_migrated']} members | "
                f"{dr['folders_created']} folders"
            )

        return True

    except Exception as exc:
        logger.error(f"Shared Drive migration failed: {exc}", exc_info=True)
        parent_mgr.finish_migration_run("FAILED")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Mode: report
# ─────────────────────────────────────────────────────────────────────────────

def report_mode(args) -> bool:
    global logger

    if not args.migration_id:
        logger.error("--migration-id required for report mode")
        return False

    mgr = _build_sql_manager(migration_id=args.migration_id)
    s   = mgr.get_checkpoint_summary()

    logger.info("=" * 80)
    logger.info("MIGRATION SQL REPORT")
    logger.info(f"  Migration ID : {s['migration_id']}")
    logger.info(f"  Mode         : {s['mode']}")
    logger.info(f"  Total        : {s['total']}")
    logger.info(f"  Done         : {s['done']}")
    logger.info(f"  Failed       : {s['failed']}")
    logger.info(f"  Pending      : {s['pending']}")
    logger.info(f"  Skipped      : {s['skipped']}")
    logger.info(f"  Ignored      : {s['ignored']}")
    logger.info(f"  In Progress  : {s['in_progress']}")
    logger.info("=" * 80)
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    global logger

    args = parse_arguments()

    log_file = Config.REPORT_DIR / Config.LOG_FILE
    setup_logging(args.log_level, str(log_file))
    logger = create_logger(__name__)

    logger.info("=" * 80)
    logger.info("Google Workspace Drive Migration Tool (v2 — Cloud SQL + GCS)")
    logger.info(f"Mode: {args.mode}")
    logger.info("=" * 80)

    if not validate_setup():
        sys.exit(1)

    # Report mode doesn't need auth
    if args.mode == "report":
        sys.exit(0 if report_mode(args) else 1)

    auth_manager = _build_auth_manager()
    try:
        auth_manager.authenticate_all()
    except Exception as exc:
        logger.error(f"Authentication failed: {exc}")
        sys.exit(1)

    success = False

    if args.mode == "validate":
        success = validate_mode(auth_manager)

    elif args.mode == "dry-run":
        success = dry_run_mode(auth_manager, args)

    elif args.mode in ("full", "custom"):
        if args.mode == "custom" and (
            not args.user_mapping or not Path(args.user_mapping).exists()
        ):
            logger.error("--user-mapping file required for custom mode")
            sys.exit(1)
        success = full_migration_mode(auth_manager, args)

    elif args.mode == "resume":
        success = resume_migration_mode(auth_manager, args)

    elif args.mode == "shared-drives":
        success = shared_drives_mode(auth_manager, args)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()