"""
Standalone CLI for Checkpoint Management
Provides command-line interface for managing migration checkpoints
"""

import argparse
import sys
import logging
from pathlib import Path

from checkpoint_manager import CheckpointManager, MigrationStatus
from checkpoint_utils import (
    list_all_checkpoints,
    cleanup_old_checkpoints,
    export_checkpoint_summary,
    generate_checkpoint_report,
    get_checkpoint_progress,
    merge_checkpoints,
    compare_checkpoints,
    validate_checkpoint_integrity
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def cmd_list(args):
    """List all checkpoints"""
    checkpoints = list_all_checkpoints(args.folder)
    
    if not checkpoints:
        print("\nNo checkpoints found\n")
        return
    
    print(f"\nFound {len(checkpoints)} checkpoint file(s):")
    print("="*110)
    print(f"{'#':<4} {'Timestamp':<18} {'Source Domain':<25} {'Dest Domain':<25} {'Size':<12} {'Modified':<20}")
    print("="*110)
    
    for i, cp in enumerate(checkpoints, 1):
        modified_str = cp['modified'].strftime('%Y-%m-%d %H:%M:%S')
        print(f"{i:<4} {cp['timestamp']:<18} {cp['source_domain']:<25} " +
              f"{cp['dest_domain']:<25} {cp['size_mb']:>8.2f} MB  {modified_str}")
    
    print("="*110 + "\n")


def cmd_inspect(args):
    """Inspect a checkpoint file"""
    checkpoint_file = args.checkpoint_file
    
    if not checkpoint_file:
        # Try to find latest
        checkpoints = list_all_checkpoints(args.folder)
        if not checkpoints:
            print("No checkpoints found")
            return
        
        checkpoint_file = checkpoints[0]['full_path']
        print(f"Using latest checkpoint: {checkpoint_file}\n")
    
    if not Path(checkpoint_file).exists():
        print(f"Checkpoint file not found: {checkpoint_file}")
        return
    
    # Generate detailed report
    generate_checkpoint_report(checkpoint_file)


def cmd_stats(args):
    """Show checkpoint statistics"""
    checkpoint_file = args.checkpoint_file
    
    if not checkpoint_file:
        checkpoints = list_all_checkpoints(args.folder)
        if not checkpoints:
            print("No checkpoints found")
            return
        
        checkpoint_file = checkpoints[0]['full_path']
        print(f"Using latest checkpoint: {checkpoint_file}\n")
    
    if not Path(checkpoint_file).exists():
        print(f"Checkpoint file not found: {checkpoint_file}")
        return
    
    # Parse domain from filename
    filename = Path(checkpoint_file).name
    parts = filename.replace('drive_migration_checkpoint_', '').replace('.csv', '').split('_to_')
    
    if len(parts) != 2:
        print("Invalid checkpoint filename format")
        return
    
    source_domain = parts[0].replace('_', '.')
    dest_domain_parts = parts[1].rsplit('_', 2)
    dest_domain = '_'.join(dest_domain_parts[:-2]).replace('_', '.')
    
    checkpoint = CheckpointManager(
        source_domain=source_domain,
        dest_domain=dest_domain,
        resume_from=checkpoint_file
    )
    
    checkpoint.print_summary()


def cmd_progress(args):
    """Show progress for checkpoint"""
    checkpoint_file = args.checkpoint_file
    
    if not checkpoint_file:
        checkpoints = list_all_checkpoints(args.folder)
        if not checkpoints:
            print("No checkpoints found")
            return
        
        checkpoint_file = checkpoints[0]['full_path']
    
    if not Path(checkpoint_file).exists():
        print(f"Checkpoint file not found: {checkpoint_file}")
        return
    
    progress = get_checkpoint_progress(checkpoint_file)
    
    print("\n" + "="*60)
    print("MIGRATION PROGRESS")
    print("="*60)
    print(f"\nCheckpoint: {Path(checkpoint_file).name}")
    print(f"\nTotal Items:      {progress['total_items']:,}")
    print(f"Processable:      {progress['total_processable']:,} (excluding ignored)")
    print(f"Completed:        {progress['completed']:,}")
    print(f"Pending:          {progress['pending']:,}")
    print(f"In Progress:      {progress['in_progress']:,}")
    print(f"Failed:           {progress['failed']:,}")
    print(f"\nFolders:          {progress['folders']:,}")
    print(f"Files:            {progress['files']:,}")
    print(f"\nCompletion:       {progress['completion_percentage']:.1f}%")
    
    # Progress bar
    bar_width = 40
    filled = int(bar_width * progress['completion_percentage'] / 100)
    bar = '█' * filled + '░' * (bar_width - filled)
    print(f"\n[{bar}] {progress['completion_percentage']:.1f}%")
    
    print("="*60 + "\n")


def cmd_clean(args):
    """Clean old checkpoint files"""
    stats = cleanup_old_checkpoints(
        checkpoint_folder=args.folder,
        days_old=args.days,
        dry_run=args.dry_run
    )
    
    print("\n" + "="*60)
    print("CHECKPOINT CLEANUP")
    print("="*60)
    print(f"\nTotal checkpoints:    {stats['total_checkpoints']}")
    print(f"Older than {args.days} days: {stats['to_delete']}")
    print(f"To keep:              {stats['kept']}")
    
    if args.dry_run:
        print("\n⚠ DRY RUN MODE - No files deleted")
        if stats.get('would_delete'):
            print("\nWould delete:")
            for filename in stats['would_delete']:
                print(f"  - {filename}")
        print("\nRun without --dry-run to actually delete files")
    else:
        if stats['deleted_files']:
            print(f"\n✓ Deleted {len(stats['deleted_files'])} file(s):")
            for filename in stats['deleted_files']:
                print(f"  - {filename}")
        else:
            print("\nNo files deleted")
    
    print("="*60 + "\n")


def cmd_export(args):
    """Export checkpoint summary to JSON"""
    checkpoint_file = args.checkpoint_file
    
    if not checkpoint_file:
        checkpoints = list_all_checkpoints(args.folder)
        if not checkpoints:
            print("No checkpoints found")
            return
        
        checkpoint_file = checkpoints[0]['full_path']
        print(f"Using latest checkpoint: {checkpoint_file}\n")
    
    if not Path(checkpoint_file).exists():
        print(f"Checkpoint file not found: {checkpoint_file}")
        return
    
    output_file = args.output or checkpoint_file.replace('.csv', '_summary.json')
    
    export_checkpoint_summary(checkpoint_file, output_file)
    print(f"\n✓ Summary exported to: {output_file}\n")


def cmd_validate(args):
    """Validate checkpoint integrity"""
    checkpoint_file = args.checkpoint_file
    
    if not checkpoint_file:
        checkpoints = list_all_checkpoints(args.folder)
        if not checkpoints:
            print("No checkpoints found")
            return
        
        checkpoint_file = checkpoints[0]['full_path']
    
    if not Path(checkpoint_file).exists():
        print(f"Checkpoint file not found: {checkpoint_file}")
        return
    
    valid = validate_checkpoint_integrity(checkpoint_file)
    
    if not valid:
        sys.exit(1)


def cmd_merge(args):
    """Merge multiple checkpoints"""
    if not args.checkpoints:
        print("No checkpoint files specified")
        print("Usage: checkpoint_cli.py merge --checkpoints file1.csv file2.csv --output merged.csv")
        return
    
    if not args.output:
        print("Output file required (--output)")
        return
    
    # Verify all input files exist
    missing = []
    for cp_file in args.checkpoints:
        if not Path(cp_file).exists():
            missing.append(cp_file)
    
    if missing:
        print(f"Checkpoint files not found:")
        for f in missing:
            print(f"  - {f}")
        return
    
    print(f"\nMerging {len(args.checkpoints)} checkpoint file(s)...\n")
    
    merge_checkpoints(args.checkpoints, args.output)


def cmd_compare(args):
    """Compare two checkpoints"""
    if not args.checkpoint1 or not args.checkpoint2:
        print("Two checkpoint files required")
        print("Usage: checkpoint_cli.py compare --checkpoint1 file1.csv --checkpoint2 file2.csv")
        return
    
    if not Path(args.checkpoint1).exists():
        print(f"Checkpoint 1 not found: {args.checkpoint1}")
        return
    
    if not Path(args.checkpoint2).exists():
        print(f"Checkpoint 2 not found: {args.checkpoint2}")
        return
    
    compare_checkpoints(args.checkpoint1, args.checkpoint2)


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description='Google Drive Migration Checkpoint Manager',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all checkpoints
  python checkpoint_cli.py list
  
  # Inspect latest checkpoint
  python checkpoint_cli.py inspect
  
  # Inspect specific checkpoint
  python checkpoint_cli.py inspect --checkpoint-file /path/to/checkpoint.csv
  
  # Show checkpoint statistics
  python checkpoint_cli.py stats
  
  # Show progress
  python checkpoint_cli.py progress
  
  # Validate checkpoint integrity
  python checkpoint_cli.py validate
  
  # Clean checkpoints older than 30 days (dry run)
  python checkpoint_cli.py clean --days 30 --dry-run
  
  # Actually delete old checkpoints
  python checkpoint_cli.py clean --days 30
  
  # Export checkpoint summary to JSON
  python checkpoint_cli.py export --output summary.json
  
  # Merge multiple checkpoints
  python checkpoint_cli.py merge --checkpoints cp1.csv cp2.csv --output merged.csv
  
  # Compare two checkpoints
  python checkpoint_cli.py compare --checkpoint1 cp1.csv --checkpoint2 cp2.csv
        """
    )
    
    parser.add_argument(
        '--folder',
        default=CheckpointManager.DEFAULT_CHECKPOINT_FOLDER,
        help='Checkpoint folder path'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # List command
    parser_list = subparsers.add_parser('list', help='List all checkpoints')
    parser_list.set_defaults(func=cmd_list)
    
    # Inspect command
    parser_inspect = subparsers.add_parser('inspect', help='Inspect checkpoint details')
    parser_inspect.add_argument('--checkpoint-file', help='Specific checkpoint file to inspect')
    parser_inspect.set_defaults(func=cmd_inspect)
    
    # Stats command
    parser_stats = subparsers.add_parser('stats', help='Show checkpoint statistics')
    parser_stats.add_argument('--checkpoint-file', help='Specific checkpoint file')
    parser_stats.set_defaults(func=cmd_stats)
    
    # Progress command
    parser_progress = subparsers.add_parser('progress', help='Show migration progress')
    parser_progress.add_argument('--checkpoint-file', help='Specific checkpoint file')
    parser_progress.set_defaults(func=cmd_progress)
    
    # Clean command
    parser_clean = subparsers.add_parser('clean', help='Clean old checkpoint files')
    parser_clean.add_argument('--days', type=int, default=30, help='Delete files older than N days')
    parser_clean.add_argument('--dry-run', action='store_true', help='Show what would be deleted')
    parser_clean.set_defaults(func=cmd_clean)
    
    # Export command
    parser_export = subparsers.add_parser('export', help='Export checkpoint summary to JSON')
    parser_export.add_argument('--checkpoint-file', help='Specific checkpoint file')
    parser_export.add_argument('--output', help='Output JSON file')
    parser_export.set_defaults(func=cmd_export)
    
    # Validate command
    parser_validate = subparsers.add_parser('validate', help='Validate checkpoint integrity')
    parser_validate.add_argument('--checkpoint-file', help='Specific checkpoint file')
    parser_validate.set_defaults(func=cmd_validate)
    
    # Merge command
    parser_merge = subparsers.add_parser('merge', help='Merge multiple checkpoints')
    parser_merge.add_argument('--checkpoints', nargs='+', required=True, help='Checkpoint files to merge')
    parser_merge.add_argument('--output', required=True, help='Output merged checkpoint file')
    parser_merge.set_defaults(func=cmd_merge)
    
    # Compare command
    parser_compare = subparsers.add_parser('compare', help='Compare two checkpoints')
    parser_compare.add_argument('--checkpoint1', required=True, help='First checkpoint file')
    parser_compare.add_argument('--checkpoint2', required=True, help='Second checkpoint file')
    parser_compare.set_defaults(func=cmd_compare)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Execute command
    try:
        args.func(args)
    except Exception as e:
        logger.error(f"Command failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()