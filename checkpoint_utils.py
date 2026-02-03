"""
Checkpoint Utilities for Google Workspace Drive Migration
Provides helper functions for managing, cleaning, and reporting on checkpoints
"""

import csv
import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


def list_all_checkpoints(checkpoint_folder: str = None) -> List[Dict]:
    """
    List all checkpoint files in folder
    
    Args:
        checkpoint_folder: Checkpoint folder path (default: './migration_checkpoints')
        
    Returns:
        List of checkpoint file information dictionaries
        
    Example:
        checkpoints = list_all_checkpoints('./migration_checkpoints')
        for cp in checkpoints:
            print(f"{cp['filename']} - {cp['modified']} - {cp['size_mb']:.2f} MB")
    """
    folder = checkpoint_folder or './migration_checkpoints'
    
    if not os.path.exists(folder):
        logger.warning(f"Checkpoint folder does not exist: {folder}")
        return []
    
    checkpoints = []
    
    for filename in os.listdir(folder):
        if filename.startswith('drive_migration_checkpoint_') and filename.endswith('.csv'):
            full_path = os.path.join(folder, filename)
            
            # Parse filename to extract metadata
            # Expected format: drive_migration_checkpoint_{source}_to_{dest}_{date}_{time}.csv
            parts = filename.replace('drive_migration_checkpoint_', '').replace('.csv', '').split('_to_')
            
            if len(parts) == 2:
                source_domain_part = parts[0]
                dest_and_timestamp = parts[1].rsplit('_', 2)
                
                if len(dest_and_timestamp) >= 3:
                    dest_domain_part = '_'.join(dest_and_timestamp[:-2])
                    date_part = dest_and_timestamp[-2]
                    time_part = dest_and_timestamp[-1]
                    
                    # Get file metadata
                    mtime = os.path.getmtime(full_path)
                    size = os.path.getsize(full_path)
                    
                    checkpoints.append({
                        'filename': filename,
                        'full_path': full_path,
                        'source_domain': source_domain_part.replace('_', '.'),
                        'dest_domain': dest_domain_part.replace('_', '.'),
                        'timestamp': f"{date_part}_{time_part}",
                        'modified': datetime.fromtimestamp(mtime),
                        'size_bytes': size,
                        'size_mb': size / (1024 * 1024)
                    })
    
    # Sort by modification time, newest first
    checkpoints.sort(key=lambda x: x['modified'], reverse=True)
    
    return checkpoints


def cleanup_old_checkpoints(
    checkpoint_folder: str = None, 
    days_old: int = 30, 
    dry_run: bool = True
) -> Dict:
    """
    Clean up old checkpoint files
    
    Args:
        checkpoint_folder: Checkpoint folder path (default: './migration_checkpoints')
        days_old: Delete files older than this many days (default: 30)
        dry_run: If True, only show what would be deleted (default: True)
        
    Returns:
        Dictionary with cleanup statistics
        
    Example:
        # Dry run to see what would be deleted
        stats = cleanup_old_checkpoints(days_old=30, dry_run=True)
        print(f"Would delete {stats['to_delete']} checkpoints")
        
        # Actually delete
        stats = cleanup_old_checkpoints(days_old=30, dry_run=False)
        print(f"Deleted {len(stats['deleted_files'])} checkpoints")
    """
    folder = checkpoint_folder or './migration_checkpoints'
    
    checkpoints = list_all_checkpoints(folder)
    
    cutoff_date = datetime.now() - timedelta(days=days_old)
    
    to_delete = [c for c in checkpoints if c['modified'] < cutoff_date]
    
    stats = {
        'total_checkpoints': len(checkpoints),
        'to_delete': len(to_delete),
        'kept': len(checkpoints) - len(to_delete),
        'dry_run': dry_run,
        'deleted_files': []
    }
    
    if not dry_run:
        for checkpoint in to_delete:
            try:
                os.remove(checkpoint['full_path'])
                stats['deleted_files'].append(checkpoint['filename'])
                logger.info(f"Deleted old checkpoint: {checkpoint['filename']}")
            except Exception as e:
                logger.error(f"Failed to delete {checkpoint['filename']}: {e}")
    else:
        stats['would_delete'] = [c['filename'] for c in to_delete]
        logger.info(f"Dry run: Would delete {len(to_delete)} checkpoints")
    
    return stats


def export_checkpoint_summary(checkpoint_file: str, output_file: str = None):
    """
    Export checkpoint summary to JSON
    
    Args:
        checkpoint_file: Path to checkpoint CSV
        output_file: Output JSON file path (optional, will auto-generate if not provided)
        
    Example:
        export_checkpoint_summary(
            'migration_checkpoints/drive_migration_checkpoint_dev_to_demo_20250120_143000.csv'
        )
        # Creates: drive_migration_checkpoint_dev_to_demo_20250120_143000_summary.json
    """
    import json
    
    # Parse domain info from filename
    filename = os.path.basename(checkpoint_file)
    parts = filename.replace('drive_migration_checkpoint_', '').replace('.csv', '').split('_to_')
    
    if len(parts) != 2:
        logger.error("Invalid checkpoint filename format")
        print(f"❌ Error: Invalid checkpoint filename format: {filename}")
        return
    
    source_domain = parts[0].replace('_', '.')
    dest_domain_parts = parts[1].rsplit('_', 2)
    dest_domain = '_'.join(dest_domain_parts[:-2]).replace('_', '.')
    
    # Import checkpoint manager
    try:
        from checkpoint_manager import CheckpointManager
    except ImportError:
        logger.error("CheckpointManager not found - cannot load checkpoint")
        print("❌ Error: checkpoint_manager module not found")
        return
    
    # Load checkpoint
    checkpoint = CheckpointManager(
        source_domain=source_domain,
        dest_domain=dest_domain,
        resume_from=checkpoint_file
    )
    
    summary = checkpoint.get_checkpoint_summary()
    
    # Add file information
    summary['checkpoint_filename'] = filename
    summary['checkpoint_size_mb'] = os.path.getsize(checkpoint_file) / (1024 * 1024)
    
    # Determine output file
    if not output_file:
        output_file = checkpoint_file.replace('.csv', '_summary.json')
    
    # Write JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"Checkpoint summary exported to: {output_file}")
    print(f"\n✓ Summary exported to: {output_file}")


def get_checkpoint_progress(checkpoint_file: str) -> Dict:
    """
    Get detailed progress information from checkpoint
    
    Args:
        checkpoint_file: Path to checkpoint CSV file
        
    Returns:
        Dictionary with progress statistics
    """
    try:
        from checkpoint_manager import CheckpointManager, MigrationStatus
    except ImportError:
        logger.error("checkpoint_manager module not found")
        return {
            'error': 'checkpoint_manager module not found',
            'total_items': 0,
            'completed': 0,
            'pending': 0,
            'failed': 0,
            'completion_percentage': 0.0
        }
    
    if not os.path.exists(checkpoint_file):
        logger.error(f"Checkpoint file not found: {checkpoint_file}")
        return {
            'error': f'File not found: {checkpoint_file}',
            'total_items': 0,
            'completed': 0,
            'pending': 0,
            'failed': 0,
            'completion_percentage': 0.0
        }
    
    # Parse domain from filename
    filename = os.path.basename(checkpoint_file)
    parts = filename.replace('drive_migration_checkpoint_', '').replace('.csv', '').split('_to_')
    
    if len(parts) != 2:
        return {'error': 'Invalid filename format'}
    
    source_domain = parts[0].replace('_', '.')
    dest_domain_parts = parts[1].rsplit('_', 2)
    dest_domain = '_'.join(dest_domain_parts[:-2]).replace('_', '.')
    
    # Load checkpoint
    checkpoint = CheckpointManager(
        source_domain=source_domain,
        dest_domain=dest_domain,
        resume_from=checkpoint_file
    )
    
    summary = checkpoint.get_checkpoint_summary()
    
    # Extract stats
    status = summary['status_breakdown']
    total_processable = summary['total_items'] - status['ignored']
    completed = status['done'] + status['skipped']
    completion_pct = (completed / total_processable * 100) if total_processable > 0 else 0.0
    
    progress = {
        'checkpoint_file': checkpoint_file,
        'total_items': summary['total_items'],
        'total_processable': total_processable,
        'folders': summary['folders'],
        'files': summary['files'],
        'pending': status['pending'],
        'in_progress': status['in_progress'],
        'done': status['done'],
        'failed': status['failed'],
        'skipped': status['skipped'],
        'ignored': status['ignored'],
        'completed': completed,
        'completion_percentage': completion_pct
    }
    
    return progress


def validate_checkpoint_integrity(checkpoint_file: str) -> bool:
    """
    Validate checkpoint file integrity
    
    Args:
        checkpoint_file: Path to checkpoint CSV file
        
    Returns:
        True if valid, False otherwise
    """
    try:
        from checkpoint_manager import CheckpointManager, CheckpointRecord
    except ImportError:
        logger.error("checkpoint_manager module not found")
        print("❌ Error: checkpoint_manager module not found")
        return False
    
    if not os.path.exists(checkpoint_file):
        logger.error(f"Checkpoint file not found: {checkpoint_file}")
        print(f"❌ Error: File not found: {checkpoint_file}")
        return False
    
    print(f"\n🔍 Validating checkpoint: {os.path.basename(checkpoint_file)}")
    
    try:
        # Parse domain from filename
        filename = os.path.basename(checkpoint_file)
        parts = filename.replace('drive_migration_checkpoint_', '').replace('.csv', '').split('_to_')
        
        if len(parts) != 2:
            print("❌ Invalid filename format")
            return False
        
        source_domain = parts[0].replace('_', '.')
        dest_domain_parts = parts[1].rsplit('_', 2)
        dest_domain = '_'.join(dest_domain_parts[:-2]).replace('_', '.')
        
        # Load checkpoint
        checkpoint = CheckpointManager(
            source_domain=source_domain,
            dest_domain=dest_domain,
            resume_from=checkpoint_file
        )
        
        # Get all records from cache
        records = list(checkpoint._cache.values())
        
        if not records:
            print("⚠️  Warning: Checkpoint is empty")
            return True
        
        # Check for duplicates
        file_ids = set()
        duplicates = []
        
        for record in records:
            if record.file_id in file_ids:
                duplicates.append(record.file_id)
            else:
                file_ids.add(record.file_id)
        
        if duplicates:
            print(f"❌ Found {len(duplicates)} duplicate file IDs:")
            for dup in duplicates[:5]:
                print(f"   - {dup}")
            if len(duplicates) > 5:
                print(f"   ... and {len(duplicates) - 5} more")
            return False
        
        # Check for invalid statuses
        valid_statuses = {'PENDING', 'IN_PROGRESS', 'DONE', 'FAILED', 'SKIPPED', 'IGNORED'}
        invalid_statuses = []
        
        for record in records:
            if record.status not in valid_statuses:
                invalid_statuses.append((record.file_name, record.status))
        
        if invalid_statuses:
            print(f"❌ Found {len(invalid_statuses)} invalid statuses:")
            for fname, status in invalid_statuses[:5]:
                print(f"   - {fname}: {status}")
            return False
        
        # Check for orphaned IN_PROGRESS items
        in_progress = [r for r in records if r.status == 'IN_PROGRESS']
        if in_progress:
            print(f"⚠️  Warning: Found {len(in_progress)} items stuck in IN_PROGRESS state")
            print(f"   (These will be retried on resume)")
        
        print(f"✓ Checkpoint is valid")
        print(f"  Total records: {len(records)}")
        print(f"  Unique file IDs: {len(file_ids)}")
        print(f"  IN_PROGRESS items: {len(in_progress)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Validation failed: {e}", exc_info=True)
        print(f"❌ Validation failed: {e}")
        return False


def merge_checkpoints(checkpoint_files: List[str], output_file: str):
    """
    Merge multiple checkpoint files into one
    
    Args:
        checkpoint_files: List of checkpoint file paths to merge
        output_file: Output merged checkpoint file path
    """
    try:
        from checkpoint_manager import CheckpointManager, CheckpointRecord
    except ImportError:
        logger.error("checkpoint_manager module not found")
        print("❌ Error: checkpoint_manager module not found")
        return
    
    print(f"\n🔄 Merging {len(checkpoint_files)} checkpoint files...")
    
    # Collect all records
    all_records = {}  # Use dict to handle duplicates
    
    for cp_file in checkpoint_files:
        if not os.path.exists(cp_file):
            print(f"⚠️  Warning: Skipping non-existent file: {cp_file}")
            continue
        
        print(f"   Reading: {os.path.basename(cp_file)}")
        
        # Parse and load checkpoint
        filename = os.path.basename(cp_file)
        parts = filename.replace('drive_migration_checkpoint_', '').replace('.csv', '').split('_to_')
        
        if len(parts) != 2:
            print(f"⚠️  Warning: Invalid filename format: {cp_file}")
            continue
        
        source_domain = parts[0].replace('_', '.')
        dest_domain_parts = parts[1].rsplit('_', 2)
        dest_domain = '_'.join(dest_domain_parts[:-2]).replace('_', '.')
        
        checkpoint = CheckpointManager(
            source_domain=source_domain,
            dest_domain=dest_domain,
            resume_from=cp_file
        )
        
        records = list(checkpoint._cache.values())
        
        for record in records:
            file_id = record.file_id
            
            # Keep most recent status (prioritize DONE > IN_PROGRESS > PENDING > FAILED)
            if file_id in all_records:
                existing = all_records[file_id]
                
                # Priority: DONE > SKIPPED > IN_PROGRESS > FAILED > PENDING > IGNORED
                priority = {
                    'DONE': 6,
                    'SKIPPED': 5,
                    'IN_PROGRESS': 4,
                    'FAILED': 3,
                    'PENDING': 2,
                    'IGNORED': 1
                }
                
                existing_priority = priority.get(existing.status, 0)
                new_priority = priority.get(record.status, 0)
                
                if new_priority > existing_priority:
                    all_records[file_id] = record
            else:
                all_records[file_id] = record
    
    # Write merged checkpoint
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'source_domain', 'destination_domain', 'source_user_email', 
            'destination_user_email', 'file_id', 'file_name', 'parent_id',
            'mime_type', 'is_folder', 'status', 'error_message', 
            'migrated_at', 'file_size', 'retry_count', 'dest_folder_id'
        ]
        
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for record in all_records.values():
            writer.writerow(record.to_dict())
    
    print(f"\n✓ Merged checkpoint created: {output_file}")
    print(f"  Total unique items: {len(all_records)}")
    
    # Show stats
    status_counts = {}
    for record in all_records.values():
        status_counts[record.status] = status_counts.get(record.status, 0) + 1
    
    print(f"\nMerged Statistics:")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")


def compare_checkpoints(checkpoint1: str, checkpoint2: str):
    """
    Compare two checkpoint files
    
    Args:
        checkpoint1: First checkpoint file path
        checkpoint2: Second checkpoint file path
    """
    try:
        from checkpoint_manager import CheckpointManager
    except ImportError:
        logger.error("checkpoint_manager module not found")
        print("❌ Error: checkpoint_manager module not found")
        return
    
    if not os.path.exists(checkpoint1):
        print(f"❌ Error: File not found: {checkpoint1}")
        return
    
    if not os.path.exists(checkpoint2):
        print(f"❌ Error: File not found: {checkpoint2}")
        return
    
    print(f"\n📊 Comparing Checkpoints:")
    print(f"   Checkpoint 1: {os.path.basename(checkpoint1)}")
    print(f"   Checkpoint 2: {os.path.basename(checkpoint2)}")
    print()
    
    # Load both checkpoints
    def load_checkpoint(cp_file):
        filename = os.path.basename(cp_file)
        parts = filename.replace('drive_migration_checkpoint_', '').replace('.csv', '').split('_to_')
        source_domain = parts[0].replace('_', '.')
        dest_domain_parts = parts[1].rsplit('_', 2)
        dest_domain = '_'.join(dest_domain_parts[:-2]).replace('_', '.')
        
        return CheckpointManager(
            source_domain=source_domain,
            dest_domain=dest_domain,
            resume_from=cp_file
        )
    
    cp1 = load_checkpoint(checkpoint1)
    cp2 = load_checkpoint(checkpoint2)
    
    summary1 = cp1.get_checkpoint_summary()
    summary2 = cp2.get_checkpoint_summary()
    
    # Compare totals
    print("="*70)
    print(f"{'Metric':<20} {'Checkpoint 1':>20} {'Checkpoint 2':>20} {'Diff':>10}")
    print("="*70)
    
    stats1 = summary1['status_breakdown']
    stats2 = summary2['status_breakdown']
    
    metrics = [
        ('Total Items', summary1['total_items'], summary2['total_items']),
        ('Done', stats1['done'], stats2['done']),
        ('Pending', stats1['pending'], stats2['pending']),
        ('Failed', stats1['failed'], stats2['failed']),
        ('In Progress', stats1['in_progress'], stats2['in_progress']),
        ('Ignored', stats1['ignored'], stats2['ignored']),
        ('Folders', summary1['folders'], summary2['folders']),
        ('Files', summary1['files'], summary2['files'])
    ]
    
    for metric, val1, val2 in metrics:
        diff = val2 - val1
        diff_str = f"+{diff}" if diff > 0 else str(diff)
        print(f"{metric:<20} {val1:>20,} {val2:>20,} {diff_str:>10}")
    
    print("="*70)


def generate_checkpoint_report(checkpoint_file: str):
    """
    Generate human-readable checkpoint report
    
    Args:
        checkpoint_file: Path to checkpoint CSV
        
    Example:
        generate_checkpoint_report('migration_checkpoints/checkpoint_20250120.csv')
    """
    try:
        from checkpoint_manager import CheckpointManager
    except ImportError:
        logger.error("checkpoint_manager module not found")
        print("❌ Error: checkpoint_manager module not found")
        return
    
    # Parse domain from filename
    filename = os.path.basename(checkpoint_file)
    parts = filename.replace('drive_migration_checkpoint_', '').replace('.csv', '').split('_to_')
    
    if len(parts) != 2:
        print("❌ Invalid checkpoint filename format")
        return
    
    source_domain = parts[0].replace('_', '.')
    dest_domain_parts = parts[1].rsplit('_', 2)
    dest_domain = '_'.join(dest_domain_parts[:-2]).replace('_', '.')
    
    # Load checkpoint
    checkpoint = CheckpointManager(
        source_domain=source_domain,
        dest_domain=dest_domain,
        resume_from=checkpoint_file
    )
    
    # Print detailed summary
    checkpoint.print_summary()
    
    # Group by user
    by_user = {}
    for record in checkpoint._cache.values():
        user = record.source_user_email
        if user not in by_user:
            by_user[user] = {
                'total': 0,
                'done': 0,
                'failed': 0,
                'pending': 0,
                'ignored': 0
            }
        
        by_user[user]['total'] += 1
        status = record.status.lower()
        if status in by_user[user]:
            by_user[user][status] += 1
    
    print("\n" + "-"*80)
    print("PER-USER BREAKDOWN")
    print("-"*80)
    
    for user, user_stats in sorted(by_user.items()):
        completion = 0
        if user_stats['total'] > 0:
            completion = (
                (user_stats['done'] + user_stats['ignored']) / user_stats['total']
            ) * 100
        
        print(f"\n📧 {user}")
        print(f"   Total: {user_stats['total']} | Done: {user_stats['done']} | "
              f"Failed: {user_stats['failed']} | Pending: {user_stats['pending']}")
        print(f"   Completion: {completion:.1f}%")
    
    print("\n" + "="*80 + "\n")


def get_latest_checkpoint(
    checkpoint_folder: str = None,
    source_domain: str = None,
    dest_domain: str = None
) -> Optional[str]:
    """
    Get the path to the latest checkpoint file
    
    Args:
        checkpoint_folder: Checkpoint folder path
        source_domain: Filter by source domain (optional)
        dest_domain: Filter by destination domain (optional)
        
    Returns:
        Path to latest checkpoint file or None
    """
    checkpoints = list_all_checkpoints(checkpoint_folder)
    
    # Filter by domains if provided
    if source_domain:
        checkpoints = [c for c in checkpoints if c['source_domain'] == source_domain]
    
    if dest_domain:
        checkpoints = [c for c in checkpoints if c['dest_domain'] == dest_domain]
    
    if checkpoints:
        return checkpoints[0]['full_path']  # Already sorted by newest first
    
    return None