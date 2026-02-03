"""
IMMEDIATE-UPDATE Checkpoint Manager with In-Memory Cache

KEY DESIGN:
1. All records loaded into memory cache at startup (O(1) lookups)
2. Each file/folder update writes to CSV IMMEDIATELY (no batching)
3. Cache updated in memory for fast queries
4. Simple, predictable: migrate file -> update CSV -> done

NO BATCHING - Every status change writes immediately to disk
"""

import os
import csv
import threading
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class MigrationStatus(Enum):
    """Migration status enumeration"""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    IGNORED = "IGNORED"


@dataclass
class CheckpointRecord:
    """Represents a single checkpoint record"""
    source_domain: str
    destination_domain: str
    source_user_email: str
    destination_user_email: str
    file_id: str
    file_name: str
    parent_id: str
    mime_type: str
    is_folder: bool
    status: str
    error_message: str = ""
    migrated_at: str = ""
    file_size: int = 0
    retry_count: int = 0
    dest_folder_id: str = ""
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for CSV writing"""
        return {
            'source_domain': self.source_domain,
            'destination_domain': self.destination_domain,
            'source_user_email': self.source_user_email,
            'destination_user_email': self.destination_user_email,
            'file_id': self.file_id,
            'file_name': self.file_name,
            'parent_id': self.parent_id,
            'mime_type': self.mime_type,
            'is_folder': str(self.is_folder).lower(),
            'status': self.status,
            'error_message': self.error_message,
            'migrated_at': self.migrated_at,
            'file_size': str(self.file_size),
            'retry_count': str(self.retry_count),
            'dest_folder_id': self.dest_folder_id
        }
    
    @staticmethod
    def from_dict(data: Dict) -> 'CheckpointRecord':
        """Create CheckpointRecord from dictionary"""
        return CheckpointRecord(
            source_domain=data.get('source_domain', ''),
            destination_domain=data.get('destination_domain', ''),
            source_user_email=data.get('source_user_email', ''),
            destination_user_email=data.get('destination_user_email', ''),
            file_id=data.get('file_id', ''),
            file_name=data.get('file_name', ''),
            parent_id=data.get('parent_id', ''),
            mime_type=data.get('mime_type', ''),
            is_folder=data.get('is_folder', 'false').lower() == 'true',
            status=data.get('status', MigrationStatus.PENDING.value),
            error_message=data.get('error_message', ''),
            migrated_at=data.get('migrated_at', ''),
            file_size=int(data.get('file_size', 0) or 0),
            retry_count=int(data.get('retry_count', 0) or 0),
            dest_folder_id=data.get('dest_folder_id', '')
        )


class CheckpointManager:
    """
    Checkpoint Manager with IMMEDIATE CSV updates
    
    Design Philosophy:
    - Keep ALL records in memory for O(1) lookups (fast queries)
    - Write to CSV IMMEDIATELY when status changes (no delays, no batching)
    - Simple and predictable: update happens when you call the method
    """
    
    DEFAULT_CHECKPOINT_FOLDER = "./migration_checkpoints"
    
    def __init__(self, source_domain: str, dest_domain: str, 
                 checkpoint_folder: str = None, resume_from: str = None):
        """Initialize checkpoint manager with immediate updates"""
        self.source_domain = source_domain
        self.dest_domain = dest_domain
        self.checkpoint_folder = checkpoint_folder or self.DEFAULT_CHECKPOINT_FOLDER
        
        self._ensure_checkpoint_folder()
        
        if resume_from and os.path.exists(resume_from):
            self.checkpoint_file = resume_from
            self.is_resuming = True
            logger.info(f"Resuming from checkpoint: {resume_from}")
        else:
            self.checkpoint_file = self._generate_checkpoint_filename()
            self.is_resuming = False
            logger.info(f"Creating new checkpoint: {self.checkpoint_file}")
        
        # Thread-safe lock for CSV writes
        self.lock = threading.Lock()
        
        self.fieldnames = [
            'source_domain', 'destination_domain', 'source_user_email', 
            'destination_user_email', 'file_id', 'file_name', 'parent_id',
            'mime_type', 'is_folder', 'status', 'error_message', 
            'migrated_at', 'file_size', 'retry_count', 'dest_folder_id'
        ]
        
        # Create file if needed
        if not os.path.exists(self.checkpoint_file):
            self._create_csv_file()
        
        # IN-MEMORY CACHE: file_id -> CheckpointRecord (for O(1) lookups)
        self._cache: Dict[str, CheckpointRecord] = {}
        
        # FOLDER MAPPING: source_folder_id -> dest_folder_id
        self._folder_mapping: Dict[str, str] = {}
        
        # Statistics
        self.stats = {
            'total_discovered': 0,
            'already_done': 0,
            'to_migrate': 0,
            'to_retry': 0,
            'ignored': 0
        }
        
        # Load everything into memory at startup
        self._load_cache()
        
        logger.info(f"Checkpoint manager ready: {len(self._cache)} records in cache")
    
    def _ensure_checkpoint_folder(self):
        """Create checkpoint folder if it doesn't exist"""
        try:
            Path(self.checkpoint_folder).mkdir(parents=True, exist_ok=True)
            logger.info(f"Checkpoint folder ready: {self.checkpoint_folder}")
        except Exception as e:
            logger.error(f"Failed to create checkpoint folder: {e}")
            raise
    
    def _generate_checkpoint_filename(self) -> str:
        """Generate checkpoint filename with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        source_clean = self.source_domain.replace('.', '_').replace('@', '_')
        dest_clean = self.dest_domain.replace('.', '_').replace('@', '_')
        filename = f"drive_migration_checkpoint_{source_clean}_to_{dest_clean}_{timestamp}.csv"
        return os.path.join(self.checkpoint_folder, filename)
    
    def _create_csv_file(self):
        """Create CSV file with headers"""
        try:
            with open(self.checkpoint_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()
            logger.info(f"Created checkpoint CSV: {self.checkpoint_file}")
        except Exception as e:
            logger.error(f"Failed to create CSV file: {e}")
            raise
    
    def _load_cache(self):
        """Load all records into memory cache (ONE TIME at startup)"""
        try:
            if not os.path.exists(self.checkpoint_file):
                return
            
            with open(self.checkpoint_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    try:
                        record = CheckpointRecord.from_dict(row)
                        self._cache[record.file_id] = record
                        
                        # Build folder mapping
                        if record.is_folder and record.dest_folder_id:
                            self._folder_mapping[record.file_id] = record.dest_folder_id
                        
                        # Update statistics
                        if record.status == MigrationStatus.DONE.value:
                            self.stats['already_done'] += 1
                        elif record.status == MigrationStatus.FAILED.value:
                            self.stats['to_retry'] += 1
                        elif record.status == MigrationStatus.IGNORED.value:
                            self.stats['ignored'] += 1
                        else:
                            self.stats['to_migrate'] += 1
                            
                    except Exception as e:
                        logger.warning(f"Failed to parse record: {e}")
            
            logger.info(f"Loaded {len(self._cache)} records into cache")
            logger.info(f"Loaded {len(self._folder_mapping)} folder mappings")
            
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")
            self._cache = {}
            self._folder_mapping = {}
    
    def _write_entire_csv(self):
        """
        Write entire CSV from memory cache
        Called IMMEDIATELY after each update (lock must be held)
        """
        try:
            with open(self.checkpoint_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()
                
                for record in self._cache.values():
                    writer.writerow(record.to_dict())
        except Exception as e:
            logger.error(f"Failed to write CSV: {e}")
            raise
    
    def register_discovered_items(self, items: List[Dict], source_email: str, dest_email: str):
        """Register newly discovered files/folders"""
        new_records = []
        
        for item in items:
            file_id = item['id']
            
            # Skip if already in cache
            if file_id in self._cache:
                existing = self._cache[file_id]
                
                # Update statistics
                if existing.status == MigrationStatus.DONE.value:
                    self.stats['already_done'] += 1
                elif existing.status == MigrationStatus.FAILED.value:
                    self.stats['to_retry'] += 1
                elif existing.status == MigrationStatus.IGNORED.value:
                    self.stats['ignored'] += 1
                else:
                    self.stats['to_migrate'] += 1
                
                continue
            
            mime_type = item.get('mimeType', '')
            is_folder = mime_type == 'application/vnd.google-apps.folder'
            
            # Check if should ignore
            should_ignore = self._should_ignore_mime_type(mime_type)
            status = MigrationStatus.IGNORED.value if should_ignore else MigrationStatus.PENDING.value
            
            record = CheckpointRecord(
                source_domain=self.source_domain,
                destination_domain=self.dest_domain,
                source_user_email=source_email,
                destination_user_email=dest_email,
                file_id=file_id,
                file_name=item.get('name', ''),
                parent_id=item.get('parents', [''])[0] if item.get('parents') else '',
                mime_type=mime_type,
                is_folder=is_folder,
                status=status,
                file_size=int(item.get('size', 0)),
                dest_folder_id=''
            )
            
            new_records.append(record)
            self._cache[file_id] = record
            
            # Update statistics
            if should_ignore:
                self.stats['ignored'] += 1
            else:
                self.stats['to_migrate'] += 1
        
        # Append new records to CSV
        if new_records:
            with self.lock:
                with open(self.checkpoint_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                    for record in new_records:
                        writer.writerow(record.to_dict())
            
            self.stats['total_discovered'] += len(new_records)
            logger.info(f"Registered {len(new_records)} new items to checkpoint")
    
    def _should_ignore_mime_type(self, mime_type: str) -> bool:
        """Check if MIME type should be ignored"""
        ignored_types = {
            'application/vnd.google-apps.script',
            'application/vnd.google-apps.form',
            'application/vnd.google-apps.site',
            'application/vnd.google-apps.shortcut',
            'application/vnd.google-apps.video'
        }
        return mime_type in ignored_types
    
    def should_skip_item(self, file_id: str) -> Tuple[bool, Optional[str]]:
        """Check if item should be skipped - O(1) lookup from cache"""
        if file_id not in self._cache:
            return (False, None)
        
        record = self._cache[file_id]
        
        if record.status == MigrationStatus.DONE.value:
            return (True, "Already migrated")
        
        if record.status == MigrationStatus.IGNORED.value:
            return (True, "Non-migratable type")
        
        return (False, None)
    
    def get_dest_folder_id(self, source_parent_id: str) -> Optional[str]:
        """Get destination folder ID - O(1) lookup"""
        return self._folder_mapping.get(source_parent_id)
    
    def register_folder_mapping(self, source_folder_id: str, dest_folder_id: str):
        """
        Register folder mapping IMMEDIATELY
        Updates cache + writes entire CSV to disk
        """
        with self.lock:
            # Update folder mapping
            self._folder_mapping[source_folder_id] = dest_folder_id
            
            # Update cache if record exists
            if source_folder_id in self._cache:
                self._cache[source_folder_id].dest_folder_id = dest_folder_id
                
                # IMMEDIATE WRITE to CSV
                self._write_entire_csv()
                logger.debug(f"Folder mapping saved: {source_folder_id} -> {dest_folder_id}")
    
    def mark_in_progress(self, file_id: str):
        """
        Mark item as IN_PROGRESS - IMMEDIATE UPDATE
        Updates cache + writes entire CSV to disk
        """
        with self.lock:
            if file_id not in self._cache:
                logger.warning(f"File not in cache: {file_id}")
                return
            
            # Update cache
            self._cache[file_id].status = MigrationStatus.IN_PROGRESS.value
            
            # IMMEDIATE WRITE to CSV
            self._write_entire_csv()
            logger.debug(f"Marked IN_PROGRESS: {file_id}")
    
    def mark_success(self, file_id: str, dest_file_id: str = ""):
        """
        Mark item as DONE - IMMEDIATE UPDATE
        Updates cache + writes entire CSV to disk
        """
        with self.lock:
            if file_id not in self._cache:
                logger.warning(f"File not in cache: {file_id}")
                return
            
            record = self._cache[file_id]
            
            # Update cache
            record.status = MigrationStatus.DONE.value
            record.migrated_at = datetime.now().isoformat()
            record.error_message = ''
            
            if dest_file_id:
                record.dest_folder_id = dest_file_id
                # Update folder mapping if it's a folder
                if record.is_folder:
                    self._folder_mapping[file_id] = dest_file_id
            
            # IMMEDIATE WRITE to CSV
            self._write_entire_csv()
            logger.debug(f"Marked DONE: {file_id}")
    
    def mark_failure(self, file_id: str, error_message: str):
        """
        Mark item as FAILED - IMMEDIATE UPDATE
        Updates cache + writes entire CSV to disk
        """
        with self.lock:
            if file_id not in self._cache:
                logger.warning(f"File not in cache: {file_id}")
                return
            
            record = self._cache[file_id]
            
            # Update cache
            record.status = MigrationStatus.FAILED.value
            record.error_message = error_message[:500]
            record.retry_count += 1
            
            # IMMEDIATE WRITE to CSV
            self._write_entire_csv()
            logger.debug(f"Marked FAILED: {file_id} - {error_message[:50]}")
    
    def mark_skipped(self, file_id: str, reason: str = ""):
        """
        Mark item as SKIPPED - IMMEDIATE UPDATE
        Updates cache + writes entire CSV to disk
        """
        with self.lock:
            if file_id not in self._cache:
                logger.warning(f"File not in cache: {file_id}")
                return
            
            # Update cache
            self._cache[file_id].status = MigrationStatus.SKIPPED.value
            self._cache[file_id].error_message = reason
            
            # IMMEDIATE WRITE to CSV
            self._write_entire_csv()
            logger.debug(f"Marked SKIPPED: {file_id}")
    
    def mark_ignored(self, file_id: str, reason: str = ""):
        """Mark item as ignored"""
        with self.lock:
            if file_id in self._cache:
                self._cache[file_id].status = MigrationStatus.IGNORED.value
                self._cache[file_id].error_message = reason
                
                # IMMEDIATE WRITE to CSV
                self._write_entire_csv()
    
    def get_checkpoint_summary(self) -> Dict:
        """Get checkpoint summary from cache (no CSV read needed)"""
        stats = {
            'total_items': len(self._cache),
            'pending': 0,
            'in_progress': 0,
            'done': 0,
            'failed': 0,
            'skipped': 0,
            'ignored': 0,
            'folders': 0,
            'files': 0
        }
        
        for record in self._cache.values():
            status_key = record.status.lower()
            if status_key in stats:
                stats[status_key] += 1
            
            if record.is_folder:
                stats['folders'] += 1
            else:
                stats['files'] += 1
        
        summary = {
            'checkpoint_file': self.checkpoint_file,
            'is_resuming': self.is_resuming,
            'source_domain': self.source_domain,
            'destination_domain': self.dest_domain,
            'total_items': stats['total_items'],
            'folders': stats['folders'],
            'files': stats['files'],
            'status_breakdown': {
                'pending': stats['pending'],
                'in_progress': stats['in_progress'],
                'done': stats['done'],
                'failed': stats['failed'],
                'skipped': stats['skipped'],
                'ignored': stats['ignored']
            },
            'discovery_stats': self.stats,
            'folder_mappings': len(self._folder_mapping)
        }
        
        if stats['total_items'] > 0:
            completed = stats['done'] + stats['skipped'] + stats['ignored']
            summary['completion_percentage'] = (completed / stats['total_items']) * 100
        else:
            summary['completion_percentage'] = 0.0
        
        return summary
    
    def print_summary(self):
        """Print checkpoint summary"""
        summary = self.get_checkpoint_summary()
        
        print("\n" + "="*80)
        print("CHECKPOINT SUMMARY")
        print("="*80)
        print(f"Checkpoint File: {summary['checkpoint_file']}")
        print(f"Mode: {'RESUMING' if summary['is_resuming'] else 'NEW MIGRATION'}")
        print(f"Source Domain: {summary['source_domain']}")
        print(f"Destination Domain: {summary['destination_domain']}")
        print(f"\nTotal Items: {summary['total_items']}")
        print(f"  - Folders: {summary['folders']}")
        print(f"  - Files: {summary['files']}")
        print(f"  - Folder Mappings: {summary['folder_mappings']}")
        print(f"\nStatus Breakdown:")
        print(f"  ✓ Done: {summary['status_breakdown']['done']}")
        print(f"  ⊘ Ignored: {summary['status_breakdown']['ignored']}")
        print(f"  ⧗ Pending: {summary['status_breakdown']['pending']}")
        print(f"  → In Progress: {summary['status_breakdown']['in_progress']}")
        print(f"  ✗ Failed: {summary['status_breakdown']['failed']}")
        print(f"  ⊗ Skipped: {summary['status_breakdown']['skipped']}")
        print(f"\nCompletion: {summary['completion_percentage']:.2f}%")
        print("="*80 + "\n")
    
    @staticmethod
    def find_latest_checkpoint(checkpoint_folder: str, source_domain: str, 
                               dest_domain: str) -> Optional[str]:
        """Find latest checkpoint file for given domains"""
        try:
            if not os.path.exists(checkpoint_folder):
                return None
            
            source_clean = source_domain.replace('.', '_').replace('@', '_')
            dest_clean = dest_domain.replace('.', '_').replace('@', '_')
            pattern = f"drive_migration_checkpoint_{source_clean}_to_{dest_clean}_"
            
            matching_files = []
            for filename in os.listdir(checkpoint_folder):
                if filename.startswith(pattern) and filename.endswith('.csv'):
                    full_path = os.path.join(checkpoint_folder, filename)
                    matching_files.append(full_path)
            
            if not matching_files:
                return None
            
            matching_files.sort(key=os.path.getmtime, reverse=True)
            logger.info(f"Found latest checkpoint: {matching_files[0]}")
            return matching_files[0]
            
        except Exception as e:
            logger.error(f"Error finding latest checkpoint: {e}")
            return None