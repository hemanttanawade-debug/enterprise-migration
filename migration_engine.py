"""
Production-Grade Migration Engine for Google Workspace Drive
CRITICAL FIXES:
1. Fixed UnboundLocalError in collaborator migration
2. Added support for Google Apps Script proper MIME handling
3. Added Google Video (Drive) support
4. Enhanced connection retry logic for large files
5. Better empty content detection and handling
"""
import logging
import time
import mimetypes
import hashlib
import io
from typing import Dict, List, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime
import json
from pathlib import Path
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from googleapiclient.errors import HttpError
import socket

logger = logging.getLogger(__name__)
mimetypes.init()


class MigrationEngine:
    """Production-grade migration engine with comprehensive error handling"""
    
    # ENHANCED MIME type mappings with ALL Google Workspace types
    GOOGLE_WORKSPACE_TYPES = {
        'application/vnd.google-apps.document': {
            'name': 'Google Docs',
            'export_mime': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'extension': '.docx',
            'import_mime': 'application/vnd.google-apps.document',
            'can_export': True,
            'native': True,
            'max_size': 200 * 1024 * 1024
        },
        'application/vnd.google-apps.spreadsheet': {
            'name': 'Google Sheets',
            'export_mime': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'extension': '.xlsx',
            'import_mime': 'application/vnd.google-apps.spreadsheet',
            'can_export': True,
            'native': True,
            'max_size': 100 * 1024 * 1024
        },
        'application/vnd.google-apps.presentation': {
            'name': 'Google Slides',
            'export_mime': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            'extension': '.pptx',
            'import_mime': 'application/vnd.google-apps.presentation',
            'can_export': True,
            'native': True,
            'max_size': 300 * 1024 * 1024
        },
        'application/vnd.google-apps.form': {
            'name': 'Google Forms',
            'export_mime': 'application/zip',
            'extension': '.zip',
            'import_mime': None,
            'can_export': True,
            'native': False,
            'max_size': None
        },
        'application/vnd.google-apps.drawing': {
            'name': 'Google Drawings',
            'export_mime': 'image/svg+xml',
            'extension': '.svg',
            'import_mime': None,
            'can_export': True,
            'native': False,
            'fallback_mime': 'application/pdf',
            'fallback_ext': '.pdf',
            'max_size': None
        },
        'application/vnd.google-apps.site': {
            'name': 'Google Sites',
            'export_mime': None,
            'extension': None,
            'import_mime': None,
            'can_export': False,
            'native': False,
            'max_size': None,
            'reason': 'Google Sites cannot be exported or migrated via API'
        },
        'application/vnd.google-apps.script': {
            'name': 'Google Apps Script',
            'export_mime': 'application/vnd.google-apps.script+json',
            'extension': '.json',
            'import_mime': None,
            'can_export': True,
            'native': False,
            'max_size': None,
            'special_handling': True  # Requires special JSON handling
        },
        'application/vnd.google-apps.map': {
            'name': 'Google My Maps',
            'export_mime': 'application/vnd.google-earth.kmz',
            'extension': '.kmz',
            'import_mime': None,
            'can_export': True,
            'native': False,
            'max_size': None
        },
        'application/vnd.google-apps.jam': {
            'name': 'Google Jamboard',
            'export_mime': 'application/pdf',
            'extension': '.pdf',
            'import_mime': None,
            'can_export': True,
            'native': False,
            'max_size': None
        },
        'application/vnd.google-apps.video': {
            'name': 'Google Drive Video',
            'export_mime': None,
            'extension': None,
            'import_mime': None,
            'can_export': False,
            'native': False,
            'max_size': None,
            'reason': 'Google Drive videos are links/pointers, not exportable files. Use standard file migration.'
        },
        'application/vnd.google-apps.folder': {
            'name': 'Folder',
            'export_mime': None,
            'extension': None,
            'import_mime': None,
            'can_export': False,
            'native': True,
            'max_size': None
        },
        'application/vnd.google-apps.shortcut': {
            'name': 'Shortcut',
            'export_mime': None,
            'extension': None,
            'import_mime': None,
            'can_export': False,
            'native': False,
            'max_size': None,
            'reason': 'Shortcuts cannot be migrated'
        }
    }
    
    # Enhanced chunk sizes with better timeout handling
    STANDARD_FILE_TYPES = {
        'application/pdf': {'name': 'PDF', 'chunk_size': 20 * 1024 * 1024, 'timeout': 300},
        'application/zip': {'name': 'ZIP Archive', 'chunk_size': 50 * 1024 * 1024, 'timeout': 600},
        'image/jpeg': {'name': 'JPEG Image', 'chunk_size': 10 * 1024 * 1024, 'timeout': 180},
        'image/png': {'name': 'PNG Image', 'chunk_size': 10 * 1024 * 1024, 'timeout': 180},
        'video/mp4': {'name': 'MP4 Video', 'chunk_size': 100 * 1024 * 1024, 'timeout': 900},
        'audio/mpeg': {'name': 'MP3 Audio', 'chunk_size': 20 * 1024 * 1024, 'timeout': 300},
        'video/webm': {'name': 'WebM Video', 'chunk_size': 100 * 1024 * 1024, 'timeout': 900},
        'text/plain': {'name': 'Text File', 'chunk_size': 5 * 1024 * 1024, 'timeout': 120},
        'text/csv': {'name': 'CSV File', 'chunk_size': 10 * 1024 * 1024, 'timeout': 180},
    }
    
    def __init__(self, source_drive_ops, dest_drive_ops, config, state_manager):
        """Initialize enhanced migration engine with better error handling"""
        self.source_ops = source_drive_ops
        self.dest_ops = dest_drive_ops
        self.config = config
        self.state = state_manager
        
        self.max_retries = 5  # Increased from 3 for better resilience
        self.retry_delay = 3  # Increased from 2
        self.exponential_backoff = True
        self.connection_timeout = 600  # 10 minutes for large files
        
        # Configure socket timeout globally
        socket.setdefaulttimeout(self.connection_timeout)
        
        self.stats = {
            'total_files': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'folders_created': 0,
            'folders_failed': 0,
            'by_type': {},
            'start_time': None,
            'end_time': None,
            'retry_count': 0,
            'bytes_transferred': 0,
            'collaborators_migrated': 0,
            'external_collaborators': 0,
            'connection_resets': 0,
            'empty_downloads': 0
        }
        
        self.file_mapping = {}
        self.processed_files: Set[Tuple] = set()
        self.failed_files = []
        self.batch_results = []
    def migrate_domain(self, user_mapping: Dict[str, str], max_workers: int = 5) -> Dict:
        """Main method to migrate entire domain with user mapping"""
        logger.info(f"Starting domain migration: {len(user_mapping)} users, {max_workers} workers")
        
        self.stats['start_time'] = datetime.now()
        
        summary = {
            'total_users': len(user_mapping),
            'completed_users': 0,
            'failed_users': 0,
            'total_files_migrated': 0,
            'total_files_failed': 0,
            'total_files_skipped': 0,
            'total_folders_created': 0,
            'total_folders_failed': 0,
            'total_collaborators_migrated': 0,
            'total_external_collaborators': 0,
            'accuracy_rate': 0.0,
            'user_results': [],
            'start_time': self.stats['start_time'].isoformat(),
            'end_time': None,
            'detailed_failures': []
        }
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_user = {
                executor.submit(self.migrate_user, src, dst): (src, dst)
                for src, dst in user_mapping.items()
            }
            
            with tqdm(total=len(user_mapping), desc="Migrating users") as pbar:
                for future in as_completed(future_to_user):
                    src_email, dst_email = future_to_user[future]
                    
                    try:
                        user_result = future.result()
                        summary['user_results'].append(user_result)
                        
                        if user_result['status'] == 'completed':
                            summary['completed_users'] += 1
                        else:
                            summary['failed_users'] += 1
                        
                        summary['total_files_migrated'] += user_result['files_migrated']
                        summary['total_files_failed'] += user_result['files_failed']
                        summary['total_files_skipped'] += user_result['files_skipped']
                        summary['total_folders_created'] += user_result.get('folders_created', 0)
                        summary['total_folders_failed'] += user_result.get('folders_failed', 0)
                        summary['total_collaborators_migrated'] += user_result.get('collaborators_migrated', 0)
                        summary['total_external_collaborators'] += user_result.get('external_collaborators', 0)
                        
                        if user_result.get('errors'):
                            summary['detailed_failures'].extend(user_result['errors'])
                        
                    except Exception as e:
                        logger.error(f"User migration failed {src_email}: {e}", exc_info=True)
                        summary['failed_users'] += 1
                        summary['user_results'].append({
                            'source_email': src_email,
                            'dest_email': dst_email,
                            'status': 'failed',
                            'error': str(e)
                        })
                    
                    pbar.update(1)
        
        total_files = summary['total_files_migrated'] + summary['total_files_failed']
        if total_files > 0:
            summary['accuracy_rate'] = (summary['total_files_migrated'] / total_files) * 100
        
        self.stats['end_time'] = datetime.now()
        summary['end_time'] = self.stats['end_time'].isoformat()
        summary['duration_seconds'] = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        logger.info(f"Domain migration completed: {summary['accuracy_rate']:.2f}% accuracy")
        logger.info(f"Files migrated: {summary['total_files_migrated']}, Failed: {summary['total_files_failed']}")
        logger.info(f"Folders created: {summary['total_folders_created']}, Failed: {summary['total_folders_failed']}")
        
        return summary

    def migrate_user(self, source_email: str, dest_email: str) -> Dict:
        """
        Migrate single user with comprehensive error handling
        """
        logger.info(f"Starting COMPLETE user migration: {source_email} -> {dest_email}")
        
        user_result = {
            'source_email': source_email,
            'dest_email': dest_email,
            'status': 'in_progress',
            'files_total': 0,
            'files_migrated': 0,
            'files_failed': 0,
            'files_skipped': 0,
            'folders_total': 0,
            'folders_created': 0,
            'folders_failed': 0,
            'native_formats': 0,
            'converted_formats': 0,
            'standard_files': 0,
            'bytes_transferred': 0,
            'collaborators_migrated': 0,
            'external_collaborators': 0,
            'accuracy_rate': 0.0,
            'errors': [],
            'warnings': [],
            'start_time': datetime.now().isoformat()
        }
        
        source_drive = None
        dest_drive = None
        
        try:
            from auth import GoogleAuthManager
            
            # Authenticate as source user
            source_auth = GoogleAuthManager(
                self.config.SOURCE_CREDENTIALS_FILE,
                self.config.SCOPES,
                delegate_email=source_email
            )
            source_auth.authenticate()
            source_drive = source_auth.get_drive_service(user_email=source_email)
            
            # Authenticate as destination user
            dest_auth = GoogleAuthManager(
                self.config.DEST_CREDENTIALS_FILE,
                self.config.SCOPES,
                delegate_email=dest_email
            )
            dest_auth.authenticate()
            dest_drive = dest_auth.get_drive_service(user_email=dest_email)
            
            # Get ALL files and folders OWNED by source user
            logger.info(f"Fetching ALL files and folders OWNED by {source_email}...")
            source_files = self._get_all_user_owned_files(source_drive, source_email)
            
            if not source_files:
                logger.info(f"No files or folders found for {source_email}")
                user_result['status'] = 'completed'
                user_result['accuracy_rate'] = 100.0
                user_result['end_time'] = datetime.now().isoformat()
                return user_result
            
            # Separate files and folders
            all_folders = [f for f in source_files if f['mimeType'] == 'application/vnd.google-apps.folder']
            all_files = [f for f in source_files if f['mimeType'] != 'application/vnd.google-apps.folder']
            
            user_result['files_total'] = len(all_files)
            user_result['folders_total'] = len(all_folders)
            
            logger.info(f"Found {user_result['files_total']} files and {user_result['folders_total']} folders OWNED by {source_email}")
            
            # STEP 1: Build folder structure
            logger.info("Building exact folder structure with proper hierarchy...")
            folder_mapping = self._build_folder_structure_with_hierarchy(
                all_folders, source_drive, dest_drive
            )
            
            user_result['folders_created'] = len(folder_mapping)
            user_result['folders_failed'] = user_result['folders_total'] - user_result['folders_created']
            
            if user_result['folders_failed'] > 0:
                user_result['warnings'].append(f"⚠ {user_result['folders_failed']} folders failed to create")
                logger.warning(f"⚠ {user_result['folders_failed']} folders failed to create")
            
            # STEP 2: Migrate ALL files
            logger.info("Migrating files with proper folder placement...")
            file_count = 0
            
            for file_info in tqdm(all_files, desc=f"Migrating {source_email}", leave=False):
                file_id = file_info['id']
                file_name = file_info['name']
                mime_type = file_info['mimeType']
                file_size = int(file_info.get('size', 0))
                
                file_count += 1
                
                # Deduplication check
                file_signature = (file_id, file_name, file_size)
                if file_signature in self.processed_files:
                    logger.debug(f"Skipping duplicate: {file_name}")
                    user_result['files_skipped'] += 1
                    continue
                
                # Get parent folder
                parent_ids = file_info.get('parents', [])
                dest_parent_id = None
                
                if parent_ids:
                    source_parent_id = parent_ids[0]
                    dest_parent_id = folder_mapping.get(source_parent_id)
                    
                    if not dest_parent_id and source_parent_id in [f['id'] for f in all_folders]:
                        logger.warning(f"Parent folder not mapped for file: {file_name}")
                
                # Migrate file with enhanced retry logic
                migration_result = self._migrate_file_with_enhanced_retry(
                    file_id, file_name, mime_type, file_size,
                    dest_parent_id, source_drive, dest_drive, dest_email
                )
                
                if migration_result['success']:
                    user_result['files_migrated'] += 1
                    user_result['bytes_transferred'] += file_size
                    
                    method = migration_result.get('method', 'unknown')
                    if method == 'native_format':
                        user_result['native_formats'] += 1
                    elif method == 'converted_format':
                        user_result['converted_formats'] += 1
                    elif method == 'standard_file':
                        user_result['standard_files'] += 1
                    
                    # Migrate collaborators with error handling
                    dest_file_id = migration_result.get('dest_file_id')
                    if dest_file_id:
                        try:
                            collab_result = self._migrate_file_collaborators_safe(
                                file_id, dest_file_id, file_name,
                                source_drive, dest_drive
                            )
                            
                            user_result['collaborators_migrated'] += collab_result.get('migrated', 0)
                            user_result['external_collaborators'] += collab_result.get('external', 0)
                        except Exception as collab_error:
                            logger.warning(f"Failed to migrate collaborators for {file_name}: {collab_error}")
                    
                    self.processed_files.add(file_signature)
                    self.stats['successful'] += 1
                    
                    logger.debug(f"✓ [{file_count}/{user_result['files_total']}] Migrated {method}: {file_name}")
                    
                else:
                    user_result['files_failed'] += 1
                    
                    error_detail = {
                        'file': file_name,
                        'file_id': file_id,
                        'mime_type': mime_type,
                        'size': file_size,
                        'error': migration_result.get('error', 'Unknown error'),
                        'error_type': migration_result.get('error_type', 'unknown'),
                        'retry_attempts': migration_result.get('retry_attempts', 0),
                        'user': source_email
                    }
                    user_result['errors'].append(error_detail)
                    
                    self.stats['failed'] += 1
                    
                    logger.warning(f"✗ [{file_count}/{user_result['files_total']}] Failed: {file_name} - {error_detail['error'][:100]}")
            
            # Calculate accuracy
            total_attempted = user_result['files_total'] - user_result['files_skipped']
            if total_attempted > 0:
                user_result['accuracy_rate'] = (user_result['files_migrated'] / total_attempted) * 100
            else:
                user_result['accuracy_rate'] = 100.0
            
            user_result['status'] = 'completed'
            user_result['end_time'] = datetime.now().isoformat()
            
            logger.info(f"✓ User migration completed: {source_email}")
            logger.info(f"  Files: {user_result['files_migrated']}/{user_result['files_total']} ({user_result['accuracy_rate']:.2f}%)")
            logger.info(f"  Folders: {user_result['folders_created']}/{user_result['folders_total']}")
            logger.info(f"  Collaborators: {user_result['collaborators_migrated']} ({user_result['external_collaborators']} external)")
            
            if user_result['files_failed'] > 0:
                logger.warning(f"  ⚠ {user_result['files_failed']} files failed to migrate")
            
        except Exception as e:
            logger.error(f"User migration failed for {source_email}: {e}", exc_info=True)
            user_result['status'] = 'failed'
            user_result['errors'].append({
                'error': str(e),
                'error_type': 'critical_exception',
                'user': source_email
            })
            user_result['end_time'] = datetime.now().isoformat()
        
        return user_result
    def _get_all_user_owned_files(self, drive_service, user_email: str) -> List[Dict]:
        """
        Get ONLY files and folders OWNED by the user
        Enhanced with better error handling for large datasets
        """
        files = []
        page_token = None
        retry_count = 0
        max_retries = 3
        
        try:
            while True:
                try:
                    response = drive_service.files().list(
                        q=f"'{user_email}' in owners and trashed=false",
                        spaces='drive',
                        fields='nextPageToken, files(id, name, mimeType, size, parents, createdTime, modifiedTime, owners, permissions)',
                        pageSize=1000,
                        pageToken=page_token,
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=False
                    ).execute()
                    
                    batch = response.get('files', [])
                    
                    # Double-check ownership
                    owned_files = []
                    for file in batch:
                        owners = file.get('owners', [])
                        if any(owner.get('emailAddress') == user_email for owner in owners):
                            owned_files.append(file)
                        else:
                            logger.debug(f"Skipping non-owned file: {file['name']}")
                    
                    files.extend(owned_files)
                    
                    logger.debug(f"Fetched {len(owned_files)} owned items (total: {len(files)})")
                    
                    page_token = response.get('nextPageToken')
                    
                    if not page_token:
                        break
                    
                    retry_count = 0  # Reset on success
                    
                except (socket.timeout, ConnectionResetError, ConnectionError) as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error(f"Failed to list files after {max_retries} retries: {e}")
                        raise
                    
                    wait_time = 5 * retry_count
                    logger.warning(f"Connection error listing files, retrying in {wait_time}s... (attempt {retry_count}/{max_retries})")
                    time.sleep(wait_time)
                    continue
            
            logger.info(f"Total files/folders fetched for {user_email}: {len(files)}")
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            raise

    def _build_folder_structure_with_hierarchy(self, folders: List[Dict], source_drive, dest_drive) -> Dict[str, str]:
        """
        Build EXACT folder structure with proper parent-child relationships
        """
        folder_mapping = {}
        
        if not folders:
            logger.info("No folders to create")
            return folder_mapping
        
        logger.info(f"Building folder structure: {len(folders)} folders")
        
        # Build folder hierarchy map
        folder_hierarchy = {}
        root_folders = []
        
        for folder in folders:
            folder_id = folder['id']
            folder_name = folder['name']
            parent_ids = folder.get('parents', [])
            
            folder_hierarchy[folder_id] = {
                'id': folder_id,
                'name': folder_name,
                'parents': parent_ids,
                'children': [],
                'level': 0,
                'created': False
            }
            
            # Identify root folders
            if not parent_ids:
                root_folders.append(folder_id)
            else:
                parent_in_owned = any(parent_ids[0] == f['id'] for f in folders)
                if not parent_in_owned:
                    root_folders.append(folder_id)
                    folder_hierarchy[folder_id]['parents'] = []
        
        # Build parent-child relationships
        for folder_id, folder_info in folder_hierarchy.items():
            for parent_id in folder_info['parents']:
                if parent_id in folder_hierarchy:
                    folder_hierarchy[parent_id]['children'].append(folder_id)
        
        # Calculate folder levels
        def calculate_level(folder_id, visited=None):
            if visited is None:
                visited = set()
            
            if folder_id in visited:
                return 0
            
            visited.add(folder_id)
            
            if folder_id not in folder_hierarchy:
                return 0
            
            folder_info = folder_hierarchy[folder_id]
            parents = folder_info['parents']
            
            if not parents or not any(p in folder_hierarchy for p in parents):
                return 0
            
            parent_id = parents[0]
            if parent_id in folder_hierarchy:
                return 1 + calculate_level(parent_id, visited)
            return 0
        
        for folder_id in folder_hierarchy:
            folder_hierarchy[folder_id]['level'] = calculate_level(folder_id)
        
        # Sort folders by level
        sorted_folders = sorted(
            folder_hierarchy.items(),
            key=lambda x: (x[1]['level'], x[1]['name'])
        )
        
        logger.info(f"Folder hierarchy: {len(root_folders)} root folders, max depth: {max([f[1]['level'] for f in sorted_folders]) if sorted_folders else 0}")
        
        # Create folders level by level
        created_count = 0
        failed_count = 0
        
        for folder_id, folder_info in sorted_folders:
            folder_name = folder_info['name']
            parent_ids = folder_info['parents']
            level = folder_info['level']
            
            # Determine destination parent
            dest_parent_id = None
            if parent_ids:
                source_parent_id = parent_ids[0]
                dest_parent_id = folder_mapping.get(source_parent_id)
                
                if not dest_parent_id and source_parent_id in folder_hierarchy:
                    parent_info = folder_hierarchy.get(source_parent_id)
                    if parent_info and not parent_info['created']:
                        logger.info(f"Creating missing parent folder first...")
                        parent_dest_id = self._create_single_folder(
                            parent_info['name'], 
                            None,
                            source_parent_id,
                            source_drive,
                            dest_drive
                        )
                        if parent_dest_id:
                            folder_mapping[source_parent_id] = parent_dest_id
                            folder_hierarchy[source_parent_id]['created'] = True
                            dest_parent_id = parent_dest_id
            
            # Create the folder
            try:
                dest_folder_id = self._create_single_folder(
                    folder_name,
                    dest_parent_id,
                    folder_id,
                    source_drive,
                    dest_drive
                )
                
                if dest_folder_id:
                    folder_mapping[folder_id] = dest_folder_id
                    folder_hierarchy[folder_id]['created'] = True
                    created_count += 1
                    
                    logger.debug(f"✓ [{created_count}/{len(folders)}] Created folder (level {level}): {folder_name}")
                    
                    # Migrate folder permissions
                    try:
                        self._migrate_folder_collaborators_safe(
                            folder_id, dest_folder_id, folder_name,
                            source_drive, dest_drive
                        )
                    except Exception as perm_error:
                        logger.warning(f"Failed to migrate folder permissions for {folder_name}: {perm_error}")
                else:
                    failed_count += 1
                    logger.error(f"✗ Failed to create folder: {folder_name}")
                    
            except Exception as e:
                failed_count += 1
                logger.error(f"✗ Exception creating folder {folder_name}: {e}")
        
        logger.info(f"✓ Folder structure created: {created_count}/{len(folders)} folders")
        
        if failed_count > 0:
            logger.warning(f"⚠ {failed_count} folders failed to create")
            self.stats['folders_failed'] += failed_count
        
        self.stats['folders_created'] += created_count
        
        # Verify folder hierarchy
        self._verify_folder_hierarchy_integrity(folder_hierarchy, folder_mapping)
        
        return folder_mapping

    def _create_single_folder(self, folder_name: str, dest_parent_id: Optional[str],
                             source_folder_id: str, source_drive, dest_drive) -> Optional[str]:
        """Create a single folder with retry logic"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                folder_metadata = {
                    'name': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder'
                }
                
                if dest_parent_id:
                    folder_metadata['parents'] = [dest_parent_id]
                
                created_folder = dest_drive.files().create(
                    body=folder_metadata,
                    fields='id,name',
                    supportsAllDrives=True
                ).execute()
                
                return created_folder['id']
                
            except HttpError as e:
                if e.resp.status == 409:
                    logger.warning(f"Folder might already exist: {folder_name}")
                    existing_id = self._find_existing_folder(folder_name, dest_parent_id, dest_drive)
                    if existing_id:
                        logger.info(f"Using existing folder: {folder_name}")
                        return existing_id
                
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Retrying folder creation in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                
                logger.error(f"HTTP error creating folder {folder_name}: {e}")
                return None
                
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Retrying folder creation in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                
                logger.error(f"Error creating folder {folder_name}: {e}")
                return None
        
        return None

    def _find_existing_folder(self, folder_name: str, parent_id: Optional[str], 
                             dest_drive) -> Optional[str]:
        """Find existing folder by name and parent"""
        try:
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_id:
                query += f" and '{parent_id}' in parents"
            
            response = dest_drive.files().list(
                q=query,
                spaces='drive',
                fields='files(id,name)',
                pageSize=10
            ).execute()
            
            files = response.get('files', [])
            if files:
                return files[0]['id']
            
            return None
            
        except Exception as e:
            logger.debug(f"Error finding existing folder: {e}")
            return None

    def _verify_folder_hierarchy_integrity(self, folder_hierarchy: Dict, folder_mapping: Dict):
        """Verify folder hierarchy integrity after creation"""
        logger.info("Verifying folder hierarchy integrity...")
        
        issues = []
        verified = 0
        
        for source_id, folder_info in folder_hierarchy.items():
            folder_name = folder_info['name']
            
            if source_id not in folder_mapping:
                issues.append(f"Folder not created: {folder_name}")
                continue
            
            verified += 1
            
            if folder_info['parents']:
                parent_id = folder_info['parents'][0]
                if parent_id in folder_hierarchy:
                    if parent_id not in folder_mapping:
                        issues.append(f"Folder '{folder_name}' has missing parent")
        
        if issues:
            logger.warning(f"Hierarchy verification found {len(issues)} issues:")
            for issue in issues[:10]:
                logger.warning(f"  - {issue}")
            if len(issues) > 10:
                logger.warning(f"  ... and {len(issues) - 10} more issues")
        else:
            logger.info(f"✓ Folder hierarchy verified - {verified} folders with intact relationships")
    def _migrate_file_with_enhanced_retry(self, file_id: str, file_name: str, mime_type: str,
                                          file_size: int, dest_parent_id: Optional[str],
                                          source_drive, dest_drive, dest_email: str) -> Dict:
        """
        Migrate file with ENHANCED retry logic for connection issues
        CRITICAL FIX: Better handling of connection resets and timeouts
        """
        last_error = None
        error_type = 'unknown'
        
        for attempt in range(self.max_retries):
            try:
                if mime_type.startswith('application/vnd.google-apps.'):
                    result = self._migrate_google_workspace_file_safe(
                        file_id, file_name, mime_type, file_size,
                        dest_parent_id, source_drive, dest_drive, dest_email
                    )
                else:
                    result = self._migrate_standard_file_safe(
                        file_id, file_name, mime_type, file_size,
                        dest_parent_id, source_drive, dest_drive
                    )
                
                if result['success']:
                    if attempt > 0:
                        logger.info(f"✓ Succeeded on attempt {attempt + 1}: {file_name}")
                    result['retry_attempts'] = attempt
                    return result
                else:
                    last_error = result.get('error', 'Migration failed')
                    error_type = result.get('error_type', 'migration_failed')
                
            except (socket.timeout, ConnectionResetError, ConnectionError, OSError) as e:
                # Network/connection errors - ALWAYS retry
                error_code = getattr(e, 'winerror', getattr(e, 'errno', 0))
                
                if error_code == 10054:  # WinError 10054
                    error_type = 'connection_reset'
                    last_error = "Connection forcibly closed by remote host"
                    self.stats['connection_resets'] += 1
                else:
                    error_type = 'connection_error'
                    last_error = f"Connection error: {str(e)}"
                
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Connection error for {file_name}, retrying in {wait_time}s (attempt {attempt + 1}/{self.max_retries}): {last_error}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Failed after {self.max_retries} connection retry attempts: {file_name}")
                    break
                
            except HttpError as e:
                status_code = e.resp.status
                last_error = str(e)
                
                if status_code == 403:
                    error_type = 'permission_denied'
                    last_error = f"Permission denied: {last_error}"
                elif status_code == 404:
                    error_type = 'not_found'
                    last_error = f"File not found: {last_error}"
                elif status_code == 429:
                    error_type = 'rate_limit'
                    last_error = f"Rate limit exceeded: {last_error}"
                elif status_code == 400:
                    error_type = 'bad_request'
                    last_error = f"Bad request (likely invalid MIME type): {last_error}"
                elif status_code in [500, 503]:
                    error_type = 'server_error'
                    last_error = f"Server error: {last_error}"
                else:
                    error_type = 'http_error'
                    last_error = f"HTTP {status_code}: {last_error}"
                
                # Retry on rate limits and server errors
                if status_code in [429, 500, 503]:
                    if attempt < self.max_retries - 1:
                        delay = self.retry_delay * (2 ** attempt) if self.exponential_backoff else self.retry_delay
                        logger.warning(f"Retrying {file_name} in {delay}s (attempt {attempt + 1}/{self.max_retries})")
                        time.sleep(delay)
                        continue
                
                # Non-retryable HTTP errors
                logger.error(f"Non-retryable error for {file_name}: {last_error}")
                break
            
            except Exception as e:
                last_error = str(e)
                error_type = 'unexpected_error'
                
                if 'exportSizeLimitExceeded' in last_error:
                    error_type = 'size_limit_exceeded'
                    last_error = f"Export size limit exceeded: {last_error}"
                    break  # Don't retry size limit errors
                elif 'timeout' in last_error.lower():
                    error_type = 'timeout'
                elif 'quota' in last_error.lower():
                    error_type = 'quota_exceeded'
                else:
                    last_error = f"Unexpected error: {last_error}"
                
                logger.error(f"Error migrating {file_name}: {last_error}")
                
                # Retry on timeouts and quota errors
                if error_type in ['timeout', 'quota_exceeded'] and attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Retrying {file_name} in {delay}s")
                    time.sleep(delay)
                    continue
                
                break
        
        return {
            'success': False,
            'error': last_error if last_error else 'Max retries exceeded',
            'error_type': error_type,
            'retry_attempts': self.max_retries
        }
    def _migrate_google_workspace_file_safe(self, file_id: str, file_name: str, mime_type: str,
                                           file_size: int, dest_parent_id: Optional[str],
                                           source_drive, dest_drive, dest_email: str) -> Dict:
        """
        Migrate Google Workspace file with ENHANCED error handling
        CRITICAL FIX: Proper handling of Apps Script and unknown types
        """
        if mime_type not in self.GOOGLE_WORKSPACE_TYPES:
            return {
                'success': False,
                'error': f"Unknown Google Workspace type: {mime_type}",
                'error_type': 'unknown_mime_type'
            }
        
        type_info = self.GOOGLE_WORKSPACE_TYPES[mime_type]
        
        if not type_info['can_export']:
            return {
                'success': False,
                'error': type_info.get('reason', f"{type_info['name']} cannot be exported"),
                'error_type': 'not_exportable'
            }
        
        try:
            # Special handling for Google Apps Script
            if mime_type == 'application/vnd.google-apps.script':
                return self._migrate_apps_script(
                    file_id, file_name, dest_parent_id,
                    source_drive, dest_drive
                )
            
            logger.debug(f"Exporting {file_name} ({type_info['name']}) as {type_info['export_mime']}")
            
            # Export from source with timeout handling
            request = source_drive.files().export_media(
                fileId=file_id,
                mimeType=type_info['export_mime']
            )
            
            file_buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(file_buffer, request, chunksize=10*1024*1024)
            
            done = False
            download_start = time.time()
            timeout = 600  # 10 minutes timeout
            
            while not done:
                if time.time() - download_start > timeout:
                    return {
                        'success': False,
                        'error': f'Download timeout after {timeout}s',
                        'error_type': 'timeout'
                    }
                
                status, done = downloader.next_chunk()
                if status:
                    progress = int(status.progress() * 100)
                    if progress % 20 == 0:
                        logger.debug(f"Export progress {file_name}: {progress}%")
            
            file_buffer.seek(0)
            exported_content = file_buffer.read()
            
            if not exported_content or len(exported_content) == 0:
                self.stats['empty_downloads'] += 1
                return {
                    'success': False,
                    'error': 'Exported content is empty',
                    'error_type': 'empty_export'
                }
            
            exported_size = len(exported_content)
            logger.debug(f"Exported {file_name}: {self._format_bytes(exported_size)}")
            
            # Import to destination
            file_metadata = {'name': file_name}
            if dest_parent_id:
                file_metadata['parents'] = [dest_parent_id]
            
            if type_info['import_mime']:
                # Native format - import back as Google Workspace file
                file_metadata['mimeType'] = type_info['import_mime']
                media = MediaIoBaseUpload(
                    io.BytesIO(exported_content),
                    mimetype=type_info['export_mime'],
                    resumable=True,
                    chunksize=10*1024*1024
                )
                
                imported_file = dest_drive.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id,name,mimeType',
                    supportsAllDrives=True
                ).execute()
                
                logger.debug(f"✓ Imported as native format: {file_name}")
                
                return {
                    'success': True,
                    'method': 'native_format',
                    'dest_file_id': imported_file['id']
                }
            else:
                # Converted format - keep as exported format
                export_name = file_name + type_info['extension']
                file_metadata['name'] = export_name
                
                media = MediaIoBaseUpload(
                    io.BytesIO(exported_content),
                    mimetype=type_info['export_mime'],
                    resumable=True,
                    chunksize=10*1024*1024
                )
                
                uploaded_file = dest_drive.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id,name,mimeType',
                    supportsAllDrives=True
                ).execute()
                
                logger.debug(f"✓ Imported as converted format: {export_name}")
                
                return {
                    'success': True,
                    'method': 'converted_format',
                    'dest_file_id': uploaded_file['id']
                }
                
        except HttpError as e:
            error_str = str(e)
            
            # Handle invalid MIME type errors (common with Apps Script)
            if 'Invalid MIME type' in error_str or 'invalidContentType' in error_str:
                logger.warning(f"Invalid MIME type for {file_name}, trying alternative approach")
                return {
                    'success': False,
                    'error': f"Invalid MIME type for upload: {type_info['export_mime']}",
                    'error_type': 'invalid_mime_type'
                }
            
            if 'exportSizeLimitExceeded' in error_str:
                # Try fallback format if available
                if 'fallback_mime' in type_info:
                    logger.info(f"Export size exceeded, trying fallback format for: {file_name}")
                    try:
                        return self._migrate_with_fallback_format(
                            file_id, file_name, type_info, dest_parent_id,
                            source_drive, dest_drive
                        )
                    except Exception as fallback_error:
                        logger.error(f"Fallback also failed: {fallback_error}")
                
                return {
                    'success': False,
                    'error': f"Export size limit exceeded for {file_name}",
                    'error_type': 'export_size_exceeded'
                }
            
            raise
        
        except Exception as e:
            logger.error(f"Failed to migrate Google Workspace file {file_name}: {e}")
            raise

    def _migrate_apps_script(self, file_id: str, file_name: str, dest_parent_id: Optional[str],
                            source_drive, dest_drive) -> Dict:
        """
        Special handling for Google Apps Script files
        Apps Script JSON export has specific requirements
        """
        try:
            logger.info(f"Migrating Apps Script: {file_name}")
            
            # Export as JSON
            request = source_drive.files().export_media(
                fileId=file_id,
                mimeType='application/vnd.google-apps.script+json'
            )
            
            file_buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(file_buffer, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            
            file_buffer.seek(0)
            script_content = file_buffer.read()
            
            if not script_content:
                return {
                    'success': False,
                    'error': 'Apps Script export is empty',
                    'error_type': 'empty_export'
                }
            
            # Save as JSON file (Apps Script cannot be re-imported as native)
            export_name = file_name + '.json'
            file_metadata = {'name': export_name}
            if dest_parent_id:
                file_metadata['parents'] = [dest_parent_id]
            
            media = MediaIoBaseUpload(
                io.BytesIO(script_content),
                mimetype='application/json',  # Use standard JSON MIME type
                resumable=True
            )
            
            uploaded_file = dest_drive.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name',
                supportsAllDrives=True
            ).execute()
            
            logger.info(f"✓ Migrated Apps Script as JSON: {export_name}")
            
            return {
                'success': True,
                'method': 'converted_format',
                'dest_file_id': uploaded_file['id']
            }
            
        except Exception as e:
            logger.error(f"Failed to migrate Apps Script {file_name}: {e}")
            return {
                'success': False,
                'error': f"Apps Script migration failed: {str(e)}",
                'error_type': 'apps_script_error'
            }

    def _migrate_with_fallback_format(self, file_id: str, file_name: str, type_info: Dict,
                                     dest_parent_id: Optional[str], source_drive, dest_drive) -> Dict:
        """Migrate using fallback format"""
        logger.info(f"Using fallback format {type_info['fallback_mime']} for: {file_name}")
        
        request = source_drive.files().export_media(
            fileId=file_id,
            mimeType=type_info['fallback_mime']
        )
        
        file_buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(file_buffer, request, chunksize=10*1024*1024)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        
        file_buffer.seek(0)
        
        fallback_name = file_name + type_info['fallback_ext']
        file_metadata = {'name': fallback_name}
        if dest_parent_id:
            file_metadata['parents'] = [dest_parent_id]
        
        media = MediaIoBaseUpload(
            file_buffer,
            mimetype=type_info['fallback_mime'],
            resumable=True,
            chunksize=10*1024*1024
        )
        
        uploaded_file = dest_drive.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,name',
            supportsAllDrives=True
        ).execute()
        
        logger.info(f"✓ Migrated with fallback format: {fallback_name}")
        
        return {
            'success': True,
            'method': 'converted_format',
            'dest_file_id': uploaded_file['id']
        }
    def _migrate_standard_file_safe(self, file_id: str, file_name: str, mime_type: str,
                                    file_size: int, dest_parent_id: Optional[str],
                                    source_drive, dest_drive) -> Dict:
        """
        Migrate standard file with ENHANCED connection error handling
        CRITICAL FIX: Better handling of connection resets for large files
        """
        try:
            logger.debug(f"Downloading standard file: {file_name} ({self._format_bytes(file_size)})")
            
            # Download from source with timeout and retry logic
            request = source_drive.files().get_media(
                fileId=file_id,
                supportsAllDrives=True
            )
            
            file_buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(file_buffer, request, chunksize=20*1024*1024)
            
            done = False
            last_progress = 0
            download_start = time.time()
            timeout = 900  # 15 minutes for very large files
            
            while not done:
                # Check timeout
                if time.time() - download_start > timeout:
                    return {
                        'success': False,
                        'error': f'Download timeout after {timeout}s',
                        'error_type': 'timeout'
                    }
                
                try:
                    status, done = downloader.next_chunk()
                    if status:
                        progress = int(status.progress() * 100)
                        # Log progress for large files
                        if file_size > 50*1024*1024 and progress >= last_progress + 20:
                            logger.debug(f"Download progress {file_name}: {progress}%")
                            last_progress = progress
                
                except (socket.timeout, ConnectionResetError, ConnectionError) as e:
                    logger.warning(f"Connection error during download of {file_name}: {e}")
                    # Let the outer retry logic handle this
                    raise
            
            file_buffer.seek(0)
            downloaded_content = file_buffer.read()
            
            if not downloaded_content or len(downloaded_content) == 0:
                self.stats['empty_downloads'] += 1
                return {
                    'success': False,
                    'error': 'Downloaded content is empty',
                    'error_type': 'empty_download'
                }
            
            actual_size = len(downloaded_content)
            logger.debug(f"Downloaded {file_name}: {self._format_bytes(actual_size)}")
            
            # Verify download size if original size was known
            if file_size > 0 and abs(actual_size - file_size) > 1024:  # Allow 1KB difference
                logger.warning(f"Size mismatch for {file_name}: expected {file_size}, got {actual_size}")
            
            # Upload to destination with retry logic
            logger.debug(f"Uploading: {file_name}")
            
            file_metadata = {'name': file_name}
            if dest_parent_id:
                file_metadata['parents'] = [dest_parent_id]
            
            # Get optimal chunk size
            chunk_size = self._get_optimal_chunk_size(mime_type, actual_size)
            
            media = MediaIoBaseUpload(
                io.BytesIO(downloaded_content),
                mimetype=mime_type,
                resumable=True,
                chunksize=chunk_size
            )
            
            upload_start = time.time()
            upload_timeout = 900  # 15 minutes
            
            try:
                uploaded_file = dest_drive.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id,name,size,mimeType',
                    supportsAllDrives=True
                ).execute()
                
                upload_time = time.time() - upload_start
                logger.debug(f"✓ Uploaded: {file_name} (ID: {uploaded_file['id']}) in {upload_time:.1f}s")
                
                return {
                    'success': True,
                    'method': 'standard_file',
                    'dest_file_id': uploaded_file['id']
                }
                
            except (socket.timeout, ConnectionResetError, ConnectionError) as e:
                logger.warning(f"Connection error during upload of {file_name}: {e}")
                # Let the outer retry logic handle this
                raise
            
        except HttpError as e:
            error_msg = str(e)
            
            if e.resp.status == 403:
                return {
                    'success': False,
                    'error': f"Permission denied downloading {file_name}",
                    'error_type': 'permission_denied'
                }
            elif e.resp.status == 404:
                return {
                    'success': False,
                    'error': f"File not found: {file_name}",
                    'error_type': 'not_found'
                }
            elif 'storage quota' in error_msg.lower() or 'insufficientFilePermissions' in error_msg:
                return {
                    'success': False,
                    'error': f"Storage quota exceeded or insufficient permissions for {file_name}",
                    'error_type': 'quota_exceeded'
                }
            else:
                return {
                    'success': False,
                    'error': f"HTTP error {e.resp.status}: {error_msg}",
                    'error_type': 'http_error'
                }
        
        except (socket.timeout, ConnectionResetError, ConnectionError, OSError) as e:
            # These will be caught by the outer retry logic
            raise
        
        except Exception as e:
            return {
                'success': False,
                'error': f"Unexpected error: {str(e)}",
                'error_type': 'unexpected_error'
            }

    def _get_optimal_chunk_size(self, mime_type: str, file_size: int) -> int:
        """
        Get optimal chunk size based on file type and size
        """
        # Check if we have a specific chunk size for this MIME type
        if mime_type in self.STANDARD_FILE_TYPES:
            base_chunk = self.STANDARD_FILE_TYPES[mime_type].get('chunk_size', 20 * 1024 * 1024)
        else:
            base_chunk = 20 * 1024 * 1024  # 20MB default
        
        # Adjust chunk size based on file size
        if file_size < 1024 * 1024:  # < 1MB
            return 512 * 1024  # 512KB chunks
        elif file_size < 10 * 1024 * 1024:  # < 10MB
            return 2 * 1024 * 1024  # 2MB chunks
        elif file_size < 50 * 1024 * 1024:  # < 50MB
            return 10 * 1024 * 1024  # 10MB chunks
        elif file_size < 100 * 1024 * 1024:  # < 100MB
            return 20 * 1024 * 1024  # 20MB chunks
        elif file_size < 500 * 1024 * 1024:  # < 500MB
            return 50 * 1024 * 1024  # 50MB chunks
        elif file_size < 1024 * 1024 * 1024:  # < 1GB
            return 100 * 1024 * 1024  # 100MB chunks
        else:  # >= 1GB
            return min(base_chunk, 100 * 1024 * 1024)  # Max 100MB chunks
    def _migrate_file_collaborators_safe(self, source_file_id: str, dest_file_id: str,
                                         file_name: str, source_drive, dest_drive) -> Dict:
        """
        Migrate file collaborators/permissions with SAFE error handling
        CRITICAL FIX: Properly initialize permissions variable before exception handling
        """
        result = {
            'migrated': 0,
            'failed': 0,
            'external': 0,
            'internal': 0,
            'skipped': 0
        }
        
        permissions = []  # CRITICAL FIX: Initialize before try block
        
        try:
            # Get all permissions from source file
            response = source_drive.permissions().list(
                fileId=source_file_id,
                fields='permissions(id,type,role,emailAddress,domain,displayName,expirationTime)',
                supportsAllDrives=True
            ).execute()
            
            permissions = response.get('permissions', [])
            
            if not permissions or len(permissions) <= 1:
                # Only owner permission, nothing to migrate
                return result
            
            # Use enhanced permissions migrator
            try:
                from permissions_migrator import EnhancedPermissionsMigrator
                
                perm_migrator = EnhancedPermissionsMigrator(
                    source_drive,
                    dest_drive,
                    self.config.SOURCE_DOMAIN,
                    self.config.DEST_DOMAIN
                )
                
                perm_result = perm_migrator.migrate_permissions(
                    source_file_id,
                    dest_file_id,
                    permissions
                )
                
                result['migrated'] = perm_result.get('migrated', 0)
                result['failed'] = perm_result.get('failed', 0)
                result['external'] = perm_result.get('external_users', 0)
                result['internal'] = perm_result.get('internal_users', 0)
                result['skipped'] = perm_result.get('skipped', 0)
                
                if result['migrated'] > 0:
                    logger.debug(f"✓ Migrated {result['migrated']} collaborators for: {file_name}")
                
            except ImportError:
                logger.warning(f"EnhancedPermissionsMigrator not available, skipping permissions for {file_name}")
                result['skipped'] = len(permissions) - 1 if len(permissions) > 1 else 0
            
        except (socket.timeout, ConnectionResetError, ConnectionError, OSError) as conn_error:
            # Connection errors during permission migration
            logger.warning(f"Connection error migrating collaborators for {file_name}: {conn_error}")
            result['failed'] = len(permissions) - 1 if len(permissions) > 1 else 0
            
        except HttpError as http_error:
            # HTTP errors during permission listing
            logger.warning(f"HTTP error migrating collaborators for {file_name}: {http_error}")
            result['failed'] = len(permissions) - 1 if len(permissions) > 1 else 0
            
        except Exception as e:
            # Any other unexpected errors
            logger.warning(f"Failed to migrate collaborators for {file_name}: {e}")
            result['failed'] = len(permissions) - 1 if len(permissions) > 1 else 0
        
        return result

    def _migrate_folder_collaborators_safe(self, source_folder_id: str, dest_folder_id: str,
                                           folder_name: str, source_drive, dest_drive) -> Dict:
        """
        Migrate folder collaborators/permissions with SAFE error handling
        CRITICAL FIX: Properly initialize permissions variable
        """
        result = {
            'migrated': 0,
            'failed': 0
        }
        
        permissions = []  # CRITICAL FIX: Initialize before try block
        
        try:
            # Get all permissions from source folder
            response = source_drive.permissions().list(
                fileId=source_folder_id,
                fields='permissions(id,type,role,emailAddress,domain,displayName)',
                supportsAllDrives=True
            ).execute()
            
            permissions = response.get('permissions', [])
            
            if not permissions or len(permissions) <= 1:
                # Only owner permission, nothing to migrate
                return result
            
            # Use enhanced permissions migrator
            try:
                from permissions_migrator import EnhancedPermissionsMigrator
                
                perm_migrator = EnhancedPermissionsMigrator(
                    source_drive,
                    dest_drive,
                    self.config.SOURCE_DOMAIN,
                    self.config.DEST_DOMAIN
                )
                
                perm_result = perm_migrator.migrate_permissions(
                    source_folder_id,
                    dest_folder_id,
                    permissions
                )
                
                result['migrated'] = perm_result.get('migrated', 0)
                result['failed'] = perm_result.get('failed', 0)
                
                if result['migrated'] > 0:
                    logger.debug(f"✓ Migrated {result['migrated']} folder collaborators: {folder_name}")
                    
            except ImportError:
                logger.warning(f"EnhancedPermissionsMigrator not available")
                result['failed'] = len(permissions) - 1 if len(permissions) > 1 else 0
            
        except (socket.timeout, ConnectionResetError, ConnectionError, OSError) as conn_error:
            logger.warning(f"Connection error migrating folder collaborators for {folder_name}: {conn_error}")
            result['failed'] = len(permissions) - 1 if len(permissions) > 1 else 0
            
        except HttpError as http_error:
            logger.warning(f"HTTP error migrating folder collaborators for {folder_name}: {http_error}")
            result['failed'] = len(permissions) - 1 if len(permissions) > 1 else 0
            
        except Exception as e:
            logger.warning(f"Failed to migrate folder collaborators for {folder_name}: {e}")
            result['failed'] = len(permissions) - 1 if len(permissions) > 1 else 0
        
        return result
    def generate_report(self, summary: Dict, output_file: str):
        """
        Generate comprehensive migration report with detailed failure analysis
        """
        try:
            # Add additional statistics
            summary['statistics'] = {
                'retry_count': self.stats.get('retry_count', 0),
                'bytes_transferred': self.stats.get('bytes_transferred', 0),
                'average_accuracy': summary.get('accuracy_rate', 0),
                'folders_created': summary.get('total_folders_created', 0),
                'folders_failed': summary.get('total_folders_failed', 0),
                'collaborators_migrated': summary.get('total_collaborators_migrated', 0),
                'external_collaborators': summary.get('total_external_collaborators', 0),
                'connection_resets': self.stats.get('connection_resets', 0),
                'empty_downloads': self.stats.get('empty_downloads', 0)
            }
            
            # Analyze failure patterns
            failure_analysis = self._analyze_failures(summary.get('detailed_failures', []))
            summary['failure_analysis'] = failure_analysis
            
            # Write JSON report
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            
            logger.info(f"JSON report generated: {output_file}")
            
            # Generate human-readable text report
            txt_file = str(Path(output_file).with_suffix('.txt'))
            self._generate_text_report(summary, txt_file)
            
            # Generate failure details report if there are failures
            if summary.get('detailed_failures'):
                failures_file = str(Path(output_file).parent / f"failures_{Path(output_file).stem}.csv")
                self._generate_failures_csv(summary['detailed_failures'], failures_file)
            
        except Exception as e:
            logger.error(f"Failed to generate report: {e}")

    def _analyze_failures(self, failures: List[Dict]) -> Dict:
        """
        Analyze failure patterns to identify common issues
        Enhanced with new error types
        """
        if not failures:
            return {
                'total_failures': 0,
                'by_error_type': {},
                'by_mime_type': {},
                'recommendations': []
            }
        
        analysis = {
            'total_failures': len(failures),
            'by_error_type': {},
            'by_mime_type': {},
            'by_user': {},
            'large_files': [],
            'connection_issues': 0,
            'recommendations': []
        }
        
        # Count by error type and mime type
        for failure in failures:
            error_type = failure.get('error_type', 'unknown')
            mime_type = failure.get('mime_type', 'unknown')
            user = failure.get('user', 'unknown')
            file_size = failure.get('size', 0)
            
            analysis['by_error_type'][error_type] = analysis['by_error_type'].get(error_type, 0) + 1
            analysis['by_mime_type'][mime_type] = analysis['by_mime_type'].get(mime_type, 0) + 1
            analysis['by_user'][user] = analysis['by_user'].get(user, 0) + 1
            
            # Track connection-related issues
            if error_type in ['connection_reset', 'connection_error', 'timeout']:
                analysis['connection_issues'] += 1
            
            # Track large files that failed
            if file_size > 100 * 1024 * 1024:  # > 100MB
                analysis['large_files'].append({
                    'file': failure.get('file', 'Unknown'),
                    'size': file_size,
                    'mime_type': mime_type,
                    'error_type': error_type
                })
        
        # Sort large files by size
        analysis['large_files'] = sorted(analysis['large_files'], key=lambda x: x['size'], reverse=True)[:10]
        
        # Generate recommendations based on failure patterns
        if analysis.get('connection_issues', 0) > 0:
            analysis['recommendations'].append(
                f"Found {analysis['connection_issues']} connection-related failures (timeouts, connection resets). "
                "This is common with large files (>100MB) or unstable network. Consider: "
                "(1) Running migration during off-peak hours, "
                "(2) Reducing max_workers to decrease network load, "
                "(3) Migrating large files separately with increased timeout values."
            )
        
        if analysis['by_error_type'].get('empty_download', 0) > 0 or analysis['by_error_type'].get('empty_export', 0) > 0:
            count = analysis['by_error_type'].get('empty_download', 0) + analysis['by_error_type'].get('empty_export', 0)
            analysis['recommendations'].append(
                f"Found {count} files that downloaded as empty. These files may be: "
                "(1) Corrupted in source, (2) Zero-byte files, "
                "(3) Files with special permissions that prevent download. "
                "Manually verify these files in the source Drive."
            )
        
        if analysis['by_error_type'].get('invalid_mime_type', 0) > 0 or analysis['by_error_type'].get('bad_request', 0) > 0:
            count = analysis['by_error_type'].get('invalid_mime_type', 0) + analysis['by_error_type'].get('bad_request', 0)
            analysis['recommendations'].append(
                f"Found {count} files with invalid MIME type errors. "
                "Google Apps Script files cannot be re-imported as native format. "
                "They will be saved as JSON exports instead."
            )
        
        if analysis['by_error_type'].get('unknown_mime_type', 0) > 0:
            count = analysis['by_error_type'].get('unknown_mime_type', 0)
            unknown_types = set()
            for f in failures:
                if f.get('error_type') == 'unknown_mime_type':
                    unknown_types.add(f.get('mime_type', 'unknown'))
            
            analysis['recommendations'].append(
                f"Found {count} files with unknown Google Workspace types: {', '.join(unknown_types)}. "
                "Google Drive videos (application/vnd.google-apps.video) are not actual files but pointers/links "
                "and cannot be migrated. Other unknown types may need manual handling."
            )
        
        if analysis['by_error_type'].get('size_limit_exceeded', 0) > 0 or analysis['by_error_type'].get('export_size_exceeded', 0) > 0:
            count = analysis['by_error_type'].get('size_limit_exceeded', 0) + analysis['by_error_type'].get('export_size_exceeded', 0)
            analysis['recommendations'].append(
                f"Found {count} files exceeding Google's export size limits. "
                "Google Sheets over ~50-100MB and very large Docs/Slides hit export limits. "
                "Options: (1) Manually download via Google Takeout, "
                "(2) Split large sheets into smaller files, "
                "(3) Share the original file with destination user instead."
            )
        
        if analysis['by_error_type'].get('permission_denied', 0) > 0:
            analysis['recommendations'].append(
                "Permission denied errors found. Verify: "
                "(1) Service account has domain-wide delegation enabled, "
                "(2) All required Drive API scopes are authorized, "
                "(3) Source and destination users exist and are active."
            )
        
        if analysis['by_error_type'].get('not_exportable', 0) > 0:
            analysis['recommendations'].append(
                "Some file types cannot be migrated via API: "
                "Google Sites, Shortcuts, and Drive Video links. "
                "These require manual recreation or alternative approaches."
            )
        
        if analysis['by_error_type'].get('rate_limit', 0) > 0:
            analysis['recommendations'].append(
                "Rate limit errors detected. Consider: "
                "(1) Reducing max_workers parameter (try 2-3 instead of 5), "
                "(2) Adding longer delays between retries, "
                "(3) Migrating in smaller batches."
            )
        
        if analysis['by_error_type'].get('quota_exceeded', 0) > 0:
            analysis['recommendations'].append(
                "Storage quota exceeded errors. Check destination account storage limits. "
                "You may need to increase storage quota or clean up existing files before migration."
            )
        
        if len(analysis['large_files']) > 0:
            total_large_size = sum(f['size'] for f in analysis['large_files'])
            analysis['recommendations'].append(
                f"Found {len(analysis['large_files'])} large files (>100MB) that failed, "
                f"totaling {self._format_bytes(total_large_size)}. "
                "Very large files are more prone to connection issues. "
                "Consider migrating these separately with: "
                "(1) Increased timeout values, (2) Single-threaded execution (max_workers=1), "
                "(3) Stable, high-bandwidth network connection."
            )
        
        return analysis

    def _generate_text_report(self, summary: Dict, output_file: str):
        """
        Generate human-readable text report with enhanced statistics
        """
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("="*80 + "\n")
                f.write("GOOGLE WORKSPACE DRIVE MIGRATION REPORT\n")
                f.write("="*80 + "\n\n")
                
                # Header information
                f.write(f"Migration Date: {summary.get('start_time', 'Unknown')}\n")
                f.write(f"Duration: {summary.get('duration_seconds', 0):.2f} seconds ({self._format_duration(summary.get('duration_seconds', 0))})\n")
                f.write(f"Overall Accuracy: {summary.get('accuracy_rate', 0):.2f}%\n\n")
                
                # Users summary
                f.write("USERS SUMMARY:\n")
                f.write("-"*80 + "\n")
                f.write(f"  Total Users: {summary.get('total_users', 0)}\n")
                f.write(f"  Completed: {summary.get('completed_users', 0)}\n")
                f.write(f"  Failed: {summary.get('failed_users', 0)}\n\n")
                
                # Files summary
                f.write("FILES SUMMARY:\n")
                f.write("-"*80 + "\n")
                f.write(f"  Total Migrated: {summary.get('total_files_migrated', 0)}\n")
                f.write(f"  Failed: {summary.get('total_files_failed', 0)}\n")
                f.write(f"  Skipped: {summary.get('total_files_skipped', 0)}\n")
                f.write(f"  Retries: {summary.get('statistics', {}).get('retry_count', 0)}\n")
                f.write(f"  Bytes Transferred: {self._format_bytes(summary.get('statistics', {}).get('bytes_transferred', 0))}\n")
                
                stats = summary.get('statistics', {})
                if stats.get('connection_resets', 0) > 0:
                    f.write(f"  Connection Resets: {stats['connection_resets']}\n")
                if stats.get('empty_downloads', 0) > 0:
                    f.write(f"  Empty Downloads: {stats['empty_downloads']}\n")
                f.write("\n")
                
                # Folders summary
                f.write("FOLDERS SUMMARY:\n")
                f.write("-"*80 + "\n")
                f.write(f"  Created: {summary.get('total_folders_created', 0)}\n")
                f.write(f"  Failed: {summary.get('total_folders_failed', 0)}\n")
                
                if summary.get('total_folders_created', 0) > 0:
                    success_rate = (summary.get('total_folders_created', 0) / 
                                  (summary.get('total_folders_created', 0) + summary.get('total_folders_failed', 0))) * 100
                    f.write(f"  Success Rate: {success_rate:.2f}%\n")
                f.write("\n")
                
                # Collaborators summary
                f.write("COLLABORATORS SUMMARY:\n")
                f.write("-"*80 + "\n")
                f.write(f"  Total Migrated: {summary.get('total_collaborators_migrated', 0)}\n")
                f.write(f"  External: {summary.get('total_external_collaborators', 0)}\n\n")
                
                # Failure analysis
                if summary.get('failure_analysis'):
                    f.write("FAILURE ANALYSIS:\n")
                    f.write("-"*80 + "\n")
                    
                    analysis = summary['failure_analysis']
                    f.write(f"  Total Failures: {analysis['total_failures']}\n")
                    
                    if analysis.get('connection_issues', 0) > 0:
                        f.write(f"  Connection Issues: {analysis['connection_issues']}\n")
                    f.write("\n")
                    
                    if analysis['by_error_type']:
                        f.write("  By Error Type:\n")
                        for error_type, count in sorted(analysis['by_error_type'].items(), 
                                                       key=lambda x: x[1], reverse=True):
                            percentage = (count / analysis['total_failures']) * 100
                            f.write(f"    - {error_type}: {count} ({percentage:.1f}%)\n")
                        f.write("\n")
                    
                    if analysis['by_mime_type']:
                        f.write("  By File Type (Top 10):\n")
                        for mime_type, count in sorted(analysis['by_mime_type'].items(), 
                                                      key=lambda x: x[1], reverse=True)[:10]:
                            f.write(f"    - {mime_type}: {count}\n")
                        f.write("\n")
                    
                    if analysis.get('large_files'):
                        f.write("  Large Files That Failed (Top 10):\n")
                        for lf in analysis['large_files']:
                            f.write(f"    - {lf['file']}: {self._format_bytes(lf['size'])} "
                                  f"({lf['mime_type']}) - {lf['error_type']}\n")
                        f.write("\n")
                    
                    if analysis['recommendations']:
                        f.write("  RECOMMENDATIONS:\n")
                        for i, rec in enumerate(analysis['recommendations'], 1):
                            # Wrap long recommendations
                            wrapped = self._wrap_text(rec, 76)
                            f.write(f"    {i}. {wrapped}\n\n")
                
                # Per-user details
                f.write("USER DETAILS:\n")
                f.write("-"*80 + "\n")
                for user_result in summary.get('user_results', []):
                    f.write(f"\n{user_result['source_email']} -> {user_result['dest_email']}\n")
                    f.write(f"  Status: {user_result['status']}\n")
                    f.write(f"  Accuracy: {user_result.get('accuracy_rate', 0):.2f}%\n")
                    f.write(f"  Files: {user_result['files_migrated']}/{user_result['files_total']} migrated, "
                           f"{user_result['files_failed']} failed, {user_result['files_skipped']} skipped\n")
                    f.write(f"  Folders: {user_result.get('folders_created', 0)}/{user_result.get('folders_total', 0)} created\n")
                    f.write(f"  Types: Native={user_result.get('native_formats', 0)}, ")
                    f.write(f"Converted={user_result.get('converted_formats', 0)}, ")
                    f.write(f"Standard={user_result.get('standard_files', 0)}\n")
                    f.write(f"  Collaborators: {user_result.get('collaborators_migrated', 0)} "
                           f"({user_result.get('external_collaborators', 0)} external)\n")
                    f.write(f"  Data Transferred: {self._format_bytes(user_result.get('bytes_transferred', 0))}\n")
                    
                    if user_result.get('warnings'):
                        f.write(f"  Warnings:\n")
                        for warning in user_result['warnings']:
                            f.write(f"    - {warning}\n")
                    
                    if user_result.get('errors'):
                        f.write(f"  Errors: {len(user_result['errors'])}\n")
                        # Show first 5 errors
                        for error in user_result['errors'][:5]:
                            f.write(f"    - {error.get('file', 'Unknown')}: {error.get('error_type', 'unknown')}\n")
                            f.write(f"      {error.get('error', 'No details')[:100]}\n")
                        if len(user_result['errors']) > 5:
                            f.write(f"    ... and {len(user_result['errors']) - 5} more errors (see failures CSV)\n")
                f.write("\n" + "="*80 + "\n")
                f.write("End of Report\n")
                f.write("="*80 + "\n")
            logger.info(f"Text report generated: {output_file}")
        except Exception as e:
            logger.error(f"Failed to generate text report: {e}")
    def _wrap_text(self, text: str, width: int) -> str:
    
        words = text.split()
        lines = []
        current_line = []
        current_length = 0
        
        for word in words:
            if current_length + len(word) + 1 <= width:
                current_line.append(word)
                current_length += len(word) + 1
            else:
                lines.append(' '.join(current_line))
                current_line = [word]
                current_length = len(word)
        
        if current_line:
            lines.append(' '.join(current_line))
        
        return '\n       '.join(lines)
    def _generate_failures_csv(self, failures: List[Dict], output_file: str):
    
        try:
            import csv
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Header
                writer.writerow([
                    'User',
                    'File Name',
                    'File ID',
                    'MIME Type',
                    'File Size (Bytes)',
                    'File Size (Human)',
                    'Error Type',
                    'Error Message',
                    'Retry Attempts'
                ])
                
                # Data
                for failure in failures:
                    file_size = failure.get('size', 0)
                    writer.writerow([
                        failure.get('user', 'Unknown'),
                        failure.get('file', 'Unknown'),
                        failure.get('file_id', 'Unknown'),
                        failure.get('mime_type', 'Unknown'),
                        file_size,
                        self._format_bytes(file_size),
                        failure.get('error_type', 'unknown'),
                        failure.get('error', 'No details')[:500],  # Truncate very long errors
                        failure.get('retry_attempts', 0)
                    ])
            
            logger.info(f"Failures CSV generated: {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to generate failures CSV: {e}")
    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes to human-readable string"""
        if bytes_value == 0:
            return "0 B"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f} PB"

    def _format_duration(self, seconds: float) -> str:
        """Format duration in seconds to human-readable string"""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.2f}h"

    def validate_migration(self, user_mapping: Dict[str, str]) -> Dict:
        """
        Validate migration results
        
        Args:
            user_mapping: User mapping dictionary
            
        Returns:
            Validation results
        """
        logger.info("Starting migration validation...")
        
        validation_results = {
            'total_users_validated': 0,
            'users_valid': 0,
            'users_invalid': 0,
            'issues': []
        }
        
        for source_email, dest_email in user_mapping.items():
            try:
                validation_results['total_users_validated'] += 1
                # Note: Detailed validation should use the MigrationValidator class
                # This is a placeholder for state-based validation
                
            except Exception as e:
                logger.error(f"Validation failed for {source_email}: {e}")
                validation_results['users_invalid'] += 1
        
        logger.info(f"Validation complete: {validation_results['users_valid']}/{validation_results['total_users_validated']} users validated")
        
        return validation_results

    def get_migration_stats(self) -> Dict:
        """
        Get current migration statistics
        
        Returns:
            Statistics dictionary
        """
        total_attempted = self.stats['total_files']
        accuracy = 0.0
        if total_attempted > 0:
            accuracy = (self.stats['successful'] / total_attempted) * 100
        
        return {
            'total_files': self.stats['total_files'],
            'successful': self.stats['successful'],
            'failed': self.stats['failed'],
            'skipped': self.stats['skipped'],
            'folders_created': self.stats['folders_created'],
            'folders_failed': self.stats['folders_failed'],
            'retry_count': self.stats['retry_count'],
            'bytes_transferred': self.stats['bytes_transferred'],
            'collaborators_migrated': self.stats['collaborators_migrated'],
            'external_collaborators': self.stats['external_collaborators'],
            'accuracy_rate': accuracy
        }

    def _detect_mime_type(self, file_name: str, content: bytes = None) -> Optional[str]:
        """
        Detect MIME type from file name
        
        Args:
            file_name: File name
            content: File content (optional, not used but kept for compatibility)
            
        Returns:
            Detected MIME type
        """
        mime_type, _ = mimetypes.guess_type(file_name)
        
        if mime_type:
            return mime_type
        
        # Fallback: Common extensions
        ext = Path(file_name).suffix.lower()
        common_types = {
            '.pdf': 'application/pdf',
            '.doc': 'application/msword',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.xls': 'application/vnd.ms-excel',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.ppt': 'application/vnd.ms-powerpoint',
            '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            '.txt': 'text/plain',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif',
            '.bmp': 'image/bmp',
            '.svg': 'image/svg+xml',
            '.mp4': 'video/mp4',
            '.avi': 'video/x-msvideo',
            '.mov': 'video/quicktime',
            '.mp3': 'audio/mpeg',
            '.wav': 'audio/wav',
            '.zip': 'application/zip',
            '.rar': 'application/x-rar-compressed',
            '.7z': 'application/x-7z-compressed',
            '.tar': 'application/x-tar',
            '.gz': 'application/gzip',
        }
        
        return common_types.get(ext, 'application/octet-stream')
    
    def _calculate_file_hash(self, content: bytes) -> str:
        return hashlib.md5(content).hexdigest()