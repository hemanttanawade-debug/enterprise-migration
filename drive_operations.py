"""
Drive operations module for file and folder management
"""
import logging
import io
import time
from typing import List, Dict, Optional, Tuple
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload, MediaIoBaseUpload
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)


class DriveOperations:
    """Handles Drive API operations"""
    
    def __init__(self, drive_service, user_email=None):
        """
        Initialize Drive operations
        
        Args:
            drive_service: Authenticated Drive API service
            user_email: User email for delegation (if using service account)
        """
        self.drive = drive_service
        self.user_email = user_email
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((HttpError, ConnectionError))
    )
    def list_files(self, user_email: str, page_size: int = 100, 
                   query: Optional[str] = None, include_shared: bool = False) -> List[Dict]:
        """
        List all files for a user
        
        Args:
            user_email: User email address
            page_size: Number of results per page
            query: Additional query filter
            include_shared: If True, include files shared with user (not owned)
            
        Returns:
            List of file metadata dictionaries
        """
        files = []
        page_token = None
        
        # Base query - files owned by user only (default)
        if include_shared:
            # Include both owned and shared files
            base_query = f"('{user_email}' in owners or sharedWithMe) and trashed=false"
        else:
            # Only files owned by user
            base_query = f"'{user_email}' in owners and trashed=false"
        
        if query:
            base_query = f"{base_query} and ({query})"
        
        try:
            while True:
                logger.debug(f"Fetching files for {user_email}...")
                
                response = self.drive.files().list(
                    q=base_query,
                    pageSize=page_size,
                    pageToken=page_token,
                    fields='nextPageToken,files(id,name,mimeType,size,createdTime,'
                           'modifiedTime,parents,owners,permissions,webViewLink,'
                           'capabilities,shared,fileExtension,md5Checksum)',
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True
                ).execute()
                
                batch_files = response.get('files', [])
                
                # Filter to only owned files if not including shared
                if not include_shared:
                    batch_files = [
                        f for f in batch_files
                        if any(owner.get('emailAddress') == user_email for owner in f.get('owners', []))
                    ]
                
                files.extend(batch_files)
                
                logger.debug(f"Retrieved {len(batch_files)} files (total: {len(files)})")
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
            
            logger.info(f"Total files for {user_email}: {len(files)}")
            return files
            
        except HttpError as e:
            logger.error(f"Error listing files for {user_email}: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def get_file_metadata(self, file_id: str) -> Optional[Dict]:
        """
        Get detailed file metadata
        
        Args:
            file_id: File ID
            
        Returns:
            File metadata dictionary or None
        """
        try:
            file_metadata = self.drive.files().get(
                fileId=file_id,
                fields='id,name,mimeType,size,createdTime,modifiedTime,parents,'
                       'owners,permissions,webViewLink,shared,fileExtension,md5Checksum',
                supportsAllDrives=True
            ).execute()
            return file_metadata
        except HttpError as e:
            logger.error(f"Error getting metadata for file {file_id}: {e}")
            return None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=20)
    )
    def download_file(self, file_id: str, file_name: str) -> Tuple[bool, Optional[bytes]]:
        """
        Download file content
        
        Args:
            file_id: File ID
            file_name: File name for logging
            
        Returns:
            Tuple of (success, file_content)
        """
        try:
            request = self.drive.files().get_media(fileId=file_id)
            file_buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(file_buffer, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logger.debug(f"Download {file_name}: {int(status.progress() * 100)}%")
            
            file_buffer.seek(0)
            logger.info(f"Downloaded: {file_name}")
            return True, file_buffer.read()
            
        except HttpError as e:
            if e.resp.status == 403:
                logger.warning(f"Cannot download {file_name}: Permission denied or Google Doc")
                return False, None
            logger.error(f"Error downloading {file_name}: {e}")
            return False, None
    
    def export_google_doc(self, file_id: str, mime_type: str, export_format: str = None) -> Tuple[bool, Optional[bytes]]:
        """
        Export Google Workspace document
        
        Args:
            file_id: File ID
            mime_type: Source MIME type
            export_format: Target export format (MIME type or extension)
            
        Returns:
            Tuple of (success, file_content)
        """
        export_mapping = {
            'application/vnd.google-apps.document': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'application/vnd.google-apps.spreadsheet': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'application/vnd.google-apps.presentation': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            'application/vnd.google-apps.drawing': 'application/pdf',
        }
        
        # Use provided export_format or map from source type
        if export_format and export_format.startswith('application/'):
            target_mime = export_format
        else:
            target_mime = export_mapping.get(mime_type, 'application/pdf')
        
        try:
            request = self.drive.files().export_media(
                fileId=file_id,
                mimeType=target_mime
            )
            file_buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(file_buffer, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logger.debug(f"Export progress: {int(status.progress() * 100)}%")
            
            file_buffer.seek(0)
            logger.info(f"Exported Google Workspace file as {target_mime}")
            return True, file_buffer.read()
            
        except HttpError as e:
            logger.error(f"Error exporting Google Doc {file_id}: {e}")
            return False, None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=20)
    )
    def upload_file(self, file_content: bytes, file_name: str, 
                   mime_type: str, parent_id: Optional[str] = None) -> Optional[str]:
        """
        Upload file to Drive
        
        Args:
            file_content: File content bytes
            file_name: File name
            mime_type: MIME type
            parent_id: Parent folder ID
            
        Returns:
            Created file ID or None
        """
        try:
            file_metadata = {'name': file_name}
            if parent_id:
                file_metadata['parents'] = [parent_id]
            
            media = MediaIoBaseUpload(
                io.BytesIO(file_content),
                mimetype=mime_type,
                resumable=True
            )
            
            file = self.drive.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name',
                supportsAllDrives=True
            ).execute()
            
            logger.info(f"Uploaded: {file_name} (ID: {file.get('id')})")
            return file.get('id')
            
        except HttpError as e:
            logger.error(f"Error uploading {file_name}: {e}")
            return None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def create_folder(self, folder_name: str, parent_id: Optional[str] = None) -> Optional[str]:
        """
        Create a folder in Drive
        
        Args:
            folder_name: Folder name
            parent_id: Parent folder ID
            
        Returns:
            Created folder ID or None
        """
        try:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_id:
                file_metadata['parents'] = [parent_id]
            
            folder = self.drive.files().create(
                body=file_metadata,
                fields='id,name',
                supportsAllDrives=True
            ).execute()
            
            logger.info(f"Created folder: {folder_name} (ID: {folder.get('id')})")
            return folder.get('id')
            
        except HttpError as e:
            logger.error(f"Error creating folder {folder_name}: {e}")
            return None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def copy_file(self, source_file_id: str, new_name: str, 
                 parent_id: Optional[str] = None) -> Optional[str]:
        """
        Copy a file within Drive
        
        Args:
            source_file_id: Source file ID
            new_name: New file name
            parent_id: Destination parent folder ID
            
        Returns:
            Copied file ID or None
        """
        try:
            file_metadata = {'name': new_name}
            if parent_id:
                file_metadata['parents'] = [parent_id]
            
            copied_file = self.drive.files().copy(
                fileId=source_file_id,
                body=file_metadata,
                fields='id,name',
                supportsAllDrives=True
            ).execute()
            
            logger.info(f"Copied file: {new_name} (ID: {copied_file.get('id')})")
            return copied_file.get('id')
            
        except HttpError as e:
            logger.error(f"Error copying file {source_file_id}: {e}")
            return None
    
    def transfer_ownership(self, file_id: str, new_owner_email: str) -> bool:
        """
        Transfer file ownership
        
        Args:
            file_id: File ID
            new_owner_email: New owner email address
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # First, add the new owner with writer permission
            permission = {
                'type': 'user',
                'role': 'writer',
                'emailAddress': new_owner_email
            }
            
            result = self.drive.permissions().create(
                fileId=file_id,
                body=permission,
                sendNotificationEmail=False,
                supportsAllDrives=True,
                fields='id'
            ).execute()
            
            permission_id = result.get('id')
            
            if not permission_id:
                logger.error(f"Failed to get permission ID for {new_owner_email}")
                return False
            
            # Now update that permission to owner role
            owner_permission = {
                'role': 'owner'
            }
            
            self.drive.permissions().update(
                fileId=file_id,
                permissionId=permission_id,
                body=owner_permission,
                transferOwnership=True,
                supportsAllDrives=True
            ).execute()
            
            logger.info(f"Transferred ownership of {file_id} to {new_owner_email}")
            return True
            
        except HttpError as e:
            if e.resp.status == 403:
                logger.warning(f"Permission denied transferring ownership of {file_id}: {e}")
            else:
                logger.error(f"Error transferring ownership of {file_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error transferring ownership of {file_id}: {e}")
            return False
    
    def get_permission_id(self, file_id: str, email: str) -> Optional[str]:
        """Get permission ID for a user"""
        try:
            permissions = self.drive.permissions().list(
                fileId=file_id,
                fields='permissions(id,emailAddress)',
                supportsAllDrives=True
            ).execute()
            
            for perm in permissions.get('permissions', []):
                if perm.get('emailAddress') == email:
                    return perm.get('id')
            
            return None
        except HttpError:
            return None


