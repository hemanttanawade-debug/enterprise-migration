"""
Enhanced Permissions migration module
Handles internal vs external users intelligently
"""
import logging
from typing import Dict, List, Optional
from googleapiclient.errors import HttpError
import time

logger = logging.getLogger(__name__)


# Backward compatibility alias
PermissionsMigrator = None  # Will be set after class definition


class EnhancedPermissionsMigrator:
    """Handles migration of file/folder permissions with smart domain mapping"""
    
    def __init__(self, source_drive, dest_drive, source_domain: str, dest_domain: str):
        """
        Initialize enhanced permissions migrator
        
        Args:
            source_drive: Source Drive API service
            dest_drive: Destination Drive API service
            source_domain: Source domain (e.g., 'dev.shivaami.in')
            dest_domain: Destination domain (e.g., 'demo.shivaami.in')
        """
        self.source_drive = source_drive
        self.dest_drive = dest_drive
        self.source_domain = source_domain
        self.dest_domain = dest_domain
    
    def migrate_permissions(self, source_file_id: str, dest_file_id: str, 
                          source_permissions: List[Dict]) -> Dict:
        """
        Migrate permissions with intelligent handling of internal vs external users
        
        CORRECTED Logic:
        - Users from source domain who are NOT the owner: Keep as EXTERNAL (no mapping)
        - Users from other domains: Keep as external (no mapping)
        - Only check if they exist in destination domain as a fallback
        
        Example:
        - priyesh@dev.shivaami.in (owner) → developers@demo.shivaami.in (handled by ownership transfer)
        - seema@dev.shivaami.in (editor) → seema@dev.shivaami.in (EXTERNAL collaborator)
        - siddhi@dev.shivaami.in (viewer) → Try siddhi@demo.shivaami.in first, fallback to siddhi@dev.shivaami.in
        - john@external.com (viewer) → john@external.com (stays external)
        
        Args:
            source_file_id: Source file ID
            dest_file_id: Destination file ID
            source_permissions: List of source permissions
            
        Returns:
            Migration result dictionary
        """
        result = {
            'total_permissions': len(source_permissions),
            'migrated': 0,
            'skipped': 0,
            'failed': 0,
            'external_users': 0,
            'internal_users': 0,
            'details': []
        }
        
        for permission in source_permissions:
            perm_type = permission.get('type')
            role = permission.get('role')
            email = permission.get('emailAddress')
            domain = permission.get('domain')
            
            # Skip owner permission (will be set separately during file creation)
            if role == 'owner':
                result['skipped'] += 1
                result['details'].append({
                    'email': email,
                    'role': role,
                    'status': 'skipped',
                    'reason': 'Owner permission handled separately'
                })
                continue
            
            # ALL collaborators are treated as EXTERNAL by default
            # We'll try destination domain first, but keep source domain as fallback
            target_email = email
            is_internal = False
            
            if email and '@' in email:
                user_domain = email.split('@')[1]
                local_part = email.split('@')[0]
                
                if user_domain == self.source_domain:
                    # User from source domain - try destination domain first
                    dest_candidate = f"{local_part}@{self.dest_domain}"
                    
                    # Try to add with destination domain email
                    success_dest, error_dest = self._create_permission(
                        dest_file_id,
                        perm_type,
                        role,
                        dest_candidate,
                        domain,
                        True  # Mark as internal attempt
                    )
                    
                    if success_dest:
                        # User exists in destination domain
                        target_email = dest_candidate
                        is_internal = True
                        result['internal_users'] += 1
                        result['migrated'] += 1
                        result['details'].append({
                            'original_email': email,
                            'target_email': target_email,
                            'role': role,
                            'type': perm_type,
                            'is_internal': True,
                            'status': 'success',
                            'note': 'User exists in destination domain'
                        })
                        logger.debug(f"✓ Migrated as internal: {target_email} as {role}")
                    else:
                        # User doesn't exist in destination - keep as external with source domain
                        target_email = email  # Keep original source domain email
                        is_internal = False
                        result['external_users'] += 1
                        
                        success_ext, error_ext = self._create_permission(
                            dest_file_id,
                            perm_type,
                            role,
                            target_email,
                            domain,
                            False  # Mark as external
                        )
                        
                        if success_ext:
                            result['migrated'] += 1
                            result['details'].append({
                                'original_email': email,
                                'target_email': target_email,
                                'role': role,
                                'type': perm_type,
                                'is_internal': False,
                                'status': 'success',
                                'note': f'Kept as external (dest user not found: {dest_candidate})'
                            })
                            logger.debug(f"✓ Migrated as external: {target_email} as {role}")
                        else:
                            result['failed'] += 1
                            result['details'].append({
                                'original_email': email,
                                'target_email': target_email,
                                'role': role,
                                'type': perm_type,
                                'is_internal': False,
                                'status': 'failed',
                                'error': error_ext
                            })
                            logger.warning(f"✗ Failed to migrate: {target_email} - {error_ext}")
                    
                    # Skip to next permission - we've handled this one
                    time.sleep(0.1)
                    continue
                else:
                    # External domain user - keep as is
                    result['external_users'] += 1
                    logger.debug(f"External user from different domain: {email}")
            
            # Create permission for external users (non-source domain)
            success, error = self._create_permission(
                dest_file_id,
                perm_type,
                role,
                target_email,
                domain,
                is_internal
            )
            
            if success:
                result['migrated'] += 1
                result['details'].append({
                    'original_email': email,
                    'target_email': target_email,
                    'role': role,
                    'type': perm_type,
                    'is_internal': is_internal,
                    'status': 'success'
                })
                logger.debug(f"✓ Migrated permission: {target_email} as {role} (external)")
            else:
                result['failed'] += 1
                result['details'].append({
                    'original_email': email,
                    'target_email': target_email,
                    'role': role,
                    'type': perm_type,
                    'is_internal': is_internal,
                    'status': 'failed',
                    'error': error
                })
                logger.warning(f"✗ Failed to migrate permission: {target_email} - {error}")
            
            # Rate limiting
            time.sleep(0.1)
        
        logger.info(f"Permission migration summary: "
                   f"{result['migrated']}/{result['total_permissions']} migrated "
                   f"({result['internal_users']} internal, {result['external_users']} external)")
        
        return result
    
    def _create_permission(self, file_id: str, perm_type: str, role: str, 
                          email: Optional[str], domain: Optional[str],
                          is_internal: bool) -> tuple:
        """
        Create a permission on destination file
        
        Args:
            file_id: Destination file ID
            perm_type: Permission type (user, group, domain, anyone)
            role: Permission role (reader, writer, commenter)
            email: Email address
            domain: Domain (for domain-wide permissions)
            is_internal: Whether this is an internal user
        
        Returns:
            Tuple of (success, error_message)
        """
        try:
            permission = {
                'type': perm_type,
                'role': role
            }
            
            # Configure based on permission type
            if perm_type == 'user' and email:
                permission['emailAddress'] = email
                # For external users, we need to handle potential "no account" errors
                send_notification = False
                
            elif perm_type == 'group' and email:
                permission['emailAddress'] = email
                send_notification = False
                
            elif perm_type == 'domain' and domain:
                permission['domain'] = domain
                send_notification = False
                
            elif perm_type == 'anyone':
                # Anyone with link - set general access
                send_notification = False
            else:
                return False, "Invalid permission configuration"
            
            # Create the permission
            self.dest_drive.permissions().create(
                fileId=file_id,
                body=permission,
                sendNotificationEmail=send_notification,
                supportsAllDrives=True
            ).execute()
            
            return True, None
            
        except HttpError as e:
            error_msg = str(e)
            status_code = e.resp.status
            
            # Handle common errors with detailed messages
            if status_code == 404:
                if is_internal:
                    return False, f"Internal user {email} not found in {self.dest_domain}"
                else:
                    return False, f"External user {email} not found (account may not exist)"
                    
            elif status_code == 403:
                return False, "Permission denied - insufficient privileges"
                
            elif status_code == 400:
                # Check specific error messages
                if 'does not have a Google account' in error_msg or 'no Google account' in error_msg.lower():
                    return False, f"User {email} does not have a Google account"
                elif 'notify people' in error_msg.lower():
                    return False, f"Cannot notify {email} (account may not exist in workspace)"
                elif 'Bad Request' in error_msg:
                    return False, f"Invalid request - {error_msg}"
                else:
                    return False, f"Bad request - {error_msg}"
                    
            else:
                return False, f"HTTP {status_code}: {error_msg}"
                
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"
    
    def copy_folder_permissions(self, source_folder_id: str, dest_folder_id: str) -> Dict:
        """
        Copy permissions from source folder to destination folder
        
        Args:
            source_folder_id: Source folder ID
            dest_folder_id: Destination folder ID
            
        Returns:
            Result dictionary
        """
        try:
            # Get source permissions
            response = self.source_drive.permissions().list(
                fileId=source_folder_id,
                fields='permissions(id,type,role,emailAddress,displayName,domain)',
                supportsAllDrives=True
            ).execute()
            
            source_permissions = response.get('permissions', [])
            
            # Migrate permissions
            result = self.migrate_permissions(
                source_folder_id,
                dest_folder_id,
                source_permissions
            )
            
            logger.info(f"Folder permissions migrated: {result['migrated']}/{result['total_permissions']}")
            
            return result
            
        except HttpError as e:
            logger.error(f"Error copying folder permissions: {e}")
            return {
                'total_permissions': 0,
                'migrated': 0,
                'failed': 0,
                'error': str(e)
            }
    
    def validate_permissions(self, source_file_id: str, dest_file_id: str) -> Dict:
        """
        Validate that permissions were migrated correctly
        
        Args:
            source_file_id: Source file ID
            dest_file_id: Destination file ID
            
        Returns:
            Validation result
        """
        validation = {
            'valid': False,
            'source_count': 0,
            'dest_count': 0,
            'missing': [],
            'extra': [],
            'details': []
        }
        
        try:
            # Get source permissions
            source_response = self.source_drive.permissions().list(
                fileId=source_file_id,
                fields='permissions(type,role,emailAddress,domain)',
                supportsAllDrives=True
            ).execute()
            
            source_perms = source_response.get('permissions', [])
            validation['source_count'] = len(source_perms)
            
            # Get destination permissions
            dest_response = self.dest_drive.permissions().list(
                fileId=dest_file_id,
                fields='permissions(type,role,emailAddress,domain)',
                supportsAllDrives=True
            ).execute()
            
            dest_perms = dest_response.get('permissions', [])
            validation['dest_count'] = len(dest_perms)
            
            # Create permission signatures for comparison
            source_sigs = {}
            for p in source_perms:
                if p.get('role') != 'owner':  # Skip owner
                    email = p.get('emailAddress')
                    domain_val = p.get('domain', 'anyone')
                    
                    # Map internal users for comparison
                    if email and '@' in email:
                        user_domain = email.split('@')[1]
                        if user_domain == self.source_domain:
                            local_part = email.split('@')[0]
                            mapped_email = f"{local_part}@{self.dest_domain}"
                        else:
                            mapped_email = email
                    else:
                        mapped_email = email
                    
                    sig = f"{p.get('type')}:{p.get('role')}:{mapped_email or domain_val}"
                    source_sigs[sig] = {'original': email, 'mapped': mapped_email}
            
            dest_sigs = set()
            for p in dest_perms:
                if p.get('role') != 'owner':  # Skip owner
                    email = p.get('emailAddress')
                    domain_val = p.get('domain', 'anyone')
                    sig = f"{p.get('type')}:{p.get('role')}:{email or domain_val}"
                    dest_sigs.add(sig)
            
            # Find differences
            for sig, emails in source_sigs.items():
                if sig not in dest_sigs:
                    validation['missing'].append({
                        'signature': sig,
                        'original_email': emails['original'],
                        'expected_email': emails['mapped']
                    })
            
            validation['extra'] = list(dest_sigs - set(source_sigs.keys()))
            validation['valid'] = len(validation['missing']) == 0
            
            return validation
            
        except Exception as e:
            logger.error(f"Error validating permissions: {e}")
            validation['error'] = str(e)
            return validation
    
    def get_permission_summary(self, file_id: str, is_source: bool = True) -> Dict:
        """
        Get a summary of permissions for a file
        
        Args:
            file_id: File ID
            is_source: Whether this is from source (True) or destination (False)
            
        Returns:
            Permission summary
        """
        drive_service = self.source_drive if is_source else self.dest_drive
        
        try:
            response = drive_service.permissions().list(
                fileId=file_id,
                fields='permissions(type,role,emailAddress,displayName,domain)',
                supportsAllDrives=True
            ).execute()
            
            permissions = response.get('permissions', [])
            
            summary = {
                'total': len(permissions),
                'owner': None,
                'editors': [],
                'viewers': [],
                'commenters': [],
                'domain_access': None,
                'anyone_access': None
            }
            
            for perm in permissions:
                role = perm.get('role')
                perm_type = perm.get('type')
                email = perm.get('emailAddress')
                
                if role == 'owner':
                    summary['owner'] = email
                elif role == 'writer':
                    summary['editors'].append(email or perm.get('domain', 'group'))
                elif role == 'reader':
                    if perm_type == 'domain':
                        summary['domain_access'] = perm.get('domain')
                    elif perm_type == 'anyone':
                        summary['anyone_access'] = 'reader'
                    else:
                        summary['viewers'].append(email or 'group')
                elif role == 'commenter':
                    summary['commenters'].append(email or 'group')
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting permission summary: {e}")
            return {'error': str(e)}


# Backward compatibility: alias for old code
PermissionsMigrator = EnhancedPermissionsMigrator