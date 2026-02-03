"""
Enhanced Permissions migration module
Handles internal vs external users intelligently
"""
import logging
from typing import Dict, List, Optional, Tuple
from googleapiclient.errors import HttpError
import time

logger = logging.getLogger(__name__)


# Backward compatibility alias
PermissionsMigrator = None  # Will be set after class definition


class EnhancedPermissionsMigrator:
    """
    Handles migration of file/folder permissions with smart domain mapping
    
    Implements 5 permission migration rules for complete access continuity
    """
    
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
        Migrate permissions following 5 strict rules
        
        RULE 1: Internal user (source domain) NOT in dest → External with source email
        RULE 2: External user (different domain) → Keep as external
        RULE 3: Internal user in BOTH domains → Map to dest domain
        RULE 4: General access (anyone/domain) → Migrate as-is
        RULE 5: Never skip permissions due to domain mismatch
        
        Args:
            source_file_id: Source file ID
            dest_file_id: Destination file ID
            source_permissions: List of source permissions
            
        Returns:
            Detailed migration result dictionary
        """
        result = {
            'total_permissions': len(source_permissions),
            'migrated': 0,
            'skipped': 0,
            'failed': 0,
            'external_users': 0,
            'internal_users': 0,
            'general_access': 0,
            'details': [],
            'classification': {
                'internal_both_domains': 0,      # RULE 3
                'internal_source_only': 0,       # RULE 1
                'external_domain': 0,            # RULE 2
                'general_access': 0              # RULE 4
            }
        }
        
        for permission in source_permissions:
            perm_type = permission.get('type')
            role = permission.get('role')
            email = permission.get('emailAddress')
            domain = permission.get('domain')
            
            # Skip owner permission (set during file creation)
            if role == 'owner':
                result['skipped'] += 1
                result['details'].append({
                    'email': email,
                    'role': role,
                    'status': 'skipped',
                    'reason': 'Owner permission handled during file creation'
                })
                continue
            
            # RULE 4: Handle General Access - Anyone with link
            if perm_type == 'anyone':
                success, error = self._create_permission(
                    dest_file_id, perm_type, role, None, None, False
                )
                
                if success:
                    result['migrated'] += 1
                    result['general_access'] += 1
                    result['classification']['general_access'] += 1
                    result['details'].append({
                        'type': 'anyone',
                        'role': role,
                        'classification': 'general_access_anyone',
                        'status': 'success',
                        'note': f'Anyone with link can {role}'
                    })
                    logger.debug(f"✓ Migrated general access: Anyone can {role}")
                else:
                    result['failed'] += 1
                    result['details'].append({
                        'type': 'anyone',
                        'role': role,
                        'classification': 'general_access_anyone',
                        'status': 'failed',
                        'error': error
                    })
                    logger.warning(f"✗ Failed general access: {error}")
                
                time.sleep(0.1)
                continue
            
            # RULE 4: Handle Domain-wide access
            if perm_type == 'domain' and domain:
                # Map to destination domain if it's the source domain
                target_domain = self.dest_domain if domain == self.source_domain else domain
                
                success, error = self._create_permission(
                    dest_file_id, perm_type, role, None, target_domain, False
                )
                
                if success:
                    result['migrated'] += 1
                    result['general_access'] += 1
                    result['classification']['general_access'] += 1
                    result['details'].append({
                        'type': 'domain',
                        'original_domain': domain,
                        'target_domain': target_domain,
                        'role': role,
                        'classification': 'general_access_domain',
                        'status': 'success',
                        'note': f'Domain-wide access for {target_domain}'
                    })
                    logger.debug(f"✓ Migrated domain access: {target_domain} can {role}")
                else:
                    result['failed'] += 1
                    result['details'].append({
                        'type': 'domain',
                        'original_domain': domain,
                        'target_domain': target_domain,
                        'role': role,
                        'classification': 'general_access_domain',
                        'status': 'failed',
                        'error': error
                    })
                    logger.warning(f"✗ Failed domain access: {error}")
                
                time.sleep(0.1)
                continue
            
            # Handle User/Group permissions
            if not email or '@' not in email:
                result['skipped'] += 1
                result['details'].append({
                    'type': perm_type,
                    'role': role,
                    'status': 'skipped',
                    'reason': 'No valid email address'
                })
                continue
            
            # Determine user classification
            user_domain = email.split('@')[1]
            local_part = email.split('@')[0]
            
            if user_domain == self.source_domain:
                # User from source domain - apply RULE 1 or RULE 3
                dest_email = f"{local_part}@{self.dest_domain}"
                
                # Try destination domain first (RULE 3 attempt)
                success_dest, error_dest = self._create_permission(
                    dest_file_id, perm_type, role, dest_email, None, True
                )
                
                if success_dest:
                    # RULE 3: User exists in both domains - migrated as internal
                    result['migrated'] += 1
                    result['internal_users'] += 1
                    result['classification']['internal_both_domains'] += 1
                    result['details'].append({
                        'original_email': email,
                        'target_email': dest_email,
                        'role': role,
                        'type': perm_type,
                        'classification': 'internal_both_domains',
                        'rule': 'RULE_3',
                        'status': 'success',
                        'note': f'User exists in both domains, mapped to {self.dest_domain}'
                    })
                    logger.debug(f"✓ [RULE 3] Internal user: {email} → {dest_email} ({role})")
                else:
                    # RULE 1: User NOT in destination - migrate as external with source email
                    success_external, error_external = self._create_permission(
                        dest_file_id, perm_type, role, email, None, False
                    )
                    
                    if success_external:
                        result['migrated'] += 1
                        result['external_users'] += 1
                        result['classification']['internal_source_only'] += 1
                        result['details'].append({
                            'original_email': email,
                            'target_email': email,  # Keep source domain
                            'role': role,
                            'type': perm_type,
                            'classification': 'internal_source_only',
                            'rule': 'RULE_1',
                            'status': 'success',
                            'note': f'User not found in {self.dest_domain}, kept as external collaborator'
                        })
                        logger.debug(f"✓ [RULE 1] External (source-only): {email} ({role})")
                    else:
                        # RULE 5: Never skip - mark as failed but attempted
                        result['failed'] += 1
                        result['classification']['internal_source_only'] += 1
                        result['details'].append({
                            'original_email': email,
                            'target_email': email,
                            'role': role,
                            'type': perm_type,
                            'classification': 'internal_source_only',
                            'rule': 'RULE_1',
                            'status': 'failed',
                            'error': f'Dest: {error_dest}, External: {error_external}',
                            'note': 'Attempted both internal and external migration (RULE 5)'
                        })
                        logger.warning(f"✗ [RULE 1] Failed: {email} - {error_external}")
                
            else:
                # RULE 2: External user (different domain) - migrate as-is
                success, error = self._create_permission(
                    dest_file_id, perm_type, role, email, None, False
                )
                
                if success:
                    result['migrated'] += 1
                    result['external_users'] += 1
                    result['classification']['external_domain'] += 1
                    result['details'].append({
                        'original_email': email,
                        'target_email': email,
                        'role': role,
                        'type': perm_type,
                        'classification': 'external_domain',
                        'rule': 'RULE_2',
                        'status': 'success',
                        'note': 'External user from different domain'
                    })
                    logger.debug(f"✓ [RULE 2] External user: {email} ({role})")
                else:
                    result['failed'] += 1
                    result['classification']['external_domain'] += 1
                    result['details'].append({
                        'original_email': email,
                        'target_email': email,
                        'role': role,
                        'type': perm_type,
                        'classification': 'external_domain',
                        'rule': 'RULE_2',
                        'status': 'failed',
                        'error': error
                    })
                    logger.warning(f"✗ [RULE 2] Failed external: {email} - {error}")
            
            # Rate limiting to avoid API quota issues
            time.sleep(0.1)
        
        # Log comprehensive summary
        logger.info(
            f"Permission migration summary: {result['migrated']}/{result['total_permissions']} migrated | "
            f"Internal (both): {result['classification']['internal_both_domains']} | "
            f"External (source-only): {result['classification']['internal_source_only']} | "
            f"External (other): {result['classification']['external_domain']} | "
            f"General: {result['general_access']} | "
            f"Failed: {result['failed']}"
        )
        
        return result
    
    def _create_permission(self, file_id: str, perm_type: str, role: str, 
                          email: Optional[str], domain: Optional[str],
                          is_internal: bool) -> Tuple[bool, Optional[str]]:
        """
        Create a permission on destination file with comprehensive error handling
        
        Args:
            file_id: Destination file ID
            perm_type: Permission type (user, group, domain, anyone)
            role: Permission role (reader, writer, commenter)
            email: Email address (for user/group)
            domain: Domain (for domain-wide permissions)
            is_internal: Whether this is an internal user (affects error messages)
        
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
            elif perm_type == 'group' and email:
                permission['emailAddress'] = email
            elif perm_type == 'domain' and domain:
                permission['domain'] = domain
            elif perm_type == 'anyone':
                pass  # No additional configuration needed
            else:
                return False, "Invalid permission configuration"
            
            # Create the permission
            self.dest_drive.permissions().create(
                fileId=file_id,
                body=permission,
                sendNotificationEmail=False,  # Don't spam users with notifications
                supportsAllDrives=True,
                transferOwnership=False
            ).execute()
            
            return True, None
            
        except HttpError as e:
            status_code = e.resp.status
            error_details = str(e)
            
            # Comprehensive error classification
            if status_code == 404:
                if is_internal:
                    return False, f"User not found in {self.dest_domain}"
                else:
                    return False, "External user/account not found"
            
            elif status_code == 403:
                if 'does not have a Google account' in error_details.lower():
                    return False, f"User {email} does not have a Google account"
                elif 'insufficient permissions' in error_details.lower():
                    return False, "Insufficient permissions to share"
                else:
                    return False, "Permission denied"
            
            elif status_code == 400:
                if 'invalidSharingRequest' in error_details:
                    return False, "User does not accept external shares"
                elif 'Bad Request' in error_details:
                    return False, f"Invalid request: {error_details[:80]}"
                else:
                    return False, f"Bad request: {error_details[:80]}"
            
            elif status_code == 429:
                return False, "Rate limit exceeded - too many permission requests"
            
            else:
                return False, f"HTTP {status_code}: {error_details[:80]}"
        
        except Exception as e:
            return False, f"Unexpected error: {str(e)[:80]}"
    
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