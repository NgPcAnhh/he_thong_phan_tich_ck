import logging
from typing import Optional
logger = logging.getLogger(__name__)

def check_license_status() -> Optional[bool]:
    try:
        from vnai.scope.api_key_checker import api_key_checker
        is_paid = api_key_checker.is_paid_user()
        return is_paid
    except ImportError:
        logger.warning("API key checker not available")
        return None
    except Exception as e:
        logger.error(f"Error checking license status: {e}")
        return None

def update_license_from_vnii() -> bool:
    try:
        from vnai.scope.api_key_checker import api_key_checker
        result = api_key_checker.check_license_with_vnii(force=True)
        if result.get('status') in ['verified','cached']:
            is_paid = result.get('is_paid', False)
            logger.info(f"License updated via API key: is_paid={is_paid}")
            return True
        else:
            logger.warning(
                f"License check failed: {result.get('status')}"
            )
            return False
    except ImportError:
        logger.warning("API key checker not available")
        return False
    except Exception as e:
        logger.error(f"Error updating license: {e}")
        return False
_original_guardian_verify = None

def guardian_verify_with_license(
    guardian_instance,
    operation_id: str,
    resource_type: str ="default"
) -> bool:
    is_paid = check_license_status()
    if is_paid is True:
        logger.debug(
            f"Skipping rate limit for paid user: {resource_type}"
        )
        return True
    global _original_guardian_verify
    if _original_guardian_verify is None:
        _original_guardian_verify = guardian_instance.__class__.verify
    return _original_guardian_verify(
        guardian_instance, operation_id, resource_type
    )
_original_content_should_display = None

def content_manager_should_display_with_license(
    content_manager_instance
) -> bool:
    is_paid = check_license_status()
    if is_paid is True:
        logger.debug("Suppressing ads for paid user")
        return False
    return True

def integrate_license_features() -> bool:
    try:
        from vnai.beam.quota import guardian
        from vnai.scope.promo import ContentManager
        original_verify = guardian.verify

        def patched_verify(operation_id: str, resource_type: str ="default"):
            return guardian_verify_with_license(
                guardian, operation_id, resource_type
            )
        guardian.verify = patched_verify
        logger.info("Guardian.verify() patched for license support")
        content_mgr = ContentManager()
        original_present = getattr(content_mgr,'present', None)

        def patched_present():
            if not content_manager_should_display_with_license(
                content_mgr
            ):
                logger.debug("Skipping ad display for paid user")
                return False
            if original_present and callable(original_present):
                return original_present()
            return False
        if hasattr(content_mgr,'present'):
            content_mgr.present = patched_present
            logger.info("ContentManager.present() patched for license")
        logger.info("License-based features successfully integrated")
        return True
    except Exception as e:
        logger.error(f"Error integrating license features: {e}")
        return False

def auto_integrate():
    try:
        integrate_license_features()
    except Exception as e:
        logger.warning(f"Auto-integration of license features failed: {e}")
auto_integrate()