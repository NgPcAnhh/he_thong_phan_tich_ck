import json
import logging
from pathlib import Path
from typing import Optional, Dict
from datetime import datetime
logger = logging.getLogger(__name__)

class APIKeyChecker:
    _instance = None
    _lock = None

    def __new__(cls):
        import threading
        if cls._lock is None:
            cls._lock = threading.Lock()
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(APIKeyChecker, cls).__new__(cls)
                cls._instance._initialize()
            return cls._instance

    def _initialize(self) -> None:
        self.checked = False
        self.last_check_time = None
        self.is_paid = None
        self.license_info = None

    def _get_vnstock_directories(self) -> list[Path]:
        paths = []
        local_path = Path.home() /".vnstock"
        paths.append(local_path)
        colab_drive_path = Path('/content/drive/MyDrive/.vnstock')
        if colab_drive_path.parent.exists():
            paths.append(colab_drive_path)
        try:
            from vnstock.core.config.ggcolab import get_vnstock_directory
            vnstock_dir = get_vnstock_directory()
            if vnstock_dir not in paths:
                paths.append(vnstock_dir)
        except ImportError:
            pass
        return paths

    def _find_api_key_file(self) -> Optional[Path]:
        for vnstock_dir in self._get_vnstock_directories():
            api_key_path = vnstock_dir /"api_key.json"
            if api_key_path.exists():
                logger.debug(f"Found api_key.json at: {api_key_path}")
                return api_key_path
        logger.debug("api_key.json not found in any vnstock directory")
        return None

    def _read_api_key(self, api_key_path: Path) -> Optional[str]:
        try:
            with open(api_key_path,'r', encoding='utf-8') as f:
                data = json.load(f)
                api_key = data.get('api_key')
                if api_key and isinstance(api_key, str):
                    return api_key.strip()
                else:
                    logger.warning(
                        f"Invalid api_key format in {api_key_path}"
                    )
                    return None
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {api_key_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error reading {api_key_path}: {e}")
            return None

    def check_license_with_vnii(
        self,
        force: bool = False
    ) -> Dict[str, any]:
        if self.checked and not force:
            return {
'is_paid': self.is_paid,
'status':'cached',
'checked_at': self.last_check_time,
'api_key_found': True,
'vnii_available': True
            }
        result = {
'is_paid': False,
'status':'unknown',
'checked_at': datetime.now().isoformat(),
'api_key_found': False,
'vnii_available': False
        }
        try:
            from vnii import lc_init
            result['vnii_available'] = True
        except ImportError:
            logger.debug("vnii package not available")
            result['status'] ='vnii_not_installed'
            return result
        api_key_path = self._find_api_key_file()
        if not api_key_path:
            logger.debug("No api_key.json found")
            result['status'] ='no_api_key_file'
            return result
        api_key = self._read_api_key(api_key_path)
        if not api_key:
            logger.warning("Could not read API key from file")
            result['status'] ='invalid_api_key_file'
            return result
        result['api_key_found'] = True
        try:
            from vnii import lc_init
            import os
            original_env = os.environ.get('VNSTOCK_API_KEY')
            os.environ['VNSTOCK_API_KEY'] = api_key
            try:
                license_info = lc_init(debug=False)
                status = license_info.get('status','').lower()
                tier = license_info.get('tier','').lower()
                is_paid = (
'recognized and verified' in status or
                    tier in ['bronze','silver','golden','gold']
                )
                result['is_paid'] = is_paid
                result['status'] ='verified' if is_paid else'free_user'
                result['license_info'] = license_info
                self.checked = True
                self.last_check_time = result['checked_at']
                self.is_paid = is_paid
                self.license_info = license_info
                logger.info(
                    f"License verified via vnii: "
                    f"is_paid={is_paid}, tier={tier}"
                )
            finally:
                if original_env is None:
                    os.environ.pop('VNSTOCK_API_KEY', None)
                else:
                    os.environ['VNSTOCK_API_KEY'] = original_env
        except SystemExit as e:
            error_msg = str(e)
            if'device limit exceeded' in error_msg.lower():
                logger.warning(f"Device limit exceeded but user is paid")
                result['status'] ='device_limit_exceeded'
                result['is_paid'] = True
                result['error'] = error_msg
                self.checked = True
                self.last_check_time = result['checked_at']
                self.is_paid = True
                self.license_info = {'status':'Device limit','tier':'paid'}
            else:
                logger.warning(f"vnii license check failed: {e}")
                result['status'] ='license_check_failed'
                result['error'] = error_msg
        except Exception as e:
            logger.error(f"Error calling vnii lc_init: {e}")
            result['status'] ='error'
            result['error'] = str(e)
        return result

    def is_paid_user(self) -> Optional[bool]:
        if not self.checked:
            result = self.check_license_with_vnii()
            return result.get('is_paid')
        return self.is_paid

    def get_license_info(self) -> Optional[Dict]:
        return self.license_info
api_key_checker = APIKeyChecker()

def check_license_via_api_key(force: bool = False) -> Dict[str, any]:
    return api_key_checker.check_license_with_vnii(force=force)

def is_paid_user_via_api_key() -> Optional[bool]:
    return api_key_checker.is_paid_user()