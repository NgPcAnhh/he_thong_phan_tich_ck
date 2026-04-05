"""
Authentication and tier management.
Provides tier detection and API key management for rate limiting.
Includes centralized authentication state manager for deduplication.
"""
import os
import json
import time
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import threading
log = logging.getLogger(__name__)

class AuthStateManager:
    def __init__(self, state_dir: Path):
        self.state_dir = state_dir
        self.state_file = state_dir /"auth_state.json"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._state = self._load_state()

    def _load_state(self) -> Dict:
        if self.state_file.exists():
            try:
                with open(self.state_file,'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                log.debug(f"Could not load auth state: {e}")
                return self._default_state()
        return self._default_state()

    def _default_state(self) -> Dict:
        return {
"session_id": self._generate_session_id(),
"authenticated": False,
"user": None,
"tier": None,
"auth_time": None,
"packages_notified": [],
"cache_ttl_minutes": 60
        }

    def _generate_session_id(self) -> str:
        return datetime.now().isoformat()

    def _save_state(self) -> None:
        try:
            with open(self.state_file,'w') as f:
                json.dump(self._state, f, indent=2)
        except IOError as e:
            log.warning(f"Could not save auth state: {e}")

    def _is_cache_valid(self) -> bool:
        if not self._state.get("auth_time"):
            return False
        auth_time = datetime.fromisoformat(self._state["auth_time"])
        ttl_minutes = self._state.get("cache_ttl_minutes", 60)
        expiry = auth_time + timedelta(minutes=ttl_minutes)
        return datetime.now() < expiry

    def should_show_message(self, package_name: str) -> bool:
        if not self._state.get("authenticated") or not self._is_cache_valid():
            return True
        packages_notified = self._state.get("packages_notified", [])
        return len(packages_notified) == 0

    def mark_authenticated(self, license_info: Dict, package_name: str) -> None:
        self._state["authenticated"] = True
        self._state["user"] = license_info.get("user","Unknown")
        self._state["tier"] = license_info.get("tier","free")
        self._state["auth_time"] = datetime.now().isoformat()
        packages_notified = self._state.get("packages_notified", [])
        if package_name not in packages_notified:
            packages_notified.append(package_name)
        self._state["packages_notified"] = packages_notified
        self._save_state()

    def get_cached_info(self) -> Optional[Dict]:
        if not self._is_cache_valid():
            return None
        return {
"user": self._state.get("user"),
"tier": self._state.get("tier"),
"from_cache": True
        }

    def reset(self) -> None:
        self._state = self._default_state()
        self._save_state()

def get_auth_state_manager(project_dir: Path) -> AuthStateManager:
    return AuthStateManager(project_dir)

class Authenticator:
    TIER_LIMITS = {
"guest": {"min": 20,"hour": 1200,"day": 5000},
"free": {"min": 60,"hour": 3600,"day": 10000},
"bronze": {"min": 180,"hour": 10800,"day": 50000},
"silver": {"min": 300,"hour": 18000,"day": 100000},
"golden": {"min": 600,"hour": 36000,"day": 150000}
    }

    def __init__(self):
        self.vnstock_dir = Path.home() /".vnstock"
        self.api_key_file = self.vnstock_dir /"api_key.json"
        self._cached_tier = None
        self._cache_timestamp = 0
        self._cache_ttl = 300

    def get_tier(self, force_refresh: bool = False) -> str:
        current_time = time.time()
        if not force_refresh and self._cached_tier and (current_time - self._cache_timestamp) < self._cache_ttl:
            return self._cached_tier
        tier = self._detect_tier()
        self._cached_tier = tier
        self._cache_timestamp = current_time
        log.debug(f"Detected tier: {tier}")
        return tier

    def _detect_tier(self) -> str:
        tier_from_vnii = self._check_vnii_tier()
        if tier_from_vnii:
            return tier_from_vnii
        if self._has_api_key():
            return"free"
        return"guest"

    def _check_vnii_tier(self) -> Optional[str]:
        try:
            import vnii
            from vnii.auth import authenticate
            license_info = authenticate(self.vnstock_dir)
            tier_string = license_info.get('tier','free')
            log.debug(f"Got tier from vnii: {tier_string}")
            return tier_string
        except ImportError:
            log.debug("vnii not installed")
            return None
        except SystemExit:
            log.debug("vnii authentication failed")
            return None
        except Exception as e:
            log.debug(f"Error checking vnii tier: {e}")
            return None

    def _has_api_key(self) -> bool:
        if os.getenv('VNSTOCK_API_KEY'):
            return True
        if self.api_key_file.exists():
            try:
                with open(self.api_key_file,'r') as f:
                    data = json.load(f)
                    api_key = data.get('api_key','').strip()
                    return bool(api_key)
            except Exception as e:
                log.debug(f"Failed to read API key: {e}")
        return False

    def get_limits(self, tier: Optional[str] = None) -> Dict[str, int]:
        if tier is None:
            tier = self.get_tier()
        return self.TIER_LIMITS.get(tier, self.TIER_LIMITS["guest"])

    def get_tier_info(self) -> Dict:
        tier = self.get_tier()
        limits = self.get_limits(tier)
        descriptions = {
"guest":"Khách (Guest - chưa đăng ký)",
"free":"Phiên bản cộng đồng (Community - có API key)",
"bronze":"Thành viên Bronze (Bronze Member)",
"silver":"Thành viên Silver (Silver Member)",
"golden":"Thành viên Golden (Golden Member)"
        }
        return {
"tier": tier,
"description": descriptions.get(tier,f"Gói {tier.title()}"),
"limits": {
"per_minute": limits["min"],
"per_hour": limits["hour"]
            }
        }

    def setup_api_key(self, api_key: str) -> bool:
        try:
            self.vnstock_dir.mkdir(exist_ok=True)
            api_key_data = {"api_key": api_key.strip()}
            with open(self.api_key_file,'w') as f:
                json.dump(api_key_data, f, indent=2)
            self._register_device_to_api(api_key.strip())
            print("✓ API key đã được lưu thành công! (API key saved successfully!)")
            print("Bạn đang sử dụng Phiên bản cộng đồng (60 requests/phút)")
            print("(You are using Community version - 60 requests/minute)")
            print("\nĐể tham gia gói thành viên tài trợ (To join sponsor membership):")
            print("  Truy cập: https://vnstocks.com/insiders-program")
            log.info("API key setup completed")
            return True
        except Exception as e:
            print(f"✗ Không thể lưu API key (Failed to save API key): {e}")
            log.error(f"API key setup failed: {e}")
            return False

    def get_api_key(self) -> Optional[str]:
        if os.getenv('VNSTOCK_API_KEY'):
            return os.getenv('VNSTOCK_API_KEY')
        if self.api_key_file.exists():
            try:
                with open(self.api_key_file,'r') as f:
                    data = json.load(f)
                    return data.get('api_key')
            except Exception as e:
                log.debug(f"Failed to read API key: {e}")
        return None

    def remove_api_key(self) -> bool:
        try:
            if self.api_key_file.exists():
                self.api_key_file.unlink()
                print("✓ API key đã được xóa (API key removed)")
                print("Bạn đang ở chế độ Khách (20 requests/phút) (You are in Guest mode - 20 requests/minute)")
            else:
                print("Không tìm thấy API key (No API key found)")
            log.info("API key removed")
            return True
        except Exception as e:
            print(f"✗ Không thể xóa API key (Failed to remove API key): {e}")
            log.error(f"API key removal failed: {e}")
            return False

    def _register_device_to_api(self, api_key: str) -> None:
        try:
            import requests
            import platform
            from vnai.scope.profile import inspector
            from vnai.scope.device import IDEDetector
            system_info = inspector.examine()
            try:
                ide_name, ide_info = IDEDetector.detect_ide()
            except Exception:
                ide_name ='Unknown'
                ide_info = {}
            payload = {
'api_key': api_key,
'device_id': system_info['machine_id'],
'device_name': system_info.get('platform', platform.node()),
'os_type': system_info['os_name'].lower(),
'os_version': system_info.get('platform', platform.release()),
'machine_info': {
'platform': system_info.get('platform', platform.platform()),
'machine': platform.machine(),
'processor': platform.processor(),
'system': platform.system(),
'release': platform.release(),
'python_version': system_info.get('python_version'),
'environment': system_info.get('environment','unknown'),
'ide_name': ide_name,
'ide_detection_method': ide_info.get('detection_method'),
'ide_frontend': ide_info.get('frontend')
                }
            }
            url ='https://vnstocks.com/api/vnstock/auth/device-register'
            try:
                requests.post(url, json=payload, timeout=5)
                log.debug("Device registered successfully to vnstocks.com")
            except Exception as req_e:
                log.debug(f"Device registration request failed: {req_e}")
        except Exception as e:
            log.debug(f"Device registration error: {e}")

    def check_api_key_status(self) -> dict:
        api_key = self.get_api_key()
        if api_key:
            preview = api_key[:15] +"..." if len(api_key) > 15 else api_key
            tier_info = self.get_tier_info()
            print(f"✓ API key: {preview}")
            print(f"✓ Tier (Gói): {tier_info['tier']}")
            print(f"✓ Giới hạn (Limits): {tier_info['limits']}")
            return {
'has_api_key': True,
'api_key_preview': preview,
'tier': tier_info['tier'],
'limits': tier_info['limits']
            }
        else:
            return {
'has_api_key': False,
'api_key_preview': None,
'tier': self.get_tier(),
'limits': self.get_limits()
            }

    def print_help(self):
        print("\n" +"="*60)
        print("VNSTOCK API KEY SETUP (Cài đặt API Key)")
        print("="*60)
        print("\n📋 Các gói sử dụng (Available Tiers):")
        print("  • Khách (Guest): 20 requests/phút. Tối đa 4 kỳ báo cáo tài chính.")
        print("  • Phiên bản cộng đồng (Community): 60 requests/phút. Tối đa 8 kỳ báo cáo tài chính.")
        print("  • Thành viên tài trợ (Sponsor): 180-500 requests/phút.")
        print("\n🔑 Lấy API Key (Get Your API Key):")
        print("  1. Truy cập: https://vnstocks.com/account")
        print("  2. Đăng ký hoặc đăng nhập (Sign up or log in)")
        print("  3. Sao chép API key của bạn (Copy your API key)")
        print("\n💾 Cài đặt API Key (Setup API Key):")
        print("  import vnai")
        print('  vnai.setup_api_key("your_api_key_here")')
        print("\n📊 Kiểm tra trạng thái (Check Status):")
        print("  import vnai")
        print("  vnai.check_api_key_status()")
        print("\n🤝 Tham gia gói thành viên tài trợ (Join Vnstock Sponsor):")
        print("  Truy cập: https://vnstocks.com/insiders-program")
        print("="*60 +"\n")
authenticator = Authenticator()