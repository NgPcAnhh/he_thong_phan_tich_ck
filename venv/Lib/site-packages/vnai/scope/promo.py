import logging
import requests
from datetime import datetime
import random
import threading
import time
import urllib.parse
_vnii_check_attempted = False

class AdCategory:
    FREE = 0
    MANDATORY = 1
    ANNOUNCEMENT = 2
    REFERRAL = 3
    FEATURE = 4
    GUIDE = 5
    SURVEY = 6
    PROMOTION = 7
    SECURITY = 8
    MAINTENANCE = 9
    WARNING = 10
API_KEY_CHECKER_AVAILABLE = None
api_key_checker = None
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.ERROR)

class ContentManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(ContentManager, cls).__new__(cls)
                cls._instance._initialize()
            return cls._instance

    def _initialize(self, debug=False):
        self.content_base_url ="https://hq.vnstocks.com/content-delivery"
        self.is_paid_user = None
        self.license_checked = False
        self._debug = debug
        global API_KEY_CHECKER_AVAILABLE, api_key_checker
        if API_KEY_CHECKER_AVAILABLE is None:
            try:
                from vnai.scope.api_key_checker import api_key_checker as _checker
                api_key_checker = _checker
                API_KEY_CHECKER_AVAILABLE = True
            except ImportError:
                API_KEY_CHECKER_AVAILABLE = False
                api_key_checker = None
        self.last_display = 0
        self.display_interval = 24 * 3600
        self.content_base_url ="https://hq.vnstocks.com/content-delivery"
        self.target_url ="https://vnstocks.com/lp-khoa-hoc-python-chung-khoan"
        self.image_url = (
"https://vnstocks.com/img/trang-chu-vnstock-python-api-phan-tich-giao-dich-chung-khoan.jpg"
        )
        self._start_periodic_display()

    def _check_license_status(self):
        if self.license_checked:
            return
        if API_KEY_CHECKER_AVAILABLE and api_key_checker:
            try:
                result = api_key_checker.check_license_with_vnii()
                if self._debug:
                    logger.info(f"API key check result: {result}")
                valid_statuses = [
'verified',
'cached',
'device_limit_exceeded'
                ]
                if result.get('status') in valid_statuses:
                    self.is_paid_user = result.get('is_paid', False)
                    self.license_checked = True
                    if self._debug:
                        status_msg = (
"Detected paid user"
                            if self.is_paid_user
                            else"Detected free user"
                        )
                        logger.info(f"{status_msg} via API key")
                else:
                    self.is_paid_user = False
                    self.license_checked = True
                    if self._debug:
                        logger.info(
                            f"No valid license: {result.get('status')}"
                        )
            except Exception as e:
                if self._debug:
                    logger.error(f"Error checking license via API key: {e}")
                self.is_paid_user = False
                self.license_checked = True
        else:
            if self._debug:
                logger.warning(
"API key checker not available. "
"Cannot determine paid user status."
                )
            self.is_paid_user = False
            self.license_checked = True

    def _start_periodic_display(self):
        def periodic_display():
            while True:
                if not self.license_checked:
                    self._check_license_status()
                if self.is_paid_user:
                    break
                sleep_time = random.randint(2 * 3600, 6 * 3600)
                time.sleep(sleep_time)
                current_time = time.time()
                if current_time - self.last_display >= self.display_interval:
                    self.present_content(context="periodic")
                else:
                    pass
        thread = threading.Thread(target=periodic_display, daemon=True)
        thread.start()

    def fetch_remote_content(self, context: str ="init", html: bool = True) -> str:
        if not self.license_checked:
            self._check_license_status()
        if self.is_paid_user:
            return""
        try:
            params = {"context": context,"html":"true" if html else"false"}
            url = f"{self.content_base_url}?{urllib.parse.urlencode(params)}"
            response = requests.get(url, timeout=3)
            if response.status_code == 200:
                return response.text
            logger.error(f"Non-200 response fetching content: {response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Failed to fetch remote content: {e}")
            return None

    def present_content(self, context: str ="init", ad_category: int = AdCategory.FREE) -> None:
        if not self.license_checked:
            self._check_license_status()
        if self.is_paid_user and ad_category == AdCategory.FREE:
            return
        self.last_display = time.time()
        environment = None
        try:
            from vnai.scope.profile import inspector
            environment = inspector.examine().get("environment","unknown")
        except Exception as e:
            logger.error(f"Không detect được environment: {e}")
            environment ="unknown"
        remote_content = self.fetch_remote_content(
            context=context, html=(environment =="jupyter")
        )
        fallback = self._generate_fallback_content(context)
        if environment =="jupyter":
            try:
                from IPython.display import display, HTML, Markdown
                if remote_content:
                    display(HTML(remote_content))
                else:
                    try:
                        display(Markdown(fallback["markdown"]))
                    except Exception as e:
                        display(HTML(fallback["html"]))
            except Exception as e:
                pass
        elif environment =="terminal":
            if remote_content:
                print(remote_content)
            else:
                print(fallback["terminal"])
        else:
            print(fallback["simple"])

    def _generate_fallback_content(self, context):
        fallback = {"html":"","markdown":"","terminal":"","simple":""}
        if context =="loop":
            fallback["html"] = (
                f"""
            <div style="border: 1px solid #e74c3c; padding: 15px; border-radius: 5px; margin: 10px 0;">
                <h3 style="color: #e74c3c;">⚠️ Bạn đang sử dụng vòng lặp với quá nhiều requests</h3>
                <p>Để tránh bị giới hạn tốc độ và tối ưu hiệu suất:</p>
                <ul>
                    <li>Thêm thời gian chờ giữa các lần gọi API</li>
                    <li>Sử dụng xử lý theo batch thay vì lặp liên tục</li>
                    <li>Tham gia gói tài trợ <a href="https://vnstocks.com/insiders-program" style="color: #3498db;">Vnstock Insider</a> để tăng 5X giới hạn API</li>
                </ul>
            </div>
            """
            )
            fallback["markdown"] = (
"""
## ⚠️ Bạn đang sử dụng vòng lặp với quá nhiều requests
Để tránh bị giới hạn tốc độ và tối ưu hiệu suất:
* Thêm thời gian chờ giữa các lần gọi API
* Sử dụng xử lý theo batch thay vì lặp liên tục
* Tham gia gói tài trợ [Vnstock Insider](https://vnstocks.com/insiders-program) để tăng 5X giới hạn API
            """
            )
            fallback["terminal"] = (
"""
╔═════════════════════════════════════════════════════════════════╗
║                                                                 ║
║   🚫 ĐANG BỊ CHẶN BỞI GIỚI HẠN API? GIẢI PHÁP Ở ĐÂY!            ║
║                                                                 ║
║   ✓ Tăng ngay 500% tốc độ gọi API - Không còn lỗi RateLimit     ║
║   ✓ Tiết kiệm 85% thời gian chờ đợi giữa các request            ║
║                                                                 ║
║   ➤ NÂNG CẤP NGAY VỚI GÓI TÀI TRỢ VNSTOCK:                      ║
║     https://vnstocks.com/insiders-program                       ║
║                                                                 ║
╚═════════════════════════════════════════════════════════════════╝
                """
            )
            fallback["simple"] = (
"🚫 Đang bị giới hạn API? Tăng tốc độ gọi API lên 500% với gói "
"Vnstock Insider: https://vnstocks.com/insiders-program"
            )
        else:
            fallback["html"] = (
                f"""
            <div style="border: 1px solid #3498db; padding: 15px; border-radius: 5px; margin: 10px 0;">
                <h3 style="color: #3498db;">👋 Chào mừng bạn đến với Vnstock!</h3>
                <p>Cảm ơn bạn đã sử dụng thư viện phân tích chứng khoán #1 tại Việt Nam cho Python</p>
                <ul>
                    <li>Tài liệu: <a href="https://vnstocks.com/docs/category/s%E1%BB%95-tay-h%C6%B0%E1%BB%9Bng-d%E1%BA%ABn" style="color: #3498db;">vnstocks.com/docs</a></li>
                    <li>Cộng đồng: <a href="https://www.facebook.com/groups/vnstock.official" style="color: #3498db;">vnstocks.com/community</a></li>
                </ul>
                <p>Khám phá các tính năng mới nhất và tham gia cộng đồng để nhận hỗ trợ.</p>
            </div>
            """
            )
            fallback["markdown"] = (
"""
## 👋 Chào mừng bạn đến với Vnstock!
Cảm ơn bạn đã sử dụng package phân tích chứng khoán #1 tại Việt Nam
* Tài liệu: [Sổ tay hướng dẫn](https://vnstocks.com/docs)
* Cộng đồng: [Nhóm Facebook](https://facebook.com/groups/vnstock.official)
Khám phá các tính năng mới nhất và tham gia cộng đồng để nhận hỗ trợ.
                """
            )
            fallback["terminal"] = (
"""
╔════════════════════════════════════════════════════════════╗
║                                                            ║
║  👋 Chào mừng bạn đến với Vnstock!                         ║
║                                                            ║
║  Cảm ơn bạn đã sử dụng package phân tích                   ║
║  chứng khoán #1 tại Việt Nam                               ║
║                                                            ║
║  ✓ Tài liệu: https://vnstocks.com/docs                     ║
║  ✓ Cộng đồng: https://facebook.com/groups/vnstock.official ║
║                                                            ║
║  Khám phá các tính năng mới nhất và tham gia               ║
║  cộng đồng để nhận hỗ trợ.                                 ║
║                                                            ║
╚════════════════════════════════════════════════════════════╝
                """
            )
            fallback["simple"] = (
"👋 Chào mừng bạn đến với Vnstock! "
"Tài liệu: https://vnstocks.com/onboard | "
"Cộng đồng: https://facebook.com/groups/vnstock.official"
            )
        return fallback
manager = ContentManager()

def present(context: str ="init", ad_category: int = AdCategory.FREE) -> None:
    manager.present_content(context=context, ad_category=ad_category)