import os
import pathlib
import json
import time
import threading
import functools
from datetime import datetime
TC_VAR ="ACCEPT_TC"
TC_VAL ="tôi đồng ý"
TC_PATH = pathlib.Path.home() /".vnstock" /"id" /"terms_agreement.txt"
TERMS_AND_CONDITIONS ="""
Khi tiếp tục sử dụng Vnstock, bạn xác nhận rằng bạn đã đọc, hiểu và đồng ý với Chính sách quyền riêng tư và Điều khoản, điều kiện về giấy phép sử dụng Vnstock.
Chi tiết:
- Giấy phép sử dụng phần mềm: https://vnstocks.com/docs/tai-lieu/giay-phep-su-dung
- Chính sách quyền riêng tư: https://vnstocks.com/docs/tai-lieu/chinh-sach-quyen-rieng-tu
"""

class Core:
    def __init__(self):
        self.initialized = False
        self.webhook_url = None
        self.init_time = datetime.now().isoformat()
        self.home_dir = pathlib.Path.home()
        self.project_dir = self.home_dir /".vnstock"
        self.id_dir = self.project_dir /'id'
        self.terms_file_path = TC_PATH
        self.system_info = None
        self.project_dir.mkdir(exist_ok=True)
        self.id_dir.mkdir(exist_ok=True)
        self.initialize()

    def initialize(self):
        if self.initialized:
            return True
        if not self._check_terms():
            self._accept_terms()
        from vnai.scope.profile import inspector
        inspector.setup_vnstock_environment()
        try:
            from vnai.scope.device import device_registry
            vnstock_version = getattr(__import__('vnstock'),
'__version__','0.0.1')
            if device_registry.needs_reregistration(vnstock_version):
                system_info = inspector.examine()
                device_registry.register(system_info, vnstock_version)
                self.system_info = system_info
            else:
                self.system_info = device_registry.get_registry()
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            msg = f"Device registration failed: {e}. Using fallback."
            logger.warning(msg)
            self.system_info = inspector.examine()
        from vnai.scope.promo import ContentManager
        manager = ContentManager()
        if manager.is_paid_user is not True:
            from vnai.scope.promo import present
            present()
        from vnai.scope.state import record
        record("initialization", {"timestamp": datetime.now().isoformat()})
        from vnai.flow.relay import conduit
        conduit.queue({
"type":"system_info",
"data": {
"commercial": inspector.detect_commercial_usage(),
"packages": inspector.scan_packages()
            }
        }, priority="high")
        self.initialized = True
        return True

    def _check_terms(self):
        return os.path.exists(self.terms_file_path)

    def _accept_terms(self):
        from vnai.scope.profile import inspector
        system_info = inspector.examine()
        if TC_VAR in os.environ and os.environ[TC_VAR] == TC_VAL:
            os.environ[TC_VAR] = TC_VAL
        else:
            os.environ[TC_VAR] = TC_VAL
        now = datetime.now()
        machine_id = system_info['machine_id']
        user_msg = (
            f"Người dùng có mã nhận dạng {machine_id} "
            f"đã chấp nhận "
        )
        signed_agreement = (
            f"{user_msg}"
            f"điều khoản & điều kiện sử dụng Vnstock lúc {now}\n"
            f"---\n\n"
            f"THÔNG TIN THIẾT BỊ: {json.dumps(system_info, indent=2)}\n\n"
            f"Đính kèm bản sao nội dung bạn đã đọc, "
            f"hiểu rõ và đồng ý dưới đây:\n"
            f"{TERMS_AND_CONDITIONS}"
        )
        with open(self.terms_file_path,"w", encoding="utf-8") as f:
            f.write(signed_agreement)
        env_file = self.id_dir /"environment.json"
        env_data = {
"accepted_agreement": True,
"timestamp": now.isoformat(),
"machine_id": machine_id
        }
        with open(env_file,"w") as f:
            json.dump(env_data, f)
        return True

    def status(self):
        from vnai.beam.pulse import monitor
        from vnai.scope.state import tracker
        return {
"initialized": self.initialized,
"health": monitor.report(),
"metrics": tracker.get_metrics()
        }

    def configure_privacy(self, level="standard"):
        from vnai.scope.state import tracker
        return tracker.setup_privacy(level)
_core_instance = None
_core_lock = threading.Lock()

def _get_core():
    global _core_instance
    if _core_instance is None:
        with _core_lock:
            if _core_instance is None:
                _core_instance = Core()
    return _core_instance

def tc_init():
    return _get_core().initialize()

def setup():
    return _get_core().initialize()

def optimize_execution(resource_type="default"):
    from vnai.beam.quota import optimize
    return optimize(resource_type)

def agg_execution(resource_type="default"):
    from vnai.beam.quota import optimize
    opts = optimize(resource_type, ad_cooldown=1500,
                    content_trigger_threshold=100000)
    return opts

def measure_performance(module_type="function"):
    from vnai.beam.metrics import capture
    return capture(module_type)

def accept_license_terms(terms_text=None):
    if terms_text is None:
        terms_text = TERMS_AND_CONDITIONS
    from vnai.scope.profile import inspector
    system_info = inspector.examine()
    terms_file_path = (
        pathlib.Path.home() /".vnstock" /"id" /
"terms_agreement.txt"
    )
    os.makedirs(os.path.dirname(terms_file_path), exist_ok=True)
    with open(terms_file_path,"w", encoding="utf-8") as f:
        f.write(f"Terms accepted at {datetime.now().isoformat()}\n")
        f.write(f"System: {json.dumps(system_info)}\n\n")
        f.write(terms_text)
    return True

def accept_vnstock_terms():
    from vnai.scope.profile import inspector
    system_info = inspector.examine()
    home_dir = pathlib.Path.home()
    project_dir = home_dir /".vnstock"
    project_dir.mkdir(exist_ok=True)
    id_dir = project_dir /'id'
    id_dir.mkdir(exist_ok=True)
    env_file = id_dir /"environment.json"
    env_data = {
"accepted_agreement": True,
"timestamp": datetime.now().isoformat(),
"machine_id": system_info['machine_id']
    }
    try:
        with open(env_file,"w") as f:
            json.dump(env_data, f)
        print("Vnstock terms accepted successfully.")
        return True
    except Exception as e:
        print(f"Error accepting terms: {e}")
        return False

def configure_privacy(level="standard"):
    from vnai.scope.state import tracker
    return tracker.setup_privacy(level)

def check_commercial_usage():
    from vnai.scope.profile import inspector
    return inspector.detect_commercial_usage()

def authenticate_for_persistence():
    from vnai.scope.profile import inspector
    return inspector.get_or_create_user_id()