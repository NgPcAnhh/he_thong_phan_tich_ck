import time
import functools
import threading
from collections import defaultdict
from datetime import datetime

class RateLimitExceeded(Exception):
    def __init__(self, resource_type, limit_type="min", current_usage=None, limit_value=None, retry_after=None):
        self.resource_type = resource_type
        self.limit_type = limit_type
        self.current_usage = current_usage
        self.limit_value = limit_value
        self.retry_after = retry_after
        message = f"Bạn đã gửi quá nhiều request tới {resource_type}. "
        if retry_after:
            message += f"Vui lòng thử lại sau {round(retry_after)} giây."
        else:
            message +="Vui lòng thêm thời gian chờ giữa các lần gửi request."
        super().__init__(message)

class Guardian:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(Guardian, cls).__new__(cls)
                cls._instance._initialize()
            return cls._instance

    def _initialize(self):
        self.resource_limits = defaultdict(lambda: defaultdict(int))
        self.usage_counters = defaultdict(lambda: defaultdict(list))
        self.resource_limits["default"] = {"min": 60,"hour": 3000}
        self.resource_limits["TCBS"] = {"min": 60,"hour": 3000}
        self.resource_limits["VCI"] = {"min": 60,"hour": 3000}
        self.resource_limits["MBK"] = {"min": 600,"hour": 36000}
        self.resource_limits["MAS.ext"] = {"min": 600,"hour": 36000}
        self.resource_limits["VCI.ext"] = {"min": 600,"hour": 36000}
        self.resource_limits["FMK.ext"] = {"min": 600,"hour": 36000}
        self.resource_limits["VND.ext"] = {"min": 600,"hour": 36000}
        self.resource_limits["CAF.ext"] = {"min": 600,"hour": 36000}
        self.resource_limits["SPL.ext"] = {"min": 600,"hour": 36000}
        self.resource_limits["VDS.ext"] = {"min": 600,"hour": 36000}
        self.resource_limits["FAD.ext"] = {"min": 600,"hour": 36000}

    def verify(self, operation_id, resource_type="default"):
        current_time = time.time()
        limits = self.resource_limits.get(resource_type, self.resource_limits["default"])
        minute_cutoff = current_time - 60
        self.usage_counters[resource_type]["min"] = [
            t for t in self.usage_counters[resource_type]["min"]
            if t > minute_cutoff
        ]
        minute_usage = len(self.usage_counters[resource_type]["min"])
        minute_exceeded = minute_usage >= limits["min"]
        if minute_exceeded:
            from vnai.beam.metrics import collector
            collector.record(
"rate_limit",
                {
"resource_type": resource_type,
"limit_type":"min",
"limit_value": limits["min"],
"current_usage": minute_usage,
"is_exceeded": True
                },
                priority="high"
            )
            raise RateLimitExceeded(
                resource_type=resource_type,
                limit_type="min",
                current_usage=minute_usage,
                limit_value=limits["min"],
                retry_after=60 - (current_time % 60)
            )
        hour_cutoff = current_time - 3600
        self.usage_counters[resource_type]["hour"] = [
            t for t in self.usage_counters[resource_type]["hour"]
            if t > hour_cutoff
        ]
        hour_usage = len(self.usage_counters[resource_type]["hour"])
        hour_exceeded = hour_usage >= limits["hour"]
        from vnai.beam.metrics import collector
        collector.record(
"rate_limit",
            {
"resource_type": resource_type,
"limit_type":"hour" if hour_exceeded else"min",
"limit_value": limits["hour"] if hour_exceeded else limits["min"],
"current_usage": hour_usage if hour_exceeded else minute_usage,
"is_exceeded": hour_exceeded
            }
        )
        if hour_exceeded:
            raise RateLimitExceeded(
                resource_type=resource_type,
                limit_type="hour",
                current_usage=hour_usage,
                limit_value=limits["hour"],
                retry_after=3600 - (current_time % 3600)
            )
        self.usage_counters[resource_type]["min"].append(current_time)
        self.usage_counters[resource_type]["hour"].append(current_time)
        return True

    def usage(self, resource_type="default"):
        current_time = time.time()
        limits = self.resource_limits.get(resource_type, self.resource_limits["default"])
        minute_cutoff = current_time - 60
        hour_cutoff = current_time - 3600
        self.usage_counters[resource_type]["min"] = [
            t for t in self.usage_counters[resource_type]["min"]
            if t > minute_cutoff
        ]
        self.usage_counters[resource_type]["hour"] = [
            t for t in self.usage_counters[resource_type]["hour"]
            if t > hour_cutoff
        ]
        minute_usage = len(self.usage_counters[resource_type]["min"])
        hour_usage = len(self.usage_counters[resource_type]["hour"])
        minute_percentage = (minute_usage / limits["min"]) * 100 if limits["min"] > 0 else 0
        hour_percentage = (hour_usage / limits["hour"]) * 100 if limits["hour"] > 0 else 0
        return max(minute_percentage, hour_percentage)

    def get_limit_status(self, resource_type="default"):
        current_time = time.time()
        limits = self.resource_limits.get(resource_type, self.resource_limits["default"])
        minute_cutoff = current_time - 60
        hour_cutoff = current_time - 3600
        minute_usage = len([t for t in self.usage_counters[resource_type]["min"] if t > minute_cutoff])
        hour_usage = len([t for t in self.usage_counters[resource_type]["hour"] if t > hour_cutoff])
        return {
"resource_type": resource_type,
"minute_limit": {
"usage": minute_usage,
"limit": limits["min"],
"percentage": (minute_usage / limits["min"]) * 100 if limits["min"] > 0 else 0,
"remaining": max(0, limits["min"] - minute_usage),
"reset_in_seconds": 60 - (current_time % 60)
            },
"hour_limit": {
"usage": hour_usage,
"limit": limits["hour"],
"percentage": (hour_usage / limits["hour"]) * 100 if limits["hour"] > 0 else 0,
"remaining": max(0, limits["hour"] - hour_usage),
"reset_in_seconds": 3600 - (current_time % 3600)
            }
        }
guardian = Guardian()

class CleanErrorContext:
    _last_message_time = 0
    _message_cooldown = 5

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is RateLimitExceeded:
            current_time = time.time()
            if current_time - CleanErrorContext._last_message_time >= CleanErrorContext._message_cooldown:
                print(f"\n⚠️ {str(exc_val)}\n")
                CleanErrorContext._last_message_time = current_time
            import sys
            sys.exit(f"Rate limit exceeded. {str(exc_val)} Process terminated.")
            return False
        return False

def optimize(resource_type='default', loop_threshold=10, time_window=5, ad_cooldown=150, content_trigger_threshold=3,
            max_retries=2, backoff_factor=2, debug=False):
    if callable(resource_type):
        func = resource_type
        return _create_wrapper(func,'default', loop_threshold, time_window, ad_cooldown, content_trigger_threshold,
                             max_retries, backoff_factor, debug)
    if loop_threshold < 2:
        raise ValueError(f"loop_threshold must be at least 2, got {loop_threshold}")
    if time_window <= 0:
        raise ValueError(f"time_window must be positive, got {time_window}")
    if content_trigger_threshold < 1:
        raise ValueError(f"content_trigger_threshold must be at least 1, got {content_trigger_threshold}")
    if max_retries < 0:
        raise ValueError(f"max_retries must be non-negative, got {max_retries}")
    if backoff_factor <= 0:
        raise ValueError(f"backoff_factor must be positive, got {backoff_factor}")

    def decorator(func):
        return _create_wrapper(func, resource_type, loop_threshold, time_window, ad_cooldown, content_trigger_threshold,
                             max_retries, backoff_factor, debug)
    return decorator

def _create_wrapper(func, resource_type, loop_threshold, time_window, ad_cooldown, content_trigger_threshold,
                   max_retries, backoff_factor, debug):
    call_history = []
    last_ad_time = 0
    consecutive_loop_detections = 0
    session_displayed = False
    session_start_time = time.time()
    session_timeout = 1800
    @functools.wraps(func)

    def wrapper(*args, **kwargs):
        nonlocal last_ad_time, consecutive_loop_detections, session_displayed, session_start_time
        current_time = time.time()
        content_triggered = False
        if current_time - session_start_time > session_timeout:
            session_displayed = False
            session_start_time = current_time
        retries = 0
        while True:
            call_history.append(current_time)
            while call_history and current_time - call_history[0] > time_window:
                call_history.pop(0)
            loop_detected = len(call_history) >= loop_threshold
            if debug and loop_detected:
                print(f"[OPTIMIZE] Đã phát hiện vòng lặp cho {func.__name__}: {len(call_history)} lần gọi trong {time_window}s")
            if loop_detected:
                consecutive_loop_detections += 1
                if debug:
                    print(f"[OPTIMIZE] Số lần phát hiện vòng lặp liên tiếp: {consecutive_loop_detections}/{content_trigger_threshold}")
            else:
                consecutive_loop_detections = 0
            should_show_content = (consecutive_loop_detections >= content_trigger_threshold) and                                 (current_time - last_ad_time >= ad_cooldown) and                                 not session_displayed
            if should_show_content:
                last_ad_time = current_time
                consecutive_loop_detections = 0
                content_triggered = True
                session_displayed = True
                if debug:
                    print(f"[OPTIMIZE] Đã kích hoạt nội dung cho {func.__name__}")
                try:
                    from vnai.scope.promo import manager
                    try:
                        from vnai.scope.profile import inspector
                        environment = inspector.examine().get("environment", None)
                        manager.present_content(environment=environment, context="loop")
                    except ImportError:
                        manager.present_content(context="loop")
                except ImportError:
                    print(f"Phát hiện vòng lặp: Hàm '{func.__name__}' đang được gọi trong một vòng lặp")
                except Exception as e:
                    if debug:
                        print(f"[OPTIMIZE] Lỗi khi hiển thị nội dung: {str(e)}")
            try:
                with CleanErrorContext():
                    guardian.verify(func.__name__, resource_type)
            except RateLimitExceeded as e:
                from vnai.beam.metrics import collector
                collector.record(
"error",
                    {
"function": func.__name__,
"error": str(e),
"context":"resource_verification",
"resource_type": resource_type,
"retry_attempt": retries
                    },
                    priority="high"
                )
                if not session_displayed:
                    try:
                        from vnai.scope.promo import manager
                        try:
                            from vnai.scope.profile import inspector
                            environment = inspector.examine().get("environment", None)
                            manager.present_content(environment=environment, context="loop")
                            session_displayed = True
                            last_ad_time = current_time
                        except ImportError:
                            manager.present_content(context="loop")
                            session_displayed = True
                            last_ad_time = current_time
                    except Exception:
                        pass
                if retries < max_retries:
                    wait_time = backoff_factor ** retries
                    retries += 1
                    if hasattr(e,"retry_after") and e.retry_after:
                        wait_time = min(wait_time, e.retry_after)
                    if debug:
                        print(f"[OPTIMIZE] Đã đạt giới hạn tốc độ cho {func.__name__}, thử lại sau {wait_time} giây (lần thử {retries}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
            start_time = time.time()
            success = False
            error = None
            try:
                result = func(*args, **kwargs)
                success = True
                return result
            except Exception as e:
                error = str(e)
                raise
            finally:
                execution_time = time.time() - start_time
                try:
                    from vnai.beam.metrics import collector
                    collector.record(
"function",
                        {
"function": func.__name__,
"resource_type": resource_type,
"execution_time": execution_time,
"success": success,
"error": error,
"in_loop": loop_detected,
"loop_depth": len(call_history),
"content_triggered": content_triggered,
"timestamp": datetime.now().isoformat(),
"retry_count": retries if retries > 0 else None
                        }
                    )
                    if content_triggered:
                        collector.record(
"ad_opportunity",
                            {
"function": func.__name__,
"resource_type": resource_type,
"call_frequency": len(call_history) / time_window,
"consecutive_loops": consecutive_loop_detections,
"timestamp": datetime.now().isoformat()
                            }
                        )
                except ImportError:
                    pass
            break
    return wrapper

def rate_limit_status(resource_type="default"):
    return guardian.get_limit_status(resource_type)