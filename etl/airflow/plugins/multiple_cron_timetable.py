from airflow import settings
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import Timetable
from airflow.timetables.interval import CronDataIntervalTimetable
import pendulum

try:
    from airflow.timetables.registry import register_timetable
except Exception:
    register_timetable = None


class MultipleCronTimetable(Timetable):
    def __init__(self, cron_expressions, timezone=None):
        if not cron_expressions:
            raise ValueError("cron_expressions must not be empty")

        self._cron_expressions = tuple(cron_expressions)
        self._timezone = timezone or settings.TIMEZONE
        self._timetables = [
            CronDataIntervalTimetable(expr, timezone=self._timezone)
            for expr in self._cron_expressions
        ]

    @property
    def summary(self):
        return " | ".join(self._cron_expressions)

    def infer_manual_data_interval(self, run_after):
        return self._timetables[0].infer_manual_data_interval(run_after)

    def next_dagrun_info(self, *, last_automated_data_interval, restriction):
        candidates = []
        for timetable in self._timetables:
            info = timetable.next_dagrun_info(
                last_automated_data_interval=last_automated_data_interval,
                restriction=restriction,
            )
            if info is not None:
                candidates.append(info)

        if not candidates:
            return None

        return min(candidates, key=lambda info: info.run_after)

    def serialize(self):
        return {
            "cron_expressions": list(self._cron_expressions),
            "timezone": self._timezone.name,
        }

    @classmethod
    def deserialize(cls, data):
        timezone = pendulum.timezone(data["timezone"])
        return cls(data["cron_expressions"], timezone=timezone)


class MultipleCronTimetablePlugin(AirflowPlugin):
    name = "multiple_cron_timetable_plugin"
    timetables = [MultipleCronTimetable]


if register_timetable is not None:
    try:
        register_timetable(MultipleCronTimetable)
    except Exception:
        pass
