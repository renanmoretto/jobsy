import time
import traceback
from typing import Callable, Literal, Optional, List
from datetime import datetime, timedelta
from functools import partial

import pytz


class Scheduler:
    def __init__(self, safe_execution: bool = True):
        self.jobs: List[Job] = []
        self.safe_execution = safe_execution

    def every(self, interval: str) -> 'Job':
        return Job(self, func=None, job_type='periodic', interval=interval)

    def add_job(self, job: 'Job'):
        self.jobs.append(job)

    def run_pending(self):
        for job in self.jobs:
            if job.should_run:
                job.run(self.safe_execution)

    def run_all(self):
        for job in self.jobs:
            job.run(self.safe_execution)

    def loop(self, interval: int = 1):
        while True:
            self.run_pending()
            time.sleep(interval)


class Job:
    def __init__(
        self,
        scheduler: Scheduler,
        job_type: Literal['periodic', 'time'],
        interval: Optional[str] = None,
        at: Optional[str] = None,
        func: Optional[Callable] = None,
        name: Optional[str] = None,
    ):
        if job_type == 'periodic':
            if interval is None:
                raise ValueError("'interval' is required for periodic jobs")
        elif job_type == 'time':
            if at is None:
                raise ValueError("'at' is required for time jobs")

        self.scheduler = scheduler
        self.func = func
        self.job_type = job_type
        self.interval = interval
        self.at = at
        if name is None:
            if func is None:
                self.name = None
            else:
                self.name = func.__name__
        else:
            self.name = name
        self.how: Literal['sync', 'thread', 'process'] = 'sync'
        self.days: List[str] = []
        self.days_int: List[int] = []
        self.start: Optional[str] = None
        self.end: Optional[str] = None
        self.last_run: Optional[datetime] = None
        self.next_run: datetime = self._calc_next_run()

        self.scheduler.add_job(self)

    def _calc_next_run_periodic(self) -> datetime:
        # TODO: add tz
        interval_seconds = _interval_to_seconds(self.interval)
        if self.last_run is None:
            return datetime.now() + timedelta(seconds=interval_seconds)
        return self.last_run + timedelta(seconds=interval_seconds)

    def _calc_next_run_time(self) -> datetime:
        raise NotImplementedError('Time jobs are not implemented yet')

    def _calc_next_run(self) -> datetime:
        if self.job_type == 'periodic':
            return self._calc_next_run_periodic()
        elif self.job_type == 'time':
            return self._calc_next_run_time()
        return self.next_run

    @property
    def should_run(self) -> bool:
        if self.next_run is None:
            return False

        if self.start is not None and self.end is not None:
            if self.next_run < datetime.strptime(
                self.start, '%H:%M'
            ) or self.next_run > datetime.strptime(self.end, '%H:%M'):
                return False

        if self.days_int:
            if self.next_run.weekday() not in self.days_int:
                return False

        return self.next_run <= datetime.now()

    def run(self, safe: bool = True):
        func = safe_wrap(self.func, self.name) if safe else self.func
        if self.how == 'sync':
            func()
        elif self.how == 'thread':
            raise NotImplementedError('Threading is not implemented yet')
        elif self.how == 'process':
            raise NotImplementedError('Multiprocessing is not implemented yet')

        self.last_run = datetime.now()
        self.next_run = self._calc_next_run()

    def do(self, job: Callable, *args, **kwargs) -> 'Job':
        self.func = partial(job, *args, **kwargs)

        return self

    def as_name(self, name: str) -> 'Job':
        self.name = name
        return self

    def tz(self, tz: str) -> 'Job':
        self.tz = pytz.timezone(tz)
        return self

    def between(self, start: str, end: str) -> 'Job':
        self.start = start
        self.end = end
        return self

    def on(self, *days: List[str | int]) -> 'Job':
        self.days = days
        for day in days:
            if isinstance(day, str):
                days_map = {
                    'mon': 0,
                    'tue': 1,
                    'wed': 2,
                    'thu': 3,
                    'fri': 4,
                    'sat': 5,
                    'sun': 6,
                }
                try:
                    day_int = days_map[day]
                except KeyError:
                    raise ValueError(f'Invalid day: {day}')
            else:
                if day < 0 or day > 6:
                    raise ValueError(f'Invalid day: {day}')
                day_int = day
            self.days_int.append(day_int)
        return self


def safe_wrap(job: Callable, name: str | None = None):
    """
    Wrapper that returns a safe version of the given job callable.
    Any exception raised during execution will be caught and printed.
    """

    def wrapper():
        # Handle both regular functions and partial objects for name
        if name is not None:
            job_name = name
        elif hasattr(job, '__name__'):
            job_name = job.__name__
        elif hasattr(job, 'func'):  # For partial objects
            job_name = job.func.__name__
        else:
            job_name = 'unknown_job'

        try:
            job()
        except Exception as e:
            print(f'Error executing job {job_name}, {str(e)}')
            traceback.print_exc()

    return wrapper


def _interval_to_seconds(interval: str) -> int:
    unit = interval[-1]
    if unit == 's':
        return int(interval[:-1])
    elif unit == 'm':
        return int(interval[:-1]) * 60
    elif unit == 'h':
        return int(interval[:-1]) * 3600
    elif unit == 'd':
        return int(interval[:-1]) * 86400
    else:
        raise ValueError(f'Invalid interval: {interval}')


default_scheduler = Scheduler()


def every(interval: str) -> Job:
    return default_scheduler.every(interval)


def loop(interval: int = 1, scheduler: Scheduler = default_scheduler):
    scheduler.loop(interval)
