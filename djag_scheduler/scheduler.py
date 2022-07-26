"""Djag Scheduler Implementation."""

import uuid
from collections import defaultdict
from datetime import datetime, timedelta
import math
import traceback

from django.conf import settings
from django.db import transaction, close_old_connections  # noqa
from django.core.cache import caches
from django.core.cache.backends.base import InvalidCacheBackendError
from django.utils.timezone import is_aware

from celery import current_app
from celery.beat import (
    Scheduler, ScheduleEntry, _evaluate_entry_args, _evaluate_entry_kwargs  # noqa
)
from celery.utils.log import get_logger
from kombu.utils.encoding import safe_repr
from croniter import croniter
import pytz
from pytz.exceptions import UnknownTimeZoneError

import djag_scheduler.models.user_action_model as action_choices
from djag_scheduler.models import (
    PeriodicTask, TaskDependency,
    UserAction
)

try:
    DEFAULT_TIMEZONE = pytz.timezone(
        getattr(settings, '{0}_TIMEZONE'.format(current_app.namespace))
    )
except UnknownTimeZoneError:
    DEFAULT_TIMEZONE = pytz.utc

DEFAULT_INTERVAL = getattr(settings, 'DJAG_DEFAULT_INTERVAL', 60)
SCHEDULE_CHECK_INTERVAL = getattr(settings, 'DJAG_SCHEDULE_CHECK_INTERVAL', 300)
TASK_ESTIMATED_RUN_TIME = getattr(settings, 'DJAG_TASK_ESTIMATED_RUN_TIME', 60)
RESILIENT_SYNC_INTERVAL = getattr(settings, 'DJAG_RESILIENT_SYNC_INTERVAL', 600)

logger = get_logger(__name__)


class DjagTaskEntry(ScheduleEntry):
    """Djag task entry for each periodic task"""

    def __init__(self, scheduler, model, app=None):  # noqa
        """Initialize the djag-task entry."""
        self.app = app or current_app._get_current_object()  # noqa
        self.scheduler = scheduler
        self.finalized = True

        DjagTaskEntry.clean_model(model)

        # Initialize scheduler fields
        self.id = model.pk
        self.running = model.running
        self.last_cron = DjagTaskEntry.set_timezone(model.last_cron, pytz.utc)
        self.last_cron_start = model.last_cron_start
        self.last_cron_end = model.last_cron_end
        self.current_cron = self.last_cron
        self.total_run_count = model.total_run_count

        self.update_entry(model)

    @classmethod
    def clean_model(cls, model):
        """Clean model in-case of abrupt terminations"""
        fresh_run = not (model.last_cron_start or model.last_cron_end)
        dirty_first_run = model.last_cron_start and not model.last_cron_end

        def unfinished(): return model.running != 0 or model.last_cron_start > model.last_cron_end

        if not fresh_run and (dirty_first_run or unfinished()):
            model.running = 0
            model.last_cron_start = model.last_cron_end

            return False

        return True

    @classmethod
    def set_timezone(cls, dt, timezone):
        """Set timezone"""
        if not dt:
            return dt

        if is_aware(dt):
            return dt.astimezone(timezone)
        else:
            dt.replace(tzinfo=timezone)

    def update_entry(self, model):
        """Update non-scheduler fields from the new model"""

        self.model = model  # noqa
        self.name = model.name
        self.task = model.task
        self.cron_base = model.cron_base  # noqa
        self.skip_misfire = model.skip_misfire  # noqa
        self.coalesce_misfire = model.coalesce_misfire  # noqa
        self.grace_period = model.grace_period  # noqa
        self.args = model.args or []
        self.kwargs = model.kwargs or {}
        self.options = {'headers': model.headers or {}}

        for option in ['queue', 'exchange', 'routing_key', 'priority']:
            value = getattr(model, option)
            if value is None:
                continue
            self.options[option] = value

        try:
            self.crontab = model.crontab.crontab  # noqa
            self.timezone = model.crontab.timezone  # noqa
        except model.DoesNotExist:
            self.finalized = False

    def next_cron(self, last_cron=None):
        """Determine next_cron"""
        last_cron = last_cron or self.current_cron or self.last_cron or self.cron_base

        try:
            cron_iter = croniter(self.crontab, DjagTaskEntry.set_timezone(last_cron, self.timezone))
        except Exception:   # noqa
            return None, SCHEDULE_CHECK_INTERVAL

        prev_result = None
        while True:
            result = cron_iter.get_next(datetime).astimezone(tz=pytz.utc)
            now = datetime.now(tz=pytz.utc).timestamp()

            interval = result.timestamp() - now
            if self.skip_misfire and -interval > self.grace_period:  # Skip misfires when grace_period is exceeded
                continue
            elif self.coalesce_misfire:
                if -interval > self.grace_period:
                    prev_result = result
                    continue
                else:
                    if prev_result:  # If there were misfires, coalesce them into one event (last misfired event).
                        return prev_result, 0

            # Default return condition
            return result, max(0, interval)

    def is_due(self):
        """Determine the task's due status"""
        if not self.finalized:
            return None, SCHEDULE_CHECK_INTERVAL

        cron, sec = self.next_cron()
        if sec:
            # Skip dependency resolution when there is time to wait.
            # We might need to wait longer due to unresolved dependency
            return cron, sec

        next_tick = -math.inf
        for task_pk, future_depends, change_dt in DjagTaskDAG.get_dependency(self.id):
            task_entry = self.scheduler.get_entry(task_pk)

            if future_depends:
                if task_entry.running:
                    # Wait for the running task to complete
                    next_tick = max(next_tick, TASK_ESTIMATED_RUN_TIME)
                else:
                    task_cron, task_sec = task_entry.next_cron()

                    # Unblock self if task's next cron falls beyond self's last cron
                    if self.last_cron and task_cron <= self.last_cron:
                        next_tick = max(next_tick, task_sec + TASK_ESTIMATED_RUN_TIME)
                    else:
                        next_tick = max(next_tick, sec)
            else:
                if task_entry.last_cron and cron <= task_entry.last_cron:
                    # Clearance from the dependency
                    next_tick = max(next_tick, sec)
                elif task_entry.running:
                    # Wait for task to complete
                    next_tick = max(next_tick, TASK_ESTIMATED_RUN_TIME)
                elif self.scheduler.is_disabled(task_entry.id):
                    # Wait for schedule changes
                    next_tick = max(next_tick, SCHEDULE_CHECK_INTERVAL)
                else:
                    # Wait for task's next execution + run-time (best case scenario)
                    _, task_sec = task_entry.next_cron()
                    next_tick = max(next_tick, task_sec + TASK_ESTIMATED_RUN_TIME)

        return cron, max(next_tick, sec)

    def save(self, fields=('running', 'last_cron', 'last_cron_start',
                           'last_cron_end', 'total_run_count')):
        """Save model state to the DB"""
        for field in fields:
            value = getattr(self, field)
            setattr(self.model, field, value)

        try:
            # Update specific fields to prevent reinsertion if model is deleted else-where
            self.model.save(update_fields=fields)
        except ValueError:
            # Model is deleted
            return False, 'deleted'
        except:  # noqa
            # Save error
            return False, 'unknown'

        return True, 'success'

    def activate(self, cron):
        """The task djag-entry represents is in execution"""
        self.running += 1
        self.last_cron_start = datetime.now(tz=pytz.utc)

        if not self.current_cron or (cron and cron > self.current_cron):
            self.current_cron = cron

        self.save(fields=('running', 'last_cron_start'))

    def deactivate(self, cron):
        """The task djag-entry represents completed execution"""
        self.running -= 1
        self.last_cron_end = datetime.now(tz=pytz.utc)
        self.total_run_count += 1

        if not self.last_cron or (cron and cron > self.last_cron):
            self.last_cron = cron

        self.save(fields=('running', 'last_cron', 'last_cron_end',
                          'total_run_count'))

    def __reduce__(self):
        return self.__class__, (
            self.id, self.name, self.task, self.crontab, self.timezone,
            self.running, self.last_cron, self.last_cron_start,
            self.last_cron_end, self.total_run_count, self.args,
            self.kwargs, self.options
        )

    def __repr__(self):
        return '<DjagTaskEntry: task_pk-{0} {1} {2}(*{3}, **{4}) {5} {6}>'.format(
            self.id, self.name, self.task, safe_repr(self.args),
            safe_repr(self.kwargs), self.crontab, str(self.timezone)
        )


class DjagTaskDAG:
    """Djag Periodic Tasks DAG"""
    __task_dag = None

    @classmethod
    def get_task_dag(cls):
        """Return Task DAG"""
        if cls.__task_dag or cls.compute_task_dag():
            return cls.__task_dag

    @classmethod
    def compute_task_dag(cls):
        """Compute Task DAG"""

        # Group tasks based on depender
        task_groups = defaultdict(set)
        for dependency in TaskDependency.objects.all():
            task_groups[dependency.depender.pk].add((
                dependency.dependee.id,
                dependency.future_depends,
                dependency.change_dt
            ))

        # Construct task DAG
        cls.__task_dag = {}
        for depender, info in task_groups.items():
            cls.__task_dag[depender] = info

        # Add tasks that have no dependency
        for task in PeriodicTask.objects.all():
            if task.pk not in cls.__task_dag:
                cls.__task_dag[task.pk] = set()

        return True

    @classmethod
    def get_dependency(cls, task_pk):
        """Get dependencies of task"""
        if cls.__task_dag or cls.compute_task_dag():
            return cls.__task_dag.get(task_pk)


class DjagScheduler(Scheduler):
    """Database-backed Djag Scheduler."""

    Entry = DjagTaskEntry
    Model = PeriodicTask
    Changes = UserAction

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._schedule = {}
        self._entry_dict = {}
        self._taskid_to_entry = {}

        # Clear existing changes on init
        self._schedule_last_check = None
        self.schedule_changes()
        self._update_schedule = True

        self.sync_every = RESILIENT_SYNC_INTERVAL
        self.max_interval = DEFAULT_INTERVAL

    def setup_schedule(self):
        """Setup default schedules"""
        pass  # Override parent's

    def schedule_changes(self):
        """Look for schedule changes"""
        def check_exceeded():
            return (self._schedule_last_check + timedelta(seconds=SCHEDULE_CHECK_INTERVAL) <
                    datetime.now(tz=pytz.utc))

        if self._schedule_last_check and not check_exceeded():
            return False

        self._schedule_last_check = datetime.now(tz=pytz.utc)

        try:
            changes = self.Changes.objects.filter(action__in=[
                action_choices.TASK_CHANGED,
                action_choices.DEPENDENCY_CHANGED,
                action_choices.SCHEDULE_CHANGED
            ])

            if changes:
                changes.delete()
                self._update_schedule = True
                return True
        except:  # noqa
            pass

        return False

    def _apply_async(self, entry, producer):
        """Core logic of apply_async"""
        task = self.app.tasks.get(entry.task)
        entry_args = _evaluate_entry_args(entry.args)
        entry_kwargs = _evaluate_entry_kwargs(entry.kwargs)

        if task:
            return task.apply_async(
                entry_args, entry_kwargs, producer=producer, **entry.options
            )
        else:
            return self.send_task(
                entry.task, entry_args, entry_kwargs, producer=producer, **entry.options
            )

    def apply_async(self, entry, producer=None, advance=True, **kwargs):
        """Override apply_sync to include custom task_id and to activate entry"""
        task_id = str(uuid.uuid4())
        cron = kwargs.pop('cron', None)

        # Try passing djag_run_dt and on TypeError pass with out it.
        entry.kwargs['djag_run_dt'] = cron
        entry.options.update({'task_id': task_id})
        try:
            try:
                result = self._apply_async(entry, producer)
            except TypeError:
                entry.kwargs.pop('djag_run_dt')
                result = self._apply_async(entry, producer)
        except Exception as exc:
            raise exc
        else:
            self._taskid_to_entry[task_id] = entry.id, cron
            entry.activate(cron)
        finally:
            self._tasks_since_sync += 1
            if self.should_sync():
                self._do_sync()

        return result

    def apply_entry(self, entry, producer=None, **kwargs):
        logger.info('DjagScheduler: Sending due task %s (%s)', entry.name, entry.task)
        try:
            result = self.apply_async(entry, producer=producer, advance=False, **kwargs)
        except Exception as exc:
            logger.error('Message Error: %s\n%s', exc, traceback.format_stack(), exc_info=True)
        else:
            logger.debug('%s sent. id->%s', entry.task, result.id)

    def tick(self, *args, **kwargs):
        """Scheduler main entry point"""

        # Finish off executions
        self.culminate_tasks()

        # Check schedule changes
        self.schedule_changes()

        next_tick = math.inf
        for entry in self.schedule.values():
            cron, sec = entry.is_due()
            if sec == 0:
                next_tick = min(next_tick, entry.next_cron(cron)[1])
                self.apply_entry(entry, producer=self.producer, cron=cron)
            else:
                next_tick = min(next_tick, sec)

        return min(next_tick, SCHEDULE_CHECK_INTERVAL, DEFAULT_INTERVAL)

    def reserve(self, entry):
        return entry

    def sync(self):
        """
        Sync (may be) un-synced entries to DB.
        entry.activate/deactivate() continuously syncs changes
        this is to build resilience against tasks whose sync-failed
        """
        for task_pk in list(self._entry_dict.keys()):
            status, code = self._entry_dict[task_pk].save()
            if code == 'deleted':
                # model is deleted remove from entry_dict as well
                del self._entry_dict[task_pk]

    @property
    def schedule(self):
        """Return schedule"""
        if self._update_schedule:
            self._update_schedule = False

            self._schedule = {}
            for model in self.Model.objects.enabled():
                if model.pk in self._entry_dict:
                    self._entry_dict[model.pk].update_entry(model)
                else:
                    self._entry_dict[model.pk] = self.Entry(self, model, app=self.app)

                self._schedule[model.pk] = self._entry_dict[model.pk]

            # Recompute task DAG
            DjagTaskDAG.compute_task_dag()

        return self._schedule

    def culminate_tasks(self):
        """Invoke deactivation on the entries"""
        try:
            task_cache = caches['djag_scheduler']
        except InvalidCacheBackendError:
            return

        task_status = task_cache.get_many(list(self._taskid_to_entry.keys()))
        task_ids = list(task_status.keys())

        if not task_ids:
            return

        for task_id in task_ids:
            entry_id, cron = self._taskid_to_entry[task_id]
            entry = self._entry_dict.get(entry_id)

            if entry:
                entry.deactivate(cron)

            del self._taskid_to_entry[task_id]

        task_cache.delete_many(task_ids)

    def get_entry(self, task_pk):
        """Given task_pk get entry"""
        return self._entry_dict.get(task_pk)

    def is_disabled(self, task_pk):
        """Return True if a task is disabled"""
        return task_pk not in self.schedule
