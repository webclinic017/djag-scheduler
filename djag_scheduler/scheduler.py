"""Djag Scheduler Implementation."""

import math
import traceback
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from celery import current_app
from celery.beat import (
    Scheduler, ScheduleEntry
)
from celery.utils.log import get_logger
from croniter import croniter
from django.conf import settings
from django.core.cache import caches
from django.core.cache.backends.base import InvalidCacheBackendError
from django.utils.timezone import is_aware
from kombu.utils.encoding import safe_repr

import djag_scheduler.models.user_action_model as action_choices
from djag_scheduler.models import (
    PeriodicTask, TaskDependency,
    UserAction
)

utc_zone = ZoneInfo('UTC')

try:
    DEFAULT_TIMEZONE = ZoneInfo(
        getattr(settings, '{0}_TIMEZONE'.format(current_app.namespace))
    )
except ZoneInfoNotFoundError:
    DEFAULT_TIMEZONE = utc_zone

DEFAULT_INTERVAL = getattr(settings, 'DJAG_DEFAULT_INTERVAL', 60)
SCHEDULE_CHECK_INTERVAL = getattr(settings, 'DJAG_SCHEDULE_CHECK_INTERVAL', 300)
TASK_ESTIMATED_RUN_TIME = getattr(settings, 'DJAG_TASK_ESTIMATED_RUN_TIME', 60)
RESILIENT_SYNC_INTERVAL = getattr(settings, 'DJAG_RESILIENT_SYNC_INTERVAL', 600)

logger = get_logger(__name__)


class DjagTaskEntry(ScheduleEntry):
    """Djag task entry for each periodic task"""

    OPTIONS_KEYS = ('headers', 'queue', 'exchange', 'routing_key', 'priority')

    def __init__(self, scheduler, model, app=None):  # noqa
        """Initialize the djag-task entry."""
        self.scheduler = scheduler
        self.model = model
        self.app = app or current_app._get_current_object()  # noqa

        # Initialize scheduler fields
        try:
            self.id = model.id
            self.name = model.name
            self.task = model.task
            self.crontab = model.crontab.crontab
            self.timezone = model.crontab.timezone
            self.cron_base = model.cron_base
            self.args = model.args
            self.kwargs = dict(model.kwargs)

            self.options = {}
            for option in self.__class__.OPTIONS_KEYS:
                value = getattr(model, option, None)
                if value is not None:
                    self.options[option] = value

            self.enabled = model.enabled
            self.skip_misfire = model.skip_misfire
            self.coalesce_misfire = model.coalesce_misfire
            self.grace_period = model.grace_period
            self.last_cron = DjagTaskEntry.set_timezone(model.last_cron, utc_zone)
            self.last_cron_start = model.last_cron_start
            self.last_cron_end = model.last_cron_end
            self.running = model.running
            self.exception_cron = model.exception_cron
            self.total_run_count = model.total_run_count
            self.date_changed = model.date_changed
            self.description = model.description

            self.current_cron = self.last_cron

            self.finalized = True
        except:  # noqa
            self.finalized = False

    @classmethod
    def set_timezone(cls, dt, timezone):
        """Set timezone"""
        if not dt:
            return dt

        if is_aware(dt):
            return dt.astimezone(timezone)
        else:
            dt.replace(tzinfo=timezone)

    def update_entry(self, model, fields):
        """Update the modified fields from the model"""
        self.model = model

        try:
            for field in fields:
                if field == 'crontab':
                    self.crontab = model.crontab.crontab
                    self.timezone = model.crontab.timezone
                elif field == 'last_cron':
                    self.last_cron = DjagTaskEntry.set_timezone(model.last_cron, utc_zone)
                elif field == 'kwargs':
                    self.kwargs = dict(model.kwargs)
                elif field in self.__class__.OPTIONS_KEYS:
                    value = getattr(model, field, None)
                    if value is not None:
                        self.options[field] = value
                    elif field in self.options:
                        del self.options[field]
                else:
                    setattr(self, field, getattr(model, field))

            self.finalized = True
        except:  # noqa
            self.finalized = False

    def next_cron(self, last_cron=None):
        """Determine next_cron"""
        last_cron = last_cron or self.current_cron or self.last_cron or self.cron_base

        try:
            cron_iter = croniter(self.crontab, DjagTaskEntry.set_timezone(last_cron, self.timezone))
        except:  # noqa
            return None, SCHEDULE_CHECK_INTERVAL

        prev_result = None
        while True:
            result = cron_iter.get_next(datetime).astimezone(tz=utc_zone)
            now = datetime.now(tz=utc_zone).timestamp()

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
        if not self.finalized or self.exception_cron:
            return None, SCHEDULE_CHECK_INTERVAL

        cron, sec = self.next_cron()
        if sec:
            # Skip dependency resolution when there is time to wait.
            # We might need to wait longer due to unresolved dependency
            return cron, sec

        next_tick = -math.inf
        for task_id, future_depends in DjagTaskDAG.get_dependencies(self.id):
            task_entry = self.scheduler.get_entry(task_id)

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

    def save(self, fields=('running', 'last_cron', 'last_cron_start', 'last_cron_end',
                           'exception_cron', 'total_run_count')):
        """Save model state to the DB"""
        for field in fields:
            value = getattr(self, field)
            setattr(self.model, field, value)

        try:
            # Update specific fields to prevent reinsertion if model is deleted else-where
            self.model.save(
                update_fields=fields, insert_task_change=False
            )
        except ValueError:
            # Model is deleted
            return True, 'deleted'
        except:  # noqa
            # Save error
            return False, 'unknown'

        return True, 'success'

    def activate(self, cron):
        """The task djag-entry represents is in execution"""
        self.running += 1
        self.last_cron_start = datetime.now(tz=utc_zone)

        if not self.current_cron or (cron and cron > self.current_cron):
            self.current_cron = cron

        return self.save(fields=('running', 'last_cron_start'))

    def handle_exception(self, cron):
        """The task djag-entry represents resulted in a exception"""
        if not self.exception_cron or (cron and cron < self.exception_cron):
            self.exception_cron = cron

            # Update current_cron to last_cron to reschedule self.exception_cron (when cleared)
            self.current_cron = self.last_cron

        return self.save(fields=('exception_cron',))

    def deactivate(self, cron):
        """The task djag-entry represents completed execution"""
        self.running -= 1
        self.last_cron_end = datetime.now(tz=utc_zone)
        self.total_run_count += 1

        if not self.last_cron or (cron and cron > self.last_cron):
            self.last_cron = cron

        return self.save(fields=('running', 'last_cron', 'last_cron_end',
                                 'total_run_count'))

    def __reduce__(self):
        return self.__class__, (
            self.id, self.name, self.task, self.crontab, self.timezone,
            self.running, self.last_cron, self.last_cron_start,
            self.last_cron_end, self.total_run_count, self.args,
            self.kwargs, self.options
        )

    def __repr__(self):
        return '<DjagTaskEntry: task_id-{0} {1} {2}(*{3}, **{4}) {5} {6}>'.format(
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
        cls.__task_dag = defaultdict(set)
        for dependency in TaskDependency.objects.all():
            cls.__task_dag[dependency.depender.pk].add((
                dependency.dependee.pk,
                dependency.future_depends
            ))

        # Add tasks that have no dependency
        for task in PeriodicTask.objects.all():
            if task.pk not in cls.__task_dag:
                cls.__task_dag[task.pk] = set()

        return True

    @classmethod
    def get_dependencies(cls, task_id):
        """Get dependencies of task"""
        if cls.__task_dag or cls.compute_task_dag():
            return cls.__task_dag.get(task_id)


class DjagScheduler(Scheduler):
    """Database-backed Djag Scheduler."""

    Entry = DjagTaskEntry
    Model = PeriodicTask
    Changes = UserAction

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._schedule = {}
        self._entry_dict = {}
        self._run_id_to_entry = {}
        self._to_save = {}

        # For init-ing the schedule and task-dag for the first time
        self._update_schedule = True
        self._update_task_dag = True

        # Clear existing changes on init
        self._schedule_last_check = None

        self.sync_every = RESILIENT_SYNC_INTERVAL
        self.max_interval = DEFAULT_INTERVAL

    @classmethod
    def clean_model(cls, model):
        """Clean model in-case of abrupt terminations"""
        not_first_run = model.last_cron_start or model.last_cron_end
        dirty_first_run = model.last_cron_start and not model.last_cron_end

        if not_first_run and (
                dirty_first_run or model.running or
                model.last_cron_start > model.last_cron_end
        ):
            model.running = 0
            model.last_cron_start = model.last_cron_end

            return False

        return True

    def setup_schedule(self):
        """Setup default schedules"""
        pass  # Override parent's

    def delete_entry(self, task_id):
        """Delete entry from the to_save, entry_dict"""
        if task_id in self._to_save:
            del self._to_save[task_id]

        if task_id in self._entry_dict:
            del self._entry_dict[task_id]

    def handle_entry_save(self, entry, saved, status):
        """Handle DjagTaskEntry save"""
        if status == 'deleted':
            self.delete_entry(entry.id)
        elif saved:
            # If entry saved remove it from to_save dict
            if entry.id in self._to_save:
                del self._to_save[entry.id]
        elif not saved:
            # If entry not saved add to to_save dict
            self._to_save[entry.id] = entry

    def apply_async(self, entry, producer=None, advance=True, **kwargs):
        """Override apply_sync to include custom task_id and to activate entry"""
        run_id = str(uuid.uuid4())
        cron = kwargs.pop('cron', None)

        # Try passing djag_run_dt and on TypeError pass with out it.
        entry.kwargs['djag_run_dt'] = cron
        entry.options.update({'task_id': run_id})

        try:
            try:
                result = super().apply_async(entry, producer, advance, **kwargs)
            except Exception as exc:
                if isinstance(exc.__context__, TypeError):
                    entry.kwargs.pop('djag_run_dt')
                    result = super().apply_async(entry, producer, advance, **kwargs)
                else:
                    raise exc
        except Exception as exc:  # noqa
            # Notify DjagTaskEntry of exception
            self.handle_entry_save(entry, *entry.handle_exception(cron))

            raise exc

        self._run_id_to_entry[run_id] = entry.id, cron
        self.handle_entry_save(entry, *entry.activate(cron))

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

        # Culminate runs
        self.culminate_tasks()

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
        Sync un-synced entries to DB. entry.activate/deactivate() syncs changes
        this is to build resilience against tasks whose sync-failed
        """
        for entry_id in list(self._to_save.keys()):
            entry = self._to_save[entry_id]
            self.handle_entry_save(entry, *entry.save())

    @property
    def schedule(self):
        """Return schedule"""
        now = datetime.now(tz=utc_zone)
        if not self._schedule_last_check or (
                self._schedule_last_check + timedelta(seconds=SCHEDULE_CHECK_INTERVAL) <= now
        ):
            self._schedule_last_check = now
            try:
                # One TASK_CHANGED added per SCHEDULE_CHANGED per task. Reading SCHEDULE_CHANGED to delete.
                changes = self.Changes.objects.filter(action__in=[
                    action_choices.TASK_CHANGED,
                    action_choices.DEPENDENCY_CHANGED,
                    action_choices.SCHEDULE_CHANGED
                ])

                task_updates = {}
                for change in changes:
                    if change.action == action_choices.TASK_CHANGED:
                        self._update_schedule = True

                        payload = change.payload
                        if payload['status'] == 'deleted':
                            self.delete_entry(payload['task_id'])
                        elif task_id := payload['task_id']:
                            if task_id not in task_updates:
                                task_updates[task_id] = set()

                            task_updates[task_id].update(payload['fields'])
                    elif change.action == action_choices.DEPENDENCY_CHANGED:
                        self._update_task_dag = True

                # Delete already read changes
                changes.delete()

                # Construct schedule dict
                if self._update_schedule:
                    self._update_schedule = False
                    self._schedule = {}

                    for model in self.Model.objects.enabled():
                        if model.pk not in self._entry_dict:
                            self.__class__.clean_model(model)  # Clean model when it is loaded for the first time
                            self._entry_dict[model.pk] = self.Entry(self, model, app=self.app)
                        elif model.pk in task_updates:
                            self._entry_dict[model.pk].update_entry(
                                model, task_updates[model.pk]
                            )  # Update the modified fields in the task-entry

                        self._schedule[model.pk] = self._entry_dict[model.pk]

                # Dependencies changed! Update dependency DAG.
                if self._update_task_dag:
                    self._update_task_dag = False
                    DjagTaskDAG.compute_task_dag()
            except:  # noqa
                pass

        return self._schedule

    def culminate_tasks(self):
        """Invoke deactivation on the entries"""
        try:
            djag_cache = caches['djag_scheduler']
        except InvalidCacheBackendError:
            return

        run_status = djag_cache.get_many(list(self._run_id_to_entry.keys()))
        run_ids = list(run_status.keys())

        if not run_ids:
            return

        for run_id in run_ids:
            entry_id, cron = self._run_id_to_entry[run_id]
            entry = self._entry_dict.get(entry_id)

            if entry:
                if run_status[run_id] == 'SUCCESS':
                    self.handle_entry_save(entry, *entry.deactivate(cron))
                else:
                    self.handle_entry_save(entry, *entry.handle_exception(cron))

            del self._run_id_to_entry[run_id]

        djag_cache.delete_many(run_ids)

    def get_entry(self, task_id):
        """Given task_id get entry"""
        return self._entry_dict.get(task_id)

    def is_disabled(self, task_id):
        """Return True if a task is disabled"""
        return task_id not in self.schedule
