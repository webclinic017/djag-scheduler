"""Djag Scheduler Implementation."""

import math
import queue
import traceback
import uuid
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from celery import current_app
from celery.beat import (
    Scheduler, ScheduleEntry
)
from celery.utils.log import get_logger
from croniter import croniter
from django.conf import settings
from django.utils.timezone import is_naive
from kombu.utils.encoding import safe_repr

import djag_scheduler.models.user_action_model as action_choices
from djag_scheduler.event_queue import DjagEventQueue
from djag_scheduler.models import (
    PeriodicTask, TaskDependency,
    UserAction
)

utc_zone = ZoneInfo('UTC')

MAX_WAIT_INTERVAL = getattr(settings, 'DJAG_MAX_WAIT_INTERVAL') or 0

try:
    DEFAULT_TIMEZONE = ZoneInfo(
        getattr(settings, '{0}_TIMEZONE'.format(current_app.namespace))
    )
except ZoneInfoNotFoundError:
    DEFAULT_TIMEZONE = utc_zone

SYNC_RETRY_INTERVAL = getattr(settings, 'DJAG_SYNC_RETRY_INTERVAL')
if SYNC_RETRY_INTERVAL is None or SYNC_RETRY_INTERVAL <= 0:
    SYNC_RETRY_INTERVAL = 600

logger = get_logger(__name__)


def utc_time():
    """Current UTC time"""
    return datetime.now(tz=utc_zone)


def utc_timestamp():
    """Current UTC timestamp"""
    return utc_time().timestamp()


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
            self.cron_base = self.__class__.set_timezone(
                model.cron_base
            )
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
            self.last_cron = self.__class__.set_timezone(
                model.last_cron, rep_tz=utc_zone
            )
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
    def set_timezone(cls, dt, rep_tz=DEFAULT_TIMEZONE, res_tz=utc_zone):
        """Set timezone"""
        if not dt:
            return dt

        # Format the date with the right timezone
        if is_naive(dt):
            dt = dt.replace(tz_info=rep_tz)

        return dt.astimezone(res_tz)

    def update_entry(self, model, fields):
        """Update the modified fields from the model"""
        self.model = model

        try:
            for field in fields:
                if field == 'crontab':
                    self.crontab = model.crontab.crontab
                    self.timezone = model.crontab.timezone
                elif field == 'cron_base':
                    self.cron_base = self.__class__.set_timezone(
                        model.cron_base
                    )
                elif field == 'last_cron':
                    self.last_cron = self.__class__.set_timezone(
                        model.last_cron, rep_tz=utc_zone
                    )
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

    def next_cron(self, cron=None):
        """Determine next_cron"""
        cron = cron or self.current_cron or self.last_cron or self.cron_base

        try:
            cron_iter = croniter(
                self.crontab, self.__class__.set_timezone(cron, res_tz=self.timezone)
            )
        except:  # noqa
            return None, -1

        prev_result = None
        while True:
            result = cron_iter.get_next(datetime).astimezone(tz=utc_zone)
            interval = utc_timestamp() - result.timestamp()

            # Compute next cron
            if self.skip_misfire and interval > self.grace_period:  # Skip misfires when grace_period is exceeded
                continue
            elif self.coalesce_misfire:
                if interval > self.grace_period:
                    prev_result = result
                    continue
                else:
                    if prev_result:  # If there were misfires, coalesce them into one event (last misfired event).
                        return prev_result, 0

            # Default return condition
            return result, max(0.0, -interval)

    def is_due(self):
        """Determine the task's due status"""
        if not self.finalized or self.exception_cron:
            # Wait for the schedule change
            return None, -1

        cron, sec = self.next_cron()
        if sec:
            # Skip dependency resolution when there is time to wait / When next_cron returns error.
            # We might need to wait longer due to unresolved dependency
            return cron, sec

        for task_id, future_depends in DjagTaskDAG.get_dependencies(self.id):
            task_entry = self.scheduler.get_entry(task_id)

            if future_depends:
                if task_entry.running:
                    # Wait for the running task to complete
                    return None, -1
                else:
                    task_cron, task_sec = task_entry.next_cron()
                    if task_cron is None:
                        # Wait for the exception to be fixed by the schedule change
                        return None, -1

                    # Only wait if the task's next cron falls before self's last cron
                    if self.last_cron and task_cron <= self.last_cron:
                        return None, -1
            else:
                if not task_entry.last_cron or cron > task_entry.last_cron:
                    # Wait for the dependency to run
                    return None, -1

        # Task (self) ready for being scheduled!
        return cron, 0

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
        self.last_cron_start = utc_time()

        if not self.current_cron or (cron and cron > self.current_cron):
            self.current_cron = cron

        return self.save(fields=('running', 'last_cron_start'))

    def handle_exception(self, cron):
        """The task djag-entry represents resulted in a exception"""
        self.running -= 1

        if not self.exception_cron or (cron and cron < self.exception_cron):
            self.exception_cron = cron

            # Update current_cron to last_cron to reschedule self.exception_cron (when cleared)
            self.current_cron = self.last_cron

        return self.save(fields=('running', 'exception_cron'))

    def deactivate(self, cron):
        """The task djag-entry represents completed execution"""
        self.running -= 1
        self.last_cron_end = utc_time()
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
        return '<{0}: task_id-{1} {2} {3}(*{4}, **{5}) {6} {7}>'.format(
            self.__class__.__qualname__, self.id, self.name, self.task,
            safe_repr(self.args), safe_repr(self.kwargs), self.crontab,
            str(self.timezone)
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
        self._refresh_schedule = True
        self._update_schedule = True
        self._update_task_dag = True

        # Task post execution event queue
        self._tpe_queue = queue.SimpleQueue()

        # Sync params
        self._last_sync_ts = None

        self.sync_every = SYNC_RETRY_INTERVAL
        self.max_interval = MAX_WAIT_INTERVAL

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
        cron = kwargs.pop('cron', None)
        run_id = '{0}-{1}-{2}'.format(
            entry.id, cron.timestamp(), str(uuid.uuid4())
        )

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
        logger.info(
            '%s: Sending due task %s (%s)',
            self.__class__.__qualname__, entry.name, entry.task
        )
        try:
            result = self.apply_async(entry, producer=producer, advance=False, **kwargs)
        except Exception as exc:
            logger.error('Message Error: %s\n%s', exc, traceback.format_stack(), exc_info=True)
        else:
            logger.debug('%s sent. id->%s', entry.task, result.id)

    def tick(self, *args, **kwargs):
        """Scheduler main entry point"""

        while True:  # Once it comes it can never go!
            next_tick = math.inf
            for entry in self.schedule.values():
                cron, sec = entry.is_due()
                if sec == 0:
                    # Compute next_cron and consider next_sec only when next_cron() succeeds
                    next_cron, next_sec = entry.next_cron(cron)
                    if next_cron:
                        next_tick = min(next_tick, next_sec)

                    self.apply_entry(entry, producer=self.producer, cron=cron)
                elif sec > 0:  # sec can be negative value, which indicates wait for the Djag events
                    next_tick = min(next_tick, sec)

            if MAX_WAIT_INTERVAL > 0:
                next_tick = min(next_tick, MAX_WAIT_INTERVAL)

            # Wait till task cron / Process the events with a grace period / Sync Schedule
            while True:
                sync_now = False
                try:
                    if self._to_save:
                        next_sync = 0 if not self._last_sync_ts else (
                                self._last_sync_ts + self.sync_every - utc_timestamp()
                        )

                        # Sync if next_sync falls before next_tick but recompute
                        # it if an event occurs before next_sync
                        if next_sync <= next_tick:
                            next_tick = next_sync
                            sync_now = True

                    event = DjagEventQueue.get(
                        block=True, timeout=None if next_tick == math.inf else next_tick
                    )
                    try:
                        payload = event.payload
                        if payload['event'] == 'TASK_EXECUTED':
                            self._tpe_queue.put((payload['task_id'], payload['state']))
                        elif payload['event'] in ('TASK_CHANGED', 'DEPENDENCY_CHANGED'):
                            self._refresh_schedule = True
                    except:  # noqa
                        pass

                    # Now just wait for little time anticipating further events
                    next_tick = 2  # No hard-and-fast reason for 2 sec
                except DjagEventQueue.Empty:
                    if sync_now:
                        self.sync()
                        self._last_sync_ts = utc_timestamp()

                    break

            # Culminate runs
            if not self._tpe_queue.empty():
                self.culminate_runs()

    def reserve(self, entry):
        return entry

    def should_sync(self):
        """Handle sync in-home"""
        return False

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
        if self._refresh_schedule:
            self._refresh_schedule = False
            try:
                # One TASK_CHANGED added per SCHEDULE_CHANGED per task. Reading SCHEDULE_CHANGED just to delete.
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

                    for model in self.Model.objects.all():
                        if model.pk not in self._entry_dict:
                            self.__class__.clean_model(model)  # Clean model when it is loaded for the first time
                            self._entry_dict[model.pk] = self.Entry(self, model, app=self.app)
                        elif model.pk in task_updates:
                            self._entry_dict[model.pk].update_entry(
                                model, task_updates[model.pk]
                            )  # Update the modified fields in the task-entry

                        if model.enabled:
                            self._schedule[model.pk] = self._entry_dict[model.pk]

                # Dependencies changed! Update dependency DAG.
                if self._update_task_dag:
                    self._update_task_dag = False
                    DjagTaskDAG.compute_task_dag()
            except:  # noqa
                pass

        return self._schedule

    def culminate_runs(self):
        """Invoke deactivation on the entries"""
        while not self._tpe_queue.empty():
            run_id, state = self._tpe_queue.get(block=False)
            # If scheduler restarts references are lost
            try:
                entry_id, cron = self._run_id_to_entry[run_id]
                entry = self._entry_dict.get(entry_id)
            except:  # noqa
                continue

            if entry:
                if state == 'SUCCESS':
                    self.handle_entry_save(entry, *entry.deactivate(cron))
                else:
                    self.handle_entry_save(entry, *entry.handle_exception(cron))

            del self._run_id_to_entry[run_id]

    def get_entry(self, task_id):
        """Given task_id get entry"""
        return self._entry_dict.get(task_id)

    def is_disabled(self, task_id):
        """Return True if a task is disabled"""
        return task_id not in self.schedule
