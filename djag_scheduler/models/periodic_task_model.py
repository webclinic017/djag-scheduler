"""Periodic-Task model"""

import json

from django.db import models
from django.db.models import signals
from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator
from django.utils.timezone import now

from djag_scheduler import managers
from .crontab_schedule_model import CrontabSchedule
from .user_action_model import UserAction
import djag_scheduler.models.user_action_model as action_choices


class PeriodicTask(models.Model):
    """Model representing a periodic task."""

    name = models.CharField(
        max_length=200, unique=True,
        verbose_name='Name',
        help_text='Short Description For This Task'
    )
    task = models.CharField(
        max_length=200, unique=True,
        verbose_name='Task Name',
        help_text='The Name of the Celery Task that Should be Run.  '
                  '(Example: "proj.tasks.import_contacts")'
    )

    crontab = models.ForeignKey(
        CrontabSchedule, on_delete=models.CASCADE, verbose_name='Crontab Schedule',
        help_text='Crontab Schedule to run the task on.'
    )
    cron_base = models.DateTimeField(
        blank=True, default=now, verbose_name='Cron Base',
        help_text='Cron base from which tasks are run (defaults to create time if blank)'
    )

    args = models.JSONField(
        blank=True, default=list,
        verbose_name='Positional Arguments',
        help_text='JSON encoded positional arguments (Example: ["arg1", "arg2"])'
    )
    kwargs = models.JSONField(
        blank=True, default=dict,
        verbose_name='Keyword Arguments',
        help_text='JSON encoded keyword arguments (Example: {"argument": "value"})'
    )

    queue = models.CharField(
        max_length=200, blank=True, null=True, default=None,
        verbose_name='Queue Override',
        help_text='Queue defined in CELERY_TASK_QUEUES. '
                  'Leave None for default queuing.'
    )

    # you can use low-level AMQP routing options here,
    # but you almost certainly want to leave these as None
    # http://docs.celeryproject.org/en/latest/userguide/routing.html#exchanges-queues-and-routing-keys
    exchange = models.CharField(
        max_length=200, blank=True, null=True, default=None,
        verbose_name='Exchange',
        help_text='Override Exchange for low-level AMQP routing'
    )
    routing_key = models.CharField(
        max_length=200, blank=True, null=True, default=None,
        verbose_name='Routing Key',
        help_text='Override Routing Key for low-level AMQP routing'
    )
    headers = models.JSONField(
        blank=True, default=dict,
        verbose_name='AMQP Message Headers',
        help_text='JSON encoded message headers for the AMQP message.'
    )

    priority = models.PositiveIntegerField(
        default=None, validators=[MaxValueValidator(255)],
        blank=True, null=True,
        verbose_name='Priority',
        help_text='Priority Number between 0 and 255. '
                  'Supported by: RabbitMQ, Redis (priority reversed, 0 is highest).'
    )

    enabled = models.BooleanField(
        default=True,
        verbose_name='Enable Task',
        help_text='Set to False to disable the schedule'
    )
    skip_misfire = models.BooleanField(
        default=False,
        verbose_name='Skip Misfires',
        help_text='Skip all misfire events'
    )
    coalesce_misfire = models.BooleanField(
        default=False,
        verbose_name='Coalesce Misfires',
        help_text='Coalesce all misfire events into one event'
    )
    grace_period = models.PositiveIntegerField(
        blank=True, null=True, default=None,
        verbose_name='Grace Period',
        help_text='Misfire grace period in seconds'
    )

    last_cron = models.DateTimeField(
        auto_now=False, auto_now_add=False,
        editable=False, blank=True, null=True,
        verbose_name='Last Ran Cron',
        help_text='The last cron djag-scheduler completed running'
    )
    last_cron_start = models.DateTimeField(
        auto_now=False, auto_now_add=False,
        editable=False, blank=True, null=True,
        default=None, verbose_name='Last Cron Start Time',
        help_text='Task\'s last cron start time'
    )
    last_cron_end = models.DateTimeField(
        auto_now=False, auto_now_add=False,
        editable=False, blank=True, null=True,
        default=None, verbose_name='Last Cron End Time',
        help_text='Task\'s last cron end time'
    )
    running = models.IntegerField(
        default=0, editable=False,
        verbose_name='Running Instances',
        help_text='Total running instances of the task at the moment'
    )
    exception_cron = models.DateTimeField(
        auto_now=False, auto_now_add=False,
        editable=True, blank=True, null=True,
        verbose_name='Exception Cron',
        help_text='The cron run which raised an exception'
    )

    total_run_count = models.PositiveIntegerField(
        default=0, editable=False,
        verbose_name='Total Run Count',
        help_text='Running count of how many times the schedule '
                  'has triggered the task'
    )
    date_changed = models.DateTimeField(
        auto_now=True,
        verbose_name='Last Modified',
        help_text='Datetime that this PeriodicTask was last modified'
    )
    description = models.TextField(
        blank=True,
        verbose_name='Description',
        help_text='Detailed description about the details of this Periodic Task'
    )

    objects = managers.PeriodicTaskManager()
    no_changes = False

    class Meta:
        """Table information."""

        verbose_name = 'periodic task'
        verbose_name_plural = 'periodic tasks'

    def clean(self):
        """Clean model data"""
        if not self.cron_base:
            self.cron_base = now()

        if self.skip_misfire and self.coalesce_misfire:
            raise ValidationError('Misfires cant be skipped and coalesced at once')

        if not isinstance(json.loads(json.dumps(self.args)), list):
            raise ValidationError({
                'args': ValidationError('"args" should be a JSON array')}
            )

        if (self.skip_misfire or self.coalesce_misfire) and not self.grace_period:
            raise ValidationError({
                'grace_period': ValidationError('Grace period must be set for skipping or coalescing misfires')
            })

    def save(self, *args, insert_task_change=True, update_fields=None, **kwargs):
        """Save model data"""
        self.full_clean()

        if insert_task_change:
            if self.pk is None:
                # Task is added for the first time
                self.__class__.insert_task_change(
                    self, [
                        f.name for f in self.__class__._meta.get_fields() if isinstance(f, models.Field)  # noqa
                    ]
                )
            else:
                old = self.__class__.objects.get(pk=self.pk)
                fields = []

                # Give preference to update_fields if set
                for field in (update_fields or [
                    f.name for f in self.__class__._meta.get_fields() if isinstance(f, models.Field)  # noqa
                ]):
                    try:
                        if getattr(self, field) != getattr(old, field):
                            fields.append(field)
                    except:  # noqa
                        pass

                if fields:
                    self.__class__.insert_task_change(self, fields)

        super().save(*args, update_fields=update_fields, **kwargs)

    def __str__(self):
        return self.task

    @classmethod
    def insert_task_change(cls, instance, fields=(), *args, **kwargs):
        """Insert Task Change record"""
        if not isinstance(instance, PeriodicTask):
            return

        user_action = UserAction(
            action=action_choices.TASK_CHANGED,
            payload=dict(
                task_id=instance.pk,
                task_name=instance.name,
                task=instance.task,
                status='changed',
                fields=fields
            )
        )
        user_action.save()

    @classmethod
    def insert_task_delete(cls, instance, *args, **kwargs):
        """Insert Task Change record"""
        if not isinstance(instance, PeriodicTask):
            return

        user_action = UserAction(
            action=action_choices.TASK_CHANGED,
            payload=dict(
                task_id=instance.pk,
                task_name=instance.name,
                task=instance.task,
                status='deleted',
                fields=()
            )
        )
        user_action.save()


signals.post_delete.connect(PeriodicTask.insert_task_delete, sender=PeriodicTask)
