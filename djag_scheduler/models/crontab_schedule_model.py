"""Crontab schedule model"""
from zoneinfo import available_timezones

from django.db import models
from django.db.models import signals
from django.conf import settings
from django.core.exceptions import ValidationError

from celery import current_app
import timezone_field
from croniter import croniter

from .user_action_model import UserAction
import djag_scheduler.models.user_action_model as action_choices


def default_timezone():
    """Return timezone string from Django settings `CELERY_TIMEZONE` variable.
    If is not defined or is not a valid timezone, return `"UTC"` instead.
    """
    try:
        celery_timezone = getattr(
            settings, '{0}_TIMEZONE'.format(current_app.namespace)
        )
        if celery_timezone in available_timezones():
            return celery_timezone
        else:
            return 'UTC'
    except AttributeError:
        return 'UTC'


class CrontabSchedule(models.Model):
    """Timezone Aware Crontab-like schedule.
    Example:  Run every hour at 0 minutes for days of month 10-15
    minute="0", hour="*", day_of_week="*",
    day_of_month="10-15", month_of_year="*"
    """

    #
    # The worst case scenario for day of month is a list of all 31 day numbers
    # '[1, 2, ..., 31]' which has a length of 115. Likewise, minute can be
    # 0..59 and hour can be 0..23. Ensure we can accommodate these by allowing
    # 4 chars for each value (what we save on 0-9 accommodates the []).
    # We leave the other fields at their historical length.
    #
    minute = models.CharField(
        max_length=60 * 4, default='*',
        verbose_name='Minute(s)',
        help_text='Cron Minutes to Run. Use "*" for "all". (Example: "0,30")'
    )
    hour = models.CharField(
        max_length=24 * 4, default='*',
        verbose_name='Hour(s)',
        help_text='Cron Hours to Run. Use "*" for "all". (Example: "8,20")'
    )
    day_of_month = models.CharField(
        max_length=31 * 4, default='*',
        verbose_name='Day(s) Of The Month',
        help_text='Cron Days Of The Month to Run. Use "*" for "all". '
                  '(Example: "1,15")'
    )
    month_of_year = models.CharField(
        max_length=64, default='*',
        verbose_name='Month(s) Of The Year',
        help_text='Cron Months Of The Year to Run. Use "*" for "all". '
                  '(Example: "0,6")'
    )
    day_of_week = models.CharField(
        max_length=64, default='*',
        verbose_name='Day(s) Of The Week',
        help_text='Cron Days Of The Week to Run. Use "*" for "all". '
                  '(Example: "0,5")'
    )

    timezone = timezone_field.TimeZoneField(
        default=default_timezone,
        use_pytz=False,
        verbose_name='Cron Timezone',
        help_text='Timezone to Run the Cron Schedule on. Default is UTC.',
    )

    class Meta:
        """Table information."""

        verbose_name = 'crontab'
        verbose_name_plural = 'crontabs'
        ordering = ['month_of_year', 'day_of_month',
                    'day_of_week', 'hour', 'minute', 'timezone']

    def clean(self):
        """Clean model data"""
        if not croniter.is_valid(self.crontab):
            raise ValidationError('CronIter: Crontab is not valid')

    def save(self, *args, update_fields=None, **kwargs):
        """Save model data"""
        self.full_clean()

        if self.pk is None:
            # Crontab schedule is added for the first time
            self.__class__.insert_schedule_change(self)
        else:
            old = self.__class__.objects.get(pk=self.pk)

            # Give preference to update_fields if set
            for field in (update_fields or [
                f.name for f in self.__class__._meta.get_fields() if isinstance(f, models.Field)  # noqa
            ]):
                try:
                    if getattr(self, field) != getattr(old, field):
                        from .periodic_task_model import PeriodicTask

                        self.__class__.insert_schedule_change(self)

                        # Get periodic tasks which uses this cron
                        for task in PeriodicTask.objects.filter(crontab__pk=self.pk):
                            task.__class__.insert_task_change(task, ('crontab',))

                        break
                except:  # noqa
                    pass

        super().save(*args, update_fields=update_fields, **kwargs)

    def __str__(self):
        return self.crontab + ' ' + str(self.timezone)

    @property
    def crontab(self):
        return ' '.join([  # noqa
            self.minute, self.hour, self.day_of_month,
            self.month_of_year, self.day_of_week
        ])

    @classmethod
    def insert_schedule_change(cls, instance, *args, **kwargs):
        """Insert Task Deleted record into UserAction"""
        if not isinstance(instance, CrontabSchedule):
            return

        user_action = UserAction(
            action=action_choices.SCHEDULE_CHANGED,
            payload=dict(
                schedule_id=instance.pk,
                schedule=str(instance)
            )
        )
        user_action.save()


signals.post_delete.connect(CrontabSchedule.insert_schedule_change, sender=CrontabSchedule)
