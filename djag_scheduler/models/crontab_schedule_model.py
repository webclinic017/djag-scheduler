"""Crontab schedule model"""
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
    except AttributeError:
        return 'UTC'
    return celery_timezone if celery_timezone in [
        choice[0].zone for choice in timezone_field.TimeZoneField.default_choices
    ] else 'UTC'


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
    day_of_week = models.CharField(
        max_length=64, default='*',
        verbose_name='Day(s) Of The Week',
        help_text='Cron Days Of The Week to Run. Use "*" for "all". '
                  '(Example: "0,5")'
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

    timezone = timezone_field.TimeZoneField(
        default=default_timezone,
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

    def save(self, *args, **kwargs):
        """Save model data"""
        self.full_clean()
        super().save(*args, **kwargs)

    def __str__(self):
        return self.crontab + " " + str(self.timezone)

    @property
    def crontab(self):
        return ' '.join([
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
                schedule_id=instance.id,
                schedule=str(instance)
            )
        )
        user_action.save()


signals.post_save.connect(CrontabSchedule.insert_schedule_change, sender=CrontabSchedule)
signals.post_delete.connect(CrontabSchedule.insert_schedule_change, sender=CrontabSchedule)
