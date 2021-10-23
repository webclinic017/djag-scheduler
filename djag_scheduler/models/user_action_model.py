"""User Actions Model"""
from django.db import models

# Internal Actions
TASK_CHANGED = -1
SCHEDULE_CHANGED = -2
DEPENDENCY_CHANGED = -3

# Public Actions
UNCLASSIFIED_ACTION = 1

# Action Choices
INTERNAL_ACTION_CHOICES = (
    (TASK_CHANGED, 'Task Changed'),
    (SCHEDULE_CHANGED, 'Schedule Changed'),
    (DEPENDENCY_CHANGED, 'Dependency Changed')
)

PUBLIC_ACTION_CHOICES = (
    (UNCLASSIFIED_ACTION, 'Unclassified Action'),
)

ACTION_CHOICES = INTERNAL_ACTION_CHOICES + PUBLIC_ACTION_CHOICES


class BaseUserAction(models.Model):
    """Base Model representing user actions"""

    action = models.IntegerField(
        choices=ACTION_CHOICES,
        verbose_name='User Action',
        help_text='Action to be performed'
    )
    payload = models.JSONField(
        default=dict,
        verbose_name='Action Payload',
        help_text='Action\'s payload data'
    )
    create_dt = models.DateTimeField(
        auto_now_add=True,
        verbose_name='Creation DateTime'
    )

    class Meta:
        abstract = True


class UserAction(BaseUserAction):
    """Model representing user actions"""

    class Meta:
        verbose_name = 'User Action'
        verbose_name_plural = 'User Actions'
