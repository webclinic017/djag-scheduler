"""User Action Audit Model"""

from django.db import models
from django.db.models import signals

from .user_action_model import BaseUserAction, UserAction


class UserActionAudit(BaseUserAction):
    """Model representing user actions audit"""

    delete_dt = models.DateTimeField(
        auto_now_add=True,
        verbose_name='Deletion DateTime'
    )

    class Meta:
        """Table information"""

        verbose_name = 'User Action Audit'
        verbose_name_plural = 'User Action Audits'

    @classmethod
    def audit_user_action(cls, sender, instance, using, *args, **kwargs):
        """Save user-action to user-action-audit"""
        if not isinstance(instance, UserAction):
            return

        audit_record = UserActionAudit(
            action=instance.action,
            payload=instance.payload,
            create_dt=instance.create_dt
        )
        audit_record.save()


signals.post_delete.connect(UserActionAudit.audit_user_action, sender=UserAction)
