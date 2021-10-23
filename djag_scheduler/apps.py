from django.apps import AppConfig

__all__ = ['DjagSchedulerConfig']


class DjagSchedulerConfig(AppConfig):
    """Default configuration for Djag-Scheduler app."""

    name = 'djag_scheduler'
    label = 'djag_scheduler'
    verbose_name = 'Djag Scheduler'
    default_auto_field = 'django.db.models.AutoField'

