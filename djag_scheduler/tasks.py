"""Djag scheduler tasks/connectors"""

from django.core.cache import caches
from django.core.cache.backends.base import InvalidCacheBackendError

from celery.signals import task_postrun


@task_postrun.connect
def task_executed(task_id, *args, **kwargs):
    """Set executed task ids in django cache"""
    try:
        task_cache = caches['djag_scheduler']
    except InvalidCacheBackendError:
        return

    task_cache.set(task_id, 'executed', None)
