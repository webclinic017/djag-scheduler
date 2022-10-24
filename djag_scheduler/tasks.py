"""Djag scheduler tasks/connectors"""

from celery.signals import task_postrun

from djag_scheduler.event_queue import DjagEventQueue


@task_postrun.connect
def task_executed(task_id, state, *args, **kwargs):
    """Send task post execution event"""
    DjagEventQueue.put({
        'event': 'TASK_EXECUTED',
        'task_id': task_id,
        'state': state
    })
