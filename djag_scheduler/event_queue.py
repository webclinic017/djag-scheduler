"""Djag Event Queue"""

from django.conf import settings
from kombu import Connection


class DjagEventQueue:
    """Djag Event Queue"""

    queue_name = 'DJAG_EVENT_QUEUE-8763051701'
    conn = Connection(
        **getattr(settings, 'DJAG_KOMBU_CONN_ARGS', dict())
    )
    queue = conn.SimpleQueue(queue_name)

    @classmethod
    def get(cls, block=True, timeout=None):
        return cls.queue.get(block=block, timeout=timeout)

    @classmethod
    def put(cls, message, **kwargs):
        cls.queue.put(message=message, **kwargs)

    @classmethod
    def close(cls):
        cls.queue.close()
