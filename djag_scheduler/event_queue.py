"""Djag Event Queue"""

from django.conf import settings
from kombu import Connection
from kombu.simple import SimpleQueue


class DjagEventQueue:
    """Djag Event Queue"""

    Empty = SimpleQueue.Empty
    DEFAULT_QUEUE_NAME = 'DJAG_EVENT_QUEUE-8763051701'

    conn = Connection(
        **(getattr(settings, 'DJAG_KOMBU_CONN_ARGS', None) or {})
    )
    queue = conn.SimpleQueue(
        getattr(settings, 'DJAG_EVENT_QUEUE_NAME', None) or DEFAULT_QUEUE_NAME
    )

    @classmethod
    def get(cls, block=True, timeout=None, ack_msg=True):
        """Get event, if requested acknowledge as well"""
        event = cls.queue.get(block=block, timeout=timeout)
        if ack_msg:
            event.ack()

        return event

    @classmethod
    def put(cls, message, **kwargs):
        cls.queue.put(message=message, **kwargs)

    @classmethod
    def close(cls):
        cls.queue.close()
