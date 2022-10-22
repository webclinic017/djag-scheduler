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
    def get(cls, block=True, timeout=None, ack_msg=True):
        try:
            event = cls.queue.get(block=block, timeout=timeout)
            if ack_msg:
                event.ack()

            return event
        except Exception as e:
            raise e

    @classmethod
    def put(cls, message, **kwargs):
        cls.queue.put(message=message, **kwargs)

    @classmethod
    def close(cls):
        cls.queue.close()
