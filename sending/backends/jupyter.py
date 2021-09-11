from queue import Empty
from typing import Any, Dict, Optional

from jupyter_client.asynchronous.client import AsyncKernelClient

from ..base import AbstractPubSubManager, QueuedMessage


class JupyterKernelPubSubManager(AbstractPubSubManager):
    def __init__(self, client=None):
        if client is None:
            self.client = AsyncKernelClient()
        else:
            self.client = client

        self._channels_to_poll = {}

        super().__init__()

    async def _create_topic_subscription(self, topic_name: str):
        channel_attr_name = f"{topic_name}_channel"
        if not hasattr(channel_attr_name, self.client):
            raise ValueError("Unknown kernel channel requested")

        channel_attr = getattr(channel_attr_name)
        channel_attr.start()
        self._channels_to_poll[topic_name] = channel_attr

    async def _cleanup_topic_subscription(self, topic_name: str):
        channel_attr_name = f"{topic_name}_channel"
        channel_attr = getattr(channel_attr_name)
        if channel_attr.is_alive():
            channel_attr.stop()
            del self._channels_to_poll[topic_name]

    async def _publish(self, message: QueuedMessage):
        channel = self._channels_to_poll[message.topic]
        channel.send(message.contents)

    async def _poll(self):
        for topic, channel in self._channels_to_poll.items():
            try:
                msg = await channel.get_msg()
                self.schedule_for_delivery(topic, msg)
            except Empty:
                pass

    def message(
        self,
        msg_type: str,
        content: Optional[Dict] = None,
        parent: Optional[Dict[str, Any]] = None,
        header: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        return self.client.session.msg(msg_type, content, parent, header, metadata)
