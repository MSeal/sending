from queue import Empty
from typing import Optional, Union

from jupyter_client import AsyncKernelClient
from zmq import SocketOption

from ..base import AbstractPubSubManager, QueuedMessage


class JupyterKernelManager(AbstractPubSubManager):
    def __init__(self, connection_info: dict, *, max_message_size: int = None):
        super().__init__()
        self._client = AsyncKernelClient()
        self._client.load_connection_info(connection_info)

        # TODO(nick): automatically subscribe to monitor socket and figure out when we're
        # disconnected so we can unsub/resub
        if max_message_size:
            self.set_context_option(SocketOption.MAXMSGSIZE, max_message_size)

    def set_context_option(self, option: int, val: Union[int, bytes]):
        self._client.context.setsockopt(option, val)

    async def _create_topic_subscription(self, topic_name: str):
        if not hasattr(self._client, f"{topic_name}_channel"):
            pass

        channel_obj = getattr(self._client, f"{topic_name}_channel")
        channel_obj.start()

    async def _cleanup_topic_subscription(self, topic_name: str):
        if not hasattr(self._client, f"{topic_name}_channel"):
            pass

        channel_obj = getattr(self._client, f"{topic_name}_channel")
        channel_obj.stop()

        # Reset the underlying channel object so jupyter_client will recreate it
        # if we subscribe to this again.
        setattr(self._client, f"_{topic_name}_channel", None)

    def send(
        self,
        topic_name: str,
        msg_type: str,
        content: Optional[dict],
        parent: Optional[dict] = None,
        header: Optional[dict] = None,
        metadata: Optional[dict] = None,
    ):
        msg = self._client.session.msg(msg_type, content, parent, header, metadata)
        self.outbound_queue.put_nowait(QueuedMessage(topic_name, msg, None))

    async def _publish(self, message: QueuedMessage):
        topic_name = message.topic
        if not hasattr(self._client, f"{topic_name}_channel"):
            pass

        channel_obj = getattr(self._client, f"{topic_name}_channel")
        channel_obj.start()
        channel_obj.send(message.contents)

    async def _poll(self):
        for topic_name in self.subscribed_topics:
            channel_obj = getattr(self._client, f"{topic_name}_channel")

            while True:
                try:
                    msg = await channel_obj.get_msg(timeout=0)
                    self.schedule_for_delivery(topic_name, msg)
                except Empty:
                    break
