import asyncio
from queue import Empty
from typing import Optional, Union

from jupyter_client import AsyncKernelClient
from jupyter_client.channels import ZMQSocketChannel
from zmq import NOBLOCK, Event, Socket, SocketOption, pyzmq_version, zmq_version
from zmq.asyncio import Context
from zmq.utils.monitor import recv_monitor_message

from ..base import AbstractPubSubManager, QueuedMessage, SystemEvents
from ..logging import logger


class JupyterKernelManager(AbstractPubSubManager):
    def __init__(self, connection_info: dict, *, max_message_size: int = None):
        super().__init__()
        self.connection_info = connection_info
        self._monitor_sockets_for_topic: dict[str, Socket] = {}
        self.max_message_size = max_message_size

    async def initialize(
        self, *, queue_size=0, inbound_workers=1, outbound_workers=1, poll_workers=1
    ):
        logger.debug(f"Initializing Jupyter Kernel Manager: {zmq_version()=}, {pyzmq_version()=}")
        self._context = Context()
        if self.max_message_size:
            self.set_context_option(SocketOption.MAXMSGSIZE, self.max_message_size)

        self._client = AsyncKernelClient(context=self._context)
        self._client.load_connection_info(self.connection_info)

        return await super().initialize(
            queue_size=queue_size,
            inbound_workers=inbound_workers,
            outbound_workers=outbound_workers,
            poll_workers=poll_workers,
        )

    async def shutdown(self, now=False):
        await super().shutdown(now)
        # https://github.com/zeromq/pyzmq/issues/1003
        self._context.destroy(linger=0)

    def set_context_option(self, option: int, val: Union[int, bytes]):
        self._context.setsockopt(option, val)

    async def _create_topic_subscription(self, topic_name: str):
        if hasattr(self._client, f"{topic_name}_channel"):
            channel_obj = getattr(self._client, f"{topic_name}_channel")
            channel_obj.start()

            monitor_socket = channel_obj.socket.get_monitor_socket()
            self._monitor_sockets_for_topic[topic_name] = monitor_socket

    async def _cleanup_topic_subscription(self, topic_name: str):
        if hasattr(self._client, f"{topic_name}_channel"):
            channel_obj = getattr(self._client, f"{topic_name}_channel")
            channel_obj.socket.disable_monitor()
            channel_obj.close()

            # Reset the underlying channel object so jupyter_client will recreate it
            # if we subscribe to this again.
            setattr(self._client, f"_{topic_name}_channel", None)
            del self._monitor_sockets_for_topic[topic_name]

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
        if topic_name not in self.subscribed_topics:
            await self._create_topic_subscription(topic_name)
        if hasattr(self._client, f"{topic_name}_channel"):
            channel_obj = getattr(self._client, f"{topic_name}_channel")
            channel_obj.send(message.contents)

    def _cycle_socket(self, topic):
        channel_obj = getattr(self._client, f"{topic}_channel")
        channel_obj.socket.disable_monitor()
        channel_obj.close()
        connect_fn = getattr(self._client, f"connect_{topic}")
        channel_obj.socket = connect_fn()
        monitor_socket = channel_obj.socket.get_monitor_socket()
        self._monitor_sockets_for_topic[topic] = monitor_socket

    async def _poll(self):
        for topic_name in self.subscribed_topics:
            channel_obj: ZMQSocketChannel = getattr(self._client, f"{topic_name}_channel")

            while True:
                try:
                    msg = await channel_obj.get_msg(timeout=0)
                    self.schedule_for_delivery(topic_name, msg)
                except Empty:
                    break

        topics_to_cycle = []
        for topic, socket in self._monitor_sockets_for_topic.items():
            while await socket.poll(0):
                msg = await recv_monitor_message(socket, flags=NOBLOCK)
                logger.debug(f"ZMQ event: {topic=} {msg['event']=} {msg['value']=}")
                if msg["event"] == Event.DISCONNECTED:
                    self._emit_system_event(topic, SystemEvents.FORCED_DISCONNECT)
                    topics_to_cycle.append(topic)

        for topic in topics_to_cycle:
            # If the ZMQ socket is disconnected, try cycling it
            # This is helpful in situations where ZMQ disconnects peers
            # when it violates some constraint such as the max message size.
            logger.info(f"ZMQ disconnected for topic '{topic}', cycling socket")
            self._cycle_socket(topic)

    async def _poll_loop(self):
        while True:
            try:
                await self._poll()
            except Exception:
                logger.exception("Uncaught exception encountered while polling backend")
            finally:
                await asyncio.sleep(0.001)
