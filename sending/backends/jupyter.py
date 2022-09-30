import asyncio
import collections
from typing import List, Optional, Union, Dict

from jupyter_client import AsyncKernelClient
from jupyter_client.channels import ZMQSocketChannel
import zmq

from zmq.asyncio import Context
from zmq.utils.monitor import recv_monitor_message

from ..base import AbstractPubSubManager, QueuedMessage, SystemEvents
from ..logging import logger


class JupyterKernelManager(AbstractPubSubManager):
    def __init__(
        self,
        connection_info: dict,
        *,
        max_message_size: int = None,
    ):
        super().__init__()
        self.connection_info = connection_info
        self.max_message_size = max_message_size
        self.channel_tasks: Dict[str, List[asyncio.Task]] = collections.defaultdict(list)

    async def initialize(
        self, *, queue_size=0, inbound_workers=1, outbound_workers=1, poll_workers=1
    ):
        logger.debug(
            f"Initializing Jupyter Kernel Manager: {zmq.zmq_version()=}, {zmq.pyzmq_version()=}"
        )
        self._context = Context()
        if self.max_message_size:
            self.set_context_option(zmq.SocketOption.MAXMSGSIZE, self.max_message_size)

        self._client = AsyncKernelClient(context=self._context)
        self._client.load_connection_info(self.connection_info)

        return await super().initialize(
            queue_size=queue_size,
            inbound_workers=inbound_workers,
            outbound_workers=outbound_workers,
            poll_workers=poll_workers,
        )

    async def shutdown(self, now=False):
        for topic_name, task_list in self.channel_tasks.items():
            for task in task_list:
                task.cancel()
        await super().shutdown(now)
        # https://github.com/zeromq/pyzmq/issues/1003
        self._context.destroy(linger=0)

    def set_context_option(self, option: int, val: Union[int, bytes]):
        self._context.setsockopt(option, val)

    async def watch_for_channel_messages(self, topic_name: str, channel_obj: ZMQSocketChannel):
        while True:
            msg: dict = await channel_obj.get_msg()
            print(msg)
            self.schedule_for_delivery(topic_name, msg)

    async def watch_for_disconnect(self, monitor_socket: zmq.Socket):
        while True:
            msg: dict = await recv_monitor_message(monitor_socket)
            event: zmq.Event = msg["event"]
            if event == zmq.EVENT_DISCONNECTED:
                return

    async def watch_channel(self, topic_name: str):
        channel_name = f"{topic_name}_channel"
        while True:
            # The channel properties (e.g. self._client.iopub_channel) will connect the socket
            # if self._client._iopub_channel is None.
            channel_obj: ZMQSocketChannel = getattr(self._client, channel_name)
            monitor_socket = channel_obj.socket.get_monitor_socket()
            monitor_task = asyncio.create_task(self.watch_for_disconnect(monitor_socket))
            message_task = asyncio.create_task(
                self.watch_for_channel_messages(topic_name, channel_obj)
            )

            # add tasks to self.channel_tasks so we can cleanup during topic unsubscribe / shutdown
            self.channel_tasks[topic_name].append(monitor_task)
            self.channel_tasks[topic_name].append(message_task)

            # Run the monitor and message tasks. Message task should run forever.
            # If the monitor task returns then it means the socket was disconnected
            # (max message size) and we need to cycle it.
            done, pending = await asyncio.wait(
                [monitor_task, message_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            for task in done:
                if task.exception():
                    raise task.exception()

            self.channel_tasks[topic_name].remove(monitor_task)
            self.channel_tasks[topic_name].remove(message_task)
            logger.info(f"Cycling topic {topic_name} after disconnect")
            print(f"Cycling topic {topic_name} after disconnect")
            self._emit_system_event(topic_name, SystemEvents.FORCED_DISCONNECT)
            channel_obj.close()
            setattr(self._client, f"_{channel_name}", None)

    async def _create_topic_subscription(self, topic_name: str):
        task = asyncio.create_task(self.watch_channel(topic_name))
        self.channel_tasks[topic_name].append(task)

    async def _cleanup_topic_subscription(self, topic_name: str):
        print(f"Cleaning up topic {topic_name}")
        if topic_name in self.channel_tasks:
            for task in self.channel_tasks[topic_name]:
                task.cancel()
                self.channel_tasks[topic_name].remove(task)
            await asyncio.sleep(0)
            # Reset the channel object on our jupyter_client
            setattr(self._client, f"_{topic_name}_channel", None)
        else:
            logger.warning(
                f"Got a call to cleanup topic {topic_name} but it wasn't in the channel_tasks dict"
            )

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

    async def _poll(self):
        pass

    async def _poll_loop(self):
        pass
