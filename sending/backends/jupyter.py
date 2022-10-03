import asyncio
import collections
from typing import Dict, List, Optional, Union

import jupyter_client.session
import zmq
from jupyter_client import AsyncKernelClient
from jupyter_client.channels import ZMQSocketChannel
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
        # If max_message_size is set, we'll disconnect (and reconnect immediately) to zmq
        # channels that try to send a message greater than that size. It prevents applications
        # from OOM crashing reading in large outputs or other messages
        self.max_message_size = max_message_size
        # Tasks that ultiamtely watch the zmq channels for messages. Keep track of these
        # for cleanup (unsubscribe_from_topic, shutdown)
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
        # Cancelling channel watching tasks here is equivalent to shutting down poll worker
        # in other Sending backend implementations.
        for topic_name, task_list in self.channel_tasks.items():
            for task in task_list:
                task.cancel()
        await super().shutdown(now)
        # https://github.com/zeromq/pyzmq/issues/1003
        self._context.destroy(linger=0)

    def set_context_option(self, option: int, val: Union[int, bytes]):
        self._context.setsockopt(option, val)

    def send(
        self,
        topic_name: str,
        msg_type: str,
        content: Optional[dict],
        parent: Optional[dict] = None,
        header: Optional[dict] = None,
        metadata: Optional[dict] = None,
    ):
        """
        Put a message onto the outbound queue which will be picked up by the outbound
        worker and sent over zmq to the Kernel. Most messages will get sent over the shell
        channel, although some may go over control as wel.

        Example:
        mgr.send("shell", "execute_request", {"code": "print('hello')", "silent": False})
        """
        # format the message into a Jupyter specced dictionary then drop into outbound queue
        # to get sent over the wire when outbound worker calls ._publish
        jupyter_session: jupyter_client.session.Session = self._client.session
        jupyter_msg: dict = jupyter_session.msg(msg_type, content, parent, header, metadata)
        self.outbound_queue.put_nowait(QueuedMessage(topic_name, jupyter_msg, None))

    async def _publish(self, message: QueuedMessage):
        """
        When the outbound worker observes a message on the outbound queue, it will call this
        method to actually send the message over the wire.
        """
        topic_name = message.topic
        if topic_name not in self.subscribed_topics:
            await self._create_topic_subscription(topic_name)
        if hasattr(self._client, f"{topic_name}_channel"):
            channel_obj: ZMQSocketChannel = getattr(self._client, f"{topic_name}_channel")
            channel_obj.send(message.contents)

    # Normally in Sending backends there is the concept of a poll_worker which calls into _poll
    # as part of a custom _poll_loop implementation. The poll_worker is what reads data over the
    # wire (redis, socket, websocket, etc. zmq in the case of Jupyter/ipykernel). However the way
    # this backend is written, reading data from zmq after subscribe_to_topic is called is handled
    # by _watch_channel task (and its child tasks). poll_worker and these _poll methods do nothing.
    async def _poll(self):
        pass

    async def _poll_loop(self):
        pass

    async def _create_topic_subscription(self, topic_name: str):
        """
        Start observing messages on a zmq channel after a call to mgr.subscribe_to_topic('iopub')
        """
        task = asyncio.create_task(self._watch_channel(topic_name))
        self.channel_tasks[topic_name].append(task)

    async def _cleanup_topic_subscription(self, topic_name: str):
        """
        Clean up channel observing tasks after a call to mgr.unsubscribe_from_topic('iopub')
        """
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

    async def _watch_channel(self, topic_name: str):
        """
        When a user subscribes to a topic (mgr.subscribe_to_topic('iopub')), this function starts
        two child tasks:
          1. Pull messages off the zmq channel and trigger any registered callbacks
          2. Watch the monitor socket for disconnect events and reconnect / restart tasks

        If a disconnect is observed, the two tasks are both cancelled and restarted.
        Unsubscribing from a topic cancels this task and the child tasks.
        """
        channel_name = f"{topic_name}_channel"

        # context_hook (primarily for adding structlog contextvars) is normally called in base
        # _poll_loop, so that it's applied to every read the poll worker does. For the Jupyter
        # implementation, we don't use _poll_loop, all reads from zmq start with tasks here,
        # and any contextvars set in this method will be picked up by tasks created here.
        if self.context_hook:
            await self.context_hook()
        while True:
            # The channel properties (e.g. self._client.iopub_channel) will connect the socket
            # if self._client._iopub_channel is None. Channel objects have a monitor object
            # to observe lifecycle of the socket such as handshake / disconnect
            channel_obj: ZMQSocketChannel = getattr(self._client, channel_name)
            message_task = asyncio.create_task(
                self._watch_for_channel_messages(topic_name, channel_obj)
            )

            monitor_socket = channel_obj.socket.get_monitor_socket()
            monitor_task = asyncio.create_task(self._watch_for_disconnect(monitor_socket))

            # If the _watch_channel task gets cancelled from a .unsubscribe_from_topic call,
            # the two child tasks won't automatically be cancelled. Store these up at the class
            # level so that _cleanup_topic_subscription can cancel them.
            self.channel_tasks[topic_name].append(monitor_task)
            self.channel_tasks[topic_name].append(message_task)

            # Await the monitor and message tasks. Message task should run forever.
            # If the monitor task returns then it means the socket was disconnected,
            # presumably from receiving a message larger than max message size.
            done, pending = await asyncio.wait(
                [monitor_task, message_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            for task in done:
                if task.exception():
                    raise task.exception()

            logger.info(f"Cycling topic {topic_name} after disconnect")
            self.channel_tasks[topic_name].remove(monitor_task)
            self.channel_tasks[topic_name].remove(message_task)

            # Emit an event so that callbacks registered to pickup the disconnect can do things like
            # send user-facing messages that an output stream was too big and won't be displayed
            self._emit_system_event(topic_name, SystemEvents.FORCED_DISCONNECT)
            channel_obj.close()

            # Setting jupyter_client._iopub_channel to None will cause the next reference to
            # the jupyter_client.iopub_channel @property to reconnect the socket.
            # (see top of this while loop!)
            setattr(self._client, f"_{channel_name}", None)

    async def _watch_for_channel_messages(self, topic_name: str, channel_obj: ZMQSocketChannel):
        """
        Read in any messages on a specific jupyter_client channel and drop them into the inbound
        worker queue which will trigger registered callback functions by predicate / topic
        """
        while True:
            msg: dict = await channel_obj.get_msg()
            self.schedule_for_delivery(topic_name, msg)

    async def _watch_for_disconnect(self, monitor_socket: zmq.Socket):
        """
        An awaitable task that ends when a particular socket has a disconnect event. Used in
        conjunction with watch_for_channel_messages to cycle a socket when it's disconnected.
        """
        while True:
            msg: dict = await recv_monitor_message(monitor_socket)
            event: zmq.Event = msg["event"]
            if event == zmq.EVENT_DISCONNECTED:
                return
