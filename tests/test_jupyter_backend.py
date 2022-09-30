import asyncio

import pytest
from jupyter_client import manager

from sending.backends.jupyter import JupyterKernelManager
from sending.base import SystemEvents
import time
from typing import List


@pytest.fixture()
def ipykernel():
    km, kc = manager.start_new_kernel()
    yield kc.get_connection_info()
    kc.stop_channels()
    km.shutdown_kernel()


class JupyterMonitor:
    """
    This can help progress an asyncio loop forward until certain messages have been seen
    coming across the ZMQ socket, even across different channels.
    """

    def __init__(self, mgr: JupyterKernelManager):
        self.next_event = asyncio.Event()
        self.last_seen_event: dict = None
        mgr.register_callback(self.record_event)

    async def record_event(self, event: dict):
        self.last_seen_event = event
        self.next_event.set()

    async def run_until_seen(self, msg_types: List[str], timeout: float):
        """
        await multiple `asyncio.wait_for`'s until we've observed all msg_types we expect
        or an overall timeout has been reached. msg_types can contain dupes e.g. wait
        to see 'status', 'execute_reply', and 'status' again.
        """
        deadline = time.time() + timeout
        while msg_types:
            max_wait = deadline - time.time()
            await asyncio.wait_for(self.next_event.wait(), timeout=max_wait)
            if self.last_seen_event["msg_type"] in msg_types:
                msg_types.remove(self.last_seen_event["msg_type"])
            self.next_event.clear()


class TestJupyterBackend:
    async def test_jupyter_backend(self, mocker, ipykernel):
        """
        Test basic execution in ipykernel. When we send an execute_request over to the Kernel,
        we should observe a series of replies come over the shell and iopub channels.
         - status (iopub) goes to busy
         - execute_input (iopub) says the cell is running
         - stream (iopub) has the cell output
         - execute_reply (shell) says the cell is complete
         - status (iopub) goes to idle
        We can't guarentee order of messages received between channels -- execute_reply could
        come before or after stream, and before or after the idle status.
        """
        iopub_cb = mocker.MagicMock()
        shell_cb = mocker.MagicMock()
        mgr = JupyterKernelManager(ipykernel)
        monitor = JupyterMonitor(mgr)
        await mgr.initialize()

        mgr.register_callback(iopub_cb, on_topic="iopub")
        mgr.register_callback(shell_cb, on_topic="shell")

        await mgr.subscribe_to_topic("iopub")
        await mgr.subscribe_to_topic("shell")

        # Execute a cell. Should expect 5 messages:
        # - status (iopub), kernel going to busy
        # - execute_input (iopub), cell is running
        # - stream (iopub), cell output
        # - execute_reply (shell), cell is complete
        # - status (iopub), kernel going to idle
        mgr.send("shell", "execute_request", {"code": "print('asdf')", "silent": False})
        await monitor.run_until_seen(
            msg_types=["status", "execute_input", "execute_reply", "stream", "status"],
            timeout=3,
        )

        # the callbacks on iopub and shell channel should have gotten one argument from
        # each message which is the jupyter message as a dict. Assert the order of
        # received messages over iopub, but we can't guarentee order of received messages
        # betwen shell / iopub channel (e.g. execute_reply could come before or after
        # the kernel status goes back to idle)
        iopub_msgs = iopub_cb.call_args_list
        assert len(iopub_msgs) == 4
        assert iopub_msgs[0].args[0]["msg_type"] == "status"
        assert iopub_msgs[0].args[0]["content"]["execution_state"] == "busy"
        assert iopub_msgs[1].args[0]["msg_type"] == "execute_input"
        assert iopub_msgs[2].args[0]["msg_type"] == "stream"
        assert iopub_msgs[3].args[0]["msg_type"] == "status"
        assert iopub_msgs[3].args[0]["content"]["execution_state"] == "idle"

        shell_msgs = shell_cb.call_args_list
        assert len(shell_msgs) == 1
        assert shell_msgs[0].args[0]["msg_type"] == "execute_reply"

        # If we reset the iopub callback list and unsubscribe from monitoring iopub,
        # then when we execute another request we'll only see the messages on shell not
        # anything on iopub
        iopub_cb.reset_mock()
        shell_cb.reset_mock()
        await mgr.unsubscribe_from_topic("iopub")
        mgr.send("shell", "execute_request", {"code": "print('asdf')", "silent": False})
        await monitor.run_until_seen(msg_types=["execute_reply"], timeout=3)
        await mgr.shutdown()
        iopub_cb.assert_not_called()
        shell_cb.assert_called_once()

    @pytest.mark.xfail(reason="Cycling sockets is buggy in the current implementation")
    async def test_reconnection(self, mocker, ipykernel):
        """
        Test that if a message over the zmq channel is too large, we won't receive it
        but we will still be able to send further execute requests without issue.
        """
        disconnect_event = mocker.MagicMock()

        mgr = JupyterKernelManager(ipykernel, max_message_size=5 * 1024)
        monitor = JupyterMonitor(mgr)

        await mgr.initialize()

        mgr.register_callback(disconnect_event, on_system_event=SystemEvents.FORCED_DISCONNECT)
        await mgr.subscribe_to_topic("iopub")
        await mgr.subscribe_to_topic("shell")

        # Execute a cell and run until we've seen the normal expected messages.
        mgr.send("shell", "execute_request", {"code": "print('foo')", "silent": False})
        await monitor.run_until_seen(
            msg_types=["status", "execute_input", "stream", "execute_reply", "status"],
            timeout=3,
        )

        # Execute a cell with output larger than our max message size. We should observe
        # status going to busy, execute_input, then a disconnect event where we would normally
        # see a stream. The iopub channel should cycle, and hopefully catch the status going
        # idle. We'll also see execute_reply on shell channel.
        mgr.send(
            "shell",
            "execute_request",
            {"code": "print('x' * 2**13)", "silent": False},
        )
        try:
            await monitor.run_until_seen(
                msg_types=["status", "execute_input", "execute_reply", "status"], timeout=3
            )
        except asyncio.TimeoutError:
            raise Exception("Did not see the expected messages after cycling the iopub channel")
        finally:
            await mgr.shutdown()
        disconnect_event.assert_called()

        # Prove that after cycling the socket, normal executions work the same as always
        mgr.send("shell", "execute_request", {"code": "print('baz')", "silent": False})
        await monitor.run_until_seen(
            msg_types=["status", "execute_input", "stream", "execute_reply", "status"],
            timeout=3,
        )
        await mgr.shutdown()
