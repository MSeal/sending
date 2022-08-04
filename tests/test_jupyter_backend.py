import asyncio
import os

import pytest
from jupyter_client import manager

from sending.backends.jupyter import JupyterKernelManager
from sending.base import SystemEvents


@pytest.fixture()
def ipykernel():
    km, kc = manager.start_new_kernel()
    yield kc.get_connection_info()
    kc.stop_channels()
    km.shutdown_kernel()


class TestJupyterBackend:
    async def test_jupyter_backend(self, mocker, ipykernel):
        cb = mocker.MagicMock()
        mgr = JupyterKernelManager(ipykernel)
        await mgr.initialize()
        mgr.register_callback(cb, on_topic="iopub")

        await mgr.subscribe_to_topic("iopub")
        mgr.send("shell", "execute_request", {"code": "print('asdf')", "silent": False})
        await asyncio.sleep(1)
        await mgr._drain_queues()

        # Quick sanity test for message ordering
        iopub_msgs = cb.call_args_list
        assert len(iopub_msgs) == 4
        assert iopub_msgs[0].args[0]["msg_type"] == "status"
        assert iopub_msgs[0].args[0]["content"]["execution_state"] == "busy"
        assert iopub_msgs[1].args[0]["msg_type"] == "execute_input"
        assert iopub_msgs[2].args[0]["msg_type"] == "stream"
        assert iopub_msgs[3].args[0]["msg_type"] == "status"
        assert iopub_msgs[3].args[0]["content"]["execution_state"] == "idle"

        cb.reset_mock()
        await mgr.unsubscribe_from_topic("iopub")
        mgr.send("shell", "execute_request", {"code": "print('asdf')", "silent": False})
        await asyncio.sleep(1)
        await mgr.shutdown()
        cb.assert_not_called()

    async def test_reconnection(self, mocker, ipykernel):
        cb = mocker.MagicMock()
        system_event_cb = mocker.MagicMock()
        mgr = JupyterKernelManager(ipykernel, max_message_size=1024)
        await mgr.initialize()
        mgr.register_callback(cb, on_topic="iopub")
        mgr.register_callback(system_event_cb, on_system_event=SystemEvents.FORCED_DISCONNECT)
        await mgr.subscribe_to_topic("iopub")

        mgr.send("shell", "execute_request", {"code": "print('asdf')", "silent": False})
        await asyncio.sleep(1)
        await mgr._drain_queues()
        cb.assert_called()
        system_event_cb.assert_not_called()

        cb.reset_mock()
        mgr.send(
            "shell", "execute_request", {"code": f"print('{os.urandom(2048)}')", "silent": False}
        )
        await asyncio.sleep(1)
        await mgr._drain_queues()
        cb.assert_called()
        system_event_cb.assert_called()

        cb.reset_mock()
        system_event_cb.reset_mock()
        mgr.send("shell", "execute_request", {"code": "print('asdf')", "silent": False})
        await asyncio.sleep(1)
        await mgr.shutdown()
        cb.assert_called()
        system_event_cb.assert_not_called()

    async def test_force_close(self, mocker):
        remote_disconnect_db = mocker.MagicMock()
        forced_disconnect_db = mocker.MagicMock()
        km, kc = manager.start_new_kernel()
        mgr = JupyterKernelManager(kc.get_connection_info())
        await mgr.initialize()
        await mgr.subscribe_to_topic("shell")
        mgr.register_callback(forced_disconnect_db, on_system_event=SystemEvents.FORCED_DISCONNECT)
        mgr.register_callback(remote_disconnect_db, on_system_event=SystemEvents.REMOTE_DISCONNECT)
        kc.stop_channels()
        km.shutdown_kernel()
        await mgr.shutdown()
        remote_disconnect_db.assert_called()
        forced_disconnect_db.assert_not_called()
