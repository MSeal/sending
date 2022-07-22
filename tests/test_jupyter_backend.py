import asyncio

import pytest
from jupyter_client import manager

from sending.backends.jupyter import JupyterKernelManager


@pytest.fixture(scope="session")
def ipykernel():
    yield manager.start_new_kernel()


@pytest.mark.jupyter
class TestJupyterBackend:
    async def test_jupyter_backend(self, mocker, ipykernel):
        async def predicate(message):
            return message.topic == "iopub"

        cb = mocker.MagicMock()
        mgr = JupyterKernelManager(ipykernel[1].get_connection_info())
        await mgr.initialize()
        mgr.register_callback(cb, predicate)

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
