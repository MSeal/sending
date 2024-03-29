import asyncio
import json
import uuid
from typing import Callable

import httpx
import pytest
import structlog
from managed_service_fixtures import AppDetails, AppManager

from sending.backends.websocket import WebsocketManager
from sending.base import Callback, QueuedMessage


@pytest.fixture(scope="session")
def websocket_server(managed_asgi_app_factory: Callable[[], AppManager]) -> AppDetails:
    """
    Starts the FastAPI app defined in tests/websocket_server.py as an external process,
    returns connection details. WS clients should connect to AppDetails.ws_base + '/ws'
    """
    app_location = "tests.websocket_server:app"
    with managed_asgi_app_factory(app_location) as app_details:
        yield app_details


@pytest.fixture
async def manager(websocket_server: AppDetails) -> WebsocketManager:
    """
    Return an instance of WebsocketManager set up to connect to the FastAPI app
    defined in tests/websocket_server.py. Tests can attach callbacks and hooks.
    Calling manager.initialize() starts the websocket connection.
    """
    manager = WebsocketManager(ws_url=websocket_server.ws_base + "/ws")
    manager.max_reconnections = 1
    yield manager
    if not manager._shutting_down:
        await manager.shutdown()


@pytest.fixture
async def json_manager(manager: WebsocketManager) -> WebsocketManager:
    """
    WebsocketManager that serializes/deserializes all messages to and from json
    """
    manager.inbound_message_hook = lambda msg: json.loads(msg)
    manager.outbound_message_hook = lambda msg: json.dumps(msg)
    return manager


async def run_until_message_type(manager: WebsocketManager, msg_type: str) -> dict:
    """
    Helper function for tests, iterate through the messages a manager receives
    until it sees a message with the given type.
    Times out if no messages come in for 1 full second.
    """
    while True:
        await asyncio.wait_for(manager.next_event.wait(), 1)
        if manager.last_seen_message["type"] == msg_type:
            break
    return manager.last_seen_message


async def test_basic_send(manager: WebsocketManager):
    """
    Test the most basic Websocket manager that sends and receives plain text.
    After the websocket is connected, send a message and then run until it has received
    the expected response
    """
    await manager.initialize()
    assert manager.auth_hook is None
    manager.send(json.dumps({"type": "unauthed_echo_request", "text": "Hello plain_manager"}))
    await asyncio.wait_for(manager.next_event.wait(), 1)
    reply = json.loads(manager.last_seen_message)
    assert reply == {"type": "unauthed_echo_reply", "text": "Hello plain_manager"}


async def test_message_hooks(json_manager: WebsocketManager):
    """
    Test that the inbound and outbound message hooks serialize/deserialize json
    """
    await json_manager.initialize()
    json_manager.send({"type": "unauthed_echo_request", "text": "Hello json_manager"})
    reply = await run_until_message_type(json_manager, "unauthed_echo_reply")
    assert reply == {"type": "unauthed_echo_reply", "text": "Hello json_manager"}


async def test_init_hook(json_manager: WebsocketManager):
    """
    Test that an init_hook is called immediately after websocket connection,
    """

    async def init_hook(mgr: WebsocketManager):
        mgr.send({"type": "unauthed_echo_request", "text": "Hello init_hook"})

    json_manager.init_hook = init_hook
    await json_manager.initialize()
    reply = await run_until_message_type(json_manager, "unauthed_echo_reply")
    assert reply == {"type": "unauthed_echo_reply", "text": "Hello init_hook"}


async def test_auth_hook(json_manager: WebsocketManager):
    """
    Test that an auth_hook is called immediately after websocket connection,
    """

    async def auth_hook(mgr: WebsocketManager):
        ws = await mgr.unauth_ws
        msg = json.dumps({"type": "auth_request", "token": str(uuid.UUID(int=1))})
        await ws.send(msg)

    json_manager.auth_hook = auth_hook
    json_manager.register_callback(
        json_manager.on_auth,
        on_predicate=lambda topic, msg: msg["type"] == "auth_reply" and msg["success"],
    )
    await json_manager.initialize()
    reply = await run_until_message_type(json_manager, "auth_reply")
    assert reply == {"type": "auth_reply", "success": True}


async def test_auth_on_reconnect(json_manager: WebsocketManager, websocket_server: AppDetails):
    """
    Test that the auth hook is called after the websocket connects.
    Also test that if we set max_reconnections to 1, then the Manager begins shutting down
    when observing a second disconnect from the server.
    """
    token = str(uuid.UUID(int=2))

    async def auth_hook(mgr: WebsocketManager):
        ws = await mgr.unauth_ws
        msg = json.dumps({"type": "auth_request", "token": token})
        await ws.send(msg)

    json_manager.auth_hook = auth_hook
    json_manager.register_callback(
        json_manager.on_auth,
        on_predicate=lambda topic, msg: msg["type"] == "auth_reply" and msg["success"],
    )
    json_manager.max_reconnections = 1
    await json_manager.initialize()
    # test that we're authenticated on the server side
    json_manager.send({"type": "authed_echo_request", "text": "Hello auth"})
    reply = await run_until_message_type(json_manager, "authed_echo_reply")
    assert reply == {"type": "authed_echo_reply", "text": "Hello auth"}

    # disconnect us server-side
    async with httpx.AsyncClient(base_url=websocket_server.url) as client:
        resp = await client.get(f"/disconnect/{token}")
    assert resp.status_code == 204

    json_manager.send({"type": "authed_echo_request", "text": "Hello auth2"})
    reply = await run_until_message_type(json_manager, "authed_echo_reply")
    assert reply == {"type": "authed_echo_reply", "text": "Hello auth2"}

    assert not json_manager._shutting_down
    assert json_manager.reconnections == 1

    # disconnect us for a second time, should not reconnect due to max_reconnections
    async with httpx.AsyncClient(base_url=websocket_server.url) as client:
        resp = await client.get(f"/disconnect/{token}")
    assert resp.status_code == 204

    await asyncio.sleep(0.01)
    assert json_manager._shutting_down


async def test_hooks_in_subclass(websocket_server: AppDetails):
    """
    Test that creating hooks as methods in a subclass definition work
    as well as attaching the hooks to instances of the class.
    """

    class Sub(WebsocketManager):
        def __init__(self, ws_url):
            super().__init__(ws_url)
            self.register_callback(
                self.on_auth,
                on_predicate=lambda topic, msg: msg["type"] == "auth_reply" and msg["success"],
            )

        async def inbound_message_hook(self, raw_contents: str):
            return json.loads(raw_contents)

        async def outbound_message_hook(self, msg: dict):
            return json.dumps(msg)

        async def auth_hook(self, mgr):
            ws = await self.unauth_ws
            msg = json.dumps({"type": "auth_request", "token": str(uuid.UUID(int=3))})
            await ws.send(msg)

    mgr = Sub(ws_url=websocket_server.ws_base + "/ws")
    await mgr.initialize()
    mgr.send({"type": "authed_echo_request", "text": "Hello subclass"})
    reply = await run_until_message_type(mgr, "authed_echo_reply")
    assert reply == {"type": "authed_echo_reply", "text": "Hello subclass"}
    await mgr.shutdown()


# Two fixtures below used by test_structlog_contextvars_worker_hook
# Pattern pulled from https://www.structlog.org/en/stable/testing.html
@pytest.fixture(name="log_output")
def fixture_log_output():
    return structlog.testing.LogCapture()


@pytest.fixture
def fixture_configure_structlog(log_output):
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.CallsiteParameterAdder(
                {structlog.processors.CallsiteParameter.FUNC_NAME}
            ),
            log_output,
        ]
    )


@pytest.mark.usefixtures("fixture_configure_structlog")
async def test_structlog_contextvars_worker_hook(websocket_server: AppDetails, log_output):
    """
    Test that we can bind contextvars within the context_hook method and that any callbacks
    or outbound publishing methods will include those in logs.
    """

    class Sub(WebsocketManager):
        def __init__(self, ws_url):
            super().__init__(ws_url)
            self.session_id = None
            self.register_callback(self.log_received)

        async def context_hook(self):
            structlog.contextvars.bind_contextvars(session_id=self.session_id)

        async def callback_hook(self, message: QueuedMessage, callback: Callback):
            structlog.contextvars.bind_contextvars(
                topic=message.topic, callback_name=callback.qualname
            )

        async def connect_hook(self, mgr):
            ws = await self.unauth_ws
            self.session_id = ws.response_headers.get("session_id")

        async def inbound_message_hook(self, raw_contents: str):
            return json.loads(raw_contents)

        async def outbound_message_hook(self, msg: dict):
            return json.dumps(msg)

        async def _publish(self, message: QueuedMessage):
            await super()._publish(message)
            structlog.get_logger().info(f"Publishing {message.contents}")

        async def log_received(self, message: dict):
            structlog.get_logger().info(f"Received {message}")

    mgr = Sub(ws_url=websocket_server.ws_base + "/ws")
    await mgr.initialize()
    # Wait until we're connected before sending a message, otherwise the outbound worker
    # will drop into .send / ._publish before we have a session_id set
    await mgr.connected.wait()
    mgr.send({"type": "unauthed_echo_request", "text": "Hello 1"})
    # move forward in time until we get the next message from the webserver
    await mgr.next_event.wait()
    publish_log = log_output.entries[0]
    assert publish_log["event"] == 'Publishing {"type": "unauthed_echo_request", "text": "Hello 1"}'
    assert publish_log["session_id"]
    assert publish_log["func_name"] == "_publish"

    receive_log = log_output.entries[1]
    assert receive_log["event"] == "Received {'type': 'unauthed_echo_reply', 'text': 'Hello 1'}"
    assert receive_log["session_id"]
    assert receive_log["func_name"] == "log_received"
    assert receive_log["topic"] == ""
    assert (
        receive_log["callback_name"]
        == "test_structlog_contextvars_worker_hook.<locals>.Sub.log_received"
    )

    await mgr.shutdown()


async def test_disable_polling(mocker):
    """
    Test that registered callbacks (record_last_seen_message) are still called
    when we use .schedule_for_delivery after initializing the WebsocketManager
    with the enable_polling=False flag, so it doesn't attempt to make a connection
    to an external server.

    Also test that callbacks which call .send() do drop messages into the _publish
    method, which would normally then send data over the wire.
    """
    mgr = WebsocketManager(ws_url="ws://test")
    publish = mocker.patch.object(mgr, "_publish")
    await mgr.initialize(enable_polling=False)

    @mgr.callback(on_topic="")
    def echo(msg):
        mgr.send(msg)

    mgr.schedule_for_delivery(topic="", contents="echo test")
    await mgr.next_event.wait()
    assert mgr.last_seen_message == "echo test"
    await asyncio.sleep(0.01)
    publish.assert_called_once_with(QueuedMessage(topic="", contents="echo test", session_id=None))
    await mgr.shutdown()
