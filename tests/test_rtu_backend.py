import asyncio
import uuid

import httpx
import pytest
from managed_service_fixtures import AppDetails, managed_asgi_app_factory
from origami.types import rtu

from sending.backends.rtu import RTUManager


@pytest.fixture(scope="session")
def rtu_server(managed_asgi_app_factory) -> AppDetails:
    """
    Start the FastAPI app defiened in tests/rtu_server.py in an external process using uvicorn.
    AppDetails.url and AppDetails.ws_base have the connection details (host/port) for http and ws endpoints.
    """
    app_location = "tests.rtu_server:app"
    with managed_asgi_app_factory(app_location) as app_details:
        yield app_details


async def run_until_event(manager: RTUManager, event: str) -> rtu.GenericRTUReply:
    """
    Helper function. Moves forward through each received RTU-structured message coming
    over the websocket until a specific event type is observed.
    """
    while True:
        if manager.last_seen_message and manager.last_seen_message.event == event:
            return manager.last_seen_message
        await asyncio.wait_for(manager.next_event.wait(), timeout=5)


@pytest.mark.rtu
async def test_connection_flow(rtu_server: AppDetails):
    """
    Tests that the RTUManager backend can open up a websocket connection,
    send an AuthenticateRequest message, and then a File Subscription request
    after it observes the AuthenticateReply response.
    """
    manager = RTUManager(
        ws_url=rtu_server.ws_base + "/rtu",
        jwt=str(uuid.UUID(int=1)),
        file_id=uuid.uuid4(),
        version_id=uuid.uuid4(),
    )
    await manager.initialize()

    event = await run_until_event(manager, "authenticate_reply")
    assert event.data["success"]
    assert manager.authed_ws.done()
    assert await manager.authed_ws == await manager.unauth_ws

    event = await run_until_event(manager, "subscribe_reply")
    assert event.data["success"]

    await manager.shutdown()


@pytest.mark.rtu
async def test_invalid_jwt(rtu_server: AppDetails):
    """
    Tests that the RTUManager backend will shutdown and stop processing
    if the RTU validation server rejects its Authenticate Request.
    """
    manager = RTUManager(
        ws_url=rtu_server.ws_base + "/rtu",
        jwt=str(uuid.UUID(int=42)),
        file_id=uuid.uuid4(),
        version_id=uuid.uuid4(),
    )
    await manager.initialize()
    assert manager.inbound_queue is not None

    event = await run_until_event(manager, "authenticate_reply")
    assert not event.data["success"]

    assert manager._shutting_down


@pytest.mark.rtu
async def test_reconnect(rtu_server: AppDetails):
    """
    Test that the RTUManager will reconnect if the server disconnects.
    In the test RTU server, we can trigger a server-side disconnect by hitting GET /disconnect-ws/{our-jwt}.
    In the real world of course we care about reconnecting in the event the RTU server crashes
    """
    manager = RTUManager(
        ws_url=rtu_server.ws_base + "/rtu",
        jwt=str(uuid.UUID(int=2)),
        file_id=uuid.uuid4(),
        version_id=uuid.uuid4(),
    )
    await manager.initialize()

    event = await run_until_event(manager, "authenticate_reply")
    assert event.data["success"]
    assert manager.reconnections == 0

    async with httpx.AsyncClient(base_url=rtu_server.url) as client:
        resp = await client.get(f"/disconnect-ws/{manager.jwt}")
    assert resp.status_code == 204

    event = await run_until_event(manager, "authenticate_reply")
    assert event.data["success"]
    assert manager.reconnections == 1

    await manager.shutdown(now=False)
