"""
A test RTU server for the `RTUManager` Sending backend.

A real RTU validation server (i.e. Gate) would do so much more than what we need here.

XXX: long term this will probably move into some separate package that comprises the "building blocks"
     of RTU validation server and RTU clients: the RTU models, a minimal server, a generic client (maybe based on Sending).

What we care about right now:
 - Testing reconnections
 - Authenticating over websocket and testing persisted auth
 - Returning File Subscribe reply after a File Subscribe request
"""
import uuid
from datetime import datetime
from typing import List, Optional

import websockets
from fastapi import Depends, FastAPI, WebSocket, WebSocketDisconnect, status
from origami.types import rtu
from starlette.websockets import WebSocketState

# debug=True to help make errors during testing show up more clearly
app = FastAPI(debug=True)


class WebsocketSession:
    """
    Wrapper around a basic websocket connection to maintain state about whether it has
    performed an AuthenticateRequest / AuthenticateReply exchange yet
    """

    def __init__(self, ws: WebSocket):
        self.ws = ws
        self.authenticated = False
        self.auth_token: Optional[str] = None


class WebsocketManager:
    """
    Singleton collection of WebsocketSessions. Behaviors used in tests:
     - Force disconnect an authenticated session by GET /disconnect-ws/{jwt} to test reconnect
    """

    _singleton_instance = None

    def __init__(self):
        self.sessions: List[WebsocketSession] = []

    @classmethod
    async def singleton(cls):
        if cls._singleton_instance is None:
            cls._singleton_instance = cls()
        return cls._singleton_instance

    dependency = singleton

    async def connect(self, session: WebsocketSession):
        await session.ws.accept()
        self.sessions.append(session)

    async def disconnect(self, session: WebsocketSession):
        if session.ws.client_state == WebSocketState.CONNECTED:
            await session.ws.close()
        if session in self.sessions:
            self.sessions.remove(session)


# XXX: Maybe turn these into real JWT's in the future, for now this is enough
# to test valid / invalid AuthenticateRequest messages
VALID_JWTS = [str(uuid.UUID(int=1)), str(uuid.UUID(int=2))]


async def on_auth_request(req: rtu.GenericRTURequest, session: WebsocketSession):
    """
    Callback for AuthenticateRequest messages.
    If the client has a valid JWT, set their session to .authenticated = True
    """
    if req.data["token"] in VALID_JWTS:
        session.authenticated = True
        session.auth_token = str(req.data["token"])
        return rtu.AuthenticationReply(
            transaction_id=req.transaction_id,
            channel="system",
            event="authenticate_reply",
            msg_id=uuid.uuid4(),
            executing_user_id=uuid.uuid4(),
            processed_timestamp=datetime.now(),
            data=rtu.AuthenticationReplyData(success=True),
        )
    else:
        return rtu.AuthenticationReply(
            transaction_id=req.transaction_id,
            channel="system",
            event="authenticate_reply",
            msg_id=uuid.uuid4(),
            executing_user_id=uuid.uuid4(),
            processed_timestamp=datetime.now(),
            data=rtu.AuthenticationReplyData(success=False),
        )


async def on_subscribe_request(req: rtu.GenericRTURequest, session: WebsocketSession):
    """
    If the session is authenticated, honor the File Subscribe request.
    """
    if not session.authenticated:
        # Leaving this authentication check here but noting that it's not used in any tests right now.
        # The RTUManager will shutdown if the auth request fails, so there's no test to send a FileSubscribeRequest
        # without being authenticated successfully.
        return rtu.FileSubscribeReplySchema(
            transaction_id=req.transaction_id,
            channel=req.channel,
            event="subscribe_reply",
            msg_id=uuid.uuid4(),
            executing_user_id=uuid.uuid4(),
            processed_timestamp=datetime.now(),
            data=rtu.FileSubscribeActionReplyData(success=False, user_subscriptions=[]),
        )
    return rtu.FileSubscribeReplySchema(
        transaction_id=req.transaction_id,
        channel=req.channel,
        event="subscribe_reply",
        msg_id=uuid.uuid4(),
        executing_user_id=uuid.uuid4(),
        processed_timestamp=datetime.now(),
        data=rtu.FileSubscribeActionReplyData(success=True, user_subscriptions=[]),
    )


@app.websocket("/rtu")
async def rtu_websocket(
    ws: WebSocket, manager: WebsocketManager = Depends(WebsocketManager.dependency)
):
    """
    RTU websocket endpoint
    """
    session = WebsocketSession(ws)
    await manager.connect(session)

    while True:
        try:
            raw_data = await ws.receive_text()
            req = rtu.GenericRTURequest.parse_raw(raw_data)
            if req.event == "authenticate_request":
                reply = await on_auth_request(req, session)
                await ws.send_text(reply.json())

            if req.event == "subscribe_request":
                reply = await on_subscribe_request(req, session)
                await ws.send_text(reply.json())
        except WebSocketDisconnect:
            # We end up here if the client had disconnected before we called ws.receive_text()
            await manager.disconnect(session)
            break
        except websockets.ConnectionClosedOK:
            # I think we end up here if we disconnected the client (e.g. from a GET /disconnect-ws/{jwt})
            await manager.disconnect(session)
            break


@app.get("/disconnect-ws/{session_token}", status_code=status.HTTP_204_NO_CONTENT)
async def disconnect(
    session_token: str, manager: WebsocketManager = Depends(WebsocketManager.dependency)
):
    """
    Endpoint to trigger a server-side websocket disconnect, as if the RTU validation server crashed.
    Used in tests for the RTU client reconnection logic.
    """
    for session in manager.sessions:
        if session.auth_token == session_token:
            await manager.disconnect(session)
