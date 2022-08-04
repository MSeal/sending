"""
A FastAPI app used to test the WebsocketManager Sending backend. This thing
gets spun up as an external service using managed-service-fixtures in 
tests/test_websocket_backend.py.
"""

import uuid
from typing import List, Optional

import websockets
from fastapi import Depends, FastAPI, WebSocket, WebSocketDisconnect, status
from starlette.websockets import WebSocketState

# debug=True to help make errors during testing show up more clearly
app = FastAPI(debug=True)


class WebsocketSession:
    """
    Wrapper around a basic websocket connection to maintain state about whether it has
    performed some kind of authentication exchange
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


# Create an arbitrary auth pattern, tests will subclass the WebsocketManager
# sending backend to implement the auth pattern
VALID_TOKENS = [str(uuid.UUID(int=1)), str(uuid.UUID(int=2)), str(uuid.UUID(int=3))]


async def on_auth_request(token: str, session: WebsocketSession):
    """
    Callback for when a client sends a msg like {'type': 'auth_request', 'token': '<token>'}
    If the token is in VALID_TOKENS, set the session as authenticated.
    Other callback functions may check whether a session is authenticated or not.
    """
    if token in VALID_TOKENS:
        session.authenticated = True
        session.auth_token = token
        await session.ws.send_json({"type": "auth_reply", "success": True})
    else:
        await session.ws.send_json({"type": "auth_reply", "success": False})


async def on_unauthed_echo(text: str, session: WebsocketSession):
    """
    Always echoes some text, no matter whether the session is authenticated.
    """
    await session.ws.send_json({"type": "unauthed_echo_reply", "text": text})


async def on_authed_echo(text: str, session: WebsocketSession):
    """
    Echoes some text if the Websocket Session is authenticated.
    Returns an error message if not.
    """
    if session.authenticated:
        await session.ws.send_json({"type": "authed_echo_reply", "text": text})
    else:
        await session.ws.send_json(
            {"type": "authed_echo_reply", "error": "You are not authenticated"}
        )


@app.websocket("/ws")
async def websocket_endpoint(
    ws: WebSocket, manager: WebsocketManager = Depends(WebsocketManager.dependency)
):
    """
    Websocket endpoint. Expects messages to be JSON serialized and have a key 'type'.
    Callbacks are called based on the type value:
     - 'auth_request': expects a 'token' key. If it in VALID_TOKENS, set session as authenticated
     - 'echo_request': expects a 'text' key. If the session is authenticated, echo the text back.
    """
    session = WebsocketSession(ws)
    await manager.connect(session)

    while True:
        try:
            data = await ws.receive_json()
            if data["type"] == "auth_request":
                await on_auth_request(token=data["token"], session=session)
            elif data["type"] == "unauthed_echo_request":
                await on_unauthed_echo(text=data["text"], session=session)
            elif data["type"] == "authed_echo_request":
                await on_authed_echo(text=data["text"], session=session)
        except WebSocketDisconnect:
            # We end up here if the client had disconnected before we called ws.receive_text()
            await manager.disconnect(session)
            break
        except websockets.ConnectionClosedOK:
            # I think we end up here if we disconnected the client (e.g. GET /disconnect/{jwt})
            await manager.disconnect(session)
            break


@app.get("/disconnect/{session_token}", status_code=status.HTTP_204_NO_CONTENT)
async def disconnect(
    session_token: str, manager: WebsocketManager = Depends(WebsocketManager.dependency)
):
    """
    Endpoint to trigger a server-side websocket disconnect, as if the websocket server crashed.
    Used in tests for the client reconnection logic.
    """
    for session in manager.sessions:
        if session.auth_token == session_token:
            await manager.disconnect(session)
