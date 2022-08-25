import asyncio
import logging
from typing import Any, Callable, Optional

import websockets

from sending.base import AbstractPubSubManager, QueuedMessage
from sending.util import ensure_async

logger = logging.getLogger(__name__)


class WebsocketManager(AbstractPubSubManager):
    def __init__(self, ws_url: str):
        """
        Websocket-based Sending backend. This class handles creating the initial
        websocket connection and reconnecting on server-side disconnect.

        One key difference between this Backend and others is that it ignores
        the concept of `topic`. All messages seen over the wire are dropped
        onto the inbound queue with topic '', and .send() is overwritten
        to only take a message, defaulting outbound topic to ''.

        Real-world applications are likely to subclass this and add hooks
        either as class methods or attach them to an instance after init.

         - inbound_message_hook: deserialize incoming messages over websocket
         - outbound_message_hook: serialize outgoing messages over websocket
         - init_hook: called after websocket connection is established
         - auth_hook: called just before init_hook, useful if you need to send
                      some kind of auth request. This effects .send(), make sure
                      you also register a callback to .on_auth that is called
                      when you receive an auth response.
        """
        super().__init__()
        self.ws_url = ws_url

        # Save / overwrite response headers on each connection
        self.response_headers = {}

        # An unauth_ws and authed_ws pair of Futures are created so that
        # sub-classes can easily implement a pattern where messages are only
        # sent to the server after the session has been authenticated.
        self.unauth_ws = asyncio.Future()
        self.authed_ws = asyncio.Future()
        # Can use await mgr.connected.wait() to block until the websocket is connected
        # in tests or in situations where you want connect_hook / context_hook to have
        # information available to it from the websocket response (e.g. RTU session id)
        self.connected = asyncio.Event()

        # When an outbound worker is ready to send a message over the wire, it
        # calls ._publish which will await the unauth_ws or authed_ws Future.
        # However, if something goes wrong and those Futures never resolve then
        # it can leave you in a hard-to-debug state. So ._publish should use an
        # asyncio.wait_for(await <ws future>, timeout=self.publish_timeout)
        self.publish_timeout: float = 5.0

        # Used to prevent automatic websocket reconnect when we're trying to shutdown
        self._shutting_down = False

        # For debug and testing, .next_event gets set/cleared after every received message
        self.next_event = asyncio.Event()
        self.last_seen_message = None
        self.reconnections = 0
        # If this is set, then the WebsocketManager will stop reconnecting after this
        # many reconnections.
        self.max_reconnections = None

        # Optional hooks that can be defined in a subclass or attached to an instance.
        # - connect_hook is called first when websocket is established, useful to
        #   set contextvars or store state before init / auth hooks are called
        #
        # - auth_hook is called next, and also effects how .send() works.
        #   If auth_hook is defined then .send() won't actually transmit data over the wire
        #   until on_auth callback has been triggered.
        #   You want to define an auth_hook if the websocket server expects a first message
        #   to be some kind of authentication
        #
        # - init_hook is called next after auth_hook, useful to kick off messages after
        #   auth_hook, or if authentication is not part of the websocket server flow.
        #
        # - disconnect_hook is called when the websocket connection is lost
        #
        if not hasattr(self, "auth_hook"):
            self.auth_hook: Optional[Callable] = None
        if not hasattr(self, "init_hook"):
            self.init_hook: Optional[Callable] = None
        if not hasattr(self, "connect_hook"):
            self.connect_hook: Optional[Callable] = None
        if not hasattr(self, "disconnect_hook"):
            self.disconnect_hook: Optional[Callable] = None

        self.register_callback(self.record_last_seen_message)

    async def record_last_seen_message(self, message: Any):
        """
        Automatically registered callback. Used for debugging and testing.

        await mgr.next_event.wait()
        assert mgr.last_seen_message == <what you expect>

        Alternatively, if messages may come out of order, iterate until
        you see the type of message you want to test for.

        while True:
            await asyncio.wait_for(mgr.next_event.wait(), timeout=1)
            if mgr.last_seen_message['key_field'] == key_of_interest:
                break
        assert mgr.last_seen_message['value_field'] == expected_value
        """
        self.last_seen_message = message
        self.next_event.set()
        self.next_event.clear()

    async def on_auth(self, message: Any):
        """
        Example callback for what should happen when an auth flow is complete
        """
        self.authed_ws.set_result(self.unauth_ws.result())

    def send(self, message: Any):
        """
        Override the default Sending behavior to only accept a message instead of topic + message.
        Topic is not a concept supported in this Backend.
        """
        # QueuedMessage is a NamedTuple of topic, contents, session_id
        self.outbound_queue.put_nowait(QueuedMessage("", message, None))

    async def _publish(self, message: QueuedMessage):
        """
        Once a message has been picked up from the inbound queue, processed by a callback,
        then dropped onto the outbound queue, the outbound worker will call this method.

        QueuedMessage is namedtuple of topic, contents, session_id. Only contents matter
        for this Backend.

        Subclasses that need to serialize outbound data can define a .outbound_message_hook
        instead of overriding this method. For instance, if using self.send(<pydantic model>)
        you may want to define .outbound_message_hook = lambda model: model.json()
        """
        # Assume if an implementation has an auth_hook then it wants to delay
        # sending outbound messages over the wire until the session is authenticated.
        # use asyncio.wait_for so we don't end up in a hard-to-debug state if a Future
        # doesn't resolve for some reason.

        if self.auth_hook:
            if not self.authed_ws.done():
                # special logging here because this is a sign that you might be in
                # a particularly bad position. Something called .send() before
                # callback to .on_auth or similar 'set the authed_ws Future' triggered.
                logger.debug("Message queued, waiting for authed_ws to be set")
            ws = await asyncio.wait_for(self.authed_ws, timeout=self.publish_timeout)
        else:
            ws = await asyncio.wait_for(self.unauth_ws, timeout=self.publish_timeout)
        logger.debug(f"Sending: {message.contents}")
        await ws.send(message.contents)

    async def _poll_loop(self):
        """
        When WebsocketManager.initialize() is awaited, it creates an asyncio Task that runs
        this function. This is the meat of the WebsocketManager class. It handles creating
        the websocket connection, reconnecting if the server disconnects, receiving messages
        over the wire, and putting them onto the inbound message queue.

        It also uses authentication pattern hooks if they're implemented.
        """
        # Automatic reconnect https://websockets.readthedocs.io/en/stable/reference/client.html
        async for websocket in websockets.connect(self.ws_url):
            self.unauth_ws.set_result(websocket)
            if self.connect_hook:
                fn = ensure_async(self.connect_hook)
                await fn(self)
            if self.context_hook:
                await self.context_hook()
            self.connected.set()
            try:
                # Call the auth and init hooks (casting to async if necessary), passing in 'self'
                if self.auth_hook:
                    fn = ensure_async(self.auth_hook)
                    await fn(self)
                if self.init_hook:
                    fn = ensure_async(self.init_hook)
                    await fn(self)
                async for message in websocket:
                    logger.debug(f"Received: {message}")
                    self.schedule_for_delivery(topic="", contents=message)
            except websockets.ConnectionClosed:
                # This will get raised if there's an error trying to connect,
                # keeping it separate from unknown exceptions that a subclass might want
                # to handle differently.
                continue
            except Exception as e:
                await self.on_exception(e)
                continue
            finally:
                if self._shutting_down:
                    break
                elif self.max_reconnections and self.reconnections >= self.max_reconnections:
                    logger.warning("Hit max reconnection attempts, not reconnecting")
                    return await self.shutdown()
                logger.info("Websocket server disconnected, resetting Futures and reconnecting")
                if self.disconnect_hook:
                    fn = ensure_async(self.disconnect_hook)
                    await fn(self)
                self.connected.clear()
                self.unauth_ws = asyncio.Future()
                self.authed_ws = asyncio.Future()
                self.reconnections += 1

    async def shutdown(self, now: bool = False):
        """
        Custom shutdown logic to take account of closing our automatically-reconnecting websocket.
        In an ideal world, we drain all outbound messages, stop the task that's reading new
        inbound messages, and then perform the websocket close handshake.
        """
        self._shutting_down = True
        await super().shutdown(now)
        if self.unauth_ws.done():
            ws = await self.unauth_ws
            await ws.close()

    async def on_exception(self, exc: Exception):
        # Called when we get an exception iterating over websocket messages before
        # we reconnect, in case a Subclass wants to do something with it
        logger.exception(exc)

    async def _create_topic_subscription(self, topic_name: str):
        # Required method by the ABC base, but topics are irrelevant to this Backend
        pass

    async def _cleanup_topic_subscription(self, topic_name: str):
        # Required method by the ABC base, but topics are irrelevant to this Backend
        pass

    async def _poll(self):
        # Required method by the ABC base, but never used.
        # Normally this does some kind of message cleaning and adds the message onto
        # the inbound queue to be processed. _poll_loop handles that in this Backend.
        pass
