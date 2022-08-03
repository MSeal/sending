import asyncio
import uuid

import structlog
import websockets
from origami.types import rtu

from sending.base import AbstractPubSubManager, QueuedMessage

logger = structlog.get_logger(__name__)


class RTUManager(AbstractPubSubManager):
    def __init__(self, ws_url: str, jwt: str, file_id: str, version_id: uuid.UUID):
        """
        Websocket-based Sending backend that is designed to send and receive RTU modeled data.

        ws_url: Full websocket url to connect to
        jwt: JSON Web Token used for authenticating after websocket is connected
        file_id: The File ID we want to subscribe to deltas for after authenticating
        version_id: The version of the File stored in S3. An RTU client should pull that down
                    as the seed Notebook to apply deltas to. RTU File subscription takes
                    version_id as a parameter and will return appropriate deltas
        """
        super().__init__()
        self.ws_url = ws_url
        self.jwt = jwt
        self.file_id = file_id
        self.file_version_id = version_id

        # Will be WebSocketClientProtocol, .unauth_ws is set immediately after websocket connect
        # while .authed_ws is set after observing a valid Auth Reply event. Callback functions
        # can be queued up but won't run until self.authed_ws is set.
        self.unauth_ws = asyncio.Future()
        self.authed_ws = asyncio.Future()

        # Used to prevent automatic websocket reconnect when we're shutting down
        self._shutting_down = False

        # For debug and testing, gets set/reset after every received message
        self.next_event = asyncio.Event()
        self.last_seen_message = None
        self.reconnections = 0

        # Record every RTU message we see, primarily for debug/testing
        self.register_callback(self.record_last_seen_message)

        # When we get the Auth reply, set .authed_ws and send a File subscribe request
        self.register_callback(
            self.on_auth_reply,
            on_predicate=lambda topic, msg: topic == "system" and msg.event == "authenticate_reply",
        )

    async def _poll_loop(self):
        """
        This is called by RTUManager.initialize(). It is effectively the "start action".
        The loop below will handle reconnecting if the remote server disconnects.

        It's important to consider the asynchronous flow of messages:
         - We want to send an Auth Request first thing
         - We may be receiving System messsages while Authenticating
         - After getting the Auth Reply, we want to send a Subscribe Request (handled by .on_auth_reply)
        """
        # websockets library handles reconnecting automatically
        async for websocket in websockets.connect(self.ws_url):
            try:
                # self.unauth_ws and self.authed_ws are both asyncio.Future objects that will eventually
                # point to the same websocket object. The latter will get set after observing an Auth Reply event
                self.unauth_ws.set_result(websocket)

                # Send Auth request then begin queuing up any received websocket messages for callbacks
                auth_request = rtu.AuthenticationRequest(
                    transaction_id=uuid.uuid4(),
                    data=rtu.AuthenticationRequestData(token=self.jwt),
                )
                logger.info(
                    f"Sending AuthenticationRequest with token: {self.jwt[:5]}...{self.jwt[-5:]}"
                )
                await websocket.send(auth_request.json())

                async for msg in websocket:
                    rtu_msg = rtu.GenericRTUReply.parse_raw(msg)
                    logger.info(f"Received msg: {rtu_msg}")
                    self.schedule_for_delivery(topic=rtu_msg.channel, contents=rtu_msg)

            except websockets.ConnectionClosed:
                logger.info("Error during websocket connection, retrying")
                pass
            except Exception as e:
                logger.exception(f"Exception: {e}")
                continue
            finally:
                if self._shutting_down:
                    break
                logger.info("Websocket server disconnected, resetting Futures and reconnecting")
                self.unauth_ws = asyncio.Future()
                self.authed_ws = asyncio.Future()
                self.reconnections += 1

    async def shutdown(self, now: bool = False):
        """
        Close down inbound and outbound queue workers, the _poll_loop worker, and send websocket close handshake
        """
        self._shutting_down = True
        await super().shutdown(now)
        if self.unauth_ws.done():
            ws = await self.unauth_ws
            await ws.close()

    def send(self, message: rtu.GenericRTURequest):
        # override base .send to pull the topic out of the RTU message's .channel
        self.outbound_queue.put_nowait(QueuedMessage(message.channel, message, None))

    async def on_auth_reply(self, message: rtu.GenericRTUReply):
        """
        Automatically registered callback for channel='system' event='authenticate_reply' RTU messages.
         - sets .authed_ws to the websocket object so that any call to .send / ._publish will begin working
         - sends a SubscribeRequest to subscribe for the file / version
        """
        logger.info("in on_auth_reply")
        if message.data["success"]:
            self.authed_ws.set_result(self.unauth_ws.result())
            logger.info(
                f"Authentication successful, subscribing to file: {self.file_id} version: {self.file_version_id}"
            )

            subscribe_request = rtu.FileSubscribeRequestSchema(
                transaction_id=uuid.uuid4(),
                channel=f"files/{self.file_id}",
                event="subscribe_request",
                data=rtu.FileSubscribeRequestData(from_version_id=self.file_version_id),
            )
            self.send(message=subscribe_request)
        else:
            logger.info(f"Authentication failed, shutting down")
            await self.shutdown(now=False)

    async def record_last_seen_message(self, message: rtu.GenericRTUReply):
        """
        Automatically registered callback on every RTU message that comes over the websocket.
        Used for debugging and testing.
        """
        self.last_seen_message = message
        self.next_event.set()
        self.next_event.clear()

    async def _create_topic_subscription(self, topic_name: str):
        # todo: send ws message subscribing to files channel
        pass

    async def _cleanup_topic_subscription(self, topic_name: str):
        # todo: send ws message unsubscribing from files channel
        pass

    async def _poll(self):
        # required to override because it's @abstractmethod but the logic is implemented
        # in _poll_loop for this class
        pass

    async def _publish(self, message: QueuedMessage):
        """
        .send puts messages on the outbound queue and the outbound worker calls this to actually send
        stuff over the wire. message.contents should be an RTU data model.
        """
        ws = await self.authed_ws
        logger.debug(f"Sending message: {message.contents}")
        await ws.send(message.contents.json())
