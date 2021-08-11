# Standard Library
import asyncio
import logging
import os
import signal

# Third Party
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrTimeout

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class NatsWrapper:
    def __init__(self):
        self.nc = None
        self.NATS_USERNAME = None
        self.NATS_PASSWORD = None
        self.NKEY_SEED_FILENAME = None
        self.NATS_SERVER_URL = os.environ["NATS_SERVER_URL"]
        if "NATS_ENDPOINT" in os.environ:
            self.NATS_SERVER_URL = os.environ["NATS_ENDPOINT"]
        if "NKEY_SEED_FILENAME" in os.environ:
            self.NKEY_SEED_FILENAME = os.environ["NKEY_SEED_FILENAME"]
        elif "NATS_USERNAME" in os.environ:
            self.NATS_USERNAME = os.environ["NATS_USERNAME"]
            self.NATS_PASSWORD = os.environ["NATS_PASSWORD"]
        self.loop = None

    async def connect(self):
        self.nc = NATS()
        self.loop = asyncio.get_event_loop()
        self.add_signal_handler()

        async def error_cb(e):
            logging.warning(f"Error: {str(e)}")

        async def closed_cb():
            logging.warning("Closed connection to NATS")

        async def on_disconnect():
            logging.warning("Disconnected from NATS")

        async def reconnected_cb():
            logging.warning(
                f"Reconnected to NATS at nats://{self.nc.connected_url.netloc}"
            )

        options = {
            "loop": self.loop,
            "error_cb": error_cb,
            "closed_cb": closed_cb,
            "reconnected_cb": reconnected_cb,
            "disconnected_cb": on_disconnect,
            "servers": [self.NATS_SERVER_URL],
            "max_reconnect_attempts": -1,
            "reconnect_time_wait": 5,
            "verbose": True,
            "user": self.NATS_USERNAME,
            "password": self.NATS_PASSWORD,
            "nkeys_seed": self.NKEY_SEED_FILENAME,            
        }
        try:
            await self.nc.connect(**options)
            logging.info(f"Connected to NATS at {self.nc.connected_url.netloc}...")
        except Exception as e:
            logging.info("Failed to connect to nats")
            logging.error(e)

    async def close(self):
        await self.nc.close()

    def add_signal_handler(self):
        def signal_handler():
            if self.nc.is_closed:
                return
            logging.warning("Disconnecting...")
            self.loop.create_task(self.nc.close())

        for sig in ("SIGINT", "SIGTERM"):
            self.loop.add_signal_handler(getattr(signal, sig), signal_handler)

    async def subscribe(
        self,
        nats_subject: str,
        payload_queue: asyncio.Queue = None,
        nats_queue: str = "",
        subscribe_handler=None,
    ):
        async def default_subscribe_handler(msg):
            subject = msg.subject
            reply = msg.reply
            payload_data = msg.data.decode()
            await payload_queue.put(payload_data)

        if subscribe_handler is None:
            subscribe_handler = default_subscribe_handler
        await self.nc.subscribe(nats_subject, queue=nats_queue, cb=subscribe_handler)

    async def publish(self, nats_subject: str, payload_df):
        """
        this is not necessary at this point though
        """
        await self.nc.publish(nats_subject, payload_df)

    async def request(self, nats_subject: str, payload, timeout=1):
        return await self.nc.request(nats_subject, payload, timeout=timeout)


async def nats_request(nw):
    my_request = b"Request!"
    try:
        response = await nw.request(
            "logs",
            my_request,
            timeout=1,
        )
        response = response.data.decode()
    except ErrTimeout:
        response = ""
    logging.info(f" <nats request> response for the request : {response}")


async def nats_reply(nw):
    async def receive_and_reply(msg):
        reply_subject = msg.reply
        if msg.data:
            reply_message = b"YES"
        else:
            reply_message = b"NO"
        logging.info(f"<nats reply> reply_message : {reply_message}")
        await nw.publish(reply_subject, reply_message)

    await nw.subscribe("logs", subscribe_handler=receive_and_reply)


async def nats_subscriber(nw, payload_queue):
    async def my_handler(msg):
        subject = msg.subject
        reply = msg.reply
        payload_data = msg.data.decode()
        logging.info(f"<nats subscriber> received payload message : {payload_data}")
        await payload_queue.put(payload_data)

    await nw.subscribe(
        nats_subject="preprocessing-logs",
        payload_queue=payload_queue,
        subscribe_handler=my_handler,
    )


async def nats_publisher(nw):
    payload = b"Message"
    logging.info(f"<nats publisher> sending message : {payload}")
    await nw.publish("preprocessing-logs", payload)


async def run(loop):
    logging.info("Attempting to connect to NATS")
    await nw.connect()

    # publish and subscribe
    payload_queue = asyncio.Queue(loop=loop)
    await nats_subscriber(nw, payload_queue)
    await nats_publisher(nw)

    # request and reply
    await nats_reply(nw)
    await nats_request(nw)

    await nw.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    nw = NatsWrapper()
    task = loop.create_task(run(loop))
    loop.run_until_complete(task)
    loop.close()
