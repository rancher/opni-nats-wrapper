import asyncio
import logging

from nats.errors import TimeoutError
from nats_wrapper import NatsWrapper


async def jetstream_streaming(js):
    # Persist messages on 'foo's subject.
    # await js.add_stream(name="sample-stream", subjects=["foo"])
    await js.add_stream(name="new-stream", subjects=["foo1"])

    for i in range(0, 10):
        ack = await js.publish("foo1", f"hello world: {i}".encode())
        print(ack)

    # Create pull based consumer on 'foo'.
    psub = await js.pull_subscribe("foo1", "psub")

    # Fetch and ack messagess from consumer.
    for i in range(0, 10):
        msgs = await psub.fetch(1)
        for msg in msgs:
            await msg.ack()
            print(msg)

    # Create single ephemeral push based subscriber.
    sub = await js.subscribe("foo1")
    msg = await sub.next_msg()
    await msg.ack()

    # Create single push based subscriber that is durable across restarts.
    sub = await js.subscribe("foo1", durable="myapp1")
    msg = await sub.next_msg()
    await msg.ack()

    # Create deliver group that will be have load balanced messages.
    async def qsub_a(msg):
        print("QSUB A:", msg)
        await msg.ack()

    async def qsub_b(msg):
        print("QSUB B:", msg)
        await msg.ack()

    await js.subscribe("foo1", "workers1", cb=qsub_a)
    await js.subscribe("foo1", "workers1", cb=qsub_b)

    for i in range(0, 10):
        ack = await js.publish("foo1", f"hello world: {i}".encode())
        print("\t", ack)

    # Create ordered consumer with flow control and heartbeats
    # that auto resumes on failures.
    osub = await js.subscribe("foo1", ordered_consumer=True)
    data = bytearray()

    while True:
        try:
            msg = await osub.next_msg()
            data.extend(msg.data)
        except TimeoutError:
            break
    print("All data in stream:", len(data))


async def kv_usage(nw):
    b = "my_kv_bucket"
    await nw.get_bucket(b)
    kv = await nw.create_bucket(b)
    await kv.put("hello1", b"world2")
    entry = await kv.get("hello1")
    logging.info(f"KeyValue.Entry: key={entry.key}, value={entry.value}")
    await kv.delete("hello1")
    await nw.delete_bucket(b)


async def main():
    nw = NatsWrapper()
    await nw.connect()

    # Create JetStream context.
    js = nw.get_jetstream()
    await jetstream_streaming(js)
    await kv_usage(nw)

    await nw.close()


if __name__ == "__main__":
    asyncio.run(main())
