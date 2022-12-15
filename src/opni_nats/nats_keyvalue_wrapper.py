from nats.js.kv import KeyValue


class NatsKeyValueWrapper:
    def __init__(self, bucket: str, kv: KeyValue):
        self.bucket = bucket
        self.kv = kv

    def get_bucket_name(self) -> str:
        return self.bucket

    async def get(self, key: str, revision: int = None) -> bytes:
        try:
            entry = await self.kv.get(key, revision)
            return entry.value
        except Exception as e:
            return None

    async def put(self, key: str, value: bytes) -> int:
        return await self.kv.put(key, value)

    async def delete(self, key: str, last: int = None):
        await self.kv.delete(key=key, last=last)
