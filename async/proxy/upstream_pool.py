import asyncio
import itertools


class UpstreamPool:

    def __init__(self, upstreams, max_conns_per_upstream):
        self.upstreams = [up for up in upstreams]
        self.cycle = itertools.cycle(self.upstreams)
        self.upstream_keys_by_semaphore = {
            self.get_upstream_key(up): asyncio.Semaphore(max_conns_per_upstream)
            for up in self.upstreams
        }

    @staticmethod
    def get_upstream_key(upstream):
        return upstream["host"], upstream["port"]

    def get_next_upstream(self):
        return next(self.cycle)

    # Тут можно было использовать @asynccontextmanager но я уже использую semaphore как менеджер контекста
    # Поэтому тут решил попробовать реализовать ручное изменение счётчика соединений
    async def open_connection(self):
        upstream = self.get_next_upstream()
        key = self.get_upstream_key(upstream)
        semaphore = self.upstream_keys_by_semaphore[key]

        await semaphore.acquire()
        try:
            reader, writer = await asyncio.open_connection(
                upstream["host"],
                upstream["port"]
            )
            return upstream, reader, writer
        except Exception:
            semaphore.release()
            raise

    def release(self, upstream):
        key = self.get_upstream_key(upstream)
        self.upstream_keys_by_semaphore[key].release()
