import asyncio
import socket
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Tuple

from loguru import logger


UpstreamKey = Tuple[str, int]


@dataclass
class UpstreamConnection:
    upstream: dict
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    def is_usable(self) -> bool:
        if self.writer.is_closing():
            return False

        transport = self.writer.transport
        if transport is not None and transport.is_closing():
            return False

        return not self.reader.at_eof()


class UpstreamState:
    def __init__(self, max_connections: int):
        self.max_connections = max_connections
        self.open_connections = 0
        self.idle_connections: Deque[UpstreamConnection] = deque()
        self.condition = asyncio.Condition()


class UpstreamPool:
    def __init__(self, upstreams, max_conns_per_upstream):
        self.upstreams: List[dict] = [up for up in upstreams]
        self.max_conns_per_upstream = max_conns_per_upstream
        self._states: Dict[UpstreamKey, UpstreamState] = {
            self.get_upstream_key(upstream): UpstreamState(max_conns_per_upstream)
            for upstream in self.upstreams
        }
        self._next_index = 0

    @staticmethod
    def get_upstream_key(upstream) -> UpstreamKey:
        return upstream["host"], upstream["port"]

    @staticmethod
    def enable_tcp_keepalive(writer: asyncio.StreamWriter) -> None:
        sock = writer.get_extra_info("socket")
        if sock is None:
            return

        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
        if hasattr(socket, "TCP_KEEPINTVL"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 15)
        if hasattr(socket, "TCP_KEEPCNT"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 4)

    def _ordered_upstreams(self) -> List[dict]:
        if not self.upstreams:
            raise RuntimeError("No upstreams configured")

        start_index = self._next_index
        self._next_index = (self._next_index + 1) % len(self.upstreams)
        return [
            self.upstreams[(start_index + offset) % len(self.upstreams)]
            for offset in range(len(self.upstreams))
        ]

    async def _try_acquire(self, upstream: dict) -> Optional[UpstreamConnection]:
        state = self._states[self.get_upstream_key(upstream)]

        async with state.condition:
            while state.idle_connections:
                connection = state.idle_connections.pop()
                if connection.is_usable():
                    return connection

                state.open_connections -= 1
                state.condition.notify()
                await self._close_connection(connection)

            if state.open_connections < state.max_connections:
                state.open_connections += 1
                should_create = True
            else:
                should_create = False

        if not should_create:
            return None

        try:
            reader, writer = await asyncio.open_connection(
                upstream["host"],
                upstream["port"],
            )
            self.enable_tcp_keepalive(writer)
            logger.info(
                "Created new upstream connection to {}:{}",
                upstream["host"],
                upstream["port"],
            )
            return UpstreamConnection(upstream=upstream, reader=reader, writer=writer)
        except Exception:
            async with state.condition:
                state.open_connections -= 1
                state.condition.notify()
            raise

    async def _wait_and_acquire(self, upstream: dict) -> UpstreamConnection:
        state = self._states[self.get_upstream_key(upstream)]

        while True:
            async with state.condition:
                while True:
                    while state.idle_connections:
                        connection = state.idle_connections.pop()
                        if connection.is_usable():
                            return connection

                        state.open_connections -= 1
                        state.condition.notify()
                        await self._close_connection(connection)

                    if state.open_connections < state.max_connections:
                        state.open_connections += 1
                        should_create = True
                        break

                    await state.condition.wait()

            if should_create:
                try:
                    reader, writer = await asyncio.open_connection(
                        upstream["host"],
                        upstream["port"],
                    )
                    self.enable_tcp_keepalive(writer)
                    logger.info(
                        "Created new upstream connection to {}:{}",
                        upstream["host"],
                        upstream["port"],
                    )
                    return UpstreamConnection(upstream=upstream, reader=reader, writer=writer)
                except Exception:
                    async with state.condition:
                        state.open_connections -= 1
                        state.condition.notify()
                    raise

    async def acquire(self) -> UpstreamConnection:
        ordered_upstreams = self._ordered_upstreams()

        for upstream in ordered_upstreams:
            connection = await self._try_acquire(upstream)
            if connection is not None:
                return connection

        return await self._wait_and_acquire(ordered_upstreams[0])

    async def release(self, connection: UpstreamConnection, reusable: bool) -> None:
        state = self._states[self.get_upstream_key(connection.upstream)]

        if reusable and connection.is_usable():
            async with state.condition:
                state.idle_connections.append(connection)
                state.condition.notify()
            return

        async with state.condition:
            state.open_connections -= 1
            state.condition.notify()

        await self._close_connection(connection)

    async def _close_connection(self, connection: UpstreamConnection) -> None:
        try:
            connection.writer.close()
            await connection.writer.wait_closed()
        except Exception as exc:
            logger.error(f"Upstream close error: {exc}")

    def stats_snapshot(self):
        snapshot = {}
        for upstream in self.upstreams:
            key = self.get_upstream_key(upstream)
            state = self._states[key]
            snapshot[f"{key[0]}:{key[1]}"] = {
                "open_connections": state.open_connections,
                "idle_connections": len(state.idle_connections),
                "max_connections": state.max_connections,
            }
        return snapshot
