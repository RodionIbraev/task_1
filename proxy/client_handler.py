import asyncio
import socket
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from loguru import logger


async def safe_close(writer):
    if writer is None:
        return

    try:
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        logger.error(f"Close error: {e}")


def enable_tcp_keepalive(writer):
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


@dataclass
class MessageHead:
    start_line: bytes
    raw_header_lines: List[bytes]
    headers: Dict[str, str]

    def header(self, name: str, default: Optional[str] = None) -> Optional[str]:
        return self.headers.get(name.lower(), default)

    def serialized(self) -> bytes:
        filtered_header_lines = []

        for raw_line in self.raw_header_lines:
            decoded = raw_line.decode("latin1").rstrip("\r\n")
            header_name = decoded.split(":", 1)[0].strip().lower()
            if header_name == "proxy-connection":
                continue
            filtered_header_lines.append(raw_line)

        return self.start_line + b"".join(filtered_header_lines) + b"\r\n"


class HttpProtocol:
    @staticmethod
    async def read_head(reader, timeout_policy) -> Optional[MessageHead]:
        while True:
            start_line = await timeout_policy.read(reader.readline())
            if not start_line:
                return None
            if start_line != b"\r\n":
                break

        raw_header_lines: List[bytes] = []
        headers: Dict[str, str] = {}

        while True:
            line = await timeout_policy.read(reader.readline())
            if line in {b"", b"\r\n"}:
                break

            raw_header_lines.append(line)
            decoded = line.decode("latin1").rstrip("\r\n")
            name, _, value = decoded.partition(":")
            headers[name.strip().lower()] = value.strip()

        return MessageHead(
            start_line=start_line,
            raw_header_lines=raw_header_lines,
            headers=headers,
        )

    @staticmethod
    def parse_request_line(start_line: bytes) -> Tuple[str, str, str]:
        method, target, version = start_line.decode("latin1").rstrip("\r\n").split(" ", 2)
        return method, target, version

    @staticmethod
    def parse_status_line(start_line: bytes) -> Tuple[str, int, str]:
        version, status_code, reason = start_line.decode("latin1").rstrip("\r\n").split(" ", 2)
        return version, int(status_code), reason

    @staticmethod
    def should_keep_alive(version: str, connection_header: Optional[str]) -> bool:
        connection_value = (connection_header or "").lower()

        if version == "HTTP/1.1":
            return connection_value != "close"

        return connection_value == "keep-alive"

    @staticmethod
    def has_chunked_encoding(headers: MessageHead) -> bool:
        transfer_encoding = headers.header("transfer-encoding", "")
        return "chunked" in transfer_encoding.lower()

    @staticmethod
    def content_length(headers: MessageHead) -> Optional[int]:
        raw_value = headers.header("content-length")
        if raw_value is None:
            return None
        return int(raw_value)

    @staticmethod
    def request_has_body(headers: MessageHead) -> bool:
        if HttpProtocol.has_chunked_encoding(headers):
            return True

        content_length = HttpProtocol.content_length(headers)
        return content_length is not None and content_length > 0

    @staticmethod
    def response_has_body(method: str, status_code: int) -> bool:
        if method.upper() == "HEAD":
            return False
        if 100 <= status_code < 200:
            return False
        return status_code not in {204, 304}

    @staticmethod
    async def relay_exactly(reader, writer, timeout_policy, byte_count: int) -> None:
        remaining = byte_count
        while remaining > 0:
            chunk = await timeout_policy.read(reader.read(min(65536, remaining)))
            if not chunk:
                raise ConnectionError("Unexpected EOF while reading fixed-size body")
            writer.write(chunk)
            await timeout_policy.write(writer.drain())
            remaining -= len(chunk)

    @staticmethod
    async def relay_chunked_body(reader, writer, timeout_policy) -> None:
        while True:
            chunk_size_line = await timeout_policy.read(reader.readline())
            if not chunk_size_line:
                raise ConnectionError("Unexpected EOF while reading chunk size")

            writer.write(chunk_size_line)
            await timeout_policy.write(writer.drain())

            chunk_size_token = chunk_size_line.split(b";", 1)[0].strip()
            chunk_size = int(chunk_size_token, 16)

            if chunk_size == 0:
                while True:
                    trailer_line = await timeout_policy.read(reader.readline())
                    if not trailer_line:
                        raise ConnectionError("Unexpected EOF while reading chunk trailers")
                    writer.write(trailer_line)
                    await timeout_policy.write(writer.drain())
                    if trailer_line == b"\r\n":
                        return
            else:
                await HttpProtocol.relay_exactly(reader, writer, timeout_policy, chunk_size + 2)

    @staticmethod
    async def relay_message_body(
        reader,
        writer,
        timeout_policy,
        headers: MessageHead,
        *,
        body_expected: bool,
    ) -> str:
        if not body_expected:
            return "none"

        if HttpProtocol.has_chunked_encoding(headers):
            await HttpProtocol.relay_chunked_body(reader, writer, timeout_policy)
            return "chunked"

        content_length = HttpProtocol.content_length(headers)
        if content_length is not None:
            if content_length > 0:
                await HttpProtocol.relay_exactly(reader, writer, timeout_policy, content_length)
            return "fixed"

        return "until_eof"

    @staticmethod
    async def relay_until_eof(reader, writer, timeout_policy) -> None:
        while True:
            chunk = await timeout_policy.read(reader.read(65536))
            if not chunk:
                return
            writer.write(chunk)
            await timeout_policy.write(writer.drain())


class ClientHandler:
    def __init__(self, timeout_policy, upstream_pool):
        self.timeout_policy = timeout_policy
        self.upstream_pool = upstream_pool

    async def handle_client(self, client_reader, client_writer):
        client_connection = client_writer.get_extra_info("peername")
        enable_tcp_keepalive(client_writer)
        logger.info(f"Start serving {client_connection}")

        try:
            while True:
                should_continue = await self.timeout_policy.total(
                    self._serve_single_request(client_reader, client_writer)
                )
                if not should_continue:
                    break
        except asyncio.TimeoutError:
            logger.error(f"Connection timed out for {client_connection}")
        except Exception as e:
            logger.exception(f"Proxy error for {client_connection}: {e}")
        finally:
            logger.info(f"Connection closed for {client_connection}")
            await safe_close(client_writer)

    async def _serve_single_request(self, client_reader, client_writer) -> bool:
        request_head = await HttpProtocol.read_head(client_reader, self.timeout_policy)
        if request_head is None:
            return False

        method, target, request_version = HttpProtocol.parse_request_line(request_head.start_line)
        client_wants_keepalive = HttpProtocol.should_keep_alive(
            request_version,
            request_head.header("connection"),
        )

        upstream_connection = None
        upstream_reusable = False

        try:
            upstream_connection = await self.timeout_policy.connect(self.upstream_pool.acquire())
            up_reader = upstream_connection.reader
            up_writer = upstream_connection.writer

            up_writer.write(request_head.serialized())
            await self.timeout_policy.write(up_writer.drain())

            request_body_mode = await HttpProtocol.relay_message_body(
                client_reader,
                up_writer,
                self.timeout_policy,
                request_head,
                body_expected=HttpProtocol.request_has_body(request_head),
            )

            if request_body_mode == "until_eof":
                await HttpProtocol.relay_until_eof(client_reader, up_writer, self.timeout_policy)

            response_head = await HttpProtocol.read_head(up_reader, self.timeout_policy)
            if response_head is None:
                raise ConnectionError("Upstream closed connection before sending response head")

            response_version, status_code, _ = HttpProtocol.parse_status_line(response_head.start_line)
            upstream_allows_keepalive = HttpProtocol.should_keep_alive(
                response_version,
                response_head.header("connection"),
            )

            client_writer.write(response_head.serialized())
            await self.timeout_policy.write(client_writer.drain())

            response_body_mode = await HttpProtocol.relay_message_body(
                up_reader,
                client_writer,
                self.timeout_policy,
                response_head,
                body_expected=HttpProtocol.response_has_body(method, status_code),
            )

            if response_body_mode == "until_eof":
                await HttpProtocol.relay_until_eof(up_reader, client_writer, self.timeout_policy)

            keepalive_possible = (
                upstream_allows_keepalive
                and response_body_mode != "until_eof"
                and request_body_mode != "until_eof"
            )
            upstream_reusable = keepalive_possible

            return client_wants_keepalive and keepalive_possible
        finally:
            if upstream_connection is not None:
                await self.upstream_pool.release(upstream_connection, reusable=upstream_reusable)
