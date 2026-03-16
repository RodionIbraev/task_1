import asyncio
from loguru import logger


async def safe_close(writer):
    if writer is None:
        return

    try:
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        logger.error(f"Close error: {e}")


class ClientHandler:
    def __init__(self, timeout_policy, upstream_pool):
        self.timeout_policy = timeout_policy
        self.upstream_pool = upstream_pool

    async def handle_client(self, client_reader, client_writer):
        client_connection = client_writer.get_extra_info("peername")
        logger.info(f"Start serving {client_connection}")

        up_writer = None
        upstream = None

        try:
            upstream, up_reader, up_writer = await self.timeout_policy.connect(
                self.upstream_pool.open_connection()
            )
            logger.info("Connected to upstream")

            raw_request = await self.timeout_policy.read(
                ClientConnectionHandler.parse_http_request(client_reader)
            )

            up_writer.write(raw_request)
            await self.timeout_policy.write(up_writer.drain())

            logger.info("Start proxy streaming")

            await self.timeout_policy.total(
                asyncio.gather(
                    ClientConnectionHandler.pipe(client_reader, up_writer, self.timeout_policy),
                    ClientConnectionHandler.pipe(up_reader, client_writer, self.timeout_policy),
                )
            )

        except Exception as e:
            logger.error(f"Proxy error: {e}")

        finally:
            logger.info("Connection closed")
            await safe_close(up_writer)
            await safe_close(client_writer)
            if upstream is not None:
                self.upstream_pool.release(upstream)


class ClientConnectionHandler:

    @staticmethod
    async def parse_http_request(reader):
        request_line = await reader.readline()

        headers = []
        raw_headers = []

        while True:
            line = await reader.readline()

            if line == b"\r\n" or not line:
                break

            headers.append(line.decode().strip())
            raw_headers.append(line)

        logger.info(f"Start-line: {request_line}")
        logger.info(f"Headers: {headers}")

        raw_request = request_line + b"".join(raw_headers) + b"\r\n"
        return raw_request

    @staticmethod
    async def pipe(reader, writer, timeout_policy):
        try:
            while True:
                chunk = await timeout_policy.read(reader.read(1024))

                if not chunk:
                    if writer.can_write_eof():
                        writer.write_eof()
                    break

                writer.write(chunk)
                await timeout_policy.write(writer.drain())

        except asyncio.CancelledError:
            raise

        except Exception as e:
            logger.exception(f"Pipe failed: {e}")
            raise
