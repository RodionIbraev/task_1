import asyncio


class ProxyServer:
    @classmethod
    async def run_server(cls, host, port, max_client_conns, client_handler):
        client_semaphore = asyncio.Semaphore(max_client_conns)

        async def limited_handle_client(reader, writer):
            async with client_semaphore:
                await client_handler.handle_client(reader, writer)

        server = await asyncio.start_server(
            limited_handle_client, host, port
        )

        async with server:
            await server.serve_forever()
