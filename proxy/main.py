import asyncio

from client_handler import ClientHandler
from config import TimeoutPolicy, project_config
from logger_config import setup_logger
from proxy_server import ProxyServer
from upstream_pool import UpstreamPool


if __name__ == "__main__":
    setup_logger()
    timeout_policy = TimeoutPolicy()
    upstream_pool = UpstreamPool(
        upstreams=project_config["upstreams"],
        max_conns_per_upstream=project_config["limits"]["max_conns_per_upstream"],
    )
    client_handler = ClientHandler(timeout_policy, upstream_pool)

    asyncio.run(
        ProxyServer.run_server(
            project_config["listen"]["host"],
            project_config["listen"]["port"],
            project_config["limits"]["max_client_conns"],
            client_handler
        )
    )
