import yaml
import asyncio

with open("config.yaml", "r") as settings_file:
    config = yaml.safe_load(settings_file)

project_config = config


class TimeoutPolicy:
    def __init__(self):
        self.connect_sec = project_config["timeouts"]["connect_ms"] / 1000
        self.read_sec = project_config["timeouts"]["read_ms"] / 1000
        self.write_sec = project_config["timeouts"]["write_ms"] / 1000
        self.total_sec = project_config["timeouts"]["total_ms"] / 1000

    async def connect(self, awaitable):
        return await asyncio.wait_for(awaitable, self.connect_sec)

    async def read(self, awaitable):
        return await asyncio.wait_for(awaitable, self.read_sec)

    async def write(self, awaitable):
        return await asyncio.wait_for(awaitable, self.write_sec)

    async def total(self, awaitable):
        return await asyncio.wait_for(awaitable, self.total_sec)
