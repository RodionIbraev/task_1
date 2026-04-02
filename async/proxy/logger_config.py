from loguru import logger

from config import project_config


def setup_logger():
    logger.remove()
    logger.add("proxy_server.log", level=project_config["logging"]["level"], enqueue=True)
