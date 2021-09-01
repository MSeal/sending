"""Logging configuration"""
import logging
import os

logger = logging.getLogger("sending")

if os.environ.get("SENDING__ENABLE_LOGGING"):
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
