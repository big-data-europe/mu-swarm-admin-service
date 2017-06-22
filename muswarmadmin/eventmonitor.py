import asyncio
import logging


logger = logging.getLogger(__name__)


async def event_monitor(docker, handlers):
    try:
        logger.debug("Event monitor started")
        async for event in docker.events(decode=True):
            if event["Type"] in handlers:
                for handler in handlers[event["Type"]]:
                    try:
                        await handler(event)
                    except Exception:
                        logger.exception("Event handler %r failed", handler)
    except asyncio.CancelledError:
        pass
    finally:
        logger.debug("Event monitor stopped")
