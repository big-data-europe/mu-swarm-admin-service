import asyncio
import logging


logger = logging.getLogger(__name__)


class StopScheduler(Exception):
    pass


class ActionScheduler:
    executers = {}

    @classmethod
    async def execute(cls, key, action, args, loop=None):
        if key not in cls.executers:
            cls.executers[key] = cls(key, loop=loop)
        await cls.executers[key].enqueue(action, args)

    @classmethod
    async def graceful_cancel(cls):
        logger.debug("Gracefully cancelling all action schedulers...")
        for executer in list(cls.executers.values()):
            await executer.cancel()

    def __init__(self, name, loop=None):
        logger.debug("Registering new action scheduler %s", name)
        self.name = name
        self.loop = asyncio.get_event_loop() if loop is None else loop
        self.queue = asyncio.Queue()
        self.executer = self.loop.create_task(self.executer())

    async def executer(self):
        try:
            while True:
                action, args = await self.queue.get()
                logger.debug("Executer %s: running action %r with args: %r",
                             self.name, action, args)
                try:
                    await action(*args)
                except StopScheduler:
                    raise
                except Exception:
                    logger.exception("Action %r with arguments %r failed",
                                     action, args)
                finally:
                    self.queue.task_done()
        except StopScheduler:
            del ActionScheduler.executers[self.name]
        except asyncio.CancelledError:
            pass
        finally:
            logger.debug("Executer %s is finished", self.name)

    async def cancel(self):
        logger.debug("Cancelling action scheduler %s...", self.name)
        await self.queue.join()
        self.executer.cancel()
        await self.executer
        del ActionScheduler.executers[self.name]

    async def enqueue(self, action, args):
        logger.debug("Enqueue action %r with args: %r", action, args)
        await self.queue.put((action, args))


class OneActionScheduler(ActionScheduler):
    executers = {}

    async def enqueue(self, action, args):
        if not self.queue.empty():
            logger.debug("Ignore action %r with args: %r", action, args)
            return
        await super(OneActionScheduler, self).enqueue(action, args)
