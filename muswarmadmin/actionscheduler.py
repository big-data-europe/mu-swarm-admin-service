import asyncio
import logging


logger = logging.getLogger(__name__)


class StopScheduler(Exception):
    """
    Exception raised in the execution of an action to stop a scheduler
    """
    pass


class ActionScheduler:
    """
    The action scheduler is a way to get background task execution in the event
    loop executed serially. The actions are grouped by a key given when
    enqueueing the action. Every action is then executed one by one until the
    end of its queue
    """
    executers = {}

    @classmethod
    async def execute(cls, key, action, args, loop=None):
        """
        Enqueue an action to an executer, create one if it doesn't exist
        """
        if key not in cls.executers:
            cls.executers[key] = cls(key, loop=loop)
        await cls.executers[key].enqueue(action, args)

    @classmethod
    async def graceful_cancel(cls):
        """
        Gracefully await for all the actions of all the queues to be executed
        and finished then remove all the executers
        """
        logger.debug("Gracefully cancelling all action schedulers...")
        for executer in list(cls.executers.values()):
            await executer.cancel()

    def __init__(self, name, loop=None):
        """
        Create an ActionScheduler instance, start a background task that will
        consume the queue continuously one by one
        """
        logger.debug("Registering new action scheduler %s", name)
        self.name = name
        self.loop = asyncio.get_event_loop() if loop is None else loop
        self.queue = asyncio.Queue()
        self.executer = self.loop.create_task(self.executer())

    async def executer(self):
        """
        The background task that execute the actions one by one
        """
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
            del type(self).executers[self.name]
        except asyncio.CancelledError:
            pass
        finally:
            logger.debug("Executer %s is finished", self.name)

    async def cancel(self):
        """
        Wait for the queue to be empty then remove the background task and the
        ActionScheduler itself
        """
        logger.debug("Cancelling action scheduler %s...", self.name)
        await self.queue.join()
        self.executer.cancel()
        await self.executer
        if self.name in type(self).executers:
            del type(self).executers[self.name]

    async def enqueue(self, action, args):
        """
        Enqueue an action with arguments to this ActionScheduler
        """
        logger.debug("Enqueue action %r with args: %r", action, args)
        await self.queue.put((action, args))


class OneActionScheduler(ActionScheduler):
    """
    Same as the ActionScheduler but allows only one action in the queue: all
    new action added to the queue will be automatically discarded. An action
    can be queued while the scheduler is still executing the previous action
    """
    executers = {}

    async def enqueue(self, action, args):
        if not self.queue.empty():
            logger.debug("Ignore action %r with args: %r", action, args)
            return
        await super(OneActionScheduler, self).enqueue(action, args)
