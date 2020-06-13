from message import Message
from typing import NamedTuple, Callable, Any, Dict
import asyncio
from enum import Enum
from utils import trace_log, trace_log_raft
"""
This file provides a nice abstraction to be able await a message being recieved.
For example, an async method can use
    `msg = await async_scheduler.get_message_wait_future(lambda msg: msg.src == 100)`
to block until a message from node `100` to arrive.
"""

class AsyncJob(NamedTuple):
    """
    Waiting for one message.
    When activation_condition holds for a msg, the future is resolved with msg (if future not canceled).
    """
    activation_condition: Callable[[Message], bool]
    future: Any # Future[Message]

class Handler(NamedTuple):
    """
    A handler for messages.
    When activation_condition holds for a msg, handler_method(msg) is called.
    """
    activation_condition: Callable[[Message], bool]
    handle_method: Any # Awaitable

class AsyncScheduler:
    def __init__(self):
        """
        init : Creates an Async Scheduler that manages various processes.
            Variables:
                waitingList : List of Jobs we are waiting for an activation
                            condition on.
                handlers    : Functions that are called when activation condition
                            is met
        """
        self.waitingList : [ AsyncJob ] = []
        self.handlers : [ Handler ] = []

    def deliver_message(self, msg: Message):
        """
        deliver_message :   Sends message to AsyncScheduler to Process
            Input(s):
                Message msg :   Message to be processed by Scheduler
            Output(s):
                None
            Side Effect(s):
                1. Sets result of jobs that were waiting in handler
                2. Runs Handler() for handlers triggered by message
        """
        if type(msg.src) is int:
            trace_fn = trace_log
        else:
            trace_fn = trace_log_raft
        trace_fn("AsyncScheduler: Delivering message", msg)
        to_remove = []
        for async_job in self.waitingList:
            if async_job.future.cancelled():
                to_remove.append(async_job)
            elif async_job.activation_condition(msg):
                to_remove.append(async_job)
                async_job.future.set_result(msg)

        for job in to_remove:
            self.waitingList.remove(job)

        for handler in self.handlers:
            if handler.activation_condition(msg):
                trace_fn("AsyncScheduler: Delivering message", msg)
                asyncio.create_task(handler.handle_method(msg))

    def register_job(self, job: AsyncJob):
        """
        register_job    :   Adds Job to Async Scheduler
            Input(s):
                AsyncJob job :   Job to be added to scheduler
            Output(s):
                None
            Side Effect(s):
                1. Adds job to waitingList
        """
        self.waitingList.append(job)

    def register_handler(self, handler: Handler):
        """
        register_handler        :   Adds Job to Async Scheduler
            Input(s):
                Handler handler :   Handler to be added to Scheduler
            Output(s):
                A function of 0 args that cancels the handler
            Side Effect(s):
                1. Adds job to handlers
        """
        self.handlers.append(handler)
        return lambda: self.handlers.remove(handler)

    def get_message_wait_future(self, acitvation_condition):
        """
        get_message_wait_future :   Returns a future (awaitable object) based on
                                    some activation condition.
            Input(s):
                activation_condition :   Condition for when result will be returned
            Output(s):
                Future fut  :   Awaitable Future() for result.
            Side Effect(s):
                1. Adds AsyncJob to waitingList for the future
        """
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.waitingList.append(AsyncJob(acitvation_condition, fut))
        return fut

    async def wait_for_with_timeout(self, activation_condition, timeout: int):
        """
        wait_for_with_timeout:   waits for some activation condition with a timeout
            Input(s):
                activation_condition :   Condition for when result will be returned
            Output(s):
                 Msg: Message, which matches activation_condition if found
                 None, if the request timed out
            Side Effect(s):
                1. Adds AsyncJob to waitingList for the future
        """
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.waitingList.append(AsyncJob(activation_condition, fut))
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return None

    def schedule_monitor_job(self, f: Callable, wait: float):
        """
        (Note: We may want to change this to have the periodic task stored
            in the handler as well. Allows us to better monitor a list of
            periodic tasks & if we want to shut all periodic tasks down,
            create 1 location to do so. Would make it more modular)
        schedule_monitor_job    :   Schedules some function to be called in some
                                    # of seconds. Returns method to cancel it
            Input(s):
                activation_condition :   Condition for when result will be returned
            Output(s):
                Cancelation Function for Task
            Side Effect(s):
                1. Adds periodic function to scheduler
        """
        async def periodic():
            while True:
                past_task = asyncio.create_task(f())
                await asyncio.sleep(wait)
                past_task.cancel()
        task = asyncio.create_task(periodic())
        return lambda: task.cancel()
