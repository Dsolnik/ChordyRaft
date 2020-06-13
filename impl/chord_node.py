from collections import namedtuple
from typing import NamedTuple, Callable, Any, Dict
from async_scheduler import AsyncScheduler, Handler, AsyncJob
from enum import Enum
from utils import trace_log, addr_later, hash_key, debug_log, debug_test
import asyncio

import config
from succ_router import SuccRouter
from finger_table_router import FingerTableRouter
from persistant_key_value_store import PersistantKeyValueStore
from message import Message, MessageType, MessageSender
from key_transferer import KeyTransferer
from successor_list import SuccessorList
"""
This file defines a ChordNode, which is a node that represents an addr in the Chord Node space.
"""
class ChordNode: 
    """
    A Node in the Chord Ring.
    """
    def __init__(self, 
                addr: int, 
                another_node: int, 
                persistent_storage: PersistantKeyValueStore, 
                async_scheduler: AsyncScheduler, 
                message_sender: MessageSender, 
                key_transferer: KeyTransferer):
        self.alive = True
        trace_log('ChordNode.__init__: Starting Node addr: ', addr, ' with relay-node ', another_node)
        
        self.chord_addr = addr

        self.router = FingerTableRouter(addr, async_scheduler, message_sender, key_transferer, persistent_storage)
        self.persistent_storage = persistent_storage
        self.async_scheduler = async_scheduler
        self.message_sender = message_sender
        self.another_node = another_node
        self.key_transferer = key_transferer

        # Register the handlers
        self.handlers = [
            async_scheduler.register_handler(Handler(lambda msg: msg.message_type == MessageType.GET.value, self.try_twice_get))
            ,async_scheduler.register_handler(Handler(lambda msg: msg.message_type == MessageType.SET.value, self.try_twice_set))
            ,async_scheduler.register_handler(Handler(lambda msg: msg.message_type == MessageType.GET_VALUE.value, self.handle_get_value))
            ,async_scheduler.register_handler(Handler(lambda msg: msg.message_type == MessageType.SET_VALUE.value, self.handle_set_value))]
        trace_log('ChordNode.__init__: Done Starting Node addr: ', addr, ' with relay-node ', another_node)

    async def startup(self):
        """
        Startup a chord node. Here, the Event Loop from async.io is already set up.
        """
        # Startup a chord node.
        debug_log("ChordNode.startup", self.chord_addr)
        await self.router.startup(self.another_node)

    def hard_shutdown(self):
        """
        Shutdown a chord node. This cancels every handler and shuts down the router, deregistering all handlers.
        """
        if self.alive:
            self.alive = not self.alive
            for cancel_handler in self.handlers:
                cancel_handler()
            self.router.shutdown()
    
    def soft_shutdown(self):
        """
        No longer be alive
        """
        self.alive = False
    
    def recover(self):
        self.alive = True

    # UTILITIES 
    # ASYNC UTILITIES
    async def send_message_and_await(self, msg: Message, activation_condition):
        """
        Send `msg` and block until a msg which fulfills `activation_condition` holds, returning that msg 
        """
        fut = self.async_scheduler.get_message_wait_future(activation_condition)
        self.send_msg(msg)
        return await fut

    async def send_message_and_await_with_timeout(self, msg: Message, activation_condition, timeout: int):
        """
        Send a message and await response with timeout.
        """
        fut = self.async_scheduler.wait_for_with_timeout(activation_condition, timeout)
        self.message_sender.send_msg(msg)
        return await fut
    
    # HANDLERS 
    # NOTE: Handlers must be async functions (AsyncScheduler requires it)
    async def handle_set_value(self, msg: Message):
        """
        Handle a request from another node to set the value of a key in persistent storage
        """
        assert ('key' in msg.args and 'value' in msg.args)
        key, value = msg.args['key'], msg.args['value']
        debug_test("Chord", self.chord_addr,": setting ", key, "to value", value)
        await self.persistent_storage.set(key, value)
        debug_test("Chord", self.chord_addr,": setting ", key, "to value", value, "complete")
        # if config.USING_SUCCESSOR_LIST_REPL:
        #     self.successor_list.replicate_set_to_successors(key, value)
        self.message_sender.create_and_send_message(MessageType.SET_VALUE_REPLY, msg.src, {'key' : key, 'value': value})

    async def handle_get_value(self, msg: Message):
        """
        Handle a request to get the value of a key in persistent storage
        """
        assert ('key' in msg.args)
        src_node_addr, key = msg.src, msg.args['key']
        value = await self.persistent_storage.get(key)
        self.message_sender.create_and_send_message(MessageType.GET_VALUE_REPLY, src_node_addr, {'key': key, 'value' : value})
    
    async def keep_resending_until_response(self, msg: Message, activation_condition):
        while True:
            msg = await self.send_message_and_await_with_timeout(msg, activation_condition, config.CHORD_RETRY_TIME)
            if msg != None:
                return msg

    async def try_twice_set(self, msg: Message):
        """
        Try to execute the set twice.
        """
        task = asyncio.create_task(self.handle_set(msg))
        done, not_done = await asyncio.wait([task], timeout=30)
        if len(done) == 0:
            for n in not_done:
                n.cancel()
            done, not_done = await asyncio.wait([task], timeout=30)
        req_id = msg.args['id']
        self.message_sender.create_and_send_message(MessageType.SET_RESPONSE_TO_BROKER, 0, {'id' : req_id, 'error': 'timed_out'})
        
    async def try_twice_get(self, msg: Message):
        """
        Try to execute the get twice.
        """
        task = asyncio.create_task(self.handle_get(msg))
        done, not_done = await asyncio.wait([task], timeout=30)
        if len(done) == 0:
            for n in not_done:
                n.cancel()
            done, not_done = await asyncio.wait([task], timeout=30)
        req_id = msg.args['id']
        self.message_sender.create_and_send_message(MessageType.GET_RESPONSE_TO_BROKER, 0, {'id' : req_id, 'error': 'timed_out'})
        
    async def handle_set(self, msg: Message):
        """
        Handle a client request to set the value of a key among the chord network.
        """
        if not self.alive:
            return
        # Set the key
        assert ('key' in msg.args and 'value' in msg.args and 'id' in msg.args)
        key = msg.args['key']
        req_id = msg.args['id']
        value = msg.args['value']
        debug_test("Chord", self.chord_addr,": set request key", key, "to value", value, "with hash", hash_key(key))
        
        node_responsible_addr = await self.router.find_node_responsible_for_key(key) 
        debug_test("Chord", self.chord_addr,": found node responsible for", key, node_responsible_addr)

        if (node_responsible_addr == self.chord_addr):
            debug_test("Chord", self.chord_addr,": setting ", key, "to value", value)
            await self.persistent_storage.set(key, value)
            debug_test("Chord", self.chord_addr,": setting ", key, "to value", value, "complete")
            # if config.USING_SUCCESSOR_LIST_REPL:
            #     await self.successor_list.replicate_set_to_successors(key, value)
        else:
            set_msg = self.message_sender.create_message(MessageType.SET_VALUE, node_responsible_addr, { 'key': key, 'value' : value })
            set_msg_res = await self.keep_resending_until_response(set_msg, lambda msg: msg.has_type(MessageType.SET_VALUE_REPLY) and msg.src == node_responsible_addr)
            assert('key' in set_msg_res.args and 'value' in set_msg_res.args)
            assert(set_msg_res.args['key'] == key and set_msg_res.args['value'] == value)
        self.message_sender.create_and_send_message(MessageType.SET_RESPONSE_TO_BROKER, node_responsible_addr, {'id' : req_id, 'key': str(key), 'value' : value})
    
    async def handle_get(self, msg: Message):
        """
        Handle a client request to get the value of a key among the chord network.
        """
        if not self.alive:
            return
        trace_log('ChordNode: Running get handler')
        assert ('key' in msg.args and 'id' in msg.args)
        req_id = msg.args['id']
        key = msg.args['key']

        node_responsible_addr = await self.router.find_node_responsible_for_key(key) 
        debug_test("Chord", self.chord_addr,": found node responsible for", key, node_responsible_addr)

        if (node_responsible_addr == self.chord_addr):
            value = await self.persistent_storage.get(key)
        else:
            get_value_msg = self.message_sender.create_message(MessageType.GET_VALUE, node_responsible_addr, {'key' : key})
            get_value_reply = await self.keep_resending_until_response(get_value_msg, lambda msg: msg.message_type == MessageType.GET_VALUE_REPLY.value and msg.src == node_responsible_addr)
            assert ('key' in get_value_reply.args and 'value' in get_value_reply.args)
            assert (get_value_reply.args['key'] == key)
            value = get_value_reply.args['value']

        self.message_sender.create_and_send_message(MessageType.GET_RESPONSE_TO_BROKER, None, {'id' : req_id, 'key': str(key), 'value' : value})
        