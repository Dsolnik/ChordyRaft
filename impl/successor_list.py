import config
import math
from typing import NamedTuple, Dict, Optional
import asyncio

from message import Message
from async_scheduler import AsyncScheduler, Handler, AsyncJob
from message import MessageSender, Message, MessageType
from key_transferer import KeyTransferer
from persistant_key_value_store import PersistantKeyValueStore
from heartbeater import HeartBeater
from utils import trace_log, hash_key, debug_log, addr_later, in_interval, add_addr, Interval, opposite_interval
"""
This is a class to manage the successor list. It worked with replication. This made the system possibly lose data.
Instead, we don't do this.
"""

log_successor_list = True 
def debug_log_successor_list(*args):
    if log_successor_list:
        debug_log(*args)

class SuccessorList:
    def __init__(self, addr: int, async_scheduler: AsyncScheduler, message_sender: MessageSender, persistent_storage: PersistantKeyValueStore, key_transferer: KeyTransferer, replicate_data_to_successors = True):
        self.async_scheduler = async_scheduler
        self.message_sender = message_sender
        self.key_transferer = key_transferer
        self.persistent_storage = persistent_storage
        self.addr = addr
        self.router = None 
        self.replicate_data_to_successors = replicate_data_to_successors
        self.successor_list = [None] * config.NUM_SUCCESSORS_IN_SUCCESSOR_LIST
        self.async_scheduler.register_handler(Handler(lambda msg: msg.has_type(MessageType.R_U_THERE), self.handle_r_u_there))
        self.async_scheduler.register_handler(Handler(lambda msg: msg.has_type(MessageType.SET_VALUE_COPY), self.handle_set_value_copy))
    
    def register_router(self, router):
        """
        Register a router
        """
        self.router = router

    def startup(self):
        """
        Startup the montior that stabalizes the successor list.
        """
        trace_log('SuccessorList.Startup', self.addr, 'starting up.')
        self.async_scheduler.schedule_monitor_job(self.stabalize_successor_list, config.MONITOR_TIME)
        self.async_scheduler.schedule_monitor_job(self.stabalize_predecessor, config.MONITOR_TIME)

    # Helps for sending messages
    async def send_message_and_await(self, msg: Message, activation_condition):
        """
        send a message and await a response
        """
        fut = self.async_scheduler.get_message_wait_future(activation_condition)
        self.message_sender.send_msg(msg)
        return await fut

    async def send_message_and_await_with_timeout(self, msg: Message, activation_condition, timeout: int):
        """
        Send a message and await response with timeout.
        """
        fut = self.async_scheduler.wait_for_with_timeout(activation_condition, timeout)
        self.message_sender.send_msg(msg)
        return await fut
    
    async def try_twice_send_message(self, msg: Message, activation_condition):
        """
        Try to send the message twice with timeout of config.TIMEOUT_TIME
        """
        try_one_msg = await self.send_message_and_await_with_timeout(msg, activation_condition, config.TIMEOUT_TIME)
        if (try_one_msg == None):
            return await self.send_message_and_await_with_timeout(msg, activation_condition, config.TIMEOUT_TIME)
        return try_one_msg
    
    # Helpers for communicating with nodes
    async def get_successor(self, node_id: int):
        """
        Ask the node for its successor 
        """
        msg_to_send = self.message_sender.create_message(MessageType.FIND_SUCCESSOR, node_id)
        find_successor_message : Message = await self.send_message_and_await(msg_to_send, lambda msg: msg.has_type(MessageType.FIND_SUCCESSOR_REPLY) and msg.src == node_id)
        assert ('successor' in find_successor_message.args)
        return find_successor_message.args['successor']

    async def set_value(self, node_id: int, key: str, value: str):
        """
        Set the value on some node
        """
        set_msg = self.message_sender.create_message(MessageType.SET_VALUE_COPY, node_id, { 'key': key, 'value' : value })
        set_msg_res = await self.send_message_and_await(set_msg, lambda msg: msg.has_type(MessageType.SET_VALUE_COPY_REPLY) and msg.src == node_id 
                                                                    and ('key' in msg.args and 'value' in msg.args and msg.args['key'] == key and msg.args['value'] == value))
        return set_msg_res

    async def is_alive(self, node_id: int):
        """
        See if a node is alive.
        Eventually completes
        """
        msg_to_send = self.message_sender.create_message(MessageType.R_U_THERE, node_id)
        msg = await self.try_twice_send_message(msg_to_send,  lambda msg: msg.has_type(MessageType.I_M_HERE) and msg.src == node_id)
        return msg != None

    # Maintaining the successor list
    async def prune_start_of_successor_list(self):
        """
        Prunes the start of the successor list for nodes that have died.
        """
        trace_log('SuccessorList.prune_start_of_sucessor_list', self.addr, ':Starting to stabalize.')
        i = 0
        while i < config.NUM_SUCCESSORS_IN_SUCCESSOR_LIST:
            # Run this NUM_SUCCESSORS_IN_SUCCESSOR_LIST times
            successor = self.successor_list[0]
            if successor == None:
                return 
            # If there are no successors in the list, we return None.
            trace_log('SuccessorList.prune_start_of_sucessor_list', self.addr, ':Checking if',successor, 'is alive')
            is_alive = await self.is_alive(successor)
            trace_log('SuccessorList.prune_start_of_sucessor_list', self.addr, ':Checking if',successor, 'is alive', is_alive)
            # If the node is alive, we have pruned!
            if is_alive:
                return 
            # The node is not alive, so we shift the successor list over one
            self.successor_list = self.successor_list[1:] + [None]
    
    async def replicate_our_data_to(self, nodes):
        for new_successor in nodes:
            if new_successor != None:
                if self.router.predecessor == None:
                    debug_log('SuccessorList.Stabalize', self.addr,':new successor so Transferring keys', new_successor)
                    await self.key_transferer.copy_to(new_successor, Interval(self.router.successor, False, self.addr, True))
                else:
                    debug_log('SuccessorList.Stabalize', self.addr,':new successor so Transferring keys', new_successor)
                    await self.key_transferer.copy_to(new_successor, Interval(self.router.predecessor, False, self.addr, True))

    async def stabalize_predecessor(self):
        if self.router.predecessor != None:
            alive = await self.is_alive(self.router.predecessor)
            if not alive:
                self.router.predecessor = None

    async def stabalize_successor_list(self):
        """
        Maintains the successor_list
        """
        # prune the start of the list for dead entries.
        trace_log('SuccessorList.stabalize', self.addr, ':Starting to stabalize.')
        await self.prune_start_of_successor_list()
        old_successors = set(self.successor_list)
        # If no successor alive, we are our own successor.
        if self.successor_list[0] == None:
            self.router.successor = self.addr
        else:
            # refresh the list.
            await self.refresh_successor_list()
            self.router.successor = self.successor_list[0]

        # We always need to get to this point. We cannot block before getting here otherwise the successor pointers will not
        #   eventually be right.

        # Copy data to all new successors 
        if self.replicate_data_to_successors:
            await self.replicate_our_data_to(set(self.successor_list).difference(old_successors))

    async def replicate_set_to_successors(self, key:str, value:str):
        """
        Replicates the set of key, value to all successors
        Returns True if successful or False is not able to replicate to all successors.
        """
        set_on_all_nodes = True
        for node in self.successor_list:
            if node != None:
                value_set = await self.set_value(node, key, value)
                set_on_all_nodes = set_on_all_nodes and value_set != None
        return set_on_all_nodes 

    async def refresh_successor_list(self):
        """
        Refreshes successors s.t. self.successor_list[i].successor = self.successor_list[i + 1]
        Assumes self.successor_list[0] is up.
        Eventually completes.
        Returns True if successful, False if not successful.
        """
        debug_log_successor_list('SuccessorList.refresh_successor_list', self.message_sender.addr,'Starting', self.successor_list)
        i = 1
        while i < config.NUM_SUCCESSORS_IN_SUCCESSOR_LIST:
            # Assume the previous successor is not None, so we can get our successor
            previous_success = self.successor_list[i - 1]
            assert(previous_success != None)
            msg_to_send = self.message_sender.create_message(MessageType.FIND_SUCCESSOR, previous_success)
            new_successor_msg = await self.try_twice_send_message(msg_to_send, lambda msg: msg.has_type(MessageType.FIND_SUCCESSOR_REPLY) and msg.src == previous_success)
            if (new_successor_msg == None):
                # Somebody's successor is not responsive. We failed. We need to wait for the system to stabalize and try again.
                debug_log_successor_list('SuccessorList.refresh_successor_list',self.message_sender.addr,'Failure. Setting', i,'got no response from', previous_success, self.successor_list)
                self.successor_list[i - 1] = None
                return
            else: 
                new_successor = new_successor_msg.get_args(['successor'])
                debug_log_successor_list('SuccessorList.refresh_successor_list',self.message_sender.addr,'setting', i,'found a sucessor', new_successor)
                # If we have gone in a loop, we stop.
                if new_successor == self.addr or new_successor in self.successor_list:
                    debug_log_successor_list('SuccessorList.refresh_successor_list',self.message_sender.addr,' finished Successfully. Found loop at index', i, self.successor_list)
                    return
                # We found the proper successor.
                self.successor_list[i] = new_successor
                i += 1
        debug_log_successor_list('SuccessorList.refresh_successor_list',self.message_sender.addr,' finished Successfully. Refreshed all entries.', self.successor_list)

    async def update_successor_list_new_successor(self, successor: int):
        """
        Updates the successor list with a new successor
        """
        if self.successor_list[0] != None:
            # Assert this actually is a new successor.
            assert(addr_later(self.addr, self.successor_list[0], successor))

        if successor == self.addr:
            return
        
        old_successors = set(self.successor_list)
        self.successor_list[0] = successor
        await self.refresh_successor_list()
        await self.replicate_our_data_to(set(self.successor_list).difference(old_successors))


    async def initialize_successor_list(self, successor: int):
        """
        Initialize the successor list
        """
        if successor == self.addr:
            return

        self.successor_list[0] = successor
        await self.refresh_successor_list()
        await self.replicate_our_data_to(self.successor_list)

    # Handlers
    async def handle_r_u_there(self, msg: Message):
        """
        Respond to R_U_THERE with I_M_HERE
        """
        i_am_here = self.message_sender.create_message(MessageType.I_M_HERE, msg.src)
        self.message_sender.send_msg(i_am_here)

    async def handle_set_value_copy(self, msg: Message):
        """
        Set the value on some node
        """
        key, value = msg.get_args(['key', 'value'])
        self.persistent_storage.set(key, value)
        self.message_sender.create_and_send_message(MessageType.SET_VALUE_COPY_REPLY, msg.src, {'key' : key, 'value': value})

