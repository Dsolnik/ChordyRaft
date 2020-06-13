from collections import namedtuple
from typing import NamedTuple, Callable, Any, Dict, List
from async_scheduler import AsyncScheduler, Handler, AsyncJob
from enum import Enum
from utils import addr_later, hash_key, trace_log, debug_log, debug_test
import asyncio

import colorama

import config
from persistant_key_value_store import PersistantKeyValueStore
from message import Message, MessageType, MessageSender 
from raft_node import RaftNode, Role
"""
A PersistentStorage class that uses a Raft Cluster
"""
# These are keys that are reserved for storing the state of the chord node. 
reserved_keys = set(["successor", "predecessor"] + [f"finger_table[{i}]" for i in range(config.FINGER_TABLE_BITS)])
class RaftPersistentKeyValueStore:
    """
    Persistent Storage that stores data on a RAFT cluster.
    """
    def __init__(self, 
                addr: str,
                peers: List[int],
                async_scheduler: AsyncScheduler,
                message_sender: MessageSender,
                on_become_leader, 
                on_step_down):
        trace_log(colorama.Fore.LIGHTRED_EX + "RaftPersistentKeyValueStore", addr, "starting up")
        self.addr = addr 
        self.persistent_storage = PersistantKeyValueStore()
        self.raft_node = RaftNode(addr, peers, self.persistent_storage, async_scheduler, message_sender, on_become_leader, on_step_down)

    async def startup(self):
        """
        Startup the Raft node to connect to the cluster.
        """
        await self.raft_node.startup()
    
    async def get(self, key):
        """
        Get the key from the cluster.
        """
        assert(self.is_leader())
        debug_test("Raft", self.addr, ": Getting key", key)
        value_to_return = await self.raft_node.get_value(key)
        return value_to_return

    async def set(self, key, value):
        """
        Set the key to value in the cluster.
        """
        assert(self.is_leader())
        debug_log("RaftPersistentKeyValueStore", self.addr, key, value)
        await self.raft_node.set_value(key, value)
    
    async def get_keys(self):
        """
        Get all the non reserved keys in the cluster.
        """
        keys = await self.persistent_storage.get_keys()
        return [key for key in keys if key not in reserved_keys]
    
    async def add_dict(self, dic):
        """
        Add a dict to the cluster.
        """
        await self.persistent_storage.add_dict(dic)

    def is_leader(self):
        """
        Get if we are leader of the cluster.
        """
        return self.raft_node.role == Role.LEADER.value