from collections import namedtuple
from typing import NamedTuple, Callable, Any, Dict, List
from async_scheduler import AsyncScheduler, Handler, AsyncJob
from enum import Enum
from utils import trace_log, addr_later, hash_key
import asyncio

import config
from persistant_key_value_store import PersistantKeyValueStore
from message import Message, MessageType, MessageSender
"""
A utility class that represents the Log in a Raft Node
"""
class Log():
    """
    A log of a Raft Node. 
    The Log is 0 indexed (not 1 indexed as in the RAFT paper)
    """

    def __init__(self):
        self.entries = []

    def latest_term(self):
        """
        Get the last term of the log
        """
        if len(self.entries) != 0:
            return self.entries[-1].term
        else:
            return -1

    def latest_index(self):
        """
        Get the last index of the log
        """
        last_index = len(self.entries) - 1
        return last_index

    def term_at_index(self, index : int):
        """
        Get the term at index
        """
        len_entries = len(self.entries)
        if index < 0 or index >= len_entries:
            return None
        return self.entries[index].term

    def append_entry(self, entry):
        """
        Append `entry` to the log.
        """
        self.entries += [entry]
       
    def remove_entries_at_and_after(self, index):
        """
        Remove all entries at and after the index.
        """
        assert (index > 0 and index < len(self.entries))
        self.entries = self.entries[:index]
    
    def get_entries_at_and_after(self, index):
        """
        Get all entries at and after the index `index`. 
        """
        return self.entries[index:]
    
    def size(self):
        return len(self.entries)

class LogEntry(NamedTuple):
    """
    An entry in the Log.
    """
    # the term number of the log entry
    term: int
    # This is a command to set the keys of args to the values of args
    # args = { "a" : 1, "b": 2} indicates the command is to set a to 1 and b to 2 
    args: Dict[str, str]
