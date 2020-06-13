from persistant_key_value_store import PersistantKeyValueStore
from collections import namedtuple
from typing import NamedTuple, Callable, Any, Dict
from async_scheduler import AsyncScheduler, Handler, AsyncJob
from enum import Enum
from utils import trace_log, debug_log, addr_later, hash_key, Interval, parse_interval_str
from message import Message, MessageType, MessageSender
import config
import colorama
import asyncio
"""
A utility to transfer keys between 2 chord nodes
"""

debug_log_key_transfers = False

class KeyTransferer:

    def __init__(self, addr: int, persistant_storage: PersistantKeyValueStore, async_scheduler: AsyncScheduler, message_sender: MessageSender):
        self.message_sender = message_sender
        self.async_scheduler = async_scheduler
        self.persistant_storage = persistant_storage
        self.chord_addr = addr
        self.router = None

        async_scheduler.register_handler(Handler(lambda msg: msg.message_type == MessageType.TRANSFER_KEYS.value, self.handle_transfer_keys_to_me))
    
    def send_confirm_transfer(self, copy_node: int, interval: Interval, data_dict: Dict[str, str]):
        """
        Send a message that confirms a transfer was completed.
        """
        confirm_msg = self.message_sender.create_message(MessageType.TRANSFER_KEYS_CONFIRMED, copy_node, 
            {'interval' : interval.to_string(), 'keys_transfered': list(data_dict.keys())})
        self.message_sender.send_msg(confirm_msg)

    def register_router(self, router):
        """
        We register the router because we need to know the successor
        to relay data not intended for us
        """
        self.router = router

    async def handle_transfer_keys_to_me(self, msg: Message):
        """
        Handle a request to transfer keys to me.
        """
        if self.router == None:
            return
        interval, data_dict, copy_node, receiving_node = msg.get_args(['interval', 'data_dict', 'copy_node', 'receiving_node'])
        interval = parse_interval_str(interval)
        receiving_node = int(receiving_node)
        copy_node = int(copy_node)
        if debug_log_key_transfers:
            debug_log("KeyTransferer", self.chord_addr, ": Handling transfer keys from", copy_node, "to", self.chord_addr, "of interval", interval.to_string(), "current-pred", self.router.predecessor, " succ ", self.router.successor)

        data_to_store = None 
        # Check that we should recieve these and that this is in fact intended for us.
        if (self.chord_addr == self.router.successor or self.router.predecessor == None):
            # These are intended for us, because we control everything
            # Store the data on disk
            data_to_store = data_dict
        else:
            # assert(interval.top == self.chord_addr and interval.top_closed)
            print("interval is", interval, "predecessor", self.router.predecessor)
            if (interval.in_interval(self.router.predecessor)):
                # This means the whole interval isn't for us.
                # Take out what's ours and foward what ever else there is
                our_key_range = Interval(self.chord_addr, True, self.router.successor, False)
                our_keys = set(key for key in data_dict.keys() if our_key_range.in_interval(hash_key(key)))
                dict_that_is_ours = dict({key: data_dict[key] for key in our_keys})

                other_keys = set(data_dict.keys()).difference(our_keys)
                dict_that_isnt_ours = {key: data_dict[key] for key in other_keys}
                # Assume we are the end of the interval, then we need to pass it back wards.
                other_range = Interval(interval.bottom, interval.bottom_closed, self.router.predecessor, True)
                # Pass along the rest of the interval
                await self.copy_key_values_to(self.router.predecessor, other_range, dict_that_isnt_ours)
                data_to_store = dict_that_is_ours
            else:
                # This means we are replicating
                data_to_store = data_dict

        # Store the data on disk
        debug_log("KeyTransferer", self.chord_addr, ": Handling transfer keys from", copy_node, "to", self.chord_addr, "of interval", interval.to_string(), "current-pred", self.router.predecessor, " succ ", self.router.successor, "data", data_to_store)
        await self.persistant_storage.add_dict(data_to_store)

        # Confirm what we stored.
        self.send_confirm_transfer(copy_node, interval, data_dict)

    async def copy_key_values_to(self, to_node: int, interval: Interval, dict_to_transfer: Dict[str, str]):
        """
        Copy key_values in the `interval` from persistent_storage to `to_node`
        """
        keys_to_transfer = dict_to_transfer.keys()
        msg_to_transfer = self.message_sender.create_message(MessageType.TRANSFER_KEYS, to_node, 
            {'interval' : interval.to_string(), 'data_dict' : dict_to_transfer, 'copy_node': self.chord_addr, 'receiving_node': to_node})
        recieved_msg = await self.send_message_and_await_response(msg_to_transfer, MessageType.TRANSFER_KEYS_CONFIRMED)

        keys_transfered_successfully, interval_transfered_successfully = recieved_msg.get_args(['keys_transfered', 'interval'])
        assert (set(keys_transfered_successfully) == set(dict_to_transfer.keys()))
        trace_log(colorama.Fore.MAGENTA + "KeyTransferer", self.chord_addr, ": Copy Successfully completed from", self.chord_addr, "to", to_node, "of interval", interval.to_string())
        if debug_log_key_transfers:
            debug_log("KeyTransferer", self.chord_addr, ": Copy Successfully completed from", self.chord_addr, "to", to_node, "of interval", interval.to_string())

    async def copy_to(self, to_node: int, interval: Interval):
        """
        Copy the keys of an interval from our persistant storage to the `to_node`.
        """
        trace_log("KeyTransferer ", self.chord_addr, " : Starting copy of keys from myself to ", to_node)
        all_keys = await self.persistant_storage.get_keys()
        keys_to_transfer = [key for key in all_keys if interval.in_interval(hash_key(key))]
        dict_to_transfer = {}
        for key in keys_to_transfer:
            dict_to_transfer[key] = await self.persistant_storage.get(key)
        await self.copy_key_values_to(to_node, interval, dict_to_transfer)

    # UTILITIES 
    # ASYNC UTILITIES
    async def send_message_and_await_response(self, msg: Message, msg_type: MessageType):
        """
        Send `msg` and block until a msg which fulfills `activation_condition` holds, returning that msg 
        """
        fut = self.async_scheduler.get_message_wait_future(lambda m: m.has_type(msg_type) and m.src == msg.dst)
        self.message_sender.send_msg(msg)
        return await fut



