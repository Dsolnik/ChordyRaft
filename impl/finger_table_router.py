import config
import math
from typing import NamedTuple, Dict, Optional, Union
import asyncio

from message import Message
from async_scheduler import AsyncScheduler, Handler, AsyncJob
from message import MessageSender, Message, MessageType
from key_transferer import KeyTransferer
from utils import trace_log, hash_key, debug_log, addr_later, in_interval, add_addr, Interval, opposite_interval
from successor_list import SuccessorList
from persistant_key_value_store import PersistantKeyValueStore
"""
This sets up the Routing Logic for the Finger Table Router.
"""

debug_fingers_on_fix_fingers = True

class Finger(NamedTuple):
    interval: Interval
    node_addr: Optional[int] 

def finger_table_to_string(f_table):
    return [(finger.interval.to_string(), finger.node_addr) for finger in f_table]

class FingerTableRouter:
    """
    This class maintains the finger table, successor, and predecessor.
    Mainly, this is doing through self.stabalize and self.fix_fingers()

    Persistent Storage can be either a RaftPersistentKeyValueStore which stores values amongst a raft cluster or
                                     a PersistentKeyValueStore which stores values as a dict in memory
    """
    def __init__(self, chord_addr: int, 
                    async_scheduler: AsyncScheduler, 
                    message_sender: MessageSender, 
                    key_transferer: KeyTransferer, 
                    persistent_storage: PersistantKeyValueStore,
                    successor_list: SuccessorList = None,
                    ):
        trace_log("FingerTableRouter.__init__ starting")

        self.chord_addr = chord_addr
        self.async_scheduler = async_scheduler
        self.message_sender = message_sender
        self.key_transferer = key_transferer
        self.key_transferer.register_router(self)
        self.persistent_storage = persistent_storage
        key_transferer.register_router(self)

        # State variables. Initialized during startup
        self.predecessor = None 
        self.successor = None
        self.finger_table = None
        
        self.handlers = [
            self.async_scheduler.register_handler(Handler(lambda msg: msg.has_type(MessageType.FIND_SUCCESSOR), self.handle_find_successor))
            ,self.async_scheduler.register_handler(Handler(lambda msg: msg.has_type(MessageType.FIND_PREDECESSOR), self.handle_find_predecessor))
            ,self.async_scheduler.register_handler(Handler(lambda msg: msg.has_type(MessageType.FIND_NODE_RESPONSIBLE_FOR_ADDR), self.handle_find_node_responsible_for_addr))
            ,self.async_scheduler.register_handler(Handler(lambda msg: msg.has_type(MessageType.I_MIGHT_BE_YOUR_PREDECESSOR), self.handle_i_might_be_your_predecessor))
            ,self.async_scheduler.register_handler(Handler(lambda msg: msg.has_type(MessageType.GET_CLOSEST_PRECEDING_FINGER), self.handle_closest_preceding_finger_node))]
    
    async def set_finger_node_addr(self, i, node_addr: int):
        """
        Set the `i`th entry of the finger table to node_addr
        """
        debug_log("FingerTableRouter.set_finger_node_addr", self.chord_addr, "setting finger", i, "to", node_addr)
        if self.finger_table[i].node_addr == None or self.finger_table[i].node_addr != node_addr:
            await self.persistent_storage.set(f"finger_table[{i}]", str(node_addr))
            self.finger_table[i] = Finger(self.finger_table[i].interval, node_addr)
        debug_log("FingerTableRouter.set_finger_node_addr", self.chord_addr, "setting finger", i, "to", node_addr, "DONE")
    
    async def set_successor(self, node_addr: int):
        """
        Set Successor
        """
        debug_log("FingerTableRouter.set_succ", self.chord_addr, "setting succ to ", node_addr)
        if self.successor == None or self.successor != node_addr:
            await self.persistent_storage.set("successor", str(node_addr))
            self.successor = node_addr
        debug_log("FingerTableRouter.set_succ", self.chord_addr, "setting succ to ", node_addr, "DONE")

    async def set_predecessor(self, node_addr: int):
        """
        Set Predecessor
        """
        debug_log("FingerTableRouter.set_pred", self.chord_addr, "setting predecessor to ", node_addr)
        if self.predecessor == None or self.predecessor != node_addr:
            await self.persistent_storage.set("predecessor", str(node_addr))
            self.predecessor = node_addr
        debug_log("FingerTableRouter.set_pred", self.chord_addr, "setting predecessor to ", node_addr, "DONE")

    # HELPERS 
    async def send_message_and_await(self, msg: Message, activation_condition):
        """
        Send `msg` and block until a msg which fulfills `activation_condition` holds, returning that msg 
        """
        fut = self.async_scheduler.get_message_wait_future(activation_condition)
        self.message_sender.send_msg(msg)
        return await fut

    def empty_finger_table(chord_addr: int):
        """
        Create an empty finger table.
        Static Method.
        """
        return [Finger(
                    Interval(add_addr(chord_addr, 2 ** b), True, add_addr(chord_addr, 2 ** (b + 1)), False), None) 
                for b in range(config.FINGER_TABLE_BITS)]

    # -----------------------------------------
    # FUNCTIONS TO ASK ANOTHER NODE SOMETHING
    # -----------------------------------------
    async def get_predecessor(self, node_id: int):
        """
        Ask the node for its predecessor
        """
        request_predecessor = self.message_sender.create_message(MessageType.FIND_PREDECESSOR, node_id)
        predecessor_msg = await self.send_message_and_await(request_predecessor, 
            lambda msg: msg.has_type(MessageType.FIND_PREDECESSOR_REPLY) and msg.src == node_id)
        assert ('predecessor' in predecessor_msg.args)
        return predecessor_msg.args['predecessor']

    async def get_successor(self, node_id: int):
        """
        Ask the node for its successor 
        """
        msg_to_send = self.message_sender.create_message(MessageType.FIND_SUCCESSOR, node_id)
        find_successor_message : Message = await self.send_message_and_await(msg_to_send, lambda msg: msg.has_type(MessageType.FIND_SUCCESSOR_REPLY) and msg.src == node_id)
        assert ('successor' in find_successor_message.args)
        return find_successor_message.args['successor']

    async def request_closest_preceding_finger(self, to_node: int, addr: int):
        """
        Ask the `to_node` to go into its finger table and look up addr 
        """
        request_cpf = self.message_sender.create_message(MessageType.GET_CLOSEST_PRECEDING_FINGER, to_node, {'addr' : addr})
        request_cpf_reply = await self.send_message_and_await(request_cpf,
            lambda msg: msg.has_type(MessageType.GET_CLOSEST_PRECEDING_FINGER_REPLY) and msg.src == to_node)
        assert('addr' in request_cpf_reply.args and 'finger_node' in request_cpf_reply.args)
        assert(request_cpf_reply.args['addr'] == addr)
        return request_cpf_reply.args['finger_node']

    async def request_node_responsible_for(self, node_id: int, relay_node: int):
        """
        Ask the `relay_node` to find which node is responsible for `node_id`
        """
        request_successor = self.message_sender.create_message(MessageType.FIND_NODE_RESPONSIBLE_FOR_ADDR, relay_node, {'addr' : self.chord_addr})
        node_who_used_to_be_responsible_reply = await self.send_message_and_await(request_successor, 
            lambda msg : msg.has_type(MessageType.FIND_NODE_RESPONSIBLE_FOR_ADDR_REPLY) and msg.src == relay_node)

        assert('addr' in node_who_used_to_be_responsible_reply.args and 'node' in node_who_used_to_be_responsible_reply.args)
        return node_who_used_to_be_responsible_reply.args['node']
    
    def notify_i_might_be_your_predecessor(self, to_node: int):
        """
        Notify the node `to_node` that we might be its predecessor
        """
        notify_sucessor = self.message_sender.create_message(MessageType.I_MIGHT_BE_YOUR_PREDECESSOR, to_node , {'addr': self.chord_addr})
        self.message_sender.send_msg(notify_sucessor)

    # -----------------------------------------
    #  Monitors
    # -----------------------------------------
    async def stabilize(self):
        """
        Monitor job to stabilize the predecessor and sucessor fields.
        """
        trace_log("FingerTableRouter.stabilize: addr ", self.chord_addr, " Stabilizing...")
        if (self.successor == self.chord_addr):
            # If we were the first one to start
                return 
        
        successor_predecessor = await self.get_predecessor(self.successor)
        if (successor_predecessor == None):
            trace_log("FingerTableRouter.stabilize: addr ", self.chord_addr,"Error when finding successor_predecessor. Unresponsive successor", self.successor)
        trace_log("FingerTableRouter.stabilize: addr ", self.chord_addr, "found predecessor ", successor_predecessor)

        # This means there is only 1 other node in the network
        if successor_predecessor == None:
            self.notify_i_might_be_your_predecessor(self.successor)
            return 

        # This means there are >2 nodes in the network
        if in_interval(self.chord_addr, self.successor, successor_predecessor):
            debug_log("ChordNode.FingerTableRouter", self.chord_addr, " setting successor at line 173")
            await self.set_successor(successor_predecessor)

        if (successor_predecessor != self.chord_addr):
            # notify our sucessor that we might be their predecessor
            self.notify_i_might_be_your_predecessor(self.successor)

        trace_log("FingerTableRouter.stabilize Complete: addr: ", self.chord_addr, "predecessor: ", self.predecessor, " successor: ", self.successor)

    async def fix_finger(self):
        """
        Fix the finger table.
        """
        trace_log("FingerTableRouter.fix_fingers: addr ", self.chord_addr, " Fixing fingers... succ", self.successor, 'pred', self.predecessor)
        await self.set_finger_node_addr(0, self.successor)
        for b in range(1, config.FINGER_TABLE_BITS):
            node_responsible = await self.find_node_responsible_for_addr(self.finger_table[b].interval.bottom)
            await self.set_finger_node_addr(b, node_responsible)
        trace_log("FingerTableRouter.fix_fingers: addr ", self.chord_addr, " Fixed fingers...")
        if debug_fingers_on_fix_fingers:
            debug_log(self.chord_addr, finger_table_to_string(self.finger_table), 'succ', self.successor, 'pred', self.predecessor)

    # -----------------------------------------
    # Startup
    # -----------------------------------------
    async def initialize_finger_table(self):
        """
        Initialize the finger table.
        NOTE: Only call this after self.successor is initialized (after startup)
        """
        await self.set_finger_node_addr(0, self.successor)
        # If we are the only node
        if (self.chord_addr == self.successor):
            for b in range(1, config.FINGER_TABLE_BITS):
                await self.set_finger_node_addr(b, self.finger_table[b - 1].node_addr)
            return 
        
        # Otherwise, we are not the first node
        for b in range(1, config.FINGER_TABLE_BITS):
            current_interval = self.finger_table[b].interval
            # If the previous finger is in the current interval, it should be this node too
            if current_interval.in_interval(self.finger_table[b - 1].node_addr):
                await self.set_finger_node_addr(b, self.finger_table[b - 1].node_addr)
            else:
                # Otherwise, we need to find the right finger.
                chord_node_responsible = await self.find_node_responsible_for_addr(self.finger_table[b].interval.bottom)
                await self.set_finger_node_addr(b, chord_node_responsible)

    async def load_finger_table(self):
        """
        Load the finger table from persistent_storage into self.finger_table.
        """
        i = 0
        while i < config.FINGER_TABLE_BITS:
            finger_table_node = await self.persistent_storage.get(f"finger_table[{i}]")
            if finger_table_node == None:
                break 
            else:
                self.finger_table[i] = Finger(self.finger_table[i].interval, int(finger_table_node))
                i += 1
        return i == config.FINGER_TABLE_BITS

    async def startup(self, another_node: Union[int, None]):
        """
        Startup. TO be call after IO Loop started. Sets self.successor
        """
        successor_stored = await self.persistent_storage.get("successor")
        predecessor_stored = await self.persistent_storage.get("predecessor")
        self.successor = None if successor_stored == None else int(successor_stored)
        self.predecessor = None if predecessor_stored == None else int(predecessor_stored)
        self.finger_table = FingerTableRouter.empty_finger_table(self.chord_addr)
        finger_table_loaded = await self.load_finger_table()

        # Load the finger table
        trace_log("FingerTableRouter.startup addr:", self.chord_addr, "Running Startup!")
        self.first_node = (another_node == None)
        if self.successor == None:
            debug_log("FingerTableRouter.startup", self.chord_addr, ":No Successor found in storage first node", self.first_node, another_node)
            if self.first_node:
                debug_log("FingerTableRouter.startup", self.chord_addr, ":239 setting succ")
                await self.set_successor(self.chord_addr)
            else:
                # If we have another node, relay through them to find our successor
                new_successor = await self.request_node_responsible_for(self.chord_addr, another_node)
                debug_log("FingerTableRouter.startup", self.chord_addr, ":244 setting succ")
                await self.set_successor(new_successor)
                trace_log("FingerTableRouter.startup ", self.chord_addr, ":Found Successor! ", self.successor)
        else:
            debug_log("FingerTableRouter.startup", self.chord_addr, ":Successor found in storage", self.successor)
        await self.initialize_finger_table()

        await self.post_startup()
    
    def shutdown(self):
        """
        Shutdown the finger table. Cancel all handlers and monitors (self.stabalize, self.fix_finger)
        """
        for cancel_handler in self.handlers:
            cancel_handler()
        self.cancel_fix_finger()
        self.cancel_stabilize()

    async def post_startup(self):
       """
       Post Startup. To be called after startup. Schedules monitor jobs.
       """
       self.cancel_stabilize = self.async_scheduler.schedule_monitor_job(self.stabilize, config.MONITOR_TIME) # call self.cancel_stabilize() to stop stabilizing
       self.cancel_fix_finger = self.async_scheduler.schedule_monitor_job(self.fix_finger, config.MONITOR_TIME) # call self.cancel_stabilize() to stop stabilizing

    # -----------------------------------------
    # Finger Table Routing
    # -----------------------------------------
    async def closest_preceding_finger_node(self, addr: int):
        """
        Find the node in the finger table with the addr closest to `addr`.
        """
        await self.set_finger_node_addr(0, self.successor)
        for finger in reversed(self.finger_table):
            # The Interval (self.chord_addr, addr)
            self_addr_to_addr = Interval(self.chord_addr, False, addr, False)
            if finger.node_addr != None and self_addr_to_addr.in_interval(finger.node_addr):
                return finger.node_addr
    
    # Find Node Responsible
    async def find_node_responsible_for_addr(self, addr: int):
        """
        Find the node responsible for an address
        """
        trace_log('FingerTableRouter.find_node_responsible', self.chord_addr,': Finding the node responsible for addr', addr, 'succ', self.successor, 'pred', self.predecessor)

        # This means no other node has joined yet
        if (self.chord_addr == self.successor):
            return self.chord_addr
        
        # If we are responsible
        if (Interval(self.chord_addr, False, self.successor, True).in_interval(addr)):
            return self.successor

        node_to_inquire = await self.closest_preceding_finger_node(addr)
        trace_log('FingerTableRouter.find_node_responsible : node_to_inquire ', node_to_inquire)
        node_to_inquire_successor = await self.get_successor(node_to_inquire)
        trace_log('FingerTableRouter.find_node_responsible : node_to_inquire_succ ', node_to_inquire_successor)
        iteration_num = 1

        if node_to_inquire == node_to_inquire_successor:
            return node_to_inquire

        # A Key k is assigned to
        # the first node whose identifier is equal to or follows (the identifier of) k in the identifier space
        # while (not addr \in (node_to_inquire, node_to_inquire_successor))
        while not Interval(node_to_inquire, False, node_to_inquire_successor, False).in_interval(addr):
            trace_log('FingerTableRouter.find_node_responsible: Running Find Node Responsible iteration ', iteration_num, ' node_to_inquire', node_to_inquire)

            node_to_inquire = await self.request_closest_preceding_finger(node_to_inquire, addr)
            trace_log('done acquiring new node to acquire', node_to_inquire)
            node_to_inquire_successor = await self.get_successor(node_to_inquire)

            trace_log('FingerTableRouter.find_node_responsible: Finished Find Node Responsible iteration ', iteration_num, '. Found next node ', node_to_inquire, ' which is ', node_to_inquire)
            iteration_num += 1

            if node_to_inquire == self.chord_addr:
                return self.chord_addr

        node_responsible = node_to_inquire_successor
        trace_log('FingerTableRouter', self.chord_addr, ': Found the node responsible for addr ', addr, ' : ', node_responsible)
        return node_responsible

    async def find_node_responsible_for_key(self, key: str):
        """
        Find the node responsible for a key
        """
        hashed_key = hash_key(key)
        node_responsible = await self.find_node_responsible_for_addr(hashed_key)
        trace_log('FingerTableRouter', self.chord_addr, ': Found the node responsible for key ', key, ' : ', node_responsible)
        return node_responsible

    # -----------------------------------------
    # Handlers
    # Note: Must be async
    # -----------------------------------------
    async def handle_find_node_responsible_for_addr(self, msg: Message):
        """
        Handle a request to find node responsible for an addr
        """
        assert ('addr' in msg.args)
        addr = msg.args['addr']
        node_responsible = await self.find_node_responsible_for_addr(addr)
        find_node_reply = self.message_sender.create_message(MessageType.FIND_NODE_RESPONSIBLE_FOR_ADDR_REPLY, msg.src, {'addr' : addr, 'node': node_responsible})
        self.message_sender.send_msg(find_node_reply)

    async def handle_find_predecessor(self, msg: Message):
        """
        Handle a request to find our predecessor
        """
        msg_to_send = self.message_sender.create_message(MessageType.FIND_PREDECESSOR_REPLY, msg.src, {'predecessor' : self.predecessor})
        self.message_sender.send_msg(msg_to_send)

    async def handle_find_successor(self, msg: Message):
        """
        Handle a request to find our successor 
        """
        msg_to_send = self.message_sender.create_message(MessageType.FIND_SUCCESSOR_REPLY, msg.src, {'successor' : self.successor})
        self.message_sender.send_msg(msg_to_send)

    async def handle_closest_preceding_finger_node(self, msg: Message):
        """
        Handles a request to find the closest preceding finger to an addr in our finger table
        """
        assert('addr' in msg.args)
        addr = msg.args['addr']
        finger_node = await self.closest_preceding_finger_node(addr)
        msg_to_send = self.message_sender.create_message(MessageType.GET_CLOSEST_PRECEDING_FINGER_REPLY, msg.src, {'addr' : addr, 'finger_node': finger_node})
        self.message_sender.send_msg(msg_to_send)

    async def handle_i_might_be_your_predecessor(self, msg: Message):
        """
        Handles a notification from another node that it may be our predecessor
        We move data here. TODO: Consider moving data when we change self.successor too.
        """
        assert ('addr' in msg.args)
        candidate_predecessor = msg.args['addr']
        trace_log("SuccRouter addr:", self.chord_addr, "Handling I might be your predecessor", candidate_predecessor)

        # If we are the first node.
        if (self.successor == self.chord_addr):
            debug_log("ChordNode.FingerTableRouter", self.chord_addr, " setting successor at line 401")
            await self.set_successor(candidate_predecessor)
            trace_log("SuccRouter addr:", self.chord_addr, "Setting new successor ", candidate_predecessor)

        # We know there are at least two nodes in the system.
        if (self.predecessor == None):
            # From our point of view, we used to be responsible for (self.sucessor, self.chord_addr]
            # Now, candidate_predecessor is coming in and taking (self.successor, candidate_predecessor] from us
            # So we transfer (self.successor, candidate_predecessor] to them.
            if (self.successor != candidate_predecessor):
                # debug_log(self.chord_addr, "Transfer because self.prdecessor == None, pred", self.predecessor, 'suc', self.successor)
                await self.key_transferer.copy_to(candidate_predecessor, Interval(self.successor, False, candidate_predecessor, True))
            else:
                # There is only one other node, so we should transfer him the other side
                # debug_log(self.chord_addr, "Transfer because self.prdecessor == None, pred", self.predecessor, 'suc', self.successor)
                await self.key_transferer.copy_to(candidate_predecessor, Interval(self.chord_addr, False, candidate_predecessor, True))

            # Normally, we don't need this. However, this fixes an edge cases involved with starting up with 1 node and having
            # 2 concurrent joins.
            await self.set_predecessor(candidate_predecessor)
            trace_log("SuccRouter addr:", self.chord_addr, "No current predecessor. Setting predecessor ", candidate_predecessor)
        elif (in_interval(self.predecessor, self.chord_addr, candidate_predecessor)):
            # From our point of view, we are responsible for (self.predecessor, self.chord_addr]
            # However, now we are only responisble for (candidate_predecessor, self.chord_addr]
            # So, we transfer out the difference: (self.predecessor, candidate_predecessor]
            debug_log(self.chord_addr, "Transfer because candidate_predecessor \in (self.predecessor, self.chord_addr), pred", self.predecessor, 'suc', self.successor)
            await self.key_transferer.copy_to(candidate_predecessor, Interval(self.predecessor, False, candidate_predecessor, True))
            await self.set_predecessor(candidate_predecessor)
            trace_log("SuccRouter addr:", self.chord_addr, "Setting new predecessor ", candidate_predecessor)
