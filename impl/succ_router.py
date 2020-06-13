"""
router.py - Contain the Routing Logic/Fingertable Maitenance
"""
import config
import math
from typing import NamedTuple, Dict
import asyncio

from message import Message
from async_scheduler import AsyncScheduler, Handler, AsyncJob
from message import MessageSender, Message, MessageType
from key_transferer import KeyTransferer
from utils import trace_log, debug_log, hash_key, addr_later, in_interval, add_addr, Interval, opposite_interval
"""
Deprecated.
This is the Finger_Table_Router but without Fingers! 
All it uses is succ and pred
"""

class SuccRouter:
    def __init__(self, addr: int, async_scheduler: AsyncScheduler, message_sender: MessageSender, key_transferer: KeyTransferer):
        trace_log("Starting SuccRouter!")
        self.addr = addr
        self.async_scheduler = async_scheduler
        self.message_sender = message_sender
        self.key_transferer = key_transferer
        key_transferer.register_router(self)

        self.predecessor = None # initialized in startup
        self.successor = None # defined in startup

        self.async_scheduler.register_handler(Handler(lambda msg: msg.has_type(MessageType.FIND_SUCCESSOR), self.handle_find_successor))
        self.async_scheduler.register_handler(Handler(lambda msg: msg.has_type(MessageType.FIND_PREDECESSOR), self.handle_find_predecessor))
        self.async_scheduler.register_handler(Handler(lambda msg: msg.has_type(MessageType.FIND_NODE_RESPONSIBLE_FOR_ADDR), self.handle_find_node_responsible_for_addr))
        self.async_scheduler.register_handler(Handler(lambda msg: msg.has_type(MessageType.I_MIGHT_BE_YOUR_PREDECESSOR), self.handle_i_might_be_your_predecessor))

    async def get_predecessor(self, node_id: int):
        request_predecessor = self.message_sender.create_message(MessageType.FIND_PREDECESSOR, node_id)
        predecessor_msg = await self.send_message_and_await(request_predecessor,
            lambda msg: msg.has_type(MessageType.FIND_PREDECESSOR_REPLY) and msg.src == node_id)
        assert ('predecessor' in predecessor_msg.args)
        return predecessor_msg.args['predecessor']

    async def stabilize(self):
        trace_log("SuccRouter.stabilize: addr ", self.addr, " Stabilizing...")
        if (self.successor == self.addr):
            # If we were the first one to start
                return

        # Find our sucessors predecessor
        # request_predecessor = self.message_sender.create_message(MessageType.FIND_PREDECESSOR, node_id)
        # predecessor_msg = await self.send_message_and_await(request_predecessor,
        #     lambda msg: msg.has_type(MessageType.FIND_PREDECESSOR_REPLY) and msg.src == self.successor)
        # assert ('predecessor' in predecessor_msg.args)

        # N Node Network
        #1. Find Predecessor of our Successor and Wait for Response
        successor_predecessor = await self.get_predecessor(self.successor)
        trace_log("SuccRouter.stabilize: addr ", self.addr, "found predecessor ", successor_predecessor)

        #UPDATE NODES
        #1. If Succesor does not have Predecessor, we may be their Predecessor
        if successor_predecessor == None:
            self.notify_i_might_be_your_predecessor(self.successor)
            return

        #2. If Node < Sucessor's predecessor < Succesor
        #       then our Sucessors Predecessor is Most likely adjacent to us on
        #       Ring so they become our new Sucessor
        if in_interval(self.addr, self.successor, successor_predecessor):
            self.successor = successor_predecessor

        #3. If Sucessor's predecessor ... < Node < Succesor
        #       then our Sucessors Predecessor is Most likely us
        if (successor_predecessor != self.addr):
            # notify our sucessor that we might be their predecessor
            self.notify_i_might_be_your_predecessor(self.successor)

        debug_log("SuccRouter.stabilize Complete: addr: ", self.addr, "predecessor: ", self.predecessor, " successor: ", self.successor)

    async def startup(self, another_node: int):
        trace_log("SuccRouter.startup addr:", self.addr, "Running Startup!")
        if (another_node == None):
            self.first_node = True
            self.predecessor = None
            self.successor = self.addr
        else:
            self.first_node = False
            # Find our successor
            request_successor = self.message_sender.create_message(MessageType.FIND_NODE_RESPONSIBLE_FOR_ADDR, another_node, {'addr' : self.addr})
            node_who_used_to_be_responsible_reply = await self.send_message_and_await(request_successor,
                lambda msg : msg.has_type(MessageType.FIND_NODE_RESPONSIBLE_FOR_ADDR_REPLY) and msg.src == another_node)

            assert('addr' in node_who_used_to_be_responsible_reply.args and 'node' in node_who_used_to_be_responsible_reply.args)
            node_who_used_to_be_responsible = node_who_used_to_be_responsible_reply.args['node']
            self.successor = node_who_used_to_be_responsible
            trace_log("SuccRouter.startup addr:", self.addr, "Found Successor! ", self.successor)

        await self.post_startup()

    async def post_startup(self):
       self.cancel_stabilize = self.async_scheduler.schedule_monitor_job(self.stabilize, config.MONITOR_TIME) # call self.cancel_stabilize() to stop stabilizing

    def create_message(self, message_type: MessageType, dst: int, params : Dict[str, str] = {}):
        return Message(message_type.value, self.addr, dst, params)

    def notify_i_might_be_your_predecessor(self, to:int):
        notify_sucessor = self.message_sender.create_message(MessageType.I_MIGHT_BE_YOUR_PREDECESSOR, to , {'addr': self.addr})
        self.message_sender.send_msg(notify_sucessor)

    async def find_node_responsible_for_addr(self, addr: int):
        trace_log('SuccRouter.find_node_responsible : Finding the node responsible for addr', addr)

        # This means no other node has joined yet
        if (self.addr == self.successor):
            return self.addr

        previous_node_id = self.addr
        next_node_id = self.successor
        iteration_num = 1

        # A Key k is assigned to
        # the first node whose identifier is equal to or follows (the identifier
        # of) k in the identifier space
        while addr_later(self.addr, addr, next_node_id):
            trace_log('SuccRouter.find_node_responsible: Running Successor iteration ', iteration_num)

            msg_to_send = self.message_sender.create_message(MessageType.FIND_SUCCESSOR, next_node_id)
            find_successor_message : Message = await self.send_message_and_await(msg_to_send, lambda msg: msg.message_type == MessageType.FIND_SUCCESSOR_REPLY.value and msg.src == next_node_id)

            assert ('successor' in find_successor_message.args)

            trace_log('SuccRouter.find_node_responsible: Finished successor iteration ', iteration_num, '. Found succesor of ', next_node_id, ' which is ', find_successor_message.args['successor'])
            previous_node_id = next_node_id
            next_node_id = int(find_successor_message.args['successor'])
            iteration_num += 1

            if (next_node_id == self.addr):
                # This means we went in a circle and found that we are the correct one
                break

        node_responsible = next_node_id
        trace_log('SuccRouter: Found the node responsible for addr ', addr, ' : ', node_responsible)
        return node_responsible

    async def find_node_responsible_for_key(self, key: str):
        hashed_key = hash_key(key)
        node_responsible = await self.find_node_responsible_for_addr(hashed_key)
        trace_log('SuccRouter: Found the node responsible for key ', key, ' : ', node_responsible)
        return node_responsible

    # HELPERS
    async def send_message_and_await(self, msg: Message, activation_condition):
        fut = self.async_scheduler.get_message_wait_future(activation_condition)
        self.message_sender.send_msg(msg)
        return await fut
    """
    send_message_and_fut   :   Sends message and returns Future() to wait on
        Input(s):
            Message msg                     :   Message to send
            lambda  activation_condition    :   Activation Condition
        Output(s):
            Future  fut :   Future for awaiting response to message
        Side Effect(s):
            Sends msg
    """
    async def send_message_and_fut(self, msg: Message, activation_condition):
        fut = self.async_scheduler.get_message_wait_future(activation_condition)
        self.send_msg(msg)
        return fut

    # HANDLERS
    # MUST BE ASYNC
    async def handle_find_successor(self, msg: Message):
        msg_to_send = self.message_sender.create_message(MessageType.FIND_SUCCESSOR_REPLY, msg.src, {'successor' : self.successor})
        self.message_sender.send_msg(msg_to_send)

    async def handle_find_predecessor(self, msg: Message):
        msg_to_send = self.message_sender.create_message(MessageType.FIND_PREDECESSOR_REPLY, msg.src, {'predecessor' : self.predecessor})
        self.message_sender.send_msg(msg_to_send)

    async def handle_i_might_be_your_predecessor(self, msg: Message):
        assert ('addr' in msg.args)
        candidate_predecessor = msg.args['addr']

        # If we are the first node.
        if (self.successor == self.addr):
            self.successor = candidate_predecessor
            trace_log("SuccRouter addr:", self.addr, "Setting new successor ", candidate_predecessor)
            # transfer the stuff from (self.addr, candidate_predecessor] because we are no longer responsible for that
            #   half of addr space
            # debug_log(self.addr, "Transfer because self.successor == self.addr, pred", self.predecessor, 'suc', self.successor)
            # await self.key_transferer.copy_to(candidate_predecessor, Interval(self.addr, False, candidate_predecessor, True))


        # We know there are at least two nodes in the system.
        if (self.predecessor == None):
            # From our point of view, we used to be responsible for (self.sucessor, self.addr]
            # Now, candidate_predecessor is coming in and taking (self.successor, candidate_predecessor] from us
            # So we transfer (self.successor, candidate_predecessor] to them.
            if (self.successor != candidate_predecessor):
                debug_log(self.addr, "Transfer because self.prdecessor == None, pred", self.predecessor, 'suc', self.successor)
                await self.key_transferer.copy_to(candidate_predecessor, Interval(self.successor, False, candidate_predecessor, True))
            else:
                # There is only one other node, so we should transfer him the other side
                debug_log(self.addr, "Transfer because self.prdecessor == None, pred", self.predecessor, 'suc', self.successor)
                await self.key_transferer.copy_to(candidate_predecessor, Interval(self.addr, False, candidate_predecessor, True))

            # Normally, we don't need this. However, this fixes an edge cases involved with starting up with 1 node and having
            # 2 concurrent joins.

            self.predecessor = candidate_predecessor
            trace_log("SuccRouter addr:", self.addr, "No current predecessor. Setting predecessor ", candidate_predecessor)
        elif (in_interval(self.predecessor, self.addr, candidate_predecessor)):
            # From our point of view, we are responsible for (self.predecessor, self.addr]
            # However, now we are only responisble for (candidate_predecessor, self.addr]
            # So, we transfer out the difference: (self.predecessor, candidate_predecessor]
            debug_log(self.addr, "Transfer because candidate_predecessor \in (self.predecessor, self.addr), pred", self.predecessor, 'suc', self.successor)
            await self.key_transferer.copy_to(candidate_predecessor, Interval(self.predecessor, False, candidate_predecessor, True))
            self.predecessor = candidate_predecessor
            trace_log("SuccRouter addr:", self.addr, "Setting new predecessor ", candidate_predecessor)

    async def handle_find_node_responsible_for_addr(self, msg: Message):
        assert ('addr' in msg.args)
        addr = msg.args['addr']
        node_responsible = await self.find_node_responsible_for_addr(addr)
        find_node_reply = self.message_sender.create_message(MessageType.FIND_NODE_RESPONSIBLE_FOR_ADDR_REPLY, msg.src, {'addr' : addr, 'node': node_responsible})
        self.message_sender.send_msg(find_node_reply)
