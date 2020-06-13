from typing import NamedTuple, Callable, Any, Dict, Union
from enum import Enum
from utils import trace_log
"""
All things having to do with Messages that will be sent across the network here.
"""
class MessageType(Enum):
    """
    An enum that represents the message_type of 
    messages that can be sent over the network.
    """
    R_U_THERE = 'R_U_THERE'
    I_M_HERE = 'I_M_HERE'

    FIND_SUCCESSOR = 'FIND_SUCCESSOR'
    FIND_SUCCESSOR_REPLY = 'FIND_SUCESSOR_REPLY'

    FIND_PREDECESSOR = 'FIND_PREDECESSOR'
    FIND_PREDECESSOR_REPLY= 'FIND_PREDECESSOR_REPLY'

    I_MIGHT_BE_YOUR_PREDECESSOR = "I_MIGHT_BE_YOUR_PREDECESSOR"

    GET_VALUE = 'GET_VALUE'
    GET_VALUE_REPLY = 'GET_VALUE_REPLY'

    SET_VALUE = 'SET_VALUE'
    SET_VALUE_REPLY = 'SET_VALUE_REPLY'

    SET_VALUE_COPY = 'SET_VALUE_COPY'
    SET_VALUE_COPY_REPLY = 'SET_VALUE_COPY_REPLY'

    GET = 'get'
    GET_RESPONSE_TO_BROKER = 'getResponse'

    SET = 'set'
    SET_RESPONSE_TO_BROKER = 'setResponse'

    FIND_NODE_RESPONSIBLE_FOR_ADDR = 'FIND_NODE_ADDR'
    FIND_NODE_RESPONSIBLE_FOR_ADDR_REPLY = 'FIND_NODE_ADDR_REPLY'

    TRANSFER_KEYS = 'TRANSFER_KEYS'
    TRANSFER_KEYS_CONFIRMED = 'TRANSFER_KEYS_CONFIRMED'

    GET_CLOSEST_PRECEDING_FINGER = 'GET_CLOSEST_PRECEDING_FINGER'
    GET_CLOSEST_PRECEDING_FINGER_REPLY = 'GET_CLOSEST_PRECEDING_FINGER_REPLY'

    # RAFT
    VOTE = 'VOTE'
    REQUEST_VOTE = 'REQUEST_VOTE'

    APPEND_ENTRIES = 'APPEND_ENTRIES'
    APPEND_ENTRIES_REPLY = 'APPEND_ENTRIES_REPLY'

    # Deprecated
    HEARTBEAT = 'HEARTBEAT'
    HEARTBEAT_REPLY = 'HEARTBEAT_REPLY'

class Message(NamedTuple):
    """
    A Message to be sent over Network.
    """
    message_type: str
    src: Union[int, str]
    dst: Union[int, str]
    args: Dict[str, str]

    def has_type(self, message_type: MessageType):
        """
        Check if the message has the given type.
        """
        return self.message_type == message_type.value

    def has_args(self, keys: [str]):
        """
        Check if the message has the given args.
        """
        return all(key in self.args for key in keys)

    def get_args(self, keys: [str]):
        """
        Assert that the messages has the given args and return them.
        """
        assert self.has_args(keys)
        args_to_get = list((self.args[key] for key in keys))
        if len(args_to_get) == 1:
            return args_to_get[0]
        else: 
            return args_to_get
    
class MessageSender():
    """
    MessageSender: Handles Sending Messages to Broker (Chidistributed)
    """
    def __init__(self, send_dict_to_broker, addr):
        self.send_dict_to_broker = send_dict_to_broker
        self.addr = addr

    """
    send_message: Converts Message --> JSON structure for Broker
        Input(s):
            Message msg :   Message to be sent to Broker
        Output(s):
            None
        Side Effect(s):
            Sends Message to Chistributed Broker
    """
    def send_msg(self, msg: Message):
        self.send_dict_to_broker(dict(
            { 'type': msg.message_type, 'src': '' + str(msg.src), 'destination': str(msg.dst) },
            **msg.args))

    """
    create_message: Creates Message from Inputs
        Input(s):
            MessageType message_type    :   Message Type
            int         dst             :   Destination of Message
            Dict()      params          :   Parameters
        Output(s):
            Message     msg             :   Message made from Inputs
    """
    def create_message(self, message_type: MessageType, dst: int, params : Dict[str, str] = {}):
        return Message(message_type.value, self.addr, dst, params)

    """
    create_and_send_message: Creates Message from Inputs and Sends it
        Input(s):
            MessageType message_type    :   Message Type
            int         dst             :   Destination of Message
            Dict()      params          :   Parameters
        Output(s):
            None
        Side Effect(s):
            Sends Message to Chistributed Broker
    """
    def create_and_send_message(self, message_type: MessageType, to_addr: str, args: Dict[str, str] = {}):
        # Sends a message to node node. Doesn't wait for a response
        # trace_log('MessageSender.send_message', self.addr, ': sending message with type ', message_type, ' to ', to_addr, ' args ', args)
        self.send_msg(self.create_message(message_type, to_addr, args))