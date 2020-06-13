import json
import sys
import signal
import zmq
import time
import click
import time

import colorama
colorama.init()

from zmq.eventloop import ioloop, zmqstream
from utils import addr_diff, debug_log, debug_test
import asyncio

import config
from async_scheduler import AsyncScheduler
from message import Message, MessageSender, MessageType
from chord_node import ChordNode
from key_transferer import KeyTransferer
from persistant_key_value_store import PersistantKeyValueStore
from successor_list import SuccessorList
from raft_storage import RaftPersistentKeyValueStore

"""
The main node that deals with chistributed
"""

def get_chord_id_from_name(node_name):
    return int(node_name.split(":")[0])

def get_raft_ids_from_name(node_name):
    return node_name.split(":")[1].split(",")

# Represent a node in our data store
class NetworkNode(object):
    def __init__(self, node_name, pub_endpoint, router_endpoint, peer_names, debug):
        self.context = zmq.Context()

        self.node_name = node_name

        self.debug = debug
        self.connected = False

        chord_node_addr = get_chord_id_from_name(node_name)
        chord_node_peers = set(get_chord_id_from_name(name) for name in peer_names).difference(set([chord_node_addr]))
        # SUB socket for receiving messages from the broker
        # chord_node_addr = int(node_name)
        self.chord_node_addr = chord_node_addr
        self.sub_sock_chord = self.context.socket(zmq.SUB)
        self.sub_sock_chord.connect(pub_endpoint)
        # Make sure we get messages meant for us!
        self.sub_sock_chord.setsockopt_string(zmq.SUBSCRIBE, str(chord_node_addr))
        # Create handler for SUB socket
        self.sub_chord = zmqstream.ZMQStream(self.sub_sock_chord)
        self.sub_chord.on_recv(self.handle)

        raft_nodes = get_raft_ids_from_name(node_name)
        # Raft Ids are strings
        self.raft_id = raft_nodes[0]
        self.raft_peers = raft_nodes[1:]
        self.sub_sock_raft = self.context.socket(zmq.SUB)
        self.sub_sock_raft.connect(pub_endpoint)
        # Make sure we get messages meant for us!
        self.sub_sock_raft.setsockopt_string(zmq.SUBSCRIBE, self.raft_id)
        # Create handler for SUB socket
        self.sub_raft = zmqstream.ZMQStream(self.sub_sock_raft)
        self.sub_raft.on_recv(self.handle)

        # I'm not sure if we're going to need this.
        # SUB socket for recieveing broadcast messages from the broker 
        # self.broadcast_sock = self.context.socket(zmq.SUB)
        # self.broadcast_sock.connect(pub_endpoint)
        # self.broadcast_sock.setsockopt_string(zmq.SUBSCRIBE, "BROADCAST")
        # self.broadcast = zmqstream.ZMQStream(self.broadcast_sock, self.loop)
        # self.sub.on_recv(self.handle)

        # REQ socket for sending messages to the broker
        self.req_sock = self.context.socket(zmq.REQ)
        self.req_sock.connect(router_endpoint)
        self.req_sock.setsockopt_string(zmq.IDENTITY, node_name)
        # We don't strictly need a message handler for the REQ socket,
        # but we define one in case we ever receive any errors through it.
        self.req = zmqstream.ZMQStream(self.req_sock)
        self.req.on_recv(self.handle_broker_message)

        # self.name = node_name
        self.chord_node = None
        # self.peer_ids = [int(peer) for peer in peer_names]

        # Initialize the services.

        debug_log("NetworkNode.__init__ with Raft Id", self.raft_id, " and Chord Id ", self.chord_node_addr)
        self.async_scheduler = AsyncScheduler()
        self.chord_message_sender = MessageSender(self.send_to_broker, chord_node_addr)
        self.raft_message_sender = MessageSender(self.send_to_broker, self.raft_id)

        def became_leader():
            """
            Start up the chord node! We're the leader!
            """
            print(colorama.Fore.GREEN + "Node " + self.node_name + " Became leader " + colorama.Style.RESET_ALL)
            debug_test(f"Node {self.raft_id}: became leader... Starting up Chord Node for addr", self.chord_node_addr)
            debug_test(f"Chord {self.chord_node_addr}: starting up because {self.raft_id} leader ")
            self.chord_node = ChordNode(chord_node_addr, 
                self.another_node, self.persistent_storage, self.async_scheduler, self.chord_message_sender, self.key_transferer)
            asyncio.create_task(self.chord_node.startup())

        def no_longer_leader():
            """
            Oh no. Shut down the chord node. We're not the leader. :(
            """
            print(colorama.Fore.GREEN + "Node " + self.node_name + " is no longer leader " + colorama.Style.RESET_ALL)
            debug_test(f"Node {self.raft_id}: no longer leader... Shutting down Chord Node for addr", self.chord_node_addr)
            debug_test(f"Chord {self.chord_node_addr}: shutting down because {self.raft_id} not longer leader")
            self.chord_node.hard_shutdown()

        self.persistent_storage = RaftPersistentKeyValueStore(self.raft_id, self.raft_peers, self.async_scheduler, self.raft_message_sender, 
            became_leader,
            no_longer_leader
        )

        self.key_transferer = KeyTransferer(chord_node_addr, self.persistent_storage, self.async_scheduler, self.chord_message_sender)
    
        # We assume that the minimum chord_node addr will be started first. There is no relay node for them.
        if min(chord_node_peers) > chord_node_addr:
            self.another_node = None
        else:
            # Otherwise, the relay node is the node that was first started.
            self.another_node = min(chord_node_peers)
        print(self.another_node)

        # Capture signals to ensure an orderly shutdown
        for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
            signal.signal(sig, self.shutdown)

    def start(self):
        ioloop.IOLoop.instance().start()

    # Logging functions
    def log(self, msg):
        log_msg = ">>> %10s -- %s" % (self.node_name, msg)
        print(colorama.Style.BRIGHT + log_msg + colorama.Style.RESET_ALL)

    def log_debug(self, msg):
        if self.debug:
            log_msg = ">>> %10s -- %s" % (self.node_name, msg)
            print(colorama.Fore.BLUE + log_msg + colorama.Style.RESET_ALL)

    # Handle replies received through the REQ socket. Typically,
    # this will just be an acknowledgement of the message sent to the
    # broker through the REQ socket, but can also be an error message.
    def handle_broker_message(self, msg_frames):
        # TODO: Check whether there is an error
        pass

    # Sends a message to the broker
    def send_to_broker(self, d):
        self.req.send_json(d)
        self.log_debug("Sent: %s" % d)

    # Handles messages received from the broker (which can originate in
    # other nodes or in the broker itself)
    def handle(self, msg_frames):
        # Unpack the message frames.
        # in the event of a mismatch, format a nice string with msg_frames in
        # the raw, for debug purposes
        assert len(msg_frames) == 3, ((
            "Multipart ZMQ message had wrong length. "
            "Full message contents:\n{}").format(msg_frames))
        decoded = msg_frames[0].decode('ascii')
        print(decoded)
        if (decoded != str(self.chord_node_addr) and decoded != self.raft_id and decoded != self.node_name):
            return
        # Second field is the empty delimiter
        msg = json.loads(msg_frames[2])

        self.log_debug("Received " + str(msg_frames))
        
        if msg['type'] == 'hello':
            # Only handle this message if we are not yet connected to the broker.
            # Otherwise, we should ignore any subsequent hello messages.
            if not self.connected:
                # Send helloResponse
                self.connected = True
                self.send_to_broker({'type': 'helloResponse', 'source': self.node_name})
                self.log("Node is running")
                # Run startup processes
                asyncio.create_task(self.persistent_storage.startup())
        else:
            src = msg.pop('src', '-1')
            dst = msg.pop('destination', '-1')
            message_type = msg.pop('type', None)
            assert message_type != None
            key = msg.get('key', None)
            value = msg.get('value', None)

            if (message_type in [MessageType.GET.value, MessageType.SET.value]):
                # This indicates that it is a get or a set. We only reply if we are leader

                debug_test("NetworkNode", self.node_name, ":got command from client", msg)
                # This is a special command to kill a node
                if key == "die":
                    debug_test("NetworkNode", self.node_name, ":got kill command from client", msg)
                    if value == "die":
                        self.persistent_storage.raft_node.shutdown()
                        if self.chord_node != None:
                            self.chord_node.hard_shutdown()
                    elif value == "recover":
                        self.persistent_storage.raft_node.recover()
                        if self.chord_node != None:
                            self.chord_node.recover()
                    else:
                        debug_test("if the key is die the value needs to be either `die` or `recover`, instead it was", value)
                    return

                dst = get_chord_id_from_name(dst)
                debug_test(self.node_name, "got", message_type, "with chord node available", self.chord_node != None)
                if (self.chord_node == None):
                    # We are not leader.
                    debug_log("NetworkNode: get request for ", self.node_name, "but we're not leader")
                    debug_test("NetworkNode: get request for ", self.node_name, "but we're not leader")
                    self.send_to_broker({'type': f"{message_type}Response", 
                                         'id': msg['id'], 
                                         'error': "Cannot process request. We are not the leader"})
            else:
                # This is not sent from the client. This is an internal message
                if dst[0] != 'R':
                    dst = int(dst)
                if src[0] != 'R':
                    src = int(src)

            self.async_scheduler.deliver_message(Message(message_type, src, dst, msg))

    # Performs an orderly shutdown
    def shutdown(self, sig, frame):
        self.sub_sock_chord.close()
        self.req_sock.close()
        sys.exit(0)

# Command-line parameters
@click.command()
@click.option('--pub-endpoint', type=str, default='tcp://127.0.0.1:23310')
@click.option('--router-endpoint', type=str, default='tcp://127.0.0.1:23311')
@click.option('--node-name', type=str)
@click.option('--peer', multiple=True)
@click.option('--debug', is_flag=True)
def run(pub_endpoint, router_endpoint, node_name, peer, debug):
    # Create a node and run it
    n = NetworkNode(node_name, pub_endpoint, router_endpoint, peer, debug)

    n.start()
    
if __name__ == '__main__':
    run()
