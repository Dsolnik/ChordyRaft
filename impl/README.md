# distributed_systems

The general structure of the project is as follows:

NetworkNode (network_node.py) is the node created to deal with chistributed.
This parses messages into instances of Message (message.py) and creates a RaftNode (raft_node.py).

The RaftNode contains all logic involving the RAFT Cluster.

-   The Raft node uses PersistantKeyValueStore(persistent_key_value_store.py) to store data using get/set in memory
-   The Raft node uses Log(log.py) to store data

The ChordNode represents the Client for the Chord Node: it handles gets and set requests.

-   The ChordNode uses a Router to figure out the node responsible for an address
    -   The First Router is SuccRouter (succ\*router.py) which uses successor to route
    -   The Second Router is FingerTableRouter (finger\*table_router.py) which uses a finger table to route
-   The ChordNode uses a PersistentStorage to set and get values
    -   The first option for PersistentStorage is PersistentKeyValueStore(persistent\*key_value_store.py) to store data in memory
    -   The second option for PersistentStorage is RaftPersistentKeyValueStore(raft\*storage.py) which stores data amongst a cluster of Raft Nodes.
-   The ChordNode uses a KeyTransferer (key\*transferer.py) to transfer data between Chord Nodes when a new node joins
    -   This KeyTransferer can take in a PersistentStorage option (as above) and store data transfering using that

Both ChordNode and RaftNode use the AsyncScheduler(async_scheduler.py) to block waiting for messages to arrive
config.py contains configuration data

SuccList contains an optional addon which maintains the successor list of a ChordNode and can replicate data to successors if using Chord as an AP system using KeyTransferer (key_transferer.py)

utils.py contains various utilities used in all of the above
