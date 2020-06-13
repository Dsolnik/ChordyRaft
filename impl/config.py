"""
config.py - Contain the Constants for the Project
"""

#BASE of ROUTING
BASE = 2

# KEYSPACE SIZE
FINGER_TABLE_BITS = 10
HIGHEST_ADDR = 2 ** FINGER_TABLE_BITS
FILE_TYPE = ".txt"

NUM_SUCCESSORS_IN_SUCCESSOR_LIST = 4
TIMEOUT_TIME = 1
MONITOR_TIME = 15

USING_SUCCESSOR_LIST = False
USING_SUCCESSOR_LIST_REPL = False

CHORD_RETRY_TIME = 20

# RAFT Options
ELECTION_TIMEOUT_MIN = 8
ELECTION_TIMEOUT_MAX = 10
IDLE_HEARTBEAT_FREQUENCY = 3