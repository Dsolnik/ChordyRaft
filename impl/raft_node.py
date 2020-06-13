from collections import namedtuple
from typing import NamedTuple, Callable, Any, Dict, List
from async_scheduler import AsyncScheduler, Handler, AsyncJob
from enum import Enum
from utils import trace_log_raft, addr_later, hash_key, debug_log_raft, debug_test
import asyncio
import sys

import config
#from succ_router import SuccRouter
#from finger_table_router import FingerTableRouter
from persistant_key_value_store import PersistantKeyValueStore
from message import Message, MessageType, MessageSender 
from log import Log, LogEntry
from random import uniform
#from key_transferer import KeyTransferer
"""
A file that creates a node in the Raft cluster.
"""
class Role(Enum):
    LEADER = 'LEADER'
    CANDIDATE = 'CANDIDATE'
    FOLLOWER = 'FOLLOWER'

class RaftNode:

    def __init__(self, addr: str, peers : List[int],
                 persistent_storage: PersistantKeyValueStore,
                 async_scheduler: AsyncScheduler,
                 message_sender: MessageSender,
                 on_become_leader,
                 on_step_down
                 ):
        self.alive = True
        self.async_scheduler = async_scheduler
        self.message_sender = message_sender
        self.persistent_storage = persistent_storage
        self.peers = peers
        self.on_become_leader = on_become_leader
        self.on_step_down = on_step_down
 
        trace_log_raft('RaftNode.__init__: Starting Node addr: ', addr, ' with relay-node ', None)
        self.addr = addr
        self.role = None
        self.commit_index_waiters = [] # List of (comm_num, Task) waiting for commit index to be at least `comm_num` 

        self.log = Log()  # Log Entries
        # Election State
        self.current_term = 0  # Last Term Recieved
        self.voted_for = None  # Addr Voted for in the most recent election, (self.current_term if self.candidate)
        self.election_timer = None

        self.append_entries_running = None # A Task to ensure we only run one Append_Entries call at once if we are leader.

        # Candidate State (initialized when somebody becomes candidate)
        self.candidacy = None # A Task to ensure that we only run one campaign at a time.

        # Leader State (reinitialized after election)
        self.next_index = None 
        # matchIndex is a measurement of how much of our log (as leader) we know to be
        # replicated at each other server. This is used to determine when some prefix
        # of entries in our log from the current term has been replicated to a
        # majority of servers, and is thus safe to apply.
        self.match_index = None 
        # nextIndex is how much of our log (as leader) matches that of each other peer. This is used to determine what entries 
        # to send to each peer next.
        self.next_index = None
        # The Raft entries up to and including this index are considered committed by
        # Raft, meaning they will not change, and can safely be applied to persistent_storage.
        self.commit_index = -1
        # The last command in the log to be applied to persistent_storage.
        self.last_applied = -1

        # Register the handlers
        self.handlers = [
            async_scheduler.register_handler(Handler(lambda msg: msg.has_type(MessageType.REQUEST_VOTE), self.handle_request_vote))
            ,async_scheduler.register_handler(Handler(lambda msg: msg.has_type(MessageType.APPEND_ENTRIES), self.handle_append_entries))]

    def set_role(self, role):
        """
        Change our role
        """
        if self.role != Role.LEADER.value and role == Role.LEADER:
            self.on_become_leader()
        elif self.role == Role.LEADER.value and role != Role.LEADER:
            self.on_step_down()
        self.role = role.value

    async def startup(self):
        """
        Startup a Raft Node.
        """
        self.cancel_idle_heartbeat = self.async_scheduler.schedule_monitor_job(self.idle_heartbeat, config.IDLE_HEARTBEAT_FREQUENCY)
        if not self.role == Role.LEADER.value:
            self.become_follower()
            self.set_election_timer()
    
    def shutdown(self):
        """
        Shutdown a Raft Node.
        """
        if self.alive:
            self.alive = False
            debug_test("Raft Node", self.addr, "No longer alive")
            debug_log_raft("Raft Node", self.addr, "no longer alive")
            self.cancel_idle_heartbeat()
            for cancel_handler in self.handlers:
                cancel_handler()
        sys.exit()
    
    def recover(self):
        """
        Recover a Raft Node.
        """
        self.alive = True
        debug_test("Raft Node", self.addr, "Recovered")
        debug_log_raft("Raft Node", self.addr, "Recovered")

    def start_new_append_entries(self):
        """
        Start a new round of sending out append_entries requests to peers
        """
        if self.append_entries_running != None:
            # debug_log_raft("Raft", self.addr, "sending out new append entries")
            # cancel the old append entries because we're sending a new one!
            self.append_entries_running.cancel()
        self.append_entries_running = asyncio.create_task(self.reconcile_logs_with_peers())

    async def idle_heartbeat(self):
        """
        The idle heartbeat to make sure that nobody else tries to become leader if we are.
        """
        # debug_log_raft("Raft", self.addr, "got into idle heartbeat")
        if self.role == Role.LEADER.value:
            self.start_new_append_entries()

    # UTILITIES
    # ASYNC UTILITIES
    async def send_message_and_await(self, msg: Message, activation_condition):
        """
        Send `msg` and block until a msg which fulfills `activation_condition` holds, returning that msg 
        """
        fut = self.async_scheduler.get_message_wait_future(activation_condition)
        self.message_sender.send_msg(msg)
        return await fut
    #---------------------------------------------------------------------------
    #STATE CHANGERS

    """
    become_candidate : Become a Candidate and Initialize state to prepare to start a new vote.
    """
    def become_candidate(self):
        debug_log_raft("RaftNode ", self.addr, ": Becoming Candidate")

        self.set_role(Role.CANDIDATE)
        self.set_election_timer()

    """
    become_leader : Become Leader and Send APPEND_ENTRIES
    """
    def become_leader(self):
        debug_log_raft("Node ", self.addr, ": NEW LEADER")
        self.set_role(Role.LEADER)
        # When a leader first comes to power,
        # it initializes all nextIndex values to the index just after the
        # last one in its log
        self.match_index = {p: -1 for p in self.peers}
        self.next_index = {p: self.log.latest_index() + 1 for p in self.peers}
        self.clear_election_timer()
        self.start_new_append_entries()

    def become_follower(self):
        """
        Become a follower.
        """
        debug_log_raft("RaftNode ", self.addr, ": BECOME FOLLOWER")
        self.set_role(Role.FOLLOWER)
        self.next_index = None
        self.match_index = None
        self.voted_for = None

    def step_down(self, new_term):
        """
        Step down as leader because somebody else has a higher term.
        """
        self.current_term = new_term
        self.become_follower()
        self.set_election_timer()

    async def set_commit_index(self, commit_index):
        """
        Go through and apply everything between self.last_applied and new commit index
        """
        while self.last_applied < commit_index:
            self.last_applied += 1
            debug_log_raft("RAFT NODE", self.addr, "APPLYING LOG ENTRY", self.last_applied, self.log.entries)
            entry = self.log.entries[self.last_applied]
            # Apply the args
            args = entry.args
            for key in args:
                debug_log_raft("RAFT NODE", self.addr, "APPLYING LOG ENTRY", self.last_applied, "setting", key, "to", args[key])
                debug_test("Raft", self.addr, ": Applying log entry", self.last_applied, "setting", key, "to", args[key])
                await self.persistent_storage.set(key, args[key])

        self.commit_index = commit_index
        # Resolve any waiters if their request has been committed.
        for commit_num, fut in self.commit_index_waiters:
            if commit_num >= self.commit_index:
                debug_log_raft("Resolving set request with num ", commit_num)
                fut.set_result(True)

    async def set_match_index(self, peer, match_index):
        """
        Set the match index for a given node.
        """
        debug_log_raft("RAFT NODE", self.addr, "role value", self.role, "setting match index", peer, match_index)
        assert (self.role == Role.LEADER.value)
        self.match_index[peer] = match_index
        while True:
            # • If there exists an N such that N > commitIndex, a majority
            # of matchIndex[i] ≥ N, and log[N].term == currentTerm:
            # set commitIndex = N (§5.3, §5.4).
            if (1 + sum(self.match_index[match] >= (self.commit_index + 1) for match in self.match_index)) / (len(self.peers) + 1) >= .5:
                debug_log_raft("Raft", self.addr, "Setting commit index to be", self.commit_index + 1)
                await self.set_commit_index(self.commit_index + 1)
            else:
                return

    #---------------------------------------------------------------------------
    #ELECTION TIMER
    def clear_election_timer(self):
        """
        Clear the election timer.
        """
        assert (self.election_timer != None)
        self.election_timer.cancel()
        debug_log_raft(self.addr,"cancelling election timer")
        self.election_timer = None

    def set_election_timer(self):
        """
        Start an election timeout timer.
            If we were a candidate for an election, we time out the election and start a new election timeout.
        """
        debug_log_raft("Raft", self.addr, "restarting election timeout")
        if(self.election_timer != None):
            # print("cancelinng election timer")
            self.election_timer.cancel()

        async def timeout():
            time_to_sleep = uniform(config.ELECTION_TIMEOUT_MIN, config.ELECTION_TIMEOUT_MAX)
            await asyncio.sleep(time_to_sleep)
            if self.candidacy != None:
                self.candidacy.cancel()
            # Can't figure out why this isn't stopping so let's just not try and elect if we are the main guy.
            if not self.role == Role.LEADER.value:
                self.candidacy = asyncio.create_task(self.start_canidacy())

        self.election_timer = asyncio.create_task(timeout())

    async def get_value(self, key: str):
        """
        Get a value from the RAFT cluster
        """
        debug_log_raft("Raft", self.addr, "Getting key", key)
        assert (self.role == Role.LEADER.value)
        value = await self.persistent_storage.get(key)
        return value
    
    async def set_value(self, key: str, value: str):
        """
        Set a value on the RAFT cluster
        """
        assert (self.role == Role.LEADER.value)
        self.log.append_entry(LogEntry(self.current_term, {key: value}))
        loop = asyncio.get_running_loop()
        entry_applied = loop.create_future()

        self.commit_index_waiters.append((self.log.latest_index(), entry_applied))
        self.start_new_append_entries()
        await entry_applied

    async def handle_request_vote(self, msg : Message):
        """
        Handle a request for a vote from a candidate
        """
        if not self.alive:
            return
        assert('term' in msg.args and
               'last_log_index' in msg.args and
               'last_log_term' in msg.args)
        #UNPACK MESSAGE
        candidate_id = msg.src
        term = msg.args['term']
        last_log_index = msg.args['last_log_index']
        last_log_term = msg.args['last_log_term']

        #-----------------------------------------------------------------------
        # REJECTION LOGIC
        #-----------------------------------------------------------------------
        # If we are out of date, then become follower
        if(self.current_term < term):
            self.step_down(term)

        # REJECT Candidates who are OUT OF DATE
        elif(self.current_term > term):
            self.send_VOTE(candidate_id, False)
            return

        # REJECT Canidates if we ALREADY VOTED
        if(self.voted_for != None and
           self.voted_for != candidate_id):
           self.send_VOTE(candidate_id, False)
           return

        our_last_log_index = self.log.latest_index()
        our_last_log_term = self.log.latest_term()

        #REJECT Candidates with OLD LOGS
        if(last_log_term < our_last_log_term):
            self.send_VOTE(candidate_id, False)
            return

        #REJECT Candidates with shorter logs on same term
        if(last_log_term == our_last_log_term and
           last_log_index < our_last_log_index):
           self.send_VOTE(candidate_id, False)
           return

        #-----------------------------------------------------------------------
        #1. Vote for Candidate
        self.voted_for = candidate_id
        self.send_VOTE(candidate_id, True)

        #2. Reset Election Timer
        self.set_election_timer()

    #---------------------------------------------------------------------------
    #RAW SENDERS
    def send_REQUEST_VOTE(self, dst):
        """
        Ask `dst` for a vote for me in the current election.
        """
        args = {'term' : self.current_term,
                'last_log_index' : self.log.latest_index(),
                'last_log_term' : self.log.latest_term()
               }
        self.message_sender.create_and_send_message(MessageType.REQUEST_VOTE,
                                                    dst, args)
    def send_VOTE(self, dst, accept):
        """
        `dst`, I respectfully `accept` your nomination for the `self.current_term` term.
        """
        args = {'term' : self.current_term,
                'vote_granted' : accept}
        self.message_sender.create_and_send_message(MessageType.VOTE, dst, args)

    #---------------------------------------------------------------------------
 
    async def start_canidacy(self):
        """
        Run a campaign to be the leader.
        """
        debug_log_raft("RaftNode ", self.addr, ": Starts Canidacy")
        #1.Follower increments Term
        self.current_term += 1

        #1. Transition to Canidate State
        # Clear all tasks requesting a vote.
        self.become_candidate()

        debug_log_raft("RaftNode ", self.addr, ": Successful Start Canidacy 00")
        #2. Vote for Ourself
        self.voted_for = self.addr
        # self.votes += 1/app

        #3. Sends Vote Reqeust to All Peers
        debug_log_raft("RaftNode ", self.addr, ": REQUESTING VOTES")
        await self.request_votes_and_get_majority()
        debug_log_raft("Becoming the leader because we got a majority of votes.")
        self.become_leader()


    async def get_vote_from_peer(self, peer):
        """
        Get a vote from a peer for the current election cycle.
        """
        debug_log_raft("RAFT ", self.addr, "REQUESTING VOTES FROM ", peer)
        condition = lambda msg: (msg.has_type(MessageType.VOTE) and msg.src == peer)
        vote_response = self.async_scheduler.get_message_wait_future(condition)
        self.send_REQUEST_VOTE(peer)
        vote_msg = await vote_response
        term, vote_granted = vote_msg.get_args(['term', 'vote_granted'])
        if term > self.current_term:
            self.step_down()
            return
        return vote_granted, peer

    async def request_votes_and_get_majority(self):
        """
        Request votes from all your peers for the current election cycle and don't stop until we get a majority!
        """
        debug_log_raft("RaftNode", self.addr,": requesting votes from peers:", self.peers)
        # We assume that self.vote_request_task is None for every peer.
        vote_request_tasks = [self.get_vote_from_peer(peer) for peer in self.peers]
        have_majority = False
        num_votes = 1
        while not have_majority:
            finished, unfinished = await asyncio.wait(vote_request_tasks, return_when=asyncio.FIRST_COMPLETED)
            for finished_task in finished:
                vote, peer = finished_task.result()
                debug_log_raft("RaftNode", self.addr,":Got a vote of", vote, "from peer", peer)
                if vote == True:
                    num_votes += 1
            debug_log_raft("RaftNode", self.addr,":We now have ", num_votes, " votes majority is ", num_votes / (len(self.peers) + 1))
            # If we have a majority.
            if num_votes / (len(self.peers) + 1) >= .5:
                have_majority = True

    def make_append_entries_msg(self, peer):
        """
        Make the append_entries msg to be sent to `peer`
        """
        prev_index = self.next_index[peer]
        prev_index_term = self.log.term_at_index(prev_index)
        args =  {'term': self.current_term,
                    'prev_log_index' : prev_index,
                    'prev_log_term' : prev_index_term,
                    'entries' : self.log.get_entries_at_and_after(prev_index + 1),
                    'leader_commit' : self.commit_index
                    }
        return self.message_sender.create_message(MessageType.APPEND_ENTRIES, peer, args) , self.log.size()

    async def reconcile_logs_with_append_entries(self, peer):
        """
        Send an append_entries RPC to `peer` to reconcile our logs. 
        """
        if not self.alive:
            return

        debug_log_raft("Raft", self.addr, ": reconciling log with", peer)
        accept = False 
        while not accept:
            if not self.alive:
                return

            append_entries_msg, log_size_to_send = self.make_append_entries_msg(peer)
            response = await self.send_message_and_await(append_entries_msg, 
                lambda msg: (msg.has_type(MessageType.APPEND_ENTRIES_REPLY) and msg.src == peer and 
                    ('prev_log_index' in msg.args) and (msg.args['prev_log_index'] == append_entries_msg.args['prev_log_index'])
                    and ('entries_len' in msg.args) and (msg.args['entries_len'] == len(append_entries_msg.args['entries']))
                    ))

            debug_log_raft("Raft", self.addr, ": got response reconciling log with", peer, response)
            term, accept = response.get_args(['term', 'accept'])
            debug_log_raft("Raft", self.addr, ": got response reconciling log with", peer, "term", term, "accept", accept)
            if accept:
                debug_test("Raft", self.addr, ": Reconciling log with", peer, "complete")

            # We're behind
            if self.current_term < term:
                self.step_down(term)
                return
            
            if not accept:
                self.next_index[peer] -= 1
            else:
                # debug_log_raft("Raft", self.addr, ": got response reconciling log", peer," with matching up to ", log_size_to_send)
                self.next_index[peer] = log_size_to_send
                await self.set_match_index(peer,log_size_to_send - 1)

    async def reconcile_logs_with_peers(self):
        """
        Reconcile our log with all our peers.
        """
        debug_log_raft("Raft", self.addr, ": Reconciling logs with peers")
        # debug_test("Raft", self.addr, ": sending out APPEND_ENTRIES to peers")
        try:
            peer_tasks = [asyncio.create_task(self.reconcile_logs_with_append_entries(peer)) for peer in self.peers]
            # debug_log_raft("Raft", self.addr, ": reconciling logs with peers", peer_tasks)
            await asyncio.wait(peer_tasks)
        finally:
            for task in peer_tasks:
                task.cancel()
        

    def respond_to_append_entries(self, msg: Message, accept):
        """
        Respond to the append_entries RPC `msg` with `accept`
        """
        self.message_sender.create_and_send_message(MessageType.APPEND_ENTRIES_REPLY,
                                                    msg.src,
                                                    {'term': self.current_term,
                                                     'accept' : accept,
                                                     'prev_log_index': msg.args['prev_log_index'],
                                                     'entries_len': len(msg.args['entries'])
                                                     })

    async def handle_append_entries(self, append_entries_msg : Message):
        """
        Handle an append_entries RPC from a leader to reconcile our log.
        """
        if not self.alive:
            return
        # debug_log_raft("Raft", self.addr, ": handling append_entries")

        (term,
         prev_log_index,
         prev_log_term,
         entries,
         leader_commit) = append_entries_msg.get_args(['term',
                                                        'prev_log_index',
                                                        'prev_log_term',
                                                        'entries',
                                                        'leader_commit'])
        entries = [LogEntry(entry['term'], entry['args']) for entry in entries]

        if self.current_term < term:
            # We're behind
            self.step_down(term)
        else:
            # We got a message from the leader, so we reset the election timer.
            self.set_election_timer()

        assert (self.role != Role.LEADER.value)
        # We are now a follower (in case we were a candidate before)
        if self.role == Role.CANDIDATE.value:
            self.become_follower()

        prev_log_matches = prev_log_term is None or \
            self.log.term_at_index(prev_log_index) == prev_log_term

        if (term < self.current_term or (not prev_log_matches)):
            self.respond_to_append_entries(append_entries_msg, False)
            return

        """
        If an existing entry conflicts with a new one (same index
        but different terms), delete the existing entry and all that
        follow it (§5.3). Append any new entries not in the log.
        """
        index_in_our_log = prev_log_index + 1
        for entry in entries:
            entry_term = entry.term
            log_term = self.log.term_at_index(index_in_our_log)
            if (log_term == None):
                # If there's no entry at this index, we append the entry
                self.log.append_entry(entry)
            elif (log_term != entry_term):
                # If the entry at this point is different, we are behind and so
                #   we remove every entry after this and copy the entries given by the leader
                self.log.remove_entries_at_and_after(index_in_our_log)
                self.log.append_entry(entry)
            index_in_our_log += 1

        # self.log.entries = self.log.entires[:prev_log_index + entries_after_prev_log_entry] + entries[entries_after_prev_log_entry - 1:]
        # self.log.replace_logs_after_index(prev_log_index + entries_after_prev_log_entry, entries[entries_after_prev_log_entry - 1:])
        # last_matching_index = prev_log_index + entries_after_prev_log_entry 
        if leader_commit > self.commit_index:
            new_commit = min(leader_commit, self.log.latest_index())
            await self.set_commit_index(new_commit)
        self.respond_to_append_entries(append_entries_msg, True)
        debug_log_raft("RAFT", self.addr, "new log entries", self.log.entries, "from", append_entries_msg)