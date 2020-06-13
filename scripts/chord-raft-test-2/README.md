This test tests a 3 chord node cluster,
Each chord node in constructed of a 3 node cluster of Raft Node, except 500, which is constructed of a cluster of 4 nodes

Chord Addr 100

-   Raft Node R1
-   Raft Node R2
-   Raft Node R3

Chord Addr 300

-   Raft Node R10
-   Raft Node R11
-   Raft Node R12

Chord Addr 500

-   Raft Node R20
-   Raft Node R21
-   Raft Node R22
-   Raft Node R23

To run this test:

1. Put chistributed.conf, start-nodes.chi in the main folder.
2. Go into utils.py and make sure that testing = True but debug_to_file = False
3. Run `chistributed --debug --run start-nodes.chi`
4. Wait a few seconds then check log.txt.
5. Find the entry in log.txt that says either R1, R2 or R3 has become leader. It should be R1 but confirm.
6. To set the value of key dan to rocks, run
    - If the leader is R1, run `set -n 100:R1,R2,R3 -k dan -v rocks`
    - If the leader is R2, run `set -n 100:R2,R1,R3 -k dan -v rocks`
    - If the leader is R3, run `set -n 100:R3,R1,R2 -k dan -v rocks`
7. To kill the leader, run
    - If the leader is R1, run `set -n 100:R1,R2,R3 -k die -v die`
    - If the leader is R2, run `set -n 100:R2,R1,R3 -k die -v die`
    - If the leader is R3, run `set -n 100:R3,R1,R2 -k die -v die`
8. Now, check to see who of R10,R11,R12 is the leader by checking log.txt to find the correct entry.
    - If the leader is R10, run `get -n 100:R10,R11,R12 -k dan`
    - If the leader is R11, run `get -n 100:R11,R10,R12 -k dan`
    - If the leader is R12, run `get -n 100:R12,R10,R11 -k dan`
9. End the session.
