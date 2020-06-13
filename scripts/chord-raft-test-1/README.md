This test tests a single node chord.
It tests that RAFT works correctly when a single node dies that is not the leader dies.

Chord Addr 100

-   Raft Node R1
-   Raft Node R2
-   Raft Node R3

To run this test:

1. Put chistributed.conf, start-nodes.chi in the main folder.
2. Go into utils.py and make sure that testing = True but debug_to_file = False
3. Run `chistributed --debug --run start-nodes.chi`
4. Wait a few seconds then check log.txt.
5. Find the entry in log.txt that says either R1, R2 or R3 has become leader. It should be R1 but confirm.
   First, let's confirm that if we try to set a value on a node that is not the leader we get an error.
6. To set the value of key dan to rocks on a node that is not the leader, run
    - If the leader is R1, run `set -n 100:R2,R1,R3 -k dan -v rocks`
    - If the leader is R2, run `set -n 100:R1,R2,R3 -k dan -v rocks`
    - If the leader is R3, run `set -n 100:R1,R2,R3 -k dan -v rocks`
7. To set the value of the key dan to rocks on a node that is the leader, run
    - If the leader is R1, run `set -n 100:R1,R2,R3 -k dan -v rocks`
    - If the leader is R2, run `set -n 100:R2,R1,R3 -k dan -v rocks`
    - If the leader is R3, run `set -n 100:R3,R1,R2 -k dan -v rocks`
8. To get the value of the key dan, run
    - If the leader is R1, run `get -n 100:R1,R2,R3 -k dan`
    - If the leader is R2, run `get -n 100:R2,R1,R3 -k dan`
    - If the leader is R3, run `get -n 100:R3,R1,R2 -k dan`
9. To fail a node that is not the leader, run
    - If the leader is R1, run `set -n 100:R2,R1,R3 -k die -v die`
    - If the leader is R2, run `set -n 100:R1,R2,R3 -k die -v die`
    - If the leader is R3, run `set -n 100:R1,R2,R3 -k die -v die`
10. To get the value of the key dan, run
    - If the leader is R1, run `get -n 100:R1,R2,R3 -k dan`
    - If the leader is R2, run `get -n 100:R2,R1,R3 -k dan`
    - If the leader is R3, run `get -n 100:R3,R1,R2 -k dan`
11. To set the value of the key danny, run
    - If the leader is R1, run `set -n 100:R1,R2,R3 -k danny -v rocks`
    - If the leader is R2, run `set -n 100:R2,R1,R3 -k danny -v rocks`
    - If the leader is R3, run `set -n 100:R3,R1,R2 -k danny -v rocks`
12. To get the value of the key danny, run
    - If the leader is R1, run `get -n 100:R1,R2,R3 -k danny`
    - If the leader is R2, run `get -n 100:R2,R1,R3 -k danny`
    - If the leader is R3, run `get -n 100:R3,R1,R2 -k danny`
13. End the session.
