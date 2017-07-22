Project 4
Ketan Shevale
B00599170

Compilation:
1. Unzip tar ball
2. Use make command to compile source code

2 Two-Phase Commit
The architecture of replicated file service is very simple: It consists of a single “coordinator” process and multiple “participant” processes:
coordinator the “coordinator” process should expose an RPC interface to clients that contains 
three methods: writeFile, readFile, and deleteFile. When the coordinator process receives a state-changing operation (write-
File or deleteFile), it uses two-phase commit to commit that state-changing operation to all participants. 
When the coordinator receives a readFile operation, it selects a participant at random to issue the request
against. The coordinator should be concurrent. 
It should allow multiple clients to connect to it and should be able to process multiple operations concurrently. 
Participant each participant implements a durable remote file service you built in Part 1. 
It exposes an RPC interface to the coordinator to participate in two-phase commit. 
It is up to you what specific methods the participants should support
