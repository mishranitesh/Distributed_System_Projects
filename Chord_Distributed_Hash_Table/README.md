#### Language used to implement Chord Distributed Hash Table (DHT):

* Python - Version 3

-------------------------------------------------------------------------------------------------------------------------

#### Steps to compile and run the program on Remote Linux machines:

* Follow below steps to Setup Finger Table for each Chord Server nodes:
    * chmod +x init
    * Update nodes.txt file with the running Chord Server's IP and Port (use ip:port format).
    * Run command using bash terminal --> ./init nodes.txt

* Use below command to Start Chord Server on some distributed host:
    * make start_server port=\<available-port-number>

* Use below command to Test Basic Chord Client:
    * make test_basic_client ip=\<chord-server-ip-address> port=\<chord-server-port-number>

* Use below command to Test Advanced Chord Client:
    * make test_advanced_client ip=\<chord-server-ip-address> port=\<chord-server-port-number>

* Use below command to Clean the linked object files:
    * make clean

-------------------------------------------------------------------------------------------------------------------------

#### Brief description about the implementation:

* "void writeFile(RFile)" method:
    * It accepts object of RFile from Client side as parameter.
    * First call findSucc() method with SHA256 hash key of input filename.
    * Above method gives object of NodeID of the successor of input file.
    * If returned NodeID is same as the current NodeID then write the input file in dictionary repository.
    * Otherwise, raised SystemException with appropriate message.

* "NodeID readFile(string)" method:
    * It accepts string filename as input parameter.
    * First call findSucc() method with SHA256 hash key of input filename.
    * Above method gives object of NodeID of the successor of input filename.
    * If returned NodeID is same as the current NodeID then read file content from dictionary and return RFile object.
    * Otherwise, raised SystemException with appropriate message.

* "void setFingertable(list\<NodeID>)" method:
    * It accepts list of NodeID as input parameter.
    * If input list is not empty, then save the list to the server instance variable.
    * Otherwise in case of empty list, raised SystemException with appropriate message.

* "NodeID findSucc(string)" method:
    * It accepts SHA256 hash key as input parameter.
    * First it calls findPred() method with the input key to get the closest predecessor of the key.
    * Then it calls getNodeSucc() to get the successor NodeID of the input key.
    * Redirected SystemException if findPred() method throws some exception during its execution.

* "NodeID findPred(string)" method:
    * It accepts SHA256 hash key as input parameter.
    * Traverse through Chord Server's Finger Table to find closest predecessor of the input key.
    * It current node is not the closest predecessor then perform another RPC call to get next closest predecessor.
    * If finger table of current NodeID is empty then raised SystemException with appropriate message.

* "NodeID getNodeSucc()" method:
    * If current NodeID's finger table is not empty then return the first NodeID of the table.
    * Otherwise, raised SystemException with appropriate message. 

-------------------------------------------------------------------------------------------------------------------------


