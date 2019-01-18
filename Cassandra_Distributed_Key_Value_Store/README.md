## Key-Value Store with Configurable Consistency using Google Protocol Buffer messaging protocol

------------------------------------------------------------------------------------------------------------------

#### Steps to compile and run the program on Remote Linux machines:

* **Command to generate Python Code from replica.proto file**:
	* protoc --python_out=./ KeyValueStore.proto

* **Command to run Replica Key-Value Store Server**:
	* **make start_replica name=\<Replica0> port=\<9090> consistency_mechanism=\<"Read_Repair" OR "Hinted_Handoff"> filename=\<list_replicas.txt> use_file=\<yes OR no>**
	* Examples:
		* make start_replica name=Replica0 port=9090 consistency_mechanism=Read_Repair filename=list_replicas.txt use_file=no
		* make start_replica name=Replica1 port=9091 consistency_mechanism=Hinted_Handoff filename=list_replicas.txt use_file=no
		* make start_replica name=Replica2 port=9092 consistency_mechanism=Read_Repair filename=list_replicas.txt use_file=yes
		* make start_replica name=Replica3 port=9093 consistency_mechanism=Hinted_Handoff filename=list_replicas.txt use_file=yes

* **Command to setup Replicas Connections with each other**:
	* **make setup_connection filename=\<list_replicas.txt>**
	* Example:
		* make setup_connection filename=list_replicas.txt

* **Command to run the Client**:
	* **make start_client coordinator_ip=\<10.33.1.96> coordinator_port=\<9090>**
	* Example:
		* make start_client coordinator_ip=10.33.1.96 coordinator_port=9090

------------------------------------------------------------------------------------------------------------------

#### Pragramming Language:

* Python - Version 3

------------------------------------------------------------------------------------------------------------------
