#!usr/bin/python3

import os
import sys
import math
import time
import socket
import datetime
import threading
from constants import *

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import KeyValueStore_pb2
from google.protobuf.internal.encoder import _VarintEncoder
from google.protobuf.internal.decoder import _DecodeVarint


# Method to encode an int as a protobuf varint
def encode_varint(value):
    """ Encode an int as a protobuf varint """
    data = []
    _VarintEncoder()(data.append, value, False)
    return b''.join(data)


# Method to decode a protobuf varint to an int
def decode_varint(data):
    """ Decode a protobuf varint to an int """
    return _DecodeVarint(data, 0)[0]


# Global Dictionary to hold key value pair
global_key_value_dictionary = {}
# Global mutex to prevent concurrent access of global_key_value_dictionary
global_key_value_dict_mutex = threading.Lock()

# Global mutex to prevent write ahead log file access
global_log_file_mutex = threading.Lock()


# Function to populate dictionary using write ahead log file (if applicable)
def check_write_ahead_log(arg_replica_nameIn):
    filename = os.getcwd() + "/" + arg_replica_nameIn + "_write_ahead.log"
    if os.path.isfile(filename):
        global_log_file_mutex.acquire()
        with open(filename, 'r') as fp:
            global_key_value_dict_mutex.acquire()
            for line in fp:
                f_key, f_value, f_timestamp = line.split(':')
                global_key_value_dictionary[f_key.strip()] = (f_value.strip(), f_timestamp.strip())
            global_key_value_dict_mutex.release()
        fp.close()
        global_log_file_mutex.release()
    else:
        # Create new empty file (touch filename)
        os.mknod(filename)


# Class to represent Replica Information
class ReplicaInfo:
    """Class to represent Replica Information"""

    def __init__(self, r_nameIn, r_ipIn, r_portIn):
        self.replica_name = r_nameIn
        self.replica_ip_address = r_ipIn
        self.replica_port_number = int(r_portIn)


# Class to represent Key-Value Replica Store
class KeyValueReplicaStore(threading.Thread):
    """Class to represent Key-Value Replica Store"""

    # Static Variable to hold all active replicas in the system
    global_replica_info_list = []
    # Mutex to prevent global global_replica_info_list
    global_replica_info_list_mutex = threading.Lock()

    # Static variable to hold Mapping of Replica name with respective TCP socket
    global_replica_socket_dictionary = {}
    # Mutex to prevent global replica socket dictionary
    global_replica_socket_dict_mutex = threading.Lock()

    # List to hold daemon threads
    global_daemon_thread_list = []
    # Mutex to prevent global_daemon_thread_list
    global_daemon_thread_list_mutex = threading.Lock()

    # Static variable to hold global sequencer
    global_client_sequencer = 0
    # Mutex to prevent global_client_sequencer
    global_client_sequencer_mutex = threading.Lock()

    # Constructor
    def __init__(self, incoming_socketIn, incoming_socket_addressIn, r_nameIn, r_ipIn, r_portIn, r_consistency_mechanismIn):
        threading.Thread.__init__(self)
        self.incoming_socket = incoming_socketIn
        self.incoming_socket_address = incoming_socket_addressIn
        self.replica_name = r_nameIn
        self.replica_ip = r_ipIn
        self.replica_port = r_portIn
        self.replica_consistency_mechanism = r_consistency_mechanismIn
        self.kv_message_instance = KeyValueStore_pb2.KeyValueMessage()

    # Method (run) works as entry point for each thread - overridden from threading.Thread
    def run(self):
        # Identify message type here by parsing incoming socket message
        data = self.incoming_socket.recv(1)
        size = decode_varint(data)
        self.kv_message_instance.ParseFromString(self.incoming_socket.recv(size))

        # Filter received message with conditional statements
        if self.kv_message_instance.WhichOneof('key_value_message') == 'init_replica':
            # Receive InitReplica message object message from controller
            self.receive_InitReplica_message()

            # Set up TCP Full Duplex connection with all the replicas in the system
            self.setup_replica_connections()

            # Sleep for sometime till setup connection is completed
            time.sleep(2)

            if DEBUG_STDERR:
                print("Global Replica Socket Dictionary:", file=sys.stderr)
                KeyValueReplicaStore.global_replica_socket_dict_mutex.acquire()
                for k, v in KeyValueReplicaStore.global_replica_socket_dictionary.items():
                    print(k, ":", v, file=sys.stderr)
                KeyValueReplicaStore.global_replica_socket_dict_mutex.release()
        elif self.kv_message_instance.WhichOneof('key_value_message') == 'connect_replica':
            # Store with key - other replica name and value - other replica socket object
            received_r_name = self.kv_message_instance.connect_replica.replica_name
            self.add_global_replica_socket_dictionary(received_r_name, self.incoming_socket)
        elif self.kv_message_instance.WhichOneof('key_value_message') == 'connect_client':
            # Handle each client request using separate thread
            client_thread = threading.Thread(name='Handle_Client_Request', target=self.handle_client_request, args=("client", self.incoming_socket), daemon=True)
            client_thread.start()
            KeyValueReplicaStore.global_daemon_thread_list_mutex.acquire()
            KeyValueReplicaStore.global_daemon_thread_list.append(client_thread)
            KeyValueReplicaStore.global_daemon_thread_list_mutex.release()

        # Main thread should wait till all threads complete their operation
        KeyValueReplicaStore.global_daemon_thread_list_mutex.acquire()
        for cur_thread in KeyValueReplicaStore.global_daemon_thread_list:
            cur_thread.join()
        KeyValueReplicaStore.global_daemon_thread_list_mutex.release()

    # Function to handle client request
    def handle_client_request(self, requestorIn, client_socketIn):
        # Generate unique transaction id and sent it to client socket
        l_seq = self.generate_unique_sequence()

        # Create KeyValueMessage object and wrap ConnectClient object inside it
        kv_message_instance = KeyValueStore_pb2.KeyValueMessage()
        kv_message_instance.connect_client.src = "coordinator"
        kv_message_instance.connect_client.transaction_id = l_seq

        # Send KeyValueMessage to client socket
        data = kv_message_instance.SerializeToString()
        size = encode_varint(len(data))
        client_socketIn.sendall(size + data)

        print("Unique transaction_id = ", l_seq, " sent to respective ", requestorIn, file=sys.stderr)

        while True:
            # Identify message type by parsing client socket
            kv_message_instance = KeyValueStore_pb2.KeyValueMessage()
            data = client_socketIn.recv(1)
            size = decode_varint(data)
            kv_message_instance.ParseFromString(client_socketIn.recv(size))

            if kv_message_instance.WhichOneof('key_value_message') == 'client_request':
                # Parse request parameters
                req_type = kv_message_instance.client_request.request_type
                req_trans_id = kv_message_instance.client_request.transaction_id
                req_request_id = kv_message_instance.client_request.request_id
                req_consistency_level = kv_message_instance.client_request.consistency_level
                response_hold_counter = req_consistency_level

                if req_type == "get":
                    # Local variables
                    req_key = kv_message_instance.client_request.get_request.key
                    req_value = ""
                    req_timestamp = ""

                    if DEBUG_STDERR:
                        print("(a) GET :: request received for Key = ", req_key, file=sys.stderr)

                    # Find the owner replicas for input key - ByteOrderedPartitioner
                    index = int(math.floor(req_key / 64.0))
                    replica_list = []
                    KeyValueReplicaStore.global_replica_info_list_mutex.acquire()
                    replica_list[0] = KeyValueReplicaStore.global_replica_info_list[index % 4].replica_name
                    replica_list[1] = KeyValueReplicaStore.global_replica_info_list[(index + 1) % 4].replica_name
                    replica_list[2] = KeyValueReplicaStore.global_replica_info_list[(index + 2) % 4].replica_name
                    KeyValueReplicaStore.global_replica_info_list_mutex.release()

                    # TODO - for specific consistency level - ONE, QUORUM

                    # Send response immediately to client for ONE consistency level and if coordinator holds the key
                    if req_consistency_level == 1 and (self.replica_name == replica_list[0] or
                                                       self.replica_name == replica_list[1] or
                                                       self.replica_name == replica_list[2]):
                        response_hold_counter -= 1

                    # Send ReplicaRequest messages to all responsible replicas to serve client request
                    for rep_name in replica_list:
                        if rep_name != self.replica_name:
                            # Create KeyValueMessage object and wrap ConnectClient object inside it
                            kv_message_replica_req = KeyValueStore_pb2.KeyValueMessage()
                            kv_message_replica_req.replica_request.src_replica_name = self.replica_name
                            kv_message_replica_req.replica_request.dest_replica_name = rep_name
                            kv_message_replica_req.replica_request.request_type = req_type
                            kv_message_replica_req.replica_request.transaction_id = req_trans_id
                            kv_message_replica_req.replica_request.get_request.key = req_key

                            # Extract replica socket from global_replica_socket_dictionary
                            KeyValueReplicaStore.global_replica_socket_dict_mutex.acquire()
                            replica_socket = KeyValueReplicaStore.global_replica_socket_dictionary[rep_name]
                            KeyValueReplicaStore.global_replica_socket_dict_mutex.release()

                            # Send KeyValueMessage to coordinator socket
                            data = kv_message_replica_req.SerializeToString()
                            size = encode_varint(len(data))
                            replica_socket.sendall(size + data)

                    # Hold sending response to client till the time counter becomes 0
                    while response_hold_counter > 0:
                        i = 0

                    # Search key in dictionary
                    global_key_value_dict_mutex.acquire()
                    if req_key in global_key_value_dictionary:
                        (req_value, req_timestamp) = global_key_value_dictionary[req_key]
                    global_key_value_dict_mutex.release()

                    if DEBUG_STDERR:
                        print("(b) GET :: value for given key is ", (req_value, req_timestamp), file=sys.stderr)

                    # Create KeyValueMessage object and wrap ConnectClient object inside it
                    kv_message_response = KeyValueStore_pb2.KeyValueMessage()
                    kv_message_response.coordinator_response.response_type = req_type
                    kv_message_response.coordinator_response.transaction_id = req_trans_id
                    kv_message_response.coordinator_response.key = req_key
                    kv_message_response.coordinator_response.value = req_value
                    if req_value != "":
                        kv_message_response.coordinator_response.message = "success"
                    else:
                        kv_message_response.coordinator_response.message = "fail"

                    # Send KeyValueMessage to coordinator socket
                    data = kv_message_response.SerializeToString()
                    size = encode_varint(len(data))
                    client_socketIn.sendall(size + data)
                    if DEBUG_STDERR:
                        print("(c) GET :: sent response to client", file=sys.stderr)

                elif req_type == "put":
                    # TODO - for specific consistency level
                    req_key = kv_message_instance.client_request.put_request.key
                    req_value = kv_message_instance.client_request.put_request.value

                    if DEBUG_STDERR:
                        print("PUT request received with (Key, Value) = ", (req_key, req_value), file=sys.stderr)

                    # Before writing in-memory dictionary, write log at persistent storage
                    filename = os.getcwd() + "/" + self.replica_name + "_write_ahead.log"
                    global_log_file_mutex.acquire()
                    fd = open(filename, 'a')
                    kv_timestamp = str(datetime.datetime.now())
                    fd.write(str(req_key) + ":" + req_value + ":" + kv_timestamp + "\n")
                    fd.close()
                    global_log_file_mutex.release()

                    # Add key in in-memory dictionary
                    global_key_value_dict_mutex.acquire()
                    global_key_value_dictionary[req_key] = (req_value, kv_timestamp)
                    global_key_value_dict_mutex.release()

                    # Create KeyValueMessage object and wrap ConnectClient object inside it
                    kv_message_response = KeyValueStore_pb2.KeyValueMessage()
                    kv_message_response.coordinator_response.response_type = req_type
                    kv_message_response.coordinator_response.transaction_id = req_trans_id
                    kv_message_response.coordinator_response.key = req_key
                    kv_message_response.coordinator_response.value = req_value
                    kv_message_response.coordinator_response.message = "success"

                    # Send KeyValueMessage to coordinator socket
                    data = kv_message_response.SerializeToString()
                    size = encode_varint(len(data))
                    client_socketIn.sendall(size + data)

    # Receive InitReplica message object message from controller via socket
    def receive_InitReplica_message(self):
        # Initialize global static variables
        for b in self.kv_message_instance.init_replica.all_replicas:
            KeyValueReplicaStore.global_replica_info_list.append(ReplicaInfo(b.name, b.ip, b.port))

    # Set up TCP Full Duplex connection with all the replicas in the system which have greater name than itself
    def setup_replica_connections(self):
        for replica in KeyValueReplicaStore.global_replica_info_list:
            # Condition check to maintain Lexicographical order
            if self.replica_name < replica.replica_name:
                # Create client socket IPv4 and TCP
                try:
                    other_replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                except:
                    print("ERROR : Socket creation failed.")
                    sys.exit(1)

                # Connect client socket to server using 3 way handshake and send replica name as message
                other_replica_socket.connect((replica.replica_ip_address, replica.replica_port_number))

                # Create KeyValueMessage object and wrap ConnectReplica object inside it
                kv_message_instance = KeyValueStore_pb2.KeyValueMessage()
                kv_message_instance.connect_replica.replica_name = self.replica_name

                # Send InitReplica message to bank replica socket
                data = kv_message_instance.SerializeToString()
                size = encode_varint(len(data))
                other_replica_socket.sendall(size + data)

                # Store with key - other replica name and value - other replica socket object
                self.add_global_replica_socket_dictionary(replica.replica_name, other_replica_socket)

    # Method to add entry in global_replica_socket_dictionary
    def add_global_replica_socket_dictionary(self, r_name, r_socket):
        KeyValueReplicaStore.global_replica_socket_dict_mutex.acquire()
        KeyValueReplicaStore.global_replica_socket_dictionary[r_name] = r_socket
        KeyValueReplicaStore.global_replica_socket_dict_mutex.release()

    # Method to generate unique sequence number
    def generate_unique_sequence(self):
        seq = ""
        KeyValueReplicaStore.global_client_sequencer_mutex.acquire()
        KeyValueReplicaStore.global_client_sequencer += 1
        seq = self.replica_name + "_" + str(KeyValueReplicaStore.global_client_sequencer)
        KeyValueReplicaStore.global_client_sequencer_mutex.release()
        return seq


# Class to represent Replica Server functionality
class ReplicaSocketServer:
    """ Class to represent Replica Server functionality """

    # Constructor
    def __init__(self, arg_replica_nameIn, arg_replica_portIn, arg_replica_consistency_mechanismIn):
        self.r_name = arg_replica_nameIn
        self.r_port = arg_replica_portIn
        self.r_consistency_mechanism = arg_replica_consistency_mechanismIn
        self.r_ip = socket.gethostbyname(socket.gethostname())
        self.r_backlog_count = 100
        self.r_thread_list = []  # holds total active thread
        # Create server socket (IPv4 and TCP)
        try:
            self.r_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except:
            print("ERROR : Socket creation failed.")
            sys.exit(1)

    # Bind replica socket with given port
    def start_replica(self):
        try:
            self.r_socket.bind(('', self.r_port))
        except:
            print("ERROR : Socket bind failed.")
            sys.exit(1)

        print("\n----------------------------------------------------------------------------------------------------", file=sys.stderr)
        print("Key Value Replica Store Information :\n----------------------------", file=sys.stderr)
        print("Replica Name ::::::::::::::::::::::: \t", self.r_name, file=sys.stderr)
        print("Replica IP Address ::::::::::::::::: \t", self.r_ip, file=sys.stderr)
        print("Replica Port Number :::::::::::::::: \t", self.r_port, file=sys.stderr)
        print("Replica Consistency Level :::::::::: \t", self.r_consistency_mechanism, file=sys.stderr)
        print("Replica Key-Value Dictionary ::::::: \t", global_key_value_dictionary, file=sys.stderr)
        print("----------------------------------------------------------------------------------------------------", file=sys.stderr)

        # Replica socket in listening mode with provided backlog count
        self.r_socket.listen(self.r_backlog_count)
        print("\nReplica Server is running.....\n", file=sys.stderr)

    # Accept client request at replica socket
    def accept_connections(self):
        while True:
            try:
                # Accept create new socket for each client request and return tuple
                client_socket, client_address = self.r_socket.accept()

                # Handle client's request using separate thread
                key_value_replica_thread = KeyValueReplicaStore(client_socket, client_address, self.r_name, self.r_ip, self.r_port, self.r_consistency_mechanism)
                key_value_replica_thread.start()

                # Save running threads in list
                self.r_thread_list.append(key_value_replica_thread)
            except:
                print("ERROR : Socket accept failed.")
                sys.exit(1)

        # Main thread should wait till all threads complete their operation
        for cur_thread in self.r_thread_list:
            cur_thread.join()


# Starting point for Key Value Replica Store
if __name__ == "__main__":
    # Validating command line arguments
    if len(sys.argv) != 4:
        print("ERROR : Invalid # of command line arguments. Expected 4 arguments.")
        sys.exit(1)

    # Local variables
    arg_replica_name = sys.argv[1]
    arg_replica_port = int(sys.argv[2])
    arg_replica_consistency_mechanism = sys.argv[3]

    # Populate dictionary using write ahead log file (if applicable)
    check_write_ahead_log(arg_replica_name)

    # Create replica socket and start accepting client request using multi-threading
    replicaSocketServer = ReplicaSocketServer(arg_replica_name, arg_replica_port, arg_replica_consistency_mechanism)
    replicaSocketServer.start_replica()
    replicaSocketServer.accept_connections()
