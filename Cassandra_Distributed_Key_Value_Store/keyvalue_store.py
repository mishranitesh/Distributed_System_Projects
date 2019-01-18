#!usr/bin/python3

import os
import sys
import math
import time
import queue
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


# Class to represent Replica Information
class ReplicaInfo:
    """Class to represent Replica Information"""

    def __init__(self, r_nameIn, r_ipIn, r_portIn):
        self.replica_name = r_nameIn
        self.replica_ip_address = r_ipIn
        self.replica_port_number = int(r_portIn)


# Class to hold information returned via replica store response
class ReplicaReadRepairInfo:
    """Class to hold information returned via replica store response"""

    def __init__(self, r_nameIn, keyIn, valueIn, timestampIn, statusIn):
        self.rr_replica_name = r_nameIn
        self.rr_key = keyIn
        self.rr_value = valueIn
        self.rr_timestamp = timestampIn
        self.rr_status = statusIn


# Class to hold information to be sent to replicas as a Hint
class HintRepairInfo:
    """Class to hold information to be sent to replicas as a Hint"""

    def __init__(self, r_nameIn, keyIn, valueIn, timestampIn):
        self.hh_replica_name = r_nameIn
        self.hh_key = keyIn
        self.hh_value = valueIn
        self.hh_timestamp = timestampIn


# Method to create new replica info
def create_replica_info(file_lineIn):
    replica_name, replica_ip_address, replica_port_number = file_lineIn.split(" ")
    return ReplicaInfo(replica_name.strip(), replica_ip_address.strip(), replica_port_number.strip())


# Global Dictionary to hold key value pair
global_key_value_dictionary = {}
# Global mutex to prevent concurrent access of global_key_value_dictionary
global_key_value_dict_mutex = threading.Lock()

# Static Variable to hold all active replicas in the system
global_replica_info_dict = {}
# Mutex to prevent global_replica_info_dict
global_replica_info_dict_mutex = threading.Lock()

# Global mutex to prevent write ahead log file access
global_log_file_mutex = threading.Lock()

# Global dictionary to hold replica responses per client request_id
global_track_response_dictionary = {}
# Mutex to prevent global_track_response_dictionary
global_track_response_dict_mutex = threading.Lock()

# Global dictionary to hold Hint Messages for Hinted Handoff Consistency Mechanism
global_hint_message_dictionary = {}
# Mutex to prevent global_hint_message_dictionary
global_hint_message_dict_mutex = threading.Lock()


# Function to populate dictionary using write ahead log file (if applicable)
def check_write_ahead_log(arg_replica_nameIn):
    filename = os.getcwd() + "/" + arg_replica_nameIn + "_write_ahead.log"
    if os.path.isfile(filename):
        global_log_file_mutex.acquire()
        with open(filename, 'r') as fp:
            global_key_value_dict_mutex.acquire()
            for line in fp:
                f_key, f_value, f_timestamp = line.split(";")
                global_key_value_dictionary[int(f_key.strip())] = (f_value.strip(), f_timestamp.strip())
            global_key_value_dict_mutex.release()
        fp.close()
        global_log_file_mutex.release()
    else:
        # Create new empty file (touch filename)
        os.mknod(filename)


# Class to represent Key-Value Replica Store
class KeyValueReplicaStore(threading.Thread):
    """Class to represent Key-Value Replica Store"""

    # List to hold daemon threads
    global_daemon_thread_list = []
    # Mutex to prevent global_daemon_thread_list
    global_daemon_thread_list_mutex = threading.Lock()

    # Static variable to hold global sequencer
    global_client_sequencer = 0
    # Mutex to prevent global_client_sequencer
    global_client_sequencer_mutex = threading.Lock()

    # Constructor
    def __init__(self, incoming_socketIn, incoming_socket_addressIn, r_nameIn, r_ipIn, r_portIn,
                 r_consistency_mechanismIn):
        threading.Thread.__init__(self)
        self.incoming_socket = incoming_socketIn
        self.incoming_socket_address = incoming_socket_addressIn
        self.replica_name = r_nameIn
        self.replica_ip = r_ipIn
        self.replica_port = r_portIn
        self.replica_consistency_mechanism = r_consistency_mechanismIn
        self.kv_message_instance = KeyValueStore_pb2.KeyValueMessage()

    # Method to create new socket for input replicaName
    def create_socket_using_replica_name(self, r_nameIn):
        r_ip, r_port = ("", 0)
        global_replica_info_dict_mutex.acquire()
        for k, v in global_replica_info_dict.items():
            if v.replica_name == r_nameIn:
                (r_ip, r_port) = v.replica_ip_address, v.replica_port_number
        global_replica_info_dict_mutex.release()

        # Create client socket IPv4 and TCP
        try:
            r_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except:
            print("ERROR : Socket creation failed.")
            sys.exit(1)

        # Return socket if connection established successfully otherwise return error
        r_msg = ""
        if r_ip != "" and r_port != 0:
            r_msg = "success"
            # Connect client socket to server using 3 way handshake
            try:
                r_socket.connect((r_ip, r_port))
            except:
                r_msg = "fail"
                if DEBUG_STDERR:
                    print("ERROR : Creating new socket for replica --> ", r_nameIn, " is failed (Replica is crashed).", file=sys.stderr)
        return r_msg, r_socket

    # Method (run) works as entry point for each thread - overridden from threading.Thread
    def run(self):
        # Identify message type here by parsing incoming socket message
        data = self.incoming_socket.recv(1)
        size = decode_varint(data)
        self.kv_message_instance.ParseFromString(self.incoming_socket.recv(size))

        # Received Request to First Time Setup Connection
        if self.kv_message_instance.WhichOneof('key_value_message') == 'init_replica':
            print("\nINIT_REPLICA message received from Setup Connection script.", file=sys.stderr)
            # Initialize global_replica_info_dict
            i = 0
            for b in self.kv_message_instance.init_replica.all_replicas:
                global_replica_info_dict[i] = ReplicaInfo(b.name, b.ip, b.port)
                i += 1
            self.incoming_socket.close()

            if DEBUG_STDERR:
                print("Elements of Global Replica Info Dictionary --> ", file=sys.stderr)
                for rep_key, rep_val in global_replica_info_dict.items():
                    print(rep_val.replica_name, rep_val.replica_ip_address, rep_val.replica_port_number, file=sys.stderr)

        elif self.kv_message_instance.WhichOneof('key_value_message') == 'connect_client':
            print("\n---------------------------------------------------------------------------------------------------", file=sys.stderr)
            print("CONNECT_CLIENT message received from Client.", file=sys.stderr)
            # Handle each client request using separate thread
            client_thread = threading.Thread(name='Handle_Client_Request', target=self.handle_client_request,
                                             args=("client", self.incoming_socket), daemon=True)
            client_thread.start()
            KeyValueReplicaStore.global_daemon_thread_list_mutex.acquire()
            KeyValueReplicaStore.global_daemon_thread_list.append(client_thread)
            KeyValueReplicaStore.global_daemon_thread_list_mutex.release()

        elif self.kv_message_instance.WhichOneof('key_value_message') == 'replica_request':
            # Local variables
            req_src_rep_name = self.kv_message_instance.replica_request.src_replica_name
            req_dest_rep_name = self.kv_message_instance.replica_request.dest_replica_name
            req_type = self.kv_message_instance.replica_request.request_type
            req_trans_id = self.kv_message_instance.replica_request.transaction_id
            req_id = self.kv_message_instance.replica_request.request_id

            # Handle GET and PUT request using conditional statement
            if req_type.lower() == "get":
                # Local variable
                res_msg = ""
                (res_value, res_timestamp) = ("", "")
                req_key = self.kv_message_instance.replica_request.get_request.key

                print("\nREPLICA_REQUEST Received | GET | Key = ", req_key, file=sys.stderr)

                # Get input key from in-memory dictionary
                global_key_value_dict_mutex.acquire()
                if req_key in global_key_value_dictionary:
                    (res_value, res_timestamp) = global_key_value_dictionary[req_key]
                    res_msg = "found"
                else:
                    res_msg = "not_found"
                global_key_value_dict_mutex.release()

                # Create KeyValueMessage object and wrap ConnectClient object inside it
                kv_message_rep_res = KeyValueStore_pb2.KeyValueMessage()
                kv_message_rep_res.replica_response.src_replica_name = req_dest_rep_name
                kv_message_rep_res.replica_response.dest_replica_name = req_src_rep_name
                kv_message_rep_res.replica_response.response_type = "get"
                kv_message_rep_res.replica_response.transaction_id = req_trans_id
                kv_message_rep_res.replica_response.request_id = req_id
                kv_message_rep_res.replica_response.key = req_key
                kv_message_rep_res.replica_response.value = res_value
                kv_message_rep_res.replica_response.timestamp = res_timestamp
                kv_message_rep_res.replica_response.message = res_msg

                # Create new socket for Coordinator
                cord_msg, cord_socket = self.create_socket_using_replica_name(req_src_rep_name)

                # Send KeyValueMessage to Coordinator socket
                if cord_msg == "success":
                    data = kv_message_rep_res.SerializeToString()
                    size = encode_varint(len(data))
                    cord_socket.sendall(size + data)
                cord_socket.close()

                print("REPLICA_REQUEST | GET SERVED | Sent Message Status = ", res_msg, file=sys.stderr)

            elif req_type.lower() == "put":
                # Local variable
                res_msg = "success"
                req_key = self.kv_message_instance.replica_request.put_request.key
                req_value = self.kv_message_instance.replica_request.put_request.value
                req_timestamp = self.kv_message_instance.replica_request.put_request.timestamp

                print("\nREPLICA_REQUEST Received | PUT | (Key, Value, Timestamp) = ", (req_key, req_value, req_timestamp), file=sys.stderr)

                # Check for latest or new Key Value pair coming in message
                global_key_value_dict_mutex.acquire()
                is_change_required = False
                if req_key in global_key_value_dictionary:
                    kv_pair_val, kv_pair_ts = global_key_value_dictionary[req_key]
                    if kv_pair_ts < req_timestamp:
                        is_change_required = True
                else:
                    is_change_required = True

                # Modify Kev-Value pair only if it is not present or it is stale
                if is_change_required:
                    # Before writing in-memory dictionary, write log at persistent storage
                    filename = os.getcwd() + "/" + self.replica_name + "_write_ahead.log"
                    global_log_file_mutex.acquire()
                    fd = open(filename, 'a')
                    fd.write(str(req_key) + ";" + req_value + ";" + req_timestamp + "\n")
                    fd.close()
                    global_log_file_mutex.release()

                    # Add key to in-memory dictionary
                    global_key_value_dictionary[req_key] = (req_value, req_timestamp)
                global_key_value_dict_mutex.release()

                # Create KeyValueMessage object and wrap ConnectClient object inside it
                kv_message_rep_res = KeyValueStore_pb2.KeyValueMessage()
                kv_message_rep_res.replica_response.src_replica_name = req_dest_rep_name
                kv_message_rep_res.replica_response.dest_replica_name = req_src_rep_name
                kv_message_rep_res.replica_response.response_type = "put"
                kv_message_rep_res.replica_response.transaction_id = req_trans_id
                kv_message_rep_res.replica_response.request_id = req_id
                kv_message_rep_res.replica_response.key = req_key
                kv_message_rep_res.replica_response.value = req_value
                kv_message_rep_res.replica_response.timestamp = req_timestamp
                kv_message_rep_res.replica_response.message = res_msg

                # Create new socket for Coordinator
                cord_msg, cord_socket = self.create_socket_using_replica_name(req_src_rep_name)

                # Send KeyValueMessage to Coordinator socket
                if cord_msg == "success":
                    data = kv_message_rep_res.SerializeToString()
                    size = encode_varint(len(data))
                    cord_socket.sendall(size + data)
                cord_socket.close()
                print("REPLICA_REQUEST | PUT SERVED | Sent Message Status = ", res_msg, file=sys.stderr)

            # Logic to make replicas consistent using Hinted Handoff mechanism
            if self.replica_consistency_mechanism == "Hinted_Handoff":
                global_hint_message_dict_mutex.acquire()
                if req_src_rep_name in global_hint_message_dictionary:
                    rep_queue = global_hint_message_dictionary[req_src_rep_name]
                    while not rep_queue.empty():
                        rh_instance = rep_queue.get()
                        # Create new socket for Replica
                        rh_msg, rh_socket = self.create_socket_using_replica_name(rh_instance.hh_replica_name)
                        if rh_msg == "success":
                            # Create HintedHandoff message and send to respective replica socket
                            kv_hh_message = KeyValueStore_pb2.KeyValueMessage()
                            kv_hh_message.hinted_handoff.key = rh_instance.hh_key
                            kv_hh_message.hinted_handoff.value = rh_instance.hh_value
                            kv_hh_message.hinted_handoff.timestamp = rh_instance.hh_timestamp
                            # Send to socket
                            data = kv_hh_message.SerializeToString()
                            size = encode_varint(len(data))
                            rh_socket.sendall(size + data)
                        else:
                            break
                global_hint_message_dict_mutex.release()

            print("CURRENT ITEMS OF IN-MEMORY DICTIONARY:", file=sys.stderr)
            global_key_value_dict_mutex.acquire()
            for k, v in global_key_value_dictionary.items():
                print(k, " --> ", v, file=sys.stderr)
            global_key_value_dict_mutex.release()

        elif self.kv_message_instance.WhichOneof('key_value_message') == 'replica_response':
            # Local variables
            ip_src_r_name = self.kv_message_instance.replica_response.src_replica_name
            ip_dest_replica_name = self.kv_message_instance.replica_response.dest_replica_name
            ip_response_type = self.kv_message_instance.replica_response.response_type
            ip_transaction_id = self.kv_message_instance.replica_response.transaction_id
            ip_request_id = self.kv_message_instance.replica_response.request_id
            ip_key = self.kv_message_instance.replica_response.key
            ip_value = self.kv_message_instance.replica_response.value
            ip_timestamp = self.kv_message_instance.replica_response.timestamp
            ip_status_message = self.kv_message_instance.replica_response.message

            print("\nREPLICA_RESPONSE | From Replica = ", ip_src_r_name, " with Status = ", ip_status_message, " and Request_Id = ", ip_request_id, file=sys.stderr)

            # Update global_track_response_dictionary to save each replica's response --> For both GET / PUT
            global_track_response_dict_mutex.acquire()
            if ip_request_id in global_track_response_dictionary:
                global_track_response_dictionary[ip_request_id].append(
                    ReplicaReadRepairInfo(ip_src_r_name, ip_key, ip_value, ip_timestamp, ip_status_message))
            else:
                global_track_response_dictionary[ip_request_id] = list()
                global_track_response_dictionary[ip_request_id].append(
                    ReplicaReadRepairInfo(ip_src_r_name, ip_key, ip_value, ip_timestamp, ip_status_message))

            # Perform Read Repair (if applicable) -- GET only
            if ip_response_type == "get":
                if self.replica_consistency_mechanism == "Read_Repair" and len(global_track_response_dictionary[ip_request_id]) == 3:
                    self.perform_read_repair(ip_request_id)
            global_track_response_dict_mutex.release()
            self.incoming_socket.close()

        elif self.kv_message_instance.WhichOneof('key_value_message') == 'read_repair':
            print("READ_REPAIR message received.", file=sys.stderr)
            # Local variables
            req_rr_key = self.kv_message_instance.read_repair.key
            req_rr_value = self.kv_message_instance.read_repair.value
            req_rr_timestamp = self.kv_message_instance.read_repair.timestamp

            # Check for latest or new Key Value pair coming in message
            global_key_value_dict_mutex.acquire()
            is_change_required = False
            if req_rr_key in global_key_value_dictionary:
                kv_pair_val, kv_pair_ts = global_key_value_dictionary[req_rr_key]
                if kv_pair_ts < req_rr_timestamp:
                    is_change_required = True
            else:
                is_change_required = True

            # Modify Kev-Value pair only if it is not present or it is stale
            if is_change_required:
                # Before writing in-memory dictionary, write log at persistent storage
                filename = os.getcwd() + "/" + self.replica_name + "_write_ahead.log"
                global_log_file_mutex.acquire()
                fd = open(filename, 'a')
                fd.write(str(req_rr_key) + ";" + req_rr_value + ";" + req_rr_timestamp + "\n")
                fd.close()
                global_log_file_mutex.release()

                # Add key to in-memory dictionary
                global_key_value_dictionary[req_rr_key] = (req_rr_value, req_rr_timestamp)
            global_key_value_dict_mutex.release()

        elif self.kv_message_instance.WhichOneof('key_value_message') == 'hinted_handoff':
            print("HINTED_HANDOFF message received.", file=sys.stderr)
            # Local variables
            req_hh_key = self.kv_message_instance.hinted_handoff.key
            req_hh_value = self.kv_message_instance.hinted_handoff.value
            req_hh_timestamp = self.kv_message_instance.hinted_handoff.timestamp

            # Check for latest or new Key Value pair coming in message
            global_key_value_dict_mutex.acquire()
            is_change_required = False
            if req_hh_key in global_key_value_dictionary:
                kv_pair_val, kv_pair_ts = global_key_value_dictionary[req_hh_key]
                if kv_pair_ts < req_hh_timestamp:
                    is_change_required = True
            else:
                is_change_required = True

            # Modify Kev-Value pair only if it is not present or it is stale
            if is_change_required:
                # Before writing in-memory dictionary, write log at persistent storage
                filename = os.getcwd() + "/" + self.replica_name + "_write_ahead.log"
                global_log_file_mutex.acquire()
                fd = open(filename, 'a')
                fd.write(str(req_hh_key) + ";" + req_hh_value + ";" + req_hh_timestamp + "\n")
                fd.close()
                global_log_file_mutex.release()

                # Add key to in-memory dictionary
                global_key_value_dictionary[req_hh_key] = (req_hh_value, req_hh_timestamp)
            global_key_value_dict_mutex.release()

        # Main thread should wait till all threads complete their operation
        KeyValueReplicaStore.global_daemon_thread_list_mutex.acquire()
        for cur_thread in KeyValueReplicaStore.global_daemon_thread_list:
            cur_thread.join()
        KeyValueReplicaStore.global_daemon_thread_list_mutex.release()

    # Method to generate unique sequence number
    def generate_unique_sequence(self):
        KeyValueReplicaStore.global_client_sequencer_mutex.acquire()
        KeyValueReplicaStore.global_client_sequencer += 1
        seq = self.replica_name + "_" + str(KeyValueReplicaStore.global_client_sequencer)
        KeyValueReplicaStore.global_client_sequencer_mutex.release()
        return seq

    # Method to perform read repair operation
    def perform_read_repair(self, client_request_idIn):
        rr_list = global_track_response_dictionary[client_request_idIn]
        rr_max_timestamp = ""
        res_rr_key = 0
        res_rr_value = ""
        is_res_found = False
        for rr in rr_list:
            if rr_max_timestamp < rr.rr_timestamp:
                rr_max_timestamp = rr.rr_timestamp
                res_rr_key = rr.rr_key
                res_rr_value = rr.rr_value
            if rr.rr_status == "found":
                is_res_found = True

        # Perform read repair if at least one entry is with found status
        if not is_res_found or rr_max_timestamp == "":
            return
        else:
            # Create ReadRepair message
            kv_rr_message = KeyValueStore_pb2.KeyValueMessage()
            kv_rr_message.read_repair.key = res_rr_key
            kv_rr_message.read_repair.value = res_rr_value
            kv_rr_message.read_repair.timestamp = rr_max_timestamp
            # Create replica socket and send ReadRepair message
            for rr in rr_list:
                if rr.rr_timestamp != rr_max_timestamp:
                    rr_sock_msg, rr_socket = self.create_socket_using_replica_name(rr.rr_replica_name)
                    if rr_sock_msg == "success":
                        data = kv_rr_message.SerializeToString()
                        size = encode_varint(len(data))
                        rr_socket.sendall(size + data)
                    rr_socket.close()

    # Function to handle some of the Coordinator's responsibilities
    # Sending requests to responsible replicas and performing Read Repair if applicable
    def handle_coordinator_task(self, client_request_msgIn, is_client_request_servedIn, kv_timestampIn):
        # Local variables
        cl_req_type = client_request_msgIn.request_type
        cl_req_trans_id = client_request_msgIn.transaction_id
        cl_req_id = client_request_msgIn.request_id
        cl_req_key = 0
        cl_req_value = ""

        if cl_req_type == "get":
            cl_req_key = client_request_msgIn.get_request.key
            print("CLIENT_REQUEST | GET Message | Key = ", cl_req_key, " and Request_Id = ", cl_req_id, file=sys.stderr)
        elif cl_req_type == "put":
            cl_req_key = client_request_msgIn.put_request.key
            cl_req_value = client_request_msgIn.put_request.value
            print("CLIENT_REQUEST | PUT Message | (Key, Value) = ", (cl_req_key, cl_req_value), " and Request_Id = ", cl_req_id, file=sys.stderr)

        # Find the owner replicas for input key - ByteOrderedPartitioner
        index = int(math.floor(cl_req_key / 64.0))
        replica_index_list = [(index + 0) % 4, (index + 1) % 4, (index + 2) % 4]

        if DEBUG_STDERR:
            print("Responsible replicas indices --> ", replica_index_list, file=sys.stderr)

        # Send ReplicaRequest messages to all responsible replicas to serve client request
        if is_client_request_servedIn:
            cord_timestamp = kv_timestampIn
        else:
            cord_timestamp = str(datetime.datetime.now())
        for rep_index in replica_index_list:
            global_replica_info_dict_mutex.acquire()
            rep_name = global_replica_info_dict[rep_index].replica_name
            global_replica_info_dict_mutex.release()
            if rep_name != self.replica_name:
                # Create KeyValueMessage object and wrap ReplicaRequest object inside it
                kv_message_replica_req = KeyValueStore_pb2.KeyValueMessage()
                kv_message_replica_req.replica_request.src_replica_name = self.replica_name
                kv_message_replica_req.replica_request.dest_replica_name = rep_name
                kv_message_replica_req.replica_request.request_type = cl_req_type
                kv_message_replica_req.replica_request.transaction_id = cl_req_trans_id
                kv_message_replica_req.replica_request.request_id = cl_req_id
                # Prepare GET/PUT request
                if cl_req_type == "get":
                    kv_message_replica_req.replica_request.get_request.key = cl_req_key
                elif cl_req_type == "put":
                    kv_message_replica_req.replica_request.put_request.key = cl_req_key
                    kv_message_replica_req.replica_request.put_request.value = cl_req_value
                    kv_message_replica_req.replica_request.put_request.timestamp = cord_timestamp

                # Create new socket for Replica
                rep_msg, rep_socket = self.create_socket_using_replica_name(rep_name)

                # Send KeyValueMessage to Replica socket
                if rep_msg == "success":
                    data = kv_message_replica_req.SerializeToString()
                    size = encode_varint(len(data))
                    rep_socket.sendall(size + data)

                    # Logic to make replicas consistent using Hinted Handoff mechanism
                    if self.replica_consistency_mechanism == "Hinted_Handoff":
                        global_hint_message_dict_mutex.acquire()
                        if rep_name in global_hint_message_dictionary:
                            rep_queue = global_hint_message_dictionary[rep_name]
                            while not rep_queue.empty():
                                rh_instance = rep_queue.get()
                                # Create new socket for Replica
                                rh_msg, rh_socket = self.create_socket_using_replica_name(rh_instance.hh_replica_name)
                                if rh_msg == "success":
                                    # Create HintedHandoff message and send to respective replica socket
                                    kv_hh_message = KeyValueStore_pb2.KeyValueMessage()
                                    kv_hh_message.hinted_handoff.key = rh_instance.hh_key
                                    kv_hh_message.hinted_handoff.value = rh_instance.hh_value
                                    kv_hh_message.hinted_handoff.timestamp = rh_instance.hh_timestamp
                                    # Send to socket
                                    data = kv_hh_message.SerializeToString()
                                    size = encode_varint(len(data))
                                    rh_socket.sendall(size + data)
                                else:
                                    break
                        global_hint_message_dict_mutex.release()
                elif rep_msg == "fail":
                    # Track Replica DOWN response in global_track_response_dict_mutex
                    global_track_response_dict_mutex.acquire()
                    if cl_req_id in global_track_response_dictionary:
                        global_track_response_dictionary[cl_req_id].append(ReplicaReadRepairInfo(rep_name, cl_req_key, "", "", "down"))
                    else:
                        global_track_response_dictionary[cl_req_id] = list()
                        global_track_response_dictionary[cl_req_id].append(ReplicaReadRepairInfo(rep_name, cl_req_key, "", "", "down"))

                    # Record Hint Messages when respective Replica is DOWN -- Only for Hinted Handoff Consistency Mechanism
                    if self.replica_consistency_mechanism == "Hinted_Handoff":
                        if cl_req_type == "put":
                            global_hint_message_dict_mutex.acquire()
                            hint_msg = HintRepairInfo(rep_name, cl_req_key, cl_req_value, cord_timestamp)
                            if rep_name in global_hint_message_dictionary:
                                global_hint_message_dictionary[rep_name].put(hint_msg)
                            else:
                                global_hint_message_dictionary[rep_name] = queue.Queue()
                                global_hint_message_dictionary[rep_name].put(hint_msg)
                            print("HINT MESSAGE SAVED | Details --> ", hint_msg.hh_replica_name, ",", hint_msg.hh_key, ",",
                                  hint_msg.hh_value, ",", hint_msg.hh_timestamp, file=sys.stderr)
                            global_hint_message_dict_mutex.release()

                    # Perform Read Repair (if applicable) -- GET only
                    if cl_req_type == "get":
                        if self.replica_consistency_mechanism == "Read_Repair" and len(global_track_response_dictionary[cl_req_id]) == 3:
                            self.perform_read_repair(cl_req_id)
                    global_track_response_dict_mutex.release()
                rep_socket.close()
            else:
                # Handle GET/PUT request
                if cl_req_type == "get":
                    # Extract Key-Value pair from Coordinator's own dictionary
                    rep_value, rep_timestamp = ("", "")
                    global_key_value_dict_mutex.acquire()
                    if cl_req_key in global_key_value_dictionary:
                        rep_value, rep_timestamp = global_key_value_dictionary[cl_req_key]
                        c_msg = "found"
                    else:
                        c_msg = "not_found"
                    global_key_value_dict_mutex.release()

                    global_track_response_dict_mutex.acquire()
                    if cl_req_id in global_track_response_dictionary:
                        global_track_response_dictionary[cl_req_id].append(
                            ReplicaReadRepairInfo(rep_name, cl_req_key, rep_value, rep_timestamp, c_msg))
                    else:
                        global_track_response_dictionary[cl_req_id] = list()
                        global_track_response_dictionary[cl_req_id].append(
                            ReplicaReadRepairInfo(rep_name, cl_req_key, rep_value, rep_timestamp, c_msg))

                    # Perform Read Repair (if applicable) -- GET only
                    if self.replica_consistency_mechanism == "Read_Repair" and len(global_track_response_dictionary[cl_req_id]) == 3:
                        self.perform_read_repair(cl_req_id)
                    global_track_response_dict_mutex.release()
                elif cl_req_type == "put":
                    # Check for latest or new Key Value pair coming in message
                    global_key_value_dict_mutex.acquire()
                    is_change_required = False
                    if cl_req_key in global_key_value_dictionary:
                        kv_pair_val, kv_pair_ts = global_key_value_dictionary[cl_req_key]
                        if kv_pair_ts < cord_timestamp:
                            is_change_required = True
                    else:
                        is_change_required = True

                    # Modify Kev-Value pair only if it is not present or it is stale
                    if is_change_required:
                        # Before writing in-memory dictionary, write log at persistent storage
                        filename = os.getcwd() + "/" + self.replica_name + "_write_ahead.log"
                        global_log_file_mutex.acquire()
                        fd = open(filename, 'a')
                        fd.write(str(cl_req_key) + ";" + cl_req_value + ";" + cord_timestamp + "\n")
                        fd.close()
                        global_log_file_mutex.release()

                        # Add key to in-memory dictionary
                        global_key_value_dictionary[cl_req_key] = (cl_req_value, cord_timestamp)
                    global_key_value_dict_mutex.release()

                    # Modify global track response dictionary
                    global_track_response_dict_mutex.acquire()
                    if cl_req_id in global_track_response_dictionary:
                        global_track_response_dictionary[cl_req_id].append(
                            ReplicaReadRepairInfo(rep_name, cl_req_key, cl_req_value, cord_timestamp, "success"))
                    else:
                        global_track_response_dictionary[cl_req_id] = list()
                        global_track_response_dictionary[cl_req_id].append(
                            ReplicaReadRepairInfo(rep_name, cl_req_key, cl_req_value, cord_timestamp, "success"))
                    global_track_response_dict_mutex.release()

    # Function to handle client request by Coordinator
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

        print("Unique transaction_id = ", l_seq, " generated by Coordinator and sent to ", requestorIn,
              "\n---------------------------------------------------------------------------------------------------\n", file=sys.stderr)

        while True:
            # Identify message type by parsing client socket
            kv_client_request = KeyValueStore_pb2.KeyValueMessage()
            try:
                data = client_socketIn.recv(1)
                size = decode_varint(data)
                kv_client_request.ParseFromString(client_socketIn.recv(size))
            except:
                print("\n---------------------------------------------------------------------------------------------------", file=sys.stderr)
                print("MESSAGE :: CLIENT REVOKED IT'S CONNECTION WITH CURRENT COORDINATOR.", file=sys.stderr)
                print("---------------------------------------------------------------------------------------------------\n", file=sys.stderr)
                break

            # Check for KeyValueMessage type - Just recheck here
            if kv_client_request.WhichOneof('key_value_message') == 'client_request':
                print("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~", file=sys.stderr)
                # Parse request parameters
                req_type = kv_client_request.client_request.request_type
                req_trans_id = kv_client_request.client_request.transaction_id
                req_request_id = kv_client_request.client_request.request_id
                req_consistency_level = kv_client_request.client_request.consistency_level
                is_client_request_served = False
                kv_timestamp = ""

                # Logic to maintain ONE consistent level and when coordinator is owner for the key
                if req_consistency_level == 1:
                    req_key = 0
                    req_value = ""
                    if req_type == "get":
                        req_key = kv_client_request.client_request.get_request.key
                    elif req_type == "put":
                        req_key = kv_client_request.client_request.put_request.key
                        req_value = kv_client_request.client_request.put_request.value

                    # Find the owner replicas for input key - ByteOrderedPartitioner
                    index = int(math.floor(req_key / 64.0))
                    replica_indices = [(index + 0) % 4, (index + 1) % 4, (index + 2) % 4]

                    is_coordinator_owner = False
                    for r_index in replica_indices:
                        global_replica_info_dict_mutex.acquire()
                        r_name = global_replica_info_dict[r_index].replica_name
                        global_replica_info_dict_mutex.release()
                        if r_name == self.replica_name:
                            is_coordinator_owner = True
                            break

                    # Create response for Client and send immediately
                    if is_coordinator_owner:
                        if DEBUG_STDERR:
                            print("Coordinator is the one of the OWNER of the key.", file=sys.stderr)

                        if req_type == "get":
                            res_value, res_timestamp = ("", "")
                            # Search key in own dictionary
                            global_key_value_dict_mutex.acquire()
                            if req_key in global_key_value_dictionary:
                                (res_value, res_timestamp) = global_key_value_dictionary[req_key]
                            global_key_value_dict_mutex.release()

                            if res_value != "":
                                # Create KeyValueMessage object and wrap CoordinatorResponse object inside it
                                kv_message_response = KeyValueStore_pb2.KeyValueMessage()
                                kv_message_response.coordinator_response.response_type = req_type
                                kv_message_response.coordinator_response.transaction_id = req_trans_id
                                kv_message_response.coordinator_response.key = req_key
                                kv_message_response.coordinator_response.value = res_value
                                kv_message_response.coordinator_response.message = "success"
                                # Send KeyValueMessage to coordinator socket
                                data = kv_message_response.SerializeToString()
                                size = encode_varint(len(data))
                                client_socketIn.sendall(size + data)
                                is_client_request_served = True

                                print("GET | ONE Consistency Level | Sent response to client", file=sys.stderr)
                        elif req_type == "put":
                            kv_timestamp = str(datetime.datetime.now())
                            # Check for latest or new Key Value pair coming in message
                            global_key_value_dict_mutex.acquire()
                            is_change_required = False
                            if req_key in global_key_value_dictionary:
                                kv_pair_val, kv_pair_ts = global_key_value_dictionary[req_key]
                                if kv_pair_ts < kv_timestamp:
                                    is_change_required = True
                            else:
                                is_change_required = True

                            # Modify Kev-Value pair only if it is not present or it is stale
                            if is_change_required:
                                # Before writing in-memory dictionary, write log at persistent storage
                                filename = os.getcwd() + "/" + self.replica_name + "_write_ahead.log"
                                global_log_file_mutex.acquire()
                                fd = open(filename, 'a')
                                fd.write(str(req_key) + ";" + req_value + ";" + kv_timestamp + "\n")
                                fd.close()
                                global_log_file_mutex.release()

                                # Add key to in-memory dictionary
                                global_key_value_dictionary[req_key] = (req_value, kv_timestamp)
                            global_key_value_dict_mutex.release()

                            # Create KeyValueMessage object and wrap CoordinatorResponse object inside it
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
                            is_client_request_served = True

                            print("PUT | ONE Consistency Level | Sent response to client", file=sys.stderr)

                # Create new thread and dedicate responsibility to send messages to other replicas and do read repair if applicable
                cord_thread = threading.Thread(name='Handle_Client_Request', target=self.handle_coordinator_task,
                                               args=(kv_client_request.client_request, is_client_request_served, kv_timestamp), daemon=False)
                cord_thread.start()

                # Logic to monitor responses from other replicas
                if not is_client_request_served:
                    while True:
                        global_track_response_dict_mutex.acquire()
                        if req_request_id not in global_track_response_dictionary or \
                                req_consistency_level >= len(global_track_response_dictionary[req_request_id]):
                            global_track_response_dict_mutex.release()
                            time.sleep(0.005)
                            continue

                        # Check for most recent value from dictionary
                        rr_obj_list = global_track_response_dictionary[req_request_id]
                        global_track_response_dict_mutex.release()
                        max_ts = ""
                        new_key = 0
                        new_val = ""
                        not_found_counter = 0
                        for rr_obj in rr_obj_list:
                            if req_consistency_level == 1 and (rr_obj.rr_status == "found" or rr_obj.rr_status == "success"):
                                new_key = rr_obj.rr_key
                                new_val = rr_obj.rr_value
                                is_client_request_served = True
                                break
                            elif req_consistency_level == 2 and (rr_obj.rr_status == "found" or rr_obj.rr_status == "success"):
                                if max_ts != "":
                                    if max_ts < rr_obj.rr_timestamp:
                                        max_ts = rr_obj.rr_timestamp
                                        new_key = rr_obj.rr_key
                                        new_val = rr_obj.rr_value
                                    is_client_request_served = True
                                    break
                                else:
                                    max_ts = rr_obj.rr_timestamp
                                    new_key = rr_obj.rr_key
                                    new_val = rr_obj.rr_value

                            if rr_obj.rr_status != "found" and rr_obj.rr_status != "success":
                                not_found_counter += 1

                        # Send exception message to client when all Replica Responses are "Not Found or Down"
                        if not_found_counter >= 3:
                            # Create KeyValueMessage object and wrap CoordinatorResponse object inside it
                            kv_message_response = KeyValueStore_pb2.KeyValueMessage()
                            kv_message_response.coordinator_response.message = "fail:Key Not Found"
                            # Send KeyValueMessage to coordinator socket
                            data = kv_message_response.SerializeToString()
                            size = encode_varint(len(data))
                            client_socketIn.sendall(size + data)
                            is_client_request_served = True
                            print("FAIL | Key Not Found | Sent response to client", file=sys.stderr)
                            break
                        elif len(rr_obj_list) == 3 and not is_client_request_served:
                            # Create KeyValueMessage object and wrap CoordinatorResponse object inside it
                            kv_message_response = KeyValueStore_pb2.KeyValueMessage()
                            kv_message_response.coordinator_response.message = "fail:Not able to get sufficient response for QUORUM."
                            # Send KeyValueMessage to coordinator socket
                            data = kv_message_response.SerializeToString()
                            size = encode_varint(len(data))
                            client_socketIn.sendall(size + data)
                            is_client_request_served = True
                            print("FAIL | Not able to get sufficient response for QUORUM | Sent response to client", file=sys.stderr)
                            break
                        elif is_client_request_served:
                            # Create KeyValueMessage object and wrap CoordinatorResponse object inside it
                            kv_message_response = KeyValueStore_pb2.KeyValueMessage()
                            kv_message_response.coordinator_response.response_type = req_type
                            kv_message_response.coordinator_response.transaction_id = req_trans_id
                            kv_message_response.coordinator_response.key = new_key
                            kv_message_response.coordinator_response.value = new_val
                            kv_message_response.coordinator_response.message = "success"
                            # Send KeyValueMessage to coordinator socket
                            data = kv_message_response.SerializeToString()
                            size = encode_varint(len(data))
                            client_socketIn.sendall(size + data)
                            is_client_request_served = True
                            print("SUCCESS | Sent response to client", file=sys.stderr)
                            break

                    if not is_client_request_served:
                        # Wait for thread to complete background tasks
                        cord_thread.join()


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
        print("Key Value Replica Store Information : \n----------------------------", file=sys.stderr)
        print("Replica Name :::::::::::::::::::::::: \t", self.r_name, file=sys.stderr)
        print("Replica IP Address :::::::::::::::::: \t", self.r_ip, file=sys.stderr)
        print("Replica Port Number ::::::::::::::::: \t", self.r_port, file=sys.stderr)
        print("Replica Consistency Mechanism ::::::: \t", self.r_consistency_mechanism, file=sys.stderr)
        print("Replica Key-Value Dictionary Items :: \t", file=sys.stderr)
        global_key_value_dict_mutex.acquire()
        for k, v in global_key_value_dictionary.items():
            print(k, v, file=sys.stderr)
        global_key_value_dict_mutex.release()
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
                key_value_replica_thread = KeyValueReplicaStore(client_socket, client_address, self.r_name, self.r_ip, self.r_port,
                                                                self.r_consistency_mechanism)
                key_value_replica_thread.start()

                # Save running threads in list
                self.r_thread_list.append(key_value_replica_thread)

            except:
                print("ERROR : Socket accept failed.")
                sys.exit(1)

        # Main thread should wait till all threads complete their operation
        for cur_thread in self.r_thread_list:
            cur_thread.join()


# Starting point for Key Value Store
if __name__ == "__main__":
    # Validating command line arguments
    if len(sys.argv) != 6:
        print("ERROR : Invalid # of command line arguments. Expected 5 arguments.")
        sys.exit(1)

    # Local variables
    arg_replica_name = sys.argv[1]
    arg_replica_port = int(sys.argv[2])
    arg_replica_consistency_mechanism = sys.argv[3]
    arg_replica_filename = sys.argv[4]
    arg_use_file_flag = sys.argv[5]  # yes or no

    # Populate dictionary using write ahead log file (if applicable)
    check_write_ahead_log(arg_replica_name)

    # Open file, sort using replica name and store in list of global_replica_info_dict
    if arg_use_file_flag.lower() == "yes":
        replica_info_list = sorted([create_replica_info(l[:-1])
                                    for l in open(arg_replica_filename)], key=lambda x: x.replica_name)
        i = 0
        for rep_ele in replica_info_list:
            global_replica_info_dict[i] = rep_ele
            i += 1

        if DEBUG_STDERR:
            print("Elements of Global Replica Info Dictionary --> ", file=sys.stderr)
            for rep_key, rep_val in global_replica_info_dict.items():
                print(rep_key, " --> ", rep_val.replica_name, rep_val.replica_ip_address, rep_val.replica_port_number, file=sys.stderr)

    # Create replica socket and start accepting client request using multi-threading
    replicaSocketServer = ReplicaSocketServer(arg_replica_name, arg_replica_port, arg_replica_consistency_mechanism)
    replicaSocketServer.start_replica()
    replicaSocketServer.accept_connections()
