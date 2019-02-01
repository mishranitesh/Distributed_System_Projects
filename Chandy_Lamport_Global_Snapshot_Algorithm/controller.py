#!usr/bin/python3

import re
import sys
import time
import pickle
import socket
import random
import threading
from os import path
from constants import *

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import bank_pb2
from google.protobuf.internal.encoder import _VarintEncoder
from google.protobuf.internal.decoder import _DecodeVarint


def encode_varint(value):
    """ Encode an int as a protobuf varint """
    data = []
    _VarintEncoder()(data.append, value, False)
    return b''.join(data)


def decode_varint(data):
    """ Decode a protobuf varint to an int """
    return _DecodeVarint(data, 0)[0]

# Class to represent Branch Information
class BranchInfo:
    """Class to represent Branch Information"""

    def __init__(self, b_nameIn, b_ipIn, b_portIn):
        self.branch_name = b_nameIn
        self.branch_ip_address = b_ipIn
        self.branch_port_number = int(b_portIn)


# Method to create new branch info
def create_branch_info(file_lineIn):
    branch_name, branch_ip_address, branch_port_number = file_lineIn.split(" ")
    return BranchInfo(branch_name.strip(), branch_ip_address.strip(), branch_port_number.strip())


# Class to represent Controller
class Controller:
    """Branches rely on a controller to set their initial balances and get notified of all branches in the distributed bank."""

    # Static variable to hold Mapping of Branch name with respective TCP socket
    global_branch_socket_dictionary = {}
    # Mutex to prevent global branch socket dictionary
    global_branch_socket_dict_mutex = threading.Lock()

    # Static variable to hold total local state balance of single snapshot
    global_total_local_state = 0
    # Mutex to prevent global total local state
    global_total_local_state_mutex = threading.Lock()

    # Constructor
    def __init__(self, branch_info_listIn, total_amountIn):
        self.branch_info_list = branch_info_listIn
        self.total_amount = total_amountIn
        self.unique_snapshot_id = 0
        if DEBUG_STDOUT:
            for l in self.branch_info_list:
                print(l.branch_name, l.branch_ip_address, l.branch_port_number)

    # Method to add branches in protobuf object
    def include_all_branches(self, new_branchIn, branch):
        new_branchIn.name = branch.branch_name
        new_branchIn.ip = branch.branch_ip_address
        new_branchIn.port = branch.branch_port_number

    # Method to send InitBranch message to all running bank branches
    def init_branch_message(self):
        print("\n------------------------------------------------------------------------", file=sys.stderr)
        for branch_var in self.branch_info_list:
            # Create client socket IPv4 and TCP
            try:
                branch_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            except:
                print("ERROR : Socket creation failed.")
                sys.exit(1)

            # Connect client socket to server using 3 way handshake
            branch_socket.connect((branch_var.branch_ip_address, branch_var.branch_port_number))

            # Create BranchMessage object and wrap InitBranch object inside it
            branch_message_instance = bank_pb2.BranchMessage()
            branch_message_instance.init_branch.balance = int(self.total_amount / len(self.branch_info_list))
            for branch in self.branch_info_list:
                self.include_all_branches(branch_message_instance.init_branch.all_branches.add(), branch)

            print("InitBranch Message --> Branch Name = ", branch_var.branch_name, ", Amount = ", branch_message_instance.init_branch.balance, file=sys.stderr)

            # Send InitBranch message to bank branch socket
            branch_socket.send(pickle.dumps(branch_message_instance))

            # Save branch info with it's socket info
            Controller.global_branch_socket_dictionary[branch_var.branch_name] = branch_socket

    # Method to handle functionality - InitSnapshot, RetrieveSnapshot and Printing retrieved results
    def start_snapshot(self):
        # List to hold daemon threads
        daemon_thread_list = []

        # Start receive daemon threads for receiving ReturnSnapshot messages of all branches
        Controller.global_branch_socket_dict_mutex.acquire()
        for b_name, b_socket in Controller.global_branch_socket_dictionary.items():
            receive_thread = threading.Thread(name='Receive_Thread_Daemon', target=self.start_receive_daemon, args=(b_name, b_socket), daemon=True)
            receive_thread.start()
            daemon_thread_list.append(receive_thread)
        Controller.global_branch_socket_dict_mutex.release()

        # Continuously send snapshot messages after certain interval
        while True:
            # Create BranchMessage object and wrap InitSnapshot object inside it
            self.unique_snapshot_id += 1

            if self.unique_snapshot_id != 1:
                Controller.global_total_local_state_mutex.acquire()
                print("Total Local Balance --> ", Controller.global_total_local_state, file=sys.stderr)
                Controller.global_total_local_state = 0
                Controller.global_total_local_state_mutex.release()

            branch_message_instance = bank_pb2.BranchMessage()
            branch_message_instance.init_snapshot.snapshot_id = self.unique_snapshot_id

            # Generate a random branch to send InitSnapshot message
            rand_b_index = random.randint(1, len(self.branch_info_list)) - 1
            rand_b_name = self.branch_info_list[rand_b_index].branch_name

            print("\nInitSnapshot Message --> Snapshot ID = ", self.unique_snapshot_id, ", Selected Branch = ", rand_b_name, file=sys.stderr)

            # Send InitSnapshot message to random bank branch socket
            rand_b_socket = Controller.global_branch_socket_dictionary[rand_b_name]
            data = branch_message_instance.SerializeToString()
            size = encode_varint(len(data))
            rand_b_socket.sendall(size + data)

            # Sleep Controller for few seconds till all branches record it's state
            time.sleep(5)

            if DEBUG_STDOUT:
                print("(b) start_snapshot -> Controller is awaken to retrieve snapshot", file=sys.stderr)

            print("\nsnapshot_id: ", self.unique_snapshot_id, file=sys.stderr)

            # Send RetrieveSnapshot message to all the branches
            for branch_var in self.branch_info_list:
                # Create BranchMessage object and wrap RetrieveSnapshot object inside it
                branch_message_instance = bank_pb2.BranchMessage()
                branch_message_instance.retrieve_snapshot.snapshot_id = self.unique_snapshot_id

                # Send RetrieveSnapshot message to all bank branch sockets
                b_socket = Controller.global_branch_socket_dictionary[branch_var.branch_name]
                data = branch_message_instance.SerializeToString()
                size = encode_varint(len(data))
                b_socket.sendall(size + data)

            # Sleep Controller for few seconds till controller receive and displays all ReturnSnapshot messages
            time.sleep(5)

        # Main thread should wait till all threads complete their operation
        for cur_thread in daemon_thread_list:
            cur_thread.join()

    # Method to start Daemon thread to receive ReturnSnapshot message
    def start_receive_daemon(self, other_branch_nameIn, other_branch_socketIn):
        # Start demon thread to receive messages from other branches
        while True:
            # Identify message type here by parsing incoming socket message
            branch_message_instance = bank_pb2.BranchMessage()
            data = other_branch_socketIn.recv(1)
            size = decode_varint(data)
            branch_message_instance.ParseFromString(other_branch_socketIn.recv(size))

            # Filter received message with conditional statements
            if branch_message_instance.WhichOneof('branch_message') == 'return_snapshot':
                if DEBUG_STDOUT:
                    print(threading.current_thread().getName(), "-(a) Controller Receive Branch = ", other_branch_nameIn, file=sys.stderr)

                channel_state_str = ""
                current_branch_num = int(re.search(r'\d+', other_branch_nameIn).group())
                counter = 0
                for b in branch_message_instance.return_snapshot.local_snapshot.channel_state:
                    if counter == current_branch_num:
                        counter += 1
                    channel_state_str += (", branch" + str(counter) + "->" + other_branch_nameIn + ": " + str(b))
                    counter += 1

                # Print snapshot result to console
                print(other_branch_nameIn, ":", branch_message_instance.return_snapshot.local_snapshot.balance, channel_state_str, file=sys.stderr)
                Controller.global_total_local_state_mutex.acquire()
                Controller.global_total_local_state += branch_message_instance.return_snapshot.local_snapshot.balance
                Controller.global_total_local_state_mutex.release()


# Starting point for Controller
if __name__ == "__main__":
    # Validating command line arguments
    if len(sys.argv) != 3:
        print("ERROR : Invalid # of command line arguments. Expected 3 arguments.")
        sys.exit(1)

    # Local variables
    total_amount = int(sys.argv[1])
    branch_filename = sys.argv[2]

    # File validation
    if not (path.exists(branch_filename) and path.isfile(branch_filename)):
        print("ERROR : Invalid file in argument.")
        sys.exit(1)

    # Open file, sort using branch name and store in list of BranchInfo Object
    branch_info_list = sorted([create_branch_info(l[:-1]) for l in open(branch_filename)], key=lambda x: x.branch_name)

    # pass list of BranchInfo objects to Controller
    controller = Controller(branch_info_list, total_amount)

    # InitBranch logic to initialize all branches with initial balance and branch list
    controller.init_branch_message()
    print("\nMESSAGE : Controller is sleeping for 10 seconds before sending InitSnapshot.", file=sys.stderr)

    # Sleep Controller for 10 seconds so that branches can form connection in between
    time.sleep(10)
    print("\nMESSAGE : Controller is awaken to initiate InitSnapshot.", file=sys.stderr)

    # InitSnapshot, RetrieveSnapshot and Printing retrieved results Logic
    controller.start_snapshot()
