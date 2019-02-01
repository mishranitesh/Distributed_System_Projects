#!usr/bin/python3

import sys
import time
import math
import pickle
import socket
import random
import datetime
import threading
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


# Class to maintain recording state
class BranchRecordingState:
    """Class to maintain recording state"""

    def __init__(self):
        self.branch_local_state = 0
        self.incoming_channel_state = {}


# Class to represent bank branch
class BankBranch(threading.Thread):
    """Class to represent bank branch"""

    # Static Variable to represent branch balance
    global_branch_balance = 0
    # Mutex to prevent global balance variable
    global_balance_mutex = threading.Lock()

    # Static Variable to hold all active branches in the system
    global_branch_info_list = []
    # Mutex to prevent global global_branch_info_list
    global_branch_info_list_mutex = threading.Lock()

    # Static variable to hold Mapping of Branch name with respective TCP socket
    global_branch_socket_dictionary = {}
    # Mutex to prevent global branch socket dictionary
    global_branch_socket_dict_mutex = threading.Lock()

    # Static variable to hold transfer enable disable flag
    global_transfer_stop = False
    # Mutex to prevent global transfer stop
    global_transfer_stop_mutex = threading.Lock()

    # Static variable to hold local and channel recording state
    global_recording_state_dictionary = {}
    # Mutex to prevent global recording state dictionary
    global_recording_state_dict_mutex = threading.Lock()

    # Constructor
    def __init__(self, incoming_socketIn, incoming_socket_addressIn, nameIn, ipIn, portIn, intervalIn):
        threading.Thread.__init__(self)
        self.incoming_socket = incoming_socketIn
        self.incoming_socket_address = incoming_socket_addressIn
        self.branch_name = nameIn
        self.branch_ip_address = ipIn
        self.branch_port_number = portIn
        self.branch_transfer_interval = intervalIn
        self.branch_message_instance = bank_pb2.BranchMessage()

    # Method (run) works as entry point for each thread - overridden from threading.Thread
    def run(self):
        if DEBUG_STDOUT:
            print(threading.current_thread().getName(), "-(a) Run : New connection request from -> ", self.incoming_socket_address, file=sys.stderr)

        # Identify message type here by parsing incoming socket message
        received_message = pickle.loads(self.incoming_socket.recv(10000))

        # Filter received message with conditional statements
        if type(received_message) is str:
            # Store with key - other branch name and value - other branch socket object
            self.add_global_branch_socket_dictionary(received_message, self.incoming_socket)
            if DEBUG_STDOUT:
                print(threading.current_thread().getName(), "-(b) Run : Other Branch Socket Added to Dict Done --> Dict Size = ", self.get_global_branch_socket_dictionary_size(), file=sys.stderr)
        else:
            if received_message.WhichOneof('branch_message') == 'init_branch':
                # Receive InitBranch message object message from controller
                self.branch_message_instance.CopyFrom(received_message)
                self.receive_InitBranch_message()
                if DEBUG_STDOUT:
                    print(threading.current_thread().getName(), "-(b) Run : receive_InitBranch_message Done --> List Size = ", self.get_global_branch_info_list_size(), file=sys.stderr)

                # Set up TCP Full Duplex connection with all the branches in the system
                self.setup_branch_connections()
                if DEBUG_STDOUT:
                    print(threading.current_thread().getName(), "-(c) Run : setup_branch_connections Done --> Dict Size = ", self.get_global_branch_socket_dictionary_size(), file=sys.stderr)

                # Sleep till the time all branch connections are set up
                while (self.get_global_branch_info_list_size() - 1) != self.get_global_branch_socket_dictionary_size():
                    if DEBUG_STDOUT:
                        print(threading.current_thread().getName(), "-(d) Run : SLEEPING.", self.get_global_branch_info_list_size() - 1, " ",
                              self.get_global_branch_socket_dictionary_size(), file=sys.stderr)
                if DEBUG_STDOUT:
                    print(threading.current_thread().getName(), "-(e) Run : AWAKEN.", file=sys.stderr)

                # List to hold daemon threads
                daemon_thread_list = []

                # Start receive Daemon thread for controller
                r_thread = threading.Thread(name='Receive_Thread_Daemon', target=self.start_controller_receive_daemon, args=("controller", self.incoming_socket), daemon=True)
                r_thread.start()
                daemon_thread_list.append(r_thread)

                # Sleep transfer daemon for sometime just to let all thread start at approx same time
                time.sleep(1)

                # Start receive Daemon thread for all incoming channels
                BankBranch.global_branch_socket_dict_mutex.acquire()
                for b_name, b_socket in BankBranch.global_branch_socket_dictionary.items():
                    receive_thread = threading.Thread(name='Receive_Thread_Daemon', target=self.start_branch_receive_daemon, args=(b_name, b_socket), daemon=True)
                    receive_thread.start()
                    daemon_thread_list.append(receive_thread)
                BankBranch.global_branch_socket_dict_mutex.release()
                if DEBUG_STDOUT:
                    print(threading.current_thread().getName(), "-(g) Run : start_branch_receive_daemon Done.", file=sys.stderr)

                # Sleep transfer daemon for sometime just to let all thread start at approx same time
                time.sleep(1)

                # Start transfer Daemon thread
                transfer_thread = threading.Thread(name='Transfer_Thread_Daemon', target=self.start_transfer_daemon, daemon=True)
                transfer_thread.start()
                daemon_thread_list.append(transfer_thread)
                if DEBUG_STDOUT:
                    print(threading.current_thread().getName(), "-(h) Run : start_transfer_daemon Done.", file=sys.stderr)

                # Main thread should wait till all threads complete their operation
                for cur_thread in daemon_thread_list:
                    cur_thread.join()

        if DEBUG_STDOUT:
            self.global_branch_socket_dict_mutex.acquire()
            print("\nBranch Socket Dictionary elements:\n----------------------------------------")
            for b_name_key, b_socket_val in BankBranch.global_branch_socket_dictionary.items():
                print(b_name_key, b_socket_val, file=sys.stderr)
            self.global_branch_socket_dict_mutex.release()

    # Receive InitBranch message object message from controller via socket
    def receive_InitBranch_message(self):
        # Initialize global static variables
        self.set_balance(self.branch_message_instance.init_branch.balance)
        for b in self.branch_message_instance.init_branch.all_branches:
            BankBranch.global_branch_info_list.append(BranchInfo(b.name, b.ip, b.port))

        if PRINT_MSG:
            print("INIT_BRANCH --> Global Branch Balance = ", self.get_balance(), file=sys.stderr)

    # Set up TCP Full Duplex connection with all the branches in the system which have greater name than itself
    def setup_branch_connections(self):
        for branch in BankBranch.global_branch_info_list:
            if self.branch_name < branch.branch_name:
                # Create client socket IPv4 and TCP
                try:
                    other_branch_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                except:
                    print("ERROR : Socket creation failed.")
                    sys.exit(1)

                # Connect client socket to server using 3 way handshake and send branch name as message
                other_branch_socket.connect((branch.branch_ip_address, branch.branch_port_number))
                other_branch_socket.send(pickle.dumps(self.branch_name))

                # Store with key - other branch name and value - other branch socket object
                self.add_global_branch_socket_dictionary(branch.branch_name, other_branch_socket)

    # Method to start Daemon thread to transfer amount to random bank branch
    def start_transfer_daemon(self):

        # Start demon thread to transfer from current branch
        while True:

            # Stop transfer while snapshot process is going on - for simplicity
            BankBranch.global_transfer_stop_mutex.acquire()
            if BankBranch.global_transfer_stop:
                BankBranch.global_transfer_stop_mutex.release()
                continue

            # Logic to randomly transfer money to another bank
            if self.get_balance() > 10:
                transfer_amt = int((random.randrange(1, 5) * self.get_balance()) / 100)
                self.remove_balance(transfer_amt)

                # Generate random destination branch
                random_dest_branch_indx = random.randint(1, self.get_global_branch_socket_dictionary_size()) - 1
                random_dest_branch_name = self.get_global_branch_socket_dictionary_key(random_dest_branch_indx)
                random_dest_branch_socket = self.get_global_branch_socket_dictionary_value(random_dest_branch_name)

                # Wrap transfer amount in BranchMessage -> Transfer message
                branch_message_instance = bank_pb2.BranchMessage()
                branch_message_instance.transfer.src_branch = self.branch_name
                branch_message_instance.transfer.dst_branch = random_dest_branch_name
                branch_message_instance.transfer.money = transfer_amt

                # Transfer amount to random destination branch
                data = branch_message_instance.SerializeToString()
                size = encode_varint(len(data))
                random_dest_branch_socket.sendall(size + data)

                if PRINT_MSG:
                    print("\n(a) From_Branch -> ", self.branch_name, ", To_Branch -> ", random_dest_branch_name, file=sys.stderr)
                    print("(b) Transferred_Amount -> ", transfer_amt, ", Remaining_Branch_Balance -> ", self.get_balance(), file=sys.stderr)
                    # print("(c) Timestamp -> ", datetime.datetime.now(), file=sys.stderr)

            BankBranch.global_transfer_stop_mutex.release()

            # Sleep for some random time before next transfer
            time.sleep(random.randrange(0, self.branch_transfer_interval) / 1000.0)

    # Method to start daemon thread to receive requests from controller
    def start_controller_receive_daemon(self, controller_nameIn, controller_socketIn):
        while True:
            if DEBUG_STDOUT:
                print("(a) Inside Controller Receive Daemon --> ", controller_nameIn, file=sys.stderr)

            # Identify message type here by parsing incoming socket message
            branch_message_instance = bank_pb2.BranchMessage()
            data = controller_socketIn.recv(1)
            size = decode_varint(data)
            branch_message_instance.ParseFromString(controller_socketIn.recv(size))

            # Filter received message with conditional statements
            if branch_message_instance.WhichOneof('branch_message') == 'init_snapshot':

                # Stop current branch transfer process
                BankBranch.global_transfer_stop_mutex.acquire()
                BankBranch.global_transfer_stop = True

                if PRINT_MSG:
                    print("\nINIT_SNAPSHOT --> Snapshot Id = ", branch_message_instance.init_snapshot.snapshot_id, file=sys.stderr)

                # Insert initialized object into global dictionary
                BankBranch.global_recording_state_dict_mutex.acquire()

                # Initialize Branch Recording state object
                r_state = BranchRecordingState()

                BankBranch.global_balance_mutex.acquire()
                r_state.branch_local_state = BankBranch.global_branch_balance
                BankBranch.global_balance_mutex.release()

                BankBranch.global_branch_info_list_mutex.acquire()
                for b in BankBranch.global_branch_info_list:
                    if b.branch_name != self.branch_name:
                        r_state.incoming_channel_state[b.branch_name] = (True, 0)  # True means recording is going on
                BankBranch.global_branch_info_list_mutex.release()

                BankBranch.global_recording_state_dictionary[branch_message_instance.init_snapshot.snapshot_id] = r_state
                BankBranch.global_recording_state_dict_mutex.release()

                if DEBUG_STDOUT:
                    print("(b) init_snapshot -> Branch Recording State Added.", file=sys.stderr)

                # Send Marker messages to all connected branches
                m_branch_message_instance = bank_pb2.BranchMessage()
                m_branch_message_instance.marker.src_branch = self.branch_name
                m_branch_message_instance.marker.snapshot_id = branch_message_instance.init_snapshot.snapshot_id
                BankBranch.global_branch_socket_dict_mutex.acquire()
                for b_name, b_socket in BankBranch.global_branch_socket_dictionary.items():
                    m_branch_message_instance.marker.dst_branch = b_name
                    data = m_branch_message_instance.SerializeToString()
                    size = encode_varint(len(data))
                    b_socket.sendall(size + data)
                BankBranch.global_branch_socket_dict_mutex.release()

                if DEBUG_STDOUT:
                    print("(c) init_snapshot -> Marker Messages Sent.", file=sys.stderr)

                # Restart current branch transfer process
                BankBranch.global_transfer_stop = False
                BankBranch.global_transfer_stop_mutex.release()

                if DEBUG_STDOUT:
                    print("(d) init_snapshot END.", file=sys.stderr)

            elif branch_message_instance.WhichOneof('branch_message') == 'retrieve_snapshot':
                if DEBUG_STDOUT:
                    print("(b) retrieve_snapshot --> START", file=sys.stderr)

                # Send ReturnSnapshot message to controller if all incoming channel states are recorded
                s_id = branch_message_instance.retrieve_snapshot.snapshot_id
                retrieve_snapshot_stop = True

                # Logic to check whether all incoming channel states are recorded
                '''while retrieve_snapshot_stop:
                    retrieve_snapshot_stop = False
                    BankBranch.global_recording_state_dict_mutex.acquire()
                    for b_name, b_tuple in BankBranch.global_recording_state_dictionary[s_id].incoming_channel_state.items():
                        if b_tuple[0] == True:
                            retrieve_snapshot_stop = True
                            break
                    BankBranch.global_recording_state_dict_mutex.release()
                    time.sleep(1)'''

                if DEBUG_STDOUT:
                    print("(c) retrieve_snapshot --> CONTINUE", file=sys.stderr)

                # Send ReturnSnapshot message to controller
                r_branch_message_instance = bank_pb2.BranchMessage()
                r_branch_message_instance.return_snapshot.local_snapshot.snapshot_id = s_id
                BankBranch.global_recording_state_dict_mutex.acquire()
                r_branch_message_instance.return_snapshot.local_snapshot.balance = BankBranch.global_recording_state_dictionary[s_id].branch_local_state
                t_str = ""
                for b_name, b_tuple in BankBranch.global_recording_state_dictionary[s_id].incoming_channel_state.items():
                    t_str += str(b_tuple[1]) + " "
                    r_branch_message_instance.return_snapshot.local_snapshot.channel_state.append(b_tuple[1])
                BankBranch.global_recording_state_dict_mutex.release()
                data = r_branch_message_instance.SerializeToString()
                size = encode_varint(len(data))
                controller_socketIn.sendall(size + data)

                if PRINT_MSG:
                    print("\nRETURN_SNAPSHOT --> Sent Channel State = ", t_str, file=sys.stderr)

    # Method to add branches in protobuf object
    def include_channel_state(self, new_state, old_state):
        new_state = old_state

    # Method to start Daemon thread to receive amount from random bank branch
    def start_branch_receive_daemon(self, other_branch_nameIn, other_branch_socketIn):
        # Start demon thread to receive messages from other branches
        while True:
            # Identify message type here by parsing incoming socket message
            branch_message_instance = bank_pb2.BranchMessage()
            data = other_branch_socketIn.recv(1)
            size = decode_varint(data)
            branch_message_instance.ParseFromString(other_branch_socketIn.recv(size))

            if DEBUG_STDOUT:
                print("\nBRANCH RECEIVED MESSAGE --> ", branch_message_instance, ", From_B -> ", other_branch_nameIn, ", From_Socket -> ", other_branch_socketIn, file=sys.stderr)

            # Filter received message with conditional statements
            if branch_message_instance.WhichOneof('branch_message') == 'transfer':

                # Increase balance local state
                self.add_balance(branch_message_instance.transfer.money)

                if PRINT_MSG:
                    print("\n(a) From_Branch -> ", branch_message_instance.transfer.src_branch, ", To_Branch -> ", branch_message_instance.transfer.dst_branch, file=sys.stderr)
                    print("(b) Received_Amount -> ", branch_message_instance.transfer.money, ", Modified_Branch_Balance = ", self.get_balance(), file=sys.stderr)
                    # print("(c) Timestamp -> ", datetime.datetime.now(), file=sys.stderr)

                # Record respective incoming channel state
                b_name = branch_message_instance.transfer.src_branch
                BankBranch.global_recording_state_dict_mutex.acquire()
                s_id = len(BankBranch.global_recording_state_dictionary)
                if s_id in BankBranch.global_recording_state_dictionary:
                    if BankBranch.global_recording_state_dictionary[s_id].incoming_channel_state[b_name][0] == True:
                        t_val = BankBranch.global_recording_state_dictionary[s_id].incoming_channel_state[b_name][1] + branch_message_instance.transfer.money
                        t_tuple = (BankBranch.global_recording_state_dictionary[s_id].incoming_channel_state[b_name][0], t_val)
                        if DEBUG_STDOUT:
                            print("(d)MODIFY CHANNEL STATE --> b_src = ", b_name, ", b_dest = ", self.branch_name, ", channel_amount = ", t_val, file=sys.stderr)
                        BankBranch.global_recording_state_dictionary[s_id].incoming_channel_state[b_name] = t_tuple
                BankBranch.global_recording_state_dict_mutex.release()

            elif branch_message_instance.WhichOneof('branch_message') == 'marker':

                if DEBUG_STDOUT:
                    print("\n(a) MARKER Received at Timestamp -> ", datetime.datetime.now(), file=sys.stderr)

                # Stop current branch transfer process
                BankBranch.global_transfer_stop_mutex.acquire()
                BankBranch.global_transfer_stop = True

                BankBranch.global_recording_state_dict_mutex.acquire()
                # When branch receives Marker message for the first time
                if branch_message_instance.marker.snapshot_id not in BankBranch.global_recording_state_dictionary:

                    if DEBUG_STDOUT:
                        print("(b) [FIRST Marker] Timestamp = ", datetime.datetime.now(), " & From = ", branch_message_instance.marker.src_branch, file=sys.stderr)

                    # Initialize Branch Recording state object
                    r_state = BranchRecordingState()

                    BankBranch.global_balance_mutex.acquire()
                    # r_state.branch_local_state = self.get_balance()
                    r_state.branch_local_state = BankBranch.global_branch_balance
                    BankBranch.global_balance_mutex.release()

                    BankBranch.global_branch_info_list_mutex.acquire()
                    for b in BankBranch.global_branch_info_list:
                        if b.branch_name != self.branch_name:
                            if b.branch_name == branch_message_instance.marker.src_branch:
                                r_state.incoming_channel_state[b.branch_name] = (False, 0)
                            else:
                                r_state.incoming_channel_state[b.branch_name] = (True, 0)
                    BankBranch.global_branch_info_list_mutex.release()

                    if DEBUG_STDOUT:
                        print(threading.current_thread().getName(), "-(c) FIRST Marker --> Branch Recording State DONE", file=sys.stderr)

                    # Insert initialized object into global dictionary
                    BankBranch.global_recording_state_dictionary[branch_message_instance.marker.snapshot_id] = r_state

                    if DEBUG_STDOUT:
                        print(threading.current_thread().getName(), "-(d) FIRST Marker --> Branch Recording State ADDED", file=sys.stderr)

                    # Send Marker messages to all connected branches
                    m_branch_message_instance = bank_pb2.BranchMessage()
                    m_branch_message_instance.marker.src_branch = self.branch_name
                    m_branch_message_instance.marker.snapshot_id = branch_message_instance.marker.snapshot_id
                    BankBranch.global_branch_socket_dict_mutex.acquire()
                    for b_name, b_socket in BankBranch.global_branch_socket_dictionary.items():
                        m_branch_message_instance.marker.dst_branch = b_name
                        data = m_branch_message_instance.SerializeToString()
                        size = encode_varint(len(data))
                        b_socket.sendall(size + data)
                    BankBranch.global_branch_socket_dict_mutex.release()

                    if DEBUG_STDOUT:
                        print(threading.current_thread().getName(), "-(e) FIRST Marker --> SENT MARKERS", file=sys.stderr)

                    # Restart current branch transfer process
                    BankBranch.global_transfer_stop = False
                    BankBranch.global_transfer_stop_mutex.release()

                else:  # When branch receives Marker message NOT for first time

                    # Restart current branch transfer process
                    BankBranch.global_transfer_stop = False
                    BankBranch.global_transfer_stop_mutex.release()

                    if DEBUG_STDOUT:
                        print("(b) [NOT FIRST Marker] Timestamp = ", datetime.datetime.now(), " & From = ", branch_message_instance.marker.src_branch, file=sys.stderr)

                    # Stop the recording of respective incoming channel
                    s_id = branch_message_instance.marker.snapshot_id
                    b_name = branch_message_instance.marker.src_branch
                    t_tuple = (False, BankBranch.global_recording_state_dictionary[s_id].incoming_channel_state[b_name][1])
                    BankBranch.global_recording_state_dictionary[s_id].incoming_channel_state[b_name] = t_tuple

                BankBranch.global_recording_state_dict_mutex.release()

    # Method to set balance as provided in input
    def set_balance(self, balanceIn):
        BankBranch.global_balance_mutex.acquire()
        BankBranch.global_branch_balance = balanceIn
        BankBranch.global_balance_mutex.release()

    # Method to get branch balance
    def get_balance(self):
        BankBranch.global_balance_mutex.acquire()
        current_balance = BankBranch.global_branch_balance
        BankBranch.global_balance_mutex.release()
        return current_balance

    # Method to increment balance as provided in input
    def add_balance(self, balanceIn):
        BankBranch.global_balance_mutex.acquire()
        BankBranch.global_branch_balance += balanceIn
        BankBranch.global_balance_mutex.release()

    # Method to decrement balance as provided in input
    def remove_balance(self, balanceIn):
        BankBranch.global_balance_mutex.acquire()
        BankBranch.global_branch_balance -= balanceIn
        BankBranch.global_balance_mutex.release()

    # Method to get current size of global_branch_info_list using key
    def get_global_branch_info_list_size(self):
        BankBranch.global_branch_info_list_mutex.acquire()
        size = len(BankBranch.global_branch_info_list)
        BankBranch.global_branch_info_list_mutex.release()
        return size

    # Method to add entry in global_branch_socket_dictionary
    def add_global_branch_socket_dictionary(self, b_name, b_socket):
        BankBranch.global_branch_socket_dict_mutex.acquire()
        BankBranch.global_branch_socket_dictionary[b_name] = b_socket
        BankBranch.global_branch_socket_dict_mutex.release()

    # Method to get key of dictionary using index - branch_name
    def get_global_branch_socket_dictionary_key(self, index):
        BankBranch.global_branch_socket_dict_mutex.acquire()
        b_name = list(BankBranch.global_branch_socket_dictionary.keys())[index]
        BankBranch.global_branch_socket_dict_mutex.release()
        return b_name

    # Method to get value from global_branch_socket_dictionary using key - branch_socket
    def get_global_branch_socket_dictionary_value(self, b_name):
        BankBranch.global_branch_socket_dict_mutex.acquire()
        b_socket = BankBranch.global_branch_socket_dictionary[b_name]
        BankBranch.global_branch_socket_dict_mutex.release()
        return b_socket

    # Method to get current size of global_branch_socket_dictionary
    def get_global_branch_socket_dictionary_size(self):
        BankBranch.global_branch_socket_dict_mutex.acquire()
        size = len(BankBranch.global_branch_socket_dictionary)
        BankBranch.global_branch_socket_dict_mutex.release()
        return size


# Class to represent Branch Socket functionality
class BranchSocketServer:
    '''Class to represent Branch Socket functionality'''

    # Constructor
    def __init__(self, arg_branch_nameIn, arg_branch_portIn, arg_branch_intervalIn):
        self.b_name = arg_branch_nameIn
        self.b_port = arg_branch_portIn
        self.b_interval = arg_branch_intervalIn
        self.b_ip = socket.gethostbyname(socket.gethostname())
        self.backlog_count = 100
        self.thread_list = []  # holds total count of thread
        # Create server sockect (IPV4 and TCP)
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except:
            print("ERROR : Socket creation failed.")
            sys.exit(1)

    def start_server(self):
        # Bind server socket with given port
        try:
            self.server_socket.bind(('', self.b_port))
        except:
            print("ERROR : Socket bind failed.")

        print("---------------------------------------------------------------", file=sys.stderr)
        print("Branch Server Information :\n----------------------------------", file=sys.stderr)
        print("Branch Name :::::::::::::: \t", self.b_name, file=sys.stderr)
        print("Branch IP Address :::::::: \t", self.b_ip, file=sys.stderr)
        print("Branch Port Number ::::::: \t", self.b_port, file=sys.stderr)
        print("Branch Transfer Interval : \t", self.b_interval, file=sys.stderr)
        print("---------------------------------------------------------------", file=sys.stderr)

        # Server socket in Listening mode with provided backlog count
        self.server_socket.listen(self.backlog_count)
        print("\nServer is running ....\n", file=sys.stderr)

    def accept_connections(self):
        while True:
            try:
                # Accept creates new socket for each client request and return tuple
                client_socket, client_address = self.server_socket.accept()

                # Handle client's request using separate thread
                bank_branch_thread = BankBranch(client_socket, client_address, self.b_name, self.b_ip, self.b_port, self.b_interval)
                bank_branch_thread.start()

                # Save running threads in list
                self.thread_list.append(bank_branch_thread)
            except:
                print("ERROR : Socket accept failed.")
                sys.exit(1)

        # Main thread should wait till all threads complete their operation
        for cur_thread in self.thread_list:
            cur_thread.join()


# Starting point for Bank Branch Socket
if __name__ == "__main__":
    # Validating command line arguments
    if len(sys.argv) != 4:
        print("ERROR : Invalid # of command line arguments. Expected 4 arguments.")
        sys.exit(1)

    # Local variables
    arg_branch_name = sys.argv[1]
    arg_branch_port = int(sys.argv[2])
    arg_branch_interval = int(sys.argv[3])

    if arg_branch_interval >= 1000:
        PRINT_MSG = True
    else:
        PRINT_MSG = False

    branchSocketServer = BranchSocketServer(arg_branch_name, arg_branch_port, arg_branch_interval)
    branchSocketServer.start_server()
    branchSocketServer.accept_connections()
