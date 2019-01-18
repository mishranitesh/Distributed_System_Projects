#!usr/bin/python3

import sys
import socket
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


# Starting point for Key-Value client
if __name__ == "__main__":
    # Validating command line arguments
    if len(sys.argv) != 3:
        print("ERROR : Invalid # of command line arguments. Expected 3 arguments.")
        sys.exit(1)

    # Local variables
    coordinator_ip = sys.argv[1]
    coordinator_port = int(sys.argv[2])
    cc_transaction_id = ""

    # Create socket IPv4 and TCP
    try:
        coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except:
        print("ERROR : Socket creation failed.")
        sys.exit(1)

    # Connect coordinator socket to server using 3 way handshake
    coordinator_socket.connect((coordinator_ip, coordinator_port))

    # Create KeyValueMessage object and wrap ConnectClient object inside it
    kv_message_instance = KeyValueStore_pb2.KeyValueMessage()
    kv_message_instance.connect_client.src = "client"
    kv_message_instance.connect_client.transaction_id = cc_transaction_id

    # Send KeyValueMessage to coordinator socket
    data = kv_message_instance.SerializeToString()
    size = encode_varint(len(data))
    coordinator_socket.sendall(size + data)

    print("\nConnectClient request sent to coordinator. Waiting for transaction_id...", file=sys.stderr)

    # Identify message type here by parsing coordinator socket message
    kv_message_instance = KeyValueStore_pb2.KeyValueMessage()
    data = coordinator_socket.recv(1)
    size = decode_varint(data)
    kv_message_instance.ParseFromString(coordinator_socket.recv(size))

    # Save coordinator generated transaction id into client's local variable and send it over subsequent requests to coordinator
    if kv_message_instance.WhichOneof('key_value_message') == 'connect_client':
        cc_transaction_id = kv_message_instance.connect_client.transaction_id
        print("Received transaction_id = ", cc_transaction_id, " from coordinator.", file=sys.stderr)

    # Request user for specific operation (get/put)
    l_request_id = 0
    while True:
        print("\n------------------------------------------------------------------------------------------------------------------------")
        u_req_choice = input("Enter request type (GET - 1 or PUT - 2) --> ")
        u_req_type = ""
        if u_req_choice == "1":
            u_req_type = "get"
        elif u_req_choice == "2":
            u_req_type = "put"
        else:
            u_req_type = "NONE"
        if u_req_type.lower() != "get" and u_req_type.lower() != "put":
            print("INVALID REQUEST TYPE (Request types 'get' or 'put' are acceptable)....Try Again !!", file=sys.stderr)
            continue

        u_consistency_level = input("Enter consistency level (ONE - 1 or QUORUM - 2) --> ")
        if int(u_consistency_level) != 1 and int(u_consistency_level) != 2:
            print("INVALID CONSISTENCY LEVEL (Only 1 or 2 are acceptable)....Try Again !!", file=sys.stderr)
            continue

        # Use unique request id for each request
        l_request_id += 1

        if u_req_type.lower() == "get":
            u_req_type = "get"
            u_key = input("Enter Key (between 0-255) --> ")
            if len(u_key.strip()) == 0 or not (0 <= int(u_key.strip()) <= 255):
                print("INVALID KEY (Key should be between 0-255)....Try Again !!", file=sys.stderr)
                continue

            # Create KeyValueMessage object and wrap ClientRequest object inside it
            new_req_id = cc_transaction_id + "_" + str(l_request_id)
            kv_message_instance = KeyValueStore_pb2.KeyValueMessage()
            kv_message_instance.client_request.request_type = u_req_type
            kv_message_instance.client_request.transaction_id = cc_transaction_id
            kv_message_instance.client_request.request_id = new_req_id
            kv_message_instance.client_request.get_request.key = int(u_key)
            kv_message_instance.client_request.consistency_level = int(u_consistency_level)

            # Send KeyValueMessage to coordinator socket
            data = kv_message_instance.SerializeToString()
            size = encode_varint(len(data))
            coordinator_socket.sendall(size + data)

            print("GET request sent to coordinator | Request_Id = (", new_req_id,"). Waiting for reply...", file=sys.stderr)

            # Identify message type by parsing coordinator socket
            kv_message_response = KeyValueStore_pb2.KeyValueMessage()
            data = coordinator_socket.recv(1)
            size = decode_varint(data)
            kv_message_response.ParseFromString(coordinator_socket.recv(size))

            if kv_message_response.WhichOneof('key_value_message') == 'coordinator_response':
                res_key = kv_message_response.coordinator_response.key
                res_value = kv_message_response.coordinator_response.value
                res_txid = kv_message_response.coordinator_response.transaction_id
                res_msg = kv_message_response.coordinator_response.message
                if res_msg == "success":
                    if DEBUG_STDERR:
                        print("Response received from coordinator for GET request | TxID --> ", res_txid, file=sys.stderr)
                    print("Key-Value Pair returned by coordinator --> ", (res_key, res_value), file=sys.stderr)
                else:
                    print("EXCEPTION :: Message --> ", res_msg.split(":")[1], file=sys.stderr)
        else:
            u_req_type = "put"
            u_key = input("Enter Key (between 0-255) --> ")
            if len(u_key.strip()) == 0:
                print("INVALID KEY....Try Again!!")
                continue
            u_value = input("Enter Value --> ")

            # Create KeyValueMessage object and wrap ClientRequest object inside it
            new_req_id = cc_transaction_id + "_" + str(l_request_id)
            kv_message_instance = KeyValueStore_pb2.KeyValueMessage()
            kv_message_instance.client_request.request_type = u_req_type
            kv_message_instance.client_request.transaction_id = cc_transaction_id
            kv_message_instance.client_request.request_id = new_req_id
            kv_message_instance.client_request.put_request.key = int(u_key)
            kv_message_instance.client_request.put_request.value = u_value
            kv_message_instance.client_request.consistency_level = int(u_consistency_level)

            # Send KeyValueMessage to coordinator socket
            data = kv_message_instance.SerializeToString()
            size = encode_varint(len(data))
            coordinator_socket.sendall(size + data)

            print("PUT request sent to coordinator | Request_Id = (", new_req_id, "). Waiting for reply...", file=sys.stderr)

            # Identify message type by parsing coordinator socket
            kv_message_response = KeyValueStore_pb2.KeyValueMessage()
            data = coordinator_socket.recv(1)
            size = decode_varint(data)
            kv_message_response.ParseFromString(coordinator_socket.recv(size))

            if DEBUG_STDERR:
                print("Response received from coordinator for PUT request.", file=sys.stderr)

            if kv_message_response.WhichOneof('key_value_message') == 'coordinator_response':
                res_key = kv_message_response.coordinator_response.key
                res_value = kv_message_response.coordinator_response.value
                res_msg = kv_message_response.coordinator_response.message
                if res_msg == "success":
                    print((res_key, res_value), " pair written at coordinator side successfully.", file=sys.stderr)
                else:
                    print("EXCEPTION :: Message --> ", res_msg.split(":")[1], file=sys.stderr)
