#!usr/bin/python3

import sys
import socket
from os import path
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


# Method to create new replica info
def create_replica_info(file_lineIn):
    replica_name, replica_ip_address, replica_port_number = file_lineIn.split(" ")
    return ReplicaInfo(replica_name.strip(), replica_ip_address.strip(), replica_port_number.strip())


# Method to add replicas in protobuf object
def include_all_replicas(new_replicaIn, replica):
    new_replicaIn.name = replica.replica_name
    new_replicaIn.ip = replica.replica_ip_address
    new_replicaIn.port = replica.replica_port_number


# Starting point for Setup Replica Connections
if __name__ == "__main__":
    # Validating command line arguments
    if len(sys.argv) != 2:
        print("ERROR : Invalid # of command line arguments. Expected 2 arguments.")
        sys.exit(1)

    # Local variables
    replica_filename = sys.argv[1]

    # File validation
    if not (path.exists(replica_filename) and path.isfile(replica_filename)):
        print("ERROR : Invalid file in argument.")
        sys.exit(1)

    # Open file, sort using replica name and store in list of ReplicaInfo Object
    replica_info_list = sorted([create_replica_info(l[:-1]) for l in open(replica_filename)], key=lambda x: x.replica_name)

    for replica_var in replica_info_list:
        # Create client socket IPv4 and TCP
        try:
            replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except:
            print("ERROR : Socket creation failed.")
            sys.exit(1)

        # Connect client socket to server using 3 way handshake
        replica_socket.connect((replica_var.replica_ip_address, replica_var.replica_port_number))

        # Create KeyValueMessage object and wrap InitReplica object inside it
        kv_message_instance = KeyValueStore_pb2.KeyValueMessage()
        for replica in replica_info_list:
            include_all_replicas(kv_message_instance.init_replica.all_replicas.add(), replica)

        # Send InitReplica message to replica socket
        data = kv_message_instance.SerializeToString()
        size = encode_varint(len(data))
        replica_socket.sendall(size + data)

        print("MSG : InitReplica Message sent to replica server --> ", replica_var.replica_name)
        replica_socket.close()
