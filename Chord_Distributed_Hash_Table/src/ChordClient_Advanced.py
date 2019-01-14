#!/usr/bin/python3

import sys
import glob
import hashlib

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
from chord.ttypes import SystemException, RFileMetadata, RFile, NodeID
from constant import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


# Class to test the functionality of Chord Server implementation
class ChordClientHandler:
    """Class to test the functionality of Chord Server implementation"""

    # Constructor to initialize data members
    def __init__(self, server_ip_address, server_port_number):
        print(GENERAL_CLIENT_MESSAGE)
        encoded_address = (server_ip_address + COLON_STRING + str(server_port_number)).encode(ENCODE_TYPE_UTF_8)
        self.user_id = hashlib.sha256(encoded_address).hexdigest()

    # Method to create new client to perform File Store RPC calls
    def _createFileStoreClient(self, ip_address, port_number):
        # Create Client side Apache Thrift configurations
        self.client_socket = TSocket.TSocket(ip_address, port_number)  # Raw socket creation using Thrift API
        self.client_transport = TTransport.TBufferedTransport(self.client_socket)  # Enable buffering in above raw socket
        self.client_protocol = TBinaryProtocol.TBinaryProtocol(self.client_transport)  # Client protocol to send contents as Binary (marshalling and unmarshalling)
        self.client_filestore = FileStore.Client(self.client_protocol)  # Client to use protocol encoder
        self.client_transport.open()  # Connect to specified Chord Server with above configuration

    # Method to test all RPC methods implemented by Chord Server
    def test_chord_server_methods(self):

        # Handle RPC exception i.e. SystemException
        try:
            # Test write file method using RPC call
            self._test_write_file()

            # Test read file method using RPC call
            self._test_read_file()

            # Close client transport
            self.client_transport.close()

        except SystemException as ex:
            print(ERROR_SYSTEM_EXCEPTION_MSG, ex)
            print("\n%s" % ex.message)
            print(GENERAL_CLIENT_MESSAGE)
            sys.exit(1)

    # Method to test writeFile RPC
    def _test_write_file(self):
        # Create rfile to write at server side
        rfile_meta_data = RFileMetadata()
        rfile = RFile()
        rfile.meta = rfile_meta_data

        # Write first file
        self._createFileStoreClient(sys.argv[1], int(sys.argv[2]))
        rfile.meta.filename = "File.txt"
        rfile.content = "This is File_Content."
        rfile_write_hash_id = hashlib.sha256(rfile.meta.filename.encode(ENCODE_TYPE_UTF_8)).hexdigest()
        succ_node_id = self.client_filestore.findSucc(rfile_write_hash_id)
        if succ_node_id.id != self.user_id:
            self.client_transport.close()
            self._createFileStoreClient(succ_node_id.ip, succ_node_id.port)
            self.client_filestore.writeFile(rfile)
            self.client_transport.close()
        else:
            self.client_filestore.writeFile(rfile)
            self.client_transport.close()

        # Write second file
        self._createFileStoreClient(sys.argv[1], int(sys.argv[2]))
        rfile.meta.filename = "Test_File.txt"
        rfile.content = "This is Test_File_Content."
        rfile_write_hash_id = hashlib.sha256(rfile.meta.filename.encode(ENCODE_TYPE_UTF_8)).hexdigest()
        succ_node_id = self.client_filestore.findSucc(rfile_write_hash_id)
        if succ_node_id.id != self.user_id:
            self.client_transport.close()
            self._createFileStoreClient(succ_node_id.ip, succ_node_id.port)
            self.client_filestore.writeFile(rfile)
            self.client_transport.close()
        else:
            self.client_filestore.writeFile(rfile)
            self.client_transport.close()

        # Write third file
        self._createFileStoreClient(sys.argv[1], int(sys.argv[2]))
        rfile.meta.filename = "Nitesh_File.txt"
        rfile.content = "This is Nitesh_File_Content."
        rfile_write_hash_id = hashlib.sha256(rfile.meta.filename.encode(ENCODE_TYPE_UTF_8)).hexdigest()
        succ_node_id = self.client_filestore.findSucc(rfile_write_hash_id)
        if succ_node_id.id != self.user_id:
            self.client_transport.close()
            self._createFileStoreClient(succ_node_id.ip, succ_node_id.port)
            self.client_filestore.writeFile(rfile)
            self.client_transport.close()
        else:
            self.client_filestore.writeFile(rfile)
            self.client_transport.close()

    # Method to test readFile RPC
    def _test_read_file(self):

        # Read first file
        self._createFileStoreClient(sys.argv[1], int(sys.argv[2]))
        rfile_read_hash_id = hashlib.sha256("File.txt".encode(ENCODE_TYPE_UTF_8)).hexdigest()
        succ_node_id = self.client_filestore.findSucc(rfile_read_hash_id)
        if succ_node_id.id != self.user_id:
            self.client_transport.close()
            self._createFileStoreClient(succ_node_id.ip, succ_node_id.port)
            rfile_returned = self.client_filestore.readFile("File.txt")
            self._print_file_details(rfile_returned)
            self.client_transport.close()
        else:
            rfile_returned = self.client_filestore.readFile("File.txt")
            self._print_file_details(rfile_returned)
            self.client_transport.close()

        # Read second file
        self._createFileStoreClient(sys.argv[1], int(sys.argv[2]))
        rfile_read_hash_id = hashlib.sha256("Test_File.txt".encode(ENCODE_TYPE_UTF_8)).hexdigest()
        succ_node_id = self.client_filestore.findSucc(rfile_read_hash_id)
        if succ_node_id.id != self.user_id:
            self.client_transport.close()
            self._createFileStoreClient(succ_node_id.ip, succ_node_id.port)
            rfile_returned = self.client_filestore.readFile("Test_File.txt")
            self._print_file_details(rfile_returned)
            self.client_transport.close()
        else:
            rfile_returned = self.client_filestore.readFile("Test_File.txt")
            self._print_file_details(rfile_returned)
            self.client_transport.close()

        # Read third file
        self._createFileStoreClient(sys.argv[1], int(sys.argv[2]))
        rfile_read_hash_id = hashlib.sha256("Nitesh_File.txt".encode(ENCODE_TYPE_UTF_8)).hexdigest()
        succ_node_id = self.client_filestore.findSucc(rfile_read_hash_id)
        if succ_node_id.id != self.user_id:
            self.client_transport.close()
            self._createFileStoreClient(succ_node_id.ip, succ_node_id.port)
            rfile_returned = self.client_filestore.readFile("Nitesh_File.txt")
            self._print_file_details(rfile_returned)
            self.client_transport.close()
        else:
            rfile_returned = self.client_filestore.readFile("Nitesh_File.txt")
            self._print_file_details(rfile_returned)
            self.client_transport.close()

        # Read file which does not exists at any of the servers
        self._createFileStoreClient(sys.argv[1], int(sys.argv[2]))
        rfile_read_hash_id = hashlib.sha256("Non_Existing_File.txt".encode(ENCODE_TYPE_UTF_8)).hexdigest()
        succ_node_id = self.client_filestore.findSucc(rfile_read_hash_id)
        if succ_node_id.id != self.user_id:
            self.client_transport.close()
            self._createFileStoreClient(succ_node_id.ip, succ_node_id.port)
            rfile_returned = self.client_filestore.readFile("Non_Existing_File.txt")
            self._print_file_details(rfile_returned)
            self.client_transport.close()
        else:
            rfile_returned = self.client_filestore.readFile("Non_Existing_File.txt")
            self._print_file_details(rfile_returned)
            self.client_transport.close()

    def _print_file_details(self, rfile_input):
        print("\n----------------------- Contents of Returned RFile from Server ------------------------")
        print("Filename --> %s" % rfile_input.meta.filename)
        print("File Version --> %d" % rfile_input.meta.version)
        print("File Content --> %s" % rfile_input.content)
        print("File Content Hash --> %s" % rfile_input.meta.contentHash)
        print("---------------------------------------------------------------------------------------\n")

    # Method to test getNodeSucc RPC
    def _test_get_node_succ(self):
        node_id_returned = self.client_filestore.getNodeSucc()
        print("\n----------------------- Contents of Returned NodeID from Server -----------------------")
        print("Node IP Address --> %s" % node_id_returned.ip)
        print("Node Port Number --> %d" % node_id_returned.port)
        print("Node Hash Value --> %s" % node_id_returned.id)
        print("Node Hash Value as integer --> %d" % int(node_id_returned.id, 16))
        print("---------------------------------------------------------------------------------------\n")


# Starting point for client execution
if __name__ == "__main__":

    # Validating number of command line arguments
    if len(sys.argv) != 3:
        print(ERROR_CLIENT_ARGV)
        sys.exit(1)

    # Local variable initialization from command line argument
    server_ip_addr = sys.argv[1]
    server_port_num = int(sys.argv[2])

    try:
        chordClientHandler = ChordClientHandler(server_ip_addr, server_port_num)
        chordClientHandler.test_chord_server_methods()
        print(GENERAL_CLIENT_MESSAGE)
    except Thrift.TException as err:
        print(ERROR_CREATE_CLIENT_MSG, err)
        print("\n%s" % err.message)
        print(GENERAL_CLIENT_MESSAGE)
