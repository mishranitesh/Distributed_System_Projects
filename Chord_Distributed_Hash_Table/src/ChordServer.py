#!/usr/bin/python3

import sys
import math
import glob
import socket
import hashlib
from constant import *

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
from chord.ttypes import SystemException, RFileMetadata, RFile, NodeID

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer


# Class to represent Chord Server to implement FileStore functionality
class ChordServerFileStoreHandler:
    """Class to represent Chord Server to implement FileStore functionality"""

    # Global dictionary to store file details in Memory for each Server
    file_system_repository_dict = {}

    # Value Constructor to initialize data members
    def __init__(self, server_port):
        self.finger_table_node_list = []  # Initialize finger table node list as empty
        self.current_node_id = self._createNodeId(server_port)  # Create current NodeID object

        print("------------------------------------------------------------------------------------------")
        print("Server IP Address ====== %s" % self.current_node_id.ip)
        print("Server Port Number ===== %s" % self.current_node_id.port)
        print("Server Hash Identifier = %s" % self.current_node_id.id)
        print("------------------------------------------------------------------------------------------\n")

    # Method to create object of NodeId
    def _createNodeId(self, server_port):

        # Create new object for NodeId
        new_node_id = NodeID()
        new_node_id.ip = str(socket.gethostbyname(socket.gethostname()))  # Set Node IP Address as string
        new_node_id.port = server_port  # Set Node Port Number as integer

        # Set Node's ID as SHA256 hash value using "<Node_IP>:<Node_Port>" as string
        encoded_address = (new_node_id.ip + COLON_STRING + str(new_node_id.port)).encode(ENCODE_TYPE_UTF_8)
        new_node_id.id = hashlib.sha256(encoded_address).hexdigest()

        return new_node_id

    # Method to create new client to perform File Store RPC calls
    def _createFileStoreClient(self, ip_address, port_number):

        # Create Client side Apache Thrift configurations
        client_socket = TSocket.TSocket(ip_address, port_number)  # Raw socket creation using Thrift API
        client_transport = TTransport.TBufferedTransport(client_socket)  # Enable buffering in above raw socket
        client_protocol = TBinaryProtocol.TBinaryProtocol(client_transport)  # Client protocol to send contents as Binary (marshalling and unmarshalling)
        client_filestore = FileStore.Client(client_protocol)  # Client to use protocol encoder
        client_transport.open()  # Connect to specified Chord Server with above configuration
        return client_filestore

    # Method to write file at server side
    def writeFile(self, rfile_input):

        # Find the SHA256 hash value for input rfile
        rfile_write_hash_id = hashlib.sha256(rfile_input.meta.filename.encode(ENCODE_TYPE_UTF_8)).hexdigest()

        # Find successor of input file key identifier
        key_succ_node_id = self.findSucc(rfile_write_hash_id)

        if DEBUG_FLAG:
            print("Inside Write ==> Input filename =============== %s" % rfile_input.meta.filename)
            print("Inside Write ==> Input file content =========== %s" % rfile_input.content)
            print("Inside Write ==> Key Identifier of Input File = %s" % rfile_write_hash_id)
            print("Inside Write ==> Successor of Key Identifier == %s" % key_succ_node_id.id)
            print("Inside Write ==> Current Node's Hash Value Id = %s\n" % self.current_node_id.id)

        # To serve the request successor node of key should be the current node
        if key_succ_node_id.ip == self.current_node_id.ip and \
                key_succ_node_id.port == self.current_node_id.port and \
                key_succ_node_id.id == self.current_node_id.id:

            # Check whether rfile exists in current file system repository
            # if yes, then update the version of the file in repository
            # otherwise, insert new entry for this file in repository
            if rfile_write_hash_id in ChordServerFileStoreHandler.file_system_repository_dict:
                existing_rfile = ChordServerFileStoreHandler.file_system_repository_dict[rfile_write_hash_id]
                existing_rfile.meta.version += 1
                existing_rfile.content = rfile_input.content
                existing_rfile.meta.contentHash = hashlib.sha256(rfile_input.content.encode(ENCODE_TYPE_UTF_8)).hexdigest()
            else:
                # Local variables
                rfile_write_meta = RFileMetadata()
                rfile_write = RFile()
                rfile_write.meta = rfile_write_meta

                # initialize new rfile using input rfile sent by Client
                rfile_write.meta.version = 0
                rfile_write.content = rfile_input.content
                rfile_write.meta.filename = rfile_input.meta.filename
                # Set content hash as SHA256 hash value using "file content" as string
                rfile_write.meta.contentHash = hashlib.sha256(rfile_write.content.encode(ENCODE_TYPE_UTF_8)).hexdigest()
                # Insert new rfile into global file system repository with version 0
                ChordServerFileStoreHandler.file_system_repository_dict[rfile_write_hash_id] = rfile_write

            if DEBUG_FLAG:
                print("\n---------------------------------------- FILE SYSTEM REPO ----------------------------------------")
                for hash_k, rfile_v in ChordServerFileStoreHandler.file_system_repository_dict.items():
                    rfile_desc = rfile_v.meta.filename + SPACE_STRING + \
                                 str(rfile_v.meta.version) + SPACE_STRING + rfile_v.content
                    print(hash_k, " --> %s" % rfile_desc)
                print("--------------------------------------------------------------------------------------------------\n")
        else:
            writeFile_system_ex = SystemException()
            writeFile_system_ex.message = ERROR_WRITE_SYSTEM_EXCP_MSG + \
                                          self.current_node_id.ip + COLON_STRING + \
                                          str(self.current_node_id.port)
            raise writeFile_system_ex

    # Method to read file at server side
    def readFile(self, filename_input):

        # Find the SHA256 hash value for input rfile
        rfile_read_hash_id = hashlib.sha256(filename_input.encode(ENCODE_TYPE_UTF_8)).hexdigest()

        # Find successor of input file key identifier
        key_succ_node_id = self.findSucc(rfile_read_hash_id)

        if DEBUG_FLAG:
            print("Inside Read ==> Input filename =============== %s" % filename_input)
            print("Inside Read ==> Key Identifier of Input File = %s" % rfile_read_hash_id)
            print("Inside Read ==> Successor of Key Identifier == %s" % key_succ_node_id.id)
            print("Inside Read ==> Current Node's Hash Value Id = %s\n" % self.current_node_id.id)

        # To serve the request successor node of key should be the current node
        if key_succ_node_id.ip == self.current_node_id.ip and \
                key_succ_node_id.port == self.current_node_id.port and \
                key_succ_node_id.id == self.current_node_id.id:

            # Check for File Not Found Exception
            if rfile_read_hash_id in ChordServerFileStoreHandler.file_system_repository_dict:
                # Local variables
                rfile_read_meta = RFileMetadata()
                rfile_read = RFile()
                rfile_read.meta = rfile_read_meta

                existing_rfile = ChordServerFileStoreHandler.file_system_repository_dict[rfile_read_hash_id]
                rfile_read.meta = existing_rfile.meta
                rfile_read.content = existing_rfile.content
                return rfile_read
            else:
                readFile_system_ex = SystemException()
                readFile_system_ex.message = ERROR_READ_FILE_NOT_FOUND + \
                                             self.current_node_id.ip + COLON_STRING + \
                                             str(self.current_node_id.port)
                raise readFile_system_ex

        else:
            readFile_system_ex = SystemException()
            readFile_system_ex.message = ERROR_READ_SYSTEM_EXCP_MSG + \
                                         self.current_node_id.ip + COLON_STRING + \
                                         str(self.current_node_id.port)
            raise readFile_system_ex

    # Method to set finger table for current server node
    def setFingertable(self, node_list_input):

        # Raise exception in case of empty input node list, otherwise set finger table node list
        if len(node_list_input) != 0:
            self.finger_table_node_list = node_list_input
        else:
            setFingertable_system_ex = SystemException()
            setFingertable_system_ex.message = ERROR_FINGERTABLE_SYSTEM_EXCP_MSG + \
                                               self.current_node_id.ip + COLON_STRING + \
                                               str(self.current_node_id.port)
            raise setFingertable_system_ex

        print(SUCCESS_SET_FINGER_TABLE_MSG)

        if DEBUG_FLAG:
            for node_id in self.finger_table_node_list:
                print("Node Info --> " + node_id.id + " : " + node_id.ip + " : " + str(node_id.port))
            print("------------------------------------------------------------------------------------------\n")

    # Method to find Node that owns the given key_identifier
    def findSucc(self, key_identifier):

        # Find predecessor Node for provided key identifier
        pred_node_id = self.findPred(key_identifier)

        if DEBUG_FLAG:
            print("Inside FindSucc ==> Pred of Key Identifier = %s" % pred_node_id.id)

        # No need to create new client if Predecessor node is current node
        if pred_node_id.id == self.current_node_id.id:
            if DEBUG_FLAG:
                print("Inside FindSucc ==> Succ of Pred Node == %s\n" % self.getNodeSucc().id)
            return self.getNodeSucc()

        # Create new client to get predecessor node's successor
        try:
            pred_client_filestore = self._createFileStoreClient(pred_node_id.ip, pred_node_id.port)
        except Thrift.TException as err:
            print(ERROR_CREATE_CLIENT_MSG, err)
            print("\n%s" % err.message)

        # Another RPC call to get the successor of found predecessor node
        try:
            pred_node_id_succ = pred_client_filestore.getNodeSucc()
        except SystemException as ex:
            print(ERROR_FINDSUCC_MSG, ex)
            print("\n%s" % ex.message)
            sys.exit(1)

        if DEBUG_FLAG:
            print("Inside FindSucc ==> Succ of Pred Node == %s\n" % pred_node_id_succ.id)

        return pred_node_id_succ

    # Method to find Node that immediately precedes given key_identifier
    def findPred(self, key_identifier):

        # Raise exception in case of empty finger table, otherwise return predecessor node
        pred_node_id = self.current_node_id
        if len(self.finger_table_node_list) != 0:
            # Logic to identify predecessor node for given key identifier
            power_num = math.pow(2, 256)
            current_node_hash_num = int(self.current_node_id.id, 16)
            key_num = int(key_identifier, 16)
            finger_table_hash_num = int(self.finger_table_node_list[0].id, 16)

            if current_node_hash_num > finger_table_hash_num:
                finger_table_hash_num += power_num
            if key_num < current_node_hash_num:
                key_num += power_num

            # Check if Key falls in the range of first entry of Finger Table
            if current_node_hash_num < key_num <= finger_table_hash_num:
                pred_node_id = self.current_node_id
            else:
                # Find closest preceding finger table entry
                for i in range(len(self.finger_table_node_list) - 1, -1, -1):
                    current_node_hash_num = int(self.current_node_id.id, 16)
                    key_num = int(key_identifier, 16)
                    finger_table_hash_num = int(self.finger_table_node_list[i].id, 16)

                    if current_node_hash_num > finger_table_hash_num:
                        finger_table_hash_num += power_num
                    if key_num < current_node_hash_num:
                        key_num += power_num
                    if current_node_hash_num < finger_table_hash_num < key_num and \
                            self.current_node_id.id != self.finger_table_node_list[i].id:
                        # Create new client to get predecessor node
                        new_client_filestore = self._createFileStoreClient(self.finger_table_node_list[i].ip,
                                                                           self.finger_table_node_list[i].port)
                        return new_client_filestore.findPred(key_identifier)

                if DEBUG_FLAG:
                    print("Inside FindPred ==> Key is not referenced by Finger Table.")

        else:
            findPred_system_ex = SystemException()
            findPred_system_ex.message = ERROR_MISSING_NODE_FINGERTABLE + \
                                         self.current_node_id.ip + COLON_STRING + \
                                         str(self.current_node_id.port)
            raise findPred_system_ex

        if DEBUG_FLAG:
            print("Inside FindPred ==> Return Pred Node Id = %s" % pred_node_id.id)

        return pred_node_id

    # Method to return closest DHT node that follows the current node in Chord key-space
    def getNodeSucc(self):

        # Raise exception in case of empty finger table, otherwise return first entry of list
        if len(self.finger_table_node_list) != 0:
            return self.finger_table_node_list[0]
        else:
            getNodeSucc_system_ex = SystemException()
            getNodeSucc_system_ex.message = ERROR_MISSING_NODE_FINGERTABLE + \
                                            self.current_node_id.ip + COLON_STRING + \
                                            str(self.current_node_id.port)
            raise getNodeSucc_system_ex


# Starting point for Chord Server execution
if __name__ == "__main__":

    # Validating number of command line arguments
    if len(sys.argv) != 2:
        print(ERROR_SERVER_ARGV)
        sys.exit(1)

    print(GENERAL_SERVER_MESSAGE)

    # Local variables
    server_port_num = int(sys.argv[1])

    # Create server side Apache Thrift configurations
    fileStoreHandler = ChordServerFileStoreHandler(server_port_num)  # Object to handle File Store RPC calls
    server_processor = FileStore.Processor(fileStoreHandler)  # Mapping of FileStore Processor and File Store Handler
    server_socket_transport = TSocket.TServerSocket(port=server_port_num)  # Server side socket enabled with provided port number
    server_tfactory = TTransport.TBufferedTransportFactory()  # Enable buffering at server side socket
    server_pfactory = TBinaryProtocol.TBinaryProtocolFactory()  # Server protocol to send contents as Binary (marshalling and unmarshalling)

    # Creation of simple thrift server object enabled with above configurations
    chord_server = TServer.TSimpleServer(server_processor, server_socket_transport, server_tfactory, server_pfactory)

    print("Server is running ...")
    print("\n------------------------------------------------------------------------------------------\n")
    chord_server.serve()  # Start node server to serve requests in Chord structure

    print("Server has served all the requests successfully." + NEW_LINE_CHAR)
