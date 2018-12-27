#!/usr/bin/python3

# Import required libraries
import sys
import socket
import pathlib
from constant import *
import httpServerHandler

'''
HTTP Server supports below functionality:
    1) Handles GET requests only
    2) Every client's request is served by separate software threads
'''


# Class to implement HTTP (Hyper Text Transfer Protocol) Server
class HttpServer:
    """Class to represent HttpServer --- Members prefix with _ are protected here"""

    # Constructor to initialize data members
    def __init__(self):

        # Function call to validate required functionality
        self._validate()

        # Create server socket (IPv4 and TCP)
        try:
            self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error as err:
            print(ERROR_SOCKET_CREATION_FAILED, err)
            sys.exit(1)

        # Server host's IP address and initial port number (0 - to choose from free ports)
        self._server_ip_address = socket.gethostbyname(socket.gethostname())
        self._server_port_number = 0

        # Maximum number of Http clients queued in Http server
        self._backlog_count = 5

        # List of threads serving the client requests
        self._threads_list = []

    # Function to validate specific requirements
    @staticmethod
    def _validate():
        # Check for files directory in current folder
        files_path = pathlib.Path(RESOURCE_ROOT_DIR)
        if files_path.exists() is False | files_path.is_dir() is False:
            print(ERROR_DIR_NOT_FOUND)
            sys.exit(1)

    # Function to bind socket and start listening
    def _start_server(self):
        print(GENERAL_SERVER_MESSAGE)

        # Bind server socket with any available free port of host
        try:
            self._server_socket.bind(('', self._server_port_number))
        except socket.error as err:
            print(ERROR_SOCKET_BINDING_FAILED, err)

        self._server_port_number = self._server_socket.getsockname()[1]
        server_address = (self._server_ip_address, self._server_port_number)

        print("-------------------------------------------------------------------")
        print("Server Host Information:\n-------------------------")
        print("Host Name of server -------------> \t", socket.gethostname())
        print("IP Address and Port Number ------> \t", server_address)
        print("-------------------------------------------------------------------")

        # Server socket in listening mode with provided backlog count
        self._server_socket.listen(self._backlog_count)
        print("\nServer is running ..." + NEW_LINE_CHAR)

    # Function to manage accepting the client requests
    def _accept_connections(self):
        while True:
            try:
                # Accept creates new socket for each client request and returns tuple
                connection_socket, client_address = self._server_socket.accept()

                # Handle client's request in HttpServerHandler using separate threads
                handler_thread = httpServerHandler.HttpServerHandler(self._server_socket,
                                                                     connection_socket, client_address)
                handler_thread.start()

                # Save running threads in list
                self._threads_list.append(handler_thread)

            except socket.error as err:
                print(ERROR_SOCKET_ACCEPT_FAILED, err)
                break

        # Wait till all running threads complete their operation before exiting from method
        for thread_id in self._threads_list:
            thread_id.join()


# Starting point for HTTP Server implementation
if __name__ == "__main__":
    httpServer = HttpServer()
    httpServer._start_server()
    httpServer._accept_connections()
