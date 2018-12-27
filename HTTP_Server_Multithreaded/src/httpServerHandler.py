#!/usr/bin/python3

# Import required libraries
import os
import time
import mimetypes
import threading
from constant import *


# Custom data type to hold resource information
class ResourceInformation:
    """Class to hold details of resource"""

    # Constructor to initialize data members
    def __init__(self, last_modified, content_type, content_length):
        self.file_last_modified_timestamp = last_modified
        self.file_content_type = content_type
        self.file_length = content_length


# Class to handle HTTP Server functionality
class HttpServerHandler(threading.Thread):
    """Class to handle HTTP client requests --- Members prefix with _ are protected here"""

    # Store the state as count for accessed resources at server side for stats
    resource_state_dict = {}

    # Mutex to implement synchronization between threads
    thread_mutex = threading.Lock()

    # Constructor
    def __init__(self, server_socket, connection_socket, client_address):
        threading.Thread.__init__(self)
        self._server_socket = server_socket
        self._connection_socket = connection_socket
        self._client_address = client_address

    # Function works as entry point for each thread
    def run(self):
        self._handle_client_request()

    # Function to collect resource information
    @staticmethod
    def _get_resource_info(requested_file):
        file_modified_time = time.gmtime(os.stat(requested_file).st_mtime)
        content_type = str(mimetypes.MimeTypes().guess_type(requested_file)[0])
        content_length = str(os.path.getsize(requested_file))

        return ResourceInformation(file_modified_time, content_type, content_length)

    # Prepare contents of response header as per status code
    @staticmethod
    def _prepare_response_header(resource_info, status_code):
        header_content = EMPTY_STRING
        if status_code == STATUS_CODE_200:
            response_timestamp = time.strftime(TIMESTAMP_FORMAT, time.gmtime())
            last_modified = time.strftime(TIMESTAMP_FORMAT, resource_info.file_last_modified_timestamp)
            header_content += STATUS_CODE_200_MESSAGE + NEW_LINE_CHAR
            header_content += HEADER_DATE + response_timestamp + NEW_LINE_CHAR
            header_content += HEADER_SERVER + SERVER_NAME_STRING + NEW_LINE_CHAR
            header_content += HEADER_LAST_MODIFIED + last_modified + NEW_LINE_CHAR
            header_content += HEADER_ACCEPT_RANGE + BYTES_STRING + NEW_LINE_CHAR
            header_content += HEADER_CONTENT_LENGTH + resource_info.file_length + NEW_LINE_CHAR
            header_content += HEADER_CONTENT_TYPE + resource_info.file_content_type + NEW_LINE_CHAR
            header_content += HEADER_CONNECTION + CLOSE_STRING + NEW_LINE_CHAR + NEW_LINE_CHAR

        return header_content

    # Receive client's HTTP request and Parse required information
    def _handle_client_request(self):
        # Extract information from client's connection socket
        request_message_byt = self._connection_socket.recv(RECEIVE_RATE)
        request_message = bytes.decode(request_message_byt)
        req_message_list = request_message.split(' ')
        request_method = req_message_list[0]

        # Handling of HTTP GET request
        if request_method == HTTP_GET_METHOD:
            requested_file = req_message_list[1]
            requested_file_path = RESOURCE_ROOT_DIR + requested_file  # Valid resource path
            # Prepare response body and header contents
            if os.path.exists(requested_file_path):
                is_success_code = True
                file_descriptor = open(requested_file_path, 'rb')  # Open file in read-byte mode
                response_body_content = file_descriptor.read()  # Read file contents
                file_descriptor.close()  # Close file descriptor

                # Prepare response header content
                resource_info = self._get_resource_info(requested_file_path)
                response_header_content = self._prepare_response_header(resource_info, STATUS_CODE_200)
                response_content = response_header_content.encode() + response_body_content

            else:
                is_success_code = False
                # Prepare response header content
                response_header_content = EMPTY_STRING
                response_timestamp = time.strftime(TIMESTAMP_FORMAT, time.gmtime())
                response_header_content += STATUS_CODE_404_MESSAGE + NEW_LINE_CHAR
                response_header_content += HEADER_DATE + response_timestamp + NEW_LINE_CHAR
                response_header_content += HEADER_SERVER + SERVER_NAME_STRING + NEW_LINE_CHAR
                response_header_content += HEADER_CONNECTION + CLOSE_STRING + NEW_LINE_CHAR + NEW_LINE_CHAR
                response_body_content = ERROR_MESSAGE_404
                response_content = (response_header_content + response_body_content).encode()

            # Send the response content through client connection socket
            self._connection_socket.send(response_content)

            # Display information of clients served with status OK
            if is_success_code:

                self.thread_mutex.acquire()  # Acquire mutex

                # Update resource count in global shared dictionary data structure
                if requested_file in HttpServerHandler.resource_state_dict:
                    HttpServerHandler.resource_state_dict[requested_file] += 1
                else:
                    HttpServerHandler.resource_state_dict[requested_file] = 1

                print(requested_file + PIPE_SEP + str(self._client_address[0]) +
                      PIPE_SEP + str(self._client_address[1]) +
                      PIPE_SEP + str(HttpServerHandler.resource_state_dict[requested_file]))

                self.thread_mutex.release()  # Release mutex
        else:
            print(ERROR_UNKNOWN_REQUEST_METHOD, request_method)
            return

        # Close connection socket for specific client
        self._connection_socket.close()
