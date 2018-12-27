#!/usr/bin/python3

PIPE_SEP = " | "
EMPTY_STRING = ""
RECEIVE_RATE = 1024
NEW_LINE_CHAR = "\n"
HTTP_GET_METHOD = "GET"
RESOURCE_ROOT_DIR = "./src/files"
TIMESTAMP_FORMAT = "%a, %d %b %Y %H:%M:%S %Z"
SERVER_NAME_STRING = "GET_HTTP_Server (Nitesh Mishra)"

STATUS_CODE_200 = 200
STATUS_CODE_200_MESSAGE = "HTTP/1.1 200 OK"

STATUS_CODE_404 = 404
STATUS_CODE_404_MESSAGE = "HTTP/1.1 404 Not Found"

HEADER_DATE = "Date: "
HEADER_SERVER = "Server: "
HEADER_LAST_MODIFIED = "Last-Modified: "
HEADER_ACCEPT_RANGE = "Accept-Ranges: "
HEADER_CONTENT_LENGTH = "Content-Length: "
HEADER_CONTENT_TYPE = "Content-Type: "
HEADER_CONNECTION = "Connection: "

BYTES_STRING = "bytes"
CLOSE_STRING = "Close"

ERROR_MESSAGE_404 = "<html><title>Status 404</title><body><h1>Status 404 : File Not Found</h1></body></html>"
ERROR_DIR_NOT_FOUND = "\nERROR :: Required directory named 'www' does not found...!!\n"
ERROR_SOCKET_CREATION_FAILED = "\nERROR :: Socket creation failed at Server side...!!\n"
ERROR_SOCKET_BINDING_FAILED = "\nERROR :: Socket binding failed at Server side...!!"
ERROR_SOCKET_ACCEPT_FAILED = "\nERROR :: Socket accept failed at Server side...!!"
ERROR_UNKNOWN_REQUEST_METHOD = "ERROR :: Unknown request method for HTTP Server ---> \t"

GENERAL_SERVER_MESSAGE = "\n#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=# SERVER SIDE #=#=#=#=#=#=#=#=#=#=#=#=#=#=#\n"
