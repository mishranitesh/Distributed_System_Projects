#!/usr/bin/python3

# Variable to enable/ disable debug messages
DEBUG_FLAG = False

GENERAL_CLIENT_MESSAGE = "\n#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=# CHORD CLIENT SIDE #=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#\n"
GENERAL_SERVER_MESSAGE = "\n#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=# CHORD SERVER SIDE #=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#=#\n"

EMPTY_STRING = ""
SPACE_STRING = " "
COLON_STRING = ":"
NEW_LINE_CHAR = "\n"
ENCODE_TYPE_UTF_8 = "utf-8"
SUCCESS_SET_FINGER_TABLE_MSG = "Finger table successfully set at Server Side.\n"

ERROR_READ_FILE_NOT_FOUND = "ERROR :: File not found at Node --> "
ERROR_CREATE_CLIENT_MSG = "ERROR :: Issue while creating File Store Client.\n"
ERROR_FINDSUCC_MSG = "ERROR :: Problem in finding successor node of current node.\n"
ERROR_MISSING_NODE_FINGERTABLE = "ERROR :: Fingertable does not exists for Node --> "
ERROR_READ_SYSTEM_EXCP_MSG = "ERROR :: File to read is not associated with Node --> "
ERROR_WRITE_SYSTEM_EXCP_MSG = "ERROR :: File to write is not associated with Node --> "
ERROR_FINGERTABLE_SYSTEM_EXCP_MSG = "ERROR :: Unable to set finger table at Node --> "
ERROR_SYSTEM_EXCEPTION_MSG = "ERROR :: System exception occurs during RPC call to Chord Server.\n"
ERROR_SERVER_ARGV = "ERROR :: Incorrect number of arguments. Server expects 1 argument i.e. Port number.\n"
ERROR_CLIENT_ARGV = "ERROR :: Incorrect number of arguments. Client expects 2 argument i.e. Server IP Address and Port number.\n"
