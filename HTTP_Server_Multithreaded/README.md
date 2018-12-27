## Multithreaded HTTP Server to handle client's GET request

------------------------------------------------------------------------------------------------------

#### Steps to compile and run the program:

* Use below command to compile and run the HTTP Server:
	* make start_server

* Use below command to clean the linked objects:
    * make clean

------------------------------------------------------------------------------------------------------

#### Brief description about the implementation:

Please find explanation of my implementation of HTTP Server as below:

* httpServer.py
	* Before creating server side socket, code will validate for required "files" folder at current directory, in case of failure exits the process.
	* Creating server side socket, binding the socket with localhost and any available free port (passing 0 as port number to let OS decide the free port).
	* Configured the server side socket to listen new client connections with BackLog value of 5 (at most 5 client requests can perform 3-way handshaking simultaneously).
	* Server socket will wait to accept new client request and will create new connection socket for each new request.
		* Each client request will be served by separate software threads to let main thread check for new client request.

* httpServerHandler.py
	* It extends "threading.Thread" library class to invoke run() method by each thread
	* Extract the relevant information from HTTP Client Request.
	* Read the contents of requested file in case of GET request.
	* Preparing HTTP Response using HTTP Header and HTTP Body contents.
	* Send the HTTP Response to respective client using tuple information came from client side.
	* Change the state of requested object in Dictionary data structure.
		* Prevent race condition for shared Dictionary, using mutex to synchronize all threads contending for this resource.
	* Close the client connection socket at the end of method.

* constant.py
	* Contains all constants used in project.

* Useful "wget" commands attributes:
    * '--server-response' : Display HTTP Server Response on console sent by server.
    * '--save-headers' : Append HTTP Server Response at the start of downloaded file.
    * '--limit-rate=1k' : Limits the rate of downloading the resource from server.

------------------------------------------------------------------------------------------------------

#### Sample input and output

* Test Case for 'Status 200':
	* SERVER SIDE:
		* Host Name of server -------------> 	 q22-08\
		  IP Address and Port Number ------> 	 ('10.33.1.72', 57977)
		* /pdf-sample.pdf | 10.33.1.72 | 45726 | 1

	* CLIENT SIDE:
		* wget --server-response --limit-rate=100k http://q22-07:40971/pdf-sample.pdf
		* HTTP/1.1 200 OK\
  		  Date: Thu, 20 Sep 2018 12:53:47 GMT\
  		  Server: GET_HTTP_Server (Nitesh Mishra)\
  		  Last-Modified: Thu, 20 Sep 2018 10:16:51 GMT\
  		  Accept-Ranges: bytes\
  		  Content-Length: 786572\
  		  Content-Type: document/pdf\
  		  Connection: Close

* Test Case for 'Status 404':
	* CLIENT SIDE:
		* wget --server-response --limit-rate=100k http://q22-07:40971/nofile.txt
		* HTTP/1.1 404 Not Found\
  		  Date: Thu, 20 Sep 2018 12:56:29 GMT\
  		  Server: GET_HTTP_Server (Nitesh Mishra)\
		  Connection: Close\

* Test Case when 'files' folder does not exists at server side:
    * SERVER SIDE:
        * Command to start the HTTP Server --> 'make start_server'
        * Server will not start and displays below error message:
            * ERROR :: Required directory named 'files' does not found...!!

------------------------------------------------------------------------------------------------------
