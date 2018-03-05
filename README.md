# Chord-Distributed-Hash-Table

Implemented distributed hash table (DHT) with an architecture like Chord (Scalable Peer-to-peer Lookup Service for Internet Applications) file system using C++ and Apache Thrift framework for communication between different entities. File system provides basic read and write operation with proper version maintenance. Different file servers are managed and tracked for load balancing using setFingertable, findSucc, findPred and getNodeSucc functions. Further, system refers to file with file id created with SHA-256 hash value of the string "<file_owner>:<file_name>” servers are referred by the SHA-256 hash value of the string "<server_ip_address>:<port_number>". 

Compilation and execution: 

1. To compile code: Extract all files and folders to a directory. 
2. Install thrift and set paths accordingly. generate thrift code with "thrift --gen cpp chord.thrift". This should generate gen-cpp directory.  
3. Run make command
4. Once binary file has been created run servers with bash script as "bash server.sh <port#>"
5. Open new window so server logs will be easily readable. 
6. Create node.txt file make sure you have content matching with server instances, then Run ./init node.txt (node.txt contains all file server details in format of: file_server_ip file_server_port)
7. Run your client to test servers on different ports.
