/* Avinash Narsale: anarsal1 */

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PlatformThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <boost/make_shared.hpp>
#include <TToString.h>

#include <iostream>
#include <stdexcept>
#include <sstream>
#include <iomanip>
#include "openssl/sha.h"
#include <mutex>
#include <sys/types.h>
#include <ifaddrs.h>
#include <math.h>
#include <algorithm>
#include <fstream>
#include <map>

#include "../gen-cpp/FileStore.h"

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::concurrency;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

mutex myMutex;
int bits_val=256;
string curr_id;
string curr_ip;
int curr_port=0;

bool mycompare (char c1, char c2)
{ return std::tolower(c1)<std::tolower(c2); }

bool mycompare_equal (char c1, char c2)
{ return std::tolower(c1)<=std::tolower(c2); }


class FileStoreHandler : public FileStoreIf {
public:
		std::vector<NodeID> finger_table;
		std::map<string,RFile> file_list;
		
		FileStoreHandler() {}
		
		void writeFile(const RFile& rFile){
			//cout << "|Server LOG| Port:" << curr_port << "|" << "--writeFile-Enter----------------" << endl;
			NodeID succ_node;
			string s_key = rFile.meta.owner+":"+rFile.meta.filename;
			unsigned char hash[SHA256_DIGEST_LENGTH];
			SHA256_CTX sha256;
			SHA256_Init(&sha256);
			SHA256_Update(&sha256, s_key.c_str(), s_key.size());
			SHA256_Final(hash, &sha256);
			stringstream ss;
			for(int i = 0; i < SHA256_DIGEST_LENGTH; i++)
			{
				ss << hex << setw(2) << setfill('0') << (int)hash[i];
			}
			
			findSucc(succ_node,ss.str());
			//cout << "|Server LOG| Port:" << curr_port << "|" << "--writeFile----------------succ_node.port: " << succ_node.port << endl;
			if(curr_id!=succ_node.id){
				SystemException io;
				const std::string message="File is not owned by this server. ";
				io.__set_message(message);
				throw io;
			}
			
			auto it=file_list.find(ss.str());
			if(it !=file_list.end()){
				// make new entry with version+1
				it->second.__set_content(rFile.content);
				it->second.meta.__set_contentHash(rFile.meta.contentHash);
				it->second.meta.__set_version(it->second.meta.version+1);
			}else{
				// make new entry with version 0
				RFile temp=rFile;
				temp.meta.__set_version(0);
				file_list.insert(std::pair<string,RFile>(ss.str(),temp));
			}
			
			/*auto it1=file_list.find(ss.str());
			cout << "|Server LOG| Port:" << curr_port << "|" << "--File written-------------------------" << endl;
			cout << "SERVER -> rFile.meta.filename:" << it1->second.meta.filename << endl;
			cout << "SERVER -> rFile.meta.version:" << it1->second.meta.version << endl;
			cout << "SERVER -> rFile.meta.owner:" << it1->second.meta.owner << endl;
			cout << "SERVER -> rFile.meta.contentHash:" << it1->second.meta.contentHash << endl;
			cout << "SERVER -> rFile.content:" << it1->second.content << endl;	
			cout << "|Server LOG| Port:" << curr_port << "|" << "-------------------------------------------------------------" << endl;*/
		}

		void readFile(RFile& _return, const std::string& filename, const UserID& owner){
			string s_key = owner+":"+filename;
			unsigned char hash[SHA256_DIGEST_LENGTH];
			SHA256_CTX sha256;
			SHA256_Init(&sha256);
			SHA256_Update(&sha256, s_key.c_str(), s_key.size());
			SHA256_Final(hash, &sha256);
			stringstream ss;
			for(int i = 0; i < SHA256_DIGEST_LENGTH; i++)
			{
				ss << hex << setw(2) << setfill('0') << (int)hash[i];
			}
			NodeID succ_node;
			//cout << "|Server LOG| Port:" << curr_port << "|" << "~~~~>curr_port before findSucc:" << curr_port << endl;
			findSucc(succ_node,ss.str());
			//cout << "|Server LOG| Port:" << curr_port << "|" << "~~~~>curr_port after findSucc:" << curr_port << " & succ port:" << succ_node.port << endl;
			
			if(curr_id!=succ_node.id){
				SystemException io;
				const std::string message="File is not owned by this server!";
				io.__set_message(message);
				throw io;
			}
			auto it=file_list.find(ss.str());
			if(it != file_list.end()){
				_return=it->second;
			}else{
				SystemException io;
				const std::string message="File does not exist on this server!";
				io.__set_message(message);
				throw io;
			}
			//cout << "|Server LOG| Port:" << curr_port << "|" << "-Read file successfully!" << endl;
		}

		void setFingertable(const std::vector<NodeID> & node_list){
			myMutex.lock();
			finger_table=node_list; 
			//cout << "|Server LOG| Port:" << curr_port << "|" <<  " finger_table entries for server: " << finger_table.size() << endl;
			myMutex.unlock();
		}

		void findSucc(NodeID& _return, const std::string& key) {
			NodeID temp_NodeID;
			findPred(temp_NodeID,key);
			//cout << "|Server LOG| Port:" << curr_port << "|" << "pred of key: " << temp_NodeID.port << endl;
			
			if(curr_id==key){
				//cout << "++++++++++|Server LOG| Port:" << curr_port << "|" << " both KEY and node are same ++++++++++" << endl;
				_return=temp_NodeID;
				return;
			}
			
			assert(temp_NodeID.port > 0);
			boost::shared_ptr<TSocket> socket_fp_1(new TSocket("localhost", temp_NodeID.port));
			boost::shared_ptr<TTransport> transport_fp_1(new TBufferedTransport(socket_fp_1));
			boost::shared_ptr<TProtocol> protocol_fp_1(new TBinaryProtocol(transport_fp_1));

			FileStoreClient client_fp_1(protocol_fp_1);
			transport_fp_1->open();
			client_fp_1.getNodeSucc(_return);
			transport_fp_1->close();
			//cout << "|Server LOG| Port:" << curr_port << "|" << "succ of key: " << _return.port << endl;
		}

		bool between_nodes(string key, string lower_node, string higher_node){
			// lower_node < key
			bool chk1=lexicographical_compare(lower_node.c_str(),lower_node.c_str()+64,key.c_str(),key.c_str()+64,mycompare);
			// key < higher_node 
			bool chk2=lexicographical_compare(key.c_str(),key.c_str()+64,higher_node.c_str(),higher_node.c_str()+64,mycompare);

			if(chk1&&chk2){
				return true;
			}
			return false;
		}
		
		void closest_preceding_finger(NodeID& _return, const std::string& key) {
			int i;
			for (i = 256 - 1; i >= 0; i--) {
				myMutex.lock();
				string finger_node_ID = finger_table[i].id;
				myMutex.unlock();
				bool reverse_chk = lexicographical_compare(key.c_str(),key.c_str()+64,
					curr_id.c_str(),curr_id.c_str()+64,mycompare);
				if(reverse_chk){
					bool id_less=lexicographical_compare(finger_node_ID.c_str(),finger_node_ID.c_str()+64,key.c_str(),key.c_str()+64);
					bool id_high=lexicographical_compare(curr_id.c_str(),curr_id.c_str()+64,finger_node_ID.c_str(),finger_node_ID.c_str()+64);
					if(id_high||id_less){
						myMutex.lock();
						_return = finger_table[i];
						myMutex.unlock();
						return;
					}
				}
				if (between_nodes(finger_node_ID, curr_id, key)) {
					myMutex.lock();
					_return = finger_table[i];
					myMutex.unlock();
					return;
				}
			}
			//cout << "|Server LOG| Port:" << curr_port << "|" << ":::::: closest_preceding_finger - INVALID _return. :::::: " << endl;
			_return.id = curr_id;
			_return.ip = curr_ip;
			_return.port = curr_port;
		}
		
		void findPred(NodeID& _return, const std::string& key) {
			if(finger_table.size()<1){
				SystemException io;
				const std::string message="Finger table does not exist for this server!";
				io.__set_message(message);
				throw io;
			}
			NodeID ret_NodeID;
			ret_NodeID.id=curr_id;
			ret_NodeID.ip=curr_ip;
			ret_NodeID.port=curr_port;
			
			NodeID succ_NodeID;
			myMutex.lock();
			succ_NodeID=finger_table[0];
			myMutex.unlock();

			//cout << "|Server LOG| Port:" << curr_port << "|" << " looking inbetween port:" << ret_NodeID.port << " and " << succ_NodeID.port << endl;

			// check if succ_NodeID < ret_NodeID
			bool reverse_chk = lexicographical_compare(succ_NodeID.id.c_str(),succ_NodeID.id.c_str()+64,
					ret_NodeID.id.c_str(),ret_NodeID.id.c_str()+64,mycompare);
					
			if(ret_NodeID.id==succ_NodeID.id){
				//skip as successor is equal to current node
				_return=ret_NodeID;
				return;
			}
			if(reverse_chk){
				bool key_less_succ=lexicographical_compare(key.c_str(),key.c_str()+64,succ_NodeID.id.c_str(),succ_NodeID.id.c_str()+64);
				bool key_high_pred=lexicographical_compare(ret_NodeID.id.c_str(),ret_NodeID.id.c_str()+64,key.c_str(),key.c_str()+64);
				if(key_less_succ||key_high_pred){
					_return=ret_NodeID;
				}
				else
				{
					NodeID temp_NodeID;
					closest_preceding_finger(temp_NodeID, key);
					if (ret_NodeID.id == temp_NodeID.id) {
						//cout << ">>|Server LOG| Port:" << curr_port << "|" << "2.1 ret_NodeID.id:" << ret_NodeID.id << endl;
						_return=ret_NodeID;
					}else{
						ret_NodeID.id = temp_NodeID.id;
						ret_NodeID.port = temp_NodeID.port;
						//cout << "|Server LOG| Port:" << curr_port << "|" << "1. Hopping to server with port_number:" << ret_NodeID.port << endl;
						assert(ret_NodeID.port > 0);
						boost::shared_ptr<TSocket> socket_fp_2(new TSocket("localhost", ret_NodeID.port));
						boost::shared_ptr<TTransport> transport_fp_2(new TBufferedTransport(socket_fp_2));
						boost::shared_ptr<TProtocol> protocol_fp_2(new TBinaryProtocol(transport_fp_2));
						FileStoreClient client_fp_2(protocol_fp_2);
						transport_fp_2->open();
						client_fp_2.findPred(ret_NodeID,key);
						transport_fp_2->close();
					}

				}
			}else
			{
				if (between_nodes(key, ret_NodeID.id, succ_NodeID.id) == false) 
				{
					NodeID temp_NodeID;
					closest_preceding_finger(temp_NodeID, key);
					if (ret_NodeID.id == temp_NodeID.id) {
						//cout << ">>|Server LOG| Port:" << curr_port << "|" << "2.2 ret_NodeID.id:" << ret_NodeID.id << endl;
						_return=ret_NodeID;
					}else{
						ret_NodeID.id = temp_NodeID.id;
						ret_NodeID.port = temp_NodeID.port;
						
						//cout << "|Server LOG| Port:" << curr_port << "|" << "2. Hopping to server with port_number:" << ret_NodeID.port << endl;
						assert(ret_NodeID.port > 0);
						boost::shared_ptr<TSocket> socket_fp_3(new TSocket("localhost", ret_NodeID.port));
						boost::shared_ptr<TTransport> transport_fp_3(new TBufferedTransport(socket_fp_3));
						boost::shared_ptr<TProtocol> protocol_fp_3(new TBinaryProtocol(transport_fp_3));
						FileStoreClient client_fp_3(protocol_fp_3);
						transport_fp_3->open();
						client_fp_3.findPred(ret_NodeID,key);
						transport_fp_3->close();
					}
				}
			}
			// return finalized node 
			_return=ret_NodeID;
		}

		void getNodeSucc(NodeID& _return) {
			if(finger_table.size()<1){
				SystemException io;
				const std::string message="Finger table does not exist!";
				io.__set_message(message);
				throw io;
			}
			myMutex.lock();
			_return=finger_table[0];
			myMutex.unlock();
		}

	protected:
//		NodeID s_NodeID;
};

int main(int argc, char* argv[]) {

	if(argc!=2){
		cerr << "Invalid arguments! Usage: ./server <port#>" << endl;
		exit(EXIT_FAILURE);
	}
	curr_port=atoi(argv[1]);
	
	struct ifaddrs *addrs, *tmp;
	if (getifaddrs(&addrs) == -1) {
		cerr << "ifaddrs is not set properly.. issue with environment setup!" << endl;
		exit(EXIT_FAILURE);
	}
	tmp = addrs;
	string eth0="eth0";
	while (tmp){
		if (tmp->ifa_addr && tmp->ifa_addr->sa_family == AF_INET){
			struct sockaddr_in *pAddr = (struct sockaddr_in *)tmp->ifa_addr;
			if(eth0.compare(string(tmp->ifa_name))==0){
				curr_ip=inet_ntoa(pAddr->sin_addr);
			}
		}
		tmp = tmp->ifa_next;
	}
	freeifaddrs(addrs);

	string s_key = curr_ip+":"+to_string(curr_port);
	unsigned char hash[SHA256_DIGEST_LENGTH];
	SHA256_CTX sha256;
	SHA256_Init(&sha256);
	SHA256_Update(&sha256, s_key.c_str(), s_key.size());
	SHA256_Final(hash, &sha256);
	stringstream ss;
	for(int i = 0; i < SHA256_DIGEST_LENGTH; i++)
	{
		ss << hex << setw(2) << setfill('0') << (int)hash[i];
	}	
	curr_id=ss.str();
	cout << "Server port:" << curr_port << " and server ip:" << curr_ip << endl;
	
	TThreadedServer server(
    boost::make_shared<FileStoreProcessor>(boost::make_shared<FileStoreHandler>()),
    boost::make_shared<TServerSocket>(curr_port), //port
    boost::make_shared<TBufferedTransportFactory>(),
    boost::make_shared<TBinaryProtocolFactory>());
	
	cout << "Starting the server..." << endl;
	server.serve();
	cout << "Done." << endl;
	return 0;
}

