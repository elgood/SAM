/*
 * TestTopK.cpp
 *
 *  Created on: Dec 25, 2016
 *      Author: elgood
 */

#define DEBUG 1

#include <string>
#include <vector>
#include <stdlib.h>
#include <iostream>

#include <boost/program_options.hpp>

#include "ReadSocket.h"
#include "ZeroMQPushPull.h"
#include "TopK.h"

using std::string;
using std::vector;
using std::cout;
using std::endl;

namespace po = boost::program_options;

using namespace sam;

int main(int argc, char** argv) {

#ifdef DEBUG
  cout << "DEBUG: At the beginning of main" << endl;
#endif

	// The ip to read the nc data from.
	string ip;

	// The port to read the nc data from.
	int ncPort;

	// The number of nodes in the cluster
	int numNodes;

	// The node id of this node
	int nodeId;

	// The prefix to the nodes
	string prefix;

	// The starting port number
	int startingPort;

	// The high-water mark
	long hwm;

  // The length of the input queue
  int queueLength;

  // How many simultaneous topk operators
  int ntopk;

  // The total number of elements in a sliding window
  int N;

  // The number of elements in a dormant or active window
  int b;

  // The number of elements to keep track of
  int k;

	time_t timestamp_sec1, timestamp_sec2;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "help message")
    ("ip", po::value<string>(&ip)->default_value("localhost"), 
      "The ip to receive the data from nc")
    ("ncPort", po::value<int>(&ncPort)->default_value(9999), 
      "The port to receive the data from nc")
    ("numNodes", po::value<int>(&numNodes)->default_value(1), 
      "The number of nodes involved in the computation")
    ("nodeId", po::value<int>(&nodeId)->default_value(0), 
      "The node id of this node")
    ("prefix", po::value<string>(&prefix)->default_value("node"), 
      "The prefix common to all nodes")
    ("startingPort", po::value<int>(&startingPort)->default_value(10000), 
      "The starting port for the zeromq communications")
    ("hwm", po::value<long>(&hwm)->default_value(10000), 
      "The high water mark (how many items can queue up before we start "
      "dropping)")
    ("queueLength", po::value<int>(&queueLength)->default_value(10000),
      "We fill a queue before sending things in parallel to all consumers."
      "  This controls the size of that queue.")
    ("ntopk", po::value<int>(&ntopk)->default_value(1),
      "The number of simultaneous topk operators")
    ("N", po::value<int>(&N)->default_value(10000),
      "The total number of elements in a sliding window")
    ("b", po::value<int>(&b)->default_value(1000),
      "The number of elements per block (active or dynamic window)")
    ("k", po::value<int>(&k)->default_value(2),
      "The number of top elements to keep track of per block.")
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

#ifdef DEBUG
  cout << "Options" << endl;
  cout << "numNodes " << numNodes << endl;
  cout << "nodeId " << nodeId << endl;
#endif


	ReadSocket receiver(ip, ncPort);
#ifdef DEBUG
  cout << "DEBUG: main created receiver " << endl;
#endif

  vector<string> hostnames(numNodes);
  vector<int> ports(numNodes);

  if (numNodes == 1) {
    hostnames[0] = "127.0.0.1";
    ports[0] = startingPort;
  } else {
    for (int i = 0; i < numNodes; i++) {
      hostnames[i] = prefix + boost::lexical_cast<string>(i);
      ports[i] = (startingPort + i);  
    }
  }

  ZeroMQPushPull consumer(queueLength,
                               numNodes, 
                               nodeId, 
                               hostnames, 
                               ports, 
                               hwm);

#ifdef DEBUG
  cout << "DEBUG: main created consumer " << endl;
#endif

  receiver.registerConsumer(&consumer);

  vector<size_t> keyFields;
  keyFields.push_back(6);
  int valueField = 8;
  string delimiter = ",";
  for (int i = 0; i < ntopk; i++) {
    TopK* topk = new TopK(N, b, k, keyFields, valueField, delimiter, nodeId);
    consumer.registerConsumer(topk); 
  }

  

  if (!receiver.connect()) {
    std::cout << "Couldn't connected to " << ip << ":" << ncPort << std::endl;
    return -1;
  }
	time(&timestamp_sec1);
  receiver.receive();
	time(&timestamp_sec2);
	std::cout << "Seconds " << timestamp_sec2 - timestamp_sec1 << std::endl;


}

