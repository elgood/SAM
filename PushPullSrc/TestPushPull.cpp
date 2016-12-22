/*
 * TestPushPull.cpp
 *
 *  Created on: Dec 10, 2016
 *      Author: elgood
 */

#include <string>
#include <list>
#include <stdlib.h>
#include <iostream>

#include <boost/program_options.hpp>

#include <ReadSocket.h>

using std::string;
using std::list;

namespace po = boost::program_options;

int main(int argc, char** argv) {

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
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }


	ReadSocket reciever(ip, ncPort);

  list<string> hostnames(numNodes);
  list<int> ports(numNodes);

  if (numNodes == 1) {
    hostnames.push_back("127.0.0.1");
    ports.push_back(startingPort);
  } else {
    for (int i = 0; i < numNodes; i++) {
      hostnames.push_back(prefix + boost::lexical_cast<string>(i));
      ports.push_back(startingPort + i);  
    }
  }


}

