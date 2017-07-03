#include "ReadSocket.h"
#include "ZeroMQPushPull.h"
#include "TopK.hpp"
#include "Filter.hpp"
#include <iostream>
#include <chrono>
#include <boost/program_options.hpp>
#include <boost/lexical_cast.hpp>
using std::string;
using std::vector;
using std::cout;
using std::endl;
namespace po = boost::program_options;
using namespace std::chrono;
using namespace sam;
int main(int argc, char** argv) {
  int numNodes;
  int nodeId;
  string prefix;
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "help message")
    ("numNodes", po::value<int>(&numNodes)->default_value(1),
    "The number of nodes involved in the computation")
    ("nodeId", po::value<int>(&nodeId)->default_value(0),
    "The node id of this node.");
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);
  if (vm.count("help")) {
    cout << desc << endl;
    return 1;
  }
  vector<string> hostnames(numNodes);
  vector<int> ports(numNodes);
  int startingPort = 10000;
  if (numNodes == 1) {
    hostnames[0] = "127.0.0.1";
    ports[0] = startingPort;
  } else { 
    for (int i = 0; i < numNodes; i++) {
      hostnames[i] = prefix + boost::lexical_cast<string>(i);
      ports[i] = (startingPort + i);
    }
  }
  FeatureMap featureMap(10000);
  int queueLength = 10000;
  int hwm = 10000;
  ZeroMQPushPull consumer(queueLength,
                           numNodes,
                           nodeId,
                           hostnames,
                           ports,
                           hwm);
  string ip = "localhost";
  int port = 9999;
  ReadSocket receiver(ip,port);
  receiver.registerConsumer(&consumer);
  auto top2 = new TopK<size_t, Netflow,DestPort,DestIp>(10000,1000,2,nodeId, featureMap, "top2");
  consumer.registerConsumer(top2);
  if (!receiver.connect()) {
    cout << "Couldn't connected to " << ip
         << ":" << port << endl;
    return -1;
  }
  milliseconds ms1 = duration_cast<milliseconds>(
                       system_clock::now().time_since_epoch());
  receiver.receive();
  milliseconds ms2 = duration_cast<milliseconds>(
                       system_clock::now().time_since_epoch());
  cout << "Seconds "
    << static_cast<double>(ms2.count() - ms1.count()) / 1000 << endl;

}
