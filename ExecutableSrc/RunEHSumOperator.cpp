

//#define DEBUG 1

#include <sam/ReadSocket.hpp>
#include <sam/ZeroMQPushPull.hpp>
#include <sam/ExponentialHistogramSum.hpp>

#include <boost/program_options.hpp>

namespace po = boost::program_options;

using namespace sam;
using namespace std::chrono;

typedef TupleStringHashFunction<Netflow, SourceIp> SourceHash;
typedef TupleStringHashFunction<Netflow, DestIp> TargetHash;
typedef ZeroMQPushPull<Netflow, NetflowTuplizer, SourceHash, TargetHash>
        
        PartitionType;

int main(int argc, char** argv)
{

  // The ip to read the nc data from.
  std::string ip;

  // The port to read the nc data from.
  int ncPort;

  // The number of nodes in the cluster
  int numNodes;

  // The node id of this node
  int nodeId;

  // The prefix to the nodes
  std::string prefix;

  // The starting port number
  int startingPort;

  // The high-water mark
  long hwm;

  // The length of the input queue
  int queueLength;

  // How many simultaneous operators
  int nop;

  // The total number of elements in a sliding window
  int N;

  // Determines size of buckets 
  int k;

  time_t timestamp_sec1, timestamp_sec2;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "help message")
    ("ip", po::value<std::string>(&ip)->default_value("localhost"),
      "The ip to receive the data from nc")
    ("ncPort", po::value<int>(&ncPort)->default_value(9999),
      "The port to receive the data from nc")
    ("numNodes", po::value<int>(&numNodes)->default_value(1),
      "The number of nodes involved in the computation")
    ("nodeId", po::value<int>(&nodeId)->default_value(0),
      "The node id of this node")
    ("prefix", po::value<std::string>(&prefix)->default_value("node"),
      "The prefix common to all nodes")
    ("startingPort", po::value<int>(&startingPort)->default_value(10000),
      "The starting port for the zeromq communications")
    ("hwm", po::value<long>(&hwm)->default_value(10000),
      "The high water mark (how many items can queue up before we start "
      "dropping)")
    ("queueLength", po::value<int>(&queueLength)->default_value(10000),
      "We fill a queue before sending things in parallel to all consumers."
      "  This controls the size of that queue.")
    ("nop", po::value<int>(&nop)->default_value(1),
      "The number of simultaneous operators")
    ("N", po::value<int>(&N)->default_value(10000),
      "The total number of elements in a sliding window")
    ("k", po::value<int>(&k)->default_value(2),
      "Determines size of buckets.")
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

  ReadSocket receiver(ip, ncPort);
#ifdef DEBUG
  cout << "DEBUG: main created receiver " << endl;
#endif

  std::vector<std::string> hostnames(numNodes);
  std::vector<std::size_t> ports(numNodes);

  if (numNodes == 1) {
    hostnames[0] = "127.0.0.1";
    ports[0] = startingPort;
  } else {
    for (int i = 0; i < numNodes; i++) {
      hostnames[i] = prefix + boost::lexical_cast<std::string>(i);
      ports[i] = (startingPort + i);
    }
  }

  size_t timeout = 1000;

  auto consumer = std::make_shared<PartitionType>( queueLength,
                               numNodes,
                               nodeId,
                               hostnames,
                               startingPort, timeout, false,
                               hwm);

#ifdef DEBUG
  cout << "DEBUG: main created consumer " << endl;
#endif

  receiver.registerConsumer(consumer);

  auto featureMap = std::make_shared<FeatureMap>();
  std::vector<size_t> keyFields;
  keyFields.push_back(6);
  int valueField = 8;
  for (int i = 0; i < nop; i++) {
    std::string identifier = "ehsum" + boost::lexical_cast<std::string>(i);
    auto op = std::make_shared<ExponentialHistogramSum<size_t, Netflow, 
                                      DestPort, DestIp>>(
                                         N, k, nodeId, featureMap, identifier);
                                                
    consumer->registerConsumer(op);
  }



  if (!receiver.connect()) {
    std::cout << "Couldn't connected to " << ip << ":" << ncPort << std::endl;
    return -1;
  }

  milliseconds ms1 = duration_cast<milliseconds>(
    system_clock::now().time_since_epoch()
  );
  receiver.receive();
  milliseconds ms2 = duration_cast<milliseconds>(
    system_clock::now().time_since_epoch()
  );
  std::cout << "Seconds "
    << static_cast<double>(ms2.count() - ms1.count()) / 1000 << std::endl;




}
