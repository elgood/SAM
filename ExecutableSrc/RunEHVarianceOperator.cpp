

//#define DEBUG 1

#include <sam/ReadSocket.hpp>
#include <sam/ZeroMQPushPull.hpp>
#include <sam/ExponentialHistogramVariance.hpp>

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
  std::string ip; ///> The ip to read the nc data from.
  std::size_t ncPort; ///> The port to read the nc data from.
  std::size_t numNodes; ///> The number of nodes in the cluster
  std::size_t nodeId; ///> The node id of this node
  std::string prefix; ///> The prefix to the nodes
  std::size_t startingPort; ///> The starting port number for push/pull sockets
  std::size_t hwm; ///> The high-water mark (Zeromq parameter)
  std::size_t queueLength; ///> The length of the input queue
  std::size_t N; ///> The total number of elements in a sliding window
  std::size_t k; ///> Determines size of buckets 
  std::size_t nop; //not used
  std::size_t capacity = 10000;////> Capacity of FeatureMap and subscriber

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "help message")
    ("ip", po::value<std::string>(&ip)->default_value("localhost"),
      "The ip to receive the data from nc")
    ("ncPort", po::value<std::size_t>(&ncPort)->default_value(9999),
      "The port to receive the data from nc")
    ("numNodes", po::value<std::size_t>(&numNodes)->default_value(1),
      "The number of nodes involved in the computation")
    ("nodeId", po::value<std::size_t>(&nodeId)->default_value(0),
      "The node id of this node")
    ("prefix", po::value<std::string>(&prefix)->default_value("node"),
      "The prefix common to all nodes")
    ("startingPort", po::value<std::size_t>(&startingPort)->default_value(
      10000), "The starting port for the zeromq communications")
    ("hwm", po::value<std::size_t>(&hwm)->default_value(10000),
      "The high water mark (how many items can queue up before we start "
      "dropping)")
    ("queueLength", po::value<std::size_t>(&queueLength)->default_value(10000),
      "We fill a queue before sending things in parallel to all consumers."
      "  This controls the size of that queue.")
    ("nop", po::value<std::size_t>(&nop)->default_value(1),
      "The number of simultaneous operators")
    ("N", po::value<std::size_t>(&N)->default_value(10000),
      "The total number of elements in a sliding window")
    ("k", po::value<std::size_t>(&k)->default_value(2),
      "Determines size of buckets.")
    ("capacity", po::value<std::size_t>(&capacity)->default_value(10000),
      "The capacity of the FeatureMap and FeatureSubcriber")
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

  ReadSocket receiver(ip, ncPort);

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

  // TODO Make commandline parameter
  size_t timeout = 1000;

  auto consumer = std::make_shared<PartitionType>(queueLength,
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
    std::string identifier = "ehvar" + boost::lexical_cast<std::string>(i);
    auto op = std::make_shared<ExponentialHistogramVariance<size_t, Netflow,
                                               DestPort, 
                                               DestIp>>(N, k,  
                                                nodeId,featureMap, identifier);
                                                  
                                                  
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
