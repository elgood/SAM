#include <string>
#include <vector>
#include <stdlib.h>
#include <iostream>
#include <chrono>
#include <boost/program_options.hpp>
#include <sam/sam.hpp>
using std::string;
using std::vector;
using std::cout;
using std::endl;
namespace po = boost::program_options;
using namespace std::chrono;
using namespace sam;
typedef TupleStringHashFunction<VastNetflow,SourceIp> Hash0;
typedef TupleStringHashFunction<VastNetflow,DestIp> Hash1;
typedef ZeroMQPushPull<VastNetflow, VastNetflowTuplizer, Hash0, Hash1> PartitionType;

}

int main(int argc, char** argv) {

  /******************* Variables ******************************/

  // There is assumed to be numNodes nodes within the cluster.
  // Each node has name prefix[0,numNodes).  Each of these
  // fields are specified with command line parameters
  int numNodes; //The total number of nodes in the cluster
  int nodeId; //Cardinal number of node within cluster
  string prefix; //Common prefix to all nodes in cluster

  // Ports for communications are allocated over a contiguous 
  // block of numbers, starting at this number
  size_t startingPort;

  // These two parameters relate to how the node receives data
  // Right now we can receive data in two ways, from a socket
  // and a CSV file.  These two are for reading data from a socket.
  // This is an area where SAM can be matured down the road.
  string ncIp; // The ip to read the nc data from.
  size_t ncPort; // The port to read the nc data from.

  // We use ZeroMQ Push/Pull sockets to communicate between
  // nodes.  We found that having multiple sockets to communicate
  // to a single node helped avoid contention.  numSockets
  // sets the total number of push and corresponding pull sockets
  // per node, so in total ((numNodes-1) * numSockets) sockets are.
  // defined per node.  It also helped to have multiple
  // threads to receive the data.  numPullThreads specifie
  // how many threads are used to cover all of the pull sockets
  // for a node.
  size_t numSockets;
  size_t numPullThreads;
  
  // A small set zeromq communications take forever.  The timeout
  // sets a hard limit on how long for push sockets to wait.
  // The value is in milliseconds
  size_t timeout;

  // The high water mark is a parameter for zeromq communications
  // and it controls how many messages can buffer before being
  // dropped.  This can be set with a SAL preamble statement:
  // HighWaterMark = <int>;
  // This sets a default value in the code that can be overriden
  // with the command-line interface of this generated code.
  size_t hwm;

  // There are two data structures to store the graph for edges
  // local to this node: a compressed sparse row (csr) and 
  // compressed sparse column (csc).  A hash structure is used 
  // to index the edges, either by the source vertex (csr) or the
  // target vertex (csc).  The array for the hash structure is
  // set once and not grown dynamically, though each slot can
  // grow arbitrarily large.  This should be set to a sufficiently
  // large value or much time will be spent linearly probing.
  size_t graphCapacity;

  // Both the SubgraphQueryResultMap (where intermediate subgraphs
  // are stored) and the EdgeRequestMap (where edge requests from
  // other nodes are stored) use a hash structure with an array.
  // The array does not grow dynmically, but each slot can get
  // arbitrarily big.  This should be set to a sufficiently
  // large value or much time will be spent linearly probing.
  // In the future, may want to split these into two different
  // parameters.
  size_t tableCapacity;

  // FeatureMap is another hashtable that needs a capacity
  // specified.  FeatureMap holds all the generated features.
  // FeatureSubscriber is used to create features.  Right now
  // it also needs a capacity specified, but in the future
  // this can likely be removed.  featureCapacity covers both
  // cases.
  size_t featureCapacity;

  // For many of the vertex centric computations we buffer
  // the values and then execute a parallel for loop.
  // queueLength determines the length of the buffer.
  size_t queueLength;

  // inputfile and outputfile are used when we are creating
  // features.  The inputfile has tuples with labels.
  // We then run the pipeline over the inputfile, create features
  // and write the results to the outputfile.
  string inputfile;
  string outputfile;

  // N is the total number of elements in a sliding window.  The
  // sliding window is not based on time.  Time-based windows are
  // future work.  This is the default value (set through the 
  // commandline interface), but can be set for each operator.
  size_t N;

  // b is the basic window size in terms elements to process.
  // A basic window is subdivision of the sliding window used
  // by some operators.  This is the default value (set through the
  // commandline interface), but can be set for each operator.
  size_t b;

  // k is a parameter for topk, the number of top items to keep
  // track of.  This is the default value (set through the
  // commandline interface), but can be set for each operator.
  size_t k;

  /****************** Process commandline arguments ****************/

  po::options_description desc(
    "There are two basic modes supported right now: "
    "1) Running the pipeline against data coming from a socket.\n"
    "2) Running the pipeline against an input file and creating\n"
    " features.\n"
    "These of course should be expanded.  Right now the process\n"
    "allows for creating features on existing data to train\n"
    "offline.  However, using the trained model on live data\n"
    "is currently not supported\n"
    "Allowed options:");
  desc.add_options()
    ("help", "help message")
    ("numNodes",
      po::value<int>(&numNodes)->default_value(1),
      "The number of nodes involved in the computation")
    ("nodeId",
       po::value<int>(&nodeId)->default_value(0),
       "The node id of this node.")
    ("prefix",
      po::value<string>(&prefix)->default_value("node"),
      "The prefix common to all nodes.  The hostnames are formed"
      "by concatenating the prefix with the node id (in"
      "[0, numNodes-1]).  However, when there is only one node"
      "we use localhost.")
    ("startingPort",
       po::value<size_t>(&startingPort)->default_value(10000),
       "The starting port for the zeromq communications")
    ("ncIp",
      po::value<string>(&ncIp)->default_value("localhost"),
      "The ip to receive the data from nc (netcat).  Right now"
      " each node receives data from a socket connection.  "
      "This can be improved in the future.")
    ("ncPort",
      po::value<size_t>(&ncPort)->default_value(9999),
      "The port to receive the data from nc")
    ("numPullThreads",
      po::value<size_t>(&numPullThreads)->default_value(1),
      "Number of pull threads (default 1)")
    ("numSockets",
      po::value<size_t>(&numSockets)->default_value(1),
      "Number of push sockets a node creates to talk to another"
      "node (default 1)")
    ("timeout",
      po::value<size_t>(&timeout)->default_value(1000),
      "How long in milliseconds to wait before giving up on push"
      "socket send")
    ("graphCapacity",
      po::value<size_t>(&graphCapacity)->default_value(100000),
      "How many slots in the csr and csc (default: 100000).")
    ("tableCapacity",
      po::value<size_t>(&tableCapacity)->default_value(1000),
      "How many slots in SubgraphQueryResultMap and EdgeRequestMap"
      " (default 1000).")
    ("featureCapacity",
      po::value<size_t>(&featureCapacity)->default_value(10000),
      "The capacity of the FeatureMap and FeatureSubcriber")
    ("hwm",
      po::value<size_t>(&hwm)->default_value(10000),
      "The high water mark (how many items can queue up before we"
      "start dropping")
    ("queueLength",
      po::value<size_t>(&queueLength)->default_value(1000),
      "We fill a queue before sending things in parallel to all"
      " consumers.  This controls the size of that queue.")
    ("create_features",
      "If specified, will read tuples from --inputfile and"
      " output to --outputfile a csv feature file")
    ("inputfile",
      po::value<string>(&inputfile),
      "If --create_features is specified, the input should be"
      " a file with labeled tuples.")
    ("outputfile",
      po::value<string>(&outputfile),
      "If --create_features is specified, the produced file will"
      " be a csv file of features.")
    ("N",
      po::value<std::size_t>(&N)->default_value(10000),
      "The total number of elements in a sliding window")
    ("b",
      po::value<std::size_t>(&b)->default_value(1000),
      "The number of elements per block (active or dynamic window)")
    ("k",
      po::value<std::size_t>(&k)->default_value(2),
      "The k in topk")
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);
  if (vm.count("help")) {
    cout << desc << endl;
    return 1;
  }

  vector<string> hostnames(numNodes);

  // When we are operating on one nodes, we set the hostname to be
  // local host or 127.0.0.1
  if (numNodes == 1) { 
    hostnames[0] = "127.0.0.1";
  } else { 
    for (int i = 0; i < numNodes; i++) {
      // Assumes all the host names can be composed by adding prefix
      // with [0,numNodes).
      hostnames[i] = prefix + boost::lexical_cast<string>(i);
    }
  }

  // The FeatureMap keeps track of all generated features produced
  // by the specified pipeline.
  auto featureMap = std::make_shared<FeatureMap>(featureCapacity);

  /***************** Creating Features ***********************/

  if(vm.count("create_features"))

    if (inputfile == "") {
       std::cout << "--create_features was specified but no input"
                 << "file was listed with --inputfile." << std::endl;
       return -1;
    }
    if (outputfile == "") {
      std::cout << "--create_features was specified but no output"
                << "file was listed with --outputfile." << std::endl;
      return -1;
    }

    auto receiver = std::make_shared<ReadCSV>(inputfile);
    auto subscriber = std::make_shared<FeatureSubscriber>(outputfile,
                                                      featureCapacity);

    createPipeline(receiver, featureMap, subscriber, NULL,
              queueLength,
              numNodes,
              nodeId,
              hostnames,
              timeout, hwm,
              N, b, k);
    subscriber->init();
    if (!receiver->connect()) {
      std::cout << "Problems opening file " << inputfile << std::endl;
      return -1;
    }
    milliseconds ms1 = duration_cast<milliseconds>(
    system_clock::now().time_since_epoch()
      );
    receiver->receive();
    milliseconds ms2 = duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()
    );
    std::cout << "Seconds for Node" << nodeId << ": "
              << static_cast<double>(ms2.count() - ms1.count()) / 1000 << std::endl;
    std::cout << "Finished" << std::endl;
    return 0;
  }
  else
  {
    auto receiver = std::make_shared<ReadSocket>(ip, ncPort);
    receiver->registerConsumer(consumer);
    createPipeline(receiver, featureMap, NULL, consumer,
                  queueLength,
                  numNodes,
                  nodeId,
                  hostnames,
                  ports,
                  hwm,
                  N, b, k);
    if (!receiver->connect()) {
      std::cout << "Couldn't connected to " << ip << ":" << ncPort << std::endl;
      return -1;
    }
    milliseconds ms1 = duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()
    );
    receiver->receive();
    milliseconds ms2 = duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()
    );
    std::cout << "Seconds for Node" << nodeId << ": "
      << static_cast<double>(ms2.count() - ms1.count()) / 1000 << std::endl;
  }
  string ip = "localhost";
  int port = 9999;
  ReadSocket receiver(ip, port);
  receiver.registerConsumer(&partitioner);

}