
//#define DEBUG

#include <boost/program_options.hpp>
#include <sam/ZeroMQUtil.hpp>

/**
 * Benchmarking the PullPull class.
 */

namespace po = boost::program_options;
using namespace sam;

int main(int argc, char** argv) {

  auto totalTimingBegin = std::chrono::high_resolution_clock::now();

  /// Parameters
  size_t numNodes; ///> The number of nodes in the cluster
  size_t nodeId; ///> The node id of this node
  uint32_t hwm; ///> The high-water mark (Zeromq parameter)
  size_t messageSize; ///> Size of the body for each message
  size_t numMessages; ///> Number of messages to send
  size_t startingPort; ///> The starting port number for push/pull sockets
  std::string prefix; ///> The prefix to the nodes
  size_t numSendThreads; ///> Number of threads sending data
  size_t numPullThreads; ///> Number of pull threads
  size_t numPushSockets; ///> Number of push sockets per node
  bool useNetflowString = false;
  int timeout; ///> Timeout in milliseconds for zmq::send() calls

  /// An example netflow string.  This is used as the message 
  /// when --netflowString is selected.
  std::string netflowString = "1,1,1365582756.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18,"
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";

  po::options_description desc("Benchmark to see what the throughput is "
    "for ZeroMQ");
  desc.add_options()
    ("help", "help message")
    ("numNodes", po::value<size_t>(&numNodes)->default_value(1), 
      "The number of nodes involved in the computation (default: 1).")
    ("nodeId", po::value<size_t>(&nodeId)->default_value(0), 
      "The node id of this node (default: 0).")
    ("hwm", po::value<uint32_t>(&hwm)->default_value(10000), 
      "The high water mark (how many items can queue up before we start "
      "dropping)")
    ("messageSize", po::value<size_t>(&messageSize)->default_value(1),
      "The size of the message body")
    ("startingPort", po::value<size_t>(&startingPort)->default_value(
      10000), "The starting port for the zeromq communications")
    ("prefix", po::value<std::string>(&prefix)->default_value("node"), 
      "The prefix common to all nodes (default is node, but localhost is"
      "used when there is only one node).")
    ("numMessages", po::value<size_t>(&numMessages)->default_value(
      10000), "The number of messages to send")
    ("numSendThreads", po::value<size_t>(&numSendThreads)->default_value(1),
      "The number of threads that are pushing data")
    ("numPullThreads", po::value<size_t>(&numPullThreads)->default_value(1),
      "The number of threads that are pulling data")
    ("numPushSockets", po::value<size_t>(&numPushSockets)->default_value(1),
      "Number of push sockets created to talk to each node.")
    ("netflowString", po::bool_switch(&useNetflowString),
      "If specified, uses an example netflow string for the message")
    ("timeout", po::value<int>(&timeout)->default_value(-1),
      "Send Timeout in milliseconds.  If -1, then block until complete."
      "  (Default -1)")
  ;

  // Parse the command line variables
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);


  // Print out the help and exit if --help was specified.
  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 1;
  }

  // Make a message of the specified size
  std::string message;

  if (!useNetflowString) {
    for(size_t i = 0; i < messageSize; i++) {
      message = message + "a";
    }
  } else {
    message = netflowString;
  }


  std::vector<std::string> hostnames;
  hostnames.resize(numNodes);

  if (numNodes == 1) { // Case when we are operating on one node
    hostnames[0] = "127.0.0.1";
  } else {
    for (size_t i = 0; i < numNodes; i++) {
      //printf("i %lu\n", i);
      // Assumes all the host names can be composed by adding prefix with
      // [0,numNodes).
      hostnames[i] = prefix + boost::lexical_cast<std::string>(i);
    }
  }

  // PushPull needs a list of callback functions.  Here we create a list
  // and add a function that doesn't do anything.
  auto noopFunction = [](std::string const& str) {
  };
  typedef PushPull::FunctionType FunctionType;
  std::vector<FunctionType> functions;
  functions.push_back(noopFunction);

  PushPull* pushPull = new PushPull(numNodes, nodeId, numPushSockets, 
                                    numPullThreads, hostnames, hwm,
                                    functions, startingPort, timeout);

  


  // Thread to create data and send it out.  
  auto function = [message, numNodes, nodeId, numPushSockets, numPullThreads,
    hostnames, hwm, pushPull, numMessages]()
  {
    std::random_device rd;
    auto myRand = std::mt19937(rd());
    auto dist = std::uniform_int_distribution<size_t>(0, numNodes-1);

    for( size_t i = 0; i < numMessages; i++)
    {
      bool found = false;
      size_t node;

      // iterate until we find a destination node that is not the current node
      while (!found) {
        node = dist(myRand);

        // Don't want to send data to ourselves.
        if (node != nodeId) {
          found = true;
        }
      }
      //printf("Node %lu sending message %s\n", nodeId, message.c_str());
      pushPull->send(message, node);
    }
  };

  std::vector<std::thread> threads;
  threads.resize(numSendThreads);

  for (size_t i = 0; i < numSendThreads; i++)
  {
    threads[i] = std::thread(function);
  }


  for (size_t i = 0; i < numSendThreads; i++)
  {
    threads[i].join();
  }

  pushPull->terminate();

  auto totalTimingEnd = std::chrono::high_resolution_clock::now();
  auto totalTimingDiff = std::chrono::duration_cast<
    std::chrono::duration<double>>(totalTimingEnd - totalTimingBegin);
  double totalTime = totalTimingDiff.count();
  size_t totalMessages = numMessages * numSendThreads;
  size_t totalReceived = pushPull->getTotalMessagesReceived();
  printf("Node %lu total time: %f \n", nodeId, totalTime);
  printf("Node %lu messages/second: %f\n", nodeId, totalMessages / totalTime);
  printf("Node %lu total messages received: %lu \n", nodeId, totalReceived);


}

