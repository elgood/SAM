
#include <zmq.hpp>
#include <boost/program_options.hpp>
#include <thread>
#include <vector>
#include <string>
#include <random>
#include "Util.hpp"


namespace po = boost::program_options;
using namespace sam;

int main(int argc, char** argv) {


  /// The zmq context
  zmq::context_t context = zmq::context_t(1);
  size_t receivedMessages = 0;

  /// A vector of all the push sockets
  std::vector<std::shared_ptr<zmq::socket_t> > pushers;

  /// Parameters
  size_t numNodes; ///> The number of nodes in the cluster
  size_t nodeId; ///> The node id of this node
  size_t hwm; ///> The high-water mark (Zeromq parameter)
  size_t messageSize; ///> Size of the body for each message
  size_t numMessages; ///> Number of messages to send
  size_t startingPort; ///> The starting port number for push/pull sockets
  std::string prefix; ///> The prefix to the nodes
  size_t numPushThreads; ///> Number of push threads
  size_t numPullThreads; ///> Number of pull threads
  size_t numPushSockets; ///> Number of push sockets

  po::options_description desc("Benchmark to see what the throughput is "
    "for ZeroMQ");
  desc.add_options()
    ("help", "help message")
    ("numNodes", po::value<size_t>(&numNodes)->default_value(1), 
      "The number of nodes involved in the computation (default: 1).")
    ("nodeId", po::value<size_t>(&nodeId)->default_value(0), 
      "The node id of this node (default: 0).")
    ("hwm", po::value<size_t>(&hwm)->default_value(10000), 
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
    ("numPushThreads", po::value<size_t>(&numPushThreads)->default_value(1),
      "The number of threads that are pushing data")
    ("numPullThreads", po::value<size_t>(&numPullThreads)->default_value(1),
      "The number of threads that are pulling data")
    ("numPushSockets", po::value<size_t>(&numPushSockets)->default_value(1),
      "The number of sockets that are pushing data")
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

  // All the hosts in the cluster.  The names are created with a 
  // concatenation of prefix with integer id of the node.
  std::vector<std::string> hostnames(numNodes); 

  if (numNodes == 1) { // Case when we are operating on one node
    hostnames[0] = "127.0.0.1";
  } else {
    for (int i = 0; i < numNodes; i++) {
      // Assumes all the host names can be composed by adding prefix with
      // [0,numNodes).
      hostnames[i] = prefix + boost::lexical_cast<std::string>(i);
    }
  }

  size_t totalNumPushSockets = (numNodes - 1) * numPushSockets; 

  for (int i = 0; i < totalNumPushSockets; i++) 
  {
    /////////// Adding push sockets //////////////
    auto pusher = std::shared_ptr<zmq::socket_t>(
                    new zmq::socket_t(context, ZMQ_PUSH));

    std::string hostname;
    std::string url = "tcp://*:";
    url = url + boost::lexical_cast<std::string>(startingPort + i);

    // The function complains if you use std::size_t, so be sure to use the
    // uint32_t class member for hwm.
    pusher->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm)); 
    DEBUG_PRINT("Node %lu binding to %s\n", nodeId, url.c_str());
    pusher->bind(url);
    pushers[i] = pusher;
  }



  /**
   * This is the function executed by the pull thread.  The pull
   * thread is responsible for polling all the pull sockets and
   * receiving data.
   */
  auto pullFunction = [numNodes, nodeId, hostnames, &receivedMessages,
    &context, hwm, startingPort, numPushSockets, numPullThreads]
    (size_t threadId) 
  {

    size_t beg = get_begin_index(numPushSockets * (numNodes - 1), threadId, 
                                 numPullThreads);
    size_t end = get_begin_index(numPushSockets * (numNodes - 1), threadId, 
                                 numPullThreads);

    size_t numVisiblePushSockets = end - beg;
    // All sockets passed to zmq_poll() function must belong to the same
    // thread calling zmq_poll().  Below we create the poll items and the
    // pull sockets.

    zmq::pollitem_t pollItems[numVisiblePushSockets];
    std::vector<zmq::socket_t*> sockets;

    // When a node sends a terminate flag, the corresponding entry is 
    // turned to true.  When all flags are true, the thread terminates.
    bool terminate[numVisiblePushSockets];

    int numAdded = 0;
    for(int i = beg; i < end; i++) {
      // Creating the zmq pull socket.
      std::string hostname = getHostnameForPull(i, nodeId, numPushSockets, 
                                                numNodes, hostnames);
      size_t port = getPortForPull(i, nodeId, numPushSockets,
                                   numNodes, startingPort);
      zmq::socket_t* socket = new zmq::socket_t(context, ZMQ_PULL);
      std::string url = "";
      url = "tcp://" + hostname + ":";
      url = url + boost::lexical_cast<std::string>(port);
      
      socket->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));
      DEBUG_PRINT("Node %lu connecting to %s\n", nodeId, url.c_str());
      socket->connect(url);
      sockets.push_back(socket);

      pollItems[numAdded].socket = *socket;
      pollItems[numAdded].events = ZMQ_POLLIN;
      terminate[numAdded] = false;
      numAdded++;
    }

    // Now we get the data from all the pull sockets through the zmq
    // poll mechanism.

    bool stop = false;
  
    while (!stop) {
      zmq::message_t message;
      int rValue = zmq::poll(pollItems, numNodes -1, 1);
      int numStop = 0;
      for (size_t i = 0; i < numNodes -1; i++) {
        if (pollItems[i].revents & ZMQ_POLLIN) {

          sockets[i]->recv(&message);
          if (isTerminateMessage(message)) {
            DEBUG_PRINT("Node %lu pullThread received terminate "
              "from %lu\n", nodeId, i);
            terminate[i] = true;
          } else if (message.size() > 0) {
            receivedMessages++;
            DEBUG_PRINT("Node %lu pullThread received tuple "
              "%s\n", nodeId, sam::toString(tuple).c_str());
            
          } else {
            DEBUG_PRINT("Node %lu pullThread received mystery"
              " message %s\n", getStringFromZmqMessage(message).c_str());
          }
        }
        if (terminate[i]) numStop++; 
      }
      if (numStop == numVisiblePushSockets) stop = true;
    }

    for (auto socket : sockets) {
      delete socket;
    }
    DEBUG_PRINT("Node %lu exiting pullThread\n", nodeId);
  };

  std::vector<std::thread> pullThreads;
  for (size_t i = 0; i < numPullThreads; i++) {
    pullThreads.push_back(std::thread(pullFunction, i)); 
  }

  // Make a message of the specified size
  std::string message;
  for(size_t i = 0; i < messageSize; i++) {
    message = message + "a";
  }
  
  std::vector<std::thread> pushThreads;
  

  for(size_t i = 0; i < numPushThreads; i++) {

    pushThreads.push_back(std::thread( 
     [nodeId, i, totalNumPushSockets, numMessages, &pushers,
      message]() 
    { 
      
      auto seed = nodeId * i *
        std::chrono::high_resolution_clock::now().time_since_epoch().count();
      std::mt19937 mt_rand(seed);
      std::uniform_int_distribution<size_t> dist(0, totalNumPushSockets - 1);
      for(size_t i = 0; i < numMessages; i++) {
        size_t node1 = dist(mt_rand);
        pushers[node1]->send(fillZmqMessage(message));
      }
    }));
  }

  for(size_t i = 0; i < numPushThreads; i++) {
    if (i != nodeId) {
      pushThreads[i].join();
    }
  }

  for(size_t i = 0; i < totalNumPushSockets; i++) {
    zmq::message_t message = terminateZmqMessage();
    pushers[i]->send(message);
  }

  for (size_t i = 0; i < numPullThreads; i++) {
    pullThreads[i].join();
  }
}
