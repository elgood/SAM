
//#define DEBUG

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
 
  auto totalTimingBegin = std::chrono::high_resolution_clock::now();

  /// The zmq context
  zmq::context_t* context = new zmq::context_t(1);

  size_t receivedMessages = 0;

  /// A vector of all the push sockets
  std::vector<std::shared_ptr<zmq::socket_t> > pushers;

  /// Parameters
  size_t numNodes; ///> The number of nodes in the cluster
  size_t nodeId; ///> The node id of this node
  uint32_t hwm; ///> The high-water mark (Zeromq parameter)
  size_t messageSize; ///> Size of the body for each message
  size_t numMessages; ///> Number of messages to send
  size_t startingPort; ///> The starting port number for push/pull sockets
  std::string prefix; ///> The prefix to the nodes
  //size_t numPushThreads; ///> Number of push threads
  size_t numPullThreads; ///> Number of pull threads
  bool useNetflowString = false;

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
    //("numPushThreads", po::value<size_t>(&numPushThreads)->default_value(1),
    //  "The number of threads that are pushing data")
    ("numPullThreads", po::value<size_t>(&numPullThreads)->default_value(1),
      "The number of threads that are pulling data")
    ("netflowString", po::bool_switch(&useNetflowString),
      "If specified, uses an example netflow string for the message")
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

  //size_t numPushSockets = numPushThreads;
  //size_t totalNumPushSockets = (numNodes - 1) * numPushSockets; 
  size_t totalNumPushSockets = (numNodes - 1); 
  DEBUG_PRINT("Total number of push sockets %lu\n", totalNumPushSockets);

  /*for (size_t i = 0; i < totalNumPushSockets; i++) 
  {

    printf("i %lu\n", i);
    /////////// Adding push sockets //////////////
    auto pusher = std::shared_ptr<zmq::socket_t>(
                   new zmq::socket_t(*context, ZMQ_PUSH));
    printf("blah5 %lu\n", nodeId);
    std::string hostname = prefix + 
      boost::lexical_cast<std::string>(nodeId);
    std::string ip = getIpString(hostname);
    std::string url = "tcp://" + ip + ":";
    url = url + boost::lexical_cast<std::string>(startingPort + i);
    printf("Node %lu binding to %s\n", nodeId, url.c_str());

    // The function complains if you use std::size_t, so be sure to use the
    // uint32_t class member for hwm.
    pusher->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm)); 
    try {
      pusher->bind(url);
    } catch (std::exception e) {
      std::string message = "Node " +
        boost::lexical_cast<std::string>(nodeId) +
        " couldn't bind to url " + url + ": " + e.what();
      throw std::runtime_error(message);
    }
    printf("blah5.5 %lu\n", nodeId);
    pushers.push_back(pusher);


  }*/

  std::atomic<size_t> messagesReceived(0);
  std::mutex zmqLock;

  /**
   * This is the function executed by the pull thread.  The pull
   * thread is responsible for polling all the pull sockets and
   * receiving data.
   */
  auto pullFunction = [numNodes, nodeId, hostnames, &receivedMessages,
    &context, hwm, startingPort, numPullThreads, &messagesReceived, &zmqLock]
    (size_t threadId) 
  {
    DEBUG_PRINT("Node %lu in pullFunction numNodes %lu threadId %lu"
      " numPullThreads %lu\n", nodeId, numNodes, threadId, numPullThreads);
    size_t beg = get_begin_index((numNodes - 1), threadId, 
                                 numPullThreads);
    size_t end = get_end_index((numNodes - 1), threadId, 
                                 numPullThreads);

    zmqLock.lock();
    size_t numVisiblePushSockets = end - beg;
    // All sockets passed to zmq_poll() function must belong to the same
    // thread calling zmq_poll().  Below we create the poll items and the
    // pull sockets.

    zmq::pollitem_t pollItems[numVisiblePushSockets];
    std::vector<zmq::socket_t*> sockets;

    // When a node sends a terminate flag, the corresponding entry is 
    // turned to true.  When all flags are true, the thread terminates.
    bool terminate[numVisiblePushSockets];
    DEBUG_PRINT("numVisiblePushSockets %lu\n", numVisiblePushSockets);

    DEBUG_PRINT("beg %lu end %lu\n", beg, end);
    int numAdded = 0;
    for(size_t i = beg; i < end; i++) {
      DEBUG_PRINT("Node %lu i %lu beg %lu end %lu\n", nodeId, i, beg, end);
      // Creating the zmq pull socket.
      std::string hostname = getHostnameForPull(i, nodeId, 1, 
                                                numNodes, hostnames);
      size_t port = getPortForPull(i, nodeId, 1,
                                   numNodes, startingPort);
      zmq::socket_t* socket = new zmq::socket_t(*context, ZMQ_PULL);
      std::string url = "";
      std::string ip = getIpString(hostname);
      
      url = "tcp://" + ip + ":";
      url = url + boost::lexical_cast<std::string>(port);
      
      socket->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));
      DEBUG_PRINT("Node %lu connecting to %s\n", nodeId, url.c_str());
      try {
        socket->connect(url);
      } catch (std::exception e) {
        std::string message = "Node " +
          boost::lexical_cast<std::string>(nodeId) +
          " couldn't connect to url " + url + ": " + e.what();
        throw std::runtime_error(message);
      }
      sockets.push_back(socket);

      pollItems[numAdded].socket = *socket;
      pollItems[numAdded].events = ZMQ_POLLIN;
      terminate[numAdded] = false;
      numAdded++;
    }

    zmqLock.unlock();

    // Now we get the data from all the pull sockets through the zmq
    // poll mechanism.

    bool stop = false;
  
    while (!stop) {
      zmq::message_t message;
      int rValue = zmq::poll(pollItems, numVisiblePushSockets, 1);
      int numStop = 0;
      for (size_t i = 0; i < numVisiblePushSockets; i++) {
        if (pollItems[i].revents & ZMQ_POLLIN) {

          sockets[i]->recv(&message);
          if (isTerminateMessage(message)) {
            DEBUG_PRINT("Node %lu pullThread received terminate "
              "from %lu\n", nodeId, i);
            terminate[i] = true;
          } else if (message.size() > 0) {
            receivedMessages++;
            DEBUG_PRINT("Node %lu pullThread received message of size %lu "
              "from %lu\n", nodeId, message.size(), i);
            
          } else {
            DEBUG_PRINT("Node %lu pullThread received mystery"
              " message %s\n", nodeId, 
              getStringFromZmqMessage(message).c_str());
          }
        }
        if (terminate[i]) numStop++; 
      }
      if (numStop == numVisiblePushSockets) stop = true;
    }

    for (auto socket : sockets) {
      delete socket;
    }

    messagesReceived.fetch_add(receivedMessages);

    DEBUG_PRINT("Node %lu exiting pullThread\n", nodeId);
  };

  std::vector<std::thread> pullThreads;
  for (size_t i = 0; i < numPullThreads; i++) {
    pullThreads.push_back(std::thread(pullFunction, i)); 
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
  
  std::vector<std::thread> pushThreads;
  pushThreads.resize(totalNumPushSockets);


  auto timingBegin = std::chrono::high_resolution_clock::now();  
  DEBUG_PRINT("node %lu sending %lu messages\n", nodeId, numMessages);
  for(size_t threadId = 0; threadId < totalNumPushSockets; threadId++) {

    pushThreads[threadId] = std::thread( 
     [nodeId, threadId, totalNumPushSockets, numMessages, &pushers,
      message, prefix, context, startingPort, hwm, &zmqLock]() 
    { 

      zmqLock.lock();
      auto pusher = std::shared_ptr<zmq::socket_t>(
                     new zmq::socket_t(*context, ZMQ_PUSH));
      std::string hostname = prefix + 
        boost::lexical_cast<std::string>(nodeId);
      std::string ip = getIpString(hostname);
      std::string url = "tcp://" + ip + ":";
      url = url + boost::lexical_cast<std::string>(startingPort + threadId);
      DEBUG_PRINT("Node %lu binding to %s\n", nodeId, url.c_str());

      // The function complains if you use std::size_t, so be sure to use the
      // uint32_t class member for hwm.
      pusher->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm)); 

      bool success = false;
      size_t numTries = 0;
      size_t maxNumTries = 1;
      try {
        pusher->bind(url);
      } catch (std::exception e) {
        std::string message = "Node " +
          boost::lexical_cast<std::string>(nodeId) +
          " couldn't bind to url " + url + ": " + e.what();
        throw std::runtime_error(message);
      }
      zmqLock.unlock();


      
      //auto seed = nodeId * threadId *
      //  std::chrono::high_resolution_clock::now().time_since_epoch().count();
      //std::mt19937 mt_rand(seed);
      //std::uniform_int_distribution<size_t> dist(0, totalNumPushSockets - 1);
      for(size_t i = 0; i < numMessages; i++) {
        //size_t node1 = dist(mt_rand);
        DEBUG_PRINT("Node %lu thread id %lu sending message %lu to socket"
          " %lu\n", nodeId, threadId, i, threadId);
        pusher->send(fillZmqMessage(message));
      }

      DEBUG_PRINT("Node %lu thread %lu sending terminate message\n",
        nodeId, threadId);
      zmq::message_t message = terminateZmqMessage();
      pusher->send(message);
    });
  }

  for(size_t i = 0; i < totalNumPushSockets; i++) {
    pushThreads[i].join();
  }
  auto timingEnd = std::chrono::high_resolution_clock::now();

  for (size_t i = 0; i < numPullThreads; i++) {
    pullThreads[i].join();
  }
  auto totalTimingEnd = std::chrono::high_resolution_clock::now();
  auto pushTimingDiff = std::chrono::duration_cast<
    std::chrono::duration<double>>(timingEnd - timingBegin);
  auto totalTimingDiff = std::chrono::duration_cast<
    std::chrono::duration<double>>(totalTimingEnd - totalTimingBegin); 
  double sendMessageTime = pushTimingDiff.count();
  double totalTime = totalTimingDiff.count();
  size_t totalMessages = numMessages * totalNumPushSockets;
  printf("Node %lu Time to send messages: %f total time: %f "
    "messages received/expected %lu / %lu "
    "messages per second %f %f\n", nodeId, sendMessageTime, totalTime,
    totalMessages, messagesReceived.load(), 
    totalMessages / sendMessageTime, totalMessages/totalTime);

  delete context;      
}
