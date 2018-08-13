#ifndef SAM_PUSH_PULL_HPP
#define SAM_PUSH_PULL_HPP

#include "Util.hpp"
#include <random>

namespace sam {

class ZeroMQUtilException : public std::runtime_error {
public:
  ZeroMQUtilException(char const * message) : std::runtime_error(message) { } 
  ZeroMQUtilException(std::string message) : std::runtime_error(message) { } 
};



/**
 * Given the zmq message, extract the data as a string.
 * \param message The zmq to get the data from.
 * \return Returns the zmq data as a string.
 */
std::string getStringFromZmqMessage( zmq::message_t const& message )
{
  // Reinterpret the void pointer to be an unsigned char pointer.
  unsigned char const * dataptr = 
    reinterpret_cast<unsigned char const*>(message.data());

  // Creates a string with proper size so we can iterate over it and 
  // copy in the values from the message.
  std::string rString(message.size(), 'x');

  std::copy(dataptr, &dataptr[message.size()], rString.begin());
  return rString;
}

/**
 * Creates an empty zmq message.  We use this to indicate a terminate
 * message.
 */
zmq::message_t emptyZmqMessage() {
  std::string str = "";
  zmq::message_t message = fillZmqMessage(str);
  return message;
}

/**
 * Creates a zmq message that means terimate
 */
zmq::message_t terminateZmqMessage() {
  return emptyZmqMessage();
}

/**
 * Checks if the zmq message is a terminate message. A terminate message
 * is one with an empty string.
 */
bool isTerminateMessage(zmq::message_t& message)
{
  if (message.size() == 0) return true;
  return false;
}

/**
 * This gives the correct hostname for the ith pull socket.
 * The total number of pull sockets to create is (numNodes - 1) *
 * numPushSockets.  Each node creates numPushSockets sockets for each
 * node to pull from (e.g. a node has numPushSockets sockets to choose from
 * for each node except itself).
 * \param i The ith socket out of (numNodes - 1) * numPushSockets
 * \param nodeId The id of the node in the cluster
 * \param numPushSockets The number of push sockets a node creates for each
 *   other node to pull from.
 * \param numNodes The total number of nodes in the cluster.
 * \param hostnames The names of the hosts in the cluster.
 */
inline
std::string getHostnameForPull(size_t i,
                        size_t nodeId,
                        size_t numPushSockets, 
                        size_t numNodes, 
                        std::vector<std::string> const& hostnames)
{
  if (i >= (numNodes - 1) * numPushSockets) {
    std::string message = "sam::getHostname i " + 
      boost::lexical_cast<std::string>(i) + " >= (" +
      boost::lexical_cast<std::string>(numNodes) + " - 1) * " +
      boost::lexical_cast<std::string>(numPushSockets);
    throw ZeroMQUtilException(message);
  }

  // This conditional accounts for the fact that a node doesn't pull from
  // itself.
  size_t index;
  if ((i / numPushSockets) < nodeId) {
    index = i / numPushSockets;
  } else {
    index = i / numPushSockets + 1;
  }
  if (index >= hostnames.size()) {
    std::string message = "sam::getHostname index " +
      boost::lexical_cast<std::string>(index) + " >= hostname.size() " +
      boost::lexical_cast<std::string>(hostnames.size());
    throw ZeroMQUtilException(message);
  }
  return hostnames[index];
}

/**
 * Similar to getHostnames, this gives you the associated port.
 * \param i The ith socket out of (numNodes - 1) * numPushSockets
 * \param nodeId The id of the node in the cluster
 * \param numPushSockets The number of push sockets a node creates for each
 *   other node to pull from.
 * \param numNodes The total number of nodes in the cluster.
 * \param startingPort The start of the port range.
 */
inline
size_t getPortForPull(size_t i, 
                    size_t nodeId,
                    size_t numPushSockets,
                    size_t numNodes,
                    size_t startingPort)
{
  if (i >= (numNodes - 1) * numPushSockets) {
    std::string message = "sam::getPortForPull i " + 
      boost::lexical_cast<std::string>(i) + " >= (" +
      boost::lexical_cast<std::string>(numNodes) + " - 1) * " +
      boost::lexical_cast<std::string>(numPushSockets);
    throw ZeroMQUtilException(message);
  }

  // Find which node we are wanting to talk to.
  size_t targetNode = i / numPushSockets;
  if ((i / numPushSockets) >= nodeId) {
    targetNode++;
  }

  size_t port;
  
  if (targetNode > nodeId) {
    port = startingPort + nodeId * numPushSockets + i % numPushSockets;
  } else if (targetNode < nodeId) {
    port = startingPort + (nodeId - 1) * numPushSockets + i % numPushSockets;
  } else {
    // This shouldn't happen
    throw ZeroMQUtilException("sam::getPort targetNode == nodeId");
  }

  return port;
}


/**
 * Class that implements the push/pull communication paradigm amongst a 
 * set of nodes within a cluster.
 *
 * To set up, follow the pattern below:
 * 1) call the constructor
 *   a) You must provide a set of functions that will be called when messages
 *      arrive.  The input parameter of the functions is a string.
 * 2) Send data to the other nodes using send().
 *
 * No information is necessary about the type of data being sent.  The only
 * requirement is that it can be serialized as an std::string.  send()
 * accepts strings as input, and the pull threads creates strings from 
 * the data it gets and sends those strings to the callback functions.
 */
class PushPull
{
public:
  typedef std::function<void(std::string const&)> FunctionType;

private:
  size_t numNodes; ///> How many nodes in the cluster
  size_t nodeId; ///> The id of this node
  std::vector<std::string> hostnames; ///> The hostnames of all the nodes
  zmq::context_t context; ///> The zmq context
  size_t numPullThreads; ///> Number of pull threads
  std::mutex zmqLock; ///> Lock for some zmq stuff
  std::atomic<size_t> totalMessagesReceived; ///> Total messages pulled.
  std::atomic<size_t> totalMessagesSent; ///> Total messages pushed.
  std::atomic<size_t> totalMessagesFailed; ///> Total messages failed to send.
  std::vector<std::thread> pullThreads; ///> All the pull threads.
  uint32_t hwm; ///> The high-water mark
  size_t startingPort; ///> The starting port
  int timeout; ///> The timeout in ms for send() calls

  std::vector<std::shared_ptr<zmq::socket_t>> pushers;

  /// The push sockets are not thread safe.  PushPull::send() can be called
  /// by multiple threads, so we create mutexes to make sure that only one
  /// thread at a time uses a push socket.   
  std::mutex* pushMutexes;
  
  /// Flag to indicate that this class should no longer send data.
  bool terminated = false; 

  /// Each node creates numPushSockets sockets to send data to the other
  /// nodes in the cluster.  
  size_t numPushSockets;

  /// The total number of push sockets created.  This is 
  /// numPushSockets * (numNodes - 1)
  size_t totalNumPushSockets;

  /// These callback functions are called any time we receive data in the 
  /// pull threads.
  std::vector<FunctionType> callbacks;

  std::mt19937 myRand;
  std::uniform_int_distribution<size_t> dist;

  bool local = false;

public:
  /**
   * Constructor.
   *
   * Creates a set of push sockets.  The total number of push sockets created
   * is (numNodes - 1) * numPushSockets.  The reason for having multiple
   * push sockets is that we want to maximize the total network bandwidth.
   * In the code base, often multiple threads are pushing.  This will help
   * ease contention.
   *
   * After creating the push sockets, it also starts the pull threads.  There
   * are numPullThreads total pull threads.  The pullThreads cover 
   * (numNodes - 1) * numPushSockets pull sockets.  Experiments so far
   * indicate that only a few pull threads are necessary.
   *
   * \param numNodes The number of nodes in the cluster.
   * \param numPushSockets The number of push sockets to create for each node
   *   except for the given node.
   * \param numPullThreads How many total pull threads will cover the entire
   *   set of pull sockets.
   * \param hostnames The hostnames of all nodes in the cluster.
   * \param hwm High-water mark.  A zeromq parameter.
   * \param callbacks A vector of function wrappers.  These are called each
   *   time a message is received from the pull threads.
   * \param timeout The amount of time in ms that a send() call waits before
   *  timing out.  If -1, blocks until completed.
   * \param local Flag indicating that all the nodes are local
   */
  PushPull(   
    size_t numNodes,
    size_t nodeId,
    size_t numPushSockets,
    size_t numPullThreads,
    std::vector<std::string> hostnames,
    uint32_t hwm,
    std::vector<FunctionType> callbacks,
    size_t startingPort,
    int timeout,
    bool local = false);

  /**
   * This can be used to explicity set the hostnames and ports for all
   * the sockets.  This is useful for testing purposes when on the same
   * host
   */




  ~PushPull();

  /**
   * Sends the data to the specified node.
   * \return Returns true if the data was sent, false otherwise.
   */
  bool send(std::string data, size_t node);

  /**
   * Terminates accepting data and prevents more data from being sent.
   */
  void terminate();

  size_t getTotalMessagesReceived() const 
  {
    return totalMessagesReceived;
  }

  size_t getTotalMessagesSent() const 
  {
    return totalMessagesSent;
  }

  size_t getTotalMessagesFailed() const 
  {
    return totalMessagesFailed;
  }

  size_t getLastPort() const
  {
    return startingPort + (numNodes - 1) * numPushSockets - 1;
  }




private:

  /**
   * Creates the push sockets.
   */
  void createPushSockets();

  /**
   * Starts the pull threads. 
   */
  void initializePullThreads();
};

PushPull::PushPull(
  size_t numNodes,
  size_t nodeId,
  size_t numPushSockets,
  size_t numPullThreads,
  std::vector<std::string> hostnames,
  uint32_t hwm,
  std::vector<FunctionType> callbacks,
  size_t startingPort,
  int timeout,
  bool local)
{
  this->numNodes       = numNodes;
  this->nodeId         = nodeId;
  this->numPushSockets = numPushSockets;
  this->numPullThreads = numPullThreads;
  this->hostnames      = hostnames;
  this->hwm            = hwm;
  this->callbacks      = callbacks;
  this->startingPort   = startingPort;
  this->timeout        = timeout;
  this->local          = local;
  totalNumPushSockets = (numNodes - 1) * numPushSockets; 

  totalMessagesReceived = 0;
  totalMessagesSent     = 0;
  totalMessagesFailed   = 0;
  
  pushMutexes = new std::mutex[totalNumPushSockets];

  createPushSockets();

  std::random_device rd;
  myRand = std::mt19937(rd());
  dist = std::uniform_int_distribution<size_t>(0, numPushSockets-1);

  initializePullThreads();
}

PushPull::~PushPull()
{
  terminate();
  delete[] pushMutexes;
}

void PushPull::terminate()
{
  if (!terminated) 
  {
    terminated = true;

    for (size_t i = 0; i < totalNumPushSockets; i++) 
    {
      bool sent = false;
      //while (!sent) {
        //printf("Node %lu Sending terminate to %lu\n", nodeId, i);
        pushMutexes[i].lock();
        sent = pushers[i]->send(terminateZmqMessage());
        pushMutexes[i].unlock();
        if (!sent) {
          printf("Node %lu PullPull::terminate failed to send terminate "
            "message to %luth push socket\n", nodeId, i);
        }
      //}
    }

    for (size_t i = 0; i < numPullThreads; i++) 
    {
      pullThreads[i].join(); 
    }
  }
}

void PushPull::createPushSockets()
{
  pushers.resize(totalNumPushSockets);
  std::string hostname = hostnames[nodeId];
  std::string ip = getIpString(hostname);
  size_t actualStartingPort = startingPort;
  if (local) {
    actualStartingPort += nodeId * totalNumPushSockets;
  }
  DEBUG_PRINT("totalNumPushSockets %lu \n", totalNumPushSockets);
  for (size_t i = 0; i < totalNumPushSockets; i++) 
  {
    zmqLock.lock();
    auto pusher = std::shared_ptr<zmq::socket_t>(
      new zmq::socket_t(context, ZMQ_PUSH));
    std::string url = "tcp://" + ip + ":";
      url = url + boost::lexical_cast<std::string>(actualStartingPort + i);
      DEBUG_PRINT("Node %lu binding to %s\n", nodeId, url.c_str());

    // The function complains if you use std::size_t, so be sure to use the
    // uint32_t class member for hwm.
    pusher->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));

    // Setting the timeout for the socket
    DEBUG_PRINT("Node %lu setting timeout %d\n", nodeId, timeout);
    pusher->setsockopt(ZMQ_SNDTIMEO, &timeout, sizeof(timeout));

    try {
      pusher->bind(url);
    } catch (std::exception e) {
      std::string message = "Node " +
        boost::lexical_cast<std::string>(nodeId) +
        " couldn't bind to url " + url + ": " + e.what();
      throw std::runtime_error(message);
    }
    zmqLock.unlock();

    pushers[i] = pusher;
  }
}

void PushPull::initializePullThreads()
{
  auto pullFunction = [this](size_t threadId)
  {
    size_t numPullThreads = this->numPullThreads;
    size_t numPushSockets = this->numPushSockets;
    size_t receivedMessages = 0;

    size_t beg = get_begin_index(totalNumPushSockets, threadId, numPullThreads);
    size_t end = get_end_index(totalNumPushSockets, threadId, numPullThreads);

    this->zmqLock.lock();
    size_t numVisiblePushSockets = end - beg;
    //printf("beg %lu end %lu numVisiblePushSockets %lu\n", beg, end,
    //  numVisiblePushSockets);

    zmq::pollitem_t pollItems[numVisiblePushSockets];
    std::vector<zmq::socket_t*> sockets;

    // When a node sends a terminate flag, the corresponding entry is
    // turned to true.  When all flags are true, the thread terminates.
    bool terminate[numVisiblePushSockets];

    // All sockets passed to zmq_poll() function must belong to the same
    // thread calling zmq_poll().  Below we create the poll items and the
    // pull sockets.

    size_t numAdded = 0;
    for( size_t i = beg; i < end; i++) {
      std::string hostname = getHostnameForPull(i, nodeId, numPushSockets,
                                                numNodes, hostnames);
      size_t port = getPortForPull(i, nodeId, numPushSockets,
                                   numNodes, startingPort);

      if (local) {
        // Find which node we are wanting to talk to.
        size_t targetNode = i / numPushSockets;
        if ((i / numPushSockets) >= nodeId) {
          targetNode++;
        } 

        port += targetNode * totalNumPushSockets;
      }

      zmq::socket_t* socket = new zmq::socket_t(context, ZMQ_PULL);
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
    this->zmqLock.unlock();

    bool stop = false;

    auto timeDataArrived = std::chrono::high_resolution_clock::now();

    while (!stop) {
      zmq::message_t message;
      // Third parameter is the timeout in milliseconds to wait for input.
      int numStop = 0;
      int rValue = zmq::poll(pollItems, numVisiblePushSockets, 1);
      for (size_t i = 0; i < numVisiblePushSockets; i++) {
        if (pollItems[i].revents & ZMQ_POLLIN) {

          sockets[i]->recv(&message);
          if (isTerminateMessage(message)) {

            
            
            DEBUG_PRINT("Node %lu PushPull pullThread received terminate "
              "from %lu\n", nodeId, i);
            terminate[i] = true;

            timeDataArrived = std::chrono::high_resolution_clock::now();

          } else if (message.size() > 0) {
            
            std::string str = getStringFromZmqMessage(message);
            receivedMessages++;

            DEBUG_PRINT("Node %lu PushPull pullThread received message of"
              " size %lu from %lu %s\n", nodeId, message.size(), i, 
               str.c_str());

            for (auto callback : callbacks) {
              callback(str);              
            }

            timeDataArrived = std::chrono::high_resolution_clock::now();

          } else {
            
            printf("Node %lu pullThread received mystery"
              " message %s\n", nodeId,
              getStringFromZmqMessage(message).c_str());
          }
        }
        if (terminate[i]) numStop++;
      }
      
      // Exit if we haven't received data for a while
      auto timeNow = std::chrono::high_resolution_clock::now();
      size_t timeDiff = 
        std::chrono::duration_cast<std::chrono::milliseconds>(
                        timeNow - timeDataArrived).count();
     
      //printf("Node %lu timeDiff %lu\n", nodeId, timeDiff);

      if (numStop == numVisiblePushSockets) stop = true;
      if (timeDiff > 10000) stop = true;
    }

    for (auto socket : sockets) {
      delete socket;
    }
    
    this->totalMessagesReceived.fetch_add(receivedMessages);

    printf("Node %lu pullThread exiting\n", this->nodeId);

  };

  for (size_t i = 0; i < numPullThreads; i++) {
    pullThreads.push_back(std::thread(pullFunction, i));
  }
}

bool PushPull::send(std::string str, size_t otherNode)
{
  DEBUG_PRINT("Node %lu->%lu PushPull::send sending %s\n", nodeId, 
    otherNode, str.c_str());

  size_t pushSocket = dist(myRand);
  size_t offset = otherNode < nodeId ? otherNode : otherNode - 1;
  size_t index = offset * numPushSockets + pushSocket;
  zmq::message_t message = fillZmqMessage(str);
  
  pushMutexes[index].lock();
  bool sent = pushers[index]->send(message);
  pushMutexes[index].unlock();
  
  DEBUG_PRINT("Node %lu->%lu sent %s rvalue %d\n", nodeId, otherNode, 
    str.c_str(), sent);
  
  if (!sent) {
    size_t failedNodeId = index / numPushSockets;
    failedNodeId = failedNodeId >= nodeId ? failedNodeId + 1 : failedNodeId;
    printf("Node %lu PushPull::send couldn't send message to %luth socket "
      "failedNodeId %lu\n", nodeId, index, failedNodeId);
    totalMessagesFailed.fetch_add(1);
  } else {
    totalMessagesSent.fetch_add(1);
  }
  return sent;

}



}

#endif
