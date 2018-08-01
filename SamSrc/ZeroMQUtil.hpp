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
    std::string message = "sam::getPort i " + 
      boost::lexical_cast<std::string>(i) + " >= (" +
      boost::lexical_cast<std::string>(numNodes) + " - ) * " +
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
 * 2) register callback functions that will be called when data arrives.
 * 3) call acceptData(), which starts the pull threads that gets data from
 *    the other nodes.
 * 4) Send data to the other nodes using send().
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
  std::vector<std::thread> pullThreads; ///> All the pull threads.
  uint32_t hwm; ///> The high-water mark
  size_t startingPort; ///> The starting port

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

public:
  PushPull(   
    size_t numNodes,
    size_t nodeId,
    size_t numPushSockets,
    size_t numPullThreads,
    std::vector<std::string> hostnames,
    uint32_t hwm,
    std::vector<FunctionType> callbacks,
    size_t startingPort);

  ~PushPull();

  /**
   * Sends the data to the specified node.
   */
  void send(std::string data, size_t node);

  /**
   * Terminates accepting data and prevents more data from being sent.
   */
  void terminate();


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
  size_t startingPort)
{
  this->numNodes       = numNodes;
  this->nodeId         = nodeId;
  this->numPushSockets = numPushSockets;
  this->numPullThreads = numPullThreads;
  this->hostnames      = hostnames;
  this->hwm            = hwm;
  this->callbacks      = callbacks;
  this->startingPort   = startingPort;
  totalNumPushSockets = (numNodes - 1) * numPushSockets; 
  
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
      while (!sent) {
        pushMutexes[i].lock();
        sent = pushers[i]->send(terminateZmqMessage());
        pushMutexes[i].unlock();
        if (!sent) {
          printf("Node %lu PullPull::terminate failed to send terminate "
            "message to %luth push socket\n", nodeId, i);
        }
      }
    }
  }
}

void PushPull::createPushSockets()
{
  pushers.resize(totalNumPushSockets);
  for (size_t i = 0; i < totalNumPushSockets; i++) 
  {
    zmqLock.lock();
    auto pusher = std::shared_ptr<zmq::socket_t>(
      new zmq::socket_t(context, ZMQ_PUSH));
    std::string hostname = hostnames[i];
    std::string ip = getIpString(hostname);
    std::string url = "tcp://" + ip + ":";
      url = url + boost::lexical_cast<std::string>(startingPort + i);
      DEBUG_PRINT("Node %lu binding to %s\n", nodeId, url.c_str());

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

    zmq::pollitem_t pollItems[numVisiblePushSockets];
    std::vector<zmq::socket_t*> sockets;

    // When a node sends a terminate flag, the corresponding entry is
    // turned to true.  When all flags are true, the thread terminates.
    bool terminate[numVisiblePushSockets];

    // All sockets passed to zmq_poll() function must belong to the same
    // thread calling zmq_poll().  Below we create the poll items and the
    // pull sockets.

    for( size_t i = beg; i < end; i++) {
      std::string hostname = getHostnameForPull(i, nodeId, 1,
                                                numNodes, hostnames);
      size_t port = getPortForPull(i, nodeId, 1,
                                   numNodes, startingPort);
      zmq::socket_t* socket = new zmq::socket_t(context, ZMQ_PULL);
      std::string url = "";
      std::string ip = getIpString(hostname);

      url = "tcp://" + ip + ":";
      url = url + boost::lexical_cast<std::string>(port);

      socket->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));

      try {
        socket->connect(url);
      } catch (std::exception e) {
        std::string message = "Node " +
          boost::lexical_cast<std::string>(nodeId) +
          " couldn't connect to url " + url + ": " + e.what();
        throw std::runtime_error(message);
      }
      sockets.push_back(socket);

      pollItems[i].socket = *socket;
      pollItems[i].events = ZMQ_POLLIN;
      terminate[i] = false;
    }
    this->zmqLock.unlock();

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
            
            DEBUG_PRINT("Node %lu pullThread received message of size %lu "
              "from %lu\n", nodeId, message.size(), i);
            receivedMessages++;
            
            std::string str = getStringFromZmqMessage(message);

            for (auto callback : callbacks) {
              callback(str);              
            }

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
    
    this->totalMessagesReceived.fetch_add(receivedMessages);

  };

  for (size_t i = 0; i < numPullThreads; i++) {
    pullThreads.push_back(std::thread(pullFunction, i));
  }
}

void PushPull::send(std::string str, size_t nodeId)
{
  size_t pushSocket = dist(myRand);
  size_t index = nodeId * numPushSockets + pushSocket;
  zmq::message_t message = fillZmqMessage(str);
  pushMutexes[index].lock();
  bool sent = pushers[index]->send(message);
  pushMutexes[index].unlock();
  if (!sent) {
    printf("Node %lu PushPull::send couldn't send message to %luth socket\n",
      nodeId, index);
  }

}



}

#endif
