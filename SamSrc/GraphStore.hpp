#ifndef SAM_GRAPHSTORE_HPP
#define SAM_GRAPHSTORE_HPP

/**
 * GraphStore.hpp
 *
 *  Created on: Dec 28, 2017
 *      Author: elgood
 */


#include "EdgeRequestList.hpp"
#include "IdGenerator.hpp"
#include "Util.hpp"
#include "AbstractConsumer.hpp"
#include "CompressedSparse.hpp"
#include "SubgraphQuery.hpp"
#include "SubgraphQueryResultMap.hpp"
#include <zmq.hpp>
#include <thread>

namespace sam {

class GraphStoreException : public std::runtime_error {
public:
  GraphStoreException(char const * message) : std::runtime_error(message) { } 
  GraphStoreException(std::string message) : std::runtime_error(message) { } 
};


/**
 * A dynamic graph that allows you to add edges.  Deleting edges occurs
 * when the edges become too old as determined by the time duration of queries.
 * 
 * The GraphStore stores edges of type TupleType.  The source template parameter
 * defines the index with TupleType of the source of the edge.  target defines
 * the index of the target of the edge.  time defines the index to the time
 * field in TupleType (every tuple must have a time field).
 *
 * Right now it is hardcoded for TupleTypes.  Hopefully later can be generalized
 * to handle other types of tuples.
 */
template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
class GraphStore : public AbstractConsumer<TupleType>
{
public:
  typedef SubgraphQueryResultMap<TupleType, source, target, time, duration,
    SourceHF, TargetHF, SourceEF, TargetEF> MapType;

  typedef SubgraphQuery<TupleType> QueryType;

  typedef SubgraphQueryResult<TupleType, source, target, time, duration>
          ResultType;


private:
 
  /// The object that creates tuples from strings. 
  Tuplizer tuplizer;

  /// This stores the query results.  It maps source or dest to query results
  /// that are looking for that source or dest.
  std::shared_ptr<MapType> resultMap;

  /// This handles the requests we get from other nodes.
  EdgeRequestList requestList; 

  /// Creates id for each tuple we get from other nodes
  SimpleIdGenerator idGenerator;

  /// There is another context in ZeroMQPushPull.  I think we can just
  /// instantiate another one here.
  zmq::context_t* context = new zmq::context_t(1);

  /// The push sockets in charge of pushing requests to other nodes.
  std::vector<std::shared_ptr<zmq::socket_t>> requestPushers;

  /// The push sockets in charge of pushing edges to other nodes.
  std::vector<std::shared_ptr<zmq::socket_t>> edgePushers;
  
  /// The thread that polls the request pull sockets.
  std::thread requestPullThread;

  /// The thread that polls the edge pull sockets.
  std::thread edgePullThread;

  size_t numNodes; ///> How many total nodes there are
  size_t nodeId; ///> The node id of this node

  /// Used for testing to make sure we got expected number of tuples.
  size_t tuplesReceived = 0;

  typedef CompressedSparse<TupleType, SourceIp, DestIp, TimeSeconds, 
                          StringHashFunction, StringEqualityFunction> csrType;
  typedef CompressedSparse<TupleType, DestIp, SourceIp, TimeSeconds, 
                          StringHashFunction, StringEqualityFunction> cscType;
  
  /// Compressed Sparse Row graph
  std::shared_ptr<csrType> csr;

  /// Compressed Sparse column graph
  std::shared_ptr<cscType> csc;

  /// The list of queries to run.
  std::vector<QueryType> queries;

  /**
   * Called by addEdge, which has the current time.  Edges older than maximum
   * allowed are deleted.
   */
  void deleteEdges();

  void processRequests();
  
  void createPushSockets(
    std::vector<std::string>& hostnames,
    std::vector<size_t>& ports,
    std::vector<std::shared_ptr<zmq::socket_t>>& pushers,
    uint32_t hwm);

public:

  /**
   * There are two types of sockets we set up.  There is a set of sockets
   * to push and pull edge description requests.  There is a another set
   * of sockets to process the edges being transferred.
   *
   * \param numNodes The number of nodes in the cluster
   * \param nodeId The id of this node.
   * \param requestHostnames A vector of all the hostnames in the cluster.
   * \param requestPorts A vector of all the ports to be used for zeromq
   *                     communications for edge requests.
   * \param edgeHostnames A vector of all the hostnames in the cluster.
   * \param edgePorts The vector of ports to be used for zeromq communications
   *                   for sending of edges.
   * \param hwm The highwater mark.
   * \param graphCapacity The number of bins in the graph representation.
   * \param timeWindow How long do we keep edges.
   */
  GraphStore(std::size_t numNodes,
             std::size_t nodeId,
             std::vector<std::string> requestHostnames,
             std::vector<std::size_t> requestPorts,
             std::vector<std::string> edgeHostnames,
             std::vector<std::size_t> edgePorts,
             uint32_t hwm,
             std::size_t graphCapacity,
             double timeWindow);

  ~GraphStore();

  /**
   * Adds the tuple to the graph store.
   */
  void addEdge(TupleType n);


  bool consume(TupleType const& tuple);

  /**
   * Called by producer to indicate that no more data is coming and that this
   * consumer should clean up and exit.
   */
  void terminate();

  /**
   * Registers a subgraph query to run against the data
   */
  void registerQuery(QueryType query) {
    queries.push_back(query);
  }

  void checkSubgraphQueries(TupleType const& tuple);

  inline size_t getTuplesReceived() { return tuplesReceived; }

};

template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
void 
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
addEdge(TupleType tuple) 
{
  

}

template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
void 
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
deleteEdges() 
{


}

template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
void 
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
checkSubgraphQueries(TupleType const& tuple) 
{

  for (QueryType const& query : queries) {
    if (query.satisfies(tuple, 0)) {
      ResultType queryResult(query, tuple);
      //TODO
      //resultMap.add(query);  
    }
  }
}


template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
bool 
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
consume(TupleType const& tuple)
{
  csc->addEdge(tuple);
  csr->addEdge(tuple);

  // TODO Delete old edges, maybe

  // Check against existing queryResults
  resultMap->process(tuple);

  // Check against all registered queries
  checkSubgraphQueries(tuple);

  // TODO Check the edge to see if it fits any open requests
  // remove this: sending all edges to the other nodes
  for (int i = 0; i < numNodes; i++) {
    if (i != nodeId) {
      //printf("%lu sending message to node %i\n", nodeId, i);
      zmq::message_t message = tupleToZmq(tuple);
      //edgePushers[i]->send(message, ZMQ_NOBLOCK);
      edgePushers[i]->send(message);
    }
  }

  return true;
}

template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
void 
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
terminate() 
{
  
  // If terminate was called, we aren't going to receive any more
  // edges, so we can push out the terminate signal to all the edge request
  // channels. 
  for(int i = 0; i < numNodes; i++) {
    if (i != nodeId) {
      zmq::message_t message = emptyZmqMessage();
      requestPushers[i]->send(message);
    }
  }

  // Also since terminate was called, we can't fulfill any more
  // edge requests, so we can push out the terminate signal to all
  // edge channels.  The terminate signal is just an empty string.
  for(int i = 0; i < numNodes; i++) {
    if (i != nodeId) {
      zmq::message_t message = emptyZmqMessage();
      edgePushers[i]->send(message);
    }
  }

  // The two threads running the pull sockets should terminate after 
  // receiving terminate messages from the other nodes.
  requestPullThread.join();
  edgePullThread.join();
}

template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
GraphStore(std::size_t numNodes,
             std::size_t nodeId,
             std::vector<std::string> requestHostnames,
             std::vector<std::size_t> requestPorts,
             std::vector<std::string> edgeHostnames,
             std::vector<std::size_t> edgePorts,
             uint32_t hwm,
             std::size_t graphCapacity,
             double timeWindow)
{
  this->numNodes = numNodes;
  this->nodeId   = nodeId;


  resultMap = std::make_shared< MapType>( graphCapacity );

  csr = std::make_shared<csrType>(graphCapacity, timeWindow); 
  csc = std::make_shared<cscType>(graphCapacity, timeWindow); 

  // Resizing the push socket vectors to be the size of numNodes
  requestPushers.resize(numNodes);
  edgePushers.resize(numNodes); 

  //printf("blah %lu\n", nodeId);
  createPushSockets(requestHostnames, requestPorts,
                    requestPushers, hwm);

  //printf("Created request push sockets %lu\n", nodeId);
  createPushSockets(edgeHostnames, edgePorts,
                    edgePushers, hwm);
  //printf("Created edge push sockets %lu\n", nodeId);
  
  /// The requestPullFunction is responsible for polling all the edge request
  /// pull sockets.
  auto requestPullFunction = [this, requestHostnames, requestPorts, hwm]() 
  {
    // All sockets passed to zmq_poll() function must belong to the same
    // thread calling zmq_poll().  Below we create the poll items and the
    // pull sockets.
    zmq::pollitem_t pollItems[this->numNodes - 1];
    std::vector<zmq::socket_t*> sockets;

    // When a node sends a terminate flag, the corresponding entry is 
    // turned to true.  When all flags are true, the thread terminates.
    bool terminate[this->numNodes - 1];

    int numAdded = 0;
    for(int i = 0; i < this->numNodes; i++) {
      if (i != this->nodeId) {

        // Creating the zmq pull socket.
        zmq::socket_t* socket = new zmq::socket_t(*(this->context), ZMQ_PULL);
        
        // Creating the address.  
        std::string ip = getIpString(requestHostnames[i]);
        std::string url = "";
        url = "tcp://" + ip + ":";
        try {
          url = url + boost::lexical_cast<std::string>(
                              requestPorts[this->nodeId]);
        } catch (std::exception e) {
          throw GraphStoreException(e.what());
        }

        try {
          socket->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));
        } catch (std::exception e) {
          std::string message = std::string("Problem setting pull socket's") + 
            std::string(" send high water mark: ") + e.what();
          throw GraphStoreException(message);
        }
        
        socket->connect(url);
        sockets.push_back(socket);

        pollItems[numAdded].socket = *socket;
        pollItems[numAdded].events = ZMQ_POLLIN;
        terminate[numAdded] = false;
        numAdded++;
      }
    }

    zmq::message_t message;
    bool stop = false;

    while (!stop) {
      int rValue = zmq::poll(pollItems, this->numNodes -1, 1);
      int numStop = 0;
      for (int i = 0; i < this->numNodes -1; i++) {
        if (pollItems[i].revents & ZMQ_POLLIN) {
          sockets[i]->recv(&message);
          if (isTerminateMessage(message)) {
            terminate[i] = true;
          } else {
            char *buff = static_cast<char*>(message.data());
            std::string sEdgeRequest(buff);
            //TODO
            //TupleTypeEdgeRequest request;
            //bool b = request.ParseFromString(sEdgeRequest);
            //requestList.addRequest(request);
          }
        }

        if (terminate[i]) numStop++;
      }
      if (numStop == this->numNodes - 1) stop = true;
    }

    for (auto socket : sockets) {
      delete socket;
    }

  };

  requestPullThread = std::thread(requestPullFunction);

  auto edgePullFunction = [this, edgeHostnames, 
                           edgePorts, hwm]() 
  {
    zmq::pollitem_t pollItems[this->numNodes - 1];
    std::vector<zmq::socket_t*> sockets;

    bool terminate[this->numNodes - 1];

    int numAdded = 0;
    for(int i = 0; i < this->numNodes; i++) {
      if (i != this->nodeId)
      {
        zmq::socket_t* socket = new zmq::socket_t(*(this->context), ZMQ_PULL);
        std::string ip = getIpString(edgeHostnames[i]);
        std::string url = "";
        url = "tcp://" + ip + ":";
        try {
          url = url + boost::lexical_cast<std::string>(edgePorts[this->nodeId]);
        } catch (std::exception e) {
          throw GraphStoreException(e.what());
        }
        socket->connect(url);
        sockets.push_back(socket);

        pollItems[numAdded].socket = *socket;
        pollItems[numAdded].events = ZMQ_POLLIN;
        terminate[numAdded] = false;
        numAdded++;
      }
    }

    zmq::message_t message;
    bool stop = false;
    while (!stop) {
      int numStop = 0;
      int rvalue = zmq::poll(pollItems, this->numNodes - 1, 1);
      for (int i = 0; i < this->numNodes - 1; i++) {
        if (pollItems[i].revents & ZMQ_POLLIN) {
          sockets[i]->recv(&message);
          if (isTerminateMessage(message)) {
            terminate[i] = true;
          } else {
            tuplesReceived++;
            char *buff = static_cast<char*>(message.data());
            std::string sTuple(buff);
            size_t id = idGenerator.generate();
            TupleType tuple = tuplizer(id, sTuple);
            //TupleType tuple = makeTupleType(id, sTupleType);
            addEdge(tuple); 
          }
        }
        if (terminate[i]) numStop++;
      }
      if (numStop == this->numNodes - 1) stop = true;
    }

    for (auto socket : sockets) {
      delete socket;
    }

  };

  edgePullThread = std::thread(edgePullFunction);
}

template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
~GraphStore()
{
}


template <typename TupleType, typename Tuplizer,
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
void
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
createPushSockets(
    std::vector<std::string>& hostnames,
    std::vector<size_t>& ports,
    std::vector<std::shared_ptr<zmq::socket_t>>& pushers,
    uint32_t hwm)
{
  for (int i = 0; i < this->numNodes; i++) 
  {
    if (i != this->nodeId) // never need to send stuff to itself
    {
      //printf("createPushSockets nodeId %lu %d\n", this->nodeId, i);
      /////////// Adding push sockets //////////////
      auto pusher = std::shared_ptr<zmq::socket_t>(
                      new zmq::socket_t(*context, ZMQ_PUSH));
      //printf("createPushSockets nodeId %lu %d created socket\n", 
      //        this->nodeId, i);

      std::string ip = getIpString(hostnames[this->nodeId]);
      std::string url = "";
      url = "tcp://" + ip + ":";
      try { 
        url = url + boost::lexical_cast<std::string>(ports[i]);
      } catch (std::exception e) {
        throw GraphStoreException(e.what());
      }

      // The function complains if you use std::size_t, so be sure to use the
      // uint32_t class member for hwm.
      try {
        pusher->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));
      } catch (std::exception e) {
        std::string message = std::string("Problem setting push socket's send")+
          std::string(" high water mark: ") + e.what();
        throw GraphStoreException(message);
      }
      //printf("createPushSockets nodeId %lu %d set socket option\n", 
      //  this->nodeId, i);
      pusher->bind(url);
      //printf("createPushSockets nodeId %lu %d connect\n", this->nodeId, i);
      pushers[i] = pusher;
    } 
  }
}


} // end namespace sam

#endif

