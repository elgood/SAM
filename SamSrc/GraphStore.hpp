#ifndef SAM_GRAPHSTORE_HPP
#define SAM_GRAPHSTORE_HPP

/**
 * GraphStore.hpp
 *
 *  Created on: Dec 28, 2017
 *      Author: elgood
 */


#include "IdGenerator.hpp"
#include "Util.hpp"
#include "AbstractConsumer.hpp"
#include "CompressedSparse.hpp"
#include "SubgraphQuery.hpp"
#include "SubgraphQueryResultMap.hpp"
#include "EdgeRequestMap.hpp"
#include <zmq.hpp>
#include <thread>
#include <cstdlib>

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
    SourceHF, TargetHF, SourceEF, TargetEF> ResultMapType;

  typedef SubgraphQuery<TupleType, time, duration> QueryType;

  typedef SubgraphQueryResult<TupleType, source, target, time, duration>
          ResultType;

  typedef EdgeRequest<TupleType, source, target> EdgeRequestType;
  typedef EdgeRequest<TupleType, target, source> CscEdgeRequestType;

  typedef EdgeRequestMap<TupleType, source, target, SourceHF, TargetHF,
    SourceEF, TargetEF> RequestMapType;

  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;

  typedef CompressedSparse<TupleType, SourceIp, DestIp, TimeSeconds, 
                          StringHashFunction, StringEqualityFunction> csrType;
  typedef CompressedSparse<TupleType, DestIp, SourceIp, TimeSeconds, 
                          StringHashFunction, StringEqualityFunction> cscType;
 
private:

  std::mutex generalLock;

  SourceHF sourceHash;
  TargetHF targetHash;
 
  /// The object that creates tuples from strings. 
  Tuplizer tuplizer;

  /// This stores the query results.  It maps source or dest to query results
  /// that are looking for that source or dest.
  std::shared_ptr<ResultMapType> resultMap;

  std::shared_ptr<RequestMapType> edgeRequestMap;

  /// Creates id for each tuple we get from other nodes
  SimpleIdGenerator idGenerator;

  /// There is another context in ZeroMQPushPull.  I think we can just
  /// instantiate another one here.
  //zmq::context_t* context = new zmq::context_t(1);
  zmq::context_t context = zmq::context_t(1);

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

  // Flag to indicate if terminate has been called. 
  bool terminated = false;
   
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

  void processRequestAgainstGraph(EdgeRequestType const& edgeRequest);
  
  /**
   * This goes through the list of new edge requests and sends them out to
   * the appropriate nodes.
   */
  void processEdgeRequests(std::list<EdgeRequestType> const& edgeRequests);


  /**
   * Sends the specified edgeRequest to the source node.
   */
  void sendMessageToSource(EdgeRequestType const& edgeRequest);

  /**
   * Sends the specified edgeRequest to the target node.
   */
  void sendMessageToTarget(EdgeRequestType const& edgeRequest);

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
   * \param tableCapacity The number of bins in the SubgraphQueryResultMap.
   * \param resultsCapacity How many completed queries can be stored in
   *          SubgraphQueryResultMap.
   * \param timeWindow How long do we keep edges.
   */
  GraphStore(std::size_t numNodes,
             std::size_t nodeId,
             std::vector<std::string> requestHostnames,
             std::vector<std::size_t> requestPorts,
             std::vector<std::string> edgeHostnames,
             std::vector<std::size_t> edgePorts,
             uint32_t hwm,
             size_t graphCapacity,
             size_t tableCapacity,
             size_t resultsCapacity,
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
    if (!query.isFinalized()) {
      throw GraphStoreException("Tried to add query that had not been"
        " finalized");
    }
    queries.push_back(query);
  }

  void checkSubgraphQueries(TupleType const& tuple,
                            std::list<EdgeRequestType>& edgeRequests);

  /**
   * Returns how many tuples the graph store has received, either through
   * consume or through edge requests.
   */
  inline size_t getTuplesReceived() { return tuplesReceived; }

  uint64_t getNumResults() const { return resultMap->getNumResults(); }

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
  csc->addEdge(tuple);
  csr->addEdge(tuple);
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
checkSubgraphQueries(TupleType const& tuple,
                     std::list<EdgeRequestType>& edgeRequests) 
{
  double startTime = std::get<time>(tuple);
  for (QueryType const& query : queries) {
    if (query.satisfies(tuple, 0, startTime)) {
      ResultType queryResult(&query, tuple);
      printf("GraphStore adding queryResult %s\n", 
        queryResult.toString().c_str());
      resultMap->add(queryResult,
                     edgeRequests);  
    }
  }
}

/**
 * Goes through the given edge requests that this node needs 
 * and sends the requests out to the appropriate node.
 */
template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
void
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
processEdgeRequests(std::list<EdgeRequestType> const& edgeRequests)
{
  printf("processEdgeRequests()\n");
  for(auto edgeRequest : edgeRequests) {
    printf("edgeRequest %s\n", edgeRequest.toString().c_str());

    if (isNull(edgeRequest.getTarget()) && isNull(edgeRequest.getSource()))
    {
      throw GraphStoreException("In GraphStore::processEdgeRequests, both the"
        " source and the target of an edge request was null.  Don't know what"
        " do with that.");
    }
    else 
    if (!isNull(edgeRequest.getTarget()) && isNull(edgeRequest.getSource()))
    {
      //If the target is not null but the source is, we send the edge request
      //to whomever owns the target.
      sendMessageToTarget(edgeRequest);
    }
    else 
    if (isNull(edgeRequest.getTarget()) && !isNull(edgeRequest.getSource()))
    {
      //If the source is not null but the target is, we send the edge request
      //to whomever owns the source.
      sendMessageToSource(edgeRequest);
    }
    else 
    if (!isNull(edgeRequest.getTarget()) && !isNull(edgeRequest.getSource()))
    {
      //If both source and target are not null, it doesn't really matter
      //to which node we send the edge request, since both nodes will have
      //matching edges.
      
      // load balancing by splitting our requests 
      if (rand() % 2 == 0 )
      {
        sendMessageToSource(edgeRequest);
      } else {
        sendMessageToTarget(edgeRequest);
      }
    }
  }
  printf("end of processEdgeRequests\n");
}

template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
void 
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
sendMessageToSource(EdgeRequestType const& edgeRequest)
{  
  zmq::message_t message = edgeRequest.toZmqMessage();
  SourceType src = edgeRequest.getSource();
  size_t node = sourceHash(src) % numNodes;
  requestPushers[node]->send(message);
}

template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
void 
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
sendMessageToTarget(EdgeRequestType const& edgeRequest)
{
  zmq::message_t message = edgeRequest.toZmqMessage();
  TargetType trg = edgeRequest.getTarget();
  size_t node = targetHash(trg) % numNodes;
  requestPushers[node]->send(message);
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
  //printf("consume\n");
  // Adds the edge to the graph
  generalLock.lock();
  addEdge(tuple);
  generalLock.unlock();
  //printf("added edge\n");

  // TODO Delete old edges, maybe

  // Check against existing queryResults.  The edgeRequest list is populated
  // with edge requests when we find we need a tuple that will reside 
  // elsewhere.
  std::list<EdgeRequestType> edgeRequests;
  resultMap->process(tuple, edgeRequests);
  printf("resultMap->processed tuple in GraphStore::consume()\n");

  // Send out the edge requests to the other nodes.
  processEdgeRequests(edgeRequests);
  //printf("processed edge requests\n");

  // See if anybody needs this tuple and send it out to them.
  edgeRequestMap->process(tuple);
  printf("edge request map processed tuple\n");

  // Check against all registered queries
  checkSubgraphQueries(tuple, edgeRequests);
  //printf("checked subgraph queries\n");

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
  if (!terminated) {  
    // If terminate was called, we aren't going to receive any more
    // edges, so we can push out the terminate signal to all the edge request
    // channels. 
    for(int i = 0; i < numNodes; i++) {
      if (i != nodeId) {
        zmq::message_t message = emptyZmqMessage();
        requestPushers[i]->send(message);
      }
    }

    // The EdgeRequestMap has all the edge pushers.  We call terminate
    // on the EdgeRequestMap to send out the terminate message.
    edgeRequestMap->terminate();

    // The two threads running the pull sockets should terminate after 
    // receiving terminate messages from the other nodes.
    requestPullThread.join();
    edgePullThread.join();
  }
  terminated = true;
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
             std::size_t graphCapacity, //How many bins for csr and csc
             std::size_t tableCapacity, //For SubgraphQueryResultMap
             std::size_t resultsCapacity, //For SubgraphQueryResultMap
             double timeWindow)
{
  this->numNodes = numNodes;
  this->nodeId   = nodeId;


  resultMap = std::make_shared< ResultMapType>( numNodes, nodeId, 
                                                tableCapacity, resultsCapacity);

  edgeRequestMap = std::make_shared< RequestMapType>(
    numNodes, nodeId, edgeHostnames, edgePorts, hwm, tableCapacity);

  csr = std::make_shared<csrType>(graphCapacity, timeWindow); 
  csc = std::make_shared<cscType>(graphCapacity, timeWindow); 

  // Resizing the push socket vectors to be the size of numNodes
  requestPushers.resize(numNodes);
  edgePushers.resize(numNodes);

  createPushSockets(&context, numNodes, nodeId, requestHostnames, requestPorts,
                    requestPushers, hwm);
  createPushSockets(&context, numNodes, nodeId, edgeHostnames, edgePorts,
                    edgePushers, hwm);

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
        zmq::socket_t* socket = new zmq::socket_t((this->context), ZMQ_PULL);
        
        // Creating the address.  
        std::string ip = getIpString(requestHostnames[i]);
        std::string url = "";
        url = "tcp://" + ip + ":";
        try {
          url = url + boost::lexical_cast<std::string>(
                              requestPorts[this->nodeId]);
        } catch (std::exception e) {
          std::string message = "Trouble with lexical cast:" + 
            std::string(e.what());
          throw GraphStoreException(message);
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
            EdgeRequestType request(sEdgeRequest);

            // When we get an edge request, we need to check against
            // the graph (existing matches) and add it to the list 
            // so that any new matches are caught.  Since there are
            // two other threads that add to the graph 
            // (the edgeRequest pull thread and the consume thread),
            // we need a lock to make sure we don't miss edges are add
            // edge multiple times. 
            
            generalLock.lock();

            edgeRequestMap->addRequest(request);

            processRequestAgainstGraph(request);

            generalLock.unlock();

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

  // Function that is responsible for pulling edges from other nodes
  // that result from edge requests. 
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
        zmq::socket_t* socket = new zmq::socket_t((this->context), ZMQ_PULL);
        std::string ip = getIpString(edgeHostnames[i]);
        std::string url = "";
        url = "tcp://" + ip + ":";
        try {
          url = url + boost::lexical_cast<std::string>(edgePorts[this->nodeId]);
        } catch (std::exception e) {
          std::string message = "Trouble lexical_cast: " +std::string(e.what());
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
  terminate();
}

template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
void
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
processRequestAgainstGraph(EdgeRequestType const& edgeRequest)
{
  if (isNull(edgeRequest.getStartTime())) {
    std::string message = "Tried to process an edge request that doesn't" 
      " have the start time defined: " + edgeRequest.toString();
    throw GraphStoreException(message);
  }
  if (isNull(edgeRequest.getStopTime())) {
    std::string message = "Tried to process an edge request that doesn't"
      " have the stop time defined: " + edgeRequest.toString();
    throw GraphStoreException(message);
  }
  SourceType src = edgeRequest.getSource();
  TargetType trg = edgeRequest.getTarget();
  std::list<TupleType> foundEdges;
  if (!isNull(src) && isNull(trg)) {
    // The source is not null, so we look up the edges in the compressed
    // sparse row graph
    csr->findEdges(edgeRequest, foundEdges);

  } else if (isNull(src) && !isNull(trg)) {
    // The target is not null, so we look up by the target using the 
    // compressed sparse column graph.  
    csc->findEdges(edgeRequest, foundEdges);

  } else if (!isNull(src) && !isNull(trg)) {
    // Doesn't matter which one we look up, so look it up in csr
    csr->findEdges(edgeRequest, foundEdges);
  } else {
    throw GraphStoreException("Tried to process an edge request but both " 
      "the source and target were null");
  }

  size_t node = edgeRequest.getReturn();
  for (auto edge : foundEdges) {
    zmq::message_t message = tupleToZmq(edge);
    edgePushers[node]->send(message);     
  }
}

} // end namespace sam

#endif

