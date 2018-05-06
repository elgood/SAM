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

  typedef CompressedSparse<TupleType, source, target, time, duration, 
                          SourceHF, SourceEF> csrType;
  typedef CompressedSparse<TupleType, target, source, time, duration,
                          TargetHF, TargetEF> cscType;

  typedef EdgeDescription<TupleType, time, duration> EdgeDescriptionType;
 
private:

  #ifdef TIMING
  double totalTimeConsume = 0; 
  double totalTimeEdgePullThread = 0;
  double totalTimeRequestPullThread = 0;
  #endif

  #ifdef DETAIL_TIMING
  double totalTimeConsumeAddEdge = 0;
  double totalTimeConsumeResultMapProcess = 0; 

  // A list of consume times
  std::list<double> consumeTimes;
  #endif

  size_t consumeCount = 0;

  //std::mutex generalLock;

  /// Once the terminate signal has been given, we don't want to push out
  /// anymore edge requests or edges to other nodes.  To prevent data from
  /// being pushed out, all pushes are done with the same mutually 
  /// exclusive set of code using the below lock.  This lock is also
  /// passed to the EdgeRequestMap.
  std::mutex terminationLock;

  SourceHF sourceHash;
  TargetHF targetHash;
 
  /// The object that creates tuples from strings. 
  Tuplizer tuplizer;

  /// This stores the query results.  It maps source or dest to query results
  /// that are looking for that source or dest.
  std::shared_ptr<ResultMapType> resultMap;

  /// This stores all the edge requests we receive. 
  std::shared_ptr<RequestMapType> edgeRequestMap;

  /// Creates id for each tuple we get from other nodes
  SimpleIdGenerator idGenerator;

  /// There is another context in ZeroMQPushPull.  I think we can just
  /// instantiate another one here.
  zmq::context_t& context; 

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

  /// Flag indicating terminate was called.
  std::atomic<bool> terminated; 

  /// How many threads to use for parallel for loops.
  size_t numThreads;
  size_t currentThread = 0;
  std::vector<std::thread> threads;
  
  std::shared_ptr<csrType> csr; ///> Compressed Sparse Row graph
  std::shared_ptr<cscType> csc; ///> Compressed Sparse column graph
  std::vector<QueryType> queries; ///> The list of queries to run.
  std::atomic<size_t> edgePushCounter; ///> Count of how many edges we send
  std::atomic<size_t> edgePullCounter; ///> Count of how many edges we pull
  std::atomic<size_t> requestPushCounter; ///>Count of how many requests we send
  std::atomic<size_t> requestPullCounter; ///>Count of how many requests we pull

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
   * Constructor.
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
   * \param numThreads How many threads to use for parallel for loops.
   */
  GraphStore(
             zmq::context_t& _context,
             std::size_t numNodes,
             std::size_t nodeId,
             std::vector<std::string> requestHostnames,
             std::vector<std::size_t> requestPorts,
             std::vector<std::string> edgeHostnames,
             std::vector<std::size_t> edgePorts,
             uint32_t hwm,
             size_t graphCapacity,
             size_t tableCapacity,
             size_t resultsCapacity,
             double timeWindow,
             size_t numThreads);

  ~GraphStore();

  /**
   * Adds the tuple to the graph store.
   */
  void addEdge(TupleType n);

  bool consume(TupleType const& tuple);
  bool consumeDoesTheWork(TupleType const& tuple);

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
   * Returns how many tuples the graph store has received through
   * edge pulls.
   */
  inline size_t getEdgesPulled() { return edgePullCounter; }

  size_t getNumResults() const { return resultMap->getNumResults(); }
  size_t getNumIntermediateResults() const { 
    return resultMap->getNumIntermediateResults();
  }

  /**
   * Returns the total number of edges that this node has sent over the
   * edge push sockets.  It is a summation of the edges that the 
   * RequestPullThread sent plus the ones that the EdgeRequestMap sent. 
   */
  size_t getTotalEdgePushes() {
    return edgeRequestMap->getTotalEdgePushes() + edgePushCounter;
  }

  /**
   * Returns the total number of edges that this node pulled over the
   * zmq pull sockets.
   */
  size_t getTotalEdgePulls() {
    return edgePullCounter;
  }

  /**
   * Returns the total number of edge requests that this nodes has issued.
   */
  size_t getTotalRequestPushes() {
    return requestPushCounter;
  }

  /**
   * Returns the total number of edge requests that this nodes has pulled
   * via the zmq pull sockets.
   */
  size_t getTotalRequestPulls() {
    return requestPullCounter.load();
  }

  ResultType getResult(size_t index) const {
    return resultMap->getResult(index);
  }


  #ifdef TIMING
  double getTotalTimeConsume() const { return totalTimeConsume; }
  double getTotalTimeEdgePullThread() const { return totalTimeEdgePullThread; }
  double getTotalTimeRequestPullThread() const { 
    return totalTimeRequestPullThread; 
  }
  #endif

  #ifdef DETAIL_TIMING
  double getTotalTimeConsumeAddEdge() const {
    return totalTimeConsumeAddEdge;
  }
  double getTotalTimeConsumeResultMapProcess() const {
    return totalTimeConsumeResultMapProcess;
  }
  double getTotalTimeProcessAgainstGraph() const {
    return resultMap->getTotalTimeProcessAgainstGraph(); 
  }
  double getTotalTimeProcessSource() const {
    return resultMap->getTotalTimeProcessSource();
  }
  double getTotalTimeProcessTarget() const {
    return resultMap->getTotalTimeProcessTarget();
  }
  double getTotalTimeProcessSourceTarget() const {
    return resultMap->getTotalTimeProcessSourceTarget();
  }
  double getTotalTimeProcessSourceProcessAgainstGraph() const {
    return resultMap->getTotalTimeProcessSourceProcessAgainstGraph();
  }
  double getTotalTimeProcessSourceLoop1() const {
    return resultMap->getTotalTimeProcessSourceLoop1();
  }
  double getTotalTimeProcessSourceLoop2() const {
    return resultMap->getTotalTimeProcessSourceLoop2();
  }
  double getTotalTimeProcessTargetLoop1() const {
    return resultMap->getTotalTimeProcessTargetLoop1();
  }
  double getTotalTimeProcessTargetLoop2() const {
    return resultMap->getTotalTimeProcessTargetLoop2();
  }
  double getTotalTimeProcessSourceTargetLoop1() const {
    return resultMap->getTotalTimeProcessSourceTargetLoop1();
  }
  double getTotalTimeProcessSourceTargetLoop2() const {
    return resultMap->getTotalTimeProcessSourceTargetLoop2();
  }

  std::list<double> const& getConsumeTimes() const {
    return consumeTimes;
  }

  #endif

  #ifdef METRICS
  size_t getTotalResultsCreatedInResultMap() const {
    return resultMap->getTotalResultsCreated();
  }
  size_t getTotalResultsDeletedInResultMap() const {
    return resultMap->getTotalResultsDeleted();
  }
  size_t getTotalEdgesAddedInCsr() const {
    return csr->getTotalEdgesAdded();
  }
  size_t getTotalEdgesDeletedInCsr() const {
    return csr->getTotalEdgesDeleted();
  }
  size_t getTotalEdgesAddedInCsc() const {
    return csc->getTotalEdgesAdded();
  }
  size_t getTotalEdgesDeletedInCsc() const {
    return csc->getTotalEdgesDeleted();
  }
  #endif


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
  #ifdef DEBUG
  printf("Node %lu entering GraphStore::addEdge tuple %s\n", nodeId, 
    sam::toString(tuple).c_str());
  #endif
  //std::lock_guard<std::mutex> lock(generalLock);
  csc->addEdge(tuple);
  csr->addEdge(tuple);
  #ifdef DEBUG
  printf("Node %lu exiting GraphStore::addEdge tuple %s\n", nodeId, 
    sam::toString(tuple).c_str());
  #endif
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
  #ifdef DEBUG
  printf("Node %lu GraphStore::checkSubgraphQueries tuple %s\n",
    nodeId, sam::toString(tuple).c_str()); 
  #endif

  // The start time of the query result is the time field of the first 
  // tuple in the query.  
  double startTime = std::get<time>(tuple);
  for (QueryType const& query : queries) {
    if (query.satisfies(tuple, 0, startTime)) {

      // We only want one node to own the query result, so we make sure
      // that this node owns the source
      SourceType src = std::get<source>(tuple);
      
      #ifdef DEBUG
      printf("Node %lu GraphStore::checkSubgraphQueries src %s "
        "soruceHash(src) %llu numNodes %lu sourceHash(src) mod numNodes %llu\n",
        nodeId, src.c_str(), sourceHash(src), numNodes, 
        sourceHash(src) % numNodes);
      #endif


      if (sourceHash(src) % numNodes == nodeId) {
        ResultType queryResult(&query, tuple);

        #ifdef DEBUG
        printf("Node %lu GraphStore::checkSubgraphQueries adding queryResult " 
          "%s\n", this->nodeId, queryResult.toString().c_str());
        #endif

        resultMap->add(queryResult, *csr, *csc, edgeRequests);  
        
        #ifdef DEBUG
        printf("Node %lu GraphStore::checkSubgraphQueries added queryResult " 
          "%s.  EdgeRequests.size() %lu\n", this->nodeId, 
          queryResult.toString().c_str(), edgeRequests.size());
        #endif
      } else {

        #ifdef DEBUG
        printf("Node %lu GraphStore::checkSubgraphQueries this node didn't own"
          "source in %s\n", this->nodeId, sam::toString(tuple).c_str());
        #endif
      }
    }
  }
  #ifdef DEBUG
  std::string message = "Node " + boost::lexical_cast<std::string>(nodeId) + 
    " GraphStore::checkSubgraphQueries edgeRequests from tuple " +
    sam::toString(tuple) + ": ";
  for(auto edgeRequest : edgeRequests) {
    message += edgeRequest.toString() + "    ";
  }
  printf("%s\n", message.c_str());
  #endif
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
  //std::lock_guard<std::mutex> lock(generalLock);
  #ifdef DEBUG
  printf("Node %lu GraphStore::processEdgeRequests() there are %lu "
    "edge requests\n", this->nodeId, edgeRequests.size());
  #endif
  
  // Don't want to issue more edge requests if we've been terminated.
  if (!terminated) {
    
    for(auto edgeRequest : edgeRequests) {

      #ifdef DEBUG
      printf("Node %lu GraphStore::processEdgeRequests() processing edgeRequest"
        " %s\n", this->nodeId, edgeRequest.toString().c_str());
      #endif

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

        //TODO partition info        
        // load balancing by splitting our requests 
        if (rand() % 2 == 0 )
        {
          sendMessageToSource(edgeRequest);
        } else {
          sendMessageToTarget(edgeRequest);
        }
      }
    }
  }
  #ifdef DEBUG
  printf("Node %lu end of GraphStore::processEdgeRequests\n", this->nodeId);
  #endif
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

  std::string str = getStringFromZmqMessage( message );
  EdgeRequestType edgeRequest1(str);


  terminationLock.lock();
  if (!terminated) {
    requestPushCounter.fetch_add(1);
    #ifdef DEBUG
    printf("Node %lu->%lu GraphStore::sendMessageToSource Sending EdgeRequest: "
           "%s terminated %d\n", nodeId, node, edgeRequest.toString().c_str(), 
           terminated.load());
    #endif
    requestPushers[node]->send(message);
  }
  terminationLock.unlock();
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

  terminationLock.lock();
  if (!terminated) {
    #ifdef DEBUG
    printf("Node %lu->%lu GraphStore::sendMessageToTarget sending EdgeRequest:"
          " %s\n", nodeId, node, edgeRequest.toString().c_str());
    #endif
    requestPushCounter.fetch_add(1);
    requestPushers[node]->send(message);
  }
  terminationLock.unlock();
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



  auto consumeFunction = [this](TupleType const& tuple) {
    this->consumeDoesTheWork(tuple);  
  };

  if (consumeCount >= numThreads) {
    threads[currentThread].join();
  }
  threads[currentThread] = std::thread(consumeFunction, tuple);
  currentThread++;

  if (currentThread >= numThreads) currentThread = 0;

  consumeCount++;

  return true;
}


template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
bool 
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
consumeDoesTheWork(TupleType const& tuple)
{

  #ifdef TIMING
  auto timestamp_consume1 = std::chrono::high_resolution_clock::now();
  #endif

  TupleType myTuple = tuple;
  // Give the tuple a new id
  std::get<0>(myTuple) = idGenerator.generate();

  #ifdef DEBUG
  printf("Node %lu GraphStore::consume tuple %s\n", nodeId, 
    sam::toString(myTuple).c_str());
  #endif


  // Adds the edge to the graph
  DETAIL_TIMING_BEG1
  addEdge(myTuple);
  DETAIL_TIMING_END1(totalTimeConsumeAddEdge)

  // Check against existing queryResults.  The edgeRequest list is populated
  // with edge requests when we find we need a tuple that will reside 
  // elsewhere.
  DETAIL_TIMING_BEG2
  std::list<EdgeRequestType> edgeRequests;
  resultMap->process(myTuple, *csr, *csc, edgeRequests);
  DETAIL_TIMING_END2(totalTimeConsumeResultMapProcess)

  // See if anybody needs this tuple and send it out to them.
  edgeRequestMap->process(myTuple);

  // Check against all registered queries
  checkSubgraphQueries(myTuple, edgeRequests);

  // Send out the edge requests to the other nodes.
  processEdgeRequests(edgeRequests);

  #ifdef TIMING
  auto timestamp_consume2 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> time_space = 
    std::chrono::duration_cast<std::chrono::duration<double>>(
      timestamp_consume2 - timestamp_consume1);
  double time_consume = time_space.count(); 
  totalTimeConsume += time_consume;

    #ifdef DETAIL_TIMING
    consumeTimes.push_back(time_consume);
    #endif

  #endif

  #ifdef DEBUG
  printf("Node %lu exiting GraphStore::consume\n", nodeId);
  #endif

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
  #ifdef DEBUG
  printf("Node %lu entering GraphStore::terminate\n", nodeId);
  #endif
  if (!terminated) {  

    terminationLock.lock();
    terminated = true;
    terminationLock.unlock();
    // If terminate was called, we aren't going to receive any more
    // edges, so we can push out the terminate signal to all the edge request
    // channels. 
    for(size_t i = 0; i < numNodes; i++) {
      if (i != nodeId) {
        zmq::message_t message = terminateZmqMessage();

        #ifdef DEBUG
        printf("Node %lu GraphStore::terminate sending terminate message"
          " to requestPusher %lu\n", this->nodeId, i);
        #endif

        requestPushers[i]->send(message);
      }
    }

    requestPullThread.join();

    #ifdef DEBUG
    printf("Node %lu requestPullThread joined\n", nodeId);
    #endif
 
    // The EdgeRequestMap has all the edge pushers.  We call terminate
    // on the EdgeRequestMap to send out the terminate message.
    edgeRequestMap->terminate();
    edgePullThread.join();

    #ifdef DEBUG
    printf("Node %lu edgePullThread joined\n", nodeId);
    #endif
  }
  #ifdef DEBUG
  printf("Node %lu exiting GraphStore::terminate\n", nodeId);
  #endif
}

/**
 * Constructor
 */
template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
GraphStore(  zmq::context_t& _context,
             std::size_t numNodes,
             std::size_t nodeId,
             std::vector<std::string> requestHostnames,
             std::vector<std::size_t> requestPorts,
             std::vector<std::string> edgeHostnames,
             std::vector<std::size_t> edgePorts,
             uint32_t hwm,
             std::size_t graphCapacity, //How many bins for csr and csc
             std::size_t tableCapacity, //For SubgraphQueryResultMap
             std::size_t resultsCapacity, //For SubgraphQueryResultMap
             double timeWindow,
             size_t numThreads) 
: context(_context)
{
  threads.resize(numThreads);

  terminated = false;
  this->numNodes = numNodes;
  this->nodeId   = nodeId;
  this->numThreads = numThreads;

  edgePushCounter = 0;
  edgePullCounter = 0;
  requestPushCounter = 0;
  requestPullCounter = 0;

  resultMap = 
    std::make_shared< ResultMapType>( numNodes, nodeId, 
      tableCapacity, resultsCapacity, numThreads);

  // Resizing the push socket vectors to be the size of numNodes
  requestPushers.resize(numNodes);
  edgePushers.resize(numNodes);

  createPushSockets(&context, numNodes, nodeId, requestHostnames, requestPorts,
                    requestPushers, hwm);
  createPushSockets(&context, numNodes, nodeId, edgeHostnames, edgePorts,
                    edgePushers, hwm);


  edgeRequestMap = std::make_shared< RequestMapType>( context,
    numNodes, nodeId, edgeHostnames, edgePorts, hwm, tableCapacity,
    edgePushers, terminationLock);

  csr = std::make_shared<csrType>(graphCapacity, timeWindow); 
  csc = std::make_shared<cscType>(graphCapacity, timeWindow); 

  std::atomic<size_t>* requestPullCounterPtr = &requestPullCounter;

  /// The requestPullFunction is responsible for polling all the edge request
  /// pull sockets.
  auto requestPullFunction = [this, requestHostnames, requestPorts, hwm,
    requestPullCounterPtr]() 
  {
    #ifdef TIMING
    auto t1 = std::chrono::high_resolution_clock::now();
    #endif
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
       
        try { 
          #ifdef DEBUG
          printf("Node %lu GraphStore request pull function connecting "
            "to %s\n", this->nodeId, url.c_str());
          #endif
          socket->connect(url);
        } catch (std::exception e) {
          std::string message = "Couldn't connect to url " + url;
          throw GraphStoreException(message);
        }
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

      //std::lock_guard<std::mutex> lock(generalLock);

      int rValue = zmq::poll(pollItems, this->numNodes -1, 1);
      int numStop = 0;
      for (size_t i = 0; i < this->numNodes -1; i++) {
        if (pollItems[i].revents & ZMQ_POLLIN) {
          sockets[i]->recv(&message);
          if (isTerminateMessage(message)) {
            
            #ifdef DEBUG
            printf("Node %lu RequestPullThread received a terminate "
                   "message from %lu\n", this->nodeId, i);
            #endif

            terminate[i] = true;
          } else if (message.size() > 0) {
          
            requestPullCounterPtr->fetch_add(1);

            std::string str = getStringFromZmqMessage(message);
            EdgeRequestType request(str);

            #ifdef DEBUG
            printf("Node %lu RequestPullThread Received an edge request"
              " length = %lu: %s\n", 
              this->nodeId, message.size(), request.toString().c_str());
            #endif

            // When we get an edge request, we need to check against
            // the graph (existing matches) and add it to the list 
            // so that any new matches are caught.  Since there are
            // two other threads that add to the graph 
            // (the edgeRequest pull thread and the consume thread),
            // we need a lock to make sure we don't miss edges are add
            // edge multiple times. 
            
            //generalLock.lock();

            edgeRequestMap->addRequest(request);
            #ifdef DEBUG
            printf("Node %lu RequestPullThread added edge request to map"
              ": %s\n", 
              this->nodeId, request.toString().c_str());
            #endif

            processRequestAgainstGraph(request);
            #ifdef DEBUG
            printf("Node %lu RequestPullThread processed edge request against "
              "graph: %s\n", 
              this->nodeId, request.toString().c_str());
            #endif

            //generalLock.unlock();
            #ifdef DEBUG
            printf("Node %lu RequestPullThread processed edge request"
              ": %s\n", 
              this->nodeId, request.toString().c_str());
            #endif


          } else {
            #ifdef DEBUG
            printf("Node %lu GraphStore::requestPullFunction received mystery "
              "message %s\n", getStringFromZmqMessage(message).c_str());
            #endif
          }
        }

        if (terminate[i]) numStop++;
      }
      if (numStop == this->numNodes - 1) stop = true;
    }

    #ifdef DEBUG
    printf("Node %lu exiting requestPullThread\n", this->nodeId);
    #endif

    for (auto socket : sockets) {
      delete socket;
    }

    #ifdef TIMING
    auto t2 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> time_space = 
      std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
    double timeRequestPullThread = time_space.count(); 
    totalTimeRequestPullThread += timeRequestPullThread;
    #endif
  };

  requestPullThread = std::thread(requestPullFunction);

  // Function that is responsible for pulling edges from other nodes
  // that result from edge requests. 
  auto edgePullFunction = [this, edgeHostnames, 
                           edgePorts, hwm]() 
  {
    #ifdef TIMING
    auto t1 = std::chrono::high_resolution_clock::now();
    #endif

    zmq::pollitem_t pollItems[this->numNodes - 1];
    std::vector<zmq::socket_t*> sockets;

    bool terminate[this->numNodes - 1];

    int numAdded = 0;
    for(size_t i = 0; i < this->numNodes; i++) {
      if (i != this->nodeId)
      {
        zmq::socket_t* socket = new zmq::socket_t((this->context), ZMQ_PULL);
        std::string ip = getIpString(edgeHostnames[i]);
        std::string url = "";
        url = "tcp://" + ip + ":";
        try {
          url = url + boost::lexical_cast<std::string>(edgePorts[this->nodeId]);
        } catch (std::exception e) {
          std::string message = "Trouble with lexical_cast: " +
            std::string(e.what());
          throw GraphStoreException(message);
        }
        try {
          #ifdef DEBUG
          printf("Node %lu GraphStore edge pull function connecting "
            "to %s\n", this->nodeId, url.c_str());
          #endif
          socket->connect(url);
        } catch (std::exception e) {
          std::string message = "Couldn't connect to url " + url;
          throw GraphStoreException(message);
        }
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

      //std::lock_guard<std::mutex> lock(generalLock);
      //#ifdef DEBUG
      //printf("Node %lu edgePullFunction before poll\n", this->nodeId);
      //#endif
      size_t numStop = 0;
      int rvalue = zmq::poll(pollItems, this->numNodes - 1, 1);
      for (size_t i = 0; i < this->numNodes - 1; i++) {
        if (pollItems[i].revents & ZMQ_POLLIN) {
          sockets[i]->recv(&message);
          if (isTerminateMessage(message)) {
            
            #ifdef DEBUG
            printf("Node %lu GraphStore::edgePullThread received a terminate "
                   "message from %lu\n",
              this->nodeId, i);
            #endif

            terminate[i] = true;
          } else if (message.size() > 0) {
            #ifdef DEBUG
            printf("Node %lu GraphStore::edgePullFunction else\n", 
              this->nodeId);
            #endif

            // Increment the counter indicating we got another edge by pulling.
            edgePullCounter.fetch_add(1);

            // Change the zmq message into a string.
            std::string str = getStringFromZmqMessage(message);

            // We give the edge a new id that is unique to this node.
            size_t id = idGenerator.generate();

            // Change the string into the expected tuple type.
            TupleType tuple = tuplizer(id, str);

            #ifdef DEBUG
            printf("Node %lu GraphStore::edgePullFunction received a tuple "
              "%s\n", this->nodeId, sam::toString(tuple).c_str());
            #endif

            // Do we need to do this?
            // Add the edge to the graph
            //addEdge(tuple);

            #ifdef DEBUG
            printf("Node %lu GraphStore::edgePullFunction added edge %s\n",
              this->nodeId, sam::toString(tuple).c_str());
            #endif

            // Process the new edge over results and see if it satifies
            // queries.  If it does, there may be new edge requests.
            std::list<EdgeRequestType> edgeRequests;
            resultMap->process(tuple, *csr, *csc, edgeRequests);

            #ifdef DEBUG
            printf("Node %lu GraphStore::edgePullFunction processed edge %s\n",
              this->nodeId, sam::toString(tuple).c_str());
            #endif

            // Send out the edge requests to the other nodes.
            processEdgeRequests(edgeRequests);

          } else {
            #ifdef DEBUG
            printf("Node %lu GraphStore::edgePullFunction received mystery "
              "message %s\n", getStringFromZmqMessage(message).c_str());
            #endif
          }

        }
        if (terminate[i]) numStop++;
      }
      if (numStop == this->numNodes - 1 && this->terminated) stop = true;
    }

    #ifdef DEBUG
    printf("Node %lu exiting edge pull thread\n", this->nodeId);
    #endif
    
    for (auto socket : sockets) {
      delete socket;
    }
    
    #ifdef TIMING
    auto t2 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> time_space = 
      std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
    double timeEdgePullThread = time_space.count(); 
    totalTimeEdgePullThread += timeEdgePullThread;
    #endif
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

  for (auto & thread : threads) {
    try {
      thread.join();
    } catch (std::exception e) {

    }
  }

  #ifdef DEBUG
  printf("Node %lu end of ~GraphStore\n", nodeId);
  #endif
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
  #ifdef DEBUG
  printf("Node %lu GraphStore::processRequestAgainstGraph edgeRequest %s\n", 
    nodeId, edgeRequest.toString().c_str());
  #endif

  if (isNull(edgeRequest.getStartTimeFirst()) ||
      isNull(edgeRequest.getStartTimeSecond())) {
    std::string message = "Tried to process an edge request that doesn't" 
      " have the start time defined: " + edgeRequest.toString();
    throw GraphStoreException(message);
  }
  if (isNull(edgeRequest.getEndTimeFirst()) ||
      isNull(edgeRequest.getStartTimeSecond())) {
    std::string message = "Tried to process an edge request that doesn't"
      " have the end time defined: " + edgeRequest.toString();
    throw GraphStoreException(message);
  }
  SourceType src = edgeRequest.getSource();
  TargetType trg = edgeRequest.getTarget();
  std::list<TupleType> foundEdges;
  if (!isNull(src) && isNull(trg)) {
    // The source is not null, so we look up the edges in the compressed
    // sparse row graph
    
    #ifdef DEBUG
    printf("Node %lu GraphStore::processRequestAgainstGraph looking up "
      " edge request %s against csr because source is not null\n",
      nodeId, edgeRequest.toString().c_str());
    #endif

    csr->findEdges(edgeRequest, foundEdges);

  } else if (isNull(src) && !isNull(trg)) {
    // The target is not null, so we look up by the target using the 
    // compressed sparse column graph.  

    #ifdef DEBUG
    printf("Node %lu GraphStore::processRequestAgainstGraph looking up "
      " edge request %s against csc because target is not null\n",
      nodeId, edgeRequest.toString().c_str());
    #endif

    csc->findEdges(edgeRequest, foundEdges);

  } else if (!isNull(src) && !isNull(trg)) {
    // Doesn't matter which one we look up, so look it up in csr

    #ifdef DEBUG
    printf("Node %lu GraphStore::processRequestAgainstGraph looking up "
      " edge request %s against csr because source and target are not null\n",
      nodeId, edgeRequest.toString().c_str());
    #endif
    
    csr->findEdges(edgeRequest, foundEdges);
  } else {
    throw GraphStoreException("Tried to process an edge request but both " 
      "the source and target were null");
  }

  size_t node = edgeRequest.getReturn();

  #ifdef DEBUG
  printf("Node %lu GraphStore::processRequestAgainstGraph found %lu edges\n",
    nodeId, foundEdges.size());
  #endif

  for (auto edge : foundEdges) {
    SourceType src = std::get<source>(edge);
    TargetType trg = std::get<target>(edge);
    size_t srcHash = sourceHash(src) % numNodes;
    size_t trgHash = targetHash(trg) % numNodes;

    // Only send the message of the node won't get the message anyway.
    if (srcHash != node && trgHash != node) {

      zmq::message_t message = tupleToZmq(edge);


      terminationLock.lock();
      if (!terminated) {
        #ifdef DEBUG
        printf("Node %lu->%lu GraphStore::processRequestAgainstGraph sending "
          "edge %s\n", nodeId, node, sam::toString(edge).c_str());
        #endif
        edgePushCounter.fetch_add(1);
        edgePushers[node]->send(message);     
      }
      terminationLock.unlock();
    }
  }
}

} // end namespace sam

#endif

