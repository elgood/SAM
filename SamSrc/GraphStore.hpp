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
#include "ZeroMQUtil.hpp"
#include "FeatureMap.hpp"
#include <zmq.hpp>
#include <thread>
#include <cstdlib>
#include <future>

namespace sam {

#define MAX_NUM_FUTURES 1028
#define TOLERANCE 1.0 

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

  typedef EdgeRequestMap<TupleType, source, target, time, SourceHF, TargetHF,
    SourceEF, TargetEF> RequestMapType;

  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;

  typedef CompressedSparse<TupleType, source, target, time, duration, 
                          SourceHF, SourceEF> csrType;
  typedef CompressedSparse<TupleType, target, source, time, duration,
                          TargetHF, TargetEF> cscType;

  typedef EdgeDescription<TupleType, time, duration> EdgeDescriptionType;
 
private:

  // Returns the node (cluster) associated with the source of the edge request.
  std::function<size_t(EdgeRequestType const&)> sourceAddressFunction;

  // Returns the node (cluster) associated with the target of the edge request.
  std::function<size_t(EdgeRequestType const&)> targetAddressFunction;

  #ifdef TIMING
  double totalTimeConsume = 0; 
  #endif

  #ifdef DETAIL_TIMING
  double totalTimeConsumeAddEdge = 0;
  double totalTimeConsumeResultMapProcess = 0; 
  double totalTimeConsumeEdgeRequestMapProcess = 0;
  double totalTimeConsumeCheckSubgraphQueries = 0;
  double totalTimeConsumeProcessEdgeRequests = 0;
  double totalTimeEdgeCallbackProcessEdgeRequests = 0;
  double totalTimeEdgeCallbackResultMapProcess = 0;
  double totalTimeRequestCallbackAddRequest = 0; 
  double totalTimeRequestCallbackProcessAgainstGraph = 0; 

  // A list of consume times
  std::list<double> consumeTimes;
  #endif

  size_t consumeCount = 0;

  // Controls access to the result map TODO: test if we actually need this
  std::mutex resultMapLock;

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

  PushPull* edgeCommunicator;
  PushPull* requestCommunicator;

  /// Flag indicating terminate was called.
  std::atomic<bool> terminated; 
  
  /// This is the count of how many edges we send from this class and not
  /// from the EdgeRequestMap.
  std::atomic<size_t> edgePushCounter; 

  /// This is the count of how many edges we failed to send from this class
  /// and not from the EdgeRequestMap.
  std::atomic<size_t> edgePushFails; 
  
  size_t numNodes; ///> How many total nodes there are
  size_t nodeId; ///> The node id of this node

  std::shared_ptr<csrType> csr; ///> Compressed Sparse Row graph
  std::shared_ptr<cscType> csc; ///> Compressed Sparse column graph
  std::vector<QueryType> queries; ///> The list of queries to run.
  
  /// Keeps track of how many consume threads are active.
  std::atomic<size_t> consumeThreadsActive; 
  
  size_t currentFuture = 0;
  std::future<bool> futures[MAX_NUM_FUTURES];

  bool cycled = false;

  void processRequestAgainstGraph(EdgeRequestType const& edgeRequest);
  
  /**
   * This goes through the list of new edge requests and sends them out to
   * the appropriate nodes.
   */
  size_t processEdgeRequests(std::list<EdgeRequestType> const& edgeRequests);

  /**
   * Sends the edge request out.  Uses the address function to determine
   * which node to send the request to.
   */
  void sendEdgeRequest(EdgeRequestType const& edgeRequest,
    std::function<size_t(EdgeRequestType const&)> addressFunction);

  double keepQueries = 1;
  std::random_device rd;
  std::mt19937 myRand;
  std::uniform_real_distribution<> dist;

  std::shared_ptr<FeatureMap> featureMap;

public:

  /**
   * Constructor.
   * There are two types of sockets we set up.  There is a set of sockets
   * to push and pull edge description requests.  There is a another set
   * of sockets to process the edges being transferred.
   *
   * \param numNodes The number of nodes in the cluster
   * \param nodeId The id of this node.
   * \param hostnames A vector of all the hostnames in the cluster.
   * \param startingPort Port number to start from.  Ports are create 
   *   sequentially from this port.  Includes both the edge communicator
   *   and the request communicator.
   * \param hwm The highwater mark.
   * \param graphCapacity The number of bins in the graph representation.
   * \param tableCapacity The number of bins in the SubgraphQueryResultMap.
   * \param resultsCapacity How many completed queries can be stored in
   *          SubgraphQueryResultMap.
   * \param numPushSockets How many push sockets to talk to one node. 
   *   numPushSockets * (numNodes - 1) push sockets are created.
   * \param numPullThreads How many pull threads to create.  Each one covers
   *   a roughly equal number of pull sockets.  Like before, there are 
   *   numPushSockets * (numNodes - 1) total pull sockets.
   * \param timeout How long in milliseconds should the communicator's pull
   *  threads wait for data before exiting the pull loop.
   * \param timeWindow How long do we keep edges.
   * \param local Boolean indicating that we are on one node.
   */
  GraphStore(
             std::size_t numNodes,
             std::size_t nodeId,
             std::vector<std::string> hostnames,
             size_t startingPort,
             uint32_t hwm,
             size_t graphCapacity,
             size_t tableCapacity,
             size_t resultsCapacity,
             size_t numPushSockets,
             size_t numPullThreads,
             size_t timeout,
             double timeWindow,
             double keepQueries,
             std::shared_ptr<FeatureMap> featureMap,
             bool local=false);

  ~GraphStore();

  /**
   * Adds the tuple to the graph store.
   * \return Returns a number representing (roughly) the amount of work it 
   *   took to add the edge.
   */
  size_t addEdge(TupleType n);

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

  size_t checkSubgraphQueries(TupleType const& tuple,
                            std::list<EdgeRequestType>& edgeRequests);

  /**
   * Returns the total number of completed query results were produced.
   */
  size_t getNumResults() const { return resultMap->getNumResults(); }
  
  size_t getNumIntermediateResults() const { 
    return resultMap->getNumIntermediateResults();
  }

  ResultType getResult(size_t index) const {
    return resultMap->getResult(index);
  }



  /**
   * Returns the total number of edges sent by the graphstore and the request
   * map.
   */
  size_t getTotalEdgePushes() {
    return edgeCommunicator->getTotalMessagesSent();
  }
 
  /**
   * Returns how many tuples the graph store has received through
   * edge pulls.
   */
  size_t getTotalEdgePulls() const { 
    return edgeCommunicator->getTotalMessagesReceived(); 
  }

  /**
   * Returns the total number of times that send messages on the edge push
   * sockets failed.
   */
  size_t getTotalEdgePushFails() { 
    return edgeCommunicator->getTotalMessagesFailed(); 
  }
 
  /**
   * Returns the total number of edge requests that this nodes has issued.
   */
  size_t getTotalRequestPushes() {
    return requestCommunicator->getTotalMessagesSent();
  }

  /**
   * Returns the total number of edge requests that this nodes has pulled
   * via the zmq pull sockets.
   */
  size_t getTotalRequestPulls() {
    return requestCommunicator->getTotalMessagesReceived();
  }

  /**
   * Returns the total number of times that send messages on the request
   * push sockets failed.
   */
  size_t getTotalRequestPushFails() { 
    return requestCommunicator->getTotalMessagesFailed(); 
  }

  #ifdef METRICS
  /**
   * Returns the number of edge map pushes
   */
  size_t getTotalEdgeRequestMapPushes() {
    return edgeRequestMap->getTotalEdgePushes();
  }

  /**
   * Returns how many push fails occurred for the EdgeRequestMap.
   */
  size_t getTotalEdgeRequestMapPushFails() {
    return edgeRequestMap->getTotalEdgePushFails();
  }

  /**
   * Returns how many edge requests were viewed by the EdgeRequestMap.
   */
  size_t getTotalEdgeRequestMapRequestsViewed() {
    return edgeRequestMap->getTotalEdgeRequestsViewed();
  }
  #endif

  #ifdef TIMING
  double getTotalTimeConsume() const { return totalTimeConsume; }
  #endif

  #ifdef DETAIL_TIMING

  /**
   * Returns how long the edgeRequestMap spent sending data to a push
   * socket.
   */
  double getTotalTimeEdgeRequestMapPush() const {
    return edgeRequestMap->getTotalTimePush();
  }

  /**
   * Returns how long the edgeRequestMap spent waiting for a lock
   * to an entry of the ale.
   */
  double getTotalTimeEdgeRequestMapLock() const {
    return edgeRequestMap->getTotalTimeLock();
  }

  double getTotalTimeConsumeAddEdge() const {
    return totalTimeConsumeAddEdge;
  }
  double getTotalTimeConsumeResultMapProcess() const {
    return totalTimeConsumeResultMapProcess;
  }
  double getTotalTimeConsumeEdgeRequestMapProcess() const {
    return totalTimeConsumeEdgeRequestMapProcess;
  }
  double getTotalTimeConsumeCheckSubgraphQueries() const {
    return totalTimeConsumeCheckSubgraphQueries;
  }
  double getTotalTimeConsumeProcessEdgeRequests() const {
    return totalTimeConsumeProcessEdgeRequests;
  }
  double getTotalTimeEdgeCallbackProcessEdgeRequests() const {
    return totalTimeEdgeCallbackProcessEdgeRequests;
  }
  double getTotalTimeEdgeCallbackResultMapProcess() const {
    return totalTimeEdgeCallbackResultMapProcess;
  }
  double getTotalTimeRequestCallbackAddRequest() const {
    return totalTimeRequestCallbackAddRequest;
  } 
  double getTotalTimeRequestCallbackProcessAgainstGraph() const {
    return totalTimeRequestCallbackProcessAgainstGraph;
  }

  /**
   * Returns the total time spent checking against the graph in
   * result map processing.
   */
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
  double getTotalTimeProcessProcessAgainstGraph() const {
    return resultMap->getTotalTimeProcessProcessAgainstGraph();
  }
  double getTotalTimeProcessLoop1() const {
    return resultMap->getTotalTimeProcessLoop1();
  }
  double getTotalTimeProcessLoop2() const {
    return resultMap->getTotalTimeProcessLoop2();
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
size_t 
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
addEdge(TupleType tuple) 
{
  DEBUG_PRINT("Node %lu entering GraphStore::addEdge tuple %s\n", nodeId, 
    sam::toString(tuple).c_str());
  //std::lock_guard<std::mutex> lock(generalLock);
  size_t workCsc = csc->addEdge(tuple);
  size_t workCsr = csr->addEdge(tuple);
  DEBUG_PRINT("Node %lu exiting GraphStore::addEdge tuple %s\n", nodeId, 
    sam::toString(tuple).c_str());
  return workCsc + workCsr;
}


template <typename TupleType, typename Tuplizer, 
          size_t source, size_t target, 
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF, 
          typename SourceEF, typename TargetEF> 
size_t 
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
checkSubgraphQueries(TupleType const& tuple,
                     std::list<EdgeRequestType>& edgeRequests) 
{
  DEBUG_PRINT("Node %lu GraphStore::checkSubgraphQueries tuple %s "
    " numQueries %lu\n",
    nodeId, sam::toString(tuple).c_str(), queries.size()); 

  size_t totalWork = 0;

  // The start time of the query result is the time field of the first 
  // tuple in the query.  
  double startTime = std::get<time>(tuple);
  for (QueryType const& query : queries) {
    totalWork++;
    if (query.satisfies(tuple, 0, startTime)) {

      // We only want one node to own the query result, so we make sure
      // that this node owns the source
      SourceType src = std::get<source>(tuple);
      
      DEBUG_PRINT("Node %lu GraphStore::checkSubgraphQueries src %s "
        "soruceHash(src) %llu numNodes %lu sourceHash(src) mod numNodes %llu\n",
        nodeId, src.c_str(), sourceHash(src), numNodes, 
        sourceHash(src) % numNodes);

      if (sourceHash(src) % numNodes == nodeId) {
        ResultType queryResult(&query, tuple, featureMap);

        DEBUG_PRINT("Node %lu GraphStore::checkSubgraphQueries adding"
          " queryResult %s from tuple %s\n", this->nodeId, 
          queryResult.toString().c_str(), toString(tuple).c_str());

        resultMap->add(queryResult, *csr, *csc, edgeRequests);  
        
        DEBUG_PRINT("Node %lu GraphStore::checkSubgraphQueries added"
          " queryResult %s for tuple %s.  EdgeRequests.size() %lu\n", 
          this->nodeId, 
          queryResult.toString().c_str(), toString(tuple).c_str(), 
          edgeRequests.size());
      } else {

        DEBUG_PRINT("Node %lu GraphStore::checkSubgraphQueries this node "
          "didn't own source in %s\n", this->nodeId, 
          sam::toString(tuple).c_str());
      }
    } else {
      DEBUG_PRINT("Node %lu GraphStore::checkSubgraphQueries tuple %s"
        " didn't satisfy query %s\n", this->nodeId, 
        sam::toString(tuple).c_str(), query.toString().c_str());
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
  return totalWork;
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
size_t
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
processEdgeRequests(std::list<EdgeRequestType> const& edgeRequests)
{
  //std::lock_guard<std::mutex> lock(generalLock);
  DEBUG_PRINT("Node %lu GraphStore::processEdgeRequests() there are %lu "
    "edge requests\n", this->nodeId, edgeRequests.size());
  
  // Don't want to issue more edge requests if we've been terminated.
  if (!terminated) {
    
    for(auto edgeRequest : edgeRequests) {

      DEBUG_PRINT("Node %lu GraphStore::processEdgeRequests() processing"
        " edgeRequest %s\n", this->nodeId, edgeRequest.toString().c_str());

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
        sendEdgeRequest(edgeRequest, targetAddressFunction);
      }
      else 
      if (isNull(edgeRequest.getTarget()) && !isNull(edgeRequest.getSource()))
      {
        //If the source is not null but the target is, we send the edge request
        //to whomever owns the source.
        sendEdgeRequest(edgeRequest, sourceAddressFunction);
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
          sendEdgeRequest(edgeRequest, sourceAddressFunction);
        } else {
          sendEdgeRequest(edgeRequest, targetAddressFunction);
        }
      }
    }
  } else {
    DEBUG_PRINT("Node %lu GraphStore::processEdgeRequests() there are %lu "
      "edge requests but terminated\n", nodeId, edgeRequests.size());
  }
  DEBUG_PRINT("Node %lu end of GraphStore::processEdgeRequests processed %lu "
    "requests \n", this->nodeId, edgeRequests.size());
  return edgeRequests.size();
}

template <typename TupleType, typename Tuplizer,
          size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void
GraphStore<TupleType, Tuplizer, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
sendEdgeRequest(EdgeRequestType const& edgeRequest,
  std::function<size_t(EdgeRequestType const&)> addressFunction)
{
  std::string message = edgeRequest.serialize();
  size_t node = addressFunction(edgeRequest);

  bool sent = requestCommunicator->send(message, node);

  if (!sent) { 
    printf("Node %lu->%lu GraphStore::sendEdgeRequest failed"
      " sending EdgeRequest: %s\n",
      nodeId, node, edgeRequest.toString().c_str()); 
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
  DEBUG_PRINT("Node %lu GraphStore::consume processing tuple %s\n",
    nodeId, sam::toString(tuple).c_str());

  DEBUG_PRINT("Node %lu GraphStore::consume about to launch async (total"
    " asnyc threads right now %lu) for tuple %s\n",
    nodeId, consumeThreadsActive.load(), toString(tuple).c_str());
  if (cycled) {
    futures[currentFuture].get();
  }

  futures[currentFuture] = std::async(std::launch::async, [this, &tuple]() {
    return this->consumeDoesTheWork(tuple);
  });
  currentFuture++;
  //auto _ = std::async(std::launch::async, [this, &tuple]() {
  // return this->consumeDoesTheWork(tuple);
  //});
  if (currentFuture >= MAX_NUM_FUTURES) {
    cycled = true;
    currentFuture = 0;
  }


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
  size_t previousCount = consumeThreadsActive.fetch_add(1);
  size_t warningLimit = 32;
  if (previousCount > warningLimit) {
    DEBUG_PRINT("Node %lu has %lu warning active consume threads\n", 
      nodeId, previousCount);
  }

  size_t totalWork = 0;

  #ifdef TIMING
  auto timestamp_consume1 = std::chrono::high_resolution_clock::now();
  #endif

  TupleType myTuple = tuple;
  // Give the tuple a new id
  std::get<0>(myTuple) = idGenerator.generate();

  DEBUG_PRINT("Node %lu GraphStore::consumeDoesTheWork tuple %s\n", nodeId, 
    sam::toString(myTuple).c_str());


  // Adds the edge to the graph
  DETAIL_TIMING_BEG1
  size_t workAddEdge = addEdge(myTuple);
  DETAIL_TIMING_END_TOL1(nodeId, totalTimeConsumeAddEdge, TOLERANCE, 
                     "GraphStore::consumeDoesTheWork addEdge")

  // Check against existing queryResults.  The edgeRequest list is populated
  // with edge requests when we find we need a tuple that will reside 
  // elsewhere.
  DETAIL_TIMING_BEG2
  std::list<EdgeRequestType> edgeRequests;
  //resultMapLock.lock();
  size_t workResultMapProcess = 
    resultMap->process(myTuple, *csr, *csc, edgeRequests);
  //resultMapLock.unlock();
  DETAIL_TIMING_END_TOL2(nodeId, totalTimeConsumeResultMapProcess,  TOLERANCE,
                     "GraphStore::consumeDoesTheWork resultMap->process")

  // See if anybody needs this tuple and send it out to them.
  DETAIL_TIMING_BEG2
  size_t workEdgeRequestMap = edgeRequestMap->process(myTuple);
  DETAIL_TIMING_END_TOL2(nodeId, totalTimeConsumeEdgeRequestMapProcess, 
    TOLERANCE, "GraphStore::consumeDoesTheWork edgeRequestMap->process")

  // Check against all registered queries
  
  size_t workCheckSubgraphQueries = 0;
  if (dist(myRand) < keepQueries) {

    DETAIL_TIMING_BEG2
    workCheckSubgraphQueries = checkSubgraphQueries(myTuple, edgeRequests);
    DETAIL_TIMING_END_TOL2(nodeId, totalTimeConsumeCheckSubgraphQueries, 
      TOLERANCE, "GraphStore::consumeDoesTheWork checkSubgraphQueries")
  }

  // Send out the edge requests to the other nodes.
  DETAIL_TIMING_BEG2
  size_t workProcessEdgeRequests = processEdgeRequests(edgeRequests);
  DETAIL_TIMING_END_TOL2(nodeId, totalTimeConsumeProcessEdgeRequests, TOLERANCE,
                     "GraphStore::consumeDoesTheWork processEdgeRequests")

  #ifdef TIMING
  auto timestamp_consume2 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> time_space = 
    std::chrono::duration_cast<std::chrono::duration<double>>(
      timestamp_consume2 - timestamp_consume1);
  double time_consume = time_space.count(); 
  totalTimeConsume += time_consume;

  //#ifdef DETAIL_TIMING
  //consumeTimes.push_back(time_consume);
  //#endif

  //#ifdef DETAIL_METRICS
  //totalWork = workAddEdge + workResultMapProcess + workEdgeRequestMap + 
  //            workCheckSubgraphQueries + workProcessEdgeRequests;
  //printf("Node %lu GraphStore::consumeDoesTheWork DETAIL_METRICS "
  //       "workAddEdge %lu "
  //       "workResultMapProcess %lu "
  //       "workEdgeRequestMap %lu "
  //       "workCheckSubgraphQueries %lu "
  //       "workProcessEdgeRequests %lu total work %lu time %f\n", 
  //       nodeId, workAddEdge, workResultMapProcess,
  //       workEdgeRequestMap, workCheckSubgraphQueries,
  //       workProcessEdgeRequests, totalWork, time_consume);
  //#endif
  
  #endif

  DEBUG_PRINT("Node %lu exiting GraphStore::consumeDoesTheWork\n", nodeId);

  consumeThreadsActive.fetch_add(-1);
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
  printf("Node %lu entering GraphStore::terminate consumeThreadsActive "
    " %lu\n", nodeId, consumeThreadsActive.load());
  if (!terminated) {  

    terminated = true;

    /*futuresLock.lock();
    for (size_t i = 0; i < MAX_NUM_FUTURES; i++) {
       printf("Node %lu i %lu MAX_NUM_FUTURES %lu\n", nodeId, i, 
         MAX_NUM_FUTURES);
       try {
         futures[i].get();
       } catch (std::system_error e) {
         printf("Caught exception with future %s\n", e.what());
       }
    }
    futuresLock.unlock();*/

    // If terminate was called, we aren't going to receive any more
    // edges, so we can push out the terminate signal to all the edge request
    // channels. 
    /*for(size_t i = 0; i < numNodes; i++) {
      if (i != nodeId) {

        printf("Node %lu GraphStore::terminate sending terminate message"
          " to request pusher %lu\n", this->nodeId, i);

        bool sent = false;
        while (!sent) {
          requestPushMutexes[i].lock();
          sent = requestPushers[i]->send(terminateZmqMessage());
          requestPushMutexes[i].unlock();
          if (!sent) {
            printf("Node %lu->%lu GraphStore::terminate failed to send"
              " terminate message to request pusher\n", this->nodeId, i);
          }
        }
      }
    }*/

    requestCommunicator->terminate();

    printf("Node %lu requestCommunicator terminated\n", nodeId);
 
    // The EdgeRequestMap has the edge communicator.  We call terminate
    // on the EdgeRequestMap to send out the terminate message.
    edgeRequestMap->terminate();

    printf("Node %lu edgeRequestMap joined\n", nodeId);
  }
  printf("Node %lu exiting GraphStore::terminate\n", nodeId);
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
GraphStore(  
             std::size_t numNodes,
             std::size_t nodeId,
             std::vector<std::string> hostnames,
             size_t startingPort,
             uint32_t hwm,
             std::size_t graphCapacity, //How many bins for csr and csc
             std::size_t tableCapacity, //For SubgraphQueryResultMap
             std::size_t resultsCapacity, //For SubgraphQueryResultMap
             size_t numPushSockets,
             size_t numPullThreads,
             size_t timeout,
             double timeWindow,
             double keepQueries,
             std::shared_ptr<FeatureMap> featureMap,
             bool local)
{
  this->featureMap = featureMap;

  sourceAddressFunction = [this](EdgeRequestType const& edgeRequest) {
    SourceType src = edgeRequest.getSource();
    size_t node = sourceHash(src) % this->numNodes;
    return node;
  };

  targetAddressFunction = [this](EdgeRequestType const& edgeRequest) {
    TargetType trg = edgeRequest.getSource();
    size_t node = targetHash(trg) % this->numNodes;
    return node;
  };

  terminated = false;
  this->numNodes = numNodes;
  this->nodeId   = nodeId;

  edgePushCounter = 0;
  edgePushFails = 0;
  consumeThreadsActive = 0;

  resultMap = 
    std::make_shared< ResultMapType>( numNodes, nodeId, 
      tableCapacity, resultsCapacity);

  csr = std::make_shared<csrType>(graphCapacity, timeWindow); 
  csc = std::make_shared<cscType>(graphCapacity, timeWindow); 

  typedef PushPull::FunctionType FunctionType;

  auto edgeCallback = [this](std::string str) 
  {
    // We give the edge a new id that is unique to this node.
    size_t id = idGenerator.generate();

    // Change the string into the expected tuple type.
    TupleType tuple = tuplizer(id, str);

    DEBUG_PRINT("Node %lu GraphStore::edgeCallback received a"
      " tuple %s\n", this->nodeId, sam::toString(tuple).c_str());

    // Do we need to do this?
    // Add the edge to the graph
    //addEdge(tuple);

    DEBUG_PRINT("Node %lu GraphStore::edgeCallback added edge %s\n",
      this->nodeId, sam::toString(tuple).c_str());

    DETAIL_TIMING_BEG1
    // Process the new edge over results and see if it satifies
    // queries.  If it does, there may be new edge requests.
    std::list<EdgeRequestType> edgeRequests;
    //resultMapLock.lock();
    resultMap->process(tuple, *csr, *csc, edgeRequests);
    //resultMapLock.unlock();
    DETAIL_TIMING_END_TOL1(this->nodeId, totalTimeEdgeCallbackResultMapProcess, 
      TOLERANCE, "GraphStore::edgeCallback resultMap->process")

    DEBUG_PRINT("Node %lu GraphStore::edgeCallback processed"
      " edge %s\n", this->nodeId, sam::toString(tuple).c_str());

    // Send out the edge requests to the other nodes.
    DETAIL_TIMING_BEG2
    processEdgeRequests(edgeRequests);
    DETAIL_TIMING_END_TOL2(this->nodeId, 
      totalTimeEdgeCallbackProcessEdgeRequests, TOLERANCE, 
      "GraphStore::edgeCallbackk processEdgeRequests")
  };

  std::vector<FunctionType> edgeCommunicatorFunctions;
  edgeCommunicatorFunctions.push_back(edgeCallback);

  edgeCommunicator = new PushPull(numNodes, nodeId, numPushSockets,
                                  numPullThreads, hostnames, hwm,
                                  edgeCommunicatorFunctions,
                                  startingPort, timeout, local); 

  size_t newStartingPort;
  if (local) {
    newStartingPort = startingPort + (numPushSockets * (numNodes-1)) * numNodes;
  } else {
    newStartingPort = edgeCommunicator->getLastPort() + 1;
  }

  edgeRequestMap = std::make_shared< RequestMapType>( 
    numNodes, nodeId, tableCapacity, edgeCommunicator);

  auto requestCallback = [this](std::string str)
  {
      
    // When we get an edge request, we need to check against
    // the graph (existing matches) and add it to the list 
    // so that any new matches are caught.  Since there are
    // two other threads that add to the graph 
    // (the edgeRequest pull thread and the consume thread),
    // we need a lock to make sure we don't miss edges are add
    // edge multiple times. 
    
    //generalLock.lock();

    EdgeRequestType request(str);
    DEBUG_PRINT("Node %lu GraphStore::requestCallback received an edge request"
      " length = %lu: %s %s\n", this->nodeId, str.size(), str.c_str(),
      request.toString().c_str());

    DETAIL_TIMING_BEG1
    edgeRequestMap->addRequest(request);
    DETAIL_TIMING_END_TOL1(this->nodeId, 
      totalTimeRequestCallbackAddRequest, 
      TOLERANCE, "GraphStore::requestCallback edgeRequestMap->addRequest")
    DEBUG_PRINT("Node %lu RequestPullThread added edge request to map"
      ": %s\n", this->nodeId, request.toString().c_str());
      

    DETAIL_TIMING_BEG2
    processRequestAgainstGraph(request);
    DETAIL_TIMING_END_TOL2(this->nodeId, 
      totalTimeRequestCallbackProcessAgainstGraph, 
      TOLERANCE, "GraphStore::requestCallback processRequestAgainstGraph")
    DEBUG_PRINT("Node %lu RequestPullThread processed edge request"
      " against graph: %s\n", this->nodeId, request.toString().c_str());
      

    DEBUG_PRINT("Node %lu RequestPullThread processed edge request"
      ": %s\n", this->nodeId, request.toString().c_str());
      
  };

  std::vector<FunctionType> requestCommunicatorFunctions;
  requestCommunicatorFunctions.push_back(requestCallback);

  requestCommunicator = new PushPull(numNodes, nodeId, numPushSockets,
                                     numPullThreads, hostnames, hwm,
                                     requestCommunicatorFunctions,
                                     newStartingPort, timeout, local);

  this->keepQueries = keepQueries;
  myRand = std::mt19937(rd());
  dist = std::uniform_real_distribution<>(0.0, 1.0);
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

  delete requestCommunicator;
  delete edgeCommunicator;

  DEBUG_PRINT("Node %lu end of ~GraphStore\n", nodeId);
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
  DEBUG_PRINT("Node %lu GraphStore::processRequestAgainstGraph edgeRequest"
    " %s\n", nodeId, edgeRequest.toString().c_str());

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
    
    DEBUG_PRINT("Node %lu GraphStore::processRequestAgainstGraph looking up "
      " edge request %s against csr because source is not null\n",
      nodeId, edgeRequest.toString().c_str());

    csr->findEdges(edgeRequest, foundEdges);

  } else if (isNull(src) && !isNull(trg)) {
    // The target is not null, so we look up by the target using the 
    // compressed sparse column graph.  

    DEBUG_PRINT("Node %lu GraphStore::processRequestAgainstGraph looking up "
      " edge request %s against csc because target is not null\n",
      nodeId, edgeRequest.toString().c_str());

    csc->findEdges(edgeRequest, foundEdges);

  } else if (!isNull(src) && !isNull(trg)) {
    // Doesn't matter which one we look up, so look it up in csr

    DEBUG_PRINT("Node %lu GraphStore::processRequestAgainstGraph looking up "
      " edge request %s against csr because source and target are not null\n",
      nodeId, edgeRequest.toString().c_str());
    
    csr->findEdges(edgeRequest, foundEdges);
  } else {
    throw GraphStoreException("Tried to process an edge request but both " 
      "the source and target were null");
  }

  size_t node = edgeRequest.getReturn();

  DEBUG_PRINT("Node %lu GraphStore::processRequestAgainstGraph found"
    " %lu edges\n", nodeId, foundEdges.size());

  for (auto edge : foundEdges) {
    SourceType src = std::get<source>(edge);
    TargetType trg = std::get<target>(edge);
    size_t srcHash = sourceHash(src) % numNodes;
    size_t trgHash = targetHash(trg) % numNodes;

    // Only send the message of the node won't get the message anyway.
    if (srcHash != node && trgHash != node) {

      std::string message = toString(edge);
      //zmq::message_t message = tupleToZmq(edge);

      double placeholder = 0;
      DETAIL_TIMING_BEG1
      DETAIL_TIMING_END_TOL1(nodeId, placeholder, TOLERANCE, 
        "GraphStore::processRequestAgainstGraph obtaining lock exceeded "
        "tolerance")
      if (!terminated) {
        DEBUG_PRINT("Node %lu->%lu GraphStore::processRequestAgainstGraph"
          " sending edge %s\n", nodeId, node, message.c_str());
        DETAIL_TIMING_BEG1
        //#ifdef NOBLOCK
        //bool sent = edgePushers[node]->send(message, ZMQ_NOBLOCK);     
        //if (!sent) { 
        //  edgePushFails.fetch_add(1);
        //  edgePushCounter.fetch_add(-1);
        //  DEBUG_PRINT("Node %lu->%lu GraphStore::processRequestAgainstGraph"
        //    " failed sending edge: %s\n",
        //    nodeId, node, sam::toString(edge).c_str()); 
        //}
        //#elif defined NOBLOCK_WHILE
        //bool sent = false;
        //while(!sent) {
        //  sent = edgePushers[node]->send(message, ZMQ_NOBLOCK);
        //}
        //#else
        bool sent = edgeCommunicator->send(message, node);
        if (!sent) { 
          edgePushFails.fetch_add(1);
          DEBUG_PRINT("Node %lu->%lu GraphStore::processRequestAgainstGraph"
            " failed sending edge: %s\n", nodeId, node, message.c_str()); 
            
        } else {
          edgePushCounter.fetch_add(1);
        }
        //#endif
        DETAIL_TIMING_END_TOL1(nodeId, placeholder, TOLERANCE, 
          "GraphStore::processRequestAgainstGraph sending message exceeded "
          "tolerance")
      }
    }
  }
}

} // end namespace sam

#endif

