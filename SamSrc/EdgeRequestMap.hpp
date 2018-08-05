#ifndef SAM_EDGE_REQUEST_MAP_HPP
#define SAM_EDGE_REQUEST_MAP_HPP

#include <atomic>
#include <zmq.hpp>
#include <boost/lexical_cast.hpp>
#include "EdgeRequest.hpp"
#include "Null.hpp"
#include "Util.hpp"
#include "TemporalSet.hpp"
#include "ZeroMQUtil.hpp"

namespace sam {

class EdgeRequestMapException : public std::runtime_error {
public:
  EdgeRequestMapException(char const * message) : 
    std::runtime_error(message) { }
  EdgeRequestMapException(std::string message) : 
    std::runtime_error(message) { }
};

/**
 * This class has a list of edge requests that have been made of a node.
 * We store them in a hash table where each entry in the hash table has a 
 * mutex lock.  Each entry is a list of edge requests that hash to the
 * same location.
 *
 * When process(tuple) is called, we find if there are any matching edge 
 * requests.  If so, we send the tuple to the appropriate node(s).
 */
template <typename TupleType, size_t source, size_t target, size_t time,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
class EdgeRequestMap
{
public:
  typedef EdgeRequest<TupleType, source, target> EdgeRequestType;
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;
  typedef TemporalSet<size_t, double> TemporalSetType;

private:
  SourceHF sourceHash;
  TargetHF targetHash;
  SourceEF sourceEquals;
  TargetEF targetEquals;

  size_t numNodes;
  size_t nodeId;

  /// The size of the hash table storing the edge requests.
  size_t tableCapacity;

  /// An array of lists of edge requests
  std::list<EdgeRequestType> *ale;

  /// mutexes for each array element of ale.
  std::mutex* mutexes;

  PushPull* edgeCommunicator;

  /// Used to keep the edge push sockets thread safe.
  //std::mutex* edgePushMutexes;

  /// Keeps track of which edges have already been sent
  //std::vector<std::shared_ptr<TemporalSetType>> seenEdges;

  /// Keeps track of how many edges we send
  std::atomic<size_t> edgePushCounter;

  //std::atomic<size_t> savedEdgePushes;

  /// This has all the push sockets that we use to send edges to other nodes.
  std::vector<std::shared_ptr<zmq::socket_t>> pushers;

  std::function<size_t(TupleType const&)> sourceIndexFunction;
  std::function<bool(EdgeRequestType const&, TupleType const&)> 
    sourceCheckFunction;
  std::function<size_t(TupleType const&)> targetIndexFunction;
  std::function<bool(EdgeRequestType const&, TupleType const&)> 
    targetCheckFunction;
  std::function<size_t(TupleType const&)> sourceTargetIndexFunction;
  std::function<bool(EdgeRequestType const&, TupleType const&)> 
    sourceTargetCheckFunction;

  std::atomic<size_t> sendFails; ///> How many pushes fail

public:
  /**
   * Constructor.  
   */
   EdgeRequestMap(std::size_t numNodes,
                  std::size_t nodeId,
                  size_t tableCapacity,
                  PushPull* edgeCommunicator);

  /**
   * Destructor.
   */
  ~EdgeRequestMap();

  /**
   * Add a request to the list.  This is called by the requestPullThread of
   * the GraphStore class.  
   */
  void addRequest(EdgeRequestType request);

  /**
   * Given the tuple, finds if there are any open edge requests that are 
   * satisfied with the given tuple. If so, sends them on to the appropriate
   * node using the push sockets. 
   * \return Returns a number representing the amount of work done.
   */
  size_t process(TupleType const& tuple);


  /**
   * Returns how many edges we've sent
   */
  size_t getTotalEdgePushes() { return edgePushCounter; }

  size_t getTotalEdgePushFails() { return sendFails; }

  /**
   * Iterates through the edge push sockets and sends a terminate
   * signal.
   */
  void terminate();

private:

  size_t process(TupleType const& tuple,
        std::function<size_t(TupleType const&)> indexFunction,
        std::function<bool(EdgeRequestType const&, TupleType const&)> 
          checkFunction);


  #ifdef DETAIL_TIMING
  double totalTimePush = 0;
  #endif

  std::atomic<bool> terminated;
};

// Constructor 
template <typename TupleType, size_t source, size_t target, size_t time,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
EdgeRequestMap<TupleType, source, target, time,
  SourceHF, TargetHF, SourceEF, TargetEF>::
EdgeRequestMap( std::size_t numNodes,
                std::size_t nodeId,
                size_t tableCapacity,
                PushPull* edgeCommunicator)
{
  this->edgeCommunicator = edgeCommunicator;

  sendFails = 0;

  sourceIndexFunction = [this](TupleType const& tuple) {
    SourceType src = std::get<source>(tuple);
    return sourceHash(src) % this->tableCapacity;
  };

  targetIndexFunction = [this](TupleType const& tuple) {
    TargetType trg = std::get<target>(tuple);
    return targetHash(trg) % this->tableCapacity;
  };
  
  sourceTargetIndexFunction = [this](TupleType const& tuple) {
    SourceType src = std::get<source>(tuple);
    TargetType trg = std::get<target>(tuple);
    return (sourceHash(src) * targetHash(trg)) % this->tableCapacity;
  };

  sourceCheckFunction = [this](EdgeRequestType const& edgeRequest,
                               TupleType const& tuple) 
  {
    SourceType src = std::get<source>(tuple);
    TargetType trg = std::get<target>(tuple);
    SourceType edgeRequestSrc = edgeRequest.getSource();
    if (this->sourceEquals(src, edgeRequestSrc)) {
      
      size_t node = edgeRequest.getReturn();
      // TODO: Partition info
      if (this->targetHash(trg) % this->numNodes != node) {
        return true;
      }
    }
    return false;
  };

  targetCheckFunction = [this](EdgeRequestType const& edgeRequest,
                               TupleType const& tuple) 
  {
    SourceType src = std::get<source>(tuple);
    TargetType trg = std::get<target>(tuple);
    TargetType edgeRequestTrg = edgeRequest.getTarget();
    DEBUG_PRINT("Node %lu EdgeRequestMap::targetCheckFunction trg %s "
      "edgeRequestTrg %s\n", this->nodeId, trg.c_str(), edgeRequestTrg.c_str());
    if (this->targetEquals(trg, edgeRequestTrg)) {
      
      size_t node = edgeRequest.getReturn();
      DEBUG_PRINT("Node %lu EdgeRequestMap::targetCheckFunction "
        "sourceHash(src) mod numNodes  %llu node %lu\n", 
        this->nodeId, sourceHash(src) % this->numNodes, node);
      // TODO: Partition info
      if (this->sourceHash(src) % this->numNodes != node) {
        DEBUG_PRINT("Node %lu targetCheckFunction returning true\n",
          this->nodeId);
        return true;
      }
    }
    return false;
  };

  sourceTargetCheckFunction = [this](EdgeRequestType const& edgeRequest,
                                     TupleType const& tuple)
  {
    SourceType src = std::get<source>(tuple);
    TargetType trg = std::get<target>(tuple);
    TargetType edgeRequestTrg = edgeRequest.getTarget();
    SourceType edgeRequestSrc = edgeRequest.getSource();
    if (targetEquals(trg, edgeRequestTrg) &&
        sourceEquals(src, edgeRequestSrc)) 
    {
      size_t node = edgeRequest.getReturn();

      // TODO: Partition info
      if (sourceHash(src) % this->numNodes != node &&
          targetHash(trg) % this->numNodes != node)
      {
        return true;
      }
    }
    return false;
  };

 
  terminated = false;
  this->numNodes = numNodes;
  this->nodeId = nodeId;
  this->tableCapacity = tableCapacity;
  mutexes = new std::mutex[tableCapacity];
  ale = new std::list<EdgeRequestType>[tableCapacity];
  edgePushCounter = 0;

}

// Destructor 
template <typename TupleType, size_t source, size_t target, size_t time,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
EdgeRequestMap<TupleType, source, target, time,
  SourceHF, TargetHF, SourceEF, TargetEF>::
~EdgeRequestMap()
{
  delete[] mutexes;
  delete[] ale;
  terminate();
  DEBUG_PRINT("Node %lu end of ~EdgeRequestMap\n", nodeId);
}



template <typename TupleType, size_t source, size_t target, size_t time,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void
EdgeRequestMap<TupleType, source, target, time,
  SourceHF, TargetHF, SourceEF, TargetEF>::
addRequest(EdgeRequestType request)
{
  SourceType src = request.getSource();
  TargetType trg = request.getTarget();

  size_t index;

  // TODO: Very similar to SubgraphQueryResult::hash.  Anyway to combine?
  if (isNull(src) && !isNull(trg))
  {
    index = targetHash(trg) % tableCapacity;
    //printf("hashing target %s index %lu edge request %s\n", 
    //trg.c_str(), index, request.toString().c_str());
  } else
  if (!isNull(src) && isNull(trg))
  {
    index = sourceHash(src) % tableCapacity;
  } else
  if (!isNull(src) && !isNull(trg))
  {
    index = (sourceHash(src) * targetHash(trg)) % tableCapacity;
  } else
  {
    throw EdgeRequestMapException("EdgeRequestMap::addRequest tried to"
      " add a request with no source or target");
  }

  mutexes[index].lock();
  ale[index].push_back(request);
  mutexes[index].unlock();

}

template <typename TupleType, size_t source, size_t target, size_t time,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
size_t
EdgeRequestMap<TupleType, source, target, time,
  SourceHF, TargetHF, SourceEF, TargetEF>::
process(TupleType const& tuple)
{
  DEBUG_PRINT("Node %lu EdgeRequestMap::process(tuple) tuple: %s\n", nodeId,
    toString(tuple).c_str());
 
  size_t totalWork = 0; 
  totalWork += process(tuple, sourceIndexFunction, sourceCheckFunction);
  totalWork += process(tuple, targetIndexFunction, targetCheckFunction);
  totalWork += process(tuple, sourceTargetIndexFunction, 
                       sourceTargetCheckFunction);
  return totalWork;
}


template <typename TupleType, size_t source, size_t target, size_t time,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
size_t
EdgeRequestMap<TupleType, source, target, time,
  SourceHF, TargetHF, SourceEF, TargetEF>::
process(TupleType const& tuple,
        std::function<size_t(TupleType const&)> indexFunction,
        std::function<bool(EdgeRequestType const&, TupleType const&)> 
          checkFunction)
{
  size_t index = indexFunction(tuple);
  size_t edgeId = std::get<0>(tuple);

  double currentTime = std::get<time>(tuple);

  // To prevent duplicates being sent, we keep track of which nodes has seen
  // the tuple already.
  bool sentEdges[numNodes];
  for (size_t i = 0; i < numNodes; i++) sentEdges[i] = false;

  size_t countSentEdges = 0;

  mutexes[index].lock();
  size_t count = 0;

  DEBUG_PRINT("Node %lu EdgeRequestMap::process number of requests to look at"
    " %lu processing tuple %s\n", nodeId, ale[index].size(), 
    toString(tuple).c_str());

  for(auto edgeRequest = ale[index].begin();
        edgeRequest != ale[index].end();)
  {

    DEBUG_PRINT("Node %lu EdgeRequestMap::process looking at edgeRequest %s "
      " processing tuple %s\n", nodeId, edgeRequest->toString().c_str(), 
      toString(tuple).c_str());
     
    // Deleting edge requests that are no longer valid because the request
    // is too old. 
    if (edgeRequest->isExpired(currentTime)) {

      DEBUG_PRINT("Node %lu EdgeRequestMap::process deleting old edgeRequest"
        " %s currentTime %f\n", nodeId, edgeRequest->toString().c_str(), 
        currentTime);
      
      edgeRequest = ale[index].erase(edgeRequest);
    } else {

      count++;
      if(checkFunction(*edgeRequest, tuple)) {
        
        size_t node = edgeRequest->getReturn();
        
        if (!sentEdges[node]) {

          if (!terminated) {
           
            DETAIL_TIMING_BEG1
            std::string message = toString(tuple);
            //zmq::message_t message = tupleToZmq(tuple);
            DETAIL_TIMING_END_TOL1(nodeId, totalTimePush, 0.001, 
              "EdgeRequestMap::process creating message exceeded tolerance")
            
            DEBUG_PRINT("Node %lu->%lu EdgeRequestMap::process sending"
              " edge %s\n", nodeId, node, toString(tuple).c_str());
            
            ////// Sending tuple and checking timing /////
            DETAIL_TIMING_BEG2
            auto sendTimingBegin = std::chrono::high_resolution_clock::now();
            
            //edgePushMutexes[node].lock();
            //bool sent = pushers[node]->send(message);
            //edgePushMutexes[node].unlock();
            bool sent = edgeCommunicator->send(message, node);
            
            auto sendTimingEnd = std::chrono::high_resolution_clock::now();
            auto sendTimingDiff = 
              std::chrono::duration_cast<std::chrono::duration<double>>(
                sendTimingEnd - sendTimingBegin);
            double sendTime = sendTimingDiff.count();
            if (sendTime > 0.001) {
              printf("Node %lu->%lu EdgeRequestMap::process sending "
                " edge %s took %f\n", nodeId, node,
                toString(tuple).c_str(), sendTime);
            }
            DETAIL_TIMING_END_TOL2(nodeId, totalTimePush, 0.001, 
              "EdgeRequestMap::process sending message exceeded "
              "tolerance")
            
            //// End sending tuple
            
            sentEdges[node] = true;
            double edgeTime = std::get<time>(tuple);
            //seenEdges[node]->insert(edgeId, edgeTime);

            if (!sent) {
              printf("Node %lu->%lu EdgeRequestMap::process error sending"
                " edge %s\n", nodeId, node, toString(tuple).c_str());
              sendFails.fetch_add(1);
            } else {
              edgePushCounter.fetch_add(1);
              countSentEdges++;
            }
          }
        }
      }
      ++edgeRequest;
    }
  }

  DEBUG_PRINT("Node %lu EdgeRequestMap::process countSentEdges %lu\n",
    nodeId, countSentEdges);
  
  mutexes[index].unlock();
  return count;
}

template <typename TupleType, size_t source, size_t target, size_t time,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void
EdgeRequestMap<TupleType, source, target, time,
  SourceHF, TargetHF, SourceEF, TargetEF>::
terminate()
{
  printf("Node %lu entering EdgeRequestMap::terminate\n", nodeId);
  if (!terminated) {
    edgeCommunicator->terminate();
  }
  terminated = true;
  printf("Node %lu exiting EdgeRequestMap::terminate\n", nodeId);
}


} //End namespace sam

#endif
