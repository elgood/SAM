#ifndef EDGE_REQUEST_LIST
#define EDGE_REQUEST_LIST

#include "EdgeRequest.hpp"
#include "Null.hpp"
#include "Util.hpp"
#include <atomic>
#include <zmq.hpp>

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
template <typename TupleType, size_t source, size_t target,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
class EdgeRequestMap
{
public:
  typedef EdgeRequest<TupleType, source, target> EdgeRequestType;
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;

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

  /// Keeps track of how many edges we send
  std::atomic<size_t> edgePushCounter;

  zmq::context_t context = zmq::context_t(1);

  /// This has all the push sockets that we use to send edges to other nodes.
  std::vector<std::shared_ptr<zmq::socket_t>> pushers;

public:
  /**
   * Constructor.  
   * \param pushers Must pass in the pushers, which is what we use to 
   *  send edges to other nodes.
   * \param tableCapacity The size of the hash table.
   */
   EdgeRequestMap(
                  std::size_t numNodes,
                  std::size_t nodeId,
                  std::vector<std::string> edgeHostnames,
                  std::vector<std::size_t> edgePorts,
                  uint32_t hwm,
                  size_t tableCapacity);

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
   */
  void process(TupleType const& tuple);

  /**
   * Returns how many edges we've sent
   */
  size_t getTotalEdgePushes() { return edgePushCounter; }

  /**
   * Iterates through the edge push sockets and sends a terminate
   * signal.
   */
  void terminate() const;

private:
  /**
   * Uses the source hash function on the tuple to find any edge requests
   * that are looking for the tuple's source.
   */
  void processSource(TupleType const& tuple);


  /**
   * Uses the target hash function on the tuple to find any edge requests
   * that are looking for the tuple's target.
   */
  void processTarget(TupleType const& tuple);


  /**
   * Uses the source and target hash function on the tuple to find any 
   * edge requests that are looking for both the tuple's source and target.
   */
  void processSourceTarget(TupleType const& tuple);

};

// Constructor 
template <typename TupleType, size_t source, size_t target,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
EdgeRequestMap<TupleType, source, target,
  SourceHF, TargetHF, SourceEF, TargetEF>::
EdgeRequestMap( 
                std::size_t numNodes,
                std::size_t nodeId,
                std::vector<std::string> hostnames,
                std::vector<std::size_t> ports,
                uint32_t hwm,
                size_t tableCapacity)
{
  this->numNodes = numNodes;
  this->nodeId = nodeId;
  createPushSockets(&context, numNodes, nodeId, hostnames, ports,
                    this->pushers, hwm);
  this->tableCapacity = tableCapacity;
  mutexes = new std::mutex[tableCapacity];
  ale = new std::list<EdgeRequestType>[tableCapacity];
  edgePushCounter = 0;
}

// Destructor 
template <typename TupleType, size_t source, size_t target,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
EdgeRequestMap<TupleType, source, target,
  SourceHF, TargetHF, SourceEF, TargetEF>::
~EdgeRequestMap()
{
  delete[] mutexes;
  delete[] ale;
}



template <typename TupleType, size_t source, size_t target,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void
EdgeRequestMap<TupleType, source, target,
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

template <typename TupleType, size_t source, size_t target,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void
EdgeRequestMap<TupleType, source, target,
  SourceHF, TargetHF, SourceEF, TargetEF>::
process(TupleType const& tuple)
{
  #ifdef DEBUG
  printf("Node %lu EdgeRequestMap::process tuple %s\n", nodeId,
    toString(tuple).c_str());
  #endif
  
  processSource(tuple);
  processTarget(tuple);
  processSourceTarget(tuple);
}

template <typename TupleType, size_t source, size_t target,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void
EdgeRequestMap<TupleType, source, target,
  SourceHF, TargetHF, SourceEF, TargetEF>::
processSource(TupleType const& tuple)
{
  //printf("processSource\n");
  SourceType src = std::get<source>(tuple);
  //printf("src %s\n", src.c_str());
  size_t index = sourceHash(src) % tableCapacity;
  zmq::message_t message = tupleToZmq(tuple);

  for (auto edgeRequest : ale[index])
  {
    //printf("processSource %s\n", edgeRequest.toString().c_str());
    SourceType edgeRequestSrc = edgeRequest.getSource();
    //printf("edgeRequestSrc %s\n", edgeRequestSrc.c_str());
    if (sourceEquals(src, edgeRequestSrc)) {
      size_t node = edgeRequest.getReturn();
      edgePushCounter.fetch_add(1);
      #ifdef DEBUG
      printf("Node %lu->%lu sending edge %s\n", nodeId, node,
        toString(tuple).c_str());
      #endif
      pushers[node]->send(message);  
    }
  }
}

template <typename TupleType, size_t source, size_t target,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void
EdgeRequestMap<TupleType, source, target,
  SourceHF, TargetHF, SourceEF, TargetEF>::
processTarget(TupleType const& tuple)
{
  #ifdef DEBUG
  printf("Node %lu EdgeRequestMap::processTarget\n", nodeId);
  #endif

  TargetType trg = std::get<target>(tuple);
  size_t index = targetHash(trg) % tableCapacity;
  zmq::message_t message = tupleToZmq(tuple);
  //printf("trg %s index %lu\n", trg.c_str(), index);

  for (auto edgeRequest : ale[index])
  {
    //printf("processTarget %s\n", edgeRequest.toString().c_str());
    TargetType edgeRequestTrg = edgeRequest.getTarget();
    //printf("%s %s\n", trg.c_str(), edgeRequestTrg.c_str());
    if (targetEquals(trg, edgeRequestTrg)) {
      //printf("equals\n");
      int node = edgeRequest.getReturn();
      //printf("before send\n");
      edgePushCounter.fetch_add(1);
      //printf("trg %s edgePushCounter %llu\n", trg.c_str(), edgePushCounter.load());
      #ifdef DEBUG
      printf("Node %lu EdgeRequestMap::processTarget trg %s Sending to node "
        "%d\n", nodeId, trg.c_str(), node);
      #endif
      pushers[node]->send(message);  
      //printf("after send\n");
    }
  }
}

template <typename TupleType, size_t source, size_t target,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void
EdgeRequestMap<TupleType, source, target,
  SourceHF, TargetHF, SourceEF, TargetEF>::
processSourceTarget(TupleType const& tuple)
{
  //printf("processSourceTarget\n");
  SourceType src = std::get<source>(tuple);
  TargetType trg = std::get<target>(tuple);
  size_t index = (sourceHash(src) * targetHash(trg)) % tableCapacity;
  zmq::message_t message = tupleToZmq(tuple);

  for (auto edgeRequest : ale[index])
  {
    //printf("processSourceTarget %s\n", edgeRequest.toString().c_str());
    TargetType edgeRequestTrg = edgeRequest.getTarget();
    SourceType edgeRequestSrc = edgeRequest.getSource();
    if (targetEquals(trg, edgeRequestTrg) &&
        sourceEquals(src, edgeRequestSrc)) 
    {
      int node = edgeRequest.getReturn();
      edgePushCounter.fetch_add(1);
      pushers[node]->send(message);  
    }
  }
}

template <typename TupleType, size_t source, size_t target,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void
EdgeRequestMap<TupleType, source, target,
  SourceHF, TargetHF, SourceEF, TargetEF>::
terminate() const 
{
  for(size_t i = 0; i < this->numNodes; i++)
  {
    if (i != this->nodeId) { 

      #ifdef DEBUG
      printf("Node %lu EdgeRequestMap::terminate() sending terminate to %lu\n",
        nodeId, i); 
      #endif

      pushers[i]->send(emptyZmqMessage());
    }
  }
}


} //End namespace sam

#endif
