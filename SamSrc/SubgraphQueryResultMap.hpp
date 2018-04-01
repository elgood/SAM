#ifndef SAM_SUBGRAPH_QUERY_RESULT_MAP_HPP
#define SAM_SUBGRAPH_QUERY_RESULT_MAP_HPP

#include "SubgraphQueryResult.hpp"

namespace sam {

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
class SubgraphQueryResultMap
{
public:
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;
  typedef SubgraphQueryResult<TupleType, source, target, time,
                                       duration> QueryResultType;
  typedef EdgeRequest<TupleType, source, target> EdgeRequestType;  
  
private:
  SourceHF sourceHash;
  TargetHF targetHash;
  SourceEF sourceEquals;
  TargetEF targetEquals;
  
  /// The size of the hash table storing intermediate query results.
  size_t tableCapacity;

  /// The total number of query results that can be stored before
  /// results are overwritten.
  size_t resultCapacity;

  /// Stores completed query results.  Right now doesn't resize.
  /// Cycles around when past original size of vector.
  std::vector<QueryResultType> queryResults;

  /// The total number of query results
  std::atomic<uint64_t> numQueryResults; 

  /// mutexes for each array element of alr.
  std::mutex* mutexes;

  /// An array of lists of results.
  std::list<QueryResultType> *alr;

  size_t numNodes;
  size_t nodeId;

public:
  /**
   * \param numNodes How many nodes in the cluster.
   * \param nodeId The node id of this node.
   * \param tableCapacity How many bins for intermediate query results.
   * \param resultsCapacity How many completed queries can be stored.
   */
  SubgraphQueryResultMap( size_t numNodes,
                          size_t nodeId,
                          size_t tableCapacity,
                          size_t resultsCapacity );

  ~SubgraphQueryResultMap();

  /**
   * For the given tuple, checks against existing intermediate query results
   * and adds the tuple if it satisfies the constraints.  When we advance
   * the query and need to look for a new edge, and the edge is assigned
   * to a different node, a new EdgeRequest is created.  These edge
   * requests are added to the edgeRequests list.
   */
  void process(TupleType const& tuple,
               std::list<EdgeRequestType>& edgeRequests);

  /**
   * Adds a new intermediate result. 
   */ 
  void add(QueryResultType const& result, 
           std::list<EdgeRequestType>& edgeRequests);

  /**
   * Returns the number of completed results that have been created.
   */
  uint64_t getNumResults() const {
    return numQueryResults; 
  }

private:
  /**
   * Uses the source hash function to find intermediate query results that
   * are looking for the source.  New edge requests are added to the
   * edgeRequests lists.
   */
  void processSource(TupleType const& tuple, 
                     std::list<EdgeRequestType>& edgeRequests);

  /**
   * Uses the dest hash function to find intermediate query results that
   * are looking for the dest.New edge requests are added to the
   * edgeRequests lists.
   */
  void processTarget(TupleType const& tuple,
                     std::list<EdgeRequestType>& edgeRequests);

  /**
   * Uses a combination of the source and dest hash functions to find
   * intermediate query results that are looking both the source and dest.
   * New edge requests are added to the
   * edgeRequests lists.
   */
  void processSourceTarget(TupleType const& tuple,
                           std::list<EdgeRequestType>& edgeRequests);
};

/// Constructor
template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
SubgraphQueryResultMap<TupleType, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
 SubgraphQueryResultMap( size_t numNodes,
                         size_t nodeId,
                         size_t tableCapacity,
                         size_t resultCapacity)
{
  this->numNodes = numNodes;
  this->nodeId = nodeId;
  this->tableCapacity = tableCapacity;
  this->resultCapacity = resultCapacity;

  queryResults.resize(resultCapacity);
  numQueryResults = 0;

  mutexes = new std::mutex[tableCapacity];

  alr = new std::list<QueryResultType>[tableCapacity];
}

/// Destructor
template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
SubgraphQueryResultMap<TupleType, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
~SubgraphQueryResultMap()
{
  delete[] mutexes;
  delete[] alr;
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void
SubgraphQueryResultMap<TupleType, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
add(QueryResultType const& result, std::list<EdgeRequestType>& edgeRequests)
{
  #ifdef DEBUG
  printf("Node %lu SubgraphQueryResultMap::add edge request size %lu\n",
    nodeId, edgeRequests.size());
  #endif
  
    
  if (!result.complete()) {
    //std::cout << "The result is not complete " << std::endl;

    // The hash function also adds an edge request to the list if the
    // thing we are looking for isn't going to come to this node.    
    size_t newIndex = result.hash(sourceHash, targetHash,
                                  edgeRequests, nodeId, numNodes) 
                                  % tableCapacity;
    std::string requestString = "";
    for(auto request : edgeRequests) {
      requestString += request.toString() + "\n";
    }
    #ifdef DEBUG
    printf("Node %lu SubgraphQueryResultMap::add result %s "
      "edgeRequests.size() %lu edge requests %s\n", nodeId, 
      result.toString().c_str(), edgeRequests.size(), requestString.c_str());
    #endif
            
    mutexes[newIndex].lock();
    alr[newIndex].push_back(result);
    mutexes[newIndex].unlock(); 
  } else {
    #ifdef DEBUG
    printf("Node %lu Complete query!\n", nodeId);
    #endif
    size_t index = numQueryResults.fetch_add(1);
    index = index % resultCapacity;
    queryResults[index] = result;     
  }
}


template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void 
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                        SourceHF, TargetHF, SourceEF, TargetEF>::
                            
process(TupleType const& tuple, 
        std::list<EdgeRequestType>& edgeRequests)
{
  processSource(tuple, edgeRequests);
  processTarget(tuple, edgeRequests);
  processSourceTarget(tuple, edgeRequests);
}


template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void SubgraphQueryResultMap<TupleType, source, target, time, duration,
                            SourceHF, TargetHF, SourceEF, TargetEF>::
processSource(TupleType const& tuple,
              std::list<EdgeRequestType>& edgeRequests)
{
  
  SourceType src = std::get<source>(tuple);
  size_t index = sourceHash(src) % tableCapacity;

  mutexes[index].lock();

  std::list<QueryResultType> rehash;

  size_t samId = std::get<0>(tuple);

  for (auto l = alr[index].begin(); 
        l != alr[index].end(); ++l ) 
  {
    if (l->boundSource() && !l->boundTarget()) {
      #ifdef DEBUG
      printf("Node %lu SubgraphQueryResultMap::processSource considering %s\n",
        nodeId, l->toString().c_str());
      #endif

      // Make sure none of the edges has the same samId as the current tuple
      if (l->noSamId(samId)) {
        //std::cout << "Adding edge in process Source " << std::endl;
        // The following call tries to add the tuple to the existing 
        // intermediate result, l.  If succesful, l remains the same
        // but a new intermediate result is created.

        #ifdef DEBUG
        printf("Node %lu SubgraphQueryResultMap::processSource about to try"
               " and add tuple\n", nodeId);
        #endif
        
        std::pair<bool, QueryResultType> p = l->addEdge(tuple);
        if (p.first) {
          #ifdef DEBUG
          printf("Node %lu SubgraphQueryResultMap::processSource added edge\n",
            nodeId);
          #endif
          //std::cout << "Original Tuple " << l->toString() << std::endl;
          //std::cout << "New Tuple " << p.second.toString() << std::endl;
          rehash.push_back(p.second);
        }
      }
    }
  }

  mutexes[index].unlock();
  
  for (QueryResultType& l : rehash) {
    #ifdef DEBUG
    printf("Node %lu SubgraphqueryResultMap::processSource rehashing " 
           "query result %s\n", nodeId, l.toString().c_str());
    #endif
    
    add(l, edgeRequests);
  }
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void 
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                       SourceHF, TargetHF, SourceEF, TargetEF>::
processTarget(TupleType const& tuple,
              std::list<EdgeRequestType>& edgeRequests)
{
  TargetType trg = std::get<target>(tuple);
  size_t index = targetHash(trg) % tableCapacity;

  mutexes[index].lock();

  std::list<QueryResultType> rehash;

  size_t samId = std::get<0>(tuple);

  for (auto l = alr[index].begin(); l != alr[index].end(); ++l ) {

    if (!l->boundSource() && l->boundTarget()) {
      if (l->noSamId(samId)) {
        std::pair<bool, QueryResultType> p = l->addEdge(tuple);
        if (p.first) {
          rehash.push_back(p.second);
        } 
      }
    }
  }

  mutexes[index].unlock();

  for (auto l : rehash) {
    add(l, edgeRequests);
  }
    
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void 
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                       SourceHF, TargetHF, SourceEF, TargetEF>::
processSourceTarget(TupleType const& tuple,
                    std::list<EdgeRequestType>& edgeRequests)
{
  SourceType src = std::get<source>(tuple);
  TargetType trg = std::get<target>(tuple);
  size_t index = (targetHash(trg) * sourceHash(src)) % tableCapacity;

  mutexes[index].lock();

  std::list<QueryResultType> rehash;

  size_t samId = std::get<0>(tuple);

  for (auto l = alr[index].begin(); l != alr[index].end(); ++l ) {
    
    if (l->boundSource() && l->boundTarget()) {
      #ifdef DEBUG
      printf("Node %lu SubgraphQueryResultMap::processSourceTarget considering"
        " %s\n", nodeId, l->toString().c_str());
      #endif
      
      if (l->noSamId(samId)) {
        std::pair<bool, QueryResultType> p = l->addEdge(tuple);
        if (p.first) {
          rehash.push_back(p.second);
        } 
      }
    }
  }

  mutexes[index].unlock();

  for (auto l : rehash) {
    add(l, edgeRequests);
  }
}





}


#endif
