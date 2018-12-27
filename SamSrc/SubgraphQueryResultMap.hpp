#ifndef SAM_SUBGRAPH_QUERY_RESULT_MAP_HPP
#define SAM_SUBGRAPH_QUERY_RESULT_MAP_HPP

#include "SubgraphQueryResult.hpp"
#include "CompressedSparse.hpp"
#include <limits>

namespace sam {

class SubgraphQueryResultMapException : public std::runtime_error
{
public:
  SubgraphQueryResultMapException(std::string str) : std::runtime_error(str) {}
};

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
  typedef CompressedSparse<TupleType, source, target, time, duration,
            SourceHF, SourceEF> CsrType;
  typedef CompressedSparse<TupleType, target, source, time, duration,
            TargetHF, TargetEF> CscType;

private:
  SourceHF sourceHash;
  TargetHF targetHash;
  SourceEF sourceEquals;
  TargetEF targetEquals;

  CsrType const& csr;
  CscType const& csc;

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

  /// mutexes for each list element of alr.
  std::mutex* mutexes;

  /// An array of lists of results.  The first level is an 
  /// array of size tableCapacity.  
  std::vector<QueryResultType> *alr;

  size_t numNodes;
  size_t nodeId;

  #ifdef DETAIL_TIMING
  double totalTimeProcessAgainstGraph = 0;
  double totalAddSimpleTime = 0;
  double totalTimeProcessSource = 0;
  double totalTimeProcessTarget = 0;
  double totalTimeProcessSourceTarget = 0;
  double totalTimeProcessProcessAgainstGraph = 0;
  double totalTimeProcessLoop1 = 0;
  double totalTimeProcessLoop2 = 0;
  #endif

  #ifdef METRICS
  size_t totalResultsDeleted = 0;
  size_t totalResultsCreated = 0;
  #endif

public:
  /**
   * Constructor.
   * \param numNodes How many nodes in the cluster.
   * \param nodeId The node id of this node.
   * \param tableCapacity How many bins for intermediate query results.
   * \param resultsCapacity How many completed queries can be stored.
   */
  SubgraphQueryResultMap( size_t numNodes,
                          size_t nodeId,
                          size_t tableCapacity,
                          size_t resultsCapacity,
                          CsrType const& _csr,
                          CscType const& _csc);

  ~SubgraphQueryResultMap();

  /**
   * For the given tuple, checks against existing intermediate query results
   * and adds the tuple if it satisfies the constraints.  When we advance
   * the query and need to look for a new edge, and the edge is assigned
   * to a different node, a new EdgeRequest is created.  These edge
   * requests are added to the edgeRequests list.
   *
   * If the edge is added, then we also need to look at the graph on the node.
   * It is possible that there exists in the graph edges that further the 
   * query.  We look for them in this method.
   *
   * \param tuple The edge to process.
   * \param edgeRequests Any edge requests that result are added here.
   * \return Returns a number representing the amount of work that this call
   *  required.
   */
  size_t process(TupleType const& tuple, 
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

  /**
   * Resets the counter for the results.
   */
  void clearResults() {
    numQueryResults = 0;
  }

  size_t getNumIntermediateResults() const {
    size_t total = 0;
    for(size_t i = 0; i < tableCapacity; i++) {
      total += alr[i].size();
    }
    return total;
  }

  size_t getResultCapacity() const {
    return resultCapacity;
  }

  QueryResultType getResult(size_t index) const {
    return queryResults[index];
  }

  #ifdef DETAIL_TIMING
  double getTotalTimeProcessAgainstGraph() const {
    return totalTimeProcessAgainstGraph;
  }

  /**
   * Total time spend in add(result, edgeRequests).
   */
  double getTotalAddSimpleTime() const {
    return totalAddSimpleTime;
  }
  double getTotalTimeProcessSource() const {
    return totalTimeProcessSource;
  }
  double getTotalTimeProcessTarget() const {
    return totalTimeProcessTarget;
  }
  double getTotalTimeProcessSourceTarget() const {
    return totalTimeProcessSourceTarget;
  }
  double getTotalTimeProcessProcessAgainstGraph() const {
    return totalTimeProcessProcessAgainstGraph;
  }
  double getTotalTimeProcessLoop1() const {
    return totalTimeProcessLoop1;
  }
  double getTotalTimeProcessLoop2() const {
    return totalTimeProcessLoop2;
  }

  #endif


  #ifdef METRICS

  size_t getTotalResultsDeleted() const { return totalResultsDeleted; }
  size_t getTotalResultsCreated() const { return totalResultsCreated; }

  #endif

private:

  std::function<size_t(TupleType const&)> sourceIndexFunction;
  std::function<size_t(TupleType const&)> targetIndexFunction;
  std::function<size_t(TupleType const&)> sourceTargetIndexFunction;
  std::function<bool(QueryResultType const&)> sourceCheckFunction;
  std::function<bool(QueryResultType const&)> targetCheckFunction;
  std::function<bool(QueryResultType const&)> sourceTargetCheckFunction;

  /**
   * Adds a new intermediate result.  Doesn't check graphs.
   * \param result The intermediate result to add.
   * \param edgeRequests Any result edge requests are added to this list.
   * \return Returns a number representing the amount of work.
   */ 
  size_t add_nocheck(QueryResultType const& result, 
           std::list<EdgeRequestType>& edgeRequests);

  size_t process(TupleType const& tuple,
        std::list<EdgeRequestType>& edgeRequests,
        std::function<size_t(TupleType const&)> indexFunction, 
        std::function<bool(QueryResultType const&)> checkFunction );

  size_t processAgainstGraph(std::list<QueryResultType>& rehash);
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
                         size_t resultCapacity,
                         CsrType const& _csr,
                         CscType const& _csc) :
                         csc(_csc), csr(_csr)
{
  sourceIndexFunction = [this](TupleType const& tuple) {
    SourceType src = std::get<source>(tuple);
    size_t index = this->sourceHash(src) % this->tableCapacity;
    DEBUG_PRINT("Node %lu SubgraphQueryResultMap::sourceIndexFunction tuple "
     "%s index %lu \n", this->nodeId, sam::toString(tuple).c_str(), index)
    return index;
  };

  sourceCheckFunction = [this](QueryResultType const& l) {
    if (l.boundSource() && !l.boundTarget()) return true;
    return false;
  };

  targetCheckFunction = [this](QueryResultType const& l ) {
    if (!l.boundSource() && l.boundTarget()) return true;
    return false;
  };

  sourceTargetCheckFunction = [this](QueryResultType const& l) {
    if (l.boundSource() && l.boundTarget()) return true;
    return false;
  };

  targetIndexFunction = [this](TupleType const& tuple) {
    DEBUG_PRINT("Node %lu SubgraphQueryResultMap::targetIndexFunction tuple "
      "%s\n", this->nodeId, sam::toString(tuple).c_str())
    TargetType trg = std::get<target>(tuple);
    size_t index = this->targetHash(trg) % this->tableCapacity;
    DEBUG_PRINT("Node %lu SubgraphQueryResultMap::targetIndexFunction tuple "
      "%s index %lu\n", this->nodeId, sam::toString(tuple).c_str(), index)
    return index;
  };

  sourceTargetIndexFunction = [this](TupleType const& tuple) {
    DEBUG_PRINT("Node %lu SubgraphQueryResultMap::sourceTargetIndexFunction "
      "tuple %s\n", this->nodeId, sam::toString(tuple).c_str())
    SourceType src = std::get<source>(tuple);
    TargetType trg = std::get<target>(tuple);
    return (this->targetHash(trg) * this->sourceHash(src)) 
             % this->tableCapacity;
  };

  this->numNodes = numNodes;
  this->nodeId = nodeId;
  this->tableCapacity = tableCapacity;
  this->resultCapacity = resultCapacity;

  queryResults.resize(resultCapacity);
  numQueryResults = 0;

  mutexes = new std::mutex[tableCapacity];

  alr = new std::vector<QueryResultType>[tableCapacity];

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
add(QueryResultType const& result, 
    std::list<EdgeRequestType>& edgeRequests)
{
  DETAIL_TIMING_BEG1

  DEBUG_PRINT("Node %lu SubgraphQueryResultMap::add with csr and csc"
    " edge request size %lu\n", nodeId, edgeRequests.size())

  std::list<QueryResultType> localQueryResults;
  localQueryResults.push_back(result);

  processAgainstGraph(localQueryResults);

  for (auto localQueryResult : localQueryResults) {
    DEBUG_PRINT("Node %lu SubgraphQueryResultMap::add considering query result"
      " %s\n", nodeId, localQueryResult.toString().c_str())

    if (!localQueryResult.complete()) {

      // The hash function also adds an edge request to the list if the
      // thing we are looking for isn't going to come to this node.    
      size_t newIndex = localQueryResult.hash(sourceHash, targetHash,
                                    edgeRequests, nodeId, numNodes) 
                                    % tableCapacity;
      std::string requestString = "";
      for(auto request : edgeRequests) {
        requestString += request.toString() + "\n";
      }
      //DEBUG_PRINT("Node %lu SubgraphQueryResultMap::add result %s "
      //  "edgeRequests.size() %lu edge requests %s\n", nodeId, 
      //  localQueryResult.toString().c_str(), edgeRequests.size(), 
      //  requestString.c_str())

      // Determine which thread has the least amount of work and give it
      // to that one.
      //size_t minIndex = 0;
      //size_t min = std::numeric_limits<size_t>::max();

      //mutexes[newIndex].lock();
      //size_t current_size = aalr[newIndex][i].size();
      //mutexes[newIndex].unlock();
      //if (current_size < min) {
      //  min = current_size;
      //  minIndex = i;
      //}
              
      mutexes[newIndex].lock();
      DEBUG_PRINT("Node %lu SubgraphQueryResultMap::add result %s "
        " adding to alr[%lu]\n", nodeId, 
        localQueryResult.toString().c_str(), newIndex);
      alr[newIndex].push_back(localQueryResult);
      mutexes[newIndex].unlock();

      METRICS_INCREMENT(totalResultsCreated)
 
    } else {
      DEBUG_PRINT("Node %lu Complete query! %s\n", nodeId, 
        localQueryResult.toString().c_str());
      size_t index = numQueryResults.fetch_add(1);
      index = index % resultCapacity;
      queryResults[index] = localQueryResult;     
    }
  }
  DEBUG_PRINT("Node %lu exiting SubgraphQueryResultMap::add(result, csr, csc, "
    "edgeRequests)\n", nodeId);

  DETAIL_TIMING_END1(totalTimeProcessAgainstGraph);
}


template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
size_t
SubgraphQueryResultMap<TupleType, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
add_nocheck(QueryResultType const& result, 
    std::list<EdgeRequestType>& edgeRequests)
{
  DEBUG_PRINT("Node %lu SubgraphQueryResultMap::add edge request size %lu\n",
    nodeId, edgeRequests.size());
    
  if (!result.complete()) {

    // The hash function also adds an edge request to the list if the
    // thing we are looking for isn't going to come to this node.    
    size_t newIndex = result.hash(sourceHash, targetHash,
                                  edgeRequests, nodeId, numNodes) 
                                  % tableCapacity;
    std::string requestString = "";
    for(auto request : edgeRequests) {
      requestString += request.toString() + "\n";
    }
    DEBUG_PRINT("Node %lu SubgraphQueryResultMap::add result %s "
      "edgeRequests.size() %lu edge requests %s\n", nodeId, 
      result.toString().c_str(), edgeRequests.size(), requestString.c_str());
    
    // Determine which thread has the least amount of work and give it
    // to that one.
    //size_t minIndex = 0;
    //size_t min = std::numeric_limits<size_t>::max();
    //for(size_t i = 0; i < numThreads; i++) {
    //  mutexes[newIndex][i].lock();
    //  size_t current_size = aalr[newIndex][i].size();
    //  mutexes[newIndex][i].unlock();
    //  if (current_size < min) {
    //    min = current_size;
    //    minIndex = i;
    //  }
    //}
     
    mutexes[newIndex].lock();
    alr[newIndex].push_back(result);
    mutexes[newIndex].unlock(); 

    METRICS_INCREMENT(totalResultsCreated)

  } else {
    DEBUG_PRINT("Node %lu Complete query! %s\n", nodeId, 
      result.toString().c_str());
    size_t index = numQueryResults.fetch_add(1);
    index = index % resultCapacity;
    queryResults[index] = result;     
  }

  return 1;
}



template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
size_t 
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                        SourceHF, TargetHF, SourceEF, TargetEF>::
process(TupleType const& tuple, 
        std::list<EdgeRequestType>& edgeRequests)
{

  DEBUG_PRINT("Node %lu entering SubgraphQueryResultMap::process("
   "tuple, csr, csc, edgeRequests) tuple %s\n", nodeId,
   sam::toString(tuple).c_str())

  DETAIL_TIMING_BEG1
  size_t workProcessSource = process(tuple, edgeRequests, 
                                   sourceIndexFunction, sourceCheckFunction);
  DETAIL_TIMING_END1(totalTimeProcessSource)

  DETAIL_TIMING_BEG2
  size_t workProcessTarget = process(tuple, edgeRequests,
                                   targetIndexFunction, targetCheckFunction);
  DETAIL_TIMING_END2(totalTimeProcessTarget)
  
  DETAIL_TIMING_BEG2
  size_t workProcessSourceTarget = process(tuple, edgeRequests, 
                          sourceTargetIndexFunction, sourceTargetCheckFunction);
  DETAIL_TIMING_END2(totalTimeProcessSourceTarget)

  DEBUG_PRINT("Node %lu End of SubgraphQueryResultMap edgeRequests.size()"
    " %lu\n", nodeId, edgeRequests.size())

  return workProcessSource + workProcessTarget + workProcessSourceTarget;
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
size_t 
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                       SourceHF, TargetHF, SourceEF, TargetEF>::
processAgainstGraph(std::list<QueryResultType>& rehash)
{
  DEBUG_PRINT("Node %lu SubgraphQueryResultMap::processAgainstGraph rehash size"
    " %lu at begining\n", nodeId, rehash.size());
  auto frontier = rehash.begin();

  #ifdef DEBUG
  size_t iter = 0;
  #endif

  size_t totalWork = 0;

  // Put a placemarker at the end so we know when to stop iterating.
  // The default constructor makes an object that is easy to tell that it
  // is null.
  rehash.push_back(QueryResultType());

  while (frontier != rehash.end()) {
    
    DEBUG_PRINT("Node %lu SubgraphQueryResultMap::processAgainstGraph "
      "rehash size %lu at beginning of while\n", nodeId, rehash.size());
    
    for (; !frontier->isNull(); ++frontier) 
    {
      DEBUG_PRINT("Node %lu SubgraphQueryResultMap::processAgainstGraph in "
        "frontier for loop\n", this->nodeId);
      // Only look at the graph if the result is not complete.
      if (!frontier->complete()) {
        std::list<TupleType> foundEdges;
        try {
          csr.findEdges(frontier->getCurrentSource(),
                      frontier->getCurrentTarget(),
                      frontier->getCurrentStartTimeFirst(),
                      frontier->getCurrentStartTimeSecond(),
                      frontier->getCurrentEndTimeFirst(),
                      frontier->getCurrentEndTimeSecond(),
                      foundEdges);
        } catch (std::exception e) {
          std::string message = "SubgraphQueryResultMap::"
            "processAgainstGraph caught exception: " + std::string(e.what());
          throw SubgraphQueryResultMapException(message);
        }

        DEBUG_PRINT("Node %lu SubgraphQueryResultMap::processAgainstGraph "
          "iter %lu number of found edges: %lu\n", nodeId, iter, 
          foundEdges.size());

        for (auto edge : foundEdges) {
          totalWork += 1;
         
          DEBUG_PRINT("Node %lu SubgraphQueryResultMap::processAgainstGraph "
            "iter %luconsidering found edge %s for query result %s\n",
            nodeId, iter, sam::toString(edge).c_str(), 
            frontier->toString().c_str());
          
          std::pair<bool, QueryResultType> p = frontier->addEdge(edge);
          if (p.first) {
            DEBUG_PRINT("Node %lu SubgraphQueryResultMap::processAgainstGraph "
              "iter %lu Created a new QueryResult: %s\n", nodeId, iter,
              p.second.toString().c_str());
            // push the new result to the end of the rehash list.
            rehash.push_back(p.second);
          }
        }
        DEBUG_PRINT("Node %lu SubgraphQueryResultMap::processAgainstGraph at" 
          " end of for loop over found edges\n", nodeId);

      }
    }
    //At this point frontier should be pointing at a null element.
    //Get rid of it and go to the next element (if there is one, otherwise
    //we exit from the while loop).
    frontier = rehash.erase(frontier);
    
    rehash.push_back(QueryResultType());
    
    DEBUG_PRINT("Node %lu SubgraphQueryResultMap::processAgainstGraph"
      " rehash size %lu after processing frontier (iteration %lu)\n", 
        nodeId, rehash.size(), iter);
    #ifdef DEBUG
    iter++;
    #endif
  }
  // Get rid of the null element
  rehash.pop_back();
  DEBUG_PRINT("Node %lu SubgraphQueryResultMap::processAgainstGraph exiting "
    "rehash size %lu totalWork %lu\n", nodeId, rehash.size(), totalWork);
      
  #ifdef DEBUG
  iter++;
  #endif

  return totalWork;

}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
size_t 
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                       SourceHF, TargetHF, SourceEF, TargetEF>::

process(TupleType const& tuple,
        std::list<EdgeRequestType>& edgeRequests,
        std::function<size_t(TupleType const&)> indexFunction,
        std::function<bool(QueryResultType const&)> checkFunction )
{
  DETAIL_TIMING_BEG1

  size_t index = indexFunction(tuple);

  std::list<QueryResultType> rehash;
  
  // We need the time to see if the intermediate result has expired (assumes
  // monotonically increasing time)
  double currentTime = std::get<time>(tuple);

  size_t samId = std::get<0>(tuple);

  size_t totalWork = 0;

  mutexes[index].lock();
  DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process(tuple, csr, csc, "
    "edgeRequests, indexFunction) alr[%lu].size() %lu\n",
    nodeId, index, alr[index].size());
  for(auto l = this->alr[index].begin();
        l != this->alr[index].end(); ) 
  {
    totalWork++;
    if (l->isExpired(currentTime)) {
      l = this->alr[index].erase(l);
      METRICS_INCREMENT(this->totalResultsDeleted)
    } else {
      if (checkFunction(*l)) {
        DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process "
         "considering %s\n", nodeId, l->toString().c_str());

        // Make sure none of the edges has the same samId as the current tuple
        if (l->noSamId(samId)) {
          // The following call tries to add the tuple to the existing 
          // intermediate result, l.  If succesful, l remains the same
          // but a new intermediate result is created.

          DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process about to try"
           " and add tuple %s to result %s\n", nodeId, toString(tuple).c_str(), 
           l->toString().c_str());
          
          std::pair<bool, QueryResultType> p = l->addEdge(tuple);
          if (p.first) {

            DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process added "
              "tuple %s to result %s\n", nodeId, toString(tuple).c_str(),
              l->toString().c_str());

            rehash.push_back(p.second);
          }
        } else {
          DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process had the id "
            "already \n", this->nodeId);
        }
      }
    ++l;
    }
  }
  mutexes[index].unlock();
  DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process total work after for "
    "loop %lu\n", nodeId, totalWork);
 
  DETAIL_TIMING_END1(totalTimeProcessLoop1)
 
  // See if the graph can further the queries
  DETAIL_TIMING_BEG2
  size_t graphWork = processAgainstGraph(rehash);
  DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process work from process"
    "AgainstGraph: %lu\n", this->nodeId, graphWork);
  totalWork += graphWork;
  DETAIL_TIMING_END2(totalTimeProcessProcessAgainstGraph)

  DETAIL_TIMING_BEG2
  size_t addWork = 0;
  for (QueryResultType& result : rehash) {
    DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process rehashing " 
           "query result %s\n", nodeId, result.toString().c_str());
    
    addWork += add_nocheck(result, edgeRequests);
  }
  DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process addWork %lu"
    " rehash size %lu\n", nodeId, addWork, rehash.size());
  DETAIL_TIMING_END2(totalTimeProcessLoop2)

  totalWork += addWork;
  DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process at end, total work"
    " %lu\n", nodeId, totalWork);
  return totalWork;
}

}


#endif
