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

  /// Some of for loops are parallelized.  This specifies how many
  /// threads to use.
  size_t numThreads;
  
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

  /// mutexes for each list element of aalr.
  std::mutex** mutexes;

  /// An array of an array of lists of results.  The first level is an 
  /// array of size tableCapacity.  The second level array is a list
  /// for each thread.
  std::vector<QueryResultType> **aalr;

  size_t numNodes;
  size_t nodeId;

  std::thread* threads;

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
   * \param numNodes How many nodes in the cluster.
   * \param nodeId The node id of this node.
   * \param tableCapacity How many bins for intermediate query results.
   * \param resultsCapacity How many completed queries can be stored.
   * \param numThreads How many threads to use for parallel processing.
   */
  SubgraphQueryResultMap( size_t numNodes,
                          size_t nodeId,
                          size_t tableCapacity,
                          size_t resultsCapacity,
                          size_t numThreads );

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
   * \param csr The compressed sparse row graph.
   * \param csc The compressed sparse column graph.
   * \param edgeRequests Any edge requests that result are added here.
   * \return Returns a number representing the amount of work that this call
   *  required.
   */
  size_t process(TupleType const& tuple, 
               CsrType const& csr,
               CscType const& csc, 
               std::list<EdgeRequestType>& edgeRequests);

  /**
   * Adds a new intermediate result. 
   */ 
  void add(QueryResultType const& result, 
           CsrType const& csr,
           CscType const& csc, 
           std::list<EdgeRequestType>& edgeRequests);

  /**
   * Returns the number of completed results that have been created.
   */
  uint64_t getNumResults() const {
    return numQueryResults; 
  }

  size_t getNumIntermediateResults() const {
    size_t total = 0;
    for(size_t i = 0; i < tableCapacity; i++) {
      total += aalr[i].size();
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
   * Adds a new intermediate result. 
   * \param result The intermediate result to add.
   * \param edgeRequests Any result edge requests are added to this list.
   * \return Returns a number representing the amount of work.
   */ 
  size_t add(QueryResultType const& result, 
           std::list<EdgeRequestType>& edgeRequests);


  // This doesn't work for some reason.  There are results that it tries
  // to erase and it causes a seg fault.
  size_t process(TupleType const& tuple,
        CsrType const& csr,
        CscType const& csc,
        std::list<EdgeRequestType>& edgeRequests,
        std::function<size_t(TupleType const&)> indexFunction, 
        std::function<bool(QueryResultType const&)> checkFunction );

  /**
   * Uses the source hash function to find intermediate query results that
   * are looking for the source.  New edge requests are added to the
   * edgeRequests lists.
   * \return Returns the amount of work for this function.
   */
  //size_t processSource(TupleType const& tuple, 
  //                   CsrType const& csr,
  //                   CscType const& csc,
  //                   std::list<EdgeRequestType>& edgeRequests);

  /**
   * Uses the dest hash function to find intermediate query results that
   * are looking for the dest.New edge requests are added to the
   * edgeRequests lists.
   * \return Returns the amount of work for this function.
   */
  //size_t processTarget(TupleType const& tuple,
  //                   CsrType const& csr,
  //                   CscType const& csc,
  //                   std::list<EdgeRequestType>& edgeRequests);

  /**
   * Uses a combination of the source and dest hash functions to find
   * intermediate query results that are looking both the source and dest.
   * New edge requests are added to the
   * edgeRequests lists.
   * \return Returns the amount of work for this function.
   */
  //size_t processSourceTarget(TupleType const& tuple,
  //                         CsrType const& csr,
  //                         CscType const& csc,
  //                         std::list<EdgeRequestType>& edgeRequests);

  size_t processAgainstGraph(std::list<QueryResultType>& rehash,
                           CsrType const& csr,
                           CscType const& csc);
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
                         size_t numThreads)
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
  this->numThreads = numThreads;

  queryResults.resize(resultCapacity);
  numQueryResults = 0;

  mutexes = new std::mutex*[tableCapacity];
  for(size_t i = 0; i < tableCapacity; i++) {
    mutexes[i] = new std::mutex[numThreads];
  }

  aalr = new std::vector<QueryResultType>*[tableCapacity];
  for(size_t i = 0; i < tableCapacity; i++) {
    aalr[i] = new std::vector<QueryResultType>[numThreads];
  }

  threads = new std::thread[numThreads];
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
  for(size_t i = 0; i < tableCapacity; i++) {
    delete[] aalr[i];
    delete[] mutexes[i];
  }
  delete[] mutexes;
  delete[] aalr;
  delete[] threads;
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void
SubgraphQueryResultMap<TupleType, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
add(QueryResultType const& result, 
    CsrType const& csr,
    CscType const& csc,
    std::list<EdgeRequestType>& edgeRequests)
{
  DETAIL_TIMING_BEG1

  DEBUG_PRINT("Node %lu SubgraphQueryResultMap::add with csr and csc"
    " edge request size %lu\n", nodeId, edgeRequests.size())

  std::list<QueryResultType> localQueryResults;
  localQueryResults.push_back(result);

  processAgainstGraph(localQueryResults, csr, csc);

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
      DEBUG_PRINT("Node %lu SubgraphQueryResultMap::add result %s "
        "edgeRequests.size() %lu edge requests %s\n", nodeId, 
        localQueryResult.toString().c_str(), edgeRequests.size(), 
        requestString.c_str())

      // Determine which thread has the least amount of work and give it
      // to that one.
      size_t minIndex = 0;
      size_t min = std::numeric_limits<size_t>::max();
      for(size_t i = 0; i < numThreads; i++) {

        mutexes[newIndex][i].lock();
        size_t current_size = aalr[newIndex][i].size();
        mutexes[newIndex][i].unlock();
        if (current_size < min) {
          min = current_size;
          minIndex = i;
        }
      }
              
      mutexes[newIndex][minIndex].lock();
      DEBUG_PRINT("Node %lu SubgraphQueryResultMap::add result %s "
        " adding to aalr[%lu][%lu]\n", nodeId, 
        localQueryResult.toString().c_str(), newIndex, minIndex);
      aalr[newIndex][minIndex].push_back(localQueryResult);
      mutexes[newIndex][minIndex].unlock();

      METRICS_INCREMENT(totalResultsCreated)
 
    } else {
      DEBUG_PRINT("Node %lu Complete query! %s\n", nodeId, 
        localQueryResult.toString().c_str())
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
add(QueryResultType const& result, 
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
    size_t minIndex = 0;
    size_t min = std::numeric_limits<size_t>::max();
    for(size_t i = 0; i < numThreads; i++) {
      mutexes[newIndex][i].lock();
      size_t current_size = aalr[newIndex][i].size();
      mutexes[newIndex][i].unlock();
      if (current_size < min) {
        min = current_size;
        minIndex = i;
      }
    }
     
    mutexes[newIndex][minIndex].lock();
    aalr[newIndex][minIndex].push_back(result);
    mutexes[newIndex][minIndex].unlock(); 

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
        CsrType const& csr,
        CscType const& csc,
        std::list<EdgeRequestType>& edgeRequests)
{

  DEBUG_PRINT("Node %lu entering SubgraphQueryResultMap::process("
   "tuple, csr, csc, edgeRequests) tuple %s\n", nodeId,
   sam::toString(tuple).c_str())

  DETAIL_TIMING_BEG1
  size_t workProcessSource = process(tuple, csr, csc, edgeRequests, 
                                   sourceIndexFunction, sourceCheckFunction);
  //size_t workProcessSource = processSource(tuple, csr, csc, edgeRequests); 
  DETAIL_TIMING_END1(totalTimeProcessSource)

  DETAIL_TIMING_BEG2
  size_t workProcessTarget = process(tuple, csr, csc, edgeRequests,
                                   targetIndexFunction, targetCheckFunction);
  //size_t workProcessTarget = processTarget(tuple, csr, csc, edgeRequests);
  DETAIL_TIMING_END2(totalTimeProcessTarget)
  
  DETAIL_TIMING_BEG2
  size_t workProcessSourceTarget = process(tuple, csr, csc, edgeRequests, 
                          sourceTargetIndexFunction, sourceTargetCheckFunction);
  //size_t workProcessSourceTarget = 
  //  processSourceTarget(tuple, csr, csc, edgeRequests); 
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
processAgainstGraph(
              std::list<QueryResultType>& rehash,
              CsrType const& csr,
              CscType const& csc)
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
    "rehash size %lu\n", 
      nodeId, rehash.size());
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
        CsrType const& csr,
        CscType const& csc,
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

  std::mutex rehashMutex;

  size_t totalWork = 0;

  // Tried out threading this but didn't get speedups, so maybe remove
  // this thread stuff.
  for(size_t threadId = 0; threadId < numThreads; threadId++) {
    mutexes[index][threadId].lock();
    DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process(tuple, csr, csc, "
      "edgeRequests, indexFunction) aalr[%lu][%lu].size() %lu\n",
      nodeId, index, threadId, aalr[index][threadId].size())
    //printf("size %lu\n", this->aalr[index][threadId].size());
    for(auto l = this->aalr[index][threadId].begin();
          l != this->aalr[index][threadId].end(); ) 
    {
      totalWork++;
      if (l->isExpired(currentTime)) {
        //#ifdef DEBUG
        //printf("index %lu threadId %lu\n", index, threadId);
        //printf("Node %lu thread %lu SubgraphQueryResultMap::process "
        //  "deleting expired result %s\n", nodeId, threadId, 
        //  l->toString().c_str());
        //#endif
        l = this->aalr[index][threadId].erase(l);
        METRICS_INCREMENT(this->totalResultsDeleted)
      } else  {
      if (checkFunction(*l)) {
        DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process "
         "considering %s\n", nodeId, l->toString().c_str());

        // Make sure none of the edges has the same samId as the current tuple
        if (l->noSamId(samId)) {
          //std::cout << "Adding edge in process Source " << std::endl;
          // The following call tries to add the tuple to the existing 
          // intermediate result, l.  If succesful, l remains the same
          // but a new intermediate result is created.

          DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process about to try"
           " and add tuple\n", nodeId);
          
          std::pair<bool, QueryResultType> p = l->addEdge(tuple);
          if (p.first) {

            DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process added "
                        "edge\n", nodeId);

            rehashMutex.lock();
            rehash.push_back(p.second);
            rehashMutex.unlock();
          }
        } else {
          DEBUG_PRINT("Node %lu SubgraphQueryResultMap::process had the id "
            "already \n", this->nodeId);
        }
      }
      ++l;
      }
    }
    mutexes[index][threadId].unlock();
  }

 
  DETAIL_TIMING_END1(totalTimeProcessLoop1)
 


  // See if the graph can further the queries
  DETAIL_TIMING_BEG2
  totalWork += processAgainstGraph(rehash, csr, csc);
  DETAIL_TIMING_END2(totalTimeProcessProcessAgainstGraph)

  DETAIL_TIMING_BEG2
  for (QueryResultType& result : rehash) {
    #ifdef DEBUG
    printf("Node %lu SubgraphqueryResultMap::process rehashing " 
           "query result %s\n", nodeId, result.toString().c_str());
    #endif
    
    totalWork += add(result, edgeRequests);
  }
  DETAIL_TIMING_END2(totalTimeProcessLoop2)

  return totalWork;
}


/*template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
size_t
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                            SourceHF, TargetHF, SourceEF, TargetEF>::
processSource(TupleType const& tuple,
              CsrType const& csr,
              CscType const& csc,
              std::list<EdgeRequestType>& edgeRequests)
{
  #ifdef DETAIL_TIMING
  auto t1 = std::chrono::high_resolution_clock::now();
  #endif
 
  SourceType src = std::get<source>(tuple);
  size_t index = sourceHash(src) % tableCapacity;

  std::list<QueryResultType> rehash;

  // We need the time to see if the intermediate result has expired (assumes
  // monotonically increasing time)
  double currentTime = std::get<time>(tuple);

  size_t samId = std::get<0>(tuple);

  std::mutex rehashMutex;

  size_t totalWork = 0;

  // Tried out threading this but didn't get speedups, so maybe remove
  // this thread stuff.
  for(size_t threadId = 0; threadId < numThreads; threadId++) {
    mutexes[index][threadId].lock();
    for(auto l = this->aalr[index][threadId].begin();
          l != this->aalr[index][threadId].end(); ) 
    {
      totalWork++;
      if (l->isExpired(currentTime)) {
        #ifdef DEBUG
        printf("Node %lu thread %lu SubgraphQueryResultMap::processSource "
          "deleting expired result %s\n", nodeId, threadId, 
          l->toString().c_str());
        #endif
        l = this->aalr[index][threadId].erase(l);
        METRICS_INCREMENT(this->totalResultsDeleted)
      } else {
      if (l->boundSource() && !l->boundTarget()) {
        #ifdef DEBUG
        printf("Node %lu SubgraphQueryResultMap::processSource "
         "considering %s\n", nodeId, l->toString().c_str());
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
            printf("Node %lu SubgraphQueryResultMap::processSource added "
        "edge\n", nodeId);
            #endif

            rehashMutex.lock();
            rehash.push_back(p.second);
            rehashMutex.unlock();
          }
        } else {
          #ifdef DEBUG
          printf("Node %lu SubgraphQueryResultMap::processSource had the id "
            "aalready \n", this->nodeId);
          #endif
        }
      }
      ++l;
      }
    }
    mutexes[index][threadId].unlock();
  }

  #ifdef DETAIL_TIMING
  auto t2 = std::chrono::high_resolution_clock::now();
  auto diff = std::chrono::duration_cast<std::chrono::duration<double>>(
    t2 - t1);
  totalTimeProcessSourceLoop1 += diff.count();
  #endif
 



  // See if the graph can further the queries
  #ifdef DETAIL_TIMING
  t1 = std::chrono::high_resolution_clock::now();
  #endif
  totalWork += processAgainstGraph(rehash, csr, csc);
  #ifdef DETAIL_TIMING
  t2 = std::chrono::high_resolution_clock::now();
  diff = std::chrono::duration_cast<std::chrono::duration<double>>(
    t2 - t1);
  totalTimeProcessSourceProcessAgainstGraph += diff.count();
  #endif


  #ifdef DETAIL_TIMING
  t1 = std::chrono::high_resolution_clock::now();
  #endif
  for (QueryResultType& result : rehash) {
    #ifdef DEBUG
    printf("Node %lu SubgraphqueryResultMap::processSource rehashing " 
           "query result %s\n", nodeId, result.toString().c_str());
    #endif
    
    add(result, edgeRequests);
  }
  #ifdef DETAIL_TIMING
  t2 = std::chrono::high_resolution_clock::now();
  diff = std::chrono::duration_cast<std::chrono::duration<double>>(
    t2 - t1);
  totalTimeProcessSourceLoop2 += diff.count();
  #endif

  return totalWork;
}*/
/*

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
size_t 
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                       SourceHF, TargetHF, SourceEF, TargetEF>::
processTarget(TupleType const& tuple,
              CsrType const& csr,
              CscType const& csc,
              std::list<EdgeRequestType>& edgeRequests)
{
  #ifdef DETAIL_TIMING
  auto beforeLoop1 = std::chrono::high_resolution_clock::now();
  #endif
  
  TargetType trg = std::get<target>(tuple);
  size_t index = targetHash(trg) % tableCapacity;

  // When we add an edge, we keep the original intermediate result (for
  // future matches) and create a new one with the additional edge.  The
  // new one is added to this list.
  std::list<QueryResultType> rehash;

  // We need the sam Id of the edge to make sure we haven't seen this edge
  // before for a given intermediate result.
  size_t samId = std::get<0>(tuple);

  // We need the time to see if the intermediate result has expired (assumes
  // monotonically increasing time)
  double currentTime = std::get<time>(tuple);

  std::mutex rehashMutex;

  size_t totalWork = 0;

  for(size_t threadId = 0; threadId < numThreads; threadId++) {
    mutexes[index][threadId].lock();
    for(auto l = this->aalr[index][threadId].begin();
          l != this->aalr[index][threadId].end(); ) 
    {
      totalWork++;
      if (l->isExpired(currentTime)) {
        #ifdef DEBUG
        printf("Node %lu thread %lu SubgraphQueryResultMap::processTarget "
          "deleting expired result %s\n", nodeId, threadId, 
          l->toString().c_str());
        #endif
        l = this->aalr[index][threadId].erase(l);
        METRICS_INCREMENT(totalResultsDeleted)
      } else {
        if (!l->boundSource() && l->boundTarget()) {
          #ifdef DEBUG
          printf("Node %lu SubgraphQueryResultMap::processTarget "
                 "considering %s\n", nodeId, l->toString().c_str());
          #endif

          // Make sure none of the edges has the same samId as the current tuple
          if (l->noSamId(samId)) {

            // The following call tries to add the tuple to the existing 
            // intermediate result, l.  If succesful, l remains the same
            // but a new intermediate result is created.

            #ifdef DEBUG
            printf("Node %lu SubgraphQueryResultMap::processTarget about to try"
                   " and add tuple\n", nodeId);
            #endif
            
            std::pair<bool, QueryResultType> p = l->addEdge(tuple);
            if (p.first) {

              #ifdef DEBUG
              printf("Node %lu SubgraphQueryResultMap::processTarget added "
                "edge\n", nodeId);
              #endif

              rehashMutex.lock();
              rehash.push_back(p.second);
              rehashMutex.unlock();
            }
          } else {
            #ifdef DEBUG
            printf("Node %lu SubgraphQueryResultMap::processTarget had the id "
              "already \n", this->nodeId);
            #endif
          }
        }
        ++l;
      }
    }

    mutexes[index][threadId].unlock();
  }

  #ifdef DETAIL_TIMING
  auto afterLoop1 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> tdiffloop1 =
    std::chrono::duration_cast<std::chrono::duration<double>>(afterLoop1-
      beforeLoop1);
  totalTimeProcessTargetLoop1 += tdiffloop1.count();
  #endif

  // See if the graph can further the queries
  totalWork += processAgainstGraph(rehash, csr, csc);

  #ifdef DETAIL_TIMING
  auto beforeLoop2 = std::chrono::high_resolution_clock::now();
  #endif

  for (auto result : rehash) {
    #ifdef DEBUG
    printf("Node %lu SubgraphqueryResultMap::processTarget rehashing " 
           "query result %s\n", nodeId, result.toString().c_str());
    #endif
 

    add(result, edgeRequests);
  }
 
  #ifdef DETAIL_TIMING
  auto afterLoop2 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> tdiffloop2 =
    std::chrono::duration_cast<std::chrono::duration<double>>(afterLoop2-
      beforeLoop2);
  totalTimeProcessTargetLoop2 += tdiffloop2.count();
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
processSourceTarget(TupleType const& tuple,
                    CsrType const& csr,
                    CscType const& csc,
                    std::list<EdgeRequestType>& edgeRequests)
{
  #ifdef DETAIL_TIMING
  auto beforeLoop1 = std::chrono::high_resolution_clock::now();
  #endif

  SourceType src = std::get<source>(tuple);
  TargetType trg = std::get<target>(tuple);
  size_t index = (targetHash(trg) * sourceHash(src)) % tableCapacity;

  // When we add an edge, we keep the original intermediate result (for
  // future matches) and create a new one with the additional edge.  The
  // new one is added to this list.
  std::list<QueryResultType> rehash;

  // We need the sam Id of the edge to make sure we haven't seen this edge
  // before for a given intermediate result.
  size_t samId = std::get<0>(tuple);

  // We need the time to see if the intermediate result has expired (assumes
  // monotonically increasing time)
  double currentTime = std::get<time>(tuple);

  std::mutex rehashMutex;

  size_t totalWork = 0;

  for(size_t threadId = 0; threadId < numThreads; threadId++) {
    mutexes[index][threadId].lock();
    for(auto l = this->aalr[index][threadId].begin();
          l != this->aalr[index][threadId].end(); ) 
    {
      totalWork++;
      if (l->isExpired(currentTime)) {
        #ifdef DEBUG
        printf("Node %lu thread %lu "
          "SubgraphQueryResultMap::processSourceTarget "
          "deleting expired result %s\n", nodeId, threadId, 
          l->toString().c_str());
        #endif
        l = this->aalr[index][threadId].erase(l);
        METRICS_INCREMENT(totalResultsDeleted)
      } else {
        if (l->boundSource() && l->boundTarget()) {
          #ifdef DEBUG
          printf("Node %lu SubgraphQueryResultMap::processSourceTarget "
                 "considering %s\n", nodeId, l->toString().c_str());
          #endif

          // Make sure none of the edges has the same samId as the current tuple
          if (l->noSamId(samId)) {

            // The following call tries to add the tuple to the existing 
            // intermediate result, l.  If succesful, l remains the same
            // but a new intermediate result is created.

            #ifdef DEBUG
            printf("Node %lu SubgraphQueryResultMap::processSourceTarget about"
                   " to try and add tuple\n", nodeId);
            #endif
            
            std::pair<bool, QueryResultType> p = l->addEdge(tuple);
            if (p.first) {

              #ifdef DEBUG
              printf("Node %lu SubgraphQueryResultMap::processSourceTarget "
                "added edge\n", nodeId);
              #endif

              rehashMutex.lock();
              rehash.push_back(p.second);
              rehashMutex.unlock();
            }
          } else {
            #ifdef DEBUG
            printf("Node %lu SubgraphQueryResultMap::processSourceTarget had "
              "the id already \n", this->nodeId);
            #endif
          }
        }
        ++l;
      }
    }
    mutexes[index][threadId].unlock();
  }

  #ifdef DETAIL_TIMING
  auto afterLoop1 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> tdiffloop1 =
    std::chrono::duration_cast<std::chrono::duration<double>>(afterLoop1-
      beforeLoop1);
  totalTimeProcessSourceTargetLoop1 += tdiffloop1.count();
  #endif



  // See if the graph can further the queries
  totalWork += processAgainstGraph(rehash, csr, csc);

  #ifdef DETAIL_TIMING
  auto beforeLoop2 = std::chrono::high_resolution_clock::now();
  #endif


  for (auto result : rehash) {

    #ifdef DEBUG
    printf("Node %lu SubgraphqueryResultMap::processSourceTarget rehashing " 
           "query result %s\n", nodeId, result.toString().c_str());
    #endif

    add(result, edgeRequests);
  }

  #ifdef DETAIL_TIMING
  auto afterLoop2 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> tdiffloop2 =
    std::chrono::duration_cast<std::chrono::duration<double>>(afterLoop2-
      beforeLoop2);
  totalTimeProcessSourceTargetLoop2 += tdiffloop2.count();
  #endif

  return totalWork;

}*/

//working
/*
template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
size_t 
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                            SourceHF, TargetHF, SourceEF, TargetEF>::
processSource(TupleType const& tuple,
              CsrType const& csr,
              CscType const& csc,
              std::list<EdgeRequestType>& edgeRequests)
{
  #ifdef DETAIL_TIMING
  auto t1 = std::chrono::high_resolution_clock::now();
  #endif
 
  SourceType src = std::get<source>(tuple);
  size_t index = sourceHash(src) % tableCapacity;

  std::list<QueryResultType> rehash;

  // We need the time to see if the intermediate result has expired (assumes
  // monotonically increasing time)
  double currentTime = std::get<time>(tuple);

  size_t samId = std::get<0>(tuple);

  std::mutex rehashMutex;

  for(size_t threadId = 0; threadId < numThreads; threadId++) {
    mutexes[index][threadId].lock();
    for(auto l = this->aalr[index][threadId].begin();
          l != this->aalr[index][threadId].end(); ) 
    {
      if (l->isExpired(currentTime)) {
	#ifdef DEBUG
	printf("Node %lu thread %lu SubgraphQueryResultMap::processSource "
	  "deleting expired result %s\n", nodeId, threadId, 
	  l->toString().c_str());
	#endif
	l = this->aalr[index][threadId].erase(l);
	METRICS_INCREMENT(this->totalResultsDeleted)
      } else {
	if (l->boundSource() && !l->boundTarget()) {
	  #ifdef DEBUG
	  printf("Node %lu SubgraphQueryResultMap::processSource "
		 "considering %s\n", nodeId, l->toString().c_str());
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
	      printf("Node %lu SubgraphQueryResultMap::processSource added "
		"edge\n", nodeId);
	      #endif

	      rehashMutex.lock();
	      rehash.push_back(p.second);
	      rehashMutex.unlock();
	    }
	  } else {
	    #ifdef DEBUG
	    printf("Node %lu SubgraphQueryResultMap::processSource had the id "
	      "aalready \n", this->nodeId);
	    #endif
	  }
	}
	++l;
      }
    }
    mutexes[index][threadId].unlock();
  }


  #ifdef DETAIL_TIMING
  auto t2 = std::chrono::high_resolution_clock::now();
  auto diff = std::chrono::duration_cast<std::chrono::duration<double>>(
    t2 - t1);
  totalTimeProcessSourceLoop1 += diff.count();
  #endif
 



  // See if the graph can further the queries
  #ifdef DETAIL_TIMING
  t1 = std::chrono::high_resolution_clock::now();
  #endif
  processAgainstGraph(rehash, csr, csc);
  #ifdef DETAIL_TIMING
  t2 = std::chrono::high_resolution_clock::now();
  diff = std::chrono::duration_cast<std::chrono::duration<double>>(
    t2 - t1);
  totalTimeProcessSourceProcessAgainstGraph += diff.count();
  #endif


  #ifdef DETAIL_TIMING
  t1 = std::chrono::high_resolution_clock::now();
  #endif
  for (QueryResultType& result : rehash) {
    #ifdef DEBUG
    printf("Node %lu SubgraphqueryResultMap::processSource rehashing " 
           "query result %s\n", nodeId, result.toString().c_str());
    #endif
    
    add(result, edgeRequests);
  }
  #ifdef DETAIL_TIMING
  t2 = std::chrono::high_resolution_clock::now();
  diff = std::chrono::duration_cast<std::chrono::duration<double>>(
    t2 - t1);
  totalTimeProcessSourceLoop2 += diff.count();
  #endif

  return 0;

}*/

/*template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
size_t 
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                       SourceHF, TargetHF, SourceEF, TargetEF>::
processTarget(TupleType const& tuple,
              CsrType const& csr,
              CscType const& csc,
              std::list<EdgeRequestType>& edgeRequests)
{
  #ifdef DETAIL_TIMING
  auto beforeLoop1 = std::chrono::high_resolution_clock::now();
  #endif
  
  TargetType trg = std::get<target>(tuple);
  size_t index = targetHash(trg) % tableCapacity;

  // When we add an edge, we keep the original intermediate result (for
  // future matches) and create a new one with the additional edge.  The
  // new one is added to this list.
  std::list<QueryResultType> rehash;

  // We need the sam Id of the edge to make sure we haven't seen this edge
  // before for a given intermediate result.
  size_t samId = std::get<0>(tuple);

  // We need the time to see if the intermediate result has expired (assumes
  // monotonically increasing time)
  double currentTime = std::get<time>(tuple);

  std::mutex rehashMutex;


  for(size_t threadId = 0; threadId < numThreads; threadId++) {
    mutexes[index][threadId].lock();
    for(auto l = this->aalr[index][threadId].begin();
          l != this->aalr[index][threadId].end(); ) 
    {
      if (l->isExpired(currentTime)) {
        #ifdef DEBUG
        printf("Node %lu thread %lu SubgraphQueryResultMap::processTarget "
          "deleting expired result %s\n", nodeId, threadId, 
          l->toString().c_str());
        #endif
        l = this->aalr[index][threadId].erase(l);
        METRICS_INCREMENT(totalResultsDeleted)
      } else {
        if (!l->boundSource() && l->boundTarget()) {
          #ifdef DEBUG
          printf("Node %lu SubgraphQueryResultMap::processTarget "
                 "considering %s\n", nodeId, l->toString().c_str());
          #endif

          // Make sure none of the edges has the same samId as the current tuple
          if (l->noSamId(samId)) {

            // The following call tries to add the tuple to the existing 
            // intermediate result, l.  If succesful, l remains the same
            // but a new intermediate result is created.

            #ifdef DEBUG
            printf("Node %lu SubgraphQueryResultMap::processTarget about to try"
                   " and add tuple\n", nodeId);
            #endif
            
            std::pair<bool, QueryResultType> p = l->addEdge(tuple);
            if (p.first) {

              #ifdef DEBUG
              printf("Node %lu SubgraphQueryResultMap::processTarget added "
                "edge\n", nodeId);
              #endif

              rehashMutex.lock();
              rehash.push_back(p.second);
              rehashMutex.unlock();
            }
          } else {
            #ifdef DEBUG
            printf("Node %lu SubgraphQueryResultMap::processTarget had the id "
              "already \n", this->nodeId);
            #endif
          }
        }
        ++l;
      }
    }

    mutexes[index][threadId].unlock();
  }

  #ifdef DETAIL_TIMING
  auto afterLoop1 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> tdiffloop1 =
    std::chrono::duration_cast<std::chrono::duration<double>>(afterLoop1-
      beforeLoop1);
  totalTimeProcessTargetLoop1 += tdiffloop1.count();
  #endif

  // See if the graph can further the queries
  processAgainstGraph(rehash, csr, csc);

  #ifdef DETAIL_TIMING
  auto beforeLoop2 = std::chrono::high_resolution_clock::now();
  #endif

  for (auto result : rehash) {
    #ifdef DEBUG
    printf("Node %lu SubgraphqueryResultMap::processTarget rehashing " 
           "query result %s\n", nodeId, result.toString().c_str());
    #endif
 

    add(result, edgeRequests);
  }
 
  #ifdef DETAIL_TIMING
  auto afterLoop2 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> tdiffloop2 =
    std::chrono::duration_cast<std::chrono::duration<double>>(afterLoop2-
      beforeLoop2);
  totalTimeProcessTargetLoop2 += tdiffloop2.count();
  #endif

  return 0; 
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
size_t 
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                       SourceHF, TargetHF, SourceEF, TargetEF>::
processSourceTarget(TupleType const& tuple,
                    CsrType const& csr,
                    CscType const& csc,
                    std::list<EdgeRequestType>& edgeRequests)
{
  #ifdef DETAIL_TIMING
  auto beforeLoop1 = std::chrono::high_resolution_clock::now();
  #endif

  SourceType src = std::get<source>(tuple);
  TargetType trg = std::get<target>(tuple);
  size_t index = (targetHash(trg) * sourceHash(src)) % tableCapacity;

  // When we add an edge, we keep the original intermediate result (for
  // future matches) and create a new one with the additional edge.  The
  // new one is added to this list.
  std::list<QueryResultType> rehash;

  // We need the sam Id of the edge to make sure we haven't seen this edge
  // before for a given intermediate result.
  size_t samId = std::get<0>(tuple);

  // We need the time to see if the intermediate result has expired (assumes
  // monotonically increasing time)
  double currentTime = std::get<time>(tuple);

  std::mutex rehashMutex;

  for(size_t threadId = 0; threadId < numThreads; threadId++) {
    mutexes[index][threadId].lock();
    for(auto l = this->aalr[index][threadId].begin();
          l != this->aalr[index][threadId].end(); ) 
    {
      if (l->isExpired(currentTime)) {
        #ifdef DEBUG
        printf("Node %lu thread %lu "
          "SubgraphQueryResultMap::processSourceTarget "
          "deleting expired result %s\n", nodeId, threadId, 
          l->toString().c_str());
        #endif
        l = this->aalr[index][threadId].erase(l);
        METRICS_INCREMENT(totalResultsDeleted)
      } else {
        if (l->boundSource() && l->boundTarget()) {
          #ifdef DEBUG
          printf("Node %lu SubgraphQueryResultMap::processSourceTarget "
                 "considering %s\n", nodeId, l->toString().c_str());
          #endif

          // Make sure none of the edges has the same samId as the current tuple
          if (l->noSamId(samId)) {

            // The following call tries to add the tuple to the existing 
            // intermediate result, l.  If succesful, l remains the same
            // but a new intermediate result is created.

            #ifdef DEBUG
            printf("Node %lu SubgraphQueryResultMap::processSourceTarget about"
                   " to try and add tuple\n", nodeId);
            #endif
            
            std::pair<bool, QueryResultType> p = l->addEdge(tuple);
            if (p.first) {

              #ifdef DEBUG
              printf("Node %lu SubgraphQueryResultMap::processSourceTarget "
                "added edge\n", nodeId);
              #endif

              rehashMutex.lock();
              rehash.push_back(p.second);
              rehashMutex.unlock();
            }
          } else {
            #ifdef DEBUG
            printf("Node %lu SubgraphQueryResultMap::processSourceTarget had "
              "the id already \n", this->nodeId);
            #endif
          }
        }
        ++l;
      }
    }
    mutexes[index][threadId].unlock();
  }

  #ifdef DETAIL_TIMING
  auto afterLoop1 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> tdiffloop1 =
    std::chrono::duration_cast<std::chrono::duration<double>>(afterLoop1-
      beforeLoop1);
  totalTimeProcessSourceTargetLoop1 += tdiffloop1.count();
  #endif



  // See if the graph can further the queries
  processAgainstGraph(rehash, csr, csc);

  #ifdef DETAIL_TIMING
  auto beforeLoop2 = std::chrono::high_resolution_clock::now();
  #endif


  for (auto result : rehash) {

    #ifdef DEBUG
    printf("Node %lu SubgraphqueryResultMap::processSourceTarget rehashing " 
           "query result %s\n", nodeId, result.toString().c_str());
    #endif

    add(result, edgeRequests);
  }

  #ifdef DETAIL_TIMING
  auto afterLoop2 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> tdiffloop2 =
    std::chrono::duration_cast<std::chrono::duration<double>>(afterLoop2-
      beforeLoop2);
  totalTimeProcessSourceTargetLoop2 += tdiffloop2.count();
  #endif

  return 0;
}
*/



}


#endif
