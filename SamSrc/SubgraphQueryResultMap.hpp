#ifndef SAM_SUBGRAPH_QUERY_RESULT_MAP_HPP
#define SAM_SUBGRAPH_QUERY_RESULT_MAP_HPP

#include "SubgraphQueryResult.hpp"
#include "CompressedSparse.hpp"
#include <limits>

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

  std::mutex generalLock;

  std::thread* threads;

  #ifdef DETAIL_TIMING
  double totalTimeProcessAgainstGraph = 0;
  double totalAddSimpleTime = 0;
  double totalTimeProcessSource = 0;
  double totalTimeProcessTarget = 0;
  double totalTimeProcessSourceTarget = 0;
  double totalTimeProcessSourceProcessAgainstGraph = 0;
  double totalTimeProcessSourceLoop1 = 0;
  double totalTimeProcessSourceLoop2 = 0;
  double totalTimeProcessTargetLoop1 = 0;
  double totalTimeProcessTargetLoop2 = 0;
  double totalTimeProcessSourceTargetLoop1 = 0;
  double totalTimeProcessSourceTargetLoop2 = 0;
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
   */
  void process(TupleType const& tuple, 
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
  double getTotalTimeProcessSourceProcessAgainstGraph() const {
    return totalTimeProcessSourceProcessAgainstGraph;
  }
  double getTotalTimeProcessSourceLoop1() const {
    return totalTimeProcessSourceLoop1;
  }
  double getTotalTimeProcessSourceLoop2() const {
    return totalTimeProcessSourceLoop2;
  }
  double getTotalTimeProcessTargetLoop1() const {
    return totalTimeProcessTargetLoop1;
  }
  double getTotalTimeProcessTargetLoop2() const {
    return totalTimeProcessTargetLoop2;
  }
  double getTotalTimeProcessSourceTargetLoop1() const {
    return totalTimeProcessTargetLoop1;
  }
  double getTotalTimeProcessSourceTargetLoop2() const {
    return totalTimeProcessTargetLoop2;
  }

  #endif

private:
  /**
   * Adds a new intermediate result. 
   * \param result The intermediate result to add.
   * \param edgeRequests Any result edge requests are added to this list.
   */ 
  void add(QueryResultType const& result, 
           std::list<EdgeRequestType>& edgeRequests);




  /**
   * Uses the source hash function to find intermediate query results that
   * are looking for the source.  New edge requests are added to the
   * edgeRequests lists.
   */
  void processSource(TupleType const& tuple, 
                     CsrType const& csr,
                     CscType const& csc,
                     std::list<EdgeRequestType>& edgeRequests);

  /**
   * Uses the dest hash function to find intermediate query results that
   * are looking for the dest.New edge requests are added to the
   * edgeRequests lists.
   */
  void processTarget(TupleType const& tuple,
                     CsrType const& csr,
                     CscType const& csc,
                     std::list<EdgeRequestType>& edgeRequests);

  /**
   * Uses a combination of the source and dest hash functions to find
   * intermediate query results that are looking both the source and dest.
   * New edge requests are added to the
   * edgeRequests lists.
   */
  void processSourceTarget(TupleType const& tuple,
                           CsrType const& csr,
                           CscType const& csc,
                           std::list<EdgeRequestType>& edgeRequests);

  void processAgainstGraph(std::list<QueryResultType>& rehash,
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
  #ifdef DETAIL_TIMING
  auto t1 = std::chrono::high_resolution_clock::now();
  #endif

  #ifdef DEBUG
  printf("Node %lu SubgraphQueryResultMap::add with csr and csc edge request"
    " size %lu\n", nodeId, edgeRequests.size());
  #endif

  std::list<QueryResultType> localQueryResults;
  localQueryResults.push_back(result);

  processAgainstGraph(localQueryResults, csr, csc);

  for (auto localQueryResult : localQueryResults) {
    #ifdef DEBUG
    printf("Node %lu SubgraphQueryResultMap::add considering query result"
      " %s\n", nodeId, localQueryResult.toString().c_str());
    #endif

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
      #ifdef DEBUG
      printf("Node %lu SubgraphQueryResultMap::add result %s "
        "edgeRequests.size() %lu edge requests %s\n", nodeId, 
        localQueryResult.toString().c_str(), edgeRequests.size(), 
        requestString.c_str());
      #endif

      // Determine which thread has the least amount of work and give it
      // to that one.
      size_t minIndex = 0;
      size_t min = std::numeric_limits<size_t>::max();
      for(size_t i = 0; i < numThreads; i++) {
        if (aalr[newIndex][i].size() < min) {
          min = aalr[newIndex][i].size();
          minIndex = i;
        }
      }
              
      mutexes[newIndex][minIndex].lock();
      aalr[newIndex][minIndex].push_back(localQueryResult);
      mutexes[newIndex][minIndex].unlock(); 
    } else {
      #ifdef DEBUG
      printf("Node %lu Complete query! %s\n", nodeId, 
        localQueryResult.toString().c_str());
      #endif
      size_t index = numQueryResults.fetch_add(1);
      index = index % resultCapacity;
      queryResults[index] = localQueryResult;     
    }
  }
  #ifdef DEBUG
  printf("Node %lu exiting SubgraphQueryResultMap::add(result, csr, csc, "
    "edgeRequests)\n", nodeId);
  #endif

  #ifdef DETAIL_TIMING
  auto t2 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> tdiff =
    std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
  totalTimeProcessAgainstGraph += tdiff.count();
  #endif

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
    
    // Determine which thread has the least amount of work and give it
    // to that one.
    size_t minIndex = 0;
    size_t min = std::numeric_limits<size_t>::max();
    for(size_t i = 0; i < numThreads; i++) {
      if (aalr[newIndex][i].size() < min) {
        min = aalr[newIndex][i].size();
        minIndex = i;
      }
    }
     
    mutexes[newIndex][minIndex].lock();
    aalr[newIndex][minIndex].push_back(result);
    mutexes[newIndex][minIndex].unlock(); 
  } else {
    #ifdef DEBUG
    printf("Node %lu Complete query! %s\n", nodeId, result.toString().c_str());
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
        CsrType const& csr,
        CscType const& csc,
        std::list<EdgeRequestType>& edgeRequests)
{
  std::lock_guard<std::mutex> lock(generalLock);

  DETAIL_TIMING_BEG
  processSource(tuple, csr, csc, edgeRequests);
  DETAIL_TIMING_END(totalTimeProcessSource)

  DETAIL_TIMING_BEG
  processTarget(tuple, csr, csc, edgeRequests);
  DETAIL_TIMING_END(totalTimeProcessTarget)

  DETAIL_TIMING_BEG
  processSourceTarget(tuple, csr, csc, edgeRequests);
  DETAIL_TIMING_END(totalTimeProcessSourceTarget)

  #ifdef DEBUG
  printf("Node %lu End of SubgraphQueryResultMap edgeRequests.size() %lu\n",
    nodeId, edgeRequests.size());
  #endif
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void 
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                       SourceHF, TargetHF, SourceEF, TargetEF>::
processAgainstGraph(
              std::list<QueryResultType>& rehash,
              CsrType const& csr,
              CscType const& csc)
{
  #ifdef DEBUG
  printf("Node %lu SubgraphQueryResultMap::processAgainstGraph rehash size"
    " %lu at begining\n", nodeId, rehash.size());
  #endif
  auto frontier = rehash.begin();
  //auto newFrontier = rehash.end();
  //auto newFrontierBegin = rehash.end();
  //std::list<QueryResultType> newFrontier;

  #ifdef DEBUG
  size_t iter = 0;
  #endif

  // Put a placemarker at the end so we know when to stop iterating.
  // The default constructor makes an object that is easy to tell that it
  // is null.
  rehash.push_back(QueryResultType());

  while (frontier != rehash.end()) {
    
    #ifdef DEBUG
    printf("Node %lu SubgraphQueryResultMap::processAgainstGraph rehash size"
      " %lu at beginning of while\n", nodeId, rehash.size());
    #endif
    

    for (; !frontier->isNull(); ++frontier) 
    {
      #ifdef DEBUG 
      printf("Node %lu SubgraphQueryResultMap::processAgainstGraph in "
        "frontier for loop\n", this->nodeId);
      #endif
      // Only look at the graph if the result is not complete.
      if (!frontier->complete()) {
        std::list<TupleType> foundEdges;
        csr.findEdges(frontier->getCurrentSource(),
                      frontier->getCurrentTarget(),
                      frontier->getCurrentStartTimeFirst(),
                      frontier->getCurrentStartTimeSecond(),
                      frontier->getCurrentEndTimeFirst(),
                      frontier->getCurrentEndTimeSecond(),
                      foundEdges);

        #ifdef DEBUG
        printf("Node %lu SubgraphQueryResultMap::processAgainstGraph "
          "iter %lu number of found edges: %lu\n", nodeId, iter, 
          foundEdges.size());
        #endif

        for (auto edge : foundEdges) {
         
          #ifdef DEBUG
          printf("Node %lu SubgraphQueryResultMap::processAgainstGraph "
            "iter %luconsidering found edge %s for query result %s\n",
            nodeId, iter, sam::toString(edge).c_str(), 
            frontier->toString().c_str());
          #endif
          
          std::pair<bool, QueryResultType> p = frontier->addEdge(edge);
          if (p.first) {
            #ifdef DEBUG
            printf("Node %lu SubgraphQueryResultMap::processAgainstGraph "
              "iter %lu Created a new QueryResult: %s\n", nodeId, iter,
              p.second.toString().c_str());
            #endif
            // push the new result to the end of the rehash list.
            rehash.push_back(p.second);
            //rehash.insert(newFrontier, p.second);
            //newFrontier = rehash.end();
          }
        }
        #ifdef DEBUG
        printf("Node %lu SubgraphQueryResultMap::processAgainstGraph at end of"
          " for loop over found edges\n", nodeId);
        #endif

      }
    }
    //At this point frontier should be pointing at a null element.
    //Get rid of it and go to the next element (if there is one, otherwise
    //we exit from the while loop).
    frontier = rehash.erase(frontier);
    
    rehash.push_back(QueryResultType());
    
    //frontier = newFrontierBegin;
    //newFrontier = rehash.end();
    //newFrontierBegin = rehash.end();
    #ifdef DEBUG
    printf("Node %lu SubgraphQueryResultMap::processAgainstGraph rehash size"
      " %lu after processing frontier (iteration %lu)\n", 
        nodeId, rehash.size(), iter);
    iter++;
    #endif
  }
  // Get rid of the null element
  rehash.pop_back();
  #ifdef DEBUG
  printf("Node %lu SubgraphQueryResultMap::processAgainstGraph exiting "
    "rehash size %lu\n", 
      nodeId, rehash.size());
  iter++;
  #endif

}



template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void SubgraphQueryResultMap<TupleType, source, target, time, duration,
                            SourceHF, TargetHF, SourceEF, TargetEF>::
processSource(TupleType const& tuple,
              CsrType const& csr,
              CscType const& csc,
              std::list<EdgeRequestType>& edgeRequests)
{
  #ifdef DETAIL_TIMING
  auto beforeLoop1 = std::chrono::high_resolution_clock::now();
  #endif
  
  SourceType src = std::get<source>(tuple);
  size_t index = sourceHash(src) % tableCapacity;

  std::list<QueryResultType> rehash;

  // We need the time to see if the intermediate result has expired (assumes
  // monotonically increasing time)
  double currentTime = std::get<time>(tuple);

  size_t samId = std::get<0>(tuple);

  std::mutex rehashMutex;


  auto processSourcef1 = [this, &tuple, &rehashMutex, &rehash, index, 
    samId, currentTime]
    (size_t threadId) 
  {

    #ifdef DETAIL_METRICS2
    printf("Node %lu thread %lu list size processSourceLoop1 %lu\n",
      nodeId, threadId, aalr[index][threadId].size()); 
    #endif
    for (auto l = this->aalr[index][threadId].begin();
          l != this->aalr[index][threadId].end(); )
    {
      if (l->isExpired(currentTime)) {
        #ifdef DEBUG
        printf("Node %lu thread %lu SubgraphQueryResultMap::processSource "
          "deleting expired result %s\n", nodeId, threadId, 
          l->toString().c_str());
        #endif
        l = this->aalr[index][threadId].erase(l);
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

  };

  for (size_t i = 0; i < numThreads; i++) {
    threads[i] = std::thread(processSourcef1, i);  
  }

  for (size_t i = 0; i < numThreads; i++) {
    threads[i].join();
  }

  #ifdef DETAIL_TIMING
  auto afterLoop1 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> tdiffloop1 =
    std::chrono::duration_cast<std::chrono::duration<double>>(afterLoop1-
      beforeLoop1);
  totalTimeProcessSourceLoop1 += tdiffloop1.count();
  #endif

  
  #ifdef DETAIL_TIMING
  auto beforeProcessAgainstGraph = std::chrono::high_resolution_clock::now();
  #endif

  // See if the graph can further the queries
  processAgainstGraph(rehash, csr, csc);

  #ifdef DETAIL_TIMING
  auto afterProcessAgainstGraph = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> tdiffProcessAgainstGraph =
    std::chrono::duration_cast<std::chrono::duration<double>>(
      afterProcessAgainstGraph - beforeProcessAgainstGraph);
  totalTimeProcessSourceProcessAgainstGraph += tdiffProcessAgainstGraph.count();
  #endif


  #ifdef DETAIL_TIMING
  auto beforeLoop2 = std::chrono::high_resolution_clock::now();
  #endif


  auto processSourcef2 = []()
  {


  };
  
  for (QueryResultType& result : rehash) {
    #ifdef DEBUG
    printf("Node %lu SubgraphqueryResultMap::processSource rehashing " 
           "query result %s\n", nodeId, result.toString().c_str());
    #endif
    
    add(result, edgeRequests);
  }

  #ifdef DETAIL_TIMING
  auto afterLoop2 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> tdiffloop2 =
    std::chrono::duration_cast<std::chrono::duration<double>>(afterLoop2-
      beforeLoop2);
  totalTimeProcessSourceLoop2 += tdiffloop2.count();
  #endif



}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void 
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                       SourceHF, TargetHF, SourceEF, TargetEF>::
processTarget(TupleType const& tuple,
              CsrType const& csr,
              CscType const& csc,
              std::list<EdgeRequestType>& edgeRequests)
{
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

  #ifdef DETAIL_TIMING
  auto beforeLoop1 = std::chrono::high_resolution_clock::now();
  #endif

  auto processTargetf1 = [this, &tuple, &rehashMutex, &rehash, index, 
    samId, currentTime]
    (size_t threadId) 
  {

    for (auto l = this->aalr[index][threadId].begin();
          l != this->aalr[index][threadId].end(); )
    {
      if (l->isExpired(currentTime)) {
        #ifdef DEBUG
        printf("Node %lu thread %lu SubgraphQueryResultMap::processTarget "
          "deleting expired result %s\n", nodeId, threadId, 
          l->toString().c_str());
        #endif
        l = this->aalr[index][threadId].erase(l);
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

  };

  for (size_t i = 0; i < numThreads; i++) {
    threads[i] = std::thread(processTargetf1, i);  
  }


  for (size_t i = 0; i < numThreads; i++) {
    threads[i].join();
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

   
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void 
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

  auto processSourceTargetf1 = [this, &tuple, &rehashMutex, &rehash, index, 
    samId, currentTime]
    (size_t threadId) 
  {

    for (auto l = this->aalr[index][threadId].begin();
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

  };

  for (size_t i = 0; i < numThreads; i++) {
    threads[i] = std::thread(processSourceTargetf1, i);  
  }

  for (size_t i = 0; i < numThreads; i++) {
    threads[i].join();
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



}





}


#endif
