#ifndef SAM_SUBGRAPH_QUERY_RESULT_MAP_HPP
#define SAM_SUBGRAPH_QUERY_RESULT_MAP_HPP

#include "SubgraphQueryResult.hpp"
#include "CompressedSparse.hpp"

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

  std::mutex generalLock;

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

private:
  /**
   * Adds a new intermediate result. 
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
add(QueryResultType const& result, 
    CsrType const& csr,
    CscType const& csc,
    std::list<EdgeRequestType>& edgeRequests)
{
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
              
      mutexes[newIndex].lock();
      alr[newIndex].push_back(localQueryResult);
      mutexes[newIndex].unlock(); 
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
            
    mutexes[newIndex].lock();
    alr[newIndex].push_back(result);
    mutexes[newIndex].unlock(); 
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
  processSource(tuple, csr, csc, edgeRequests);
  processTarget(tuple, csr, csc, edgeRequests);
  processSourceTarget(tuple, csr, csc, edgeRequests);
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
      } else {
        #ifdef DEBUG
        printf("Node %lu SubgraphQueryResultMap::processSource had the id "
          "already \n", this->nodeId);
        #endif
      }
    }
  }

  #ifdef DEBUG
  printf("Node %lu SubgraphQueryResultMap::processSource about to unlock "
    "mutex\n", nodeId);
  #endif
  mutexes[index].unlock();

  // See if the graph can further the queries
  processAgainstGraph(rehash, csr, csc);

  
  for (QueryResultType& result : rehash) {
    #ifdef DEBUG
    printf("Node %lu SubgraphqueryResultMap::processSource rehashing " 
           "query result %s\n", nodeId, result.toString().c_str());
    #endif
    
    add(result, edgeRequests);
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
              CsrType const& csr,
              CscType const& csc,
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

  // See if the graph can further the queries
  processAgainstGraph(rehash, csr, csc);

  for (auto result : rehash) {
    #ifdef DEBUG
    printf("Node %lu SubgraphqueryResultMap::processTarget rehashing " 
           "query result %s\n", nodeId, result.toString().c_str());
    #endif
 

    add(result, edgeRequests);
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
                    CsrType const& csr,
                    CscType const& csc,
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

  // See if the graph can further the queries
  processAgainstGraph(rehash, csr, csc);

  for (auto result : rehash) {

    #ifdef DEBUG
    printf("Node %lu SubgraphqueryResultMap::processSourceTarget rehashing " 
           "query result %s\n", nodeId, result.toString().c_str());
    #endif

    add(result, edgeRequests);
  }
}





}


#endif
