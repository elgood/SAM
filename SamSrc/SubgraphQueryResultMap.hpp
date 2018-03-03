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

public:
  /**
   * \param tableCapacity The size of the hash table that stores intermediate
   *                      query results.
   * \param tableCapacity How many bins for intermediate query results.
   * \param resultsCapacity How many completed queries can be stored.
   */
  SubgraphQueryResultMap( size_t tableCapacity,
                          size_t resultsCapacity );

  ~SubgraphQueryResultMap();

  /**
   * For the given tuple, checks against existing intermediate query results
   * and adds the tuple if it satisfies the constraints.
   */
  void process(TupleType const& tuple);

  /**
   * Adds a new intermediate result 
   */ 
  void add(QueryResultType const& result);

  /**
   * Returns the number of completed results that have been created.
   */
  uint64_t getNumResults() {
    return numQueryResults; 
  }

private:
  /**
   * Uses the source hash function to find intermediate query results that
   * are looking for the source.
   */
  void processSource(TupleType const& tuple);

  /**
   * Uses the dest hash function to find intermediate query results that
   * are looking for the dest.
   */
  void processTarget(TupleType const& tuple);

  /**
   * Uses a combination of the source and dest hash functions to find
   * intermediate query results that are looking both the source and dest.
   */
  void processSourceTarget(TupleType const& tuple);
};

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void
SubgraphQueryResultMap<TupleType, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
add(QueryResultType const& result)
{
  //std::cout << "Adding new intermediate result " << std::endl;
  if (!result.complete()) {
    //std::cout << "The result is not complete " << std::endl;
    size_t newIndex = result.hash(sourceHash, targetHash) % tableCapacity;
    mutexes[newIndex].lock();
    alr[newIndex].push_back(result);
    mutexes[newIndex].unlock(); 
  } else {
    //std::cout << "The result is complete " << std::endl;
    size_t index = numQueryResults.fetch_add(1);
    index = index % resultCapacity;
    queryResults[index] = result;     
  }
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
SubgraphQueryResultMap<TupleType, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
 SubgraphQueryResultMap( size_t tableCapacity,
                         size_t resultCapacity)
{
  this->tableCapacity = tableCapacity;
  this->resultCapacity = resultCapacity;

  queryResults.resize(resultCapacity);
  numQueryResults = 0;

  mutexes = new std::mutex[tableCapacity];

  alr = new std::list<QueryResultType>[tableCapacity];
}


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
                            
process(TupleType const& tuple)
{
  processSource(tuple);
  processTarget(tuple);
  processSourceTarget(tuple);
}


template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void SubgraphQueryResultMap<TupleType, source, target, time, duration,
                            SourceHF, TargetHF, SourceEF, TargetEF>::
processSource(TupleType const& tuple)
{
  
  SourceType src = std::get<source>(tuple);
  size_t index = sourceHash(src) % tableCapacity;

  mutexes[index].lock();

  std::list<QueryResultType> rehash;

  size_t samId = std::get<0>(tuple);

  for (auto l = alr[index].begin(); l != alr[index].end(); ++l ) {
    //std::cout << "processSource considering tuple " << l->toString() 
    //          << std::endl;
    if (l->noSamId(samId)) {
      //std::cout << "Adding edge in process Source " << std::endl;
      std::pair<bool, QueryResultType> p = l->addEdge(tuple);
      if (p.first) {
        //std::cout << "Original Tuple " << l->toString() << std::endl;
        //std::cout << "New Tuple " << p.second.toString() << std::endl;
        rehash.push_back(p.second);
      }
    }
  }

  mutexes[index].unlock();

  for (auto l : rehash) {
    add(l);
  }
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void 
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                       SourceHF, TargetHF, SourceEF, TargetEF>::
processTarget(TupleType const& tuple)
{
  TargetType trg = std::get<target>(tuple);
  size_t index = targetHash(trg) % tableCapacity;

  mutexes[index].lock();

  std::list<QueryResultType> rehash;

  size_t samId = std::get<0>(tuple);

  for (auto l = alr[index].begin(); l != alr[index].end(); ++l ) {
    if (l->noSamId(samId)) {
      std::pair<bool, QueryResultType> p = l->addEdge(tuple);
      if (p.first) {
        rehash.push_back(p.second);
      } 
    }
  }

  mutexes[index].unlock();

  for (auto l : rehash) {
    add(l);
  }
    
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
void 
SubgraphQueryResultMap<TupleType, source, target, time, duration,
                       SourceHF, TargetHF, SourceEF, TargetEF>::
processSourceTarget(TupleType const& tuple)
{
  SourceType src = std::get<source>(tuple);
  TargetType trg = std::get<target>(tuple);
  size_t index = (targetHash(trg) * sourceHash(src)) % tableCapacity;

  mutexes[index].lock();

  std::list<QueryResultType> rehash;

  size_t samId = std::get<0>(tuple);

  for (auto l = alr[index].begin(); l != alr[index].end(); ++l ) {
    if (l->noSamId(samId)) {
      std::pair<bool, QueryResultType> p = l->addEdge(tuple);
      if (p.first) {
        rehash.push_back(p.second);
      } 
    }
  }

  mutexes[index].unlock();

  for (auto l : rehash) {
    add(l);
  }
}





}


#endif
