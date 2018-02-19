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

  size_t capacity;

  std::mutex* mutexes;

  std::list<std::list<QueryResultType>> *alle;

public:
  SubgraphQueryResultMap( size_t capacity );

  ~SubgraphQueryResultMap();

  void process(TupleType const& tuple);

  void processSource(TupleType const& tuple);
  void processTarget(TupleType const& tuple);
};

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename SourceHF, typename TargetHF,
          typename SourceEF, typename TargetEF>
SubgraphQueryResultMap<TupleType, source, target, time, duration,
  SourceHF, TargetHF, SourceEF, TargetEF>::
 SubgraphQueryResultMap( size_t capacity )
{
  this->capacity = capacity;

  mutexes = new std::mutex[capacity];

  alle = new std::list<std::list<QueryResultType>>[capacity];
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
  delete[] alle;
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
  //processSourceTarget(tuple);
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
  size_t index = sourceHash(src) % capacity;

  /*mutexes[index].lock();

  std::list<QueryResultType> rehash;

  for (auto l = alle[index].begin(); l != alle[index].end(); ) {
    bool b = l.addEdge(tuple);
    if (b) {
      rehash.push_back(l);
      l = alle[index].erase(l); 
    } else {
      ++l;
    }
  }

  mutexes[index].unlock();

  for (auto l : rehash) {
    size_t newIndex = l.hash(sourceHash, targetHash) % capacity;
    mutexes[newIndex].lock();
    alle[newIndex].push_back(l);
    mutexes[newIndex].unlock(); 
  }*/
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
     
}




}


#endif
