#ifndef SAM_QUERY_RESULT_MAP_HPP
#define SAM_QUERY_RESULT_MAP_HPP

namespace sam {

class QueryResultException : public std::runtime_error {
public:
  QueryResultException(char const * message) : std::runtime_error(message) { }
  QueryResultException(std::string message) : std::runtime_error(message) { }
};

/**
 * This stores intermediate results of a subgraph query.
 */
template <typename TupleType,
          size_t source,
          size_t target,
          typename HF>
class QueryResult {
public:
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;

  HF hash;

private:

  SourceType source = nullValue<SourceType>();
  TargetType target = nullValue<TargetType>(); 

  
  std::vector<EdgeDescription> const & sortedEdges;

  private size_t currentEdge = 0;

public:

  QueryResult(std::vector<EdgeDescription> const & se) :
    sortedEdges(se);
  {}
  
  void setSource(SourceType source) {
    this->source = source; 
  }

  void setTarget(TargetType target) {
    this->target = target;
  }

  /**
   * This checks if the given tuple satisfies what we are looking for. If it
   * does, it increments to the next edge.
   */
  bool satisfies(TupleType t) {
    
  }

  /**
   * Returns true if the query result has fulfilled all the edges.
   */
  bool complete() {

  }

  /**
   * Returns true if the query has expired (meaning it can't be fulfilled
   * because the time constraint of the entire query has been violated).
   */
  bool expired(double currentTime) {

  }

  uint64_t hash() {
    if (!isNull(source) && isNull(target)) {
      return hash(source);    
    } else if (isNull(source) && !isNull(target)) {
      return hash(target);
    } else { //both are defined
      return hash(source + target);
    }
  }
};


// TODO Commonalities with CompressedSparse; make base class
template <typename TupleType,
          size_t source,
          size_t target,
          size_t time,
          typename HF, //Hash function
          typename EF> //Equality function
class QueryResultMap {
public:
 
private:
  
  // Time window in seconds
  double window = 1; 

  std::atomic<double> currentTime;

  HF hash;
  EF equal;
  size_t capacity;

  // array of lists of lists of edge requests
  std::list<std::list<QueryResult<TupleType, source, target, HF>>* alle;

public:
  QueryResultMap(size_t capacity, double window);
  ~QueryResultMap();
};

template <typename TupleType, size_t source, size_t target, size_t time,
          typename HF, typename EF>
QueryResultMap<TupleType, source, target, time, HF, EF>::
QueryResultMap( size_t capacity, double window) :
  currentTime(0)
{
  this->capacity = capacity;
  this->window = window;

  mutexes = new std::mutex[capacity];
  alle = new std::list<std::list<QueryResult>>[capacity];
}


template <typename TupleType, size_t source, size_t target, size_t time,
          typename HF, typename EF>
QueryResultMap<TupleType, source, target, time, HF, EF>::
~QueryResultMap( )
{
  delete mutexs;
  delete alle;
}


template <typename TupleType, size_t source, size_t target, size_t time,
          typename HF, typename EF>
QueryResultMap<TupleType, source, target, time, HF, EF>::add(
  QueryResult<TupleType, source, target, time, HF> queryResult


}

#endif
