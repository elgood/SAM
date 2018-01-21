#ifndef COMPRESSED_SPARSE_HPP
#define COMPRESSED_SPARSE_HPP

#include <Edge.hpp>
#include <map>
#include <mutex>
#include "Util.hpp"

namespace sam {

class CompressedSparseException : public std::runtime_error
{
public:
  CompressedSparseException(char const* message) : std::runtime_error(message){}
  CompressedSparseException(std::string message) : std::runtime_error(message){}

};

template <typename TupleType, 
          size_t source, 
          size_t target, 
          size_t time,
          typename HF, //Hash function
          typename EF> //Equality function
class CompressedSparse
{
public:
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;
  typedef Edge<TupleType, source, target, time>       EdgeType;

private:

  // Time window in seconds.  
  double window = 1;

  /**
   * The current time.  We update current time in method addEdge in a
   * non-perfect way.  Current time should generally increase, but because
   * of threading issues it might jump around a bit.  This should be good
   * enough.
   */
  std::atomic<double> currentTime;

  HF hash;
  EF equal;
  size_t capacity;
  std::mutex* mutexes;

  // array of lists of lists of edges
  std::list<std::list<TupleType>>* alle;

  void cleanupEdges(size_t index);

public:

  /**
   * \param capacity How big the storage is.
   * \param window How big the time window is in seconds.
   */
  CompressedSparse(size_t capacity, double window);

  ~CompressedSparse();

  void addEdge(TupleType tuple);


  /** 
   * Counts the number of edges in the graph.  Linear operation.
   */
  size_t countEdges() const;
};

template <typename TupleType, size_t source, size_t target, size_t time,
          typename HF, typename EF>
CompressedSparse<TupleType, source, target, time, HF, EF>::
CompressedSparse( size_t capacity, double window ) :
  currentTime(0)
{
  this->capacity = capacity;
  this->window = window;
  //currentTime = std::atomic<double>(0);

  mutexes = new std::mutex[capacity];

  alle = new std::list<std::list<TupleType>>[capacity];
}

template <typename TupleType, size_t source, size_t target, size_t time,
          typename HF, typename EF>
CompressedSparse<TupleType, source, target, time, HF, EF>::
~CompressedSparse()
{
  delete[] mutexes;
  delete[] alle;
}

template <typename TupleType, size_t source, size_t target, size_t time,
          typename HF, typename EF>
void 
CompressedSparse<TupleType, source, target, time, HF, EF>::addEdge(
  TupleType tuple)
{
  // Updating time in a somewhat unsafe manner that should generally work.
  //uint64_t tupleTime = convert(std::get<time>(tuple));
  double tupleTime = std::get<time>(tuple);
  if (tupleTime > currentTime.load()) {
    currentTime.store(tupleTime);
  }

  SourceType s = std::get<source>(tuple);
  size_t index = hash(s) % capacity;

  std::lock_guard<std::mutex> lock(mutexes[index]);

  bool found = false;
  std::list<TupleType>* emptyListPtr = 0;
  for (auto & l : alle[index]) {
    if (l.size() > 0) {
      try {
        SourceType s0 = std::get<source>(l.front());  
        if (equal(s, s0)) 
        {
          found = true;
          l.push_back(tuple);
          break;
        }
      } catch (std::exception e) {
        std::string message = std::string("addEdge: Error accessing first "
          "element of list") + e.what();  
        throw CompressedSparseException(message);
          
      }
    } else {
      emptyListPtr = &l;
    }
  }

  if (!found) {
    if (emptyListPtr) {
      emptyListPtr->push_back(tuple);
    } else {
      alle[index].push_back(std::list<TupleType>());
      alle[index].back().push_back(tuple);
    }
  } else {
    cleanupEdges(index);
  }
}

template <typename TupleType, size_t source, size_t target, size_t time,
          typename HF, typename EF>
void
CompressedSparse<TupleType, source, target, time, HF, EF>::cleanupEdges(
  size_t index)
{
  // Should only be called by addEdge which has this entry locked out.
  for( auto & l : alle[index]) {
    while (l.size() > 0 && 
           currentTime.load() - std::get<time>(l.front()) > window) {
      l.pop_front();
    }
  }
  
}


template <typename TupleType, size_t source, size_t target, size_t time,
          typename HF, typename EF>
size_t
CompressedSparse<TupleType, source, target, time, HF, EF>::countEdges()
const
{
   // For fun we parallelized it
  int numThreads = 4;
  std::vector<std::thread> threads;
  std::atomic<size_t> allCount(0);
  for (int i = 0; i < numThreads; i++) {
    threads.push_back(std::thread([i, numThreads, this, &allCount]() {
      size_t count = 0;
      int beg = get_begin_index(capacity, i, numThreads); 
      int end = get_end_index(capacity, i, numThreads); 
      for (int j = beg; j < end; j++) {
        for (auto l1 : this->alle[j]) {
          count += l1.size();
        }
      }
      allCount.fetch_add(count); 
    }));
  }

  for (int i = 0; i < numThreads; i++) {
    threads[i].join();
  }

  return allCount.load();
}


} // end namespace sam
#endif
