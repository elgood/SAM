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

  HF hash;
  EF equal;
  size_t capacity;
  std::mutex* mutexes;

  // array of lists of lists of edges
  std::list<std::list<TupleType>>* alle;

public:

  CompressedSparse(size_t capacity);
  ~CompressedSparse();

  void addEdge(TupleType tuple);

  /** 
   * Counts the number of edges in the graph.  Linear operation.
   */
  size_t countEdges() const;
};

template <typename TupleType, size_t source, size_t target, size_t time,
          typename HF, typename EF>
CompressedSparse<TupleType, source, target, time, HF, EF>::CompressedSparse(
  size_t capacity)
{
  this->capacity = capacity;
  mutexes = new std::mutex[capacity];

  alle = new std::list<std::list<TupleType>>[capacity];
}

template <typename TupleType, size_t source, size_t target, size_t time,
          typename HF, typename EF>
CompressedSparse<TupleType, source, target, time, HF, EF>::~CompressedSparse()
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
  SourceType s = std::get<source>(tuple);
  size_t index = hash(s) % capacity;

  std::lock_guard<std::mutex> lock(mutexes[index]);

  bool found = false;
  for (auto & l : alle[index]) {
    // Each list should have at least one element or the list would have
    // been deleted.
    try {
      SourceType s0 = std::get<source>(l.front());  
      //printf("s %s s0 %s\n", s.c_str(), s0.c_str());
      if (equal(s, s0)) 
      {
        //printf("found == true\n");
        found = true;
        l.push_back(tuple);
        //printf("l.size() %lu\n", l.size());
        break;
      }
    } catch (std::exception e) {
      std::string message = std::string("addEdge: Error accessing first "
        "element of list") + e.what();  
      throw CompressedSparseException(message);
        
    }
  }

  if (!found) {
    alle[index].push_back(std::list<TupleType>());
    alle[index].back().push_back(tuple);
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
