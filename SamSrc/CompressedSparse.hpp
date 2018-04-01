#ifndef COMPRESSED_SPARSE_HPP
#define COMPRESSED_SPARSE_HPP

#include <Edge.hpp>
#include <map>
#include <mutex>
#include "Util.hpp"
#include "EdgeRequest.hpp"
#include <thread>

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
          size_t duration,
          typename HF, //Hash function
          typename EF> //Equality function
class CompressedSparse
{
public:
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;
  typedef SourceType NodeType; // SourceType and TargetType should be the same.
  typedef EdgeRequest<TupleType, source, target> EdgeRequestType;
  typedef EdgeRequest<TupleType, target, source> ReversedEdgeRequestType;

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

  /**
   * How many slots there are in alle (the array of lists of lists of edges).
   * Each slot has a mutex associated with it so that only one thread can
   * access the slot at one time.
   */
  size_t capacity; 
 
  /**
   * A mutex for each element in alle.  This help us reduce thread contention.
   */ 
  std::mutex* mutexes;

  // array of lists of lists of edges
  std::list<std::list<TupleType>>* alle;

  /**
   * For the given slot in the hash table (alle), we clear out edges that 
   * have expired (i.e. older than currentTime - window).
   */
  void cleanupEdges(size_t index);

  /**
   * Called by the public findEdges methods, this is the logic common
   * to both.
   * \param src The source to look up, or nullValue<NodeType>() if not set.
   * \param trg The target to look up, or nullValue<NodeType>() if not set.
   * \param startTimeFirst By when the edge should have started 
   * \param startTimeSecond Before when the edge should have started
   * \param endTimeFirst By when the edge should have finished.
   * \param endTimeSecond Before when the edge should have finished. 
   * \param foundEdges We add an edges found to this list.
   */
  void findEdges(NodeType const& src, NodeType const& trg, 
                 double startTimeFirst, double startTimeSecond,
                 double endTimeFirst, double endTimeSecond,
                 std::list<TupleType>& foundEdges) const;

public:

  /**
   * \param capacity How big the storage is.
   * \param window How big the time window is in seconds.
   */
  CompressedSparse(size_t capacity, double window);

  ~CompressedSparse();
  
  /**
   * Adds the given tuple to the graph.
   */
  void addEdge(TupleType tuple);

  /**
   * Finds all edges that fulfill the given edgeRequest.
   * \param edgeRequest We find edges that match this edge request.
   * \param foundEdges We add an edges found to this list.
   */
  void findEdges(EdgeRequestType const& edgeRequest, 
                 std::list<TupleType>& foundEdges) const;

  /**
   * The source and target have been swapped, meaning that we need
   * to treat the source as the target and the target as the source.
   * This is generally the method used when this object is being used
   * as a compressed sparse column graph instead of a compressed sparse
   * row graph.
   * \param edgeRequest We find edges that match this edge request.
   * \param foundEdges We add an edges found to this list.
   */ 
  void findEdges(ReversedEdgeRequestType const& edgeRequest,
                 std::list<TupleType>& foundEdges) const;



  /** 
   * Counts the number of edges in the graph.  Linear operation.
   */
  size_t countEdges() const;
};

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration,
          typename HF, typename EF>
CompressedSparse<TupleType, source, target, time, duration, HF, EF>::
CompressedSparse( size_t capacity, double window ) :
  currentTime(0)
{
  this->capacity = capacity;
  this->window = window;

  mutexes = new std::mutex[capacity];

  alle = new std::list<std::list<TupleType>>[capacity];
}

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration,
          typename HF, typename EF>
CompressedSparse<TupleType, source, target, time, duration, HF, EF>::
~CompressedSparse()
{
  delete[] mutexes;
  delete[] alle;
}

template <typename TupleType, size_t source, size_t target,
          size_t time, size_t duration,
          typename HF, typename EF>
void
CompressedSparse<TupleType, source, target, time, duration, HF, EF>::
findEdges(
  ReversedEdgeRequestType const& edgeRequest, 
  std::list<TupleType>& foundEdges)
const
{
  // If we've been given an edge request that is reversed, that means
  // the source is the target and the target is the source.
  NodeType src = edgeRequest.getTarget();
  NodeType trg = edgeRequest.getSource();

  double startTimeFirst = edgeRequest.getStartTimeFirst();
  double startTimeSecond = edgeRequest.getStartTimeSecond();
  double endTimeFirst = edgeRequest.getEndTimeFirst();
  double endTimeSecond = edgeRequest.getEndTimeSecond();

  findEdges(src, trg, startTimeFirst, startTimeSecond, 
            endTimeFirst, endTimeSecond, foundEdges);
}

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration,
          typename HF, typename EF>
void
CompressedSparse<TupleType, source, target, time, duration, HF, EF>::
findEdges(
  EdgeRequestType const& edgeRequest, 
  std::list<TupleType>& foundEdges)
const
{
  NodeType src = edgeRequest.getSource();
  NodeType trg = edgeRequest.getTarget();

  double startTimeFirst = edgeRequest.getStartTimeFirst();
  double startTimeSecond = edgeRequest.getStartTimeSecond();
  double endTimeFirst = edgeRequest.getEndTimeFirst();
  double endTimeSecond = edgeRequest.getEndTimeSecond();

  findEdges(src, trg, startTimeFirst, startTimeSecond,
            endTimeFirst, endTimeSecond, foundEdges);
}

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration,
          typename HF, typename EF>
void
CompressedSparse<TupleType, source, target, time, duration, HF, EF>::
findEdges(
  NodeType const& src,
  NodeType const& trg,
  double startTimeFirst,
  double startTimeSecond,
  double endTimeFirst,
  double endTimeSecond,
  std::list<TupleType>& foundEdges)
const
{
  #ifdef DEBUG
  printf("CompressedSparse::findEdges src %s trg %s %f %f %f %f\n",
    src.c_str(), trg.c_str(),
    startTimeFirst, startTimeSecond, endTimeFirst, endTimeSecond);
  #endif
  
  size_t index = hash(src) % capacity;

  std::lock_guard<std::mutex> lock(mutexes[index]);

  #ifdef DEBUG
  printf("CompressedSparse::findEdges number of lists to consider: %lu\n",
    alle[index].size());
  #endif
  for (auto & l : alle[index]) {
    // l should be a list of lists

    #ifdef DEBUG
    printf("CompressedSparse::findEdges number of edges to consider: %lu\n",
      l.size());
    #endif

    if(l.size() > 0) {

      try {
        // All the tuples in each list should have the same source, so
        // look at the first one and see if it matches what we are looking
        // for.  
        SourceType s0 = std::get<source>(l.front());  
        if (equal(src, s0)) 
        {
          // If the first one matched on the source, look through
          // all other tuples in the list.
          for(auto it = l.begin(); it != l.end(); )
          {

            #ifdef DEBUG
            printf("CompressedSparse::findEdges considering graph edge %s\n",
              sam::toString(*it).c_str());
            #endif

            // Check that the edge hasn't expired.
            #ifdef DEBUG
            printf("CompressedSparse::findEdges currentTime %f tupletype"
              " %f window %f \n", currentTime.load(), std::get<time>(*it), 
              window);
            #endif

            if ( currentTime.load() - std::get<time>(*it) < window) 
            {
              #ifdef DEBUG
              printf("CompressedSparse::findEdges edge hasn't expired\n");
              #endif
              
              bool passed = true;

              // Check to see if the source matches. It always should, so
              // throw an exception if it doesn't
              SourceType candSrc = std::get<source>(*it);
              if (!equal(src, candSrc))
              {
                std::string message = "CompressedSpare::findEdges: Found an "
                  "edge where the source doesn't match the source of the "
                  "first edge.  This is a logical error.";
                throw CompressedSparseException(message);
              }

              // Check to see if the target matches if the target is defined
              // in the edge request.
              if (!isNull(trg)) {
                TargetType candTrg = std::get<target>(*it);
                if (!equal(trg, candTrg))
                {
                  passed = false;
                }
              }
              #ifdef DEBUG
              printf("CompressedSparse::findEdges pass after checking"
                "source/target: %d\n", passed);
              #endif

              if (passed) {
                // Check that the time is after starttime and 
                // before stoptime
                double candTime = std::get<time>(*it);
                double candDuration = std::get<duration>(*it);
                if (candTime < startTimeFirst ||
                    candTime > startTimeSecond ||
                    candTime + candDuration < endTimeFirst ||
                    candTime + candDuration > endTimeSecond)
                {
                  passed = false;
                }
              }
              if (passed) {
                foundEdges.push_back(*it);
              }
              ++it;
            } else {
              // The edge has expired, so we get rid of it.

              #ifdef DEBUG
              printf("CompressedSparse::findEdges the edge has expired\n");
              #endif
              
              it = l.erase(it);
            }
          }
        }
      } catch (std::exception e) {
        std::string message = std::string("addEdge: Error accessing first "
          "element of list") + e.what();  
        throw CompressedSparseException(message);
      }
    }
  }
}


template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration,
          typename HF, typename EF>
void 
CompressedSparse<TupleType, source, target, time, duration, HF, EF>::addEdge(
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

  // If we find a list that has entries where the source is the same
  // as tuple's source, this is set to true.
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

  // If we didn't find a list that has entries with the same source
  // as tuple, then we need to create a new list or use an empty one.
  if (!found) {

    // If we found an empty list, use that list.
    if (emptyListPtr) {
      emptyListPtr->push_back(tuple);
    } else {
      // No empty lists, so we need to add another list to this slot
      alle[index].push_back(std::list<TupleType>());
      alle[index].back().push_back(tuple);
    }
  } else {
    // If we did find a list, we can clean up edges that have expired.
    cleanupEdges(index);
  }
}

template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration,
          typename HF, typename EF>
void
CompressedSparse<TupleType, source, target, time, duration, HF, EF>::
cleanupEdges( size_t index )
{
  // Should only be called by addEdge and (maybe) findEdges, 
  // which has this slot (alle[index]) locked out.
  for( auto & l : alle[index]) {
    while (l.size() > 0 && 
           currentTime.load() - std::get<time>(l.front()) > window) {
      l.pop_front();
    }
  }
  
}


template <typename TupleType, size_t source, size_t target, 
          size_t time, size_t duration,
          typename HF, typename EF>
size_t
CompressedSparse<TupleType, source, target, time, duration, HF, EF>::
countEdges()
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
