#ifndef COMPRESSED_SPARSE_HPP
#define COMPRESSED_SPARSE_HPP

#include <map>
#include <mutex>
#include <sam/Util.hpp>
#include <sam/EdgeRequest.hpp>
#include <thread>

namespace sam {

class CompressedSparseException : public std::runtime_error
{
public:
  CompressedSparseException(char const* message) : std::runtime_error(message){}
  CompressedSparseException(std::string message) : std::runtime_error(message){}

};

template <typename EdgeType, 
          size_t source, 
          size_t target, 
          size_t time,
          size_t duration,
          typename HF, //Hash function
          typename EF> //Equality function
class CompressedSparse
{
public:
  typedef typename EdgeType::LocalTupleType TupleType;
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
  std::list<std::list<EdgeType>>* alle;

  /**
   * For the given slot in the hash table (alle), we clear out edges that 
   * have expired (i.e. older than currentTime - window).
   * \return Returns the number edges deleted.
   */
  size_t cleanupEdges(size_t index);

  #ifdef METRICS
  mutable size_t totalEdgesAdded = 0;
  mutable size_t totalEdgesDeleted = 0; 
  #endif


public:

  /**
   * \param capacity How big the storage is.
   * \param window How big the time window is in seconds.
   */
  CompressedSparse(size_t capacity, double window);

  ~CompressedSparse();
  
  /**
   * Adds the given tuple to the graph.
   * \param id The id of the tuple/edge.
   * \param tuple The edge to be added.
   * \return Returns a number representing the amount of work.
   */
  size_t addEdge(EdgeType tuple);

  /**
   * Finds all edges that fulfill the given edgeRequest.
   * \param edgeRequest We find edges that match this edge request.
   * \param foundEdges We add an edges found to this list.
   */
  void findEdges(EdgeRequestType const& edgeRequest, 
                 std::list<EdgeType>& foundEdges) const;

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
                 std::list<EdgeType>& foundEdges) const;


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
                 std::list<EdgeType>& foundEdges) const;


  /** 
   * Counts the number of edges in the graph.  Linear operation.
   */
  size_t countEdges() const;

  #ifdef METRICS
  size_t getTotalEdgesAdded() const { return totalEdgesAdded; }
  size_t getTotalEdgesDeleted() const { return totalEdgesDeleted; }
  #endif

};

template <typename EdgeType, size_t source, size_t target, 
          size_t time, size_t duration,
          typename HF, typename EF>
CompressedSparse<EdgeType, source, target, time, duration, HF, EF>::
CompressedSparse( size_t capacity, double window ) :
  currentTime(0)
{
  this->capacity = capacity;
  this->window = window;

  mutexes = new std::mutex[capacity];

  alle = new std::list<std::list<EdgeType>>[capacity];
}

template <typename EdgeType, size_t source, size_t target, 
          size_t time, size_t duration,
          typename HF, typename EF>
CompressedSparse<EdgeType, source, target, time, duration, HF, EF>::
~CompressedSparse()
{
  delete[] mutexes;
  delete[] alle;
}

template <typename EdgeType, size_t source, size_t target,
          size_t time, size_t duration,
          typename HF, typename EF>
void
CompressedSparse<EdgeType, source, target, time, duration, HF, EF>::
findEdges(
  ReversedEdgeRequestType const& edgeRequest, 
  std::list<EdgeType>& foundEdges)
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

template <typename EdgeType, size_t source, size_t target, 
          size_t time, size_t duration,
          typename HF, typename EF>
void
CompressedSparse<EdgeType, source, target, time, duration, HF, EF>::
findEdges(
  EdgeRequestType const& edgeRequest, 
  std::list<EdgeType>& foundEdges)
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

template <typename EdgeType, size_t source, size_t target, 
          size_t time, size_t duration,
          typename HF, typename EF>
void
CompressedSparse<EdgeType, source, target, time, duration, HF, EF>::
findEdges(
  NodeType const& src,
  NodeType const& trg,
  double startTimeFirst,
  double startTimeSecond,
  double endTimeFirst,
  double endTimeSecond,
  std::list<EdgeType>& foundEdges)
const
{
  DEBUG_PRINT("CompressedSparse::findEdges src %s trg %s %f %f %f %f\n",
    src.c_str(), trg.c_str(),
    startTimeFirst, startTimeSecond, endTimeFirst, endTimeSecond);
  
  size_t index = hash(src) % capacity;

  std::lock_guard<std::mutex> lock(mutexes[index]);

  DEBUG_PRINT("CompressedSparse::findEdges src %s trg %s  number of lists"
    " to consider: %lu\n", src.c_str(), trg.c_str(), alle[index].size());
  for (auto & l : alle[index]) {
    // l should be a list of lists

    DEBUG_PRINT("CompressedSparse::findEdges number of edges to consider: "
      "%lu\n", l.size());

    if(l.size() > 0) {

      try {
        // All the tuples in each list should have the same source, so
        // look at the first one and see if it matches what we are looking
        // for.  
        SourceType s0 = std::get<source>(l.front().tuple);  
        if (equal(src, s0)) 
        {
          // If the first one matched on the source, look through
          // all other tuples in the list.
          for(auto it = l.begin(); it != l.end(); )
          {
            size_t id = it->id;
            TupleType tuple = it->tuple;
            DEBUG_PRINT("CompressedSparse::findEdges considering graph "                      "edge %s\n", sam::toString(tuple).c_str());

            // Check that the edge hasn't expired.
            DEBUG_PRINT("CompressedSparse::findEdges currentTime %f tupletype"
              " %f window %f \n", currentTime.load(), 
              std::get<time>(tuple), 
              window);

            if ( currentTime.load() - std::get<time>(tuple) < window) 
            {
              DEBUG_PRINT_SIMPLE("CompressedSparse::findEdges edge hasn't"
                " expired\n");
              
              bool passed = true;

              // Check to see if the source matches. It always should, so
              // throw an exception if it doesn't
              SourceType candSrc = std::get<source>(tuple);
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
                TargetType candTrg = std::get<target>(tuple);
                if (!equal(trg, candTrg))
                {
                  passed = false;
                }
              }
              DEBUG_PRINT("CompressedSparse::findEdges pass after checking "
                "source/target: %d\n", passed);

              if (passed) {
                double candTime = std::get<time>(tuple);
                double candDuration = std::get<duration>(tuple);
                // Check that the time is after starttime and 
                // before stoptime
                DEBUG_PRINT("CompressedSparse::findEdges candTime %f "
                  "candDuration %f "
                  "startTimeFirst %f startTimeSecond %f "
                  "endTimeFirst %f endTimeSecond %f\n",
                  candTime, candDuration, startTimeFirst, startTimeSecond,
                  endTimeFirst, endTimeSecond);
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

              DEBUG_PRINT("CompressedSparse::findEdges the edge has expired"
                " %s\n", toString(tuple).c_str());
              
              it = l.erase(it);
              METRICS_INCREMENT(this->totalEdgesDeleted)
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


template <typename EdgeType, size_t source, size_t target, 
          size_t time, size_t duration,
          typename HF, typename EF>
size_t 
CompressedSparse<EdgeType, source, target, time, duration, HF, EF>::addEdge(
  EdgeType edge)
{
  DEBUG_PRINT("CompressedSparse::addEdge tuple %s\n",  
              edge.toString().c_str());
  METRICS_INCREMENT(totalEdgesAdded)

  TupleType tuple = edge.tuple;

  // Updating time in a somewhat unsafe manner that should generally work.
  //uint64_t tupleTime = convert(std::get<time>(tuple));
  double tupleTime = std::get<time>(tuple);
  DEBUG_PRINT("CompressedSparse::addEdge tupleTime %f currentTime %f\n",
    tupleTime, currentTime.load());
  if (tupleTime > currentTime.load()) {
    currentTime.store(tupleTime);
  }
  DEBUG_PRINT("CompressedSparse::addEdge tupleTime %f currentTime %f\n",
    tupleTime, currentTime.load());

  SourceType s = std::get<source>(tuple);
  size_t index = hash(s) % capacity;

  DEBUG_PRINT("CompressedSparse::addEdge index %lu for tuple %s\n",  
    index, sam::toString(tuple).c_str());

  std::lock_guard<std::mutex> lock(mutexes[index]);

  // If we find a list that has entries where the source is the same
  // as tuple's source, this is set to true.
  bool found = false;
  std::list<EdgeType>* emptyListPtr = 0;
  size_t work = alle[index].size();
  DEBUG_PRINT("CompressedSparse::addEdge size of bin %lu: %lu\n",
    index, alle[index].size());
  for (auto & l : alle[index]) {
    if (l.size() > 0) {
      DEBUG_PRINT("CompressedSparse::addEdge index %lu l.size %lu\n", 
        index, l.size());
      try {
        SourceType s0 = std::get<source>(l.front().tuple);  
        DEBUG_PRINT("CompressedSparse::addEdge s0 %s s %s for tuple %s\n",  
          s0.c_str(), s.c_str(), sam::toString(tuple).c_str());

        if (equal(s, s0)) 
        {
          DEBUG_PRINT("CompressedSparse::addEdge found list for tuple %s\n",
            sam::toString(tuple).c_str());
          found = true;
          l.push_back(edge);
          break;
        }
      } catch (std::exception e) {
        std::string message = std::string("addEdge: Error accessing first "
          "element of list") + e.what();  
        throw CompressedSparseException(message);
          
      }
    } else {
      DEBUG_PRINT_SIMPLE("CompressedSparse::addEdge pointing to emptylist\n");
      emptyListPtr = &l;
    }
  }

  // If we didn't find a list that has entries with the same source
  // as tuple, then we need to create a new list or use an empty one.
  if (!found) {
    DEBUG_PRINT("CompressedSparse::addEdge didn't find list for tuple %s\n",  
              sam::toString(tuple).c_str());
    work += 1;
    // If we found an empty list, use that list.
    if (emptyListPtr) {
      DEBUG_PRINT("CompressedSparse::addEdge found empty list for tuple %s\n",  
              sam::toString(tuple).c_str());
      emptyListPtr->push_back(edge);
    } else {
      // No empty lists, so we need to add another list to this slot
      DEBUG_PRINT("CompressedSparse::addEdge creating list for tuple %s\n",  
              sam::toString(tuple).c_str());
      alle[index].push_back(std::list<EdgeType>());
      alle[index].back().push_back(edge);
    }
  } else {
    // If we did find a list, we can clean up edges that have expired.
    work += cleanupEdges(index);
  }
  return work;
}

template <typename EdgeType, size_t source, size_t target, 
          size_t time, size_t duration,
          typename HF, typename EF>
size_t
CompressedSparse<EdgeType, source, target, time, duration, HF, EF>::
cleanupEdges( size_t index )
{
  // Should only be called by addEdge and (maybe) findEdges, 
  // which has this slot (alle[index]) locked out.
  size_t work = 0;
  for( auto & l : alle[index]) {
    while (l.size() > 0 && 
           currentTime.load() - std::get<time>(l.front().tuple) > window) {
      work++;
      DEBUG_PRINT("Deleting edge currentTime %f front time %f "
        "window %f\n", currentTime.load(), std::get<time>(l.front().tuple),
        window);
      l.pop_front();
      METRICS_INCREMENT(totalEdgesDeleted)
    }
  }
  return work;
}


template <typename EdgeType, size_t source, size_t target, 
          size_t time, size_t duration,
          typename HF, typename EF>
size_t
CompressedSparse<EdgeType, source, target, time, duration, HF, EF>::
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
