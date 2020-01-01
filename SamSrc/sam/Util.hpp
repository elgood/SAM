#ifndef SAM_UTIL_HPP
#define SAM_UTIL_HPP

#include <boost/lexical_cast.hpp>
#include <boost/tokenizer.hpp>
#include <numeric>
#include <iostream>
#include <queue>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <zmq.hpp>
#include <mutex>
#include <list>
#include <memory>
#include <thread>
#include <atomic>


namespace sam {


#ifdef DETAIL_TIMING
  #define DETAIL_TIMING_BEG1 \
  auto detailTimingBegin = std::chrono::high_resolution_clock::now();
  
  #define DETAIL_TIMING_BEG2 \
  detailTimingBegin = std::chrono::high_resolution_clock::now();

  #define DETAIL_TIMING_END1(var) \
  auto detailTimingEnd = std::chrono::high_resolution_clock::now();\
  auto detailTimingDiff = \
    std::chrono::duration_cast<std::chrono::duration<double>>(\
      detailTimingEnd - detailTimingBegin);\
  var += detailTimingDiff.count();

  #define DETAIL_TIMING_END_TOL1(nodeId, var, tolerance, message) \
  auto detailTimingEnd = std::chrono::high_resolution_clock::now();\
  auto detailTimingDiff = \
    std::chrono::duration_cast<std::chrono::duration<double>>(\
      detailTimingEnd - detailTimingBegin);\
  double localDiff = detailTimingDiff.count(); \
  if (localDiff > tolerance) { \
    printf("Node %lu Time tolerance exceeded: %f %s\n", nodeId, localDiff,\
            message); \
  }\
  var += detailTimingDiff.count();


  #define DETAIL_TIMING_END2(var) \
  detailTimingEnd = std::chrono::high_resolution_clock::now();\
  detailTimingDiff = std::chrono::duration_cast<std::chrono::duration<double>>(\
    detailTimingEnd - detailTimingBegin);\
  var += detailTimingDiff.count();

  #define DETAIL_TIMING_END_TOL2(nodeId, var, tolerance, message)\
  detailTimingEnd = std::chrono::high_resolution_clock::now();\
  detailTimingDiff = \
    std::chrono::duration_cast<std::chrono::duration<double>>(\
      detailTimingEnd - detailTimingBegin);\
  localDiff = detailTimingDiff.count(); \
  if (localDiff > tolerance) { \
    printf("Node %lu Time tolerance exceeded: %f %s\n", nodeId, localDiff,\
            message); \
  }\
  var += detailTimingDiff.count();

#else
  #define DETAIL_TIMING_BEG1
  #define DETAIL_TIMING_BEG2
  #define DETAIL_TIMING_END1(var)
  #define DETAIL_TIMING_END2(var)
  #define DETAIL_TIMING_END_TOL1(nodeId, var, tolerance, message)
  #define DETAIL_TIMING_END_TOL2(nodeId, var, tolerance, message)
#endif

#ifdef METRICS
  #define METRICS_INCREMENT(var) var++;
#else
  #define METRICS_INCREMENT(var) 
#endif

#ifdef DEBUG
  #define DEBUG_PRINT(message, ...) printf(message, __VA_ARGS__);
#else
  #define DEBUG_PRINT(message, ...) 
#endif


class UtilException : public std::runtime_error {
public:
  UtilException(char const * message) : std::runtime_error(message) { } 
  UtilException(std::string message) : std::runtime_error(message) { } 
};



/**
 * Generates a subtuple based on the provided index_sequence.
 */
template <typename... T, std::size_t... I>
auto subtuple(std::tuple<T...> const& t, std::index_sequence<I...>) {
  return std::make_tuple(std::get<I>(t)...);
}

/**
 * Base cae for generateKey.
 */
template <typename...  Ts>
std::string generateKey(std::tuple<Ts...> const& t)
{
  return "";
}

/**
 * Generates a key based on the keyfields provided to the template.
 */
template <size_t keyField, size_t... keyFields, typename... Ts>
std::string generateKey(std::tuple<Ts...> const& t)
{
  std::string key = boost::lexical_cast<std::string>(std::get<keyField>(t));
  return key + generateKey<keyFields...>(t);
}


/**
 * Base case for tupleToString
 */
template <int I = 0, typename... Tp>
inline typename std::enable_if<I == sizeof...(Tp), std::string>::type
tupleToString(std::tuple<Tp...> const&)
{
  return "";
}

/**
 * Generates a string from the tuple.
 */
template<int I = 0, typename... Tp>
inline typename std::enable_if<I < sizeof...(Tp), std::string>::type
tupleToString(std::tuple<Tp...>const& t)
{
  std::string result = boost::lexical_cast<std::string>(std::get<I>(t));
  result = result + "," + tupleToString<I + 1, Tp...>(t);
  if (result[result.size() - 1] == ',') {
    result = result.substr(0, result.size()-1);
  }
  return result;
}

template<typename... Tp>
std::string toString(std::tuple<Tp...>const& t) {
  std::string result = tupleToString(t);
  return result;
}

/**
 * Hash function for strings.
 */
inline
uint64_t hashFunction(std::string const& key) 
{
  uint64_t hash = 0;
  
  for (int i = 0; i < key.size(); i++) {
    hash = key[i] + (hash << 6) + (hash << 16) - hash;
  }

  return hash;
}

/**
 * A function object for hashing strings using std::hash.
 */
class StringHashFunction
{
public:
  inline
  uint64_t operator()(std::string const& s) const {
    return std::hash<std::string>{}(s);
  }
};

/**
 * A templated function object for hashing string using std::hash.
 * The first template parameter is the tuple type, which is passed to
 * the () operator.  The Index parameter indicates which tuple element
 * should be extracted from the tuple and then hashed.
 */
template <typename TupleType, int Index>
class TupleStringHashFunction
{
public:
  inline
  uint64_t operator()(TupleType const& tuple) const {
    return std::hash<std::string>{}(std::get<Index>(tuple));
  }
};

/**
 * Hash function for ints
 */
inline
uint64_t hashFunction(uint64_t key)
{
  return key * 31280644937747LL;
}

class UnsignedIntHashFunction
{
public:
  inline 
  uint64_t operator()(uint64_t key) const {
    return hashFunction(key);
  }
};

/**
 * This is used for testing purposes.  It assumes the strings are ip4 addresses.
 * It just looks at the last octet and returns that.  This is useful so that
 * we can create testing situations where we know where ip addresses will
 * go by design, rather than randomly.
 */
class LastOctetHashFunction
{
public:
  inline
  uint64_t operator()(std::string const& s) const {
    size_t index = s.find_last_of(".");
    std::string lastOctet = s.substr(index + 1);
    return boost::lexical_cast<uint64_t>(lastOctet);
  }
};

class TimeConversionFunction
{
public:
  inline
  uint64_t operator()(double time) const {
    return static_cast<uint64_t>(time * 10000000);
  }
};

class StringEqualityFunction
{
public:
  inline
  bool operator()(std::string const& s1, std::string const& s2) const
  {
    return s1.compare(s2) == 0;
  }
};

template <typename T>
double calcMean(T const& v)
{
  double sum = std::accumulate(v.begin(), v.end(), 0.0);
  return sum / v.size();
}

//template <typename T>
//double calcMean(std::deque<T> const& v)
//{
//  double sum = std::accumulate(v.begin(), v.end(), 0.0);
//  return sum / v.size();
//}

template <typename T>
double calcStandardDeviation(T const& v)
{
  double mean = calcMean(v);

  std::vector<double> diff(v.size());
  std::transform(v.begin(), v.end(), diff.begin(), 
    [mean](double x) { return x - mean; });
  double sq_sum = std::inner_product(diff.begin(), diff.end(), diff.begin(), 
                                     0.0);
  return std::sqrt(sq_sum / v.size());
}

/**
 * Converts a netflow string into a vetor of tokens.
 */
inline
std::vector<std::string> convertToTokens(std::string netflowString) {
  boost::char_separator<char> sep(",");
  boost::tokenizer<boost::char_separator<char>> tokenizer(netflowString, sep);
  std::vector<std::string> v;
  for ( std::string t : tokenizer ) {
    v.push_back(t);
  }
  return v;
}


std::string getIpString(std::string hostname) {
  hostent* hostInfo = gethostbyname(hostname.c_str());
  in_addr* address = (in_addr*)hostInfo->h_addr;
  std::string ip = inet_ntoa(* address);
  return ip;
}

/**
 * Given a string, creates a zmq message with the string as the data.
 * \param str The string that is to be the data of the message.
 * \return Returns a zmq::message_t with the string as the data.
 */
zmq::message_t fillZmqMessage(std::string const& str)
{
  zmq::message_t message(str.length());
  char* dataptr = (char*) message.data();
  std::copy(str.begin(), str.end(), dataptr);
  return message;
}

template<typename... Tp>
inline zmq::message_t tupleToZmq(std::tuple<Tp...>const& t)
{
  std::string str = toString(t);
  zmq::message_t message = fillZmqMessage(str);

  return message;
}

inline
size_t get_begin_index(size_t num_elements, size_t stream_id, 
                       size_t num_streams)
{
  return static_cast<size_t>((static_cast<double>(num_elements) / num_streams) *
                        stream_id);
}

inline
size_t get_end_index(size_t num_elements, size_t stream_id, 
                       size_t num_streams)
{
  return (stream_id + 1 < num_streams) ?
    static_cast<size_t>((static_cast<double>(num_elements) / num_streams) *
                       (stream_id + 1)) :
    num_elements;
 
}

inline
void
createPushSockets(
    zmq::context_t* context,
    size_t numNodes,
    size_t nodeId,
    std::vector<std::string>& hostnames,
    std::vector<size_t>& ports,
    std::vector<std::shared_ptr<zmq::socket_t>>& pushers,
    uint32_t hwm)
{
  pushers.resize(numNodes);
  for (int i = 0; i < numNodes; i++) 
  {
    if (i != nodeId) // never need to send stuff to itself
    {
      /////////// adding push sockets //////////////
      auto pusher = std::shared_ptr<zmq::socket_t>(
                      new zmq::socket_t(*context, ZMQ_PUSH));

      std::string ip = getIpString(hostnames[nodeId]);
      std::string url = "";
      url = "tcp://" + ip + ":";
      try { 
        url = url + boost::lexical_cast<std::string>(ports[i]);
      } catch (std::exception e) {
        throw UtilException(e.what());
      }

      // the function complains if you use std::size_t, so be sure to use the
      // uint32_t class member for hwm.
      try {
        pusher->setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));
      } catch (std::exception e) {
        std::string message = std::string("problem setting push socket's send")+
          std::string(" high water mark: ") + e.what();
        throw UtilException(message);
      }

      try {
        uint32_t timeout = 10; //milliseconds
        pusher->setsockopt(ZMQ_SNDTIMEO, &timeout, sizeof(timeout));
      } catch (std::exception e) {
        std::string message = std::string("problem setting push socket's ")+
          std::string(" timeout: ") + e.what();
        throw UtilException(message);
      }

      try {
        #ifdef DEBUG
        printf("Node %lu createPushSockets binding to %s\n", nodeId, 
          url.c_str());
        #endif
        pusher->bind(url);
      } catch (std::exception e) {
        std::string message = "Node " +
          boost::lexical_cast<std::string>(nodeId) +
          " couldn't bind to url " + url + ": " + e.what();
        throw UtilException(message);
      }
      pushers[i] = pusher;
    } 
  }
}



namespace numTrianglesDetails
{

/// Struct for holding the intermediate results.
template<typename TupleType, size_t source, size_t target, size_t time,
         size_t duration>
struct PartialTriangle {
  size_t numEdges = 0;
  TupleType netflow1;
  TupleType netflow2;

  std::string toString() const {
    std::string rString = "numEdges " + 
      boost::lexical_cast<std::string>(numEdges) + " ";
    if (numEdges > 0) {
      rString += sam::toString(netflow1) + " ";
    } 
    if (numEdges > 1) {
      rString += sam::toString(netflow2);
    }
    return rString;
  }

  /**
   * Determines if the intermediate result is expired by looking at the 
   * provided current time and seeing if we are still in the allowed
   * time window.
   */
  bool isExpired(double currentTime, double timeWindow) {
    double startTime = std::get<time>(netflow1);
    if (currentTime - startTime <= timeWindow) {
      DEBUG_PRINT("isExpired not expired startTime %f currentTime %f "
        "currentTime - startTime %f timeWindow %f comparison %f\n",
        startTime, currentTime, currentTime - startTime, timeWindow,
        currentTime - startTime - timeWindow);
      return false;
    }
    DEBUG_PRINT("isExpired is expired startTime %f currentTime %f "
      "currentTime - startTime %f timeWindow %f comparison %f\n",
      startTime, currentTime, currentTime - startTime, timeWindow,
      currentTime - startTime - timeWindow);
    return true;
  }

  /*size_t operator()(PartialTriangle const& partial) const 
  {
    if (partial.numEdges == 1) {
      auto trg = std::get<target>(partial.netflow1);
      return hashFunction(trg);        
    }
    if (partial.numEdges == 2) {
      auto trg = std::get<target>(partial.netflow2);
      return hashFunction(trg);        
    }
    throw UtilException("Tried to hash a partial triangle but the"
      " number of edges was not 1 or 2."); 
  }

  bool operator==(PartialTriangle const& other) const
  {
    if (numEdges != other.numEdges) {
      return false;
    }
    if (numEdges >= 1) {
      std::string s1 = toString(netflow1);
      std::string s2 = toString(other.netflow1);
      if (s1.compare(s2) != 0) {
        return false;
      } 
    }

    if (numEdges == 2) {
      std::string s1 = toString(netflow2);
      std::string s2 = toString(other.netflow2);
      if (s1.compare(s2) != 0) {
        return false;
      } 
    }

    return true;
  }*/

};

template <typename T>
size_t countPartials(std::vector<T> const * alr,
                     size_t tableSize) 
{
  size_t count = 0;
  for (size_t i = 0; i < tableSize; i++) {
    count += alr[i].size();
  }
  return count;
}

template<typename TupleType, size_t source, size_t target, size_t time,
         size_t duration>
void processSingleEdgePartial(
  PartialTriangle<TupleType, source, target, time, duration> partial, 
  std::vector<PartialTriangle<TupleType, source, target, time, 
             duration>> & newPartials,
  TupleType tuple,
  double queryTime)
{
  typedef PartialTriangle<TupleType, source, target, time, duration>
    PartialTriangleType;
  DEBUG_PRINT("processSingleEdgePartial: processing tuple %s\n", 
    toString(tuple).c_str());

  auto id1 = std::get<0>(partial.netflow1);
  auto id2 = std::get<0>(tuple);
  if (id1 != id2) {
    DEBUG_PRINT("processSingleEdgePartial: id1 %lu != id2 %lu\n", id1, id2);

    auto trg1 = std::get<target>(partial.netflow1);
    auto src2 = std::get<source>(tuple);
    if (trg1 == src2) 
    {
      DEBUG_PRINT("processSingleEdgePartial: trg1 %s == src2 %s\n",
        trg1.c_str(), src2.c_str());
      double t1 = std::get<time>(partial.netflow1);
      double t2 = std::get<time>(tuple);

      // Inforces strictly increasing times
      if (t1 < t2) 
      {
        DEBUG_PRINT("processSingleEdgePartial: t1 %f < t2 %f\n", t1, t2);
        double dur = std::get<duration>(tuple);
        if (t2 - t1 <= queryTime) {
          PartialTriangleType newPartial;
          newPartial.numEdges = 2;
          newPartial.netflow1 = partial.netflow1;
          newPartial.netflow2 = tuple;  

          DEBUG_PRINT("processSingleEdgePartial: newpartial %f %s %s, "
             " %f %s %s\n",
             std::get<time>(newPartial.netflow1),
             std::get<source>(newPartial.netflow1).c_str(),
             std::get<target>(newPartial.netflow1).c_str(),
             std::get<time>(newPartial.netflow2),
             std::get<source>(newPartial.netflow2).c_str(),
             std::get<target>(newPartial.netflow2).c_str());

          newPartials.push_back(newPartial);
        }
      }
    }
  }
}

template<typename TupleType, size_t source, size_t target, size_t time,
         size_t duration>
void processTwoEdgePartial(
  PartialTriangle<TupleType, source, target, time, duration> partial, 
  std::atomic<size_t>& numTriangles,
  TupleType tuple,
  double queryTime )
{
  typedef PartialTriangle<TupleType, source, target, time, duration>
    PartialTriangleType;

  auto id1 = std::get<0>(partial.netflow1);
  auto id2 = std::get<0>(partial.netflow2);
  auto id3 = std::get<0>(tuple);

  DEBUG_PRINT("processTwoEdgePartial: partial has 2 edges, ids of partial "
    "id1 %lu id2 %lu id3 %lu, tuple under consideration %s\n", 
    id1, id2, id3, toString(tuple).c_str());

  auto trg2 = std::get<target>(partial.netflow2);
  auto src3 = std::get<source>(tuple);
  DEBUG_PRINT("processTwoEdgePartial: seeing if trg2 %s = src3 %s for tuple"
    " %s \n", trg2.c_str(), src3.c_str(), 
    toString(tuple).c_str());
    
  if (trg2 == src3) {
    DEBUG_PRINT("procesTwoEdgePartial trg2 == src3 for tuple %s \n",
      toString(tuple).c_str());
    auto trg3 = std::get<target>(tuple) ;
    auto src1 = std::get<source>(partial.netflow1);
    if (trg3 == src1) {
      DEBUG_PRINT("processTwoEdgePartial trg3 %s =  src1 %s for tuple %s\n",
        trg3.c_str(), src1.c_str(), toString(tuple).c_str() ); 
      double t1 = std::get<time>(partial.netflow1);
      double t2 = std::get<time>(partial.netflow2);
      double t3 = std::get<time>(tuple);
      double dur = std::get<duration>(tuple);

      // Inforces strictly increasing times
      DEBUG_PRINT("processTwoEdgePartial checking increasing time tuple "
        "%s t3 %f t2 %f t1 %f dur %f queryTime %f"
        "\n", toString(tuple).c_str(), t3, t2, t1, dur, 
        queryTime);
      if (t3 > t2 && t3  -t1 <= queryTime) {

        DEBUG_PRINT("found triangle edge1 %lu %f %s "
          "%s, edge2 %lu %f %s %s, "
          "edge3 %lu %f %s %s\n", 
          std::get<0>(partial.netflow1),
          std::get<time>(partial.netflow1),
          std::get<source>(partial.netflow1).c_str(),
          std::get<target>(partial.netflow1).c_str(),
          std::get<0>(partial.netflow2),
          std::get<time>(partial.netflow2),
          std::get<source>(partial.netflow2).c_str(),
          std::get<target>(partial.netflow2).c_str(),
          std::get<0>(tuple),
          std::get<time>(tuple),
          std::get<source>(tuple).c_str(),
          std::get<target>(tuple).c_str());

        numTriangles.fetch_add(1);
      }
    }
  }
}


} //End numTrianglesDetails

/**
 * This is an alternate method for calculating triangles.  It is used to 
 * validate the parallel framework to make sure they get the same answer.
 * This checks that all the edges have strictly increasing times
 * and that all the edges occur within the specified time frame.
 * \param l, a vector of tuples.
 * \param queryTime, the length of the query
 */
template<typename TupleType, size_t source, size_t target, size_t time,
         size_t duration>
size_t numTriangles(std::vector<TupleType> l, double queryTime)
{
  using namespace sam::numTrianglesDetails;
  
  typedef PartialTriangle<TupleType, source, target, time, duration>
    PartialTriangleType;

  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;

  // TODO: parallelize sort
  #ifdef DETAIL_TIMING
  double totalTimeSort = 0;
  #endif
  DETAIL_TIMING_BEG1
  std::sort(l.begin(), l.end(), [](TupleType const& t1, 
                                   TupleType const& t2) ->bool
  {
    return std::get<time>(t1) < std::get<time>(t2);  
  });
  DETAIL_TIMING_END1(totalTimeSort);
  #ifdef DETAIL_TIMING
  printf("numTriangles time to sort %f\n", totalTimeSort);
  #endif

  // Set the id to be the sort order.
  for (size_t i = 0; i < l.size(); i++) {
    std::get<0>(l[i]) = i;
  }

  //std::vector<PartialTriangleType> partialTriangles;

  std::atomic<size_t> numTriangles(0);

  size_t numThreads = std::thread::hardware_concurrency();
  //size_t numThreads = 1;
  DEBUG_PRINT("numTriangles numThreads %lu\n", numThreads); 

  size_t tableSize = 10000;
  DEBUG_PRINT("numTriangles table_size %lu\n", tableSize);
  std::mutex* mutexes = new std::mutex[tableSize];
  std::list<PartialTriangleType>* alr = 
    new std::list<PartialTriangleType>[tableSize];

  TupleStringHashFunction<TupleType, source> hashSource;

  size_t numProcessed = 0;

  for (auto tuple : l) 
  {
    // New partial triangles that arise from processing this tuple
    // are added to this data structure (in parallel), and then
    // added back into partialTriangles after the parallel processing.
    std::vector<PartialTriangleType> newPartials;

    double currentTime = std::get<time>(tuple);

    DEBUG_PRINT("Beginning processing Tuple %s\n", 
      sam::toString(tuple).c_str());
    PartialTriangleType p;
    p.numEdges = 1;
    p.netflow1 = tuple;
    // A single edge is a partial triangle 
    DEBUG_PRINT("Adding to newPartials %s\n", p.toString().c_str());
    newPartials.push_back(p);
   

    std::vector<std::thread> threads;
    threads.resize(numThreads);

    // Lock used to add things to newPartials in a thread-safe way.
    std::mutex lock;
 
    //std::string src = std::get<source>(tuple);
    size_t index = hashSource(tuple) % tableSize; 
    DEBUG_PRINT("Looking for src %s index %lu\n", 
      std::get<source>(tuple).c_str(), index);

    //DEBUG_PRINT("num partialTriangles %lu\n", countPartials(alr, tableSize));

    if (numProcessed % 10000 == 0) {
      DEBUG_PRINT("Processed %lu out of %lu\n", numProcessed, l.size());
    }
    numProcessed++;

    for (auto partial = alr[index].begin(); partial != alr[index].end(); )
    {
      // Determine if the partial is expired 
      DEBUG_PRINT("considering partial %s\n", partial->toString().c_str());
      if (!partial->isExpired(currentTime, queryTime))
      {
        if (partial->numEdges == 1) {
          processSingleEdgePartial(*partial, newPartials, tuple,
                                   queryTime);
        }
        else if (partial->numEdges == 2) 
        {
          processTwoEdgePartial(*partial, numTriangles, tuple, queryTime);
        } 
        ++partial;
      } else 
      {
        DEBUG_PRINT("deleting partial triangle %s\n", 
          partial->toString().c_str());
        partial = alr[index].erase(partial);
      }
    }

    DEBUG_PRINT("num new partials %lu after processing %s\n", 
      newPartials.size(), toString(tuple).c_str());
      

    for (size_t t = 0; t < numThreads; t++)  
    {
      threads[t] = std::thread([&alr, &mutexes, &newPartials, t,
       numThreads, tuple, queryTime, &lock, &numTriangles, tableSize  ]() 
      {
        TupleStringHashFunction<TupleType, target> hashTarget;

        size_t beg = get_begin_index(newPartials.size(), t, numThreads); 
        size_t end = get_end_index(newPartials.size(), t, numThreads);
        for (size_t i = beg; i < end; i++) {
          PartialTriangleType partial = newPartials[i];
          size_t index;
          if (partial.numEdges == 1) {
            index = hashTarget(partial.netflow1) % tableSize;
          } else {
            index = hashTarget(partial.netflow2) % tableSize;
          }
          DEBUG_PRINT("Hashing partial %s based on target %s index %lu\n",
            partial.toString().c_str(), std::get<target>(tuple).c_str(), index);
          mutexes[index].lock();
          alr[index].push_back(partial);
          mutexes[index].unlock();
        }
      }
      );
    }

    for(size_t i = 0; i < numThreads; i++) {
      threads[i].join();
    }
  }

  delete[] mutexes;
  delete[] alr;

  return numTriangles;
}
  


}

#endif
