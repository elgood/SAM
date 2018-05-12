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


namespace sam {


#ifdef DETAIL_TIMING
  #define DETAIL_TIMING_BEG1 \
  auto detailTimingBegin = std::chrono::high_resolution_clock::now();
  
  #define DETAIL_TIMING_BEG2 \
  detailTimingBegin = std::chrono::high_resolution_clock::now();

  #define DETAIL_TIMING_END1(var) \
  auto detailTimingEnd = std::chrono::high_resolution_clock::now();\
  auto detailTimingDiff = std::chrono::duration_cast<std::chrono::duration<double>>(\
    detailTimingEnd - detailTimingBegin);\
  var += detailTimingDiff.count();

  #define DETAIL_TIMING_END2(var) \
  detailTimingEnd = std::chrono::high_resolution_clock::now();\
  detailTimingDiff = std::chrono::duration_cast<std::chrono::duration<double>>(\
    detailTimingEnd - detailTimingBegin);\
  var += detailTimingDiff.count();
#else
  #define DETAIL_TIMING_BEG1
  #define DETAIL_TIMING_BEG2
  #define DETAIL_TIMING_END1(var)
  #define DETAIL_TIMING_END2(var)
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

class StringHashFunction
{
public:
  inline
  uint64_t operator()(std::string const& s) const {
    return std::hash<std::string>{}(s);
    //return hashFunction(s);
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

/**
 * Given the zmq message, extract the data as a string.
 * \param message The zmq to get the data from.
 * \return Returns the zmq data as a string.
 */
std::string getStringFromZmqMessage( zmq::message_t const& message )
{
  // Reinterpret the void pointer to be an unsigned char pointer.
  unsigned char const * dataptr = 
    reinterpret_cast<unsigned char const*>(message.data());

  // Creates a string with proper size so we can iterate over it and 
  // copy in the values from the message.
  std::string rString(message.size(), 'x');

  std::copy(dataptr, &dataptr[message.size()], rString.begin());
  return rString;
}

template<typename... Tp>
inline zmq::message_t tupleToZmq(std::tuple<Tp...>const& t)
{
  std::string str = toString(t);
  zmq::message_t message = fillZmqMessage(str);

  return message;
}

/**
 * Creates an empty zmq message.  We use this to indicate a terminate
 * message.
 */
zmq::message_t emptyZmqMessage() {
  std::string str = "";
  zmq::message_t message = fillZmqMessage(str);
  return message;
}

/**
 * Creates a zmq message that means terimate
 */
zmq::message_t terminateZmqMessage() {
  return emptyZmqMessage();
  //std::string str = "terminate";
  //zmq::message_t message = fillZmqMessage(str);
  //return message;
}



/**
 * Checks if the zmq message is a terminate message. A terminate message
 * is one with an empty string.
 */
bool isTerminateMessage(zmq::message_t& message)
{
  //std::string str = getStringFromZmqMessage(message);
  //if (str.compare("terminate") == 0) {
  //  return true;
  //}
  //return false;
  if (message.size() == 0) return true;
  return false;
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
      //printf("createpushsockets nodeId %lu %d\n", nodeId, i);
      /////////// adding push sockets //////////////
      auto pusher = std::shared_ptr<zmq::socket_t>(
                      new zmq::socket_t(*context, ZMQ_PUSH));
      //printf("createpushsockets nodeId %lu %d created socket\n", 
      //        nodeId, i);

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
      //printf("createpushsockets url %s nodeId %lu %d set socket option\n", 
      //    url.c_str(), nodeId, i);
      //pusher->connect(url);
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

template<typename TupleType, size_t source, size_t target, size_t time,
         size_t duration>
size_t numTriangles(std::vector<TupleType> l, double queryTime)
{
  typedef typename std::tuple_element<source, TupleType>::type SourceType;
  typedef typename std::tuple_element<target, TupleType>::type TargetType;

  struct PartialTriangle {
    size_t numEdges = 0;
    TupleType netflow1;
    TupleType netflow2;
  };

  std::sort(l.begin(), l.end(), [](TupleType const& t1, 
                                   TupleType const& t2) ->bool
  {
    return std::get<time>(t1) < std::get<time>(t2);  
  });

  for (size_t i = 0; i < l.size(); i++) {
    std::get<0>(l[i]) = i;
  }

  std::list<PartialTriangle> partialTriangles;

  size_t numTriangles = 0;

  for (auto tuple : l) 
  {
    PartialTriangle p;
    p.numEdges = 1;
    p.netflow1 = tuple;
    
    partialTriangles.push_back(p);
    
    for (auto partial : partialTriangles) {
      if (partial.numEdges == 1) {
        auto id1 = std::get<0>(partial.netflow1);
        auto id2 = std::get<0>(tuple);
        if (id1 != id2) {

          auto trg1 = std::get<target>(partial.netflow1);
          auto src2 = std::get<source>(tuple);
          //printf("trg1 %s src2 %s\n", trg1.c_str(), src2.c_str());
          if (trg1 == src2) {
            double t1 = std::get<time>(partial.netflow1);
            double t2 = std::get<time>(tuple);
            if (t1 <= t2) {
              double dur = std::get<duration>(tuple);
              if (t2 + dur - t1 < queryTime) {
                PartialTriangle newPartial;
                newPartial.numEdges = 2;
                newPartial.netflow1 = partial.netflow1;
                newPartial.netflow2 = tuple;  
                //printf("newpartial %f %s %s, "
                //       " %f %s %s\n",
                //       std::get<time>(newPartial.netflow1),
                //       std::get<source>(newPartial.netflow1).c_str(),
                //       std::get<target>(newPartial.netflow1).c_str(),
                //       std::get<time>(newPartial.netflow2),
                //       std::get<source>(newPartial.netflow2).c_str(),
                //       std::get<target>(newPartial.netflow2).c_str());
                partialTriangles.push_back(newPartial);
              }
            }
          }
        }
      }
      else if (partial.numEdges == 2) {
        auto id1 = std::get<0>(partial.netflow1);
        auto id2 = std::get<0>(partial.netflow2);
        auto id3 = std::get<0>(tuple);
        if (id1 != id3 && id2 != id3) {
          auto trg2 = std::get<target>(partial.netflow2);
          auto src3 = std::get<source>(tuple);
          if (trg2 == src3) {
            auto trg3 = std::get<target>(tuple) ;
            auto src1 = std::get<source>(partial.netflow1);
            if (trg3 == src1) {
              double t1 = std::get<time>(partial.netflow1);
              double t2 = std::get<time>(partial.netflow2);
              double t3 = std::get<time>(tuple);
              double dur = std::get<duration>(tuple);
              if (t3 >= t2 && t3 + dur -t1 <= queryTime) {
                #ifdef DEBUG
                printf("edge1 %f %s %s, "
                       "edge2 %f %s %s, "
                       "edge3 %f %s %s\n",
                       std::get<time>(partial.netflow1),
                       std::get<source>(partial.netflow1).c_str(),
                       std::get<target>(partial.netflow1).c_str(),
                       std::get<time>(partial.netflow2),
                       std::get<source>(partial.netflow2).c_str(),
                       std::get<target>(partial.netflow2).c_str(),
                       std::get<time>(tuple),
                       std::get<source>(tuple).c_str(),
                       std::get<target>(tuple).c_str());
                #endif
                numTriangles++;
              }
            }
          }
        }
      }
    }
  }
  return numTriangles;
}
  


}

#endif
