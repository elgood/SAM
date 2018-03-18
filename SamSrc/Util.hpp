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

namespace sam {

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

template<typename... Tp>
inline zmq::message_t tupleToZmq(std::tuple<Tp...>const& t)
{
  std::string str = toString(t);
  zmq::message_t message(str.size() + 1);
  snprintf((char*) message.data(), str.size() + 1, "%s", str.c_str());
  return message;
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
    return hashFunction(s);
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

zmq::message_t fillZmqMessage(std::string const& str)
{
  zmq::message_t message(str.length() + 1);
  snprintf((char*) message.data(), str.length() + 1, "%s", str.c_str());
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
 * Checks if the zmq message is a terminate message. A terminate message
 * is one with an empty string.
 */
bool isTerminateMessage(zmq::message_t& message)
{
  char* buff = static_cast<char*>(message.data());
  return strcmp(buff, "") == 0;
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
      //printf("createpushsockets nodeId %lu %d set socket option\n", 
      //    nodeId, i);
      pusher->bind(url);
      //printf("createpushsockets nodeId %lu %d connect\n", nodeId, i);
      pushers[i] = pusher;
    } 
  }
}


  


}

#endif
