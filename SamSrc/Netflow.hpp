#ifndef NETFLOW_HPP
#define NETFLOW_HPP

#define TIME_SECONDS_FIELD            0
#define PARSE_DATE_FIELD              1
#define DATE_TIME_STR_FIELD           2
#define IP_LAYER_PROTOCOL_FIELD       3
#define IP_LAYER_PROTOCOL_CODE_FIELD  4
#define SOURCE_IP_FIELD               5
#define DEST_IP_FIELD                 6
#define SOURCE_PORT_FIELD             7
#define DEST_PORT_FIELD               8
#define MORE_FRAGMENTS                9
#define COUNT_FRAGMENTS               10
#define DURATION_SECONDS              11
#define SRC_PAYLOAD_BYTES             12
#define DEST_PAYLOAD_BYTES            13
#define SRC_TOTAL_BYTES               14
#define DEST_TOTAL_BYTES              15
#define FIRST_SEEN_SRC_PACKET_COUNT   16
#define FIRST_SEEN_DEST_PACKET_COUNT  17
#define RECORD_FORCE_OUT              18


#include <string>
#include <tuple>
#include <iostream>
#include <boost/tokenizer.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>

#include <boost/fusion/algorithm/iteration/for_each.hpp>

namespace sam {

template <int I = 0, typename FuncT, typename... Tp>
inline typename std::enable_if<I == sizeof...(Tp), void>::type
for_each(std::tuple<Tp...> const&, FuncT)
{}


template <int I = 0, typename FuncT, typename... Tp>
inline typename std::enable_if<I < sizeof...(Tp), void>::type
for_each(std::tuple<Tp...> const& t, FuncT f)
{
  f(std::get<I>(t));
  for_each<I + 1, FuncT, Tp...>(t, f);  
}

template <int I = 0, typename... Tp>
inline typename std::enable_if<I == sizeof...(Tp), std::string>::type
tupleToString(std::tuple<Tp...> const&)
{
  return "";
}

template<int I = 0, typename... Tp>
inline typename std::enable_if<I < sizeof...(Tp), std::string>::type
tupleToString(std::tuple<Tp...>const& t)
{
  std::string result = boost::lexical_cast<std::string>(std::get<I>(t));
  result = result + "," + tupleToString<I + 1, Tp...>(t);
  return result;
}

template<typename... Tp>
std::string toString(std::tuple<Tp...>const& t) {
  std::string result = tupleToString(t);
  return result.substr(0, result.size()-1);
}




typedef std::tuple<double,       //TIME_SECONDS_FIELD
                     std::string,  //PARSE_DATE_FIELD
                     std::string,  //DATE_TIME_STR_FIELD
                     std::string,  //IP_LAYER_PROTOCOL_FIELD
                     std::string,  //IP_LAYER_PROTOCOL_CODE_FIELD
                     std::string,  //SOURCE_IP_FIELD
                     std::string,  //DEST_IP_FIELD
                     int,          //SOURCE_PORT_FIELD
                     int,          //DEST_PORT_FIELD
                     std::string,  //MORE_FRAGMENTS
                     int,          //COUNT_FRAGMENTS
                     int,          //DURATION_SECONDS
                     int,          //SRC_PAYLOAD_BYTES
                     int,          //DEST_PAYLOAD_BYTES
                     int,          //SOURCE_TOTAL_BYTES
                     int,          //DEST_TOTAL_BYTES
                     int,          //FIRST_SEEN_SRC_PACKET_COUNT
                     int,          //FIRST_SEEN_DEST_PACKET_COUNT
                     int           //RECORD_FORCE_OUT
                     >
                     Netflow;

/*std::string toString(Netflow const& netflow)
{
  using std::string;
  using namespace boost::fusion;

  string result;
  for_each(netflow, [&result](auto &s) {
    result += boost::lexical_cast<string>(s) + ",";
  });
  return result;
}*/
/*std::string toString(Netflow const& n)
{
  std::string result = "";
  auto function = [&result] (auto a) {
    return result + boost::lexical_cast<std::string>(a);
  };

  for_blah(n, function);
}*/

/*
// There's got to be a better way to do this.
std::string toString(Netflow const& n)
{
  std::string result = boost::lexical_cast<std::string>(std::get<0>(n)) + "," +
                      boost::lexical_cast<std::string>(std::get<1>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<2>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<3>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<4>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<5>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<6>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<7>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<8>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<9>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<10>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<11>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<12>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<13>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<14>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<15>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<16>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<17>(n)) + "," + 
                      boost::lexical_cast<std::string>(std::get<18>(n)); 
  
  return result;
}
*/
inline
Netflow makeNetflow(std::string s) {

  double timeSeconds;
  std::string parsedDate; 
  std::string dateTimeStr;
  std::string ipLayerProtocol;
  std::string ipLayerProtocolCode;
  std::string sourceIP; 
  std::string destIP;
  int sourcePort;
  int destPort;
  std::string moreFragments;
  int contFragments;
  int durationSeconds;
  int firstSeenSrcPayloadBytes;
  int firstSeenDestPayloadBytes;
  int firstSeenSrcTotalBytes;
  int firstSeenDestTotalBytes;
  int firstSeenSrcPacketCount;
  int firstSeenDestPacketCount;
  int recordForceOut; 

  boost::char_separator<char> sep(",");
  boost::tokenizer<boost::char_separator<char>> tok(s, sep);

  int i = 0;
  BOOST_FOREACH(std::string const &t, tok) {
    switch (i) {
    case 0: timeSeconds = boost::lexical_cast<double>(t); break;
    case 1: parsedDate = t;                           break;
    case 2: dateTimeStr = t;                          break;
    case 3: ipLayerProtocol = t;                      break;
    case 4: ipLayerProtocolCode = t;                  break;
    case 5: sourceIP = t;                             break;
    case 6: destIP = t;                               break;
    case 7: sourcePort = boost::lexical_cast<int>(t); break;
    case 8: destPort = boost::lexical_cast<int>(t);   break;
    case 9: moreFragments = t;                        break;
    case 10: contFragments = boost::lexical_cast<int>(t);             break;
    case 11: durationSeconds = boost::lexical_cast<int>(t);           break;
    case 12: firstSeenSrcPayloadBytes = boost::lexical_cast<int>(t);  break;
    case 13: firstSeenDestPayloadBytes = boost::lexical_cast<int>(t); break;
    case 14: firstSeenSrcTotalBytes = boost::lexical_cast<int>(t);    break;
    case 15: firstSeenDestTotalBytes = boost::lexical_cast<int>(t);   break;
    case 16: firstSeenSrcPacketCount = boost::lexical_cast<int>(t);   break;
    case 17: firstSeenDestPacketCount = boost::lexical_cast<int>(t);  break;
    case 18: recordForceOut = boost::lexical_cast<int>(t);            break;  
    }
    i++;
  }

  return std::make_tuple(  timeSeconds,
                           parsedDate, 
                           dateTimeStr,
                           ipLayerProtocol,
                           ipLayerProtocolCode,
                           sourceIP, 
                           destIP,
                           sourcePort,
                           destPort,
                           moreFragments,
                           contFragments,
                           durationSeconds,
                           firstSeenSrcPayloadBytes,
                           firstSeenDestPayloadBytes,
                           firstSeenSrcTotalBytes,
                           firstSeenDestTotalBytes,
                           firstSeenSrcPacketCount,
                           firstSeenDestPacketCount,
                           recordForceOut);


}

/**
 * Right now we are assuming the VAST format of netflows, 
 * which has the following format:
 * Netflows have the following fields (from VAST dataset).  More
 *      detail can be found in "Week 1 Data Descriptions Final" of 
 *      VAST dataset.
 *      0) TimeSeconds (e.g 1365582756.3842709)
 *      1) parsedDate (2013-04-10 08:32:36) 
 *      2) dateTimeStr (20130410083236.384271)
 *      3) ipLayerProtocol (17)
 *      4) ipLayerProtocolCode (UDP)
 *      5) firstSeenSrcIp 
 *      6) firstSeenDestIp
 *      7) firstSeenSrcPort
 *      8) firstSeenDestPort
 *      9) moreFragments (non-zero means more records for this flow)
 *      10) contFragments (non-zero means no the first record in the flow)
 *      11) durationSeconds (integer)
 *      12) firstSeenSrcPayloadBytes
 *      13) firstSeenDestPayloadBytes
 *      14) firstSeenSrcTotalBytes
 *      15) firstSeenDestTotalBytes
 *      16) firstSeenSrcPacketCount
 *      17) firstSeenDestPacketCount
 *      18) recordForceOut
 * @author elgood
 *
 */
/*
class Netflow : public Tuple 
{
 
private: 
  std::string timeSeconds;
  std::string parsedDate; 
  std::string dateTimeStr;
  std::string ipLayerProtocol;
  std::string ipLayerProtocolCode;
  std::string sourceIP; 
  std::string destIP;
  int    sourcePort;
  int    destPort;
  std::string moreFragments;
  std::string contFragments;
  std::string durationSeconds;
  std::string firstSeenSrcPayloadBytes;
  std::string firstSeenDestPayloadBytes;
  std::string firstSeenSrcTotalBytes;
  std::string firstSeenDestTotalBytes;
  std::string firstSeenSrcPacketCount;
  std::string firstSeenDestPacketCount;
  std::string recordForceOut; 

  /// The original string used in construction.
  std::string originalString;
 
public:

  Netflow();
  Netflow(Netflow const& other);
*/
  /**
   * Constructor that expects a comma delimitted string with all the fields.
   * \param s The string with all the fields.
   */
/*  Netflow(std::string s); 

  std::string getField(size_t field) const;

  std::string toString() const {
    return originalString;
  }

};

*/
}
#endif
