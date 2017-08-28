#ifndef NETFLOW_HPP
#define NETFLOW_HPP

#define SamGeneratedId                0 
#define Label                         1 
#define TimeSeconds                   2
#define ParseDate                     3
#define DateTime                      4
#define IpLayerProtocol               5
#define IpLayerProtocolCode           6
#define SourceIp                      7
#define DestIp                        8
#define SourcePort                    9
#define DestPort                      10
#define MoreFragments                 11
#define CountFragments                12
#define DurationSeconds               13
#define SrcPayloadBytes               14
#define DestPayloadBytes              15
#define SrcTotalBytes                 16
#define DestTotalBytes                17
#define FirstSeenSrcPacketCount       18
#define FirstSeenDestPacketCount      19
#define RecordForceOut                20

#define DEFAULT_LABEL -1

#include <string>
#include <tuple>
#include <iostream>
#include <boost/tokenizer.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/fusion/algorithm/iteration/for_each.hpp>

#include "Util.hpp"

namespace sam {

/**
 * Removes the first element of a csv string. 
 */
inline std::string removeFirstElement(std::string s) 
{
  int pos = s.find(",") + 1;
  std::string withoutLabel = s.substr(pos, s.size() - pos);
  return withoutLabel; 
}

/**
 * Gets the first element in a csv string.
 */
inline std::string getFirstElement(std::string s)
{
  int pos = s.find(",") + 1;
  std::string firstElement = s.substr(0, pos -1);
  return firstElement;
}


typedef std::tuple<std::size_t,  //SamGeneratedId
                   int,          //Label
                   double,       //TimeSeconds
                   std::string,  //PARSE_DATE_FIELD
                   std::string,  //DATE_TIME_STR_FIELD
                   std::string,  //IP_LAYER_PROTOCOL_FIELD
                   std::string,  //IP_LAYER_PROTOCOL_CODE_FIELD
                   std::string,  //SourceIp
                   std::string,  //DestIp
                   int,          //SourcePort
                   int,          //DestPort
                   std::string,  //MORE_FRAGMENTS
                   int,          //COUNT_FRAGMENTS
                   double,          //DURATION_SECONDS
                   int,          //SRC_PAYLOAD_BYTES
                   int,          //DEST_PAYLOAD_BYTES
                   int,          //SOURCE_TOTAL_BYTES
                   int,          //DEST_TOTAL_BYTES
                   int,          //FIRST_SEEN_SRC_PACKET_COUNT
                   int,          //FIRST_SEEN_DEST_PACKET_COUNT
                   int          //RECORD_FORCE_OUT
                   >
                   Netflow;

/**
 * This version is the original VAST format.  The generatedId and the label
 * have to be provided.
 */
inline
Netflow makeNetflowWithoutLabel(int samGeneratedId, int label, std::string s) 
{
  //std::cout << "MakeNetflowWithoutLabel " << s << std::endl;

  boost::trim(s);
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
  int countFragments;
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

  // The first two items, samGeneratedId and label, are not provided in the
  // string, so we start at 2.
  int i = 2; 
  BOOST_FOREACH(std::string const &t, tok) {
    //std::cout << "i " << i << " t " << t << std::endl;
    switch (i) {
    case TimeSeconds: timeSeconds = boost::lexical_cast<double>(t);       break;
    case ParseDate: parsedDate = t;                                       break;
    case DateTime: dateTimeStr = t;                                       break;
    case IpLayerProtocol: ipLayerProtocol = t;                            break;
    case IpLayerProtocolCode: ipLayerProtocolCode = t;                    break;
    case SourceIp: sourceIP = t;                                          break;
    case DestIp: destIP = t;                                              break;
    case SourcePort: sourcePort = boost::lexical_cast<int>(t);            break;
    case DestPort: destPort = boost::lexical_cast<int>(t);                break;
    case MoreFragments: moreFragments = t;                                break;
    case CountFragments: countFragments = boost::lexical_cast<int>(t);    break;
    case DurationSeconds: durationSeconds = boost::lexical_cast<int>(t);  break;
    case SrcPayloadBytes: 
      firstSeenSrcPayloadBytes = boost::lexical_cast<int>(t);             break;
    case DestPayloadBytes: 
      firstSeenDestPayloadBytes = boost::lexical_cast<int>(t);            break;
    case SrcTotalBytes: 
      firstSeenSrcTotalBytes = boost::lexical_cast<int>(t);               break;
    case DestTotalBytes: 
      firstSeenDestTotalBytes = boost::lexical_cast<int>(t);              break;
    case FirstSeenSrcPacketCount: 
      firstSeenSrcPacketCount = boost::lexical_cast<int>(t);              break;
    case FirstSeenDestPacketCount: 
      firstSeenDestPacketCount = boost::lexical_cast<int>(t);             break;
    case RecordForceOut: recordForceOut = boost::lexical_cast<int>(t);    break;
    }
    i++;
  }
  //std::cout << " blah " << std::endl;
  return std::make_tuple(  samGeneratedId,
                           label,
                           timeSeconds,
                           parsedDate, 
                           dateTimeStr,
                           ipLayerProtocol,
                           ipLayerProtocolCode,
                           sourceIP, 
                           destIP,
                           sourcePort,
                           destPort,
                           moreFragments,
                           countFragments,
                           durationSeconds,
                           firstSeenSrcPayloadBytes,
                           firstSeenDestPayloadBytes,
                           firstSeenSrcTotalBytes,
                           firstSeenDestTotalBytes,
                           firstSeenSrcPacketCount,
                           firstSeenDestPacketCount,
                           recordForceOut
                           );


}


// This version assumes the string s has the label at the beginning.
inline 
Netflow makeNetflowWithLabel(int samGeneratedId, std::string s)
{
  //std::cout << "MakeNetflowWithLabel " << s << std::endl;
  // Grab the label
  int label = boost::lexical_cast<int>(getFirstElement(s));
  std::string withoutLabel = removeFirstElement(s);
  return makeNetflowWithoutLabel(samGeneratedId, label, withoutLabel);
}

/**
 * Expects a netflow string without the generated id but may have a label.
 */
inline
Netflow makeNetflow(int samGeneratedId, std::string s)
{
  //std::cout << "samGeneratedId " << samGeneratedId << " s " << s << std::endl;
  // Determine the number of elements in the csv string to know which method
  // to call.
  boost::char_separator<char> sep(",");
  boost::tokenizer<boost::char_separator<char>> tok(s, sep);
  int numTokens = std::distance(tok.begin(), tok.end());

  if (numTokens == RecordForceOut) { // Has a label
    return makeNetflowWithLabel(samGeneratedId, s);
  } else if (numTokens == RecordForceOut - 1) { // No label
    return makeNetflowWithoutLabel(samGeneratedId, DEFAULT_LABEL, s);
  } else {
    throw std::invalid_argument("String provided to makeNetflow(id,s) did not "
                               " have the proper number of elements:" +s);
                               
  }

}

/**
 * Method to use when the netflow string has all fields, including 
 * SamGeneratedId and Label.
 */
inline
Netflow makeNetflow(std::string s)
{

  boost::char_separator<char> sep(",");
  boost::tokenizer<boost::char_separator<char>> tok(s, sep);
  int numTokens = std::distance(tok.begin(), tok.end());

  // Check that we have the proper number of tokens.  RecordForceOut is the last
  // field index, so the total number of expected tokens is RecordForceOut +1.
  if (numTokens != RecordForceOut + 1) {
    throw std::invalid_argument("String provided to makeNetflow(s) did not "
                               " have the proper number of elements:" + s);
  }

  // Utilizing other methods
  // Get the id
  int id = boost::lexical_cast<int>(getFirstElement(s));
  std::string withoutId = removeFirstElement(s);
  return makeNetflow(id, withoutId);
}

}
#endif
