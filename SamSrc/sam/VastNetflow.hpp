/**
 * This file provides the definition of a netflow according to the definition 
 * used by the VAST 2013 Mini-challenge 3 dataset.  For more details on the 
 * format see http://vacommunity.org/VAST+Challenge+2013%3A+Mini-Challenge+3.
 */

#ifndef SAM_VASTNETFLOW_HPP
#define SAM_VASTNETFLOW_HPP

#define SamGeneratedId                0 
#define SamLabel                      1 
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


#include <string>
#include <tuple>
#include <iostream>
#include <boost/tokenizer.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/fusion/algorithm/iteration/for_each.hpp>
#include <zmq.hpp>

#include <sam/Util.hpp>

#define DEFAULT_LABEL -1

namespace sam {

/**
 * We catch exceptions generated from this file and wrap them with a 
 * VastNetflowException to make them easier to track.
 */
class VastNetflowException : public std::runtime_error {
public:
  VastNetflowException(char const * message) : std::runtime_error(message) { } 
  VastNetflowException(std::string  message) : std::runtime_error(message) { } 
};


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
                   long,          //SRC_PAYLOAD_BYTES
                   long,          //DEST_PAYLOAD_BYTES
                   long,          //SOURCE_TOTAL_BYTES
                   long,          //DEST_TOTAL_BYTES
                   long,          //FIRST_SEEN_SRC_PACKET_COUNT
                   long,          //FIRST_SEEN_DEST_PACKET_COUNT
                   int          //RECORD_FORCE_OUT
                   >
                   VastNetflow;



/**
 * This version is the original VAST format.  The generatedId and the label
 * have to be provided.
 */
inline
VastNetflow makeNetflowWithoutLabel(std::size_t samGeneratedId, 
                                    std::size_t label, std::string s) 
{

  /*
  std::size_t,  //SamGeneratedId
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
  long,          //SRC_PAYLOAD_BYTES
  long,          //DEST_PAYLOAD_BYTES
  long,          //SOURCE_TOTAL_BYTES
  long,          //DEST_TOTAL_BYTES
  long,          //FIRST_SEEN_SRC_PACKET_COUNT
  long,          //FIRST_SEEN_DEST_PACKET_COUNT
  int          //RECORD_FORCE_OUT
  */

  double timeSeconds = 1;
  std::string parsedDate = "blah"; 
  std::string dateTimeStr = "blah";
  std::string ipLayerProtocol = "blah";
  std::string ipLayerProtocolCode = "blah";
  std::string sourceIP = "192.168.0.1"; 
  std::string destIP = "192.168.0.1";
  int sourcePort = 55;
  int destPort = 66;
  std::string moreFragments = "0";
  int countFragments = 1;
  int durationSeconds = 1;
  int firstSeenSrcPayloadBytes = 1;
  int firstSeenDestPayloadBytes = 1;
  int firstSeenSrcTotalBytes = 1;
  int firstSeenDestTotalBytes = 1;
  int firstSeenSrcPacketCount = 1;
  int firstSeenDestPacketCount = 1;
  int recordForceOut = 0; 

  std::stringstream ss(s);
  std::string item;
  std::getline(ss, item, ',');
  timeSeconds = boost::lexical_cast<double>(item); std::getline(ss, item, ','); 
  parsedDate = item; std::getline(ss, item, ',');
  dateTimeStr = item; std::getline(ss, item, ',');
  ipLayerProtocol = item; std::getline(ss, item, ',');
  ipLayerProtocolCode = item; std::getline(ss, item, ',');
  sourceIP = item; std::getline(ss, item, ',');                                          
  destIP = item; std::getline(ss, item, ',');
  sourcePort = boost::lexical_cast<int>(item); std::getline(ss, item, ',');
  destPort = boost::lexical_cast<int>(item); std::getline(ss, item, ',');
  moreFragments = item; std::getline(ss, item, ',');
  countFragments = boost::lexical_cast<int>(item); std::getline(ss, item, ',');
  durationSeconds = boost::lexical_cast<double>(item); std::getline(ss, item, ',');
  firstSeenSrcPayloadBytes = boost::lexical_cast<long>(item); std::getline(ss, item, ',');
  firstSeenDestPayloadBytes = boost::lexical_cast<long>(item); std::getline(ss, item, ',');
  firstSeenSrcTotalBytes = boost::lexical_cast<long>(item); std::getline(ss, item, ',');
  firstSeenDestTotalBytes = boost::lexical_cast<long>(item); std::getline(ss, item, ',');
  firstSeenSrcPacketCount = boost::lexical_cast<long>(item); std::getline(ss, item, ',');
  firstSeenDestPacketCount = boost::lexical_cast<long>(item); std::getline(ss, item, ',');
  recordForceOut = boost::lexical_cast<int>(item); std::getline(ss, item, ',');
  

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
VastNetflow makeNetflowWithLabel(int samGeneratedId, std::string s)
{
  //std::cout << "MakeNetflowWithLabel " << s << std::endl;
  // Grab the label
  int label;
  try {
    label = boost::lexical_cast<int>(getFirstElement(s));
  } catch (std::exception e) {
    std::cout << "Error in makeNetflowWithLabel " << e.what() << std::endl;
    throw VastNetflowException(e.what());
  }
  std::string withoutLabel = removeFirstElement(s);
  return makeNetflowWithoutLabel(samGeneratedId, label, withoutLabel);
}

/**
 * Expects a netflow string without the generated id but may have a label.
 */
inline
VastNetflow makeNetflow(int samGeneratedId, std::string s)
{
  //std::cout << "samGeneratedId " << samGeneratedId << " s " << s << std::endl;
  // Determine the number of elements in the csv string to know which method
  // to call.
  boost::char_separator<char> sep(",");
  boost::tokenizer<boost::char_separator<char>> tok(s, sep);
  int numTokens = std::distance(tok.begin(), tok.end());

  if (numTokens == RecordForceOut + 1) { // Has all fields but overriding id
    std::string withoutId = removeFirstElement(s);
    return makeNetflow(samGeneratedId, withoutId);    
  } else if (numTokens == RecordForceOut) { // Has a label
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
VastNetflow makeNetflow(std::string s)
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
  int id;
  try {
    id = boost::lexical_cast<int>(getFirstElement(s));
  } catch (std::exception e) {
    std::cout << "Error in makeNetflow " << e.what() << std::endl;
    throw VastNetflowException(e.what());
  }
  std::string withoutId = removeFirstElement(s);
  return makeNetflow(id, withoutId);
}

class VastNetflowTuplizer
{
public:
  VastNetflow operator()(size_t id, std::string s) {
    return makeNetflow(id, s);
  }

};


}
#endif
