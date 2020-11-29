/**
 * This file provides the definition of a netflow according to the definition 
 * used by the VAST 2013 Mini-challenge 3 dataset.  For more details on the 
 * format see http://vacommunity.org/VAST+Challenge+2013%3A+Mini-Challenge+3.
 */
#ifndef SAM_VASTNETFLOW_HPP
#define SAM_VASTNETFLOW_HPP

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
#include <sam/tuples/Edge.hpp>

namespace sam {

namespace vast_netflow {

const unsigned int TimeSeconds =                   0;
const unsigned int ParseDate =                     1;
const unsigned int DateTime =                      2;
const unsigned int IpLayerProtocol =               3;
const unsigned int IpLayerProtocolCode =           4;
const unsigned int SourceIp =                      5;
const unsigned int DestIp =                        6;
const unsigned int SourcePort =                    7;
const unsigned int DestPort =                      8;
const unsigned int MoreFragments =                 9;
const unsigned int CountFragments =               10;
const unsigned int DurationSeconds =              11;
const unsigned int SrcPayloadBytes =              12;
const unsigned int DestPayloadBytes =             13;
const unsigned int SrcTotalBytes =                14;
const unsigned int DestTotalBytes =               15;
const unsigned int FirstSeenSrcPacketCount =      16;
const unsigned int FirstSeenDestPacketCount =     17;
const unsigned int RecordForceOut =               18;

/**
 * We catch exceptions generated from this file and wrap them with a 
 * VastNetflowException to make them easier to track.
 */
class VastNetflowException : public std::runtime_error {
public:
  VastNetflowException(char const * message) : std::runtime_error(message) { } 
  VastNetflowException(std::string  message) : std::runtime_error(message) { } 
};

typedef std::tuple<double,       //TimeSeconds
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
 * Converts a string that is in csv vast format into a tuple.
 */
inline
VastNetflow makeVastNetflow(std::string const& s) 
{
  // All the fields that need to be populated.
  // These values aren't used.
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
  durationSeconds = boost::lexical_cast<double>(item); 
  std::getline(ss, item, ',');
  firstSeenSrcPayloadBytes = boost::lexical_cast<long>(item); 
  std::getline(ss, item, ',');
  firstSeenDestPayloadBytes = boost::lexical_cast<long>(item); 
  std::getline(ss, item, ',');
  firstSeenSrcTotalBytes = boost::lexical_cast<long>(item); 
  std::getline(ss, item, ',');
  firstSeenDestTotalBytes = boost::lexical_cast<long>(item); 
  std::getline(ss, item, ',');
  firstSeenSrcPacketCount = boost::lexical_cast<long>(item); 
  std::getline(ss, item, ',');
  firstSeenDestPacketCount = boost::lexical_cast<long>(item); 
  std::getline(ss, item, ',');
  recordForceOut = boost::lexical_cast<int>(item); std::getline(ss, item, ',');
  

  return std::make_tuple(timeSeconds,
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

class MakeVastNetflow
{
public:
  VastNetflow operator()(std::string const& s)
  {
    return makeVastNetflow(s); 
  }
};


} // end namespace vast_netflow

} // end namespace sam

#endif
