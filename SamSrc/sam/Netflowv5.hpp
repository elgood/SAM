/**
 * This file provides the definition of a netflow v5.
 */

#ifndef SAM_NETFLOWV5_HPP
#define SAM_NETFLOWV5_HPP

#define SamGeneratedId                0 
#define SamLabel                      1 
#define UnixSecs                      2
#define UnixNsecs                     3
#define SysUptime                     4
#define Exaddr                        5
#define Dpkts                         6
#define Doctets                       7
#define First                         8
#define Last                          9
#define EngineType                    10
#define EngineId                      11
#define SourceIP                      12
#define DestIp                        13
#define NextHop                       14
#define SnmpInput                     15
#define SnmpOutput                    16
#define SourcePort                    17
#define DestPort                      18
#define Protocol                      19
#define Tos                           20
#define TcpFlags                      21
#define SourceMask                    22
#define DestMask                      23
#define SourrcAS                      24
#define DestAS                        25


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
 * Netflowv5Exception to make them easier to track.
 */
class Netflowv5Exception : public std::runtime_error {
public:
  Netflowv5Exception(char const * message) : std::runtime_error(message) { } 
  Netflowv5Exception(std::string  message) : std::runtime_error(message) { } 
};

 

typedef std::tuple<std::size_t //SamgeGenerateID
                    int,        //Label
                    long        //UnixSecs                      
                    long        //UnixNsecs                   
                    long        //SysUptime                  
                    std::string //Exaddr                    
                    std::size_t //Dpkts                    
                    std::size_t //Doctets                 
                    long        //First                  
                    long        //Last               
                    std::size_t //EngineType        
                    std::size_t //EngineId         
                    std::string //SourceIP        
                    std::string //DestIp         
                    std::string //NextHop       
                    std::size_t //SnmpInput    
                    std::size_t //SnmpOutput         
                    std::size_t //SourcePort        
                    std::size_t //DestPort         
                    std::size_t //Protocol        
                    std::size_t //Tos            
                    std::size_t //TcpFlags      
                    std::size_t //SourceMask  
                    std::size_t //DestMask   
                    std::size_t //SourrcAS  
                    std::size_t //DestAS   
                  > Netflowv5;


/**
 * This version is the original VAST format.  The generatedId and the label
 * have to be provided.
 */
inline
Netflowv5 makeNetflowWithoutLabel(int samGeneratedId, 
                                    int label, std::string s) 
{
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
Netflowv5 makeNetflowWithLabel(int samGeneratedId, std::string s)
{
  //std::cout << "MakeNetflowWithLabel " << s << std::endl;
  // Grab the label
  int label;
  try {
    label = boost::lexical_cast<int>(getFirstElement(s));
  } catch (std::exception e) {
    std::cout << "Error in makeNetflowWithLabel " << e.what() << std::endl;
    throw Netflowv5Exception(e.what());
  }
  std::string withoutLabel = removeFirstElement(s);
  return makeNetflowWithoutLabel(samGeneratedId, label, withoutLabel);
}

/**
 * Expects a netflow string without the generated id but may have a label.
 */
inline
Netflowv5 makeNetflow(int samGeneratedId, std::string s)
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
Netflowv5 makeNetflow(std::string s)
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
    throw Netflowv5Exception(e.what());
  }
  std::string withoutId = removeFirstElement(s);
  return makeNetflow(id, withoutId);
}

class Netflowv5Tuplizer
{
public:
  Netflowv5 operator()(size_t id, std::string s) {
    return makeNetflow(id, s);
  }

};


}
#endif
