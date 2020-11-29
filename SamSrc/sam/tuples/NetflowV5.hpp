/**
 * This file provides the definition of a netflow v5.
 */

#ifndef SAM_NETFLOWV5_HPP
#define SAM_NETFLOWV5_HPP


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

namespace sam {

namespace netflowv5 {

const unsigned int UnixSecs       = 0;
const unsigned int UnixNsecs     = 1;
const unsigned int SysUptime     = 2;
const unsigned int Exaddr        = 3;
const unsigned int Dpkts         = 4;
const unsigned int Doctets        = 5;
const unsigned int First1         = 6;
const unsigned int Last1          = 7;
const unsigned int EngineType     = 8;
const unsigned int EngineId       = 9;
const unsigned int SourceIp       = 10;
const unsigned int DestIp         = 11;
const unsigned int NextHop        = 12;
const unsigned int SnmpInput      = 13;
const unsigned int SnmpOutput     = 14;
const unsigned int SourcePort     = 15;
const unsigned int DestPort       = 16;
const unsigned int Protocol       = 17;
const unsigned int Tos            = 18;
const unsigned int TcpFlags       = 19;
const unsigned int SourceMask     = 20;
const unsigned int DestMask       = 21;
const unsigned int SourrcAS       = 22;
const unsigned int DestAS         = 23;


/**
 * We catch exceptions generated from this file and wrap them with a 
 * NetflowV5Exception to make them easier to track.
 */
class NetflowV5Exception : public std::runtime_error {
public:
  NetflowV5Exception(char const * message) : std::runtime_error(message) { } 
  NetflowV5Exception(std::string  message) : std::runtime_error(message) { } 
};

 
typedef std::tuple<long,        //UnixSecs                      
                    long,        //UnixNsecs                   
                    long,        //SysUptime                  
                    std::string, //Exaddr                    
                    std::size_t, //Dpkts                    
                    std::size_t, //Doctets                 
                    long,        //First                  
                    long,        //Last               
                    std::size_t, //EngineType        
                    std::size_t, //EngineId         
                    std::string, //SourceIP        
                    std::string, //DestIp         
                    std::string, //NextHop       
                    std::size_t, //SnmpInput    
                    std::size_t, //SnmpOutput         
                    std::size_t, //SourcePort        
                    std::size_t, //DestPort         
                    std::size_t, //Protocol        
                    std::size_t, //Tos            
                    std::size_t, //TcpFlags      
                    std::size_t, //SourceMask  
                    std::size_t, //DestMask   
                    std::size_t, //SourrcAS  
                    std::size_t //DestAS   
                  > NetflowV5;


/**
 * Converts a string that is in csv format into a tuple. 
 */
inline
NetflowV5 makeNetflowV5(std::string s) 
{

  // All these fields will be populated.
  long        unixSecs;
  long        unixNsecs;                   
  long        sysUptime;                  
  std::string exaddr;                    
  std::size_t dpkts;                    
  std::size_t doctets;                 
  long        first;                  
  long        last;               
  std::size_t engineType;     
  std::size_t engineId;         
  std::string sourceIP;        
  std::string destIp;         
  std::string nextHop;       
  std::size_t snmpInput;    
  std::size_t snmpOutput;        
  std::size_t sourcePort;        
  std::size_t destPort;         
  std::size_t protocol;        
  std::size_t tos;            
  std::size_t tcpFlags;      
  std::size_t sourceMask;  
  std::size_t destMask;   
  std::size_t sourrcAS;  
  std::size_t destAS;   

  std::stringstream ss(s);
  std::string item;
  std::getline(ss, item, ',');
  unixSecs = boost::lexical_cast<long>(item);
  std::getline(ss, item, ',');                     
  unixNsecs = boost::lexical_cast<long>(item);
  std::getline(ss, item, ',');                     
  sysUptime = boost::lexical_cast<long>(item);
  std::getline(ss, item, ',');                     
  exaddr = boost::lexical_cast<long>(item);
  std::getline(ss, item, ',');                     
  dpkts = boost::lexical_cast<size_t>(item);
  std::getline(ss, item, ',');                     
  doctets = boost::lexical_cast<size_t>(item);
  std::getline(ss, item, ',');                     
  first = boost::lexical_cast<long>(item);                 
  std::getline(ss, item, ',');                     
  last = boost::lexical_cast<long>(item);              
  std::getline(ss, item, ',');                     
  engineType = boost::lexical_cast<size_t>(item);       
  std::getline(ss, item, ',');                     
  engineId = boost::lexical_cast<size_t>(item);        
  std::getline(ss, item, ',');                     
  sourceIP = item;        
  std::getline(ss, item, ',');                     
  destIp = item;         
  std::getline(ss, item, ',');                     
  nextHop = item;       
  std::getline(ss, item, ',');                     
  snmpInput = boost::lexical_cast<size_t>(item);   
  std::getline(ss, item, ',');                     
  snmpOutput = boost::lexical_cast<size_t>(item);       
  std::getline(ss, item, ',');                     
  sourcePort = boost::lexical_cast<size_t>(item);     
  std::getline(ss, item, ',');                     
  destPort = boost::lexical_cast<size_t>(item);        
  std::getline(ss, item, ',');                     
  protocol = boost::lexical_cast<size_t>(item);       
  std::getline(ss, item, ',');                     
  tos = boost::lexical_cast<size_t>(item);           
  std::getline(ss, item, ',');                     
  tcpFlags = boost::lexical_cast<size_t>(item);     
  std::getline(ss, item, ',');                     
  sourceMask = boost::lexical_cast<size_t>(item); 
  std::getline(ss, item, ',');                     
  destMask = boost::lexical_cast<size_t>(item);  
  std::getline(ss, item, ',');                     
  sourrcAS = boost::lexical_cast<size_t>(item); 
  std::getline(ss, item, ',');                     
  destAS = boost::lexical_cast<size_t>(item);  

  return std::make_tuple( unixSecs,                       
                          unixNsecs,                   
                          sysUptime,                  
                          exaddr,                    
                          dpkts,                    
                          doctets,                 
                          first,                  
                          last,               
                          engineType,        
                          engineId,         
                          sourceIP,        
                          destIp,         
                          nextHop,       
                          snmpInput,    
                          snmpOutput,         
                          sourcePort,        
                          destPort,         
                          protocol,        
                          tos,            
                          tcpFlags,      
                          sourceMask,  
                          destMask,   
                          sourrcAS,  
                          destAS   
                          );

}

class MakeNetflowV5
{
public:
  NetflowV5 operator()(std::string const& s)
  {
    return makeNetflowV5(s); 
  }
};

} // end namespace netflowv5

} // end namespace sam
#endif
