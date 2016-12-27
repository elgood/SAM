#include "Netflow.h"
#include <boost/tokenizer.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <vector>

namespace sam {

Netflow::Netflow(string s) 
{
  boost::char_separator<char> sep(",");
  boost::tokenizer<boost::char_separator<char>> tok(s, sep);


  int i = 0;
  BOOST_FOREACH(string const &t, tok) {
    switch (i) {
    case 0: timeSeconds = t;                          break;
    case 1: parsedDate = t;                           break;
    case 2: dateTimeStr = t;                          break;
    case 3: ipLayerProtocol = t;                      break;
    case 4: ipLayerProtocolCode = t;                  break;
    case 5: sourceIP = t;                             break;
    case 6: destIP = t;                               break;
    case 7: sourcePort = boost::lexical_cast<int>(t); break;
    case 8: destPort = boost::lexical_cast<int>(t);   break;
    case 9: moreFragments = t;                        break;
    case 10: contFragments =t;                        break;
    case 11: durationSeconds = t;                     break;
    case 12: firstSeenSrcPayloadBytes = t;            break;
    case 13: firstSeenDestPayloadBytes = t;           break;
    case 14: firstSeenSrcTotalBytes = t;              break;
    case 15: firstSeenDestTotalBytes = t;             break;
    case 16: firstSeenSrcPacketCount = t;             break;
    case 17: firstSeenDestPacketCount = t;            break;
    case 18: recordForceOut = t;                      break;  
    }
    i++;
  }
  
}

string Netflow::getSourceIP() const { return sourceIP; }
string Netflow::getDestIP() const { return destIP; }

string Netflow::getField(size_t field) const
{
    switch (field) {
    case 0: return timeSeconds;
    case 1: return parsedDate;
    case 2: return dateTimeStr;
    case 3: return ipLayerProtocol;
    case 4: return ipLayerProtocolCode;
    case 5: return sourceIP;
    case 6: return destIP;
    case 7: return boost::lexical_cast<string>(sourcePort);
    case 8: return boost::lexical_cast<string>(destPort);
    case 9: return moreFragments;
    case 10: return contFragments;
    case 11: return durationSeconds;
    case 12: return firstSeenSrcPayloadBytes;
    case 13: return firstSeenDestPayloadBytes;
    case 14: return firstSeenSrcTotalBytes;
    case 15: return firstSeenDestTotalBytes;
    case 16: return firstSeenSrcPacketCount;
    case 17: return firstSeenDestPacketCount;
    case 18: return recordForceOut;
    }
    throw std::out_of_range("Unknown field id" + 
                            boost::lexical_cast<string>(field));
}

}
