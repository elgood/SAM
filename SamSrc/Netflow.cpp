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


}
