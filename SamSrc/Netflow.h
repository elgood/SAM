#ifndef NETFLOW_H
#define NETFLOW_H

#define TIME_SECONDS_FIELD            0
#define PARSE_DATE_FIELD              1
#define DATE_TIME_STR_FIELD           2
#define IP_LAYER_PROTOCOL_FIELD       3
#define IP_LAYER_PROTOCOL_CODE_FIELD  4
#define SOURCE_IP_FIELD               5
#define DEST_IP_FIELD                 6
#define SOURCE_PORT_FIELD             7
#define DEST_PORT_FIELD               8


#include <string>

using std::string;

namespace sam {

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

class Netflow {
 
private: 
  string timeSeconds;
  string parsedDate; 
  string dateTimeStr;
  string ipLayerProtocol;
  string ipLayerProtocolCode;
  string sourceIP; 
  string destIP;
  int    sourcePort;
  int    destPort;
  string moreFragments;
  string contFragments;
  string durationSeconds;
  string firstSeenSrcPayloadBytes;
  string firstSeenDestPayloadBytes;
  string firstSeenSrcTotalBytes;
  string firstSeenDestTotalBytes;
  string firstSeenSrcPacketCount;
  string firstSeenDestPacketCount;
  string recordForceOut; 
 
public:
  /**
   * Constructor that expects a comma delimitted string with all the fields.
   * \param s The string with all the fields.
   */
  Netflow(string s); 

  string getSourceIP() const;
  string getDestIP() const; 

  string getField(size_t field) const;

};

}

#endif
