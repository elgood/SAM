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
#define MORE_FRAGMENTS                9
#define COUNT_FRAGMENTS               10
#define DURATION_SECONDS              11
#define SRC_PAYLOAD_BYTES             12
#define DEST_PAYLOAD_BYTES            13
#define SRC_TOTAL_BYTES               14
#define DEST_TOTAL_BYTES              15


#include <string>
#include "Tuple.hpp"

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

  /**
   * Constructor that expects a comma delimitted string with all the fields.
   * \param s The string with all the fields.
   */
  Netflow(std::string s); 

  std::string getField(size_t field) const;

  std::string toString() const {
    return originalString;
  }

};

}

#endif
