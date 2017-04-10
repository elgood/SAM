#ifndef NETFLOW_GENERATORS_HPP
#define NETFLOW_GENERATORS_HPP

#include <boost/lexical_cast.hpp>
#include <cstdlib>
#include <chrono>

/*************************************************************************/
// Utilities for creating artificial netflow data for testing purposes.
/*************************************************************************/

namespace sam {

std::string generateRandomIp()
{
  std::string result = "";
  result = result + boost::lexical_cast<std::string>(rand() % 255) + ".";
  result = result + boost::lexical_cast<std::string>(rand() % 255) + ".";
  result = result + boost::lexical_cast<std::string>(rand() % 255) + ".";
  result = result + boost::lexical_cast<std::string>(rand() % 255);
  return result;
}

int generateRandomPort()
{
  return rand() % 65355;
}

std::string secondsSinceEpoch() 
{
  using namespace std::chrono;
  auto timestamp = duration_cast<milliseconds>(
    system_clock::now().time_since_epoch());
  double doubleTime = static_cast<double>(timestamp.count()) / 1000;
  return boost::lexical_cast<std::string>(doubleTime);
}

/** 
 * The abstract netflow generator.
 */
class AbstractNetflowGenerator {
public:
  AbstractNetflowGenerator() {}
  virtual ~AbstractNetflowGenerator() {}
  virtual std::string generate() = 0;
};

/**
 * Evenly spreads out the traffic to one IP along n destination ports
 */
class UniformDestPort : public AbstractNetflowGenerator
{
private:
  std::string destIp; ///> The single destination 
  int numPorts; ///> The number of destination ports.
  int iter = 0;
  int* ports; ///> An array of ports

public:
  UniformDestPort(std::string destIp, int numPorts) : 
    AbstractNetflowGenerator()
  {
    this->destIp = destIp;
    this->numPorts = numPorts;  
    ports = new int[numPorts]; 
    for (int i = 0; i < numPorts; i++) ports[i] = i + 1;
  }

  ~UniformDestPort() {
    delete[] ports;
  }

  std::string generate() 
  {
    std::string result;
    result = secondsSinceEpoch() + ",";
    result = result + "parseDate,dateTimeStr,ipLayerProtocol,";
    result = result + "ipLayerProtocolCode," + generateRandomIp() + ",";
    result = result + destIp + ",";
    result = result + boost::lexical_cast<std::string>(generateRandomPort());
    result = result + ",";
    result = result + boost::lexical_cast<std::string>(ports[iter]) + ",";
    result = result + "moreFragments,contFragments,durationSeconds,";
    result = result + "firstSeenSrcPayloadBytes,firstSeenDestPayloadBytes,";
    result = result + "firstSeenSrcTotalBytes,firstSeenDestTotalBytes,";
    result = result + "firstSeenSrcPacketCoutn,firstSeenDestPacketCount,";
    result = result + "recordForceOut";
    iter = (iter + 1) % numPorts;
    return result;
  }

private:
};


}

#endif
