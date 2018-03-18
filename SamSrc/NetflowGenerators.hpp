#ifndef NETFLOW_GENERATORS_HPP
#define NETFLOW_GENERATORS_HPP

#include <boost/lexical_cast.hpp>
#include <cstdlib>
#include <chrono>
#include <random>

/*************************************************************************/
// Utilities for creating artificial netflow data for testing purposes.
/*************************************************************************/

namespace sam {

/**
 * Generates strings that look like ip4 addresses.
 */
std::string generateRandomIp()
{
  std::string result = "";
  result = result + boost::lexical_cast<std::string>(rand() % 255) + ".";
  result = result + boost::lexical_cast<std::string>(rand() % 255) + ".";
  result = result + boost::lexical_cast<std::string>(rand() % 255) + ".";
  result = result + boost::lexical_cast<std::string>(rand() % 255);
  return result;
}

/**
 * Generates a random port in the range 0-65355
 */
int generateRandomPort()
{
  return rand() % 65355;
}

/** 
 * Provides a string that has the current seconds since epoch.  It 
 * is a float in a string form.
 */
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
 * Evenly spreads out the traffic to one IP along n destination ports.
 * The strings generated are in VAST csv form.  There is no SamGenerateId
 * and no label.  The source ports are from randomly generated Ips.
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
    
    // Get the port number for this iteration
    result = result + boost::lexical_cast<std::string>(ports[iter]) + ",";
    result = result + "1,1,1,";
    result = result + "1,1,";
    result = result + "1,1,";
    result = result + "1,1,";
    result = result + "1";

    // We cycle through ports each time a netflow is generated.
    iter = (iter + 1) % numPorts;

    return result;
  }

private:
};

/**
 * This generates traffic between one pair of one client and one server.
 * You can specify mean and deviation for a normal distribution for the
 * payload size for both the client and the server.
 */
class OnePairSizeDist : public AbstractNetflowGenerator
{
private:
  std::random_device rd;

  /// This is a particular type of random generator.  Mersenne Twister with
  /// a state size of 19937 bits.
  std::mt19937 gen;

  /// The destination IP for all generated netflows.
  std::string destIp;

  /// The source IP for all generate netflows.
  std::string sourceIp;  
  
  double meanDestFlow;
  double meanSourceFlow;
  double devDestFlow;
  double devSourceFlow;
  std::normal_distribution<double> destDist;
  std::normal_distribution<double> sourceDist;
public:

  /**
   * Constructor.
   * \param destIp The destination IP as a string.
   * \param sourceIp The source IP as a string.
   * \param meanDestFlow The mean flow size of payload from destination/server.
   * \param meanSourceFlow The mean flow size of payload from source/client.
   * \param devDestFlow The standard deviation of flow size from 
   *                    destination/server.
   * \param devSource The standard deviation of flow size from 
   *                  source/client.
   */
  OnePairSizeDist(std::string destIp, std::string sourceIp,
                      double meanDestFlow, double meanSourceFlow,
                      double devDestFlow, double devSourceFlow)
  {
    gen = std::mt19937(rd());
    this->destIp = destIp;
    this->sourceIp = sourceIp;
    this->meanDestFlow = meanDestFlow;
    this->meanSourceFlow = meanSourceFlow;
    destDist = std::normal_distribution<double>(meanDestFlow, devDestFlow);
    sourceDist = std::normal_distribution<double>(meanSourceFlow, 
                                                  devSourceFlow);
  }

  ~OnePairSizeDist() {
  }

  std::string generate()
  {
    int destPayloadBytes = static_cast<int>(destDist(gen));
    int sourcePayloadBytes = static_cast<int>(sourceDist(gen));
    int destTotalBytes = destPayloadBytes + 10;
    int sourceTotalBytes = sourcePayloadBytes + 10;
    std::string result;
    result = secondsSinceEpoch() + ",";
    result = result + "parseDate,dateTimeStr,ipLayerProtocol,";
    result = result + "ipLayerProtocolCode," + generateRandomIp() + ",";
    result = result + destIp + ",";
    result = result + boost::lexical_cast<std::string>(generateRandomPort());
    result = result + ",";
    result = result + "1000,"; // port 
    result = result + "1,1,1,"; // MoreFragments, CountFragments,DurationSeconds
    result = result + boost::lexical_cast<std::string>(sourcePayloadBytes)+","; 
    result = result + boost::lexical_cast<std::string>(destPayloadBytes)+","; 
    result = result + boost::lexical_cast<std::string>(sourceTotalBytes)+",";
    result = result + boost::lexical_cast<std::string>(destTotalBytes) +",";
    result = result + "1,1,";
    result = result + "1";
    return result;
  }
};


}

#endif
