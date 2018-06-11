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

  /**
   * Generates a netflow formatted as a string.
   * Unless this function is overridden The time is taken from the system clock.
   * \return Returns a netflow as a string.
   */
  virtual std::string generate();

  /**
   * This function allows you to specify the time of the generated netflow.
   * \param epochTime The time in seconds since the epoch. 
   */
  virtual std::string generate(double epochTime) = 0;
};

inline  
std::string AbstractNetflowGenerator::generate()
{
  double epochTime = boost::lexical_cast<double>(secondsSinceEpoch());
  return generate(epochTime);
}


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

  std::string generate() {
    return AbstractNetflowGenerator::generate();
  }

  std::string generate(double epochTime) 
  {
    std::string result;
    result = boost::lexical_cast<std::string>(epochTime) + ",";
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
 * Creates completely random source and destination ip addresses
 */
class RandomGenerator : public AbstractNetflowGenerator
{
private:

public:
  /**
   * Constructor.
   */
  RandomGenerator() {}

  /**
   * Destructor.
   */
  ~RandomGenerator() {}

  std::string generate() {
    return AbstractNetflowGenerator::generate();
  }

  std::string generate(double epochTime) {
    std::string result;
    result = boost::lexical_cast<std::string>(epochTime) + ",";
    result = result + "parseDate,dateTimeStr,ipLayerProtocol,";
    result = result + "ipLayerProtocolCode," + generateRandomIp() + ",";
    result = result + generateRandomIp() + ",";
    result = result + boost::lexical_cast<std::string>(generateRandomPort());
    result = result + ",";
    
    // Get the port number for this iteration
    result = result + boost::lexical_cast<std::string>(generateRandomPort());
    result = result + ",";
    result = result + "1,1,1,";
    result = result + "1,1,";
    result = result + "1,1,";
    result = result + "1,1,";
    result = result + "1";
    return result;
  }
};

/**
 * Chooses at random source and destination from a small set of n vertices.
 * The source and target Ips have the form node<x>, where <x> is from 0 to
 * n-1.  Right now the class allows edges that have target and destination
 * as the same.
 */
class RandomPoolGenerator : public AbstractNetflowGenerator
{
private:
  /// The number of vertices.
  size_t numVertices;

public:
  /**
   * Constructor.
   */
  RandomPoolGenerator(size_t n, size_t randomSeed = 0) {
    srand(randomSeed);
    this->numVertices = n;
  }

  /**
   * Destructor.
   */
  ~RandomPoolGenerator() {}

  /**
   * Uses AbstractNetflowGenerator::generate, which calls generate(epochTime)
   * with the current clock time.
   */
  std::string generate() {
    return AbstractNetflowGenerator::generate();
  }

  std::string generate(double epochTime) {

    size_t sourceInt = rand() % numVertices;
    size_t targetInt = rand() % numVertices;

    while (targetInt == sourceInt) {
      targetInt = rand() % numVertices;
    }
  
    std::string sourceStr = "node" + 
      boost::lexical_cast<std::string>(sourceInt);
    std::string targetStr = "node" + 
      boost::lexical_cast<std::string>(targetInt);

    std::string result;
    result = boost::lexical_cast<std::string>(epochTime) + ",";
    result = result + "parseDate,dateTimeStr,ipLayerProtocol,";
    result = result + "ipLayerProtocolCode," + sourceStr + ",";
    result = result + targetStr + ",";
    result = result + boost::lexical_cast<std::string>(generateRandomPort());
    result = result + ",";
    
    // Get the port number for this iteration
    result = result + boost::lexical_cast<std::string>(generateRandomPort());
    result = result + ",";
    result = result + "1,1,1,";
    result = result + "1,1,";
    result = result + "1,1,";
    result = result + "1,1,";
    result = result + "1";
    return result;
  }
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
  OnePairSizeDist(std::string sourceIp, std::string destIp,
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

  std::string generate() {
    return AbstractNetflowGenerator::generate();
  }


  std::string generate(double epochTime)
  {
    int destPayloadBytes = static_cast<int>(destDist(gen));
    int sourcePayloadBytes = static_cast<int>(sourceDist(gen));
    int destTotalBytes = destPayloadBytes + 10;
    int sourceTotalBytes = sourcePayloadBytes + 10;
    std::string result;
    result = boost::lexical_cast<std::string>(epochTime) + ",";
    result = result + "parseDate,dateTimeStr,ipLayerProtocol,";
    result = result + "ipLayerProtocolCode," + sourceIp + ",";
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
