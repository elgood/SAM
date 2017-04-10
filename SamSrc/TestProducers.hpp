#ifndef TEST_PRODUCERS_HPP
#define TEST_PRODUCERS_HPP

#include <vector>
#include <boost/lexical_cast.hpp>
#include "BaseProducer.h"
#include "NetflowGenerators.hpp"
#include "Netflow.h"

/******************************************************************
 * Producers used to create repeatable and understandable scenarios
 * to test various consumers in SAM.
 ******************************************************************/

namespace sam {
 
/**
 */
class TopKProducer : public BaseProducer
{
private:
  int numExamples;
  std::vector<std::shared_ptr<UniformDestPort>> servers;
  std::vector<std::shared_ptr<UniformDestPort>> nonservers;
  std::list<std::string> serverIps;
  std::list<std::string> nonserverIps;

  // This is for keeping metrics on the netflows generated.  It is a mapping
  // from ip/port to counts.
  std::map<std::pair<std::string, int>, int> ipPortMap;

public:
  TopKProducer(int queueLength, 
               int numExamples, 
               int numServers, 
               int numNonservers); 


  virtual ~TopKProducer() {} 
  void run();
  std::list<std::string> const& getServerIps() const;
  std::list<std::string> const& getNonserverIps() const;

  std::map<std::pair<std::string, int>, int> const& getIpPortMap() const
  { return ipPortMap; }


};

TopKProducer::TopKProducer(int queueLength, 
             int numExamples, 
             int numServers, 
             int numNonservers) : BaseProducer(queueLength) 
{
  this->numExamples = numExamples;
  int last = 1; 
  // Servers will have two ports, thus the top two dest ports > 0.9 will
  // evaluate to true.
  for (int i = 0; i < numServers; i++) {
     std::string ip = "192.168.0." + boost::lexical_cast<std::string>(last);
     serverIps.push_back(ip);
     last++;
     servers.push_back(std::shared_ptr<UniformDestPort>(
                       new UniformDestPort(ip, 2)));
     
     // Initializing the counts for the ip/port combos.  This is for metrics
     // and testing. 
     ipPortMap[std::pair<std::string, int>(ip, 1)] = 0;
     ipPortMap[std::pair<std::string, int>(ip, 2)] = 0;

  }
  // Servers will have three ports, thus the top two dest ports > 0.9 will
  // evaluate to false and will be classified as a non server.
  for (int i = 0; i < numNonservers; i++) {
     std::string ip = "192.168.0." + boost::lexical_cast<std::string>(last);
     nonserverIps.push_back(ip);
     last++;
     nonservers.push_back(std::shared_ptr<UniformDestPort>(
                       new UniformDestPort(ip, 3))); 

     // Initializing the counts for the ip/port combos.  This is for metrics
     // and testing. 
     ipPortMap[std::pair<std::string, int>(ip, 1)] =0;
     ipPortMap[std::pair<std::string, int>(ip, 2)] =0;
     ipPortMap[std::pair<std::string, int>(ip, 3)] =0;
  }
}

void TopKProducer::run() {

  for (int i = 0; i < numExamples; i++) {
    for (auto g : servers) {
      std::string s = g->generate();

      // Obtaining metrics on the netflows generated
      Netflow netflow(s);
      auto ipPort = std::pair<std::string, int>(netflow.getField(DEST_IP_FIELD),
        boost::lexical_cast<int>(netflow.getField(DEST_PORT_FIELD)));
      ipPortMap[ipPort] += 1;
      
      // Doing the parallel feed
      parallelFeed(s);
    }
    for (auto g : nonservers) {
      std::string s = g->generate();

      // Obtaining metrics on the netflows generated
      Netflow netflow(s);
      ipPortMap[std::pair<std::string, int>(netflow.getField(DEST_IP_FIELD),
        boost::lexical_cast<int>(netflow.getField(DEST_PORT_FIELD)))] += 1;
 
      parallelFeed(s);
    }
  }
}

std::list<std::string> const& TopKProducer::getServerIps() const
{
  return serverIps; 
}

std::list<std::string> const& TopKProducer::getNonserverIps() const
{
  return nonserverIps;
}

}

#endif
