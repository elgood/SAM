#ifndef TEST_PRODUCERS_HPP
#define TEST_PRODUCERS_HPP

#include <vector>
#include <boost/lexical_cast.hpp>
#include <map>
#include <sam/BaseProducer.hpp>
#include <sam/tuples/VastNetflowGenerators.hpp>
#include <sam/tuples/VastNetflow.hpp>
#include <sam/tuples/Tuplizer.hpp>
#include <sam/tuples/Edge.hpp>

/******************************************************************
 * Producers used to create repeatable and understandable scenarios
 * to test various consumers in SAM.
 ******************************************************************/

namespace sam {

using namespace sam::vast_netflow;

/**
 * Creates a situation where there are n popular sites that recieve
 * p fraction of the traffic.  
 */
class PopularSites : 
  public BaseProducer<Edge<size_t, EmptyLabel, VastNetflow>>
{
public:
  typedef Edge<size_t, EmptyLabel, VastNetflow> EdgeType;
  typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer;
private:
  size_t numExamples; ///> Number of netflows to produce
  size_t numPopular; ///> How many IPs are popular.
  double p; ///> Probability of netflow being to popular site

public:
  PopularSites(size_t nodeId,
              size_t queueLength,
              size_t numExamples,
              size_t numPopular,
              double p);

  virtual ~PopularSites() {} 
  
  void run();
};

PopularSites::PopularSites(size_t nodeId,
                           size_t queueLength,
                           size_t numExamples,
                           size_t numPopular,
                           double p) :
                           BaseProducer(nodeId, queueLength)
{
  this->numExamples = numExamples;
  this->numPopular = numPopular;
  this->p = p;
}

void PopularSites::run()
{
  Tuplizer tuplizer;
  RandomGenerator generator;
  std::random_device rd;
  std::mt19937 myRand;
  std::uniform_real_distribution<> dis(0, 1.0);
  std::uniform_int_distribution<> whichPop(1, numPopular);

  for ( size_t i = 0; i < numExamples; i++) 
  {
    std::string s = generator.generate();
    EdgeType edge = tuplizer(i, s);

    if (dis(myRand) < p) {
      size_t popId = whichPop(myRand);
      std::get<DestIp>(edge.tuple) =
        boost::lexical_cast<std::string>(popId);
    }

    parallelFeed(edge);
  }
}


 
/**
 * Class that allows you to produce a scenario where some IPs have
 * two ports that accept traffic, and other IPs have three ports that accept
 * traffic.  Using the definition from Disclosure where top2 dest ports >
 * 0.9 are classified as servers, this will create a distinction between
 * server IPs and nonserver IPs when using a filter that uses the topk 
 * feature.
 */
class TopKProducer : public BaseProducer<Edge<size_t, EmptyLabel, VastNetflow>>
{
public: 
  typedef Edge<size_t, EmptyLabel, VastNetflow> EdgeType;
  typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer;
private:
  int numExamples;
  std::vector<std::shared_ptr<UniformDestPort>> servers;
  std::vector<std::shared_ptr<UniformDestPort>> nonservers;
  std::list<std::string> serverIps;
  std::list<std::string> nonserverIps;

  // This is for keeping metrics on the netflows generated.  It is a mapping
  // from ip/port to counts.  It is used for testing purposes.
  std::map<std::pair<std::string, int>, int> ipPortMap;

public:
  TopKProducer(size_t nodeId,
               int queueLength, 
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

TopKProducer::TopKProducer(size_t nodeId,
                           int queueLength, 
                           int numExamples, 
                           int numServers, 
                           int numNonservers) : 
                           BaseProducer(nodeId, queueLength)
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
  // NonServers will have three ports, thus the top two dest ports > 0.9 will
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

  Tuplizer tuplizer;
  for (int i = 0; i < numExamples; i++) 
  {
    int serverId = 0;
    for (auto g : servers) {
      std::string s = g->generate();

      // Obtaining metrics on the netflows generated
      // Using i and serverId as the SamGeneratedId
      EdgeType edge = tuplizer(i, s);
      auto ipPort = std::pair<std::string, int>(
        std::get<DestIp>(edge.tuple),
        std::get<DestPort>(edge.tuple));
      ipPortMap[ipPort] += 1;
      
      // Doing the parallel feed
      //parallelFeed(serverId * numExamples + i, netflow);
      parallelFeed(edge);

      serverId++;
    }
    for (auto g : nonservers) {
      std::string s = g->generate();

      // Obtaining metrics on the netflows generated
      //VastNetflow netflow = makeNetflow(serverId* numExamples + i, s);
      EdgeType edge = tuplizer(i, s);
      ipPortMap[std::pair<std::string, int>(std::get<DestIp>(
        edge.tuple),
        std::get<DestPort>(edge.tuple))] += 1;

      parallelFeed(edge);
      serverId++;
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

/**
 * Generic netflow producer that uses a vector of netflow generators
 * to produce netflows.  Generates a netflow numExamples times for
 * each generator.
 */
class GeneralNetflowProducer : 
  public BaseProducer<Edge<size_t, EmptyLabel, VastNetflow>>
{
public:
  typedef Edge<size_t, EmptyLabel, VastNetflow> EdgeType;
  typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer;
private:
  std::vector<std::shared_ptr<AbstractVastNetflowGenerator>> generators;
  int numExamples;
public:
  GeneralNetflowProducer(
    size_t nodeId, 
    int queueLength,
    int numExamples,
    std::vector<std::shared_ptr<AbstractVastNetflowGenerator>>  
      const& _generators) :
    BaseProducer(nodeId, queueLength),
    generators(_generators)
  {
    this->numExamples = numExamples;
  }

  void run();
  

};

void GeneralNetflowProducer::run()
{
  int count = 0; //Remove
  Tuplizer tuplizer;
  for (int i = 0; i < numExamples; i++) 
  {
    for (int j = 0; j < generators.size(); j++) 
    {
      count++; //Remove
      std::string s = generators[j]->generate();
      EdgeType edge = tuplizer(i, s);

      parallelFeed(edge);
    }  
  }
}

}

#endif
