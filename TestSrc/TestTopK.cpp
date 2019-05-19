#define BOOST_TEST_MAIN TestTopK
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <iostream>
#include <sam/TestProducers.hpp>
#include <sam/TopK.hpp>
#include <sam/Filter.hpp>
#include <sam/Expression.hpp>
#include <sam/VastNetflow.hpp>

using namespace sam;

struct F
{
  std::shared_ptr<FeatureMap> featureMap;
  string identifier = "topk";
  int N = 10000; 
  int b = 1000;
  int k = 3;

  F()
  {
    featureMap = std::make_shared<FeatureMap>();
  }
};

BOOST_FIXTURE_TEST_CASE( test_topk_no_key, F )
{
  size_t queueLength = 1000;
  size_t numPopular = 2;
  size_t numExamples = 100000;
  double probabilityPop = 0.5;

  PopularSites producer(queueLength, numExamples, numPopular,
                        probabilityPop);

  auto topk = std::make_shared<TopK<VastNetflow, DestIp>>
    (N, b, k, 0, featureMap, identifier);

  producer.registerConsumer(topk);

  producer.run();

  std::shared_ptr<Feature const> feature = featureMap->at("", identifier);

  auto func0 = [](Feature const * feature)->double {
    auto topKFeature = static_cast<TopKFeature const *>(feature);
    return topKFeature->getFrequencies()[0];    
  };
  auto func1 = [](Feature const * feature)->double {
    auto topKFeature = static_cast<TopKFeature const *>(feature);
    return topKFeature->getFrequencies()[1];    
  };

  double frequency0 = feature->evaluate<double>(func0);
  double frequency1 = feature->evaluate<double>(func0);
  BOOST_CHECK_CLOSE(frequency0, 0.25, 5); 
  BOOST_CHECK_CLOSE(frequency1, 0.25, 5);
}

BOOST_FIXTURE_TEST_CASE( test_topk_server, F )
{
  int queueLength = 1000;
  int numExamples = 100000;
  int numServers = 2;
  int numNonservers = 2;

  // The TopKProducer creates a situation where there are numServers servers
  // and numNonservers non-servers.  A server is defined as > 90% of traffic
  // to the top two ports.
  TopKProducer producer(queueLength, numExamples, numServers, numNonservers);
 
  // Creating the topk computation and registering it as a consumer
  // of the data source: producer.
  auto topk = std::make_shared<TopK<VastNetflow, DestPort, DestIp>>
    (N, b, k, 0, featureMap, identifier);
  producer.registerConsumer(topk);

  // TODO: The below section creating the filter isn't actually tested.
  // Add more tests to see that the filter works.

  /////// Creating the filter expression /////////////////////////
  // We create an infix expression to filter for servers, i.e.
  //  Servers = FILTER VertsByDest BY top2.value(0) + top2.value(1) < 0.9;
  // The infix is the part after the BY.

  // First function token
  auto function1 = [](Feature const * feature)->double {
    auto topKFeature = static_cast<TopKFeature const *>(feature);
    return topKFeature->getFrequencies()[0];  
  };
  auto funcToken1 = std::make_shared<FuncToken<VastNetflow>>(featureMap, function1,
                                                         identifier);

  // Addition token
  auto addOper = std::make_shared<AddOperator<VastNetflow>>(featureMap);

  // Second function token
  auto function2 = [](Feature const * feature)->double {
    auto topKFeature = static_cast<TopKFeature const *>(feature);
    return topKFeature->getFrequencies()[1];
  };
  auto funcToken2 = std::make_shared<FuncToken<VastNetflow>>(featureMap, function2,
                                                         identifier);

  // Lessthan token
  auto lessThanToken = std::make_shared<LessThanOperator<VastNetflow>>(featureMap);
  
  // Number token
  auto numberToken = std::make_shared<NumberToken<VastNetflow>>(featureMap, 0.9);

  std::list<std::shared_ptr<ExpressionToken<VastNetflow>>> infixList;
  infixList.push_back(funcToken1);
  infixList.push_back(addOper);
  infixList.push_back(funcToken2);
  infixList.push_back(lessThanToken);
  infixList.push_back(numberToken);

  auto filterExpression = std::make_shared<Expression<VastNetflow>>(infixList);
  auto filter = std::make_shared<Filter<VastNetflow, DestIp>>(
    filterExpression, 0, featureMap, "servers", queueLength);

  producer.registerConsumer(filter);

  producer.run();
  for (std::string ip : producer.getServerIps()) {
    std::shared_ptr<Feature const> feature = featureMap->at(ip, identifier);
    int index1 = 0;
    auto function1 = [&index1](Feature const * feature)->double {
      auto topKFeature = static_cast<TopKFeature const *>(feature);
      return topKFeature->getFrequencies()[index1];    
    };

    // TopKProducer sends data in a uniform random way to two ports to the 
    // server ips, so the top two values should both be about 0.5 frequency.
    double value = feature->evaluate<double>(function1);
    BOOST_CHECK_CLOSE(value, 0.5, 0.01);
    
    index1 = 1; 
    value = feature->evaluate<double>(function1);
    BOOST_CHECK_CLOSE(value, 0.5, 0.01);
  }
  for (std::string ip : producer.getNonserverIps()) {
    std::shared_ptr<Feature const> feature = featureMap->at(ip, identifier);
    std::vector<double> parameters;
    parameters.push_back(0);

    int index1 = 0;
    auto function1 = [&index1](Feature const * feature)->double {
      auto topKFeature = static_cast<TopKFeature const *>(feature);
      return topKFeature->getFrequencies()[index1];    
    };

    // TopKProducer sends data in a uniform random way to three ports
    // to the non-server ips, so the top three values should all be about 0.33
    double value = feature->evaluate<double>(function1);
    BOOST_CHECK_CLOSE(value, 0.333333, 0.01);
    
    index1 = 1;
    value = feature->evaluate<double>(function1);
    BOOST_CHECK_CLOSE(value, 0.333333, 0.01);

    index1 = 2;
    value = feature->evaluate<double>(function1);
    BOOST_CHECK_CLOSE(value, 0.333333, 0.01);
  }
}
