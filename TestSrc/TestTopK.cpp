#define BOOST_TEST_MAIN TestTopK
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <iostream>
#include "TestProducers.hpp"
#include "TopK.hpp"
#include "Filter.hpp"
#include "Expression.hpp"
#include "Netflow.hpp"

using namespace sam;

BOOST_AUTO_TEST_CASE( test_topk )
{
  int queueLength = 1000;
  int numExamples = 100000;
  int numServers = 2;
  int numNonservers = 2;

  // The TopKProducer creates a situation where there are numServers servers
  // and numNonservers non-servers.  A server is defined as > 90% of traffic
  // to the top two ports.
  TopKProducer producer(queueLength, numExamples, numServers, numNonservers);
  auto featureMap = std::make_shared<FeatureMap>();
  std::vector<size_t> keyFields;
  keyFields.push_back(6);
  int valueField = 8;
  string identifier = "top2";
  int N = 10000; 
  int b = 1000;
  int k = 3;
  auto top2 = std::make_shared<TopK<size_t, Netflow, DestPort, DestIp>>
    (N, b, k, 0, featureMap, identifier);
                              

  producer.registerConsumer(top2);

  // Five tokens for the 
  // First function token
  auto function1 = [](Feature const * feature)->double {
    auto topKFeature = static_cast<TopKFeature const *>(feature);
    return topKFeature->getFrequencies()[0];  
  };
  auto funcToken1 = std::make_shared<FuncToken<Netflow>>(featureMap, function1,
                                                         identifier);

  // Addition token
  auto addOper = std::make_shared<AddOperator<Netflow>>(featureMap);

  // Second function token
  auto function2 = [](Feature const * feature)->double {
    auto topKFeature = static_cast<TopKFeature const *>(feature);
    return topKFeature->getFrequencies()[1];
  };
  auto funcToken2 = std::make_shared<FuncToken<Netflow>>(featureMap, function2,
                                                         identifier);

  // Lessthan token
  auto lessThanToken = std::make_shared<LessThanOperator<Netflow>>(featureMap);
  
  // Number token
  auto numberToken = std::make_shared<NumberToken<Netflow>>(featureMap, 0.9);

  std::list<std::shared_ptr<ExpressionToken<Netflow>>> infixList;
  infixList.push_back(funcToken1);
  infixList.push_back(addOper);
  infixList.push_back(funcToken2);
  infixList.push_back(lessThanToken);
  infixList.push_back(numberToken);

  auto filterExpression = std::make_shared<Expression<Netflow>>(infixList);
  auto filter = std::make_shared<Filter<Netflow, DestIp>>(
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

    double value = feature->evaluate(function1);
    BOOST_CHECK_CLOSE(value, 0.5, 0.01);
    
    index1 = 1; 
    value = feature->evaluate(function1);
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



    double value = feature->evaluate(function1);
    BOOST_CHECK_CLOSE(value, 0.333333, 0.01);
    
    index1 = 1;
    value = feature->evaluate(function1);
    BOOST_CHECK_CLOSE(value, 0.333333, 0.01);

    index1 = 2;
    value = feature->evaluate(function1);
    BOOST_CHECK_CLOSE(value, 0.333333, 0.01);
  }
}
