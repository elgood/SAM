#define BOOST_TEST_MAIN TestTopK
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <iostream>
#include "TestProducers.hpp"
#include "TopK.hpp"
#include "Filter.hpp"
#include "FilterExpression.hpp"
#include "Netflow.h"

using namespace sam;

BOOST_AUTO_TEST_CASE( test_topk )
{
  int queueLength = 1000;
  int numExamples = 100000;
  int numServers = 2;
  int numNonservers = 2;
  TopKProducer producer(queueLength, numExamples, numServers, numNonservers);
  FeatureMap featureMap;
  vector<size_t> keyFields;
  keyFields.push_back(6);
  int valueField = 8;
  string identifier = "top2";
  int N = 10000; 
  int b = 1000;
  int k = 3;
  TopK<size_t, Netflow> top2(N, b, k, keyFields, valueField, 0, featureMap, 
                              identifier);

  producer.registerConsumer(&top2);

  FilterExpression filterExpression("top2.value(0) + top2.value(1) < 0.9");
  Filter filter(filterExpression, keyFields, 0, featureMap, "servers", 
                queueLength);

  producer.registerConsumer(&filter);

  producer.run();
  for (std::string ip : producer.getServerIps()) {
    std::cout << "server ip " << ip << std::endl;
    std::shared_ptr<Feature const> feature = featureMap.at(ip, identifier);
    std::vector<double> parameters;
    parameters.push_back(0);
    double value = feature->evaluate(VALUE_FUNCTION, parameters);
    BOOST_CHECK_CLOSE(value, 0.5, 0.01);
    parameters[0] = 1;
    value = feature->evaluate(VALUE_FUNCTION, parameters);
    BOOST_CHECK_CLOSE(value, 0.5, 0.01);
  }
  for (std::string ip : producer.getNonserverIps()) {
    std::cout << "nonserver ip " << ip << std::endl;
    std::shared_ptr<Feature const> feature = featureMap.at(ip, identifier);
    std::vector<double> parameters;
    parameters.push_back(0);
    double value = feature->evaluate(VALUE_FUNCTION, parameters);
    BOOST_CHECK_CLOSE(value, 0.333333, 0.01);
    
    parameters[0] = 1;
    value = feature->evaluate(VALUE_FUNCTION, parameters);
    BOOST_CHECK_CLOSE(value, 0.333333, 0.01);

    parameters[0] = 2;
    value = feature->evaluate(VALUE_FUNCTION, parameters);
    BOOST_CHECK_CLOSE(value, 0.333333, 0.01);
  }
}
