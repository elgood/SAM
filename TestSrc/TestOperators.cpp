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
  TopKProducer producer(queueLength, numExamples, numServers, numNonservers);
  FeatureMap featureMap;
  vector<size_t> keyFields;
  keyFields.push_back(6);
  int valueField = 8;
  string identifier = "top2";
  int N = 10000; 
  int b = 1000;
  int k = 3;
  TopK<size_t, Netflow, 8, 6> top2(N, b, k, 0, featureMap, identifier);
                              

  producer.registerConsumer(&top2);

  // Five tokens for the 
  // First function token
  std::string function1 = "value";
  std::vector<double> parameters1;
  parameters1.push_back(0);
  auto funcToken1 = std::make_shared<FuncToken<Netflow>>(featureMap, identifier,
                                                        function1, parameters1);

  // Addition token
  auto addOper = std::make_shared<AddOperator<Netflow>>(featureMap);

  // Second function token
  std::string function2 = "value";
  std::vector<double> parameters2;
  parameters2.push_back(1);
  auto funcToken2 = std::make_shared<FuncToken<Netflow>>(featureMap, identifier,
                                                        function2, parameters2);

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



  Expression<Netflow> filterExpression(infixList);
  Filter<Netflow, DEST_IP_FIELD> filter(filterExpression, 0, featureMap, 
                                        "servers", queueLength);
                

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
