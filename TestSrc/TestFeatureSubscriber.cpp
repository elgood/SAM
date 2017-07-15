#define BOOST_TEST_MAIN TestExponentialHistogram
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <iostream>
#include <thread>
#include <chrono>
#include <boost/lexical_cast.hpp>
#include <boost/tokenizer.hpp>
#include <boost/foreach.hpp>
#include "FeatureProducer.hpp"
#include "FeatureSubscriber.hpp"
#include "NetflowGenerators.hpp"
#include "TestProducers.hpp"
#include "FeatureMap.hpp"
#include "ExponentialHistogramSum.hpp"
#include "ExponentialHistogramVariance.hpp"

using namespace sam;

class DummyFeatureProducer : public FeatureProducer
{
private:
  double value;
  int count = 0;
  int milli = 0;
public:
  DummyFeatureProducer(double _value, int _milli) : 
    value(_value), milli(_milli) 
  {}

  void consume() {
    for (int i = 0; i < subscribers.size(); i++) {
      subscribers[i]->update(boost::lexical_cast<std::string>(count), 
                             names[i], value);
      std::this_thread::sleep_for(std::chrono::milliseconds(milli));
    }
    count++;
  }

};

/**
 * There is one feature subscriber and multiple feature producers, but
 * everything happens in the same thread.
 */
BOOST_AUTO_TEST_CASE( test_feature_subscriber_single_thread )
{

  int numFeatures = 5;
  std::shared_ptr<DummyFeatureProducer>* producers =
    new std::shared_ptr<DummyFeatureProducer>[numFeatures]();
  for(int i = 0; i < numFeatures; i++) {
    producers[i] = std::make_shared<DummyFeatureProducer>(i, 0); 
  }

  std::vector<std::string> featureNames;
  for (int i = 0; i < numFeatures; i++) {
    featureNames.push_back(boost::lexical_cast<std::string>(i));
  }

  int capacity = 10000;
  auto subscriber = std::make_shared<FeatureSubscriber>(featureNames,
                                                        capacity);
  int i = 0;
  for (int i = 0; i < numFeatures; i++) {
    producers[i]->registerSubscriber( subscriber, 
      boost::lexical_cast<std::string>(i) );
  }

  int numTimes = 100;
  for (int i = 0; i < numTimes; i++) {
    for (int j = 0; j < numFeatures; j++) {
      producers[j]->consume();
    }
  }

  std::string result = subscriber->getOutput();

  // Each line should look like 0,1,2,3,4
  
  boost::char_separator<char> newline("\n");
  boost::char_separator<char> comma(",");
  boost::tokenizer<boost::char_separator<char>> newlineTok(result, newline);
  BOOST_FOREACH(std::string const &line, newlineTok)
  {
    boost::tokenizer<boost::char_separator<char>> csvTok(line, comma);
    int i = 0;
    BOOST_FOREACH(std::string const &t, csvTok)
    {
      BOOST_CHECK_EQUAL(boost::lexical_cast<std::string>(i), t); 
      i++;
    }
  }

  BOOST_CHECK_EQUAL(0, subscriber->size());
  producers[0]->consume();
  BOOST_CHECK_EQUAL(1, subscriber->size());
}

/**
 * Each feature producer is in a separate thread, but all contributing to 
 * the same FeatureSubscriber.
 */
BOOST_AUTO_TEST_CASE( test_feature_subscriber_multi_thread )
{
  int numThreads = 5;
  int capacity = 10000;


  std::vector<std::string> featureNames;
  for (int i = 0; i < numThreads; i++) {
    featureNames.push_back(boost::lexical_cast<std::string>(i));
  }

  // Each of the threads will add to subscriber concurrently 
  auto subscriber = std::make_shared<FeatureSubscriber>(featureNames, capacity);

  // The number of concurrent threads
  std::vector<std::thread> threads;

  std::shared_ptr<DummyFeatureProducer>* producers =
    new std::shared_ptr<DummyFeatureProducer>[numThreads]();
  for(int i = 0; i < numThreads; i++) {
    producers[i] = std::make_shared<DummyFeatureProducer>(i, i * 10); 
  }

  // Each thread produces one feature with the name "i"
  for (int i = 0; i < numThreads; i++) {
    producers[i]->registerSubscriber( subscriber, 
      boost::lexical_cast<std::string>(i) );
  }

  for (int i = 0; i < numThreads; i++)
  {
    auto producer = producers[i];
    threads.push_back(std::thread([producer]() {
      int numTimes = 100;
      for (int i = 0; i < numTimes; i++) {
        producer->consume();
      }
    }));
  }

  for (int i = 0; i < numThreads; i++) {
    threads[i].join();
  }

  std::string result = subscriber->getOutput();

  boost::char_separator<char> newline("\n");
  boost::char_separator<char> comma(",");
  boost::tokenizer<boost::char_separator<char>> newlineTok(result, newline);
  BOOST_FOREACH(std::string const &line, newlineTok)
  {
    boost::tokenizer<boost::char_separator<char>> csvTok(line, comma);
    int i = 0;
    BOOST_FOREACH(std::string const &t, csvTok)
    {
      BOOST_CHECK_EQUAL(boost::lexical_cast<std::string>(i), t); 
      i++;
    }
  }
}

/**
 * We use a GeneralNetflowProducer to create a stream of netflows.
 * This feeds into different FeatureProducers.  
 */
BOOST_AUTO_TEST_CASE( test_feature_subscriber )
{
  std::string destIp   = "192.168.0.1";
  std::string sourceIp = "192.186.0.2";
  double meanDestFlowSize   = 100.0;
  double meanSourceFlowSize = 50.0;
  double devDestFlowSize    = 2.0;
  double devSourceFlowSize  = 3.0;
  auto generator = std::make_shared<OnePairSizeDist>(destIp, sourceIp,
                            meanDestFlowSize, meanSourceFlowSize,
                            devDestFlowSize, devSourceFlowSize);
  std::vector<std::shared_ptr<AbstractNetflowGenerator>> generators;
  generators.push_back(generator);

  int queueLength = 1000;
  int numExamples = 20000;
  GeneralNetflowProducer netflowProducer( queueLength,
                                          numExamples,
                                          generators);


  // Size of hash table for both featureMap and featureSubscriber
  int capacity = 10000;

  // The global feature map
  FeatureMap featureMap(capacity);

  int N = 1000;
  int k = 2;
  int nodeId = 0;
  std::string idAveSourceFlowSize = "aveSourceFlowSize";
  auto aveSourceFlowSize = std::make_shared<ExponentialHistogramAve<
                             size_t, Netflow, SrcPayloadBytes, DestIp>>(
                             N, k, nodeId, featureMap, idAveSourceFlowSize);
                            
  std::string idAveDestFlowSize = "aveDestFlowSize";
  auto aveDestFlowSize = std::make_shared<ExponentialHistogramAve<
                             size_t, Netflow, DestPayloadBytes, DestIp>>(
                             N, k, nodeId, featureMap, idAveDestFlowSize);

  std::string idVarSourceFlowSize = "varSourceFlowSize";
  auto varSourceFlowSize = std::make_shared<ExponentialHistogramVariance<
                             size_t, Netflow, SrcPayloadBytes, DestIp>>(
                             N, k, nodeId, featureMap, idVarSourceFlowSize);
  
  std::string idVarDestFlowSize = "varDestFlowSize";
  auto varDestFlowSize = std::make_shared<ExponentialHistogramVariance<
                             size_t, Netflow, DestPayloadBytes, DestIp>>(
                             N, k, nodeId, featureMap, idVarDestFlowSize);

  
  // The names of the features that we will subscribe to.  These are the 
  // the same as the operator identifiers.
  std::vector<std::string> featureNames;
  featureNames.push_back(idAveSourceFlowSize);
  featureNames.push_back(idAveDestFlowSize);
  featureNames.push_back(idVarSourceFlowSize);
  featureNames.push_back(idVarDestFlowSize);

  // Connect the feature producers to the netflow producer. 
  netflowProducer.registerConsumer(aveSourceFlowSize);
  netflowProducer.registerConsumer(aveDestFlowSize);
  netflowProducer.registerConsumer(varSourceFlowSize);
  netflowProducer.registerConsumer(varDestFlowSize);

  auto subscriber = std::make_shared<FeatureSubscriber>(featureNames, capacity);

  // Telling the feature producers about the subscriber
  aveSourceFlowSize->registerSubscriber( subscriber, idAveSourceFlowSize);
  aveDestFlowSize->registerSubscriber( subscriber, idAveDestFlowSize);
  varSourceFlowSize->registerSubscriber( subscriber, idVarSourceFlowSize);
  varDestFlowSize->registerSubscriber( subscriber, idVarDestFlowSize);

  netflowProducer.run();

  std::string result = subscriber->getOutput();

  //std::cout << result << std::endl;

  boost::char_separator<char> newline("\n");
  boost::char_separator<char> comma(",");
  boost::tokenizer<boost::char_separator<char>> newlineTok(result, newline);
  int skip = 200; //skip the first lines because they are inaccurate
  int linesSeen = 0;
  BOOST_FOREACH(std::string const &line, newlineTok)
  {
    if (linesSeen > skip) {
      boost::tokenizer<boost::char_separator<char>> csvTok(line, comma);
      int i = 0;
      BOOST_FOREACH(std::string const &t, csvTok)
      {
        switch (i) {
          /// The mean seems closer to expected value.  The variance
          /// can be off by more for some reason.
          case 0: 
            BOOST_CHECK_CLOSE(boost::lexical_cast<double>(t), 
                              meanSourceFlowSize, 15);
            break;
          case 1:
            BOOST_CHECK_CLOSE(boost::lexical_cast<double>(t), 
                              meanDestFlowSize, 15);
            break;
          case 2:
            BOOST_CHECK_CLOSE(pow(boost::lexical_cast<double>(t), 0.5), 
                              devSourceFlowSize, 30);
            break;
          case 3:
            BOOST_CHECK_CLOSE(pow(boost::lexical_cast<double>(t), 0.5), 
                              devDestFlowSize, 30);
            break;
        }
        i++;
      }
    }
    linesSeen++;
  }
}
