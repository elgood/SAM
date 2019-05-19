#define BOOST_TEST_MAIN TestFeatureSubscriber
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <iostream>
#include <thread>
#include <chrono>
#include <boost/lexical_cast.hpp>
#include <boost/tokenizer.hpp>
#include <boost/foreach.hpp>
#include <sam/FeatureProducer.hpp>
#include <sam/FeatureSubscriber.hpp>
#include <sam/VastNetflowGenerators.hpp>
#include <sam/TestProducers.hpp>
#include <sam/FeatureMap.hpp>
#include <sam/ExponentialHistogramSum.hpp>
#include <sam/ExponentialHistogramVariance.hpp>
#include <stdio.h>

using namespace sam;

class DummyFeatureProducer : public FeatureProducer
{
private:
  double value;
  std::size_t count = 0;
  int milli = 0;
public:
  DummyFeatureProducer(double _value, int _milli) : 
    value(_value), milli(_milli) 
  {}

  void consume() {
    for (int i = 0; i < subscribers.size(); i++) {
      subscribers[i]->update(count, names[i], value);
      std::this_thread::sleep_for(std::chrono::milliseconds(milli));
    }
    count++;
  }

};

/**
 * Tests the logic around init.  Init must be called before update is called.
 * addFeature must be called before init.
 */
BOOST_AUTO_TEST_CASE( test_init )
{
  int numFeatures = 5;
  std::string outputfile = "TestTransformProducerOutput.txt";
  FeatureSubscriber subscriber(outputfile);
  BOOST_CHECK_THROW(subscriber.update(1, "featureName", 5.0), 
                    std::logic_error);
  
  // Should fail because no features have been added.
  BOOST_CHECK_THROW(subscriber.init(), std::logic_error);

  subscriber.addFeature("featureName1");
  subscriber.addFeature("featureName2");

  subscriber.init();
  
  BOOST_CHECK_THROW(subscriber.addFeature("blah"), std::logic_error);
  remove(outputfile.c_str());
}

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

  int capacity = 10000;
  std::string outputfile = "TestTransformProducerOutput.txt";
  auto subscriber = std::make_shared<FeatureSubscriber>(outputfile, capacity);
  int i = 0;
  for (int i = 0; i < numFeatures; i++) {
    std::string featureName = boost::lexical_cast<std::string>(i);

    // calls FeatureSubscriber::addFeature(featureName)
    producers[i]->registerSubscriber( subscriber, featureName);  
  }

  subscriber->init();

  BOOST_CHECK_EQUAL(subscriber->getNumFeatures(), numFeatures);

  int numTimes = 100;
  for (int i = 0; i < numTimes; i++) {
    for (int j = 0; j < numFeatures; j++) {
      producers[j]->consume();
    }
  }

  // Each line should look like 0,1,2,3,4
  auto infile = std::ifstream(outputfile);
  std::string line;
  int numLines = 0;
  while(std::getline(infile, line)) {
    boost::char_separator<char> comma(",");
    boost::tokenizer<boost::char_separator<char>> csvTok(line, comma);
    int i = 0;
    BOOST_FOREACH(std::string const &t, csvTok)
    {
      BOOST_CHECK_EQUAL(boost::lexical_cast<std::string>(i), t); 
      i++;
    }
    numLines++;
  }
  BOOST_CHECK_EQUAL(numLines, numTimes);
 
  remove(outputfile.c_str());
}

/**
 * Each feature producer is in a separate thread, but all contributing to 
 * the same FeatureSubscriber.
 */
BOOST_AUTO_TEST_CASE( test_feature_subscriber_multi_thread )
{
  int numThreads = 5;
  int capacity = 10000;

  // Each of the threads will add to subscriber concurrently 
  std::string outputfile = "TestTransformProducerOutput.txt";
  auto subscriber = std::make_shared<FeatureSubscriber>(outputfile, capacity);

  // The number of concurrent threads
  std::vector<std::thread> threads;

  std::shared_ptr<DummyFeatureProducer>* producers =
    new std::shared_ptr<DummyFeatureProducer>[numThreads]();
  for(int i = 0; i < numThreads; i++) {
    producers[i] = std::make_shared<DummyFeatureProducer>(i, i * 10); 
  }

  // Each thread produces one feature with the name "i"
  for (int i = 0; i < numThreads; i++) {
    std::string featureName = boost::lexical_cast<std::string>(i);
    
    // calls FeatureSubscriber::addFeature(featureName)
    producers[i]->registerSubscriber( subscriber, featureName);
  }

  subscriber->init();
  int numTimes = 100;
  for (int i = 0; i < numThreads; i++)
  {
    auto producer = producers[i];
    threads.push_back(std::thread([producer, numTimes]() {
      for (int i = 0; i < numTimes; i++) {
        producer->consume();
      }
    }));
  }

  for (int i = 0; i < numThreads; i++) {
    threads[i].join();
  }

  // Each line should look like 0,1,2,3,4
  auto infile = std::ifstream(outputfile);
  std::string line;
  int numLines = 0;
  while(std::getline(infile, line)) {
    boost::char_separator<char> comma(",");
    boost::tokenizer<boost::char_separator<char>> csvTok(line, comma);
    int i = 0;
    BOOST_FOREACH(std::string const &t, csvTok)
    {
      BOOST_CHECK_EQUAL(boost::lexical_cast<std::string>(i), t); 
      i++;
    }
    numLines++;
  }
  BOOST_CHECK_EQUAL(numLines, numTimes);
 
  remove(outputfile.c_str());
}

/**
 * We use a GeneralNetflowProducer to create a stream of netflows.
 * This feeds into different FeatureProducers.  
 */
BOOST_AUTO_TEST_CASE( test_feature_subscriber )
{
  std::string destIp   = "192.168.0.1";
  std::string sourceIp = "192.168.0.2";
  double meanDestFlowSize   = 100.0;
  double meanSourceFlowSize = 50.0;
  double devDestFlowSize    = 2.0;
  double devSourceFlowSize  = 3.0;
  auto generator = std::make_shared<OnePairSizeDist>(destIp, sourceIp,
                            meanDestFlowSize, meanSourceFlowSize,
                            devDestFlowSize, devSourceFlowSize);
  std::vector<std::shared_ptr<AbstractVastNetflowGenerator>> generators;
  generators.push_back(generator);

  int queueLength = 1000;
  int numExamples = 200000;
  GeneralNetflowProducer netflowProducer( queueLength,
                                          numExamples,
                                          generators);


  // Size of hash table for both featureMap and featureSubscriber
  int capacity = 10000;

  // The global feature map
  auto featureMap = std::make_shared<FeatureMap>(capacity);

  int numFeatures = 4;
  std::string outputfile = "TestTransformProducerOutput.txt";
  auto subscriber = std::make_shared<FeatureSubscriber>(outputfile, capacity);

  int N = 1000;
  int k = 2;
  int nodeId = 0;
  std::string idAveSourceFlowSize = "aveSourceFlowSize";
  auto aveSourceFlowSize = std::make_shared<ExponentialHistogramAve<
                             double, VastNetflow, SrcPayloadBytes, DestIp>>(
                             N, k, nodeId, featureMap, idAveSourceFlowSize);
                            
  std::string idAveDestFlowSize = "aveDestFlowSize";
  auto aveDestFlowSize = std::make_shared<ExponentialHistogramAve<
                             double, VastNetflow, DestPayloadBytes, DestIp>>(
                             N, k, nodeId, featureMap, idAveDestFlowSize);

  std::string idVarSourceFlowSize = "varSourceFlowSize";
  auto varSourceFlowSize = std::make_shared<ExponentialHistogramVariance<
                             double, VastNetflow, SrcPayloadBytes, DestIp>>(
                             N, k, nodeId, featureMap, idVarSourceFlowSize);
  
  std::string idVarDestFlowSize = "varDestFlowSize";
  auto varDestFlowSize = std::make_shared<ExponentialHistogramVariance<
                             double, VastNetflow, DestPayloadBytes, DestIp>>(
                             N, k, nodeId, featureMap, idVarDestFlowSize);

  // Connect the feature producers to the netflow producer. 
  netflowProducer.registerConsumer(aveSourceFlowSize);
  netflowProducer.registerConsumer(aveDestFlowSize);
  netflowProducer.registerConsumer(varSourceFlowSize);
  netflowProducer.registerConsumer(varDestFlowSize);


  // Telling the feature producers about the subscriber
  aveSourceFlowSize->registerSubscriber( subscriber, idAveSourceFlowSize);
  aveDestFlowSize->registerSubscriber( subscriber, idAveDestFlowSize);
  varSourceFlowSize->registerSubscriber( subscriber, idVarSourceFlowSize);
  varDestFlowSize->registerSubscriber( subscriber, idVarDestFlowSize);

  subscriber->init();

  netflowProducer.run();
  //std::cout << "netflowproducers num read items " 
  //          << netflowProducer.getNumReadItems() << std::endl;
  subscriber->close();

  auto infile = std::ifstream(outputfile);
  std::string line;
  int skip = 200; //skip the first lines because they are inaccurate
  int linesSeen = 0;
  boost::char_separator<char> comma(",");
  while(std::getline(infile, line)) {
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
                              meanSourceFlowSize, 5);
            break;
          case 1:
            BOOST_CHECK_CLOSE(boost::lexical_cast<double>(t), 
                              meanDestFlowSize, 5);
            break;
          case 2:
            BOOST_CHECK_CLOSE(pow(boost::lexical_cast<double>(t), 0.5), 
                              devSourceFlowSize, 15);
            break;
          case 3:
            BOOST_CHECK_CLOSE(pow(boost::lexical_cast<double>(t), 0.5), 
                              devDestFlowSize, 15);
            break;
        }
        i++;
      }
    }
    linesSeen++;
  }
  BOOST_CHECK_EQUAL(linesSeen, numExamples);
  remove(outputfile.c_str());
}
