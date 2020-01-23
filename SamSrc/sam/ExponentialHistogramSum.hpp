#ifndef EXPONENTIAL_HISTOGRAM_SUM_HPP
#define EXPONENTIAL_HISTOGRAM_SUM_HPP

/**
 * Calculates the sum over a sliding window using Mayur Datar's work on 
 * exponential histograms.
 */

#include <iostream>
#include <map>

#include <sam/AbstractConsumer.hpp>
#include <sam/BaseComputation.hpp>
#include <sam/ExponentialHistogram.hpp>
#include <sam/Features.hpp>
#include <sam/Util.hpp>
#include <sam/FeatureProducer.hpp>

namespace sam {

template <typename T, typename InputType,  
          size_t valueField, size_t... keyFields>
class ExponentialHistogramSum: public AbstractConsumer<InputType>, 
                               public BaseComputation,
                               public FeatureProducer
{
private:

  // Determines number of buckets.  If there are k/2 + 2 buckets
  // of the same size (k + 2 buckets if the bucket size equals 1), 
  // the oldest two buckets are combined.
  size_t k; 

  // The size of the sliding window
  size_t N; 

  // A mapping from keyFields to the associated exponential histogram.
  std::map<std::string, std::shared_ptr<ExponentialHistogram<T>>> allWindows;

public:
  /**
   * Constructor.
   * \param N The number of elements in the sliding window.
   * \param k Determines the number of buckets.  If there are k/2 + 2 buckets
   *          of the same size (k + 2 buckets if bucket size equals 1),
   *          the oldest two buckets are combined. 
   * \param nodeId The nodeId of the node that is running this operator.
   * \param featureMap The global featureMap that holds the features produced
   *                   by this operator.
   * \param identifier A unique identifier associated with this operator.
   */
  ExponentialHistogramSum(size_t N, size_t k,
                          size_t nodeId,
                          std::shared_ptr<FeatureMap> featureMap,
                          std::string identifier) :
                          BaseComputation(nodeId, featureMap, identifier) 
                                          
  {
    this->N = N;
    this->k = k;
  }

  /**
   * Main method of an operator.  Processes the tuple.
   * \param input The tuple to process.
   */
  bool consume(InputType const& input) {
    this->feedCount++;

    if (this->feedCount % this->metricInterval == 0) {
      std::cout << "NodeId " << this->nodeId << " number of keys " 
                << allWindows.size() << " feedCount " << this->feedCount
                << std::endl;
    }

    // Generates unique key from key fields
    std::string key = generateKey<keyFields...>(input);

    // Create an exponential histogram if it doesn't exist for the given key
    if (allWindows.count(key) == 0) {
      auto eh = std::shared_ptr<ExponentialHistogram<T>>(
                  new ExponentialHistogram<T>(N, k));
      allWindows[key] = eh;
    }

    // Update the data structure
    T value = std::get<valueField>(input);
    allWindows[key]->add(value);

    // Getting the current sum and providing that to the feature map.
    T currentSum = allWindows[key]->getTotal();
    SingleFeature feature(currentSum);

    // Update the freature map with the new feature.  The feature map
    // takes as input the key for this item, the identifier for this operator,
    // and the feature itself.  The key and the identifier together uniquely
    // identify the feature.
    this->featureMap->updateInsert(key, this->identifier, feature);

    // This assumes the identifier of the tuple is the first element
    std::size_t id = std::get<0>(input);
    this->notifySubscribers(id, currentSum);

    return true;
  }

  void terminate() {}

};

//TODO Should make the function a template parameter so we don't have to copy
// the code.
template <typename T, typename InputType,
          size_t valueField, size_t... keyFields>
class ExponentialHistogramAve: public AbstractConsumer<InputType>, 
                               public BaseComputation,
                               public FeatureProducer
{
private:

  // Determines number of buckets.  If there are k/2 + 2 buckets
  // of the same size (k + 2 buckets if the bucket size equals 1), 
  // the oldest two buckets are combined.
  size_t k; 

  // The size of the sliding window
  size_t N; 

  // Mapping from string key to the ExponentialHistogram representing the
  // key.
  std::map<std::string, std::shared_ptr<ExponentialHistogram<T>>> allWindows;

public:
  ExponentialHistogramAve(size_t N, size_t k,
                          size_t nodeId,
                          std::shared_ptr<FeatureMap> featureMap,
                          std::string identifier) :
                          BaseComputation(nodeId, featureMap, identifier) 
                                          
  {
    this->N = N;
    this->k = k;
  }

  bool consume(InputType const& input) {
    this->feedCount++;
    if (this->feedCount % this->metricInterval == 0) {
      std::string message = "ExponentialHistogramAve id " + this->identifier +
        " NodeId " + boost::lexical_cast<std::string>(this->nodeId) + 
        " number of keys " + 
        boost::lexical_cast<std::string>(allWindows.size()) + 
        " feedCount " + boost::lexical_cast<std::string>(this->feedCount)+ "\n";
      printf("%s", message.c_str());
    }

    // Generates unique key from key fields
    std::string key = generateKey<keyFields...>(input);

    // Create an exponential histogram if it doesn't exist for the given key
    if (allWindows.count(key) == 0) {
      auto eh = std::shared_ptr<ExponentialHistogram<T>>(
                  new ExponentialHistogram<T>(N, k));
      allWindows[key] = eh;
    }

    T value = std::get<valueField>(input);

    allWindows[key]->add(value);

    // Getting the current sum and providing that to the featuremap data structure.
    T currentSum = allWindows[key]->getTotal();
    SingleFeature feature(currentSum/ allWindows[key]->getNumItems());
    this->featureMap->updateInsert(key, this->identifier, feature);
  
    std::size_t id = std::get<0>(input);

    // Notify any subscribers of the new value, which is a frequency.
    this->notifySubscribers(id, currentSum / allWindows[key]->getNumItems());

    return true;
  }

  void terminate() {}
};


}
#endif

