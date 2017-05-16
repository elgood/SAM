#ifndef EXPONENTIAL_HISTOGRAM_SUM_HPP
#define EXPONENTIAL_HISTOGRAM_SUM_HPP

/**
 * This is based on Mayur Datar's work with exponential histograms.
 */

#include <iostream>
#include <map>

#include "AbstractConsumer.hpp"
#include "BaseComputation.hpp"
#include "ExponentialHistogram.hpp"
#include "Features.hpp"

namespace sam {

template <typename T, typename InputType,  
          size_t valueField, size_t... keyFields>
class ExponentialHistogramSum: public AbstractConsumer<InputType>, 
                               public BaseComputation<valueField, keyFields...>
{
private:

  // Determines number of buckets.  If there are k/2 + 2 buckets
  // of the same size (k + 2 buckets if the bucket size equals 1), 
  // the oldest two buckets are combined.
  size_t k; 

  // The size of the sliding window
  size_t N; 

  std::map<string, std::shared_ptr<ExponentialHistogram<T>>> allWindows;

public:
  ExponentialHistogramSum(size_t N, size_t k,
                          size_t nodeId,
                          FeatureMap& featureMap,
                          string identifier) :
                          BaseComputation<valueField, keyFields...>(nodeId,
                                          featureMap, identifier) 
  {
    this->N = N;
    this->k = k;
  }

  bool consume(InputType const& input) {
    this->feedCount++;
    if (this->feedCount % this->metricInterval == 0) {
      std::cout << "NodeId " << this->nodeId << " number of keys " 
                << allWindows.size() << std::endl;
    }

    // Generates unique key from key fields
    string key = this->generateKey(input);

    // Create an exponential histogram if it doesn't exist for the given key
    if (allWindows.count(key) == 0) {
      auto eh = std::shared_ptr<ExponentialHistogram<T>>(
                  new ExponentialHistogram<T>(N, k));
      std::pair<std::string, 
                std::shared_ptr<ExponentialHistogram<T>>> p(key, eh);
      allWindows[key] = eh;
    }

    T value = std::get<valueField>(input);

    allWindows[key]->add(value);

    // Getting the current sum and providing that to the imux data structure.
    T currentSum = allWindows[key]->getTotal();
    SingleFeature feature(currentSum);
    this->featureMap.updateInsert(key, this->identifier, feature);

    return true;
  }

};

//TODO Should make the function a template parameter so we don't have to copy
// the code.
template <typename T, typename InputType,
          size_t valueField, size_t... keyFields>
class ExponentialHistogramAve: public AbstractConsumer<InputType>, 
                               public BaseComputation<valueField, keyFields...>
{
private:

  // Determines number of buckets.  If there are k/2 + 2 buckets
  // of the same size (k + 2 buckets if the bucket size equals 1), 
  // the oldest two buckets are combined.
  size_t k; 

  // The size of the sliding window
  size_t N; 

  std::map<string, std::shared_ptr<ExponentialHistogram<T>>> allWindows;

public:
  ExponentialHistogramAve(size_t N, size_t k,
                          size_t nodeId,
                          FeatureMap& featureMap,
                          string identifier) :
                          BaseComputation<valueField, keyFields...>(nodeId,
                                          featureMap, identifier) 
  {
    this->N = N;
    this->k = k;
  }

  bool consume(InputType const& input) {
    this->feedCount++;
    if (this->feedCount % this->metricInterval == 0) {
      std::cout << "NodeId " << this->nodeId << " number of keys " 
                << allWindows.size() << std::endl;
    }

    // Generates unique key from key fields
    string key = this->generateKey(input);

    // Create an exponential histogram if it doesn't exist for the given key
    if (allWindows.count(key) == 0) {
      auto eh = std::shared_ptr<ExponentialHistogram<T>>(
                  new ExponentialHistogram<T>(N, k));
      std::pair<std::string, 
                std::shared_ptr<ExponentialHistogram<T>>> p(key, eh);
      allWindows[key] = eh;
    }

    T value = std::get<valueField>(input);

    allWindows[key]->add(value);

    // Getting the current sum and providing that to the imux data structure.
    T currentSum = allWindows[key]->getTotal();
    SingleFeature feature(currentSum/ allWindows[key]->getNumSlots());
    this->featureMap.updateInsert(key, this->identifier, feature);

    return true;
  }

};


}
#endif

