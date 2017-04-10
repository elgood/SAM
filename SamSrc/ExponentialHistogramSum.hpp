#ifndef EXPONENTIAL_HISTOGRAM_SUM_HPP
#define EXPONENTIAL_HISTOGRAM_SUM_HPP

/**
 * This is based on Mayur Datar's work with exponential histograms.
 */

#include <iostream>
#include <map>

#include "AbstractConsumer.h"
#include "BaseComputation.h"
#include "ExponentialHistogram.hpp"
#include "Features.hpp"

namespace sam {

template <typename T>
class ExponentialHistogramSum: public AbstractConsumer, public BaseComputation
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
                          vector<size_t> keyFields,
                          size_t valueField,
                          size_t nodeId,
                          FeatureMap& featureMap,
                          string identifier) :
                          BaseComputation(keyFields, valueField, nodeId,
                                          featureMap, identifier) 
  {
    this->N = N;
    this->k = k;
  }

  bool consume(string s) {
    feedCount++;
    if (feedCount % metricInterval == 0) {
      std::cout << "NodeId " << nodeId << " number of keys " 
                << allWindows.size() << std::endl;
    }

    Netflow netflow(s);

    // Generates unique key from key fields
    string key = generateKey(netflow);

    // Create an exponential histogram if it doesn't exist for the given key
    if (allWindows.count(key) == 0) {
      auto eh = std::shared_ptr<ExponentialHistogram<T>>(
                  new ExponentialHistogram<T>(N, k));
      std::pair<std::string, 
                std::shared_ptr<ExponentialHistogram<T>>> p(key, eh);
      allWindows[key] = eh;
    }

    std::string sValue = netflow.getField(valueField);

    T value = boost::lexical_cast<T>(sValue);

    allWindows[key]->add(value);

    // Getting the current sum and providing that to the imux data structure.
    T currentSum = allWindows[key]->getTotal();
    SingleFeature feature(currentSum);
    featureMap.updateInsert(key, identifier, feature);

    return true;
  }

};

}
#endif

