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

using std::map;

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

  map<string, shared_ptr<ExponentialHistogram<T>>> allWindows;

public:
  ExponentialHistogramSum(size_t N, size_t k,
                          vector<size_t> keyFields,
                          size_t valueField,
                          size_t nodeId) :
                          BaseComputation(keyFields, valueField, nodeId) 
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

    string key = generateKey(netflow);

    if (allWindows.count(key) == 0) {
      auto eh = shared_ptr<ExponentialHistogram<T>>(
                  new ExponentialHistogram<T>(N, k));
      std::pair<string, shared_ptr<ExponentialHistogram<T>>> p(key, eh);
      allWindows.insert(p);
    }

    string sValue = netflow.getField(valueField);

    T value = boost::lexical_cast<T>(sValue);

    allWindows[key]->add(value);
    return true;
  }

};

}
#endif

