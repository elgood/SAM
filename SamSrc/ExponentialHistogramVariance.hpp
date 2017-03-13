#ifndef EXPONENTIAL_HISTOGRAM_SUM_HPP
#define EXPONENTIAL_HISTOGRAM_SUM_HPP

/**
 * This is based on Mayur Datar's work with exponential histograms.
 * For Variance we need to keep track of the sum of the items and the
 * sum of the squares.
 */

#include <iostream>
#include <map>

#include "AbstractConsumer.h"
#include "BaseComputation.h"
#include "ExponentialHistogram.hpp"
#include "Features.hpp"

using std::map;

namespace sam {

template <typename T>
class ExponentialHistogramVariance : public AbstractConsumer, public BaseComputation
{
private:

  // Determines number of buckets.  If there are k/2 + 2 buckets
  // of the same size (k + 2 buckets if the bucket size equals 1), 
  // the oldest two buckets are combined.
  size_t k; 

  // The size of the sliding window
  size_t N; 

  map<string, shared_ptr<ExponentialHistogram<T>>> sums;
  map<string, shared_ptr<ExponentialHistogram<T>>> squares;

public:
  ExponentialHistogramVariance(size_t N, size_t k,
                          vector<size_t> keyFields,
                          size_t valueField,
                          size_t nodeId,
                          ImuxData& imuxData,
                          string identifier) :
                          BaseComputation(keyFields, valueField, nodeId,
                                          imuxData, identifier) 
  {
    this->N = N;
    this->k = k;
  }

  bool consume(string s) {
    feedCount++;
    if (feedCount % metricInterval == 0) {
      std::cout << "NodeId " << nodeId << " number of keys " 
                << sums.size() << std::endl;
    }

    Netflow netflow(s);

    // Generates unique key from key fields
    string key = generateKey(netflow);

    if (sums.count(key) == 0) {
      auto eh = shared_ptr<ExponentialHistogram<T>>(
                  new ExponentialHistogram<T>(N, k));
      std::pair<string, shared_ptr<ExponentialHistogram<T>>> p(key, eh);
      sums.insert(p);

      eh = shared_ptr<ExponentialHistogram<T>>(
                  new ExponentialHistogram<T>(N, k));
      p = std::pair<string, shared_ptr<ExponentialHistogram<T>>>(key, eh);
      squares.insert(p);
    }

    string sValue = netflow.getField(valueField);

    T value = boost::lexical_cast<T>(sValue);

    sums[key]->add(value);
    squares[key]->add(value * value);

    // Getting the current variance and providing that to the imux data 
    // structure.
    T currentSum = sums[key]->getTotal();
    T currentSquares = squares[key]->getTotal();
    double currentVariance = calculateVariance(currentSquares, currentSum);
    auto feature = shared_ptr<SingleFeature<double>>(
                    new SingleFeature<double>(currentVariance));
    imuxData.addFeature(key, identifier, feature);



    return true;
  }

private:
  double calculateVariance(T sumOfSquares, T sum) {
    double variance = boost::lexical_cast<double>(sumOfSquares) / N -
                      boost::lexical_cast<double>(sum * sum) / (N * N);
    return variance;
  }

};

}
#endif

