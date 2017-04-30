#ifndef EXPONENTIAL_HISTOGRAM_VARIANCE_HPP
#define EXPONENTIAL_HISTOGRAM_VARIANCE_HPP

/**
 * This is based on Mayur Datar's work with exponential histograms.
 * For Variance we need to keep track of the sum of the items and the
 * sum of the squares.
 */

#include <iostream>
#include <map>

#include "AbstractConsumer.hpp"
#include "BaseComputation.hpp"
#include "ExponentialHistogram.hpp"
#include "Features.hpp"
#include "Netflow.hpp"

namespace sam {

template <typename T, size_t valueField, size_t... keyFields>
class ExponentialHistogramVariance : public AbstractConsumer<Netflow>, 
                            public BaseComputation<valueField, keyFields...>
{
private:

  // Determines number of buckets.  If there are k/2 + 2 buckets
  // of the same size (k + 2 buckets if the bucket size equals 1), 
  // the oldest two buckets are combined.
  size_t k; 

  // The size of the sliding window
  size_t N; 

  std::map<string, std::shared_ptr<ExponentialHistogram<T>>> sums;
  std::map<string, std::shared_ptr<ExponentialHistogram<T>>> squares;

public:
  ExponentialHistogramVariance(size_t N, size_t k,
                          size_t nodeId,
                          FeatureMap& featureMap,
                          std::string identifier) :
                          BaseComputation<valueField, keyFields...>(
                            nodeId,featureMap, identifier) 
                                          
  {
    this->N = N;
    this->k = k;
  }

  bool consume(Netflow const& netflow) {
    feedCount++;
    if (feedCount % this->metricInterval == 0) {
      std::cout << "NodeId " << this->nodeId << " number of keys " 
                << sums.size() << std::endl;
    }

    // Generates unique key from key fields
    string key = this->generateKey(netflow);

    if (sums.count(key) == 0) {
      auto eh = std::shared_ptr<ExponentialHistogram<T>>(
                  new ExponentialHistogram<T>(N, k));
      std::pair<std::string, 
                std::shared_ptr<ExponentialHistogram<T>>> p(key, eh);
      sums[key] = eh;

      eh = std::shared_ptr<ExponentialHistogram<T>>(
                  new ExponentialHistogram<T>(N, k));
      p = std::pair<std::string, 
                    std::shared_ptr<ExponentialHistogram<T>>>(key, eh);
      squares[key] = eh;
    }

    string sValue = boost::lexical_cast<std::string>(
                      std::get<valueField>(netflow));

    T value = boost::lexical_cast<T>(sValue);

    sums[key]->add(value);
    squares[key]->add(value * value);

    // Getting the current variance and providing that to the imux data 
    // structure.
    T currentSum = sums[key]->getTotal();
    T currentSquares = squares[key]->getTotal();
    double currentVariance = calculateVariance(currentSquares, currentSum);
    SingleFeature feature(currentVariance);
    this->featureMap.updateInsert(key, this->identifier, feature);



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

