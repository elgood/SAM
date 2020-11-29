#ifndef EXPONENTIAL_HISTOGRAM_VARIANCE_HPP
#define EXPONENTIAL_HISTOGRAM_VARIANCE_HPP

/**
 * This is based on Mayur Datar's work with exponential histograms.
 * For Variance we need to keep track of the sum of the items and the
 * sum of the squares.
 */

#include <iostream>
#include <map>

#include <sam/AbstractConsumer.hpp>
#include <sam/BaseComputation.hpp>
#include <sam/ExponentialHistogram.hpp>
#include <sam/Features.hpp>
#include <sam/Util.hpp>
#include <sam/FeatureProducer.hpp>
#include <sam/tuples/Edge.hpp>

namespace sam {

template <typename T, typename EdgeType,
          size_t valueField, size_t... keyFields>
class ExponentialHistogramVariance : 
  public AbstractConsumer<EdgeType>, 
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

  std::map<std::string, std::shared_ptr<ExponentialHistogram<T>>> sums;
  std::map<std::string, std::shared_ptr<ExponentialHistogram<T>>> squares;

public:
  ExponentialHistogramVariance(size_t N, size_t k,
                          size_t nodeId,
                          std::shared_ptr<FeatureMap> featureMap,
                          std::string identifier) :
                          BaseComputation(
                            nodeId,featureMap, identifier) 
                                          
  {
    this->N = N;
    this->k = k;
  }

  bool consume(EdgeType const& edge) 
  {
    this->feedCount++;
    if (this->feedCount % this->metricInterval == 0) {
      std::string message = "ExponentialHistogramVariance id " +
        this->identifier + " NodeId " +
        boost::lexical_cast<std::string>(this->nodeId) + 
        " number of keys " + boost::lexical_cast<std::string>(sums.size())
        + " feedCount " + boost::lexical_cast<std::string>(this->feedCount) +
        "\n";
        printf("%s", message.c_str());
    }

    // Generates unique key from key fields
    std::string key = generateKey<keyFields...>(edge.tuple);

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

    std::string sValue = boost::lexical_cast<std::string>(
                      std::get<valueField>(edge.tuple));

    T value = boost::lexical_cast<T>(sValue);

    sums[key]->add(value);
    squares[key]->add(value * value);

    // Getting the current variance and providing that to the featureMap
    T currentSum = sums[key]->getTotal();
    T currentSquares = squares[key]->getTotal();
  
    size_t numItems = sums[key]->getNumItems();
    double currentVariance = calculateVariance(currentSquares, currentSum,
                                               numItems);
    SingleFeature feature(currentVariance);
    this->featureMap->updateInsert(key, this->identifier, feature);

    notifySubscribers(edge.id, currentVariance);    

    return true;
  }

  void terminate() {}

private:
  double calculateVariance(T sumOfSquares, T sum, size_t numItems) {
    double variance = boost::lexical_cast<double>(sumOfSquares) / numItems -
                boost::lexical_cast<double>(sum * sum) / (numItems * numItems);
    return variance;
  }

};

}
#endif

