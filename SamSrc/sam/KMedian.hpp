#ifndef EXPONENTIAL_HISTOGRAM_SUM_HPP
#define EXPONENTIAL_HISTOGRAM_SUM_HPP

/**
 * Calculate the k-median
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
  class ExponentialHistogramSum: public AbstractConsumer<EdgeType>, 
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
    bool consume(EdgeType const& edge) 
    {
      this->feedCount++;

      if (this->feedCount % this->metricInterval == 0) {
        std::cout << "NodeId " << this->nodeId << " number of keys " 
                  << allWindows.size() << " feedCount " << this->feedCount
                  << std::endl;
      }

      // Generates unique key from key fields
      std::string key = generateKey<keyFields...>(edge.tuple);

      // Create an exponential histogram if it doesn't exist for the given key
      if (allWindows.count(key) == 0) {
        auto eh = std::shared_ptr<ExponentialHistogram<T>>(
                    new ExponentialHistogram<T>(N, k));
        allWindows[key] = eh;
      }

      // Update the data structure
      T value = std::get<valueField>(edge.tuple);
      allWindows[key]->add(value);

      // Getting the current sum and providing that to the feature map.
      T currentSum = allWindows[key]->getTotal();
      SingleFeature feature(currentSum);

      // Update the freature map with the new feature.  The feature map
      // takes as input the key for this item, the identifier for this operator,
      // and the feature itself.  The key and the identifier together uniquely
      // identify the feature.
      this->featureMap->updateInsert(key, this->identifier, feature);

      this->notifySubscribers(edge.id, currentSum);

      return true;
    }

    void terminate() {}

  };


}
#endif

