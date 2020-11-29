#ifndef LABEL_PRODUCER_HPP
#define LABEL_PRODUCER_HPP

/**
 * Operator that does nothing to the input. 
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
/**
 * Provides the label to subscribers. 
 */
template <typename EdgeType, size_t... keyFields> 
class LabelProducer: public AbstractConsumer<EdgeType>, 
                public BaseComputation,
                public FeatureProducer
{

public:

  /**
   * Constructor.
   * \param nodeId The nodeId of the node that is running this operator.
   * \param featureMap The global featureMap that holds the features produced
   *                   by this operator.
   * \param identifier A unique identifier associated with this operator.
   */
  LabelProducer(size_t nodeId,
           std::shared_ptr<FeatureMap> featureMap,
           std::string identifier) :
           BaseComputation(nodeId, featureMap, identifier) 
  {}

  bool consume(EdgeType const& edge) 
  {
    this->feedCount++;

    // Generates unique key from key fields
    std::string key = generateKey<keyFields...>(edge.tuple);

    double value = static_cast<double>(std::get<0>(edge.label));
    SingleFeature feature(value);
    this->featureMap->updateInsert(key, this->identifier, feature);

    this->notifySubscribers(edge.id, value);
    
    return true;
  }

  /// Nothing to do for terminate.
  void terminate() {}

};


}
#endif

