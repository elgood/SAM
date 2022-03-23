#ifndef COLLAPSED_CONSUMER_HPP
#define COLLAPSED_CONSUMER_HPP

#include <sam/Util.hpp>
#include <sam/Features.hpp>
#include <sam/BaseComputation.hpp>
#include <sam/AbstractConsumer.hpp>
#include <sam/FeatureProducer.hpp>

namespace sam {


template  <typename EdgeType, size_t... keyFields>
class CollapsedConsumer : public BaseComputation, 
                          public AbstractConsumer<EdgeType>,
                          public FeatureProducer
{
private:
  /**
   * The collapsed consumer applies an aggregation function to a list
   * of features.  This function is specified at construction time.
   */
  std::function<double(std::list<std::shared_ptr<Feature>>)> func;

  /**
   * The collapsed consumer is aggregating a list of features with
   * a targetId.  This is specified at constuction time. 
   */
  std::string targetId;

public:
  /**
   * \param _func The function that will be applied to a list of features.
   *
   */
  CollapsedConsumer(
            std::function<double(std::list<std::shared_ptr<Feature>>)> _func,
            std::string _targetId,
            size_t nodeId,
            std::shared_ptr<FeatureMap> featureMap,
            std::string newIdentifier) :
            func(_func),
            targetId(_targetId),
            BaseComputation(nodeId, featureMap, newIdentifier)
  {
    
  }
 
  bool consume(EdgeType const& edge)
  {
    this->feedCount++;

    if (this->feedCount % this->metricInterval == 0) {

      std::string message = "CollapsedConsumer id " +
        this->identifier + " NodeId " +
        boost::lexical_cast<std::string>(this->nodeId) + 
        + " feedCount " + boost::lexical_cast<std::string>(this->feedCount) +
        "\n";
        printf("%s", message.c_str());
    }

    std::string key = generateKey<keyFields...>(edge.tuple);
    DEBUG_PRINT("CollapsedConsumer::consume key %s\n", key.c_str())

    if (featureMap->exists(key, targetId))
    {
      auto mapFeature = std::static_pointer_cast<const MapFeature>(
                          this->featureMap->at(key, targetId));
      double result = mapFeature->evaluate(func);
       
      SingleFeature feature(result);
      this->featureMap->updateInsert(key, this->identifier, feature);

      this->notifySubscribers(edge.id, result);
  
      return true;   
    } else {
      DEBUG_PRINT("CollapsedConsumer::consume key %s could not be found!\n",
        key.c_str())
    }

    return false;
  }

  void terminate() {}
  
};


}

#endif
