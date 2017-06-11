#ifndef COLLAPSED_CONSUMER_HPP
#define COLLAPSED_CONSUMER_HPP

#include "Util.hpp"
#include "Features.hpp"

namespace sam {


template  <typename TupleType,
          size_t... keyFields>
class CollapsedConsumer : public BaseComputation, 
                          public AbstractConsumer<TupleType>
{
private:
  std::function<double(std::list<std::shared_ptr<Feature>>)> func;
  std::string oldIdentifier;

public:
  CollapsedConsumer(
            std::function<double(std::list<std::shared_ptr<Feature>>)> _func,
            std::string _oldIdentifier,
            size_t nodeId,
            FeatureMap& featureMap,
            std::string newIdentifier) :
            func(_func),
            oldIdentifier(_oldIdentifier),
            BaseComputation(nodeId, featureMap, newIdentifier)
  {
    
  }
 
  bool consume(TupleType const& tuple)
  {
    std::string key = generateKey<keyFields...>(tuple);

    if (featureMap.exists(key, oldIdentifier))
    {
      auto mapFeature = std::static_pointer_cast<const MapFeature>(
                          this->featureMap.at(key, oldIdentifier));
      double result = mapFeature->evaluate(func);
       
      SingleFeature feature(result);
      this->featureMap.updateInsert(key, this->identifier, feature);
  
      return true;   
    }

    return false;

  }
  
};


}

#endif
