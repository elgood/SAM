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
  CollapsedFeatureMap const& collapsedFeatureMap;
  
  std::function<double(std::list<std::shared_ptr<Feature>>)> func;

public:
  CollapsedConsumer(CollapsedFeatureMap const& _collapsedFeatureMap,
            std::function<double(std::list<std::shared_ptr<Feature>>)> _func,
            size_t nodeId,
            FeatureMap& featureMap,
            std::string identifier) :
            collapsedFeatureMap(_collapsedFeatureMap),
            func(_func),
            BaseComputation(nodeId, featureMap, identifier)
  {
    
  }
 
  bool consume(TupleType const& tuple)
  {
    std::string key = generateKey<keyFields...>(tuple);

    double result = 0;
    bool b = this->collapsedFeatureMap.applyAggregate(key, 
                                                      identifier, 
                                                      func, 
                                                      result);
    if (b) {
      SingleFeature feature(result);
      this->featureMap.updateInsert(key, this->identifier, feature);
    } 

    return b;
  }
  
};


}

#endif
