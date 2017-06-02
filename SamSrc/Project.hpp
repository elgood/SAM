#ifndef PROJECT_HPP
#define PROJECT_HPP

#include "AbstractConsumer.hpp"
#include "BaseComputation.hpp"
#include "FeatureMap.hpp"
#include "CollapsedFeatureMap.hpp"
#include "Util.hpp"

namespace sam {

/**
 * Projects out one field out of a two-field key.  I haven't figured
 * out how to generalize this.
 */
template <typename InputType, size_t keepField, size_t projectField, 
          size_t... keyFields>
class Project : public AbstractConsumer<InputType>,
                public BaseComputation
{
private:
  // A list of identifiers of the features we want to collect.
  std::list<std::string> identifiers;
  
  CollapsedFeatureMap& projectedFeatureMap;
public:
  Project(std::list<std::string> const& _identifiers,
          CollapsedFeatureMap& _projectedFeatureMap,
          size_t nodeId,
          FeatureMap& featureMap,
          std::string identifier) :
          identifiers(_identifiers),
          projectedFeatureMap(_projectedFeatureMap),
          BaseComputation(nodeId, featureMap, identifier)
  {} 

  bool consume(InputType const& input)
  {
    std::string origKey = generateKey<keyFields...>(input); 
    std::string newKey = generateKey<keepField>(input);
    std::string projectKey = generateKey<projectField>(input);

    // For all of the identifiers provided (usually the ones associated
    // with a FeatureMap), add the features from the oldFeatureMap and
    // add them to the CollapsedFeatureMap
    for (auto id : identifiers) {
      if (featureMap.exists(origKey, id)) {
        auto feature = featureMap.at(origKey, id);
        projectedFeatureMap.updateInsert(newKey, projectKey, id, *feature); 
      }
    }
    return true;
  }

};

}

#endif
