#ifndef PROJECT_HPP
#define PROJECT_HPP

#include "AbstractConsumer.hpp"
#include "BaseComputation.hpp"
#include "FeatureMap.hpp"
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
  
public:
  Project(std::list<std::string> const& _identifiers,
          size_t nodeId,
          FeatureMap& featureMap,
          std::string identifier) :
          identifiers(_identifiers),
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
        std::shared_ptr<const Feature> origFeature = featureMap.at(origKey, id);
        std::map<std::string, std::shared_ptr<Feature>> localFeatureMap;
        localFeatureMap[projectKey] = origFeature->createCopy();
        MapFeature mapFeature(localFeatureMap);
        this->featureMap.updateInsert(newKey, id, mapFeature);
      }
    }
    return true;
  }

};

}

#endif
