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
  /// A list of identifiers of the features we want to collect.
  std::list<std::string> identifiers;
  
public:
  Project(std::list<std::string> const& _identifiers,
          size_t nodeId,
          std::shared_ptr<FeatureMap> featureMap,
          std::string identifier) :
          identifiers(_identifiers),
          BaseComputation(nodeId, featureMap, identifier)
  {} 

  bool consume(InputType const& input)
  {
    //std::cout << "in project" << std::endl;
    std::string origKey = generateKey<keyFields...>(input); 
    std::string newKey = generateKey<keepField>(input);
    std::string projectKey = generateKey<projectField>(input);

    // For all of the identifiers specified we create a MapFeature.  A
    // MapFeature holds the mapping from the projected key to original
    // feature.  For example, there is a stream of data with tuples
    // <DestIp, SrcIp, TimeDiff> where time diff is the amount of time
    // between communications between DestIp and SrcIp.  Then we calculate
    // the variance on the time diff.  So there is a feature for each
    // DestIp, SrcIp pair.  Then we project out SrcIp.  Now we have k
    // variance features for a DestIp, where k is the number of unique
    // SrcIps associated with a DestIp.  In this case, projectKey is SrcIp,
    // and we create a copy of the variance feature and put it in the map.
    // The map will be the size of the number of unique SrcIps.
    // TODO: Right now, all features throughout time are kept.  So any
    // time a DestIp talks to a SrcIp, that stays around forever, no matter
    // how long ago it took place.  
    for (auto id : identifiers) {
      if (featureMap->exists(origKey, id)) {
        std::shared_ptr<const Feature> origFeature = featureMap->at(origKey, id);
        std::map<std::string, std::shared_ptr<Feature>> localFeatureMap;
        localFeatureMap[projectKey] = origFeature->createCopy();
        MapFeature mapFeature(localFeatureMap);

        // We update the global feature map with the MapFeature.  If there is
        // no MapFeature associated with the newkey, then we simply add it.
        // If there is a MapFeature, the original MapFeature and the new 
        // MapFeature are unioned.
        this->featureMap->updateInsert(newKey, id, mapFeature);
      }
    }
    return true;
  }
};

}

#endif
