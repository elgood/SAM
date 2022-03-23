#ifndef PROJECT_HPP
#define PROJECT_HPP

#include <sam/AbstractConsumer.hpp>
#include <sam/BaseComputation.hpp>
#include <sam/FeatureMap.hpp>
#include <sam/Util.hpp>
#include <sam/tuples/Edge.hpp>

namespace sam {

/**
 * Projects out one field out of a two-field key.  I haven't figured
 * out how to generalize this.
 */
template <typename EdgeType,
          size_t keepField, size_t projectField, 
          size_t... keyFields>
class Project : public AbstractConsumer<EdgeType>,
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

  bool consume(EdgeType const& edge)
  {
    //std::cout << "in project" << std::endl;
    std::string origKey = generateKey<keyFields...>(edge.tuple); 
    std::string newKey = generateKey<keepField>(edge.tuple);
    std::string projectKey = generateKey<projectField>(edge.tuple);

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
      DEBUG_PRINT("Project::consume processing id %s\n", id.c_str())
      DEBUG_PRINT("Project::consume looking for origkey %s\n", origKey.c_str())
      if (featureMap->exists(origKey, id)) {
        std::shared_ptr<const Feature> origFeature = 
          featureMap->at(origKey, id);
        std::map<std::string, std::shared_ptr<Feature>> localFeatureMap;
        localFeatureMap[projectKey] = origFeature->createCopy();
        MapFeature mapFeature(localFeatureMap);

        // We update the global feature map with the MapFeature.  If there is
        // no MapFeature associated with the newkey, then we simply add it.
        // If there is a MapFeature, the original MapFeature and the new 
        // MapFeature are unioned.
        DEBUG_PRINT("Project::consume Inserting map feature with key %s\n", 
          newKey.c_str())
        this->featureMap->updateInsert(newKey, id, mapFeature);
      }
    }
    return true;
  }

  void terminate() {}
};

}

#endif
