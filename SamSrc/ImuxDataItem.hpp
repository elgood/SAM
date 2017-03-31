#ifndef IMUX_DATA_ITEM_HPP
#define IMUX_DATA_ITEM_HPP

#include <map>
#include <iostream>

#include "Netflow.h"
#include "Features.hpp"

namespace sam {

/**
 * ImuxDataItem contains the original netflow from which additional
 * features have been generated, and also additional features that have 
 * been added.  There is a map that maps between the name given to the 
 * feature by the query specification and the feature.
 */
class ImuxDataItem
{
private:
  // The latest netflow
  std::shared_ptr<Netflow> netflow;

  // A map of additional features that have been added.  The string key
  // is the identifier specified in the query (e.g top2)
  std::map<std::string, std::shared_ptr<Feature>> additionalFeatures;

  typedef std::map<std::string, std::shared_ptr<Feature>>::value_type 
    value_type;

public:

  ImuxDataItem() {

  }

  bool existsFeature(string key) {
    if (additionalFeatures.count(key) > 0) {
      return true;
    }
    return false;
  }
  
  void addFeature(string key, std::shared_ptr<Feature> feature) {
    additionalFeatures[key] = feature;
  }

  void updateFeature(string key, Feature const& feature)
  {
    additionalFeatures[key]->update(feature);
  }

  void setNetflow(std::shared_ptr<Netflow> ntf) {
    netflow = netflow;
  }

  std::shared_ptr<Feature> getFeature(string key) const {
    return additionalFeatures.at(key);    
  }
  
};

}

#endif
