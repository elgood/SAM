#ifndef IMUX_DATA_ITEM_HPP
#define IMUX_DATA_ITEM_HPP

#include <map>
#include <iostream>

using std::map;
using std::shared_ptr;
using std::cout;
using std::endl;

namespace sam {

class AdditionalFeature {

};

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
  shared_ptr<Netflow> netflow;

  std::map<string, string> blah2;

  // A map of additional features that have been added
  std::map<string, shared_ptr<AdditionalFeature>> additionalFeatures;

  typedef std::map<string, shared_ptr<AdditionalFeature>>::value_type value_type;

public:

  ImuxDataItem() {

  }
  
  void addFeature(string key, shared_ptr<AdditionalFeature> feature) {
    value_type insertValue(key, feature);
    additionalFeatures.insert(insertValue);
  }

  void setNetflow(shared_ptr<Netflow> ntf) {
    netflow = netflow;
  }
  
};

}

#endif
