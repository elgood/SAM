#ifndef IMUX_DATA_HPP
#define IMUX_DATA_HPP

#include <map>
#include <string>
#include <iostream>

#include "ImuxDataItem.hpp"

using std::map;
using std::string;
using std::shared_ptr;
using std::cout;
using std::endl;

namespace sam {

class ImuxData {
private:

  /**
   * This holds the mapping from the key (e.g. a dest ip) to the 
   * data item associated with the key.
   */
  map<string, shared_ptr<ImuxDataItem>> imuxData; 

public:

  /**
   * Adds a feature with the specified identifier to the item
   * with the specified key. 
   */
  void addFeature(string key, string identifier, 
           shared_ptr<AdditionalFeature> feature)
  {
    if (imuxData.count(key) <= 0) {
      imuxData[key] = shared_ptr<ImuxDataItem>(new ImuxDataItem());
    }
    imuxData[key].get()->addFeature(identifier, feature);
  }

};

}

#endif
