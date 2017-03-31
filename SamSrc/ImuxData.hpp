#ifndef IMUX_DATA_HPP
#define IMUX_DATA_HPP

#include <map>
#include <string>
#include <iostream>

#include "ImuxDataItem.hpp"
#include "Features.hpp"


namespace sam {

class ImuxData {
private:

  /**
   * This holds the mapping from the key (e.g. a dest ip) to the 
   * data item associated with the key.
   */
  std::map<string, std::shared_ptr<ImuxDataItem>> imuxData; 

 

public:

  /**
   * Adds a feature with the specified identifier to the item
   * with the specified key. 
   */
  void addFeature(std::string key, std::string identifier, 
                  std::shared_ptr<Feature> feature)
  {
    if (imuxData.count(key) <= 0) {
      imuxData[key] = std::shared_ptr<ImuxDataItem>(new ImuxDataItem());
    }
    imuxData[key].get()->addFeature(identifier, feature);
  }

  bool existsFeature(std::string key, std::string identifier)
  {
    if (imuxData.count(key) <= 0) {
      return false;
    }
    return imuxData[key]->existsFeature(identifier);
  }

  void updateFeature(std::string key, std::string identifier, 
                     Feature const& feature)
  {
    imuxData[key]->updateFeature(identifier, feature);
  }

  std::shared_ptr<ImuxDataItem> getDataItem(string key) 
  {
    return imuxData.at(key);
  }

};

}

#endif
