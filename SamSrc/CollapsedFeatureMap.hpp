#ifndef COLLAPSED_FEATURE_MAP
#define COLLAPSED_FEATURE_MAP

#include <mutex>
#include <iostream>
#include "Features.hpp"

namespace sam {

class CollapsedFeatureMap
{
private:
  mutable std::mutex mu;

  int capacity;

  // The outer map is indexed by the kept keys.  The inner mapp is indexed
  // by the projected keys.  Right now the final value stored is a double.
  std::map<std::string, std::map<std::string, std::shared_ptr<Feature>>> 
    features;

public:

  CollapsedFeatureMap() {}
 
  /**
   * key is the key that was kept.
   * projectedKey is the key that was projected out.
   * featureName is the identifier of the feature
   * d is the double to insert (only doubles are supported right now).
   */ 
  bool updateInsert(std::string const& key,
                    std::string const& projectedKey,
                    std::string const& featureName,
                    Feature const& feature)
  {
    std::lock_guard<std::mutex> mutex_(mu);
    features[key + featureName][projectedKey] = feature.createCopy(); 
    return true;
  }

  /**
   * \param key is the key that was kept
   * \param featureName is the identifier of the feature
   * \param func The aggregate function to apply.
   * \param result The result of the aggregation.
   * Returns true if everything went fine.
   */
  bool applyAggregate(std::string const& key,
            std::string const& featureName,
            std::function<double(std::list<std::shared_ptr<Feature>>)> func,
            double &result) const
  {
    result = 0;
    std::lock_guard<std::mutex> mutex_(mu);

    std::list<std::shared_ptr<Feature>> mylist;
    if (features.find(key + featureName) != features.end()) {
      for (auto const& k : features.at(key + featureName)) {
        //std::cout << k.first << "  " << k.second << std::endl;
        auto feature = k.second;
        mylist.push_back(feature);
      }
      result = func(mylist);
      return true;
    }
    return false;
  }
};

}

#endif
