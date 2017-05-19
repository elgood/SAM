#ifndef COLLAPSED_FEATURE_MAP
#define COLLAPSED_FEATURE_MAP

#include <mutex>
#include <iostream>

namespace sam {

class CollapsedFeatureMap
{
private:
  std::mutex mu;

  int capacity;

  std::map<std::string, std::map<std::string, double>> features;

public:

  CollapsedFeatureMap() {}
 
  /**
   * key is the key of the projected tuple.
   * projectedKey is the key that was projected out.
   * featureName is the identifier of the feature
   * d is the double to insert (only doubles are supported right now).
   */ 
  bool updateInsert(std::string const& key,
                    std::string const& projectedKey,
                    std::string const& featureName,
                    double d)
  {
    std::lock_guard<std::mutex> mutex_(mu);
    features[key + featureName][projectedKey] = d; 
    return true;
  }

  bool applyAggregate(std::string const& key,
                      std::string const& featureName,
                      std::function<double(std::list<double>)> func,
                      double &result) 
  {
    result = 0;
    std::lock_guard<std::mutex> mutex_(mu);

    std::list<double> mylist;
    if (features[key + featureName].size() > 0) {
      for (auto const& k : features[key + featureName]) {
        //std::cout << k.first << "  " << k.second << std::endl;
        double d = k.second;
        mylist.push_back(d);
      }
      result = func(mylist);
      return true;
    }
    return false;
  }
};

}

#endif
