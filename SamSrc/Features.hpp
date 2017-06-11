#ifndef FEATURES_HPP
#define FEATURES_HPP

#include <exception>
#include <boost/lexical_cast.hpp>
#include <vector>

#define VALUE_FUNCTION "value"

namespace sam {

class Feature {
protected:
  double value;

public:
  Feature() {
    value = 0;
  }

  Feature(double value) {
    this->value = value;
  }

  /**
   * Applies a function to this feature and returns the result.
   */
  double evaluate(std::function<double(Feature const *)> func) const  {
    return func(this);
  }

  /**
   * Updates the current feature given another feature (usually of the same
   * type).
   */
  virtual void update(Feature const& feature) = 0;
  
  /**
   * Creates a deep copy of this feature and returns it.
   */
  virtual std::shared_ptr<Feature> createCopy() const = 0;
  
  virtual bool operator==(Feature const& other) const = 0;

  bool operator!=(Feature const& other) const {
    return !(*this == other);
  }

  virtual std::string toString() const = 0;

  virtual double getValue() const { return value; }
};

auto valueFunc = [](Feature const * feature)->double {
  return feature->getValue();
};

/**
 * A feature that is a map of features
 */
class MapFeature : public Feature {
private:
  std::map<std::string, std::shared_ptr<Feature>> localFeatureMap;
public:

  /**
   * Takes a reference to a const feature map and populates the localFeatureMap
   * of this object.
   */
  MapFeature(std::map<std::string, std::shared_ptr<Feature>> const& featureMap)
  {
    for (auto const& it : featureMap) {
      localFeatureMap[it.first] = it.second;
    }
  }

  double evaluate(
    std::function<double(std::list<std::shared_ptr<Feature>>)> func) const
  {
    std::list<std::shared_ptr<Feature>> mylist;
    for (auto const& k : localFeatureMap) {
      auto feature = k.second;
      mylist.push_back(feature);
    }
    double result = func(mylist);
    return result;
  }

  /**
   * This takes the feature passed as a parameter, grabs the items in that
   * map, and updates this feature's localFeatureMap with those items.
   */
  void update(Feature const& feature) {
    // Cast it to be the feature type we expect.
    auto otherFeatureMap = 
      static_cast<MapFeature const&>(feature).localFeatureMap;

    // We iterate over the items in the other map.  Generally this should
    // only be one item. 
    for(auto& it : otherFeatureMap)
    {
      localFeatureMap[it.first] = it.second;
    }
  }

  /**
   *
   */
  std::shared_ptr<Feature> createCopy() const {
    std::shared_ptr<Feature> feature = 
      std::make_shared<MapFeature>(localFeatureMap);
    return feature;
  }

  std::string toString() const {
    std::string rString = "MapFeature "; 
    return rString;
  }

  /**
   * This is expensive and not thread safe.  Is it called?
   */
  bool operator==(Feature const& other) const
  {
    // Cast it to be the feature type we expect.
    auto otherFeatureMap = 
      static_cast<MapFeature const&>(other).localFeatureMap;

    if (otherFeatureMap.size() != localFeatureMap.size()) {
      return false;
    }

    for (auto& it : otherFeatureMap) {
      if (localFeatureMap.count(it.first) > 0) {
        if (it.second != localFeatureMap.at(it.first)) {
          return false;
        }
      }
      else {
        return false;
      }
    }

    return true;
  }

};

/**
 * A boolean feature.  
 */
class BooleanFeature: public Feature {
public:
  BooleanFeature(bool value) : Feature(static_cast<double>(value)) {}

  void update(Feature const& feature) {
    value = static_cast<BooleanFeature const&>(feature).value;
  }

  std::shared_ptr<Feature> createCopy() const {
    std::shared_ptr<Feature> copy(new BooleanFeature(value));
    return copy;
  }

  bool operator==(Feature const& other) const {
    if (BooleanFeature const* f = dynamic_cast<BooleanFeature const*>(&other)) 
    {
      if (f->value == value)
        return true;
    }
    return false;
  }

  std::string toString() const {
    std::string rString = "BooleanFeature " + 
      boost::lexical_cast<std::string>(value);
    return rString;
  }
};


/**
 * This represents features that are a single value.
 * Examples include sum, variance.
 */
class SingleFeature: public Feature
{
public:
  SingleFeature(double value): Feature(value) {}

  void update(Feature const& feature) {
    value = static_cast<SingleFeature const&>(feature).value;
  }

  std::shared_ptr<Feature> createCopy() const {
    std::shared_ptr<Feature> copy(new SingleFeature(value));
    return copy;
  }

  bool operator==(Feature const& other) const {
    if (SingleFeature const* f = dynamic_cast<SingleFeature const*>(&other)) 
    {
      if (f->value == value)
        return true;
    }
    return false;
  }

  std::string toString() const {
    std::string rString = "SingleFeature " + 
      boost::lexical_cast<std::string>(value);
    return rString;
  }

};

/**
 * This encapsulates a feature that is created after analyzing
 * a netflow with TopK.  It has two vectors, a list of keys and a list of 
 * frequencies.  
 *
 */
class TopKFeature: public Feature
{
private:
  std::vector<std::string> keys;
  std::vector<double> frequencies;

public:
  TopKFeature(std::vector<std::string> keys,
              std::vector<double> frequencies)
  {
    this->keys   = keys;
    this->frequencies = frequencies;
  }

  std::vector<double> const& getFrequencies() const {
    return frequencies;
  }

  void update(Feature const& feature) {
    keys = static_cast<TopKFeature const&>(feature).keys;
    frequencies = static_cast<TopKFeature const&>(feature).frequencies;
  }


  std::shared_ptr<Feature> createCopy() const {
    std::shared_ptr<Feature> copy(new TopKFeature(keys, frequencies));
    return copy;
  }

  bool operator==(Feature const& other) const {
    if (TopKFeature const* f = dynamic_cast<TopKFeature const*>(&other)) 
    {
      if (f->keys.size() != keys.size()) {
        return false;
      }
      for (int i = 0; i < keys.size(); i++) {
        if (f->keys[i].compare(keys[i]) != 0 )
        {
          return false;
        }
      }
      if (f->frequencies.size() != frequencies.size()) {
        return false;
      }
      for (int i = 0; i < frequencies.size(); i++) {
        if (f->frequencies[i] != frequencies[i] )
        {
          return false;
        }
      }

    }
    return true;
  }

  std::string toString() const {
    std::string rString = "TopKFeature";
    return rString;
  }
};



}

#endif
