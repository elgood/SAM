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

  //virtual double evaluate(std::string const& functionName,
  //                        std::vector<double> const& parameters) const = 0;
  //virtual double evaluate() const  = 0;
  double evaluate(std::function<double(Feature const *)> func) const  {
    return func(this);
  }
  virtual void update(Feature const& feature) = 0;
  
  /**
   * Creates a deep copy (I think) of this feature and returns it.
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
/* 
class MapFeature {
private:
  std::map<std::shared_ptr<Feature>> localFeatureMap;
public:
  double evaluate(std::string const& functionName,
                  std::vector<double> const& parameters)
  {
    throw std::runtime_error("Evaluate with function " + functionName + 
      " is not defined for class MapFeature");
  }

  void update(Feature const& feature) {
    auto otherFeatureMap = 
      static_cast<MapFeature const&>(feature).localFeatureMap;
    for(auto& it : sourceMap)
    {
      localFeatureMap[it.first] = it.second;
    }
  }

};
*/
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
