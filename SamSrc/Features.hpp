#ifndef FEATURES_HPP
#define FEATURES_HPP

#include <exception>
#include <boost/lexical_cast.hpp>

#define VALUE_FUNCTION "value"

namespace sam {

class Feature {
public:
  virtual double evaluate(std::string const& functionName,
                          std::vector<double> const& parameters) const = 0;
  virtual double evaluate() const  = 0;
  virtual void update(Feature const& feature) = 0;
  virtual std::shared_ptr<Feature> createCopy() const = 0;
  virtual bool operator==(Feature const& other) const = 0;

  bool operator!=(Feature const& other) const {
    return !(*this == other);
  }

  virtual std::string toString() const = 0;
};


/**
 * A boolean feature.  
 */
class BooleanFeature: public Feature {
private:
  bool value;

public:
  BooleanFeature(bool value) {
    this->value = value;
  }

  double evaluate(std::string const& functionName,
                std::vector<double> const& parameters) const 
  {
    if (functionName.compare(VALUE_FUNCTION) == 0) {
      return value;
    }
    throw std::runtime_error("Evaluate with function " + functionName + 
      " is not defined for class BooleanFeature");
  }

  double evaluate() const {
    return value;
  }

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
private:
  double value;

public:
  SingleFeature(double _value): value(_value) {}

  double evaluate(std::string const& functionName,
                  std::vector<double> const& parameters) const 
  {
    if (functionName.compare(VALUE_FUNCTION) == 0) {
      return value;
    }
    throw std::runtime_error("Evaluate with function " + functionName + 
      " is not defined for class SingleFeature");
  }

  double evaluate() const {
    return value;
  }

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

  double evaluate(std::string const& functionName, 
                  std::vector<double> const& parameters) const 
  {
    if (functionName.compare(VALUE_FUNCTION) == 0) {
      if (parameters.size() != 1) {
        throw std::runtime_error("Expected there to be one parameter, found " +
                      boost::lexical_cast<std::string>(parameters.size())); 
      }
      int index = boost::lexical_cast<int>(parameters[0]);
      return frequencies[index];
    }
    throw std::runtime_error("Evaluate with function " + functionName + 
      " is not defined for class TopKFeature");
  }

  double evaluate() const {
    throw std::runtime_error("Evaluate with no parameters is not defined for" 
                             " class TopKFeature");
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
