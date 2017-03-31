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
  
};


}

#endif
