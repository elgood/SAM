#ifndef TOKENS_HPP
#define TOKENS_HPP

#include <boost/variant/recursive_variant.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <iostream>
#include <vector>
#include <stack>

#include "FeatureMap.hpp"

namespace sam {

template <typename... Ts>
class ExpressionToken
{};

template <typename... Ts>
class ExpressionToken<std::tuple<Ts...>>
{
protected:
  // When tokens are created, they are always created in the context of a 
  // feature map.  Thus, while not all tokens need a feature map to evaluate
  // them, all tokens have a reference to a feature map.
  FeatureMap &featureMap;
public:

  /**
   * Constructor for the base class.  
   * \param _featureMap The feature map that some tokens need to evaluate.
   */
  ExpressionToken(FeatureMap& _featureMap) : featureMap(_featureMap) {}
  
  /**
   * Returns a string representation.  Mostly for debugging.  Should be 
   * overriden by child classes.
   */
  virtual std::string toString() const { return "ExpressionToken";}

  /**
   * Evaluates the token.  Changes are made to the stack to reflect the
   * change from evaluating the token.
   * \param mystack The stack used in evaluating the postfix expression.
   * \param key The key that is used to find relevant entries in the feature
   *  map.
   * \return Returns true if the token evaluating correctly.  False otherwise.
   *  For example, sometimes data is unavailable (e.g. for PrevToken) so the
   *  token can't be evaluating this iteration.
   */
  virtual bool evaluate(std::stack<double> & mystack,
                        std::string const& key,
                        std::tuple<Ts...> const& input) 
  { return false; }

  /**
   * Returns true if the token is an operator.  False otherwise
   */
  virtual bool isOperator() const { return false; }

};

template <typename... Ts>
class NumberToken : public ExpressionToken<Ts...>
{};

/**
 * Token representing a simple number like 1 or 6.5
 */
template <typename... Ts>
class NumberToken<std::tuple<Ts...>> : 
  public ExpressionToken<std::tuple<Ts...>>
{
private:
  double number;
public:
  NumberToken(FeatureMap& featureMap, double d) : 
    ExpressionToken<std::tuple<Ts...>>(featureMap), number(d) {}
 
  std::string toString() const {
    return "NumberToken: " + boost::lexical_cast<std::string>(number);
  }

  /**
   * Simply pushes the number to the top of the stack.
   */
  bool evaluate(std::stack<double> & mystack,
                std::string const& key,
                std::tuple<Ts...> const& input) 
  {
    mystack.push(number);
    return true;
  }

  bool isOperator() const { return false; }
};

template <typename... Ts> 
class OperatorToken : public ExpressionToken<Ts...>
{};

template <typename... Ts>
class OperatorToken<std::tuple<Ts...>> : 
  public ExpressionToken<std::tuple<Ts...>>
{
private:
  int precedence;
  int associativity;
public:
  const static int RIGHT_ASSOCIATIVE = 0;
  const static int LEFT_ASSOCIATIVE = 1; 
  OperatorToken(FeatureMap& featureMap,
                int associativity,
                int precedence) : 
                ExpressionToken<std::tuple<Ts...>>(featureMap) 
  {
    this->associativity = associativity;
    this->precedence = precedence;
  }
  
  bool isOperator() const { return true; }
  bool isLeftAssociative() const { return associativity == LEFT_ASSOCIATIVE; }
  bool isRightAssociative() const { return associativity == RIGHT_ASSOCIATIVE; }
  int getPrecedence() const { return precedence; }

};

template <typename... Ts>
class AddOperator : public OperatorToken<Ts...>
{};

template <typename... Ts>
class AddOperator<std::tuple<Ts...>> : 
  public OperatorToken<std::tuple<Ts...>>
{
public:
  AddOperator(FeatureMap& featureMap) : 
    OperatorToken<std::tuple<Ts...>>(featureMap, 2, this->LEFT_ASSOCIATIVE) {}

  std::string toString() const {
    return "AddOperator";
  }

  bool evaluate(std::stack<double> & mystack,
                std::string const& key,
                std::tuple<Ts...> const& input)
  {
    if (mystack.size() >= 2) {
      double o2 = mystack.top();
      mystack.pop();
      double o1 = mystack.top();
      mystack.pop();
      double result = o1 + o2;
      mystack.push(result);
      return true;
    }
    return false;
  }
};

template <typename... Ts>
class SubOperator : public OperatorToken<Ts...>
{};

template <typename... Ts>
class SubOperator<std::tuple<Ts...>> : 
  public OperatorToken<std::tuple<Ts...>>
{
public:
  SubOperator(FeatureMap& featureMap) : 
    OperatorToken<std::tuple<Ts...>>(featureMap, 2, this->LEFT_ASSOCIATIVE) {}

  std::string toString() const {
    return "SubOperator";
  }

  bool evaluate(std::stack<double> & mystack,
                std::string const& key,
                std::tuple<Ts...> const& input)
  {
    if (mystack.size() >= 2) {
      double o2 = mystack.top();
      mystack.pop();
      double o1 = mystack.top();
      mystack.pop();
      double result = o1 - o2;
      mystack.push(result);
      return true;
    } 
    return false;
  }
};

template <typename... Ts>
class MultOperator : public OperatorToken<Ts...>
{};

template <typename... Ts>
class MultOperator<std::tuple<Ts...>> : 
  public OperatorToken<std::tuple<Ts...>>
{
public:
  MultOperator(FeatureMap& featureMap) : 
    OperatorToken<std::tuple<Ts...>>(featureMap, 3, this->LEFT_ASSOCIATIVE) {}

  std::string toString() const {
    return "MultOperator";
  }

  bool evaluate(std::stack<double> & mystack,
                std::string const& key,
                std::tuple<Ts...> const& input)
  {
    if (mystack.size() >= 2) {
      double o2 = mystack.top();
      mystack.pop();
      double o1 = mystack.top();
      mystack.pop();
      double result = o1 * o2;
      mystack.push(result);
      return true;
    }
    return false;
  }
};

template <typename... Ts>
class LessThanOperator : public OperatorToken<Ts...>
{};

template <typename... Ts>
class LessThanOperator<std::tuple<Ts...>> :
  public OperatorToken<std::tuple<Ts...>>
{
public:
  LessThanOperator(FeatureMap& featureMap) : 
    OperatorToken<std::tuple<Ts...>>(featureMap, 1, this->LEFT_ASSOCIATIVE) {}

  std::string toString() const {
    return "LessThanOperator";
  }

  bool evaluate(std::stack<double> & mystack,
                std::string const& key,
                std::tuple<Ts...> const& input)
  {
    if (mystack.size() >= 2) {
      double o2 = mystack.top();
      mystack.pop();
      double o1 = mystack.top();
      mystack.pop();
      double result = o1 < o2;
      mystack.push(result);
      return true;
    }
    return false;
  }

};



template <size_t field, typename... Ts>
class FieldToken : public ExpressionToken<Ts...>
{
};


/**
 * Represents one field of an input
 */
template <size_t field, typename... Ts>
class FieldToken<field, std::tuple<Ts...>> : 
  public ExpressionToken<std::tuple<Ts...>>
{
private:
  std::string identifier;
public:
  FieldToken(FeatureMap &featureMap) : 
    ExpressionToken<std::tuple<Ts...>>(featureMap) {}

  bool evaluate(std::stack<double> & mystack,
                std::string const& key,
                std::tuple<Ts...> const& input)
  {
    try {
      //double data = boost::lexical_cast<double>(std::get<field>(input));
      double d = std::get<0>(input);
      auto data = std::get<field>(input);
      mystack.push(data);
      return true;
    } catch (std::exception e) {
      std::cout << e.what() << std::endl;
      return false;
    }
  }

  bool isOperator() const { return false; }
};

template <typename... Ts>
class FuncToken : public ExpressionToken<Ts...>
{};

/**
 * Represents tokens of the form identifier.function(parameters), e.g.
 * top2.value(1)
 */
template <typename... Ts>
class FuncToken<std::tuple<Ts...>> : 
  public ExpressionToken<std::tuple<Ts...>>
{
private:
  std::string identifier; ///> The name of the variable, e.g. top2
  std::function<double(Feature const *)> function;
public:
  FuncToken(FeatureMap &featureMap,
            std::function<double(Feature const *)> function,
            std::string identifier)
            :
            //std::string function,
            //std::vector<double> parameters) : 
            ExpressionToken<std::tuple<Ts...>>(featureMap) 
  {
    this->identifier = identifier;
    this->function = function;
    //this->parameters = parameters;  
  }

  bool evaluate(std::stack<double> & mystack,
                std::string const& key,
                std::tuple<Ts...> const& input)
  {
    if (this->featureMap.exists(key, identifier)) {
      try {
        double d = this->featureMap.at(key, identifier)->evaluate(function);
        mystack.push(d);
      } catch (std::exception e) {
        printf("Caught exception %s\n", e.what());
      }
      return true;
    }
    return false;
  }

  bool isOperator() const { return false; }
};

template <size_t field, typename... Ts>
class PrevToken : public ExpressionToken<Ts...> 
{};


template <size_t field, typename... Ts>
class PrevToken<field, std::tuple<Ts...>> : 
  public ExpressionToken<std::tuple<Ts...>> 
{
private:
  // The identifier used to uniquely identify features produced by this
  // ExpressionToken.
  std::string identifier;
public:
 
  PrevToken(FeatureMap &featureMap) : 
    ExpressionToken<std::tuple<Ts...>>(featureMap) 
  {
    identifier = createPreviousIdentifierString();
  }

  std::string getIdentifier() {
    return identifier;
  }

  bool evaluate(std::stack<double> & mystack, 
                  std::string const& key,
                  std::tuple<Ts...> const& input) 
  {
    // Get the current value of the field.
    double currentData = boost::lexical_cast<double>(std::get<field>(input));
    
    // Create a feature out of the current data
    std::shared_ptr<Feature> feature 
      = std::make_shared<SingleFeature>(currentData);

    // Check to see if the feature has been added before
    bool exists = false;
    if (this->featureMap.exists(key, identifier)) { 

      // If the feature exists, we can return previous value
      exists = true;
      auto feature = this->featureMap.at(key, identifier);
      
      // Getting the value of the feature through this function
      auto valueFunc = [](Feature const * feature)->double { 
        return feature->getValue(); 
      };

      // Pushing back the previous value onto the stack.
      double result = feature->evaluate(valueFunc); 
      mystack.push(result);
    } 

    // Inserting the current data to become the past data
    feature = std::make_shared<SingleFeature>(currentData);
    this->featureMap.updateInsert(key, identifier, *feature);

    return exists;
     
  }

  bool isOperator() const { return false; }
private:

  /**
   * We need to create a unique identifier for created features so that we
   * can insert them into the featureMap.  This, along with the key generated
   * from the input, should be a unique combination.
   */
  std::string createPreviousIdentifierString()
  {
    boost::uuids::uuid a = boost::uuids::random_generator()();
    return "previous_" + boost::lexical_cast<std::string>(field) + "_" +
            boost::uuids::to_string(a);
  }
};


}

#endif
