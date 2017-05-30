#ifndef TOKENS_HPP
#define TOKENS_HPP

#include <boost/variant/recursive_variant.hpp>
#include <boost/lexical_cast.hpp>
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
  //std::string function; ///> The name of the function e.g. value
  //std::vector<double> parameters; ///> The parameters to the function
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
  std::string identifier;
public:
 
  PrevToken(FeatureMap &featureMap) : 
    ExpressionToken<std::tuple<Ts...>>(featureMap) 
  {
    identifier = createPreviousIdentifierString();
  }

  bool evaluate(std::stack<double> & mystack, 
                  std::string const& key,
                  std::tuple<Ts...> const& input) 
  {
    double currentData = boost::lexical_cast<double>(std::get<field>(input));
    std::shared_ptr<Feature> feature 
      = std::make_shared<SingleFeature>(currentData);

    bool exists = false;

    if (this->featureMap.exists(key, identifier)) { 
      exists = true;
      auto feature = this->featureMap.at(key, identifier);
      
      auto valueFunc = [](Feature const * feature)->double { 
        return feature->getValue(); 
      };

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
  std::string createPreviousIdentifierString()
  {
    return "previous_" + boost::lexical_cast<std::string>(field);
  }
};


}

//namespace sam {
/*
// Couldn't figure out a way to directly populate this vector, so using a 
// a struct to wrap it.  In short, I've figure out a way to populate structs
// using Spirit, so using that pattern for just about everything.  There's
// probably a better way.
struct ParameterStructure {
  std::vector<double> parameters;
};

// Another stucture to encapsulate a function call.  For example,
//  top2.value(1)
// is an example of a function call.  It has an identifier, top2, a
// function call, value, and a paramter list, 1.
struct FunctionStructure {
  std::string identifier;
  std::string function;
  ParameterStructure parameters;
};

// This is mostly for debugging.  Allows you to stream the boost::variant
// OutputItem, defined below, to cout.  The FunctionStructure is one of the
// elements of the variant, so it needs operator << defined to be able to 
// use that in debug cout statements. 
std::ostream& operator<< (std::ostream& os, const FunctionStructure& f)
{
  os << "FunctionStructure: Identifier: " << f.identifier << ", Function: " 
     << f.function;
  return os;
}
*/
/**
 * Used for the prev token in transform expressions.
 */
/* 
struct PreviousStructure {
  int index;
  std::string field;
};

// This is mostly for debugging.  Allows you to stream the boost::variant
// OutputItem, defined below, to cout.  The PreviousStructure is one of the
// elements of the variant, so it needs operator << defined to be able to 
// use that in debug cout statements. 
std::ostream& operator<< (std::ostream& os, const PreviousStructure& p)
{
  os << "Previous: Field " << p.field << " Index: " << p.index;
  return os;
}

// Convenience typedef.  We combine the possible token types into one type
// using boost::variant.
typedef
  boost::variant<
    std::string,
    FunctionStructure,
    PreviousStructure,
    double>
OutputItem;


// Now how to populate a struct using Spirit, so wrapping the vector of tokens
// in a struct.  Again, probably a better way to do this so we could use the
// vector directly.
struct ParseStructure {
  std::vector<OutputItem> tokens;
};

}

// Spirit uses boost::fusion.  It allows us to access and populate elements 
// using the at_c<index> syntax.
BOOST_FUSION_ADAPT_STRUCT(
  sam::ParameterStructure,
  (std::vector<double>, parameters)
)


// Again, registering the function structure with boost::fusion so we can use
// the at_c<index> syntax in the grammar we define.
BOOST_FUSION_ADAPT_STRUCT(
  sam::FunctionStructure,
  (std::string, identifier)
  (std::string, function)
  (sam::ParameterStructure, parameters)
)


// Registering ParseStructure with boost::fusion.
BOOST_FUSION_ADAPT_STRUCT(
  sam::ParseStructure,
  (std::vector<sam::OutputItem>, tokens)
)

BOOST_FUSION_ADAPT_STRUCT(
  sam::PreviousStructure,
  (int, index)
  (std::string, field)
)
*/

//namespace sam {

// Base class representing a token in an expression.
//class ExpressionToken {
//public:
  /**
   * Returns a string representation of the token.  Mostly for debugging.
   */
//  virtual std::string toString() const = 0; 

  /**
   * Returns true if the token represents an operator.  Used to evaluate
   * the expression.
   */
//  virtual bool isOperator() const = 0;

  /**
   * Used when the token needs access to an imux featuremap to discover
   * the value of the token (e.g. FunctionToken)
   */
  //template <typename InputType> 
  //double evaluate(std::string const& key,
  //                        FeatureMap const& featureMap,
  //                        InputType const& input) const = 0;
  
 
  /**
   * Used by operators that take two values to evaluate.
   */ 
//  virtual double evaluate(double d1, double d2) const = 0;
//};

//std::ostream& operator<< (std::ostream& os, const ExpressionToken& f) 
//{
//  os << f.toString();
//  return os;
//}


//inline std::string generateHistoryIdentifier(int i)
//{
//  if (i < 1) {
//    throw std::runtime_error("Integer must be positive");
//  }
//  return "history" + boost::lexical_cast<std::string>(i);
//}

/** 
 * A token in a transform expression that looks like 
 * prev(1).FieldName
 */
/* 
class PreviousToken : public ExpressionToken
{
public:
  PreviousToken(int _index, std::string _fieldName)
    : index(_index), fieldName(_fieldName) {}
  
  bool isOperator() const { return false; }
  
  template <typename InputType>
  double evaluate(std::string const& key, 
                  FeatureMap const& featureMap,
                  InputType const& input) const
  {
    // The identifier we need is the history index combined with the field name
    std::string featureName = generateHistoryIdentifier(index) + fieldName;
     
    std::shared_ptr<Feature const> feature = featureMap.at(key, featureName);

    return feature->evaluate(); 
  }

  double evaluate(double d1, double d2) const {
    throw std::runtime_error("evaluate(double,double) not defined for "
                             "PreviousToken"); 
  }

  int getIndex() const { return index; }
  std::string getFieldName() const { return fieldName; }
   
  std::string toString() const;
private:
  /// How far in the past.  e.g. index=1 means the previous item.  index=2
  /// means two items previous.
  int index; 

  /// The field that we are grabbing, e.g. for netflows, TimeSeconds
  std::string fieldName;
};

std::string PreviousToken::toString() const 
{
  return "Previous Token";
}
*/

/**
 * A function token looks like 
 *  top2.value(1)
 * It has an identifier (variable name), a function name, and a parameter list
 */
/* 
class FunctionToken : public ExpressionToken
{
public:
  FunctionToken(std::string _identifier,
                std::string _function, 
                std::vector<double> _parameters) :
    identifier(_identifier), function(_function), parameters(_parameters) {}

  std::string getIdentifier() const { return identifier; }
  std::string getFunction() const { return function; }
  size_t getNumParameters() const { return parameters.size(); }
  double getParameter(int i) const { return parameters[i]; }
  std::string toString() const;
  bool isOperator() const { return false; }

  template <typename InputType>
  double evaluate(std::string const& key,
                  FeatureMap const & featureMap,
                  InputType const& input) const;
  double evaluate(double d1, double d2) const;

private:
  std::string identifier;
  std::string function;
  std::vector<double> parameters;
};

std::string FunctionToken::toString() const
{
  std::string str = "FunctionToken: " + getIdentifier() + "." +
                     getFunction();
  for (auto parameter : parameters) {
    str = str + " parameter " + boost::lexical_cast<std::string>(parameter); 
  }
  return str;
}

template <typename InputType>
double FunctionToken::evaluate(std::string const& key,
                               FeatureMap const& featureMap,
                               InputType const& input) const
{
  std::shared_ptr<Feature const> feature = featureMap.at(key, identifier);
  return feature->evaluate(function, parameters); 
}

double FunctionToken::evaluate(double d1, double d2) const
{
  throw std::runtime_error("evaluate(d1, d2) not defined for ExpressionToken");
}


class NumberToken : public ExpressionToken
{
public:
  NumberToken(double _d) : d(_d) {}
  double getValue() const { return d; }
  std::string toString() const { return boost::lexical_cast<std::string>(d); }
  bool isOperator() const { return false; }
  template <typename InputType>
  double evaluate(std::string const& key,
                  FeatureMap const& featureMap,
                  InputType const& input) const;
  double evaluate(double d1, double d2) const;
private:
  double d;
};

double NumberToken::evaluate(double d1, double d2) const
{
  throw std::runtime_error("evaluate(d1, d2) not defined for NumberToken");
}

template <typename InputType>
double NumberToken::evaluate(std::string const& key,
                             FeatureMap const& featureMap,
                             InputType const& input) const
{
  return d;
}

class IdentifierToken : public ExpressionToken
{
public:
  IdentifierToken(std::string str) : identifier(str) {}
  std::string getValue() const { return identifier; }
  std::string toString() const { return identifier; }
  bool isOperator() const { return false; }
  template <typename InputType>
  double evaluate(std::string const& key,
                  FeatureMap  const& featureMap,
                  InputType const& input) const;
  double evaluate(double d1, double d2) const;
private:
  std::string identifier;
};

template <typename InputType>
double IdentifierToken::evaluate(std::string const& key,
                                 FeatureMap const& featureMap,
                                 InputType const& input) const
{
  std::shared_ptr<Feature const> feature = featureMap.at(key, identifier);
  return feature->evaluate();
}

double IdentifierToken::evaluate(double d1, double d2) const
{
  throw std::runtime_error("evaluate(d1, d2) not defined for IdentifierToken");
}

class OperatorToken : public ExpressionToken 
{
private:
  int precedence;
  int associativity;
public:
  const static int RIGHT_ASSOCIATIVE = 0;
  const static int LEFT_ASSOCIATIVE = 1;  
  virtual std::string toString() const { return "OperatorToken"; }
  bool isOperator() const { return true; }
  bool isLeftAssociative() const { return associativity == LEFT_ASSOCIATIVE; }
  bool isRightAssociative() const { return associativity == RIGHT_ASSOCIATIVE; }
  int getPrecedence() const { return precedence; }
  template <typename InputType>
  double evaluate(std::string const& key, 
                  FeatureMap const& featureMap,
                  InputType const& input) const;
public:
  OperatorToken(int associativity, int precedence) {
    this->associativity = associativity;
    this->precedence = precedence;
  }
};

template <typename InputType>
double OperatorToken::evaluate(std::string const& key,
                               FeatureMap const& featureMap,
                               InputType const& input) const
{
  throw std::runtime_error("evaluate(string, FeatureMap) not defined for"
                           " NumberToken");
}


class PlusToken : public OperatorToken 
{
public:
  PlusToken() : OperatorToken(2, LEFT_ASSOCIATIVE) {}   
  double evaluate(double d1, double d2) const { 
    return d1 + d2; 
  }
  std::string toString() const { return "Plus"; }
};

class MinusToken : public OperatorToken 
{
public:
  MinusToken() : OperatorToken(2, LEFT_ASSOCIATIVE) {}   
  double evaluate(double d1, double d2) const { return d1 - d2; }
};

class MultToken : public OperatorToken 
{
public:
  MultToken() : OperatorToken(3, LEFT_ASSOCIATIVE) {}  
  double evaluate(double d1, double d2) const { return d1 * d2; }
};

class DivideToken : public OperatorToken 
{
public:
  DivideToken() : OperatorToken(3, LEFT_ASSOCIATIVE) {}  
  double evaluate(double d1, double d2) const { return d1 / d2; }
};

class PowerToken : public OperatorToken 
{
public:
  PowerToken() : OperatorToken(4, RIGHT_ASSOCIATIVE) {}
  double evaluate(double d1, double d2) const { return pow( d1 , d2); }
};

class LessToken : public OperatorToken 
{
public:
  LessToken() : OperatorToken(1, LEFT_ASSOCIATIVE) {}  
  double evaluate(double d1, double d2) const { return d1 < d2; }
};

class GreaterToken : public OperatorToken 
{
public:
  GreaterToken() : OperatorToken(1, LEFT_ASSOCIATIVE) {}  
  double evaluate(double d1, double d2) const { 
    return d1 > d2; 
  }
  std::string toString() const { return "Greater"; }
};

class LessOrEqualToken : public OperatorToken 
{
public:
  LessOrEqualToken() : OperatorToken(1, LEFT_ASSOCIATIVE) {}  
  double evaluate(double d1, double d2) const { return d1 <= d2; }
};

class GreaterOrEqualToken : public OperatorToken 
{
public:
  GreaterOrEqualToken() : OperatorToken(1, LEFT_ASSOCIATIVE) {}
  double evaluate(double d1, double d2) const { return d1 >= d2; }
};
*/

/**
 * We make a translation from OutputItems to ExpressionTokens.  There is 
 * probably a better way to do this.  OutputItems is a boost::variant, because
 * I know how to use boost::spirit to populate boost::variant, and
 * we translate that into a class hierarchy, probably because I know how to 
 * deal with that better for all other situations.
 */
/*  
class TokenVisitor : public boost::static_visitor<>
{
public:

  TokenVisitor(std::vector<std::shared_ptr<ExpressionToken>> & _tokens) : 
    tokens(_tokens) {}

  void operator()(std::string & str) const
  {
    if (str.compare("+") == 0) {
      tokens.push_back(std::shared_ptr<ExpressionToken>(new PlusToken()));    
    } else if (str.compare("-") == 0) {
      tokens.push_back(std::shared_ptr<ExpressionToken>(new MinusToken()));
    } else if (str.compare("*") == 0) {
      tokens.push_back(std::shared_ptr<ExpressionToken>(new MultToken()));
    } else if (str.compare("/") == 0) {
      tokens.push_back(std::shared_ptr<ExpressionToken>(new DivideToken()));
    } else if (str.compare("^") == 0) {
      tokens.push_back(std::shared_ptr<ExpressionToken>(new PowerToken()));
    } else if (str.compare("<") == 0) {
      tokens.push_back(std::shared_ptr<ExpressionToken>(new LessToken()));
    } else if (str.compare(">") == 0) {
      tokens.push_back(std::shared_ptr<ExpressionToken>(new GreaterToken()));
    } else if (str.compare("<=") == 0) {
      tokens.push_back(std::shared_ptr<ExpressionToken>(new LessOrEqualToken()));
    } else if (str.compare(">=") == 0) {
      tokens.push_back(std::shared_ptr<ExpressionToken>(
        new GreaterOrEqualToken()));
    } else {
      tokens.push_back(std::shared_ptr<ExpressionToken>(
        new IdentifierToken(str)));
    } 
  }

  void operator()(FunctionStructure & f) const
  {
    tokens.push_back(std::shared_ptr<ExpressionToken>(new 
                    FunctionToken(f.identifier, f.function, 
                                  f.parameters.parameters)));
  }

  void operator()(double & d) const
  {
    tokens.push_back(std::shared_ptr<ExpressionToken>(new NumberToken(d)));
  }

  void operator()(PreviousStructure & p) const
  {
    tokens.push_back(std::shared_ptr<ExpressionToken>(new
                      PreviousToken(p.index, p.field)));
  }

private:
  std::vector<std::shared_ptr<ExpressionToken>> & tokens;
};

}
*/
#endif
