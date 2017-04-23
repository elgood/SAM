#ifndef TOKENS_HPP
#define TOKENS_HPP

#include <boost/variant/recursive_variant.hpp>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <vector>

#include "FeatureMap.hpp"

namespace sam {

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
  os << "Identifier " << f.identifier << " Function: " << f.function;
  return os;
}

/**
 * Used for the prev token in transform expressions.
 */
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
  os << "Identifier " << p.field << " Index: " << p.index;
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


namespace sam {

// Base class representing a token in an expression.
class ExpressionToken {
public:
  /**
   * Returns a string representation of the token.  Mostly for debugging.
   */
  virtual std::string toString() const = 0; 

  /**
   * Returns true if the token represents an operator.  Used to evaluate
   * the expression.
   */
  virtual bool isOperator() const = 0;

  /**
   * Used when the token needs access to an imux featuremap to discover
   * the value of the token (e.g. FunctionToken)
   */ 
  virtual double evaluate(std::string const& key,
                          FeatureMap const& featureMap) const = 0;
  
 
  /**
   * Used by operators that take two values to evaluate.
   */ 
  virtual double evaluate(double d1, double d2) const = 0;
};

std::ostream& operator<< (std::ostream& os, const ExpressionToken& f) 
{
  os << f.toString();
  return os;
}

/** 
 * A token in a transform expression that looks like 
 * prev(1).FieldName
 */
class PreviousToken : public ExpressionToken
{
public:
  PreviousToken(int _index, std::string _identifier)
    : index(_index), identifier(_identifier) {}
  
  bool isOperator() const { return false; }
  
  double evaluate(std::string const& key, FeatureMap const& featureMap) const
  {
    return 0; 
  }

  double evaluate(double d1, double d2) const {
    return 0;
  }
   
  std::string toString() const;
private:
  int index;
  std::string identifier;
};

std::string PreviousToken::toString() const 
{
  return "Previous Token";
}


/**
 * A function token looks like 
 *  top2.value(1)
 * It has an identifier (variable name), a function name, and a parameter list
 */
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
  double evaluate(std::string const& key,
                  FeatureMap const & featureMap) const;
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

double FunctionToken::evaluate(std::string const& key,
                               FeatureMap const& featureMap) const
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
  double evaluate(std::string const& key,
                  FeatureMap const& featureMap) const;
  double evaluate(double d1, double d2) const;
private:
  double d;
};

double NumberToken::evaluate(double d1, double d2) const
{
  throw std::runtime_error("evaluate(d1, d2) not defined for NumberToken");
}

double NumberToken::evaluate(std::string const& key,
                             FeatureMap const& featureMap) const
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
  double evaluate(std::string const& key,
                  FeatureMap  const& featureMap) const;
  double evaluate(double d1, double d2) const;
private:
  std::string identifier;
};

double IdentifierToken::evaluate(std::string const& key,
                                 FeatureMap const& featureMap) const
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
  double evaluate(std::string const& key, FeatureMap const& featureMap) const;
public:
  OperatorToken(int associativity, int precedence) {
    this->associativity = associativity;
    this->precedence = precedence;
  }
};

double OperatorToken::evaluate(std::string const& key,
                               FeatureMap const& featureMap) const
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


/**
 * We make a translation from OutputItems to ExpressionTokens.  There is 
 * probably a better way to do this.  OutputItems is a boost::variant, because
 * I know how to use boost::spirit to populate boost::variant, and
 * we translate that into a class hierarchy, probably because I know how to 
 * deal with that better for all other situations.
 */  
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

#endif
