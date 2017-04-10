#ifndef FILTER_TOKENIZER_HPP
#define FILTER_TOKENIZER_HPP

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/qi_parse.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_fusion.hpp>
#include <boost/spirit/include/phoenix_stl.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/lexical_cast.hpp>
#include <vector>
#include <iostream>

#include "FeatureMap.hpp"

// I think this is somehow even though we don't reference it.
namespace fusion = boost::fusion;

namespace sam {

// Couldn't figure out a way to directly populate this vector, so using a 
// a struct to wrap it.  In short, I've figure out a way to populate structs
// using Spirit, so using that pattern for just about everything.  There's
// probably a better way.
struct parameter_structure {
  std::vector<double> parameters;
};

// Another stucture to encapsulate a function call.  For example,
//  top2.value(1)
// is an example of a function call.  It has an identifier, top2, a
// function call, value, and a paramter list, 1.
struct function_structure {
  std::string identifier;
  std::string function;
  parameter_structure parameters;
};

// This is mostly for debugging.  Allows you to stream the boost::variant
// OutputItem, defined below, to cout.  The function_structure is one of the
// elements of the variant, so it needs operator << defined to be able to 
// use that in debug cout statements. 
std::ostream& operator<< (std::ostream& os, const function_structure& f)
{
  os << "Identifier " << f.identifier << " Function: " << f.function;
  return os;
}

// Convenience typedef.  We combine the possible token types into one type
// using boost::variant.
typedef
  boost::variant<
    std::string,
    function_structure,
    double>
OutputItem;


// Now how to populate a struct using Spirit, so wrapping the vector of tokens
// in a struct.  Again, probably a better way to do this so we could use the
// vector directly.
struct parse_structure {
  std::vector<OutputItem> tokens;
};

}

// Spirit uses boost::fusion.  It allows us to access and populate elements 
// using the at_c<index> syntax.
BOOST_FUSION_ADAPT_STRUCT(
  sam::parameter_structure,
  (std::vector<double>, parameters)
)


// Again, registering the function structure with boost::fusion so we can use
// the at_c<index> syntax in the grammar we define.
BOOST_FUSION_ADAPT_STRUCT(
  sam::function_structure,
  (std::string, identifier)
  (std::string, function)
  (sam::parameter_structure, parameters)
)


// Registering parse_structure with boost::fusion.
BOOST_FUSION_ADAPT_STRUCT(
  sam::parse_structure,
  (std::vector<sam::OutputItem>, tokens)
)

namespace sam {

// This is the grammar for parsing filter expressions.
template <typename Iterator>
struct filter_grammar : boost::spirit::qi::grammar<Iterator, parse_structure(),
                                 boost::spirit::ascii::space_type>
{
  filter_grammar() : filter_grammar::base_type(expr)
  {
    using boost::spirit::qi::lit;
    using boost::spirit::qi::alnum;
    using boost::spirit::qi::alpha;
    using boost::spirit::qi::lexeme;
    using boost::spirit::double_;
    using boost::phoenix::at_c;
    using boost::spirit::ascii::char_;
    using namespace boost::spirit::qi::labels;
    using boost::phoenix::push_back;

    expr = (function [push_back(at_c<0>(_val), _1)] |
           identifier [push_back(at_c<0>(_val), _1)] |
           double_ [push_back(at_c<0>(_val), _1)]) >>
           *(op [push_back(at_c<0>(_val), _1)] >>
              (function [push_back(at_c<0>(_val), _1)]  |
               identifier [push_back(at_c<0>(_val), _1)] |
               double_ [push_back(at_c<0>(_val), _1)]
              )
            );
    identifier = lexeme[alpha [_val += _1] >> +(alnum)  [_val += _1]];
    //str_double = double_ [_val = _1]; 
    function = identifier [at_c<0>(_val) = _1] >>
               lit(".") >>
               value_id [at_c<1>(_val) = _1] >>
               (lit("(") >> parameters [at_c<2>(_val) = _1] >> lit(")")
                | lit("(") >> lit(")"));

    //parameters = (identifier [push_back(at_c<0>(_val), _1)] |
    //              double_ [push_back(at_c<0>(_val), _1)]) >>
    //                *(lit(",") >>
    //                 (identifier [push_back(at_c<0>(_val), _1)]  |
    //                  double_ [push_back(at_c<0>(_val), _1)]));
    parameters = (double_ [push_back(at_c<0>(_val), _1)]) >>
                    *(lit(",") >>
                     (double_ [push_back(at_c<0>(_val), _1)]));
                      

    value_id = lit("value") [_val = "value"];
    op = lit("+") [_val = "+"] |
         lit("-") [_val = "-"] |
         lit(">") [_val = ">"] |
         lit("<") [_val = "<"] |
         lit("<=") [_val = "<="] |
         lit(">=") [_val = ">="] |
         lit("*")  [_val = "*"] |
         lit("/")  [_val = "/"] |
         lit("^")  [_val = "^"];

  }

  boost::spirit::qi::rule<Iterator, parse_structure(), 
                          boost::spirit::ascii::space_type> expr;
  boost::spirit::qi::rule<Iterator, std::string(), 
                          boost::spirit::ascii::space_type> identifier;
  boost::spirit::qi::rule<Iterator, function_structure(), 
                          boost::spirit::ascii::space_type> function;
  boost::spirit::qi::rule<Iterator, std::string()> value_id;
  boost::spirit::qi::rule<Iterator, parameter_structure(), 
                          boost::spirit::ascii::space_type> parameters;
  boost::spirit::qi::rule<Iterator, std::string()> op;
  //boost::spirit::qi::rule<Iterator, std::string()> str_double;
};



// Base class representing a token in a filter expression.
class FilterToken {
public:
  virtual std::string toString() const = 0; 
  virtual bool isOperator() const = 0;
  virtual double evaluate(std::string const& key,
                          FeatureMap const& featureMap) const = 0;
  virtual double evaluate(double d1, double d2) const = 0;
};

std::ostream& operator<< (std::ostream& os, const FilterToken& f) 
{
  os << f.toString();
  return os;
}


/**
 * A function token looks like 
 *  top2.value(1)
 * It has an identifier (variable name), a function name, and a parameter list
 */
class FunctionToken : public FilterToken
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
  throw std::runtime_error("evaluate(d1, d2) not defined for FilterToken");
}


class NumberToken : public FilterToken
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

class IdentifierToken : public FilterToken
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

class OperatorToken : public FilterToken 
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
 * A class that tokenizes a filter expression e.g. 
 *  top2.value(0) + top2.value(1) < 0.9
 */
class FilterTokenizer
{
private:
  typedef std::vector<std::shared_ptr<FilterToken>> TokenList;
  TokenList tokens; 
public:
  typedef TokenList::iterator iterator;
  typedef TokenList::const_iterator const_iterator;
  
  FilterTokenizer(std::string filterExpression);
  
  iterator begin() { return tokens.begin(); }
  iterator end() { return tokens.end(); }

  std::shared_ptr<FilterToken> get(int i) { return tokens[i]; }

private:
  void populateDataStructure(parse_structure & result); 

};

FilterTokenizer::FilterTokenizer(std::string filterExpression)
{
  using boost::spirit::ascii::space;

  parse_structure result;
  sam::filter_grammar<std::string::const_iterator> grammar;
  std::string::const_iterator iter = filterExpression.begin();
  std::string::const_iterator end  = filterExpression.end(); 
  bool r = phrase_parse(iter, end, grammar, space, result);
  if (r && iter == end) {
    populateDataStructure(result);
  } else {
    throw std::runtime_error("Couldn't not parse filter expression");
  }
}

class TokenVisitor : public boost::static_visitor<>
{
public:

  TokenVisitor(std::vector<std::shared_ptr<FilterToken>> & _tokens) : 
    tokens(_tokens) {}

  void operator()(std::string & str) const
  {
    if (str.compare("+") == 0) {
      tokens.push_back(std::shared_ptr<FilterToken>(new PlusToken()));    
    } else if (str.compare("-") == 0) {
      tokens.push_back(std::shared_ptr<FilterToken>(new MinusToken()));
    } else if (str.compare("*") == 0) {
      tokens.push_back(std::shared_ptr<FilterToken>(new MultToken()));
    } else if (str.compare("/") == 0) {
      tokens.push_back(std::shared_ptr<FilterToken>(new DivideToken()));
    } else if (str.compare("^") == 0) {
      tokens.push_back(std::shared_ptr<FilterToken>(new PowerToken()));
    } else if (str.compare("<") == 0) {
      tokens.push_back(std::shared_ptr<FilterToken>(new LessToken()));
    } else if (str.compare(">") == 0) {
      tokens.push_back(std::shared_ptr<FilterToken>(new GreaterToken()));
    } else if (str.compare("<=") == 0) {
      tokens.push_back(std::shared_ptr<FilterToken>(new LessOrEqualToken()));
    } else if (str.compare(">=") == 0) {
      tokens.push_back(std::shared_ptr<FilterToken>(new GreaterOrEqualToken()));
    } else {
      tokens.push_back(std::shared_ptr<FilterToken>(new IdentifierToken(str)));
    } 
  }

  void operator()(function_structure & f) const
  {
    tokens.push_back(std::shared_ptr<FilterToken>(new 
                    FunctionToken(f.identifier, f.function, 
                                  f.parameters.parameters)));
  }

  void operator()(double & d) const
  {
    tokens.push_back(std::shared_ptr<FilterToken>(new NumberToken(d)));
  }

private:
  std::vector<std::shared_ptr<FilterToken>> & tokens;
};

void FilterTokenizer::populateDataStructure(parse_structure & result)
{
  TokenVisitor visitor(tokens);
  std::for_each(result.tokens.begin(), result.tokens.end(), 
                boost::apply_visitor( visitor));
}

}

#endif
