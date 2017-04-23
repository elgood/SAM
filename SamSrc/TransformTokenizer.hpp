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

#include "Tokens.hpp"

namespace sam {

/*class TransformToken {
public:
  virtual std::string toString() const = 0;
};

std::ostream& operator<< (std::ostream& os, const TransformToken& f) 
{
  os << f.toString();
  return os;
}



class IdentifierToken : public TransformToken
{
  std::string toString() const;
};

std::string IdentifierToken::toString() const
{
  return "Identifier Token";
}

class TransformTokenizer
{
private:
  typedef std::vector<std::shared_ptr<TransformToken>> TokenList;
  TokenList tokens;
public:
  typedef TokenList::iterator iterator;
  typedef TokenList::const_iterator const_iterator;
 
  TransformTokenizer(std::string expression);
 
  iterator begin() { return tokens.begin(); }
  iterator end() { return tokens.end(); }

  std::shared_ptr<TransformToken> get(int i) { return tokens[i]; }

private:
  void populateDataStructure(ParsedTransformStructure & result);

};

TransformTokenizer::TransformTokenizer(std::string expression)
{
  using boost::spirit::ascii::space;

  ParsedTransformStructure result;
  sam::TransformGrammar<std::string::const_iterator> grammar;
  std::string::const_iterator iter = expression.begin();
  std::string::const_iterator end  = expression.end();
  bool r = phrase_parse(iter, end, grammar, space, result);
  if (r && iter == end) {
    populateDataStructure(result);
  } else {
    throw std::runtime_error("Couldn't parse transform expression");
  }
}

void TransformTokenizer::populateDataStructure(ParsedTransformStructure& result)
{

}
*/

}

#endif
