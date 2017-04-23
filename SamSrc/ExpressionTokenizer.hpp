#ifndef FILTER_TOKENIZER_HPP
#define FILTER_TOKENIZER_HPP

#include <boost/spirit/include/qi_parse.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/lexical_cast.hpp>
#include <vector>
#include <iostream>

#include "Tokens.hpp"
#include "Grammars.hpp"

// I think this is somehow even though we don't reference it.
//namespace fusion = boost::fusion;

namespace sam {

/**
 * A class that tokenizes a filter expression e.g. 
 *  top2.value(0) + top2.value(1) < 0.9
 */
template <typename Grammar>
class ExpressionTokenizer
{
private:
  typedef std::vector<std::shared_ptr<ExpressionToken>> TokenList;
  TokenList tokens; 
public:
  typedef TokenList::iterator iterator;
  typedef TokenList::const_iterator const_iterator;
  
  ExpressionTokenizer(std::string filterExpression);
  
  iterator begin() { return tokens.begin(); }
  iterator end() { return tokens.end(); }

  std::shared_ptr<ExpressionToken> get(int i) { return tokens[i]; }

private:
  void populateDataStructure(ParseStructure & result); 

};

template <typename Grammar>
ExpressionTokenizer<Grammar>::ExpressionTokenizer(std::string filterExpression)
{
  using boost::spirit::ascii::space;

  ParseStructure result;
  Grammar grammar;
  //sam::FilterGrammar<std::string::const_iterator> grammar;
  std::string::const_iterator iter = filterExpression.begin();
  std::string::const_iterator end  = filterExpression.end(); 
  bool r = phrase_parse(iter, end, grammar, space, result);
  if (r && iter == end) {
    populateDataStructure(result);
  } else {
    throw std::runtime_error("Couldn't parse filter expression");
  }
}

template <typename Grammar>
void ExpressionTokenizer<Grammar>::populateDataStructure(ParseStructure& result)
{
  TokenVisitor visitor(tokens);
  std::for_each(result.tokens.begin(), result.tokens.end(), 
                boost::apply_visitor( visitor));
}

}

#endif
