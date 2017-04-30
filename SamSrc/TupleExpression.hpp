#ifndef TUPLE_EXPRESSION_HPP
#define TUPLE_EXPRESSION_HPP

#include <vector>
#include <string>

#include "Expression.hpp"

namespace sam {

class TupleExpression
{
public:
  typedef Expression<TransformGrammar<std::string::const_iterator>>
          ExpressionType;
private:
  std::vector<ExpressionType> expressions;
  std::vector<std::string> names;  
public:
  TupleExpression(std::vector<std::string> const& expressions,
                  std::vector<std::string> const& names);



};

TupleExpression::TupleExpression(std::vector<std::string> const& expressions,
                                 std::vector<std::string> const& names)
{
  for (auto expression : expressions) {
    this->expressions.push_back(ExpressionType(expression));
  }
  this->names(names);

}


}


#endif
