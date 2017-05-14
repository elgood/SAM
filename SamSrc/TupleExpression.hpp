#ifndef TUPLE_EXPRESSION_HPP
#define TUPLE_EXPRESSION_HPP

#include <vector>
#include <string>
#include <typeinfo>

#include "Expression.hpp"

namespace sam {

template <typename... Ts>
class TupleExpression
{};

template <typename... Ts>
class TupleExpression<std::tuple<Ts...>>
{
private:
  std::vector<Expression<std::tuple<Ts...>>> expressions;
  std::vector<std::string> names;
  std::vector<std::string> types;  
public:
  TupleExpression(std::vector<Expression<std::tuple<Ts...>>> const& expressions,
                  std::vector<std::string> const& names)
  {
    this->expressions = std::vector<Expression<std::tuple<Ts...>>>(expressions);
    this->names = std::vector<std::string>(names);

  }
                  

  // For iterating over the expressions
  typedef typename std::vector<Expression<std::tuple<Ts...>>>::iterator 
    iterator;
  typedef typename std::vector<Expression<std::tuple<Ts...>>>::const_iterator 
    const_iterator;
          

  iterator begin() { return expressions.begin(); }
  iterator end() { return expressions.end(); }
  const_iterator begin() const { return expressions.begin(); }
  const_iterator end() const { return expressions.end(); }


};

}



#endif
