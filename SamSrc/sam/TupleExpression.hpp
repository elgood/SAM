#ifndef TUPLE_EXPRESSION_HPP
#define TUPLE_EXPRESSION_HPP

#include <vector>
#include <string>
#include <typeinfo>

#include <sam/Expression.hpp>

namespace sam {

template <typename... Ts>
class TupleExpression
{};

template <typename... Ts>
class TupleExpression<std::tuple<Ts...>>
{
private:
  std::vector<std::shared_ptr<Expression<std::tuple<Ts...>>>> expressions;
public:
  TupleExpression(std::vector<std::shared_ptr<Expression<std::tuple<Ts...>>>> 
                  const& expressions)
  {
    this->expressions = std::vector<std::shared_ptr<
                          Expression<std::tuple<Ts...>>>>(expressions);
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

  std::size_t size() const { return expressions.size(); }
  std::shared_ptr<Expression<std::tuple<Ts...>>> const& get(int i) const 
  { return expressions[i]; }

};

}



#endif
