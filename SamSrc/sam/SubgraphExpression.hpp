#ifndef SAM_SUBGRAPH_EXPRESSION_HPP
#define SAM_SUBGRAPH_EXPRESSION_HPP

#include <sam/Expression.hpp>

/**
 * The Expression class was originally designed for Filter expressions.
 * This is an attempt to generalize that logic for expressions used
 * for subgraph queries.
 */
template <typename TupleType>
class SubgraphExpression : public Expression<TupleType>
{
public:
  /**
   * Constructor for expression.  It expects a list of tokens in 
   * infix form.
   */
  Expression(std::list<std::shared_ptr<ExpressionToken<TupleType>>> 
             infixList) : Expression<TupleType>(infixList)
  { }





}

#endif
