#ifndef FILTER_EXPRESSION_HPP
#define FILTER_EXPRESSION_HPP

#include <stack>
#include <list>
#include <vector>
#include <string>
#include <iostream>
#include <stdexcept>

#include "ExpressionTokenizer.hpp"
#include "FeatureMap.hpp"

namespace sam {

template <typename Grammar>
class Expression {
private:
  // This stores the expression in postfix form.
  std::list<std::shared_ptr<ExpressionToken>> outputList;
public:  
  Expression(std::string sExpression);

  double evaluate(std::string const& key, FeatureMap const& featureMap) const;

  typedef std::list<std::shared_ptr<ExpressionToken>>::iterator iterator;
  typedef std::list<std::shared_ptr<ExpressionToken>>::const_iterator 
          const_iterator;

  iterator begin() { return outputList.begin(); }
  iterator end() { return outputList.end(); }
private:
  /**
   * Adds operator to the operator stack.
   * See shunting yard algorithm for more details.
   */
  void addOperator(std::shared_ptr<OperatorToken> o1,
    std::stack<std::shared_ptr<OperatorToken>> & operatorStack);
};

template <typename Grammar>
void Expression<Grammar>::addOperator(std::shared_ptr<OperatorToken> o1,
             std::stack<std::shared_ptr<OperatorToken>> & operatorStack)
{
  if (operatorStack.size() > 0) {
    auto top = operatorStack.top();
    bool foundQualifyingTopElement = false;
    do {
      foundQualifyingTopElement = false;
      if (
          (o1->isLeftAssociative() && 
            (o1->getPrecedence() <= top->getPrecedence())) 
          ||
          (o1->isRightAssociative() && 
            (o1->getPrecedence() < top->getPrecedence()))
         )
      {
        foundQualifyingTopElement = true;
        operatorStack.pop();
        outputList.push_back(top);
      }
     } while (operatorStack.size() > 0 && foundQualifyingTopElement);
  }
  operatorStack.push(o1);
}

template <typename Grammar>
Expression<Grammar>::Expression(std::string sExpression)
{
  typedef ExpressionTokenizer<Grammar> Tokenizer;
    
  Tokenizer tok(sExpression);
  std::stack<std::shared_ptr<OperatorToken>> operatorStack;

  // Shunting yard algorithm to get things into postfix
  for (typename Tokenizer::iterator tok_iter = tok.begin();
       tok_iter != tok.end(); ++tok_iter)
  {
    if ((*tok_iter)->isOperator()) {
      addOperator(std::static_pointer_cast<OperatorToken>(*tok_iter),
                  operatorStack);
    } else
    {
      outputList.push_back(*tok_iter);
    }
  }

  // Process the remaining operators on the operator stack.
  while (operatorStack.size() > 0)
  {
    auto top = operatorStack.top();
    outputList.push_back(top);
    operatorStack.pop();
  }
}

template <typename Grammar>
double Expression<Grammar>::evaluate(std::string const& key, 
                                  FeatureMap const& featureMap) const
{
  std::stack<double> mystack;
  for (auto token : outputList) {
    if (token->isOperator()) {
      double o2 = mystack.top();
      mystack.pop();
      double o1 = mystack.top();
      mystack.pop();
      double result = token->evaluate(o1, o2);
      mystack.push(result); 
    } else {
      double result = token->evaluate(key, featureMap);
      mystack.push(result);
    }
  }
  double result = mystack.top();
  return result;

}


}

#endif
