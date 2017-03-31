#ifndef FILTER_EXPRESSION_HPP
#define FILTER_EXPRESSION_HPP

#include <stack>
#include <list>
#include <vector>
#include <string>
#include <iostream>
#include <stdexcept>

#include "ImuxDataItem.hpp"
#include "FilterTokenizer.hpp"

namespace sam {

class FilterExpression {
private:
  // This stores the expression in postfix form.
  std::list<std::shared_ptr<FilterToken>> outputList;
  std::stack<std::shared_ptr<OperatorToken>> operatorStack;
public:  
  FilterExpression(std::string sExpression);

  double evaluate(ImuxDataItem const & item) const;
private:
  void addOperator(std::shared_ptr<OperatorToken> o1);
};


// See shunting yard algorithm for more details.
void FilterExpression::addOperator(std::shared_ptr<OperatorToken> o1)
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

FilterExpression::FilterExpression(std::string sExpression)
{

  FilterTokenizer tok(sExpression);

  // Shunting yard algorithm to get things into postfix
  for (FilterTokenizer::iterator tok_iter = tok.begin();
       tok_iter != tok.end(); ++tok_iter)
  {
    if ((*tok_iter)->isOperator()) {
      addOperator(std::static_pointer_cast<OperatorToken>(*tok_iter));
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


double FilterExpression::evaluate(ImuxDataItem const & item) const
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
      double result = token->evaluate(item);
      mystack.push(result);
    }
  }
  double result = mystack.top();
  return result;
}
  

}

#endif
