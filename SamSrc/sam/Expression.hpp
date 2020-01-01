#ifndef FILTER_EXPRESSION_HPP
#define FILTER_EXPRESSION_HPP

#include <stack>
#include <list>
#include <vector>
#include <string>
#include <iostream>
#include <stdexcept>
#include <tuple>
#include <sam/FeatureMap.hpp>
#include <sam/Tokens.hpp>
#include <sam/Util.hpp>

namespace sam {


template <typename TupleType>
class Expression 
{
  
protected:
  // Stores the expression in postfix form.
  std::list<std::shared_ptr<ExpressionToken<TupleType>>> postfixList;

public:
  /**
   * Constructor for expression.  It expects a list of tokens in 
   * infix form.
   */
  Expression(std::list<std::shared_ptr<ExpressionToken<TupleType>>> 
             infixList)
  {
    std::stack<std::shared_ptr<OperatorToken<TupleType>>> operatorStack;
    for (auto token : infixList)
    {
      if (token->isOperator()) {
        addOperator(std::static_pointer_cast<OperatorToken<TupleType>>(
                    token),
                    operatorStack);
      } else {
        postfixList.push_back(token);
      }
    }

    while (operatorStack.size() > 0) {
      auto top = operatorStack.top();
      postfixList.push_back(top);
      operatorStack.pop();
    }
  }

  bool evaluate(std::string const& key, 
                TupleType const& input, 
                double& result) const 
  {
    std::stack<double> mystack;
    int i = 0;
    for (auto token : postfixList) {
      //std::cout << "i " << i << std::endl;
      i++;
      if (!token->evaluate(mystack, key, input)) {
        return false;
      }
    }
    result = mystack.top();
    return true;
  }


private:
  void addOperator(std::shared_ptr<OperatorToken<TupleType>> o1,
  std::stack<std::shared_ptr<OperatorToken<TupleType>>> & operatorStack)
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
          postfixList.push_back(top);
        }
       } while (operatorStack.size() > 0 && foundQualifyingTopElement);
    }
    operatorStack.push(o1);
  }

};




/*
template <typename... Ts>
class Expression 
{};

template <typename... Ts>
class Expression<std::tuple<Ts...>> 
{
  
private:
  // Stores the expression in postfix form.
  std::list<std::shared_ptr<ExpressionToken<std::tuple<Ts...>>>> postfixList;

public:*/
  /**
   * Constructor for expression.  It expects a list of tokens in 
   * infix form.
   */
/*  Expression(std::list<std::shared_ptr<ExpressionToken<std::tuple<Ts...>>>> 
             infixList)
  {
    std::stack<std::shared_ptr<OperatorToken<std::tuple<Ts...>>>> operatorStack;
    for (auto token : infixList)
    {
      if (token->isOperator()) {
        addOperator(std::static_pointer_cast<OperatorToken<std::tuple<Ts...>>>(
                    token),
                    operatorStack);
      } else {
        postfixList.push_back(token);
      }
    }

    while (operatorStack.size() > 0) {
      auto top = operatorStack.top();
      postfixList.push_back(top);
      operatorStack.pop();
    }
  }

  void addOperator(std::shared_ptr<OperatorToken<std::tuple<Ts...>>> o1,
  std::stack<std::shared_ptr<OperatorToken<std::tuple<Ts...>>>> & operatorStack)
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
          postfixList.push_back(top);
        }
       } while (operatorStack.size() > 0 && foundQualifyingTopElement);
    }
    operatorStack.push(o1);
  }


  bool evaluate(std::string const& key, 
                std::tuple<Ts...> const& input, 
                double& result) const 
  {
    std::stack<double> mystack;
    int i = 0;
    for (auto token : postfixList) {
      //std::cout << "i " << i << std::endl;
      i++;
      if (!token->evaluate(mystack, key, input)) {
        return false;
      }
    }
    result = mystack.top();
    return true;
  }
};*/


}

#endif
