#include <string>
#include <stack>
#include <list>
#include <iostream>
#include <vector>
#include "FeatureMap.hpp"
#include "Netflow.hpp"

using namespace sam;

class ExpressionToken
{
protected:
  // When tokens are created, they are always created in the context of a 
  // feature map.  Thus, while not all tokens need a feature map to evaluate
  // them, all tokens have a reference to a feature map.
  FeatureMap &featureMap;
public:

  /**
   * Constructor for the base class.  
   * \param _featureMap The feature map that some tokens need to evaluate.
   */
  ExpressionToken(FeatureMap& _featureMap) : featureMap(_featureMap) {}
  
  /**
   * Returns a string representation.  Mostly for debugging.  Should be 
   * overriden by child classes.
   */
  virtual std::string toString() const { return "ExpressionToken";}

  /**
   * Evaluates the token.  Changes are made to the stack to reflect the
   * change from evaluating the token.
   * \param mystack The stack used in evaluating the postfix expression.
   * \param key The key that is used to find relevant entries in the feature
   *  map.
   * \return Returns true if the token evaluating correctly.  False otherwise.
   *  For example, sometimes data is unavailable (e.g. for PrevToken) so the
   *  token can't be evaluating this iteration.
   */
  virtual bool evaluate(std::stack<double> & mystack,
                        std::string const& key) { return false; }

  /**
   * Returns true if the token is an operator.  False otherwise
   */
  virtual bool isOperator() const { return false; }

};

/**
 * Token representing a simple number like 1 or 6.5
 */
class NumberToken : public ExpressionToken
{
private:
  double number;
public:
  NumberToken(FeatureMap& featureMap, double d) : 
    ExpressionToken(featureMap), number(d) {}
 
  std::string toString() const {
    return "NumberToken: " + boost::lexical_cast<std::string>(number);
  }

  /**
   * Simply pushes the number to the top of the stack.
   */
  bool evaluate(std::stack<double> & mystack,
                std::string const& key) 
  {
    mystack.push(number);
    return true;
  }

  bool isOperator() const { return false; }
};

class OperatorToken : public ExpressionToken
{
private:
  int precedence;
  int associativity;
public:
  const static int RIGHT_ASSOCIATIVE = 0;
  const static int LEFT_ASSOCIATIVE = 1; 
  OperatorToken(FeatureMap& featureMap,
                int associativity,
                int precedence) : ExpressionToken(featureMap) 
  {
    this->associativity = associativity;
    this->precedence = precedence;
  }
  
  bool isOperator() const { return true; }
  bool isLeftAssociative() const { return associativity == LEFT_ASSOCIATIVE; }
  bool isRightAssociative() const { return associativity == RIGHT_ASSOCIATIVE; }
  int getPrecedence() const { return precedence; }

};

class AddOperator : public OperatorToken
{
public:
  AddOperator(FeatureMap& featureMap) : 
    OperatorToken(featureMap, 2, LEFT_ASSOCIATIVE) {}

  std::string toString() const {
    return "AddOperator";
  }

  bool evaluate(std::stack<double> & mystack,
                std::string const& key)
  {
    if (mystack.size() >= 2) {
      double o2 = mystack.top();
      mystack.pop();
      double o1 = mystack.top();
      mystack.pop();
      double result = o1 + o2;
      mystack.push(result);
      return true;
    }
    return false;
  }
};

class SubOperator : public OperatorToken
{
public:
  SubOperator(FeatureMap& featureMap) : 
    OperatorToken(featureMap, 2, LEFT_ASSOCIATIVE) {}

  std::string toString() const {
    return "SubOperator";
  }

  bool evaluate(std::stack<double> & mystack,
                std::string const& key)
  {
    if (mystack.size() >= 2) {
      double o2 = mystack.top();
      mystack.pop();
      double o1 = mystack.top();
      mystack.pop();
      double result = o1 - o2;
      mystack.push(result);
      return true;
    } 
    return false;
  }
};

class MultOperator : public OperatorToken
{
public:
  MultOperator(FeatureMap& featureMap) : 
    OperatorToken(featureMap, 3, LEFT_ASSOCIATIVE) {}

  std::string toString() const {
    return "MultOperator";
  }

  bool evaluate(std::stack<double> & mystack,
                std::string const& key)
  {
    if (mystack.size() >= 2) {
      double o2 = mystack.top();
      mystack.pop();
      double o1 = mystack.top();
      mystack.pop();
      double result = o1 - o2;
      mystack.push(result);
      return true;
    }
    return false;
  }
};




/**
 * Represents one field of an input
 */
template <typename InputType, size_t field>
class FieldToken : public ExpressionToken
{
private:
  InputType input;
  std::string identifier;
public:
  FieldToken(FeatureMap &featureMap) : ExpressionToken(featureMap) {}

  bool evaluate(std::stack<double> & mystack,
                std::string const& key)
  {
    try {
      double data = boost::lexical_cast<double>(std::get<field>(input));
      mystack.push(data);
      return true;
    } catch (std::exception e) {
      std::cout << e.what() << std::endl;
      return false;
    }
  }

  void setInput (InputType input) {
    this->input = input;
  }

  bool isOperator() const { return false; }
};

/**
 * Represents tokens of the form identifier.function(parameters), e.g.
 * top2.value(1)
 */
class FuncToken : public ExpressionToken
{
private:
  std::string identifier; ///> The name of the variable, e.g. top2
  std::string function; ///> The name of the function e.g. value
  std::vector<double> parameters; ///> The parameters to the function
public:
  FuncToken(FeatureMap &featureMap,
            std::string identifier,
            std::string function,
            std::vector<double> parameters) : ExpressionToken(featureMap) 
  {
    this->identifier = identifier;
    this->function = function;
    this->parameters = parameters;  
  }

  bool evaluate(std::stack<double> & mystack,
                std::string const& key)
  {
    if (featureMap.exists(key, identifier)) {
      double d = featureMap.at(key, identifier)->evaluate(function, parameters);
      mystack.push(d);
      return true;
    }
    return false;
  }

  bool isOperator() const { return false; }
};

template <typename InputType, size_t field>
class PrevToken : public ExpressionToken 
{
private:
  InputType input;
  std::string identifier;
public:
 
  PrevToken(FeatureMap &featureMap) : ExpressionToken(featureMap) 
  {
    identifier = createPreviousIdentifierString();
  }

  bool evaluate(std::stack<double> & mystack, 
                  std::string const& key) 
  {
    double currentData = boost::lexical_cast<double>(std::get<field>(input));
    std::shared_ptr<Feature> feature 
      = std::make_shared<SingleFeature>(currentData);

    bool exists = false;

    if (featureMap.exists(key, identifier)) { 
      exists = true;
      auto feature = featureMap.at(key, identifier);
      double result = feature->evaluate(); 
      mystack.push(result);
    } 

    // Inserting the current data to become the past data
    feature = std::make_shared<SingleFeature>(currentData);
    featureMap.updateInsert(key, identifier, *feature);

    return exists;
     
  }

  void setInput (InputType input) {
    this->input = input;
  }

  bool isOperator() const { return false; }
private:
  std::string createPreviousIdentifierString()
  {
    return "previous_" + boost::lexical_cast<std::string>(field);
  }
};


class Expression {
private:
  // Stores the expression in postfix form.
  std::list<std::shared_ptr<ExpressionToken>> postfixList;
public:
  /**
   * Constructor for expression.  It expects a list of tokens in 
   * infix form.
   */
  Expression(std::list<std::shared_ptr<ExpressionToken>> infixList)
  {
    std::stack<std::shared_ptr<OperatorToken>> operatorStack;
    for (auto token : infixList)
    {
      if (token->isOperator()) {
        addOperator(std::static_pointer_cast<OperatorToken>(token),
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

  void addOperator(std::shared_ptr<OperatorToken> o1,
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
          postfixList.push_back(top);
        }
       } while (operatorStack.size() > 0 && foundQualifyingTopElement);
    }
    operatorStack.push(o1);
  }


  bool evaluate(std::string const& key, double& result) {
    std::stack<double> mystack;
    for (auto token : postfixList) {
      if (!token->evaluate(mystack, key)) {
        return false;
      }
    }
    result = mystack.top();
    return true;
  }
};




/*class NoneOperator {};
class MinusOperator {};
class AddOperator {};

class Operator : public ExpressionToken
{
  
}

*/

int main (int argc, char** argv)
{
  std::string netflowString1 = "1365582756.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18,"
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";
  std::string netflowString2 = "1365582757.384094,2013-04-10 08:32:36,"
                         "20130410083236.384094,17,UDP,172.20.2.18,"
                         "239.255.255.250,29986,1900,0,0,0,133,0,2,0,1,0,0";
  Netflow netflow1 = makeNetflow(netflowString1);
  Netflow netflow2 = makeNetflow(netflowString2);  

  FeatureMap featureMap;
  std::string key = "key";

  std::shared_ptr<ExpressionToken> token1 =
    std::make_shared<NumberToken>(featureMap, 2);
  std::shared_ptr<ExpressionToken> addOper(new AddOperator(featureMap));
  std::shared_ptr<ExpressionToken> token2(new NumberToken(featureMap, 4));

  std::list<std::shared_ptr<ExpressionToken>> postfixList;
  postfixList.push_back(token1);
  postfixList.push_back(token2);
  postfixList.push_back(addOper);
  
  Expression expression1(postfixList);
  double result = 0;
  expression1.evaluate(key, result);
  std::cout << "result " << result << std::endl;

  postfixList.clear();

  std::shared_ptr<ExpressionToken> subOper = 
    std::make_shared<SubOperator>(featureMap);
  auto tokenPrev = 
    std::make_shared<PrevToken<Netflow, TIME_SECONDS_FIELD>>(featureMap);
  tokenPrev->setInput(netflow1);
  auto tokenField =
    std::make_shared<FieldToken<Netflow, TIME_SECONDS_FIELD>>(featureMap);
  tokenField->setInput(netflow1);
  postfixList.push_back(tokenField);
  postfixList.push_back(tokenPrev);
  postfixList.push_back(subOper);

  Expression expression2(postfixList);
  expression2.evaluate(key, result);
  std::cout << "result " << result << std::endl;
   
  tokenPrev->setInput(netflow2);
  tokenField->setInput(netflow2);
  expression2.evaluate(key, result);
  std::cout << "result " << result << std::endl;

  std::vector<double> parameters;
  parameters.push_back(1);
  std::shared_ptr<ExpressionToken> funcToken =
    std::make_shared<FuncToken>(featureMap, "top2", "value", parameters);
  postfixList.clear();
  postfixList.push_back(funcToken);
  Expression expression3(postfixList);
  bool b = expression3.evaluate(key, result);
  std::cout << "boolean " << b << std::endl;
 
}
