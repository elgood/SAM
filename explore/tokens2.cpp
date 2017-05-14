
#include <iostream>
#include "Netflow.hpp"

using namespace sam;

template <typename... Ts>
class ExpressionToken
{
public:

};

template <typename... Ts>
class ExpressionToken<std::tuple<Ts...>>
{
public:
  virtual bool evaluate(std::tuple<Ts...> input) {
    double d = std::get<0>(input);
    std::cout << d <<std::endl;
    return true;
  }

};

int main ()
{
  std::string netflowString1 = "1365582756.384094,2013-04-10 08:32:36," 
                         "20130410083236.384094,17,UDP,172.20.2.18," 
                         "239.255.255.250,29986,1900,0,0,0,133,0,1,0,1,0,0";

  Netflow netflow = makeNetflow(netflowString1);
 

  ExpressionToken<Netflow> token;
  std::cout << token.evaluate(netflow) << std::endl;
}
