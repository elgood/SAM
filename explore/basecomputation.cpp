
#include <cstddef>
#include <string>
#include <tuple>
#include <iostream>
#include <boost/lexical_cast.hpp>

template <size_t... keyFields>
class BaseComputation
{
protected:
  size_t nodeId; ///> Used for debugging/metrics per node

  /// The variable name assigned to this operator.  This is specified
  /// in the query.
  std::string identifier; 

public:
  BaseComputation(size_t nodeId, std::string identifier) {
    this->nodeId = nodeId;
    this->identifier = identifier;
  }


  template<typename... Ts>
  std::string generateKey(std::tuple<Ts...> const& t) const {
    return "";
  }

  
};


template <size_t valueField, size_t keyField, size_t... keyFields>
class BaseComputation<valueField, keyField, keyFields...> 
  : BaseComputation<keyFields...>
{
private:

public:

  BaseComputation(size_t nodeId, 
                  std::string identifier) 
  : BaseComputation<keyFields...>(nodeId, identifier)
  {

  }

  template<typename... Ts>
  std::string generateKey(std::tuple<Ts...> const& t) const
  {
    std::string key = boost::lexical_cast<std::string>(std::get<keyField>(t));
    return key + BaseComputation<keyFields...>::generateKey(t);
  }
};

int main(int argc, char** argv)
{
  size_t nodeId = 0;
  std::string identifier = "id";
  BaseComputation<1, 0> bc(nodeId, identifier);
  auto t = std::make_tuple("15", "blah");
  std::string key = bc.generateKey(t);
  std::cout << "key " << key << std::endl;
}
