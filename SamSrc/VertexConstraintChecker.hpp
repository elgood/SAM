#ifndef SAM_VERTEX_CONSTRAINT_CHECKER_HPP
#define SAM_VERTEX_CONSTRAINT_CHECKER_HPP

#include "FeatureMap.hpp"
#include "Util.hpp"
#include "EdgeDescription.hpp"

namespace sam {

class VertexConstraintCheckerException : public std::runtime_error {
public:
  VertexConstraintCheckerException(char const * message) : 
    std::runtime_error(message) { }
  VertexConstraintCheckerException(std::string message) : 
    std::runtime_error(message) { }
};


template <typename SubgraphQueryType>
class VertexConstraintChecker
{
private:
  std::shared_ptr<const FeatureMap> featureMap;  
  SubgraphQueryType const* subgraphQuery;
public:
  VertexConstraintChecker(std::shared_ptr<const FeatureMap> featureMap,
                          SubgraphQueryType const* subgraphQuery)
  {
    this->featureMap = featureMap;
    this->subgraphQuery = subgraphQuery;  
  }

  bool check(std::string variable, std::string vertex) const
  {
    DEBUG_PRINT("VertexConstraintChecker checking variable %s vertex %s\n",
      variable.c_str(), vertex.c_str());
    auto existsVertex = [vertex](Feature const * feature)->bool {
      auto topKFeature = static_cast<TopKFeature const *>(feature);
      auto keys = topKFeature->getKeys();
      auto it = std::find(keys.begin(), keys.end(), vertex);
      if (it != keys.end()) {
        return true;
      }
      return false;
    };

    for (auto constraint : subgraphQuery->getConstraints(variable))
    {
      std::string featureName = constraint.featureName;
      DEBUG_PRINT("VertexConstraintChecker variable %s vertex %s featureName"
        " %s\n", variable.c_str(), vertex.c_str(), featureName.c_str());
    
      // If the feature doesn't exist, return false. 
      if (!featureMap->exists("", featureName)) {
        DEBUG_PRINT("VertexConstraintChecker returning false for "
            "variable %s and vertex %s becaure featureName %s doesn't exist\n",
            variable.c_str(), vertex.c_str(), featureName.c_str());
        return false;
      }

      auto feature = featureMap->at("", featureName);
      switch(constraint.op)
      {
        case VertexOperator::In:
          if (!feature->template evaluate<bool>(existsVertex))
          {
            DEBUG_PRINT("VertexConstraintChecker(In) returning false for "
              "variable %s and vertex %s\n", variable.c_str(), vertex.c_str());
            return false;
          }
          break;
        case VertexOperator::NotIn:
          if (feature->template evaluate<bool>(existsVertex))
          {
            DEBUG_PRINT("VertexConstraintChecker(NotIn) returning false for"
              " variable %s and vertex %s\n", variable.c_str(), vertex.c_str());
            return false;
          }
          break;
        default:
          throw VertexConstraintCheckerException(
            "Unsupported vertex constraint.");   
      }
    }
    DEBUG_PRINT("VertexConstraintChecker returning true for variable %s and "
      "vertex %s\n", variable.c_str(), vertex.c_str());
    return true;
  }

  /**
   *
   * \param variable The variable name of the vertex.
   * \param vertex The actual value of the vertex.
   */
  bool operator()(std::string variable, std::string vertex) const
  {
    return check(variable, vertex);
  }
};

}

#endif
