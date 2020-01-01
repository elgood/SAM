#ifndef SAM_VERTEX_CONSTRAINT_CHECKER_HPP
#define SAM_VERTEX_CONSTRAINT_CHECKER_HPP

#include <sam/FeatureMap.hpp>
#include <sam/Util.hpp>
#include <sam/EdgeDescription.hpp>

namespace sam {

class VertexConstraintCheckerException : public std::runtime_error {
public:
  VertexConstraintCheckerException(char const * message) : 
    std::runtime_error(message) { }
  VertexConstraintCheckerException(std::string message) : 
    std::runtime_error(message) { }
};

/**
 * A class that has the logic to check vertex constraints.
 * Vertex constraints are defined within subgraph queries and form
 * constraints on individual vertices.  The currently supported
 * constraints are 
 *
 * in - The vertex is a key in the feature map for a particular feature.
 *  Example: 
 *  bait in Top100
 *  This means that the vertex variable bait has a binding that is found
 *  as a key in the featureMap under feature Top100.
 * 
 * notIn - Similar to "in", but this time the vertex should not be found
 *  in the feature map for the specified feature.
 *
 * TODO: other vertex constraints to be defined.  The class seems somewhat
 * specific to "in" and "notIn", so may be hard to generalize to other
 * types of vertex constaints.
 */
template <typename SubgraphQueryType>
class VertexConstraintChecker
{
private:
  std::shared_ptr<const FeatureMap> featureMap;  
  SubgraphQueryType const* subgraphQuery;
public:
  
  /**
   * The constructor for the VertexConstraintChecker class.  You provide
   * 1) featureMap - This is used for the "in" and "notIn" vertex 
   *   constraints.  We use the featureMap to see if a vertex is found
   *   in the featureMap for a given feature.
   * 2) The subgraph query itself.  The only thing we use the subgraph
   *   query for is to get the list of vertex constraints.  
   *   TODO: Might be nice to get rid of the reference to the subgraph
   *   query.
   */
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

    // lambda function that checks that the 
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
