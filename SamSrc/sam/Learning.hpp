#ifndef SAM_LEARNING_HPP
#define SAM_LEARNING_HPP

#include <mlpack/core.hpp>
#include <mlpack/methods/naive_bayes/naive_bayes_classifier.hpp>

// This is currently not in use.  Learning/inference is done external to the 
// library. 
// A struct for saving the model with mappings.
struct NBCModel
{
  //! The model itself.
  mlpack::naive_bayes::NaiveBayesClassifier<> nbc;
  //! The mappings for labels.
  arma::Col<size_t> mappings;

  //! Serialize the model.
  template<typename Archive>
  void Serialize(Archive& ar, const unsigned int /* version */)
  {
    ar & mlpack::data::CreateNVP(nbc, "nbc");
    ar & mlpack::data::CreateNVP(mappings, "mappings");
  }
};




#endif
