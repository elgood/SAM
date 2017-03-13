#ifndef FEATURES_HPP
#define FEATURES_HPP


namespace sam {

/**
 * This represents features that are a single value.
 * Examples include sum, variance.
 */
template <typename T>
class SingleFeature: public AdditionalFeature
{
private:
  T feature;

public:
  SingleFeature(T _feature): feature(_feature) {}

  T getFeature() {
    return feature; 
  }

};

}

#endif
