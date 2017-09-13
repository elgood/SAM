#ifndef SAM_FEATURE_SUBSCRIBER_HPP
#define SAM_FEATURE_SUBSCRIBER_HPP

#include <vector>
#include <atomic>
#include <sstream>
#include <map>
#include <stdexcept>

#include "Util.hpp"
#include "Learning.hpp"

#define MAP_EMPTY        0
#define MAP_OCCUPIED     1
#define MAP_INTERMEDIATE 2

namespace sam {

/**
 * Has two modes, create feature mode and test mode.  In the create feature
 * mode, it outputs all of the features to a stringstream.  This might not
 * be the best approach.  A parallel implementation would be better.
 *
 * The other mode is test mode.  In test mode the feature subscriber has a
 * model that it applies to each example. 
 */
class FeatureSubscriber
{
private:
  // The column/feature names
  std::vector<std::string> names;

  std::map<std::string, int> featureIndices;

  // Stores the results of completed rows
  std::ofstream out;

  mutable std::mutex mu;

  int capacity;

  double* values = 0;
  int* counts = 0;

  // Init must be called before update is called.  This variable keeps track.
  bool initCalled = false;

  std::shared_ptr<NBCModel> model;

public:
  FeatureSubscriber(std::shared_ptr<NBCModel> model,
                    std::string outputfile,
                    int capacity = 10000) 
  {
    this->model = model;
    this->capacity = capacity;
    this->out = std::ofstream(outputfile);

    // TODO add parallel loop
    for(int i = 0; i < capacity; i++) {
      counts[i] = 0;
    }
  }

  FeatureSubscriber(std::string outputfile,
                    int capacity = 10000) 
                    
  {
    this->capacity = capacity;
    this->out = std::ofstream(outputfile);
    counts = new int[capacity];

    // TODO add parallel loop
    for(int i = 0; i < capacity; i++) {
      counts[i] = 0;
    }
  }

  void init() {
    if (getNumFeatures() <= 0) {
      throw std::logic_error("init was called but no features have been "
        "added");
    }
    initCalled = true;
    values = new double[capacity * getNumFeatures()]();
  }

  ~FeatureSubscriber() {
    if (values) {
      delete[] values;  
    }
    if (counts) {
      delete[] counts;
    }
  }
 
  /**
   * This method should be called by the FeatureProducer using 
   * FeatureProducer::registerSubscriber.
   */ 
  void addFeature(std::string name) {
    if (initCalled) {
      throw std::logic_error("addFeature was called after init was called."
        "  This is not allowed.");
    }
    names.push_back(name);
    featureIndices[names[names.size() - 1]] = names.size() - 1;
  }

  int getNumFeatures() { return names.size(); }

  /**
   * How the subscriber is informed of feature updates. 
   * Once all of the feature values have arrived for a particular record, a csv
   * line representing the data is output to a stringstream.  The stringstream
   * can then be accessed via getOutput().
   * \param key The key uniquely identifying the item that all the features
   *            are derived from.  We assume that the keys are a sequence of
   *            incresing integers.
   * \param featureName Identifies uniquely the feature that needs to be
   *            updated.  Generally this corresponds to the 
   *            BaseComputation's identifier.
   * \param value The value of the feature.
   */
  bool update(std::size_t key,
              std::string const& featureName,
              double value);

};

inline
bool FeatureSubscriber::update(std::size_t key,
                               std::string const& featureName,
                               double value)
{
  //std::cout << "update " << key << std::endl;
  int numFeatures = getNumFeatures();
  if (initCalled) {
    int index = key % capacity;
    int featureIndex = featureIndices[featureName];
    values[index * numFeatures + featureIndex] = value;
    counts[index]++;

    if (counts[index] >= numFeatures) {
      // The lock_guard makes it so that only one thread can access the 
      // following body of the if statement.

      std::lock_guard<std::mutex> lock(mu);
      //if (model) {
      //  model
      //  ss << 
      //} 

      counts[index] = 0;
      for (int j = 0; j < numFeatures - 1; j++) {
        out << values[index * numFeatures + j] << ","; 
      }
      out << values[index * numFeatures + numFeatures - 1] << std::endl;
      //std::cout << "Returning true2 " << std::endl;
      return true;
    }

    return true;
  } else {
      throw std::logic_error("update was called before init was called."
        "  This is not allowed.");

  }
}



}

#endif
