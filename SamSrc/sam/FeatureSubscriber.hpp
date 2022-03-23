#ifndef SAM_FEATURE_SUBSCRIBER_HPP
#define SAM_FEATURE_SUBSCRIBER_HPP

#include <vector>
#include <atomic>
#include <sstream>
#include <map>
#include <stdexcept>
#include <mutex>
#include <fstream>

#include <sam/Util.hpp>

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
 * model that it applies to each example.  TODO: Test mode is not implemented. 
 *
 * The only data type supported for features is doubles.
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

  // TODO: Figure out.  Before, this was a fixed sized hash table, but now
  // we just cycle through because we have a sequence of increasing integers
  // for the keys (SamGeneratedId).  As such, I think we can come up with a 
  // bound on the needed capacity as a function of the number of features. 
  int capacity;

  double* values = 0;
  std::atomic<int>* counts = 0;

  // Init must be called before update is called.  This variable keeps track.
  bool initCalled = false;

  int numRows = 0; ///> Keeps track of how many rows we've written.

  size_t numFeatures = 0;

public:

  FeatureSubscriber(std::string outputfile,
                    int capacity = 10000) 
                    
  {
    this->capacity = capacity;
    this->out = std::ofstream(outputfile);
    counts = new std::atomic<int>[capacity];

    // TODO add parallel loop
    for(int i = 0; i < capacity; i++) {
      counts[i] = 0;
    }
  }

  /**
   * Once all the features have been added using the addFeature
   * method, this function should be called.
   */
  void init() 
  {
    if (numFeatures <= 0) {
      throw std::logic_error("init was called but no features have been "
        "added");
    }
    initCalled = true;
    values = new double[capacity * numFeatures]();
  }

  ~FeatureSubscriber() 
  {
    if (values) {
      delete[] values;  
    }
    if (counts) {
      delete[] counts;
    }
  }
 
  /**
   * This method should be called by the FeatureProducer using 
   * FeatureProducer::registerSubscriber.  This method must be called
   * for each feature before init is called.
   */ 
  void addFeature(std::string name) {
    if (initCalled) {
      throw std::logic_error("addFeature was called after init was called."
        " This is not allowed.");
    }
    names.push_back(name);
    featureIndices[names[names.size() - 1]] = names.size() - 1;
    DEBUG_PRINT("FeatureSubscriber::addFeature Added feature %s with index "
     "%lu\n", name.c_str(), names.size() - 1) 
    numFeatures++;
  }

  int getNumFeatures() { return numFeatures; }

  /**
   * How the subscriber is informed of feature updates. 
   * Once all of the feature values have arrived for a particular record, a csv
   * line representing the data is output to a stringstream.  The stringstream
   * can then be accessed via getOutput().
   * \param key The key uniquely identifying the item that all the features
   *            are derived from.  We assume that the keys are a sequence of
   *            incresing integers.  This is the SamGeneratedId that is 
   *            preserved through all transformations.
   * \param featureName Identifies uniquely the feature that needs to be
   *            updated.  Generally this corresponds to the 
   *            BaseComputation's identifier.
   * \param value The value of the feature.
   */
  bool update(std::size_t key,
              std::string const& featureName,
              double value);

  void close() {
    out.close();
  }

};

inline
bool FeatureSubscriber::update(std::size_t key,
                               std::string const& featureName,
                               double value)
{
  //std::lock_guard<std::mutex> lock(mu);
  if (initCalled) {
    DEBUG_PRINT("FeatureSubscriber::update key %lu featureName %s value %f\n",
      key, featureName.c_str(), value)
    int index = key % capacity;
    int featureIndex = featureIndices[featureName];
    values[index * numFeatures + featureIndex] = value;
    counts[index].fetch_add(1);

    if (counts[index] >= numFeatures) {
      DEBUG_PRINT("FeatureSubscriber::update key %lu writing out row\n",
        key, featureName.c_str(), value)
      // counts[indes] >= numFeatures indicates that we have collected
      // all of the features associated with the input item (i.e. netflow
      // or whatever tuple).

      // The lock_guard makes it so that only one thread can access the 
      // following body of the if statement.
      std::lock_guard<std::mutex> lock(mu);

      counts[index] = 0;
      for (int j = 0; j < numFeatures - 1; j++) {
        out << values[index * numFeatures + j] << ","; 
      }
      out << values[index * numFeatures + numFeatures - 1] << std::endl;
      //std::cout << "Returning true2 " << std::endl;

      numRows++;
      //std::cout << "Numrows in FeatureSubscriber " << numRows << std::endl;
      if (this->numRows % 10000 == 0) {
        std::cout << "Feature subscriber has written out " << this->numRows 
                  << " rows" << std::endl;
      }

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
