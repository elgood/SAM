#ifndef FEATURE_MAP_HPP
#define FEATURE_MAP_HPP

#include <iostream>
#include <atomic>
#include "Features.hpp"
#include <cstdio>

#define MAP_EMPTY        0
#define MAP_OCCUPIED     1
#define MAP_INTERMEDIATE 2

namespace sam {

class FeatureMap
{
private:
  // The capacity of the parallel map.  Should be 2 * numkeys * numfeatures
  int capacity;

  // An array of features as shared ptrs.
  std::shared_ptr<Feature>* features;

  // The flag keeps track of if a slot is empty, occupied, or in an
  // intermediate state.
  std::atomic<int> volatile * flag;

  // An array of keys.  This is the combination of the key for the imux
  // data structure and the name of the feature.
  std::string* keys;

public:   
  /**
   * Capacity should be 2 * numkeys * numfeatures
   */
  FeatureMap(int capacity = 1000) {
    this->capacity = capacity;  
    features = new std::shared_ptr<Feature>[capacity];
    flag = new std::atomic<int>[capacity];
    keys = new std::string[capacity];

    // TODO: Add parallel loop
    for (int i = 0; i < capacity; i++) {
      features[i] = 0;
    }

    // TODO: Add a parallel loop
    for (int i = 0; i < capacity; i++) {
      flag[i] = 0; 
    }
  }

  ~FeatureMap() {
    delete[] features; 
    delete[] flag;  
    delete[] keys;  
  }

  /**
   * Inserts the feature to the key-featureName combo if it doesn't exist, or 
   * updates the feature if it does exist.
   * \param key The key identifying the entity (e.g. an IP address)
   * \param featureName The name of the feature (e.g. an operator name)
   * \param f The feature to be added.
   * \return Returns true if the update took place.  False if there is no room
   *         in the table.
   */
  bool updateInsert(std::string const& key, 
                     std::string const& featureName,  
                     Feature const& f);
  
  std::shared_ptr<const Feature> at(std::string const& key,
                               std::string const& featureName) const; 


  /**
   * Checks if the key/featureName combo exists.
   */ 
  bool exists(std::string const& key,
              std::string const& featureName) const; 

private:
  /**
   * The hash function used to hash the key-featureName combo.
   * \param key The combined key-featureName combo.
   * \returns Returns the int hash.
   */
  unsigned int hashFunction(std::string const& key) const;

};

inline
unsigned int FeatureMap::hashFunction(std::string const& key) const
{
  unsigned int hash = 0;
  
  for (int i = 0; i < key.size(); i++) {
    hash = key[i] + (hash << 6) + (hash << 16) - hash;
  }

  return hash;
}

inline
bool FeatureMap::exists(std::string const& key,
                        std::string const& featureName) const
{
  std::string combinedKey = key + featureName;
  unsigned int hash = hashFunction(combinedKey);
  int i = hash % capacity;
  int index = i;
  do
  {
    if (flag[i] == MAP_OCCUPIED) {
      if (keys[i].compare(combinedKey) == 0)
      {
        return true;
      }
    }

    i = (i + 1) % capacity;
  } while (i != index && flag[i] != MAP_EMPTY);
  return false;

}

inline
std::shared_ptr<Feature const> FeatureMap::at(std::string const& key, 
                                          std::string const& featureName) const
{
  std::string combinedKey = key + featureName;
  unsigned int hash = hashFunction(combinedKey);
  int i = hash % capacity;
  int index = i;
  do
  {
    if (flag[i] != MAP_EMPTY) {
      if (keys[i].compare(combinedKey) == 0)
      {
        return std::static_pointer_cast<Feature const>( features[i] );
      }
    }

    i = (i + 1) % capacity;
  } while (i != index && flag[i] != MAP_EMPTY);
  throw std::out_of_range("No value found for key " + key + ":" + 
                          featureName + "\n");
                          
}

inline
bool FeatureMap::updateInsert(std::string const& key, 
                               std::string const& featureName,  
                               Feature const& f) 
{
  std::string combinedKey = key + featureName;
  unsigned int hash = hashFunction(combinedKey);
  int i = hash % capacity;
  int index = i;
  do
  {
    if (flag[i] == MAP_EMPTY ||
        flag[i] == MAP_INTERMEDIATE)
    {
      do
      {
        int expected = MAP_EMPTY;
        bool b = std::atomic_compare_exchange_strong(&flag[i], &expected,
                                            MAP_INTERMEDIATE);
        if (b) {
          features[i] = f.createCopy(); 
          keys[i] = combinedKey;
          flag[i] = MAP_OCCUPIED;
          return true;
        }

      } while (flag[i] != MAP_OCCUPIED);
    }

    if (flag[i] == MAP_OCCUPIED && 
        (combinedKey.compare(keys[i]) == 0))
    {
      bool b;
      do
      {
        int expected = MAP_OCCUPIED;
        b = std::atomic_compare_exchange_strong(&flag[i], &expected,
                                                   MAP_INTERMEDIATE);
      } while (!b);

      features[i]->update(f);
      flag[i] = MAP_OCCUPIED;
      return true;
    }

    i = (i + 1) % capacity;
  } while (i != index);
    
  return false;
}


}

#endif
