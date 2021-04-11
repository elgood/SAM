#ifndef K_MEDIAN_HPP
#define K_MEDIAN_HPP

/**
 * K-Median Clustering on Sliding Window
 * 
 * Author: Dan Allen
 * 
 * Description: Iterate over a sliding window to calculate
 *  "k" disjoint cluster centroids. 
 * 
 * Issues:
 *  - Current implementation is only going to work with 1D values.
 * 
 * TODO:
 *  - Vector Input
 *  - Custom Feature for KMedians
 *    
 * 
 */

#include <iostream>
#include <map>
#include <cmath>
#include <boost/lexical_cast.hpp>
#include <sam/AbstractConsumer.hpp>
#include <sam/BaseComputation.hpp>
#include <sam/Features.hpp>
#include <sam/Util.hpp>
#include <sam/FeatureProducer.hpp>
#include <sam/tuples/Edge.hpp>

namespace sam 
{

  namespace KMedianDetails {

    template <typename T>
    class KMedianDataStructure {
    private:
      size_t N;
      size_t k;
      T* array; // Sliding window of fixed length N
      T* sortedArray; // Sliding window sorted so median is easy to find
      T* clusterLabel; // Cluster Label for array[i] is clusterLabel[i]
      float* kMedianCentroids; // 1 centroid per cluster
      int current = 0;
      int kIndex = 0;

    public:
      KMedianDataStructure(size_t N, size_t k) {
        this->N = N;
        this->k = k;
        int i;
        array = new T[N]();
        for (i = 0; i < N; i++) array[i] = 0;
        sortedArray = new T[N]();
        for (i = 0; i < N; i++) sortedArray[i] = 0;
        kMedianCentroids = new float[k]();
        for (i = 0; i < k; i++) kMedianCentroids[i] = 0;
      }

      ~KMedianDataStructure() {
        delete[] array;
        delete[] kMedianCentroids;
      }

      /**
       * Adds an item, removing an item if need be, and
       * updating the median
       */
      void insert(T item) {
        // Debug
        std::cout << "Insert Item: " << item << std::endl;

        // Insert item, overwriting the oldest
        array[current] = item;
        current++;
        if (current >= N) {
          current = 0;
        }

        // TODO: Calculate k-median
        // 1. Select initial centroid values
        // 2. Label Nodes based on which centroid is closest
        // 3. Calculate the next set of centroids
        // 4. Repeat at step 2 for a fixed amount of iterations
        //     or until less than some error.
        // int n = sizeof(sortedArray) / sizeof(sortedArray[0]);
        // TODO: can we optimize each k separately?
        int c1, c2;
        for (kIndex = 0; kIndex < k; kIndex++) {
          // Sort the array to easily find the median
          sortedArray = array;
          std::cout << "Length of Sliding Window: " << N << std::endl;
          std::sort(sortedArray, sortedArray + N);
          // Use the average of the middle 2 values if even number elements
          if (N % 2 == 0) {
            c1 = sortedArray[(N / 2) - 1];
            c2 = sortedArray[N / 2]; 
            kMedianCentroids[kIndex] = (c1 + c2) / 2;
          } else {
            kMedianCentroids[kIndex] = sortedArray[(int)(N / 2)];
          }
        }    
      }

      T getKMedian() {
        // Testing (only works for one k)
        return kMedianCentroids[0];
      }
    };
  }

  // TODO: Add a second valueField in template to allow 2D.
  // TODO: Explain how it can be expanded to higher dimension
  //        vectors.
  template <typename T, typename EdgeType,
            size_t keyField, size_t valueField>
  class KMedian: public AbstractConsumer<EdgeType>, 
                  public BaseComputation,
                  public FeatureProducer
  {
    public:
      typedef typename EdgeType::LocalTupleType TupleType;
    private:
      size_t N; ///> Size of sliding window
      size_t k; ///> Number of disjoint clusters
      typedef KMedianDetails::KMedianDataStructure<T> value_t;
      // Store the single sliding window
      value_t* slidingWindow;
      
    public:
      KMedian(size_t N,
              size_t k,
              size_t nodeId,
              std::shared_ptr<FeatureMap> featureMap,
              std::string identifier) :
        BaseComputation(nodeId, featureMap, identifier) 
      {
        this->N = N;
        this->k = k;
        // Build the value type to add to the window
        slidingWindow = new value_t(N, k); 
      }

      ~KMedian()   {
        delete slidingWindow;
      }

      bool consume(EdgeType const& edge) 
      {
        TupleType tuple = edge.tuple;

        this->feedCount++;
        if (this->feedCount % this->metricInterval == 0) {
          std::cout << "KMedian: NodeId " << this->nodeId << " feedCount " 
                    << this->feedCount << std::endl;
        }

        // Generates unique key from key fields 
        std::string key = generateKey<keyField>(tuple);
        std::cout << "Key: " << key << std::endl;
        

        std::string sValue = 
          boost::lexical_cast<std::string>(std::get<valueField>(tuple));
        std::cout << "Value to Insert: " << sValue << std::endl;
        T value;
        try {
          value = boost::lexical_cast<T>(sValue);
        } catch (std::exception e) {
          std::cerr << "KMedian::consume Caught exception trying to cast string "
                    << "value of " << sValue << std::endl;
          std::cerr << e.what() << std::endl;
          value = 0;
        }

        std::cout << "Insert value into slidingWindow" << std::endl;
        slidingWindow->insert(value);
        
        // Getting the current kmedian and providing that to the featureMap.
        T currentKMedian = slidingWindow->getKMedian();
        SingleFeature feature(currentKMedian);
        this->featureMap->updateInsert(key, this->identifier, feature);

        notifySubscribers(edge.id, currentKMedian);

        return true;
      }

      T getKMedian() {
        return slidingWindow->getKMedian();
      }

      // std::vector<std::string> keys() const {
      //   std::vector<std::string> theKeys;
      //   for (auto p : allWindows) {
      //     theKeys.push_back(p.first);
      //   }
      //   return theKeys;
      // }

      void terminate() {}
  };

}


#endif
