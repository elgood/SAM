# The Streaming Analytics Language (SAL) and the Streaming Analytics Machine (SAM). 

# Contents

* ExecutableSrc - This contains the source for executables that utilize
* SAL - The SAL parser that converts SAL code into c++ code (SAM).
SAM.
* SamSrc - The source code for the SAM library.
* TestSrc - Unit tests.
* explore - Directory with code that might be useful but not part of SAM.
* proto - The protobuf description files.
* scripts - Largely python code that was used to perform analysis.

# Prerequisites

* Boost
* protobuf
* cmake
* zmq

# Building

To build:  
cd SAM  
mkdir build  
cd build  
cmake ..  
make  

# Operators

Operators are sliding window algorithms that extract features from the stream of data.  Typically these operators are polylogarithmic in their spatial complexity and only use one pass through the data.  Currently supported operators are:

* Sum - Summation over the sliding window using expoential histograms.  This is implemented in ExponentialHistogramSum.hpp.
* Variance - Calculated again using an exponential histogram.  This is implmented in ExponentialHistogramVariance.hpp.
* TopK - Produces the most frequent keys and their associated frequencies.  Implemented using a Basic Window approach in TopK.hpp.

## Adding Operators

There are several polylog streaming algorithms that are candidates to implement, including:

* K-medians
* Quantiles
* Rarity
* Vector norms
* Similarity
* Count distinct items

Also, other approaches that are not necessarily polylog can also be added.  For example max, min, and autocorrelation are definite candidates.

To add a new operator, several interfaces need to be inherited, namely:

* `AbstractConsumer`
* `BaseComputation`
* `FeatureProducer`

The `AbstractConsumer` has one template argument, the `TupleType`, which is the type of tuple it is expected to consume.  There are three virtual methods in `AbstractConsumer` that must be implemented:

* `consume(TupleType const& item)` - The main method to implement.  This will have the logic for processing the item, updating associated data structures, and calculating the current value.  This method must also update the featuremap, a data structure provided by the BaseCompuation class. It must also notify any subscribers to this feature, which is provided by the FeatureProducer class.
* The destructor - Destructors have to be virtual in C++ or else bad things happen during object destruction.
* terminate - Usually does nothing.

In the `consume` method, you'll need to create a unique key to identify the item.  In most cases, you can use `generateKey` method found in Util.hpp.  For example:

```c++
std::string key = generateKey<keyFields...>(input);
```

Here `keyFields...` is a parameter pack of the fields in the tuple that form the unique key.

You then use this key to update the data structure of the operator and the feature map.  For `ExponentialHistogramSum`, the local data structure is a `std::map`, mapping from the generated key to the associated sliding window.  For the feature map, you first create a `Feature` and add that to the feature map, e.g.

```c++
T currentSum = allWindows[key]->getTotal();
SingleFeature feature(currentSum);
this->featureMap->updateInsert(key, this->identifier, feature);
```

In this example from `ExponentialHistogramSum`, we get the updated value, which in this case is the sum of the sliding window.  We then create a `SingleFeature`, which is the most common type of feature, a singleton value.  The newly created feature is then added to the feature map using the `key` and the `identifier` of the operator to properly index the fature.

