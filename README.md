# The Streaming Analytics Language (SAL) and the Streaming Analytics Machine (SAM). 

# Contents

* ExecutableSrc - This contains the source for executables that utilize
* SAL - The SAL parser that converts SAL code into c++ code (SAM).  [SAL documentation](SAL/Parser/README.md)
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

Note: On Monterey mac OS, I needed this in my environment:
LDFLAGS=-L/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/lib

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

# Tuple Types

SAM processes streams of tuples.  The currently supported tuple types are 

* [Netflow version 5](https://www.cisco.com/c/en/us/td/docs/net_mgmt/netflow_collection_engine/3-6/user/guide/format.html)
* [Vast challenge format](http://vacommunity.org/VAST+Challenge+2013%3A+Mini-Challenge+3): A netflow format that was used for the 2013 VAST Challenge.

In the future, we will support defining a schema for a tuple, which will then generate the appropriate code, but right now the process is manual.  Tuples are defined in SamSrc/sam/tuples.  NetflowV5.hpp and VastNetflow.hpp provide examples of how to create tuples within SAM.  We will use NetflowV5.hpp for the following examples.

Typically we begin with specifying namespaces:
```c++
namespace sam {
namespace netflowv5 {
```
We define a namespace for the tuple type itself because fieldnames can be duplicated between tuple types, potentially causing confusion and compiler errors.



The first step is to create a list of constants that define the numeric location of each tuple field.  You should have an entry for each field in your tuple.

```c++
const unsigned int UnixSecs       = 0;
const unsigned int UnixNsecs     = 1;
const unsigned int SysUptime     = 2;
const unsigned int Exaddr        = 3;
const unsigned int Dpkts         = 4;
const unsigned int Doctets        = 5;
...
```

Next we create a type definition to alias std::tuple to the name of our tuple.  Each template parameter of the tuple needs to match the expected type of the tuple, and in the order previously defined with the numeric indices. 

```c++
typedef std::tuple<long,        //UnixSecs                      
                    long,        //UnixNsecs                   
                    long,        //SysUptime                  
                    std::string, //Exaddr                    
                    std::size_t, //Dpkts                    
                    std::size_t, //Doctets     
...
                  > NetflowV5;
```

Next we create a way to convert a string that is comma delimited into the tuple type we are defining.


```c++
inline
NetflowV5 makeNetflowV5(std::string s)
{

  // All these fields will be populated.
  long        unixSecs;
  long        unixNsecs;
  long        sysUptime;
  std::string exaddr;
  std::size_t dpkts;
  std::size_t doctets;
...

  std::stringstream ss(s);
  std::string item;
  std::getline(ss, item, ',');
  unixSecs = boost::lexical_cast<long>(item);
  std::getline(ss, item, ',');
  unixNsecs = boost::lexical_cast<long>(item);
  std::getline(ss, item, ',');
  sysUptime = boost::lexical_cast<long>(item);
  std::getline(ss, item, ',');
  exaddr = boost::lexical_cast<long>(item);
  std::getline(ss, item, ',');
  dpkts = boost::lexical_cast<size_t>(item);
  std::getline(ss, item, ',');
  doctets = boost::lexical_cast<size_t>(item);
...

return std::make_tuple( unixSecs,
                          unixNsecs,
                          sysUptime,
                          exaddr,
                          dpkts,
                          doctets,
...
                          );
}
```

Finally we make a class that performs the transformation from string to tuple type.  We use this pattern so we can pass it to the *Tuplizer* template parameter of many classes within SAM.

```c++
class MakeNetflowV5
{
public:
  NetflowV5 operator()(std::string const& s)
  {
    return makeNetflowV5(s);
  }
};
```

Once all these pieces are defined you can make use of the tuple processing capability within SAM.  To refer to the tuple within SAL, see the [documentation on extending SAL](SAL/Parser/README.md).

