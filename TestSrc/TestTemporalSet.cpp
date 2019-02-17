#define BOOST_TEST_MAIN TestTemporalSet

#include <sam/TemporalSet.hpp>
#include <boost/test/unit_test.hpp>
#include <thread>
#include <vector>


using namespace sam;

typedef TemporalSet<size_t, double> SetType;

struct F 
{
  // Want all keys to go to same table entry.
  size_t tableSize = 1;
  double timeToLive = 20.0;
  UnsignedIntHashFunction hash; 
  SetType* set;

  F() {
    set = new SetType(tableSize, hash, timeToLive);
  }

  ~F() {
    delete set;
  }
};

BOOST_FIXTURE_TEST_CASE( test_exception, F )
{
  set->insert(10, 10.0);
  BOOST_CHECK_THROW( set->insert(10, 0.0), TemporalSetException );
}

BOOST_FIXTURE_TEST_CASE( test_contains, F )
{
  size_t key = 10;
  set->insert(key, 0.0);
  bool result = set->contains(key);
  BOOST_CHECK_EQUAL(result, true);
  result = set->contains(100);
  BOOST_CHECK_EQUAL(result, false);
}

BOOST_FIXTURE_TEST_CASE( test_delete, F )
{
  size_t key1 = 10;
  set->insert(key1, 0.0);
  BOOST_CHECK(set->contains(key1));
  size_t key2 = 20;
  set->insert(key2, 50.0);
  BOOST_CHECK(set->contains(key2));
  BOOST_CHECK(!set->contains(key1));
}

BOOST_AUTO_TEST_CASE( test_multi_threads )
{
  
  size_t tableSize = 1000;
  double timeToLive = 20.0;
  UnsignedIntHashFunction hash; 
  auto set = new SetType(tableSize, hash, timeToLive);
  size_t n = 10000;

  auto function = [](SetType* set, size_t n, size_t threadId) 
  {
    for (size_t i = 0; i < n; i++) {
      set->insert(n * threadId + i, 0);
    }
  };

  size_t numThreads = 4;
  
  std::vector<std::thread> threads;
  threads.resize(numThreads);

  for (size_t i = 0; i < numThreads; i ++) {
    threads[i] = std::thread(function, set, n, i);
  }

  for (size_t i = 0; i < numThreads; i++) {
    threads[i].join();
  }

  BOOST_CHECK_EQUAL( n * numThreads, set->size());

  for (size_t i = 0; i < numThreads * n; i++) {
    BOOST_CHECK(set->contains(i));
  }

  delete set;

}
