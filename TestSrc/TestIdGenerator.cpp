#define BOOST_TEST_MAIN TestIdGenerator
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <sam/IdGenerator.hpp>
#include <atomic>
#include <thread>

using namespace sam;

BOOST_AUTO_TEST_CASE( test_simple_id_generator )
{

  SimpleIdGenerator* idGenerator = idGenerator->getInstance();

  int numThreads = 10000;
  int numTimes = 10000; // How many times each thread requests an id
  std::atomic<std::uint32_t> sum(0);

  std::vector<std::thread> threads;

  for(int i = 0; i < numThreads; i++) {
    threads.push_back(std::thread([&idGenerator, &sum, numTimes]() {
      for (int i = 0; i < numTimes; i++) {
        sum.fetch_add(idGenerator->generate());   
      }
    }));
  }

  for (int i = 0; i < numThreads; i++) {
    threads[i].join();
  }

  uint32_t n = numThreads * numTimes;
  uint32_t expected = n * (n - 1) / 2;
  BOOST_CHECK_EQUAL(expected, sum);
}

