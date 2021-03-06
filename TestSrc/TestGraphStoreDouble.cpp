#define BOOST_TEST_MAIN TestGraphStore

//#define DEBUG

#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <zmq.hpp>
#include <sam/GraphStore.hpp>
#include <sam/tuples/VastNetflowGenerators.hpp>
#include <sam/tuples/Tuplizer.hpp>
#include <sam/tuples/Edge.hpp>
#include <sam/FeatureMap.hpp>


using namespace sam;
using namespace sam::vast_netflow;
using namespace std::chrono;


/**
 * Manufactured hash function that sends 192.168.0.1 and 192.168.0.2
 * to node 0 and 192.168.0.3 and 192.168.0.4 to node 1.
 */
class OneTwoThreeFourHashFunction
{
public:
  inline
  uint64_t operator()(std::string const& s) const {
    size_t index = s.find_last_of(".");
    if (index < s.size()) {
      size_t lastOctet = boost::lexical_cast<size_t>(s.substr(index + 1));
      if (lastOctet == 1 | lastOctet == 2) {
        return 0; 
      } else
      if (lastOctet == 3 | lastOctet == 4) {
        return 1; 
      }
      return lastOctet;
    }
    return 0;
  }
};

typedef VastNetflow TupleType;
typedef EmptyLabel LabelType;
typedef Edge<size_t, LabelType, TupleType> EdgeType;
typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer;
typedef GraphStore<EdgeType, Tuplizer, 
                   SourceIp, DestIp, 
                   TimeSeconds, DurationSeconds, 
                   OneTwoThreeFourHashFunction, OneTwoThreeFourHashFunction, 
                   StringEqualityFunction, StringEqualityFunction>
        GraphStoreType;

typedef EdgeDescription<TupleType, TimeSeconds, DurationSeconds>
        EdgeDescriptionType;

typedef GraphStoreType::QueryType QueryType;

struct DoubleNodeFixture  {

  std::shared_ptr<FeatureMap> featureMap;

  size_t numNodes = 2;
  size_t nodeId0 = 0;
  size_t nodeId1 = 1;
  std::vector<std::string> hostnames;
  size_t hwm = 1000;
  size_t graphCapacity = 1000; //For csc and csr
  size_t tableCapacity = 1000; //For SubgraphQueryResultMap results
  size_t resultsCapacity = 1000; //For final results
  double timeWindow = 100;

  size_t startingPort = 10000;
  size_t numPushSockets = 1;
  size_t numPullThreads = 1;
  size_t timeout = 1000; 

  EdgeFunction starttimeFunction = EdgeFunction::StartTime;
  EdgeFunction endtimeFunction = EdgeFunction::EndTime;
  EdgeOperator equal_edge_operator = EdgeOperator::Assignment;
  EdgeOperator greater_edge_operator = EdgeOperator::GreaterThan;
  std::string e1 = "e1";
  std::string e2 = "e2";
  std::string nodex = "nodex";
  std::string nodey = "nodey";
  std::string nodez = "nodez";

  EdgeExpression* y2x;
  EdgeExpression* z2x;
  TimeEdgeExpression* startY2Xboth;
  TimeEdgeExpression* startZ2Xbeg;

  GraphStoreType* graphStore0; 
  GraphStoreType* graphStore1;
  
  AbstractVastNetflowGenerator *generator0;
  AbstractVastNetflowGenerator *generator1;
   
  DoubleNodeFixture () 
  {
    featureMap = std::make_shared<FeatureMap>(1000);
    y2x = new EdgeExpression(nodey, e1, nodex);
    z2x = new EdgeExpression(nodez, e2, nodex);
    startY2Xboth = new TimeEdgeExpression(starttimeFunction,
                                                  e1, equal_edge_operator, 0);
    startZ2Xbeg = new TimeEdgeExpression(starttimeFunction,
                                                 e2, greater_edge_operator, 0); 

    hostnames.push_back("localhost");
    hostnames.push_back("localhost");

    generator0 = new UniformDestPort("192.168.0.0", 1);
    generator1 = new UniformDestPort("192.168.0.1", 1);
    
    graphStore0 = new GraphStoreType( 
                            numNodes, nodeId0, 
                            hostnames, startingPort,
                            hwm, graphCapacity, 
                            tableCapacity, resultsCapacity, 
                            numPushSockets, numPullThreads, timeout,
                            timeWindow, featureMap, 1, true); 
    graphStore1 = new GraphStoreType( 
                            numNodes, nodeId1, 
                            hostnames, startingPort,
                            hwm, graphCapacity, 
                            tableCapacity, resultsCapacity, 
                            numPushSockets, numPullThreads, timeout,
                            timeWindow, featureMap, 1, true); 
  

  }

  ~DoubleNodeFixture() {
    delete y2x;
    delete z2x;
    delete startY2Xboth;
    delete startZ2Xbeg;
    delete graphStore0; 
    delete graphStore1;
    delete generator0;
    delete generator1;
  }
};


///
/// This tests matching a single edge across two nodes.  This doesn't
/// test the communication of edge requests since each node can
/// process an edge by itself.
BOOST_FIXTURE_TEST_CASE( test_single_edge_match_two_nodes, DoubleNodeFixture )
{
  auto query = std::make_shared<QueryType>(featureMap);
  query->addExpression(*startY2Xboth);
  query->addExpression(*y2x);
  query->finalize();

  graphStore0->registerQuery(query);
  graphStore1->registerQuery(query);


  size_t expected0 = 0;
  size_t expected1 = 0;
  int n = 1000;

  auto graphFunction = [](GraphStoreType* graphStore, int n,
                            AbstractVastNetflowGenerator* generator,
                            size_t threadId, size_t* expected)
  {
    OneTwoThreeFourHashFunction hash;

    double time = 0.0;
   
    auto t1 = std::chrono::high_resolution_clock::now();
    double increment = 0.01;

    size_t totalNetflows = 0;

    Tuplizer tuplizer;

    for (int i = 0; i < n; i++) {
      auto currenttime = std::chrono::high_resolution_clock::now();
      duration<double> diff = duration_cast<duration<double>>(
          currenttime - t1);
     
      if (diff.count() < totalNetflows * increment) 
      {
        size_t numMilliseconds = 
          (totalNetflows * increment - diff.count()) * 1000;
        std::this_thread::sleep_for(
          std::chrono::milliseconds(numMilliseconds));
      } 

      std::string str = generator->generate(time);
      time = time + increment;
      EdgeType edge = tuplizer(totalNetflows++, str);

      // We are simulating the partitioning, so only send netflows
      // that would be sent with partitioning in place.
      std::string source = std::get<SourceIp>(edge.tuple);
      std::string target = std::get<DestIp>(edge.tuple);
      size_t sourceHash = hash(source) % 2;
      size_t targetHash = hash(target) % 2;
  
      if (sourceHash == threadId || targetHash == threadId) {
        graphStore->consume(edge);
      }
      if (sourceHash == threadId) {
        (*expected)++;
      }

    }


    std::this_thread::sleep_for(
      std::chrono::milliseconds(1000));
    graphStore->terminate();

  };

  std::thread thread0(graphFunction, graphStore0, n, generator0, 0, &expected0);
  std::thread thread1(graphFunction, graphStore1, n, generator1, 1, &expected1);

  thread0.join();
  thread1.join();

  BOOST_CHECK_EQUAL(expected0, graphStore0->getNumResults());
  BOOST_CHECK_EQUAL(expected1, graphStore1->getNumResults());
}

///
/// This test creates a two graphstores and we send each graphstore
/// a series of netflows with a source ip/ dest ip pair that is unique
/// to the graphstore.  The pattern we try to match is a->b->c->d.
/// a->b and c->d are the unique pairs for each graphstore, while b->c we 
/// create one instance of.  Let n be the number of c->d edges that occur
/// after the b->c edge.  Then the expected number of matching subgraphs
/// is (n-1)(n)/2.
///  
BOOST_FIXTURE_TEST_CASE( test_match_across_nodes, DoubleNodeFixture )
{
  AbstractVastNetflowGenerator *onePairGenerator0 = 
      new OnePairSizeDist("192.168.0.1","192.168.0.2", 1.0, 1.0, 1.0, 1.0);
  AbstractVastNetflowGenerator *onePairGenerator1 = 
      new OnePairSizeDist("192.168.0.3","192.168.0.4",1,1,1,1);

  ///// Subgraph query setup ///////
  // The node variables as expressed in the subgraph query
  std::string nodeA = "nodeA";
  std::string nodeB = "nodeB";
  std::string nodeC = "nodeC";
  std::string nodeD = "nodeD";

  // Edge identifiers
  std::string e1 = "e1";
  std::string e2 = "e2";
  std::string e3 = "e3";

  EdgeFunction starttimeFunction = EdgeFunction::StartTime;
  EdgeOperator equal_edge_operator = EdgeOperator::Assignment;
  
  // Edge Expressions
  EdgeExpression a2b(nodeA, e1, nodeB);
  EdgeExpression b2c(nodeB, e2, nodeC);
  EdgeExpression c2d(nodeC, e3, nodeD);


  // Time Edge Expressions
  TimeEdgeExpression startA2Bboth(EdgeFunction::StartTime, e1, 
                                  EdgeOperator::Assignment, 0);
  TimeEdgeExpression startB2Cbeg(EdgeFunction::StartTime, e2, 
                                  EdgeOperator::GreaterThan, 0);
  TimeEdgeExpression startC2Dbeg(EdgeFunction::StartTime, e3, 
                                  EdgeOperator::GreaterThan, 0);

  auto query = std::make_shared<QueryType>(featureMap);
  query->addExpression(a2b);
  query->addExpression(b2c);
  query->addExpression(c2d);
  query->addExpression(startA2Bboth);
  query->addExpression(startB2Cbeg);
  query->addExpression(startC2Dbeg);
  query->finalize();

  // Checking that the query laid out how we expect
  EdgeDescriptionType const& edge1 = query->getEdgeDescription(0);
  EdgeDescriptionType const& edge2 = query->getEdgeDescription(1);
  EdgeDescriptionType const& edge3 = query->getEdgeDescription(2);

  BOOST_CHECK_EQUAL(edge1.source, nodeA);
  BOOST_CHECK_EQUAL(edge2.source, nodeB);
  BOOST_CHECK_EQUAL(edge3.source, nodeC);

  graphStore0->registerQuery(query);
  graphStore1->registerQuery(query);

  int n = 2;
 
  double time = 0.0;
  double increment = 1.0;
  size_t numExtra = 10;

  // This function sends the extra netflow to bridge the two data streams. 
  auto graphFunction0 = [&time, increment, numExtra](
    GraphStoreType* graphStore,
    int n, AbstractVastNetflowGenerator* generator)
    
  {
    double time = 0.0;
   
    auto t1 = std::chrono::high_resolution_clock::now();

    std::string netflowString = "1,1,0.5,2013-04-10 08:32:36,"
                             "20130410083236.384094,17,UDP,192.168.0.2,"
                             "192.168.0.3,29986,1900,0,0,0,133,0,1,0,1,0,0";    
    Tuplizer tuplizer;

    size_t totalNetflows = 0;
    for (int i = 0; i < n; i++) {

      auto currenttime = std::chrono::high_resolution_clock::now();
      duration<double> diff = duration_cast<duration<double>>(
          currenttime - t1);
     
      if (diff.count() < totalNetflows * increment) 
      {
        size_t numMilliseconds = 
          (totalNetflows * increment - diff.count()) * 1000;
        std::this_thread::sleep_for(
          std::chrono::milliseconds(numMilliseconds));
      } 

      std::string str = generator->generate(time);
      time = time + increment;

      EdgeType edge = tuplizer(totalNetflows++, str);
      graphStore->consume(edge);
      if (i == 0) {
        graphStore->consume(edge);
      }
    }

    RandomGenerator randomGenerator;
    for(size_t i = 0; i < numExtra; i++) 
    {
      auto currenttime = std::chrono::high_resolution_clock::now();
      duration<double> diff = 
        duration_cast<duration<double>>(currenttime - t1);
      if (diff.count() < totalNetflows * increment)
      {
        size_t numMilliseconds =
          (totalNetflows * increment - diff.count()) * 1000;
        std::this_thread::sleep_for(
          std::chrono::milliseconds(numMilliseconds));
      }

      std::string str = randomGenerator.generate();
      EdgeType edge = tuplizer(totalNetflows++, str);
      graphStore->consume(edge);
    }

    graphStore->terminate();

  };

  auto graphFunction1 = [&time, increment, numExtra](
    GraphStoreType* graphStore,
    int n, AbstractVastNetflowGenerator* generator)
    
  {

    auto t1 = std::chrono::high_resolution_clock::now();

    Tuplizer tuplizer;

    size_t totalNetflows = 0;
    for (int i = 0; i < n; i++) {

      auto currenttime = std::chrono::high_resolution_clock::now();
      duration<double> diff = duration_cast<duration<double>>(
          currenttime - t1);
     
      if (diff.count() < i * increment) {
        size_t numMilliseconds = (i * increment - diff.count()) * 1000;
        std::this_thread::sleep_for(
          std::chrono::milliseconds(numMilliseconds));
      } 

      std::string str = generator->generate(time);
      time += increment;

      EdgeType edge = tuplizer(totalNetflows++, str);
      graphStore->consume(edge);
    }

    RandomGenerator randomGenerator;
    for(size_t i = 0; i < numExtra; i++) 
    {
      auto currenttime = std::chrono::high_resolution_clock::now();
      duration<double> diff = duration_cast<duration<double>>(currenttime - t1);
      if (diff.count() < totalNetflows * increment)
      {
        size_t numMilliseconds =
          (totalNetflows * increment - diff.count()) * 1000;
        std::this_thread::sleep_for(
          std::chrono::milliseconds(numMilliseconds));
      }

      std::string str = randomGenerator.generate();
      EdgeType edge = tuplizer(totalNetflows++, str);
      graphStore->consume(edge);
    }

    graphStore->terminate();

  };


  std::thread thread0(graphFunction0, graphStore0, n, onePairGenerator0);
  std::thread thread1(graphFunction1, graphStore1, n, onePairGenerator1);

  thread0.join();
  thread1.join();

  size_t totalEdgePulls0 = graphStore0->getTotalEdgePulls();
  size_t totalEdgePulls1 = graphStore1->getTotalEdgePulls();
  size_t totalEdgePushes0 = graphStore0->getTotalEdgePushes();
  size_t totalEdgePushes1 = graphStore1->getTotalEdgePushes();
  
  printf("TotalEdgePushes0 %lu\n", totalEdgePushes0);
  printf("TotalEdgePushes1 %lu\n", totalEdgePushes1);
  printf("TotalEdgePulls0 %lu\n", totalEdgePulls0);
  printf("TotalEdgePulls1 %lu\n", totalEdgePulls1);
  BOOST_CHECK_EQUAL(totalEdgePulls0, totalEdgePushes1);
  BOOST_CHECK_EQUAL(totalEdgePulls1, totalEdgePushes0);

  size_t totalRequestPulls0 = graphStore0->getTotalRequestPulls();
  size_t totalRequestPulls1 = graphStore1->getTotalRequestPulls();
  size_t totalRequestPushes0 = graphStore0->getTotalRequestPushes();
  size_t totalRequestPushes1 = graphStore1->getTotalRequestPushes();
  
  printf("TotalRequestPushes0 %lu\n", totalRequestPushes0);
  printf("TotalRequestPushes1 %lu\n", totalRequestPushes1);
  printf("TotalRequestPulls0 %lu\n", totalRequestPulls0);
  printf("TotalRequestPulls1 %lu\n", totalRequestPulls1);
  BOOST_CHECK_EQUAL(totalRequestPulls0, totalRequestPushes1);
  BOOST_CHECK_EQUAL(totalRequestPulls1, totalRequestPushes0);




  size_t totalResults = graphStore0->getNumResults() + 
                        graphStore1->getNumResults();
  BOOST_CHECK_EQUAL(1, totalResults);

  delete onePairGenerator0;
  delete onePairGenerator1;
  printf("End of test\n");
}
