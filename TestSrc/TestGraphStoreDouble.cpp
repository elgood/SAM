#define BOOST_TEST_MAIN TestGraphStore

//#define DEBUG

#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include "GraphStore.hpp"
#include "NetflowGenerators.hpp"
#include <zmq.hpp>


using namespace sam;
using namespace std::chrono;

zmq::context_t context(1);

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
    //printf("blah %s %s\n", s.c_str(), s.substr(index+1).c_str());
    size_t lastOctet = boost::lexical_cast<size_t>(s.substr(index + 1));
    if (lastOctet == 1 | lastOctet == 2) {
      return 0; 
    } else
    if (lastOctet == 3 | lastOctet == 4) {
      return 1; 
    } 
    return lastOctet;
  }
};

typedef GraphStore<Netflow, NetflowTuplizer, SourceIp, DestIp, 
                   TimeSeconds, DurationSeconds, 
                   OneTwoThreeFourHashFunction, OneTwoThreeFourHashFunction, 
                   StringEqualityFunction, StringEqualityFunction>
        GraphStoreType;

typedef EdgeDescription<Netflow, TimeSeconds, DurationSeconds>
        EdgeDescriptionType;

struct DoubleNodeFixture  {


  size_t numNodes = 2;
  size_t nodeId0 = 0;
  size_t nodeId1 = 1;
  std::vector<std::string> requestHostnames;
  std::vector<size_t> requestPorts;
  std::vector<std::string> edgeHostnames;
  std::vector<size_t> edgePorts;
  size_t hwm = 1000;
  size_t graphCapacity = 1000; //For csc and csr
  size_t tableCapacity = 1000; //For SubgraphQueryResultMap results
  size_t resultsCapacity = 1000; //For final results
  double timeWindow = 100;

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
  
  AbstractNetflowGenerator *generator0;
  AbstractNetflowGenerator *generator1;
   
  DoubleNodeFixture () {
    y2x = new EdgeExpression(nodey, e1, nodex);
    z2x = new EdgeExpression(nodez, e2, nodex);
    startY2Xboth = new TimeEdgeExpression(starttimeFunction,
                                                  e1, equal_edge_operator, 0);
    startZ2Xbeg = new TimeEdgeExpression(starttimeFunction,
                                                 e2, greater_edge_operator, 0); 

    requestHostnames.push_back("localhost");
    requestPorts.push_back(10000);
    requestHostnames.push_back("localhost");
    requestPorts.push_back(10001);
    edgeHostnames.push_back("localhost");
    edgePorts.push_back(10002);
    edgeHostnames.push_back("localhost");
    edgePorts.push_back(10003);

    generator0 = new UniformDestPort("192.168.0.0", 1);
    generator1 = new UniformDestPort("192.168.0.1", 1);
     
    size_t numThreads = 1; 
      
    graphStore0 = new GraphStoreType(context, 
                            numNodes, nodeId0, 
                            requestHostnames, requestPorts,
                            edgeHostnames, edgePorts,
                            hwm, graphCapacity, 
                            tableCapacity, resultsCapacity, 
                            timeWindow, numThreads); 
    graphStore1 = new GraphStoreType(context, 
                            numNodes, nodeId1, 
                            requestHostnames, requestPorts,
                            edgeHostnames, edgePorts,
                            hwm, graphCapacity, 
                            tableCapacity, resultsCapacity, 
                            timeWindow, numThreads); 
  

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
  SubgraphQuery<Netflow, TimeSeconds, DurationSeconds> query;
  query.addExpression(*startY2Xboth);
  query.addExpression(*y2x);
  query.finalize();

  graphStore0->registerQuery(query);
  graphStore1->registerQuery(query);


  size_t expected0 = 0;
  size_t expected1 = 0;
  int n = 1000;

  auto graphFunction = [](GraphStoreType* graphStore, int n,
                            AbstractNetflowGenerator* generator,
                            size_t threadId, size_t* expected)
  {
    OneTwoThreeFourHashFunction hash;

    for (int i = 0; i < n; i++) {
      std::string str = generator->generate();
      Netflow netflow = makeNetflow(0, str);

      // We are simulating the partitioning, so only send netflows
      // that would be sent with partitioning in place.
      std::string source = std::get<SourceIp>(netflow);
      std::string target = std::get<DestIp>(netflow);
      size_t sourceHash = hash(source) % 2;
      size_t targetHash = hash(target) % 2;
  
      if (sourceHash == threadId || targetHash == threadId) {
        graphStore->consume(netflow);
      }
      if (sourceHash == threadId) {
        (*expected)++;
      }

    }
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
  AbstractNetflowGenerator *onePairGenerator0 = 
      new OnePairSizeDist("192.168.0.1","192.168.0.2", 1.0, 1.0, 1.0, 1.0);
  AbstractNetflowGenerator *onePairGenerator1 = 
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

  SubgraphQuery<Netflow, TimeSeconds, DurationSeconds> query;
  query.addExpression(a2b);
  query.addExpression(b2c);
  query.addExpression(c2d);
  query.addExpression(startA2Bboth);
  query.addExpression(startB2Cbeg);
  query.addExpression(startC2Dbeg);
  query.finalize();

  // Checking that the query laid out how we expect
  EdgeDescriptionType const& edge1 = query.getEdgeDescription(0);
  EdgeDescriptionType const& edge2 = query.getEdgeDescription(1);
  EdgeDescriptionType const& edge3 = query.getEdgeDescription(2);

  BOOST_CHECK_EQUAL(edge1.source, nodeA);
  BOOST_CHECK_EQUAL(edge2.source, nodeB);
  BOOST_CHECK_EQUAL(edge3.source, nodeC);

  graphStore0->registerQuery(query);
  graphStore1->registerQuery(query);

  int n = 2;
 
  double time = 0.0;
  double increment = 1.0;

  // This function sends the extra netflow to bridge the two data streams. 
  auto graphFunction0 = [&time, increment](GraphStoreType* graphStore,
    int n, AbstractNetflowGenerator* generator)
    
  {
    double time = 0.0;
   
    auto starttime = std::chrono::high_resolution_clock::now();

    std::string netflowString = "1,1,0.5,2013-04-10 08:32:36,"
                             "20130410083236.384094,17,UDP,192.168.0.2,"
                             "192.168.0.3,29986,1900,0,0,0,133,0,1,0,1,0,0";    

    size_t id = 0;
    for (int i = 0; i < n; i++) {

      auto currenttime = std::chrono::high_resolution_clock::now();
      duration<double> diff = duration_cast<duration<double>>(
          currenttime - starttime);
     
      if (diff.count() < i * increment) {
        size_t numMilliseconds = (i * increment - diff.count()) * 1000;
        std::this_thread::sleep_for(
          std::chrono::milliseconds(numMilliseconds));
      } 

      std::string str = generator->generate(time);
      time = time + increment;

      Netflow n = makeNetflow(id++, str);
      graphStore->consume(n);
      if (i == 0) {
        Netflow netflow = makeNetflow(id++, netflowString);
        graphStore->consume(netflow);
      }
    }
    graphStore->terminate();

  };

  auto graphFunction1 = [&time, increment](GraphStoreType* graphStore,
    int n, AbstractNetflowGenerator* generator)
    
  {

    auto starttime = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < n; i++) {

      auto currenttime = std::chrono::high_resolution_clock::now();
      duration<double> diff = duration_cast<duration<double>>(
          currenttime - starttime);
     
      if (diff.count() < i * increment) {
        size_t numMilliseconds = (i * increment - diff.count()) * 1000;
        std::this_thread::sleep_for(
          std::chrono::milliseconds(numMilliseconds));
      } 


      std::string str = generator->generate(time);
      time += increment;

      Netflow n = makeNetflow(i, str);
      graphStore->consume(n);
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
  
  //printf("TotalEdgePushes0 %lu\n", totalEdgePushes0);
  //printf("TotalEdgePushes1 %lu\n", totalEdgePushes1);
  //printf("TotalEdgePulls0 %lu\n", totalEdgePulls0);
  //printf("TotalEdgePulls1 %lu\n", totalEdgePulls1);
  BOOST_CHECK_EQUAL(totalEdgePulls0, totalEdgePushes1);
  BOOST_CHECK_EQUAL(totalEdgePulls1, totalEdgePushes0);

  size_t totalRequestPulls0 = graphStore0->getTotalRequestPulls();
  size_t totalRequestPulls1 = graphStore1->getTotalRequestPulls();
  size_t totalRequestPushes0 = graphStore0->getTotalRequestPushes();
  size_t totalRequestPushes1 = graphStore1->getTotalRequestPushes();
  
  //printf("TotalRequestPushes0 %lu\n", totalRequestPushes0);
  //printf("TotalRequestPushes1 %lu\n", totalRequestPushes1);
  //printf("TotalRequestPulls0 %lu\n", totalRequestPulls0);
  //printf("TotalRequestPulls1 %lu\n", totalRequestPulls1);
  BOOST_CHECK_EQUAL(totalRequestPulls0, totalRequestPushes1);
  BOOST_CHECK_EQUAL(totalRequestPulls1, totalRequestPushes0);




  size_t totalResults = graphStore0->getNumResults() + 
                        graphStore1->getNumResults();
  BOOST_CHECK_EQUAL(1, totalResults);

  delete onePairGenerator0;
  delete onePairGenerator1;
  printf("End of test\n");
}
