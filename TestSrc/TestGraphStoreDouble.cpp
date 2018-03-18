#define BOOST_TEST_MAIN TestGraphStore
#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include "GraphStore.hpp"
#include "NetflowGenerators.hpp"
#include <zmq.hpp>

using namespace sam;

typedef GraphStore<Netflow, NetflowTuplizer, SourceIp, DestIp, 
                   TimeSeconds, DurationSeconds, 
                   StringHashFunction, StringHashFunction, 
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
      
    graphStore0 = new GraphStoreType(numNodes, nodeId0, 
                            requestHostnames, requestPorts,
                            edgeHostnames, edgePorts,
                            hwm, graphCapacity, 
                            tableCapacity, resultsCapacity, timeWindow); 
    graphStore1 = new GraphStoreType(numNodes, nodeId1, 
                            requestHostnames, requestPorts,
                            edgeHostnames, edgePorts,
                            hwm, graphCapacity, 
                            tableCapacity, resultsCapacity, timeWindow); 
  

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

  int n = 1000;

  auto graphFunction = [](GraphStoreType* graphStore, int n,
                            AbstractNetflowGenerator* generator)
  {
    for (int i = 0; i < n; i++) {
      std::string str = generator->generate();
      Netflow n = makeNetflow(0, str);
      graphStore->consume(n);
    }
    graphStore->terminate();

  };

  std::thread thread0(graphFunction, graphStore0, n, generator0);
  std::thread thread1(graphFunction, graphStore1, n, generator1);

  thread0.join();
  thread1.join();

  BOOST_CHECK_EQUAL(n, graphStore0->getNumResults());
  BOOST_CHECK_EQUAL(n, graphStore1->getNumResults());
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

  int n = 0;
  
  auto graphFunction = [](GraphStoreType* graphStore,
    int n, AbstractNetflowGenerator* generator)
    
  {
    for (int i = 0; i < n; i++) {
      std::string str = generator->generate();
      Netflow n = makeNetflow(0, str);
      graphStore->consume(n);
    }
    graphStore->terminate();

  };

  std::thread thread0(graphFunction, graphStore0, n, onePairGenerator0);
  std::thread thread1(graphFunction, graphStore1, n, onePairGenerator1);

  thread0.join();
  thread1.join();

  BOOST_CHECK_EQUAL(0, graphStore0->getNumResults());
  BOOST_CHECK_EQUAL(0, graphStore1->getNumResults());

  delete onePairGenerator0;
  delete onePairGenerator1;
}
