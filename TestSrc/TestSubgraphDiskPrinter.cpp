#define BOOST_TEST_MAIN TestSubgraphDiskPrinter
#include <boost/test/unit_test.hpp>
#include <cstdio>
#include <sam/SubgraphDiskPrinter.hpp>
#include <sam/AbstractSubgraphPrinter.hpp>
#include <sam/tuples/VastNetflow.hpp>
#include <sam/tuples/Edge.hpp>
#include <sam/tuples/Tuplizer.hpp>

using namespace sam;
using namespace sam::vast_netflow;

typedef VastNetflow TupleType;
typedef EmptyLabel LabelType;
typedef Edge<size_t, LabelType, TupleType> EdgeType;
typedef TuplizerFunction<EdgeType, MakeVastNetflow> Tuplizer;
typedef AbstractSubgraphPrinter<EdgeType, SourceIp, DestIp,
          TimeSeconds, DurationSeconds> AbstractPrinterType;
typedef SubgraphDiskPrinter<EdgeType, SourceIp, DestIp, 
          TimeSeconds, DurationSeconds> PrinterType;
typedef PrinterType::ResultType ResultType;
typedef SubgraphQuery<TupleType, SourceIp, DestIp, TimeSeconds, 
          DurationSeconds> QueryType;

struct SetUp {

  std::string loc = "./subgraphoutput.txt";
  std::shared_ptr<AbstractPrinterType> printer;

  Tuplizer tuplizer;

  // A netflow to use to create a subgraph query result.
  std::string netflowString1 = "156.0,2013-04-10 08:32:36,"
                           "20130410083236.384094,17,UDP,target,"
                           "bait,29986,1900,0,0,1.0,133,0,1,0,1,0,0"; 
  EdgeType netflow1 = tuplizer(0, netflowString1);

  // Expressing a subgraph query
  std::shared_ptr<TimeEdgeExpression> startTimeExpressionE1;
  std::shared_ptr<EdgeExpression> targetE1Bait;
  std::string bait = "bait";

  std::shared_ptr<FeatureMap> featureMap;

  std::shared_ptr<QueryType> query;

  SetUp() 
  {
    printer = std::make_shared<PrinterType>(loc);

    // Expressing a subgraph query
    startTimeExpressionE1 = std::make_shared<TimeEdgeExpression>(
      EdgeFunction::StartTime, "e1", EdgeOperator::Assignment, 0);
    targetE1Bait = std::make_shared<EdgeExpression>("target1", "e1", bait);

    featureMap = std::make_shared<FeatureMap>(1000);
    query = std::make_shared<QueryType>(featureMap);
    query->addExpression(*startTimeExpressionE1);
    query->addExpression(*targetE1Bait);
    query->finalize();
  }

  ~SetUp() 
  {
    std::remove(loc.c_str());
  }
};


BOOST_FIXTURE_TEST_CASE( test, SetUp )
{
  ResultType result(query, netflow1);
  printer->print(result);
}
